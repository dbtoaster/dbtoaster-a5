(**
  K3 normalization and optimization

{b Lifting (aka K3 expression normalization):}

- conditionals:
  - the point of lifting is to enable optimization of pre, post code in
     the context of each branch.
  - we lift conditionals to the top of their dependency block, pushing
     parent code into both branches as necessary.
  - lifting increases code size, thus we may want a lowering transformation
     at the very end if no other optimizations have been applied to either
     branch.

- functions: overall goal is to simplify any partially evaluted function to
  a normalized form, of lambda x -> lambda y -> ... , where x,y are the
  remaining variables to be bound before evaluation can complete 

- collection functions:
  - we lift collection operations applied to partially evaluated functions,
  - two cases:
    - partially evaluated collections, e.g.:
      map(lambda y -> ..., lambda x -> collection...)
      => lambda x -> map(lambda y -> ..., collection ...)
    - partially evaluated apply-each
      map(lambda a -> lambda b c -> ..., collection(b;c))
      => lambda a -> map(lambda b c -> ..., collection(b;c))
  - transformation order: pe-coll first, then pe-appeach 
  - when can we not lift? see notes on lifting in typechecking:
    -  collections of functions, e.g.
        map(lambda x ..., collection(lambda y -> ...))
  - cases not handled:
    - partially evaluted init aggregate values
    - partially evaluted group-by functions

- regular functions:
  - we lift lambdas above partial function applications, e.g.:
     apply(lambda a -> lambda b -> ..., x)
     => lambda b -> apply (lambda a -> ..., x)

- what about interaction of conditionals and lifting?
  - must respect data dependencies, i.e. conditionals using bindings cannot
    be lifted above the relevant lambda
  - otherwise everything else is fair game, provided we duplicate code as
    necessary.

- expression normalization algorithm:
 - recursively turn every sub expression into the form:
   lambda ... ->
       independent ifs
       then (... dependent ifs ... collection ops (..., collection) ... )
       else (... dependent ifs ... collection ops (..., collection) ... )
   where the outer most lambdas are for any free variables in the body
   and the independent ifs are those if statements that depend only on these
   free variables. Dependent ifs are those whose predicates are bound by
   some enclosed expression.

 - lifting lambdas:
   - cannot lift lambda above apply without changing typing of
     surrounding code
     - we could inline the application instead, but this will blow up
       code size, or could lead to multiple recomputations of the
       argument. In K3, since applies used for accessing maps, this would
       mean multiple conversions of maps to our internal temporary
        collections.
     - we'll only eliminate lambdas (i.e. inline) for flat arguments.

   - lifting algorithm:
     - this is not standard lambda lifting, which aims at optimizing
        closure construction, etc, rather it is to create a normalized
        form for partially evaluated K3. This ensures other optimizations
        can fully apply to the curried function's body.
     - in general, lift any remaining lambdas from all child expressions in
        left-to-right AST order
     - pattern match on:
       - collection ops, yield lifted form.
         this will fully lift lambdas out of collection operation chains.
       - project, singletons
       - binops: lambdas down each branch get lifted outside the binop, e.g.
         (lambda x -> ...) * (lambda y -> ...)
         => lambda x -> lambda y -> ... * ...
       - blocks: lambdas for each stmt get lifted
       - lambda, assoc lambda: lambdas from bodies get lifted to create
         nested form
       - apply: partial evaluations get lifted
         - we must be careful not to change application order of args
         - apply binds from left-to-right, thus nested lambdas from partials
           must stay inner-most (i.e. right-most) until lifted outside 
           the apply
       - tuple collection ops: map expr and key lambdas get lifted
       - persistent collections: cannot contain lambdas
       - updates: lift from all children

   - let's defer this since the M3 generated will not contain opportunities
     to apply this immediately... BUT can it occur in an intermediate
     form after some other optimization we apply?

 - lifting ifs: TODO
*)


(* Collection operation transformations
 *
 *  ++ transformation ordering: what order do we normalize/optimize?
 *    1. lambdas first, since they may block condition lifting
 *    2. conditions second. Note this implies all normalization transformations
 *       before any optimizations.
 *    -- see below regarding lifting ifs and collection transformation
 *       algorithm traversal
 *
 *    3. collection ops w/ function composition
 *      ++ eliminate map-, then agg-chains, i.e.:
 *           map(f,map(g,c)) and agg(f,i,map(g,c))
 *        -- assuming f,g are simple/flat
 *      ++ push down aggregations i.e.:
 *           agg(f,i,map(lambda.map(g,c2),c1))
 *
 *      ++ what about map(f,gbagg(g,i,h,c)) ?
 *        -- we can compose f/g, i.e => gbagg(compose(f,g),i,h,c)
 *        -- this is like agg-chaining since it removes a map, yielding a
 *           strictly simpler expression
 *
 *      ++ and gbagg(f,i,g,map(h,c)) ?
 *        -- we can compose g/h, i.e. => gbagg(f,i,compose(g,h),c) 
 *
 *    4. pipelining/intermediates optimization, w/ inlining+arity transforms
 *      ++ nest map-chains when parent map function is non-selective
 *         i.e. map(f=lambda x.map(g=lambda y. ..,c2),map(h=lambda z. ..,c1))
 *         with non-selective h
 *        -- selective fn definition? any pruning conditions on the variable z,
 *           where a pruning condition is the predicate part of an IfThenElse0.
 *      ++ TODO: join ordering?
 *        -- note we are getting to join ordering here. for example if g is
 *           selective w.r.t y and separable w.r.t x, but h is not, we want
 *           map(g',c2) as the outer collection, where g' only operates on y.
 *        -- if both g and h are selective we may want to transform based on
 *           cardinalities.
 *        -- this is interesting, but generally gets onto decomposition of
 *           functions w.r.t a free variable. Let's do this later.
 *      ++ TODO: vectorization/batching?
 *
 *    -- why do pipelining transforms after pushing down aggregates? does the
 *       order of these two optimizations matter? DONE: see answer below.
 *      ++ eliminating chains is a no-brainer, since the code is strictly simpler
 *      ++ pipelining should also result in strictly simpler code
 *         (i.e. fewer maps), since we eliminate map-chains w/ composition
 *      ++ this is not the case with pushing aggs, since it duplicates the agg
 *         into applying on both the inner and outer collection
 *      ++ since pushing down aggs does not yield a strictly simpler form, are
 *         there opportunities to apply the previous transforms again after
 *         pushing aggs? do we need optimization to a fixpoint?
 *        -- at the very least, two aggs might lead to agg-chain optimizations
 *        -- why fixpoint, and not just recursive optimization? and do we want
 *           to optimize bottom-up or top-down or both?
 *        -- I think we can do top-down local fix-points, that is each
 *           sub-expression converges since:
 *           i. for maps, optimizations of substructures do not change the
 *              map function f. That is, if f is selective, or nested, or flat
 *              it will remain so.
 *              However the RHS collection can change, implying we should
 *              optimize the whole expr again, BUT only if there was some
 *              change.
 *              Overall the RHS collection gets simpler at each step, while f
 *              becomes more complex through composition. That is our transforms
 *              are monotonically increasing compositions on f. We never go the
 *              other way, where we break up/decompose f, since in general, we
 *              don't know how to decompose functions.
 *          ii. for aggs, we may make the RHS more complex by duplicating an
 *              agg there in the case of a nested RHS collection. We need to
 *              show that this doesn't continue unbounded. All other cases
 *              monotonically compose with f.
 *              This works for the following reason. Consider the transform:
 *                 agg(f,i,x=map(g=lambda.map(h,c2),c1))
 *                 => agg(f,i,x'=map(g'=lambda.agg(f,0,map(h,c2)),c1))
 *              This transformation changes an RHS map operation x->x' by
 *              pushing an aggregate into a nested map function g->g'.
 *              The key point is that c1 is unchanged here. That is, the
 *              transform only affects the function part of the nested map,
 *              which just as above, only makes a function more complex (g->g').
 *              Any subsequent transformations of the RHS map operation x would
 *              continue to simplify c1, at the expense of the inner function g',
 *              and we would never reverse the effects of this aggregate
 *              pushdown operation since we do not decompose functions.
 *      ++ this answers the above question. We simultaneously match for:
 *           i. simple map- and agg-chains
 *           ii. aggregate pushdowns
 *           iii. non-selective nested map-chains
 *         and apply transformations in a top-down local-fixpoint manner. 
 *
 *    -- do collection optimizations affect partial evaluation or conditions?
 *      ++ this determines the scope of the optimization transformation, i.e.
 *         can we do lambda and condition lifting entirely before structural
 *         recursion?
 *      ++ i don't think so for partial evals.
 *      ++ for conditionals, the question is can it introduce new independent
 *         conditions (that should be lifted to the top), and suboptimal
 *         dependent conditions (that should be lifted to their uppermost
 *         positions)?
 *        -- the answer is no: collection optimizations only affect collection
 *           operations, and compose/inline functions. Function composition and
 *           inlining only eliminates function applications, changes function
 *           arities, and performs variable->code substitutions, none of which
 *           introduces or eliminates conditionals... assuming the code
 *           which we substitute for variables has no conditionals. Even if
 *           this is the case, these conditions would be dependent ones,
 *           otherwise they would already have been lifted. Dependent conditions
 *           are only locally suboptimal, i.e. not so bad. I'll defer optimizing
 *           these local dependent conditions later...
 *        -- note this is also related to join optimizations, since we may
 *           have dependent IfThenElse0s inside maps that we could lift, with a
 *           no-op else condition. However so far I've been considering
 *           only functional 2-branch IfThenElse's.
 *        -- note that lifting dependent IfThenElses can complicate detection of
 *           nesting chains. This means our collection transformation algorithm
 *           has to work around dependent IfThenElses lifted to their
 *           dependency points.
 *        -- also, it is up to the inlining transformation to determine how
 *           to break the dependency (i.e. by code substitution, or  
 *           preserving the local binding by an enclosing function application
 *           with the composed function's body)
 *        -- furthermore there will be other AST nodes besides IfThenElses that
 *           our collection transformations will have to navigate through.
 *           Dependent IfThenElses that have been lifted to their dependent
 *           lambdas are not the major concern, since we choose to transform
 *           based on the presence of a RHS collection chain (i.e.
 *           collop(...,collop(...)) ), and whether the inner (RHS) collection
 *           op function is selective. Thus we do not care about the outer
 *           collection function. The collection transform algorithm will
 *           recursively descend down that branch, jumping over
 *           non-collection ops.
 *        -- ISSUE: the hard part is when the RHS collection op is masked, e.g.
 *           in both branches of an IfThenElse, where we would have to detect
 *           equivalent branches... we cannot optimize this for now.
 *        -- the only other expression that masks a collection op is a block,
 *           where the return value is a collection op. everything else is
 *           either semantically not a collection op, or cannot have any
 *           nested collection ops lifted.
 *  
 *      ++ the conclusion is: we can do all lambda and conditional lifting
 *         prior to collection optimizations.
 *
 *    -- what about inlining composed functions? do we defer this until after
 *       all collection transformations since these only monotonically compose?
 *      ++ the question is whether inlining can lead to further collection
 *         transformations...  
 *      ++ the collection transformation algorithm will have to navigate
 *         around other expressions anyway, so what additional transforms
 *         could there be that aren't already matched?
 *      ++ if the substituted body itself contains collection operations, these
 *         could be matched for further optimization, potentially customized
 *         at each invocation point
 *      ++ consider map(f,map(g=lambda x.map(h,c2),c1)). we would want to
 *         inline the function body of g into f to apply further collection
 *         optimizations when recurring into the new f's body.
 *      ++ thus whenever we compose, we inline as deeply as possible, 
 *         substituting all instances of the composed function body
 *        -- we can extend this later to be more efficient if needs be
 *      ++ however note that M3 programs do not contain deeply nested *RHS*
 *         collections since maps are at most two-levels.
 *)

(* Functional optimizations
 * -- function composition: I think this currently has the most scope for
 *    actually impacting our code.
 *   ++ map(f,map(g,c)) => compose(f = lambda, g = lambda) = ...
 *     -- should we separate the case when f is nested, or implement with a
 *        single function, and rely on inlining to simplify the nested case?
 *     -- I think we should have a simple compose function, and do the deeper
 *        transformations with inlining
 *   ++ agg(f,i,map(g,c)) => compose(f = assoc_lambda, g = lambda) = ...
 *   ++ map(f,gbagg(g,i,c)) => compose(f = lambda, g = assoc_lambda) = ...
 *
 *
 * -- inlining transforms
 *   ++ in general, if there is only a single usage of a var in a lambda, we
 *      should inline any applications of that lambda
 *   ++ how does inlining work side-by-side w/ function composition? i.e. after
 *      we compose functions, should we always inline? DONE: yes, for now,
 *      until we want to optimize multiple invocations.
 *   ++ see the notes above about dependency breaking. this is related to
 *      multiple invocations of the substituted body and efficiency
 *
 *   ++ the interesting compositions are for nested map chains and aggregate
 *      pushdown on an RHS nested map
 *
 *     -- overall, the major simplification is that there are no arity changes
 *        any more for nested maps or aggregates. Instead we do tuple binding
 *        for substitutions wherever necessary. 
 *
 *     -- map(f=lambda x.map(g=lambda y.gb,c2),map(h=lambda z. hb, c1))
 *        => map(f'=lambda z.app(f=lambda x.map(g=lambda y.gb,c2),hb),c1)
 *        => map(f'=lambda z.sub[x->hb](map(g=lambda y.gb,c2)),c1)
 *        => map(f'=lambda z.map(g'=lambda y.sub[x->hb](gb),sub[x->hb](c2)),c1)
 *       ++ note the nested function g has its body transformed, but not its
 *          signature (it is still g' : y -> _)
 *       ++ this case does not change any internal collection depth, and
 *          still requires an outer flattening to yield a collection of tuples
 *
 *     -- the function arity complications previsouly came from our use of
 *        nested lambdas as multivariate functions. the examples below hide
 *        this since we are not considering tuples above, which would lead to
 *        different types for x,z, and would require the substitution to
 *        perform tuple binding rather than just using the tuple hb...
 *       ++ how can we inline while performing tuple binding as necessary?
 *       ++ the key step is in changing from a function application of the
 *          body hb above, into a code substitution of x->hb. Here x will be
 *          an arg type, and if it is a tuple arg, we must substitute each
 *          field of x with the appropriate field from hb.
 *
 *     --  I previously did this for a 3-way join to a 3-way product.
 *         Let's recreate that case.
 *        map(f=lambda v.map(g=lambda w.gb,c1),
 *            map(h=lambda x.map(i=lambda y.ib, c2), map(j=lambda z.jb, c0)))
 *        => map(f=lambda v.map(g=lambda w.gb,c1),
 *            map(h'=lambda z.app(lambda x.map(i=lambda y.ib, c2), jb), c0))
 *        => map(f=lambda v.map(g=lambda w.gb,c1),
 *            map(h'=lambda z.map(i=lambda y.sub[x->jb](ib), sub[x->jb](c2)), c0))
 *        => map(f'=lambda z.app(lambda v.map(g=lambda w.gb,c1),
 *            map(i=lambda y.sub[x->jb](ib), sub[x->jb](c2))), c0)
 *        => map(f'=lambda z.map(g'=lambda w.sub[v->mc2](gb),sub[v->mc2](c1)),c0)
 *           where mc1 = map(i=lambda y.sub[x->jb](ib), sub[x->jb](c2))
 *
 *       ++ above, if there is no flatten prior to map(h,...) in the original
 *          expression, the variable v is bound to a collection of tuples.
 *       ++ note that types are still consistent through substitution, that is
 *          v is replaced with a map yieled a collection of tuples
 *       ++ the interesting part is the outer map function f no longer takes
 *          a collection arg but a tuple arg, but at the expense of a map
 *          operation added to the function g.
 *       ++ what does the code look like after applying sub[v->mc2](gb) ? is
 *          there a normalized form we should use here (i.e. lambda's at the
 *          top, rather than at every occurrence?). Remember that the goal is
 *          to pipeline here, and remove any intermediate data structure
 *          construction, such as would occur when binding mc1 to v in a
 *          function application.
 *       ++ the substitution will achieve this since presumably gb does some
 *          form of tuple construction containing a collection, for example
 *          gb=map(lambda a.tuple(a,w),v)
 *          => map(lambda a.tuple(a,w), map(...))
 *          => map(lambda y.compose(tuple(a,w),sub[x->jb](ib)), sub[x->jb](c2))
 *          => map(lambda y.sub[a->sub[x->jb](ib)](tuple(a,w)), sub[x->jb](c2))
 *       ++ do we really want to defer subsitutions as seen above? what would
 *          we gain from this? it may be useful for backtracking on inlining
 *          decisions, but there's no point to this for now since we only
 *          apply the above in a 3-way product scenario
 *
 *     -- does the above inlining/substitution affect the collection depth of
 *        the program, and require additional flattening? DONE: yes, but in
 *        a typesafe way, and we *want* this to remove intermediates
 *       ++ see comments in the case of a 3-way join. the key part is the
 *          form of the code when we add a map operation to a previously flat
 *          function.
 *
 *     -- agg(f=aclambda v,w.fb,i,map(g=lambda x.map(h=lambda y.hb,c2),c1))
 *        => agg(f=aclambda v,w.fb,i,
 *             map(g=lambda x.(agg(f=aclambda v,w.fb,0,map(h=lambda y.hb,c2))),c1))
 *        => agg(f=aclambda v,w.fb,i,
 *             map(g=lambda x.(agg(f'=aclambda y,w.sub[v->hb](fb),0,c2)),c1))
 *        => agg(f''=aclambda x,w.sub[v->mc2](fb),i,c1)
 *           where mc2=agg(f'=aclambda y,w.sub[v->hb](fb),0,c2)
 *
 *       ++ note we explicitly need to check for a nested rhs to dup the agg,
 *          otherwise just composing the outer rhs map function yields: 
 *        => agg(f'=aclambda x.w.app(lambda v.fb,map(h=lambda y.hb,c2)),i,c1)
 *        => agg(f'=aclambda x.w.sub[v->map(h=lambda y.hb,c2)](fb),i,c1)
 *
 *          where there is only one instance of an agg, and thus no partial
 *          aggregation going on
 *
 *       ++ do we want to pipeline aggregation, or simply compute binding for
 *          v->mc2 once? since mc2 performs data reduction, its intermediates
 *          would be smaller than its inputs.
 *       ++ note there has to be a flatten enclosing the map here, otherwise
 *          we're expected to aggregate double collections, whereas the only
 *          current associative function we have is sum.
 *       ++ TODO: overall we still have to decide how we want to handle flatten,
 *          i.e. whether we want to change map and its function f to have ext
 *          semantics, or explicitly add flatten constructs where necessary
 *         -- I'm in favour of the latter.
 *       ++ TODO: in this case, this rule has to explicitly be defined in terms
 *          of flatten, and we also need to reconsider the other transforms to
 *          handle flattens   
 *
 *     -- note the above substitutions can also perform dead code elimination,
 *        where if a substituted variable is not used, the code to be subbed
 *        is removed from the program, which may be effective for nested 
 *        subbing as in the 3-way join.
 *)

open Types
open K3

type optimization_t = CSE | Beta | NoFilter

let var_counter = ref 0
let gen_var_sym() = incr var_counter; "var"^(string_of_int (!var_counter))

let mk_var (id,ty) = Var(id,ty)
let mk_block l = match l with [x] -> x | _ -> Block(l)

(* accessors *)
let get_fun_parts e = match e with
    | Lambda(x,b) -> x,b | _ -> failwith "invalid function"

let get_assoc_fun_parts e = match e with
    | AssocLambda(x,y,b) -> x,y,b
    | _ -> failwith "invalid assoc function"

let get_arg_vars a = match a with
    | AVar(v,_) -> [v]
    | ATuple(vt_l) -> List.map fst vt_l

let get_arg_vars_w_types a = match a with
    | AVar(v,t) -> [v,t]
    | ATuple(vt_l) -> vt_l

let get_fun_vars e = get_arg_vars (fst (get_fun_parts e))

let get_bindings e = match e with
    | Lambda(x,_) -> get_arg_vars x
    | AssocLambda(x,y,_) -> (get_arg_vars x)@(get_arg_vars y)
    | _ -> []

let get_if_parts e = match e with
    | IfThenElse(pe,te,ee) -> pe,te,ee
    | _ -> failwith "invalid IfThenElse"

(* TODO: for all these collection accessors, extract an rhs collection if it's
 * masked by an IfThenElse yet the underlying collection beneath both branches
 * is the same. *)
let unmask cexpr = cexpr

let get_map_parts e = match e with
    | Map(fn,c) -> fn,(unmask c)
    | _ -> failwith "invalid map expression"

let get_agg_parts e = match e with
    | Aggregate(agg_f, init, agg_c) -> agg_f, init, (unmask agg_c)
    | _ -> failwith "invalid aggregate expression"  

let get_gb_agg_parts e = match e with
    | GroupByAggregate(agg_f, init, gb_f, agg_c) ->
        agg_f, init, gb_f, (unmask agg_c)
    | _ -> failwith "invalid group by aggregation"


let get_free_vars bindings e =
    let add_binding bindings e = bindings@(get_bindings e) in
    let is_free_var bindings bu_acc e = match e with
        | Var(id,_) -> if not(List.mem id bindings) then [id] else []
        | _ -> List.flatten (List.flatten bu_acc)
    in fold_expr is_free_var add_binding bindings [] e

(* substitute *)
let substitute_args renamings arg =
    let sub_aux (v,t) =
        if List.mem_assoc v renamings
        then (snd(List.assoc v renamings),t) else (v,t)
    in
    begin match arg with
    | AVar(v,t) -> let x,y = sub_aux (v,t) in AVar(x,y)
    | ATuple(vt_l) -> ATuple(List.map sub_aux vt_l)
    end

let substitute (arg_to_sub, sub_expr) expr =
    let back l = let x = List.rev l in List.rev (List.tl x), List.hd x in 
    let vars_to_sub = match arg_to_sub with
        | AVar(v,_) -> [v,sub_expr]
        | ATuple(vt_l) ->
            let rec push_down_ifs tup_expr = 
               match tup_expr with
               | Tuple(t) -> t
               | IfThenElse(p,tt_expr,et_expr) ->
                  List.map2 (fun t e -> if t = e then t else IfThenElse(p,t,e))
                    (push_down_ifs tt_expr) (push_down_ifs et_expr)
               | Block l ->
                let h,b = back l in
                let fields = push_down_ifs b
                in (Block(h@[List.hd fields]))::(List.tl fields)
               | _ -> print_endline (string_of_expr sub_expr);
                 failwith ("invalid tuple binding: "^(string_of_expr sub_expr))
            in List.map2 (fun (v,_) e -> (v,e)) vt_l (push_down_ifs sub_expr)
    in
    (* Remove vars from substitutions when descending through lambdas
     * binding the same variable. Don't descend further if there are no
     * substitutions left. *)
    let remove_var subs e =
        let avl = match e with
        | Lambda(arg,body) -> get_arg_vars arg
        | AssocLambda(arg1,arg2,body) -> (get_arg_vars arg1)@(get_arg_vars arg2)
        | _ -> []
        in if avl = [] then subs
           else List.filter (fun (v,_) -> not(List.mem v avl)) subs
    in
    let sub_aux subs parts e = match e with
        | Var(id,t) ->
            (* sub if possible *)
            if List.mem_assoc id subs then List.assoc id subs else e
        | _ -> rebuild_expr e parts
    in fold_expr sub_aux remove_var vars_to_sub (Const (CFloat(0.0))) expr


(* Lambda and assoc lambda composition *)

(* returns a new version of g's arguments and body if they collide with any
 * nested variables used in f's body *)
let replace_nesting_collisions f_argvars f_body g_args g_body =
    let collisions =
      (* f_body can use vars that are not g's results due to nested functions.
         thus collisions are vars in f_body that are not part of f_args, and
         overlap with g_args.
         In the presence of collisions, we want a function with new g_args,
         with the colliding variable in g_args, g_body replaced by a new symbol.
         In general, we cannot use f_args because this function may have
         different arity than g_args. *)
      ListAsSet.inter
        (get_free_vars f_argvars f_body) (get_arg_vars g_args)
    in
    if collisions = [] then g_args, g_body
    else
      let gat = get_arg_vars_w_types g_args in
      let nargl, renamings = List.fold_left
        (fun (arg_acc, ren_acc) (v,t) ->
          if List.mem v collisions then
            let nv = gen_var_sym() in (arg_acc@[nv,t], ren_acc@[v,nv,t])
          else (arg_acc@[v,t],ren_acc))
        ([],[]) gat
      in
      let new_args = match g_args, nargl with
        | (AVar _, [x,y]) -> AVar(x,y)
        | (ATuple _, vt_l) -> ATuple(vt_l)
        | _ -> failwith "invalid number of args"
      in
      let new_body = List.fold_left (fun e (v,nv,t) ->
        substitute (AVar(v,t), Var(nv,t)) e) g_body renamings
      in new_args,new_body 

(* Compose f and g, i.e. f(g(x)) -> f \circ g *)
let compose f g =
    let f_args, f_body = get_fun_parts f in
    let g_args, g_body = get_fun_parts g in
    let ng_args, ng_body =
      replace_nesting_collisions (get_arg_vars f_args) f_body g_args g_body
    in Lambda(ng_args, substitute (f_args, ng_body) f_body)
    
let compose_assoc assoc_f g =
    let f_arg1, f_arg2, f_body = get_assoc_fun_parts assoc_f in
    let g_args, g_body =
      let ga, gb = get_fun_parts g
      in replace_nesting_collisions
           ((get_arg_vars f_arg1)@(get_arg_vars f_arg2)) f_body ga gb
    in
    let arg_collisions =
       let f2_vars = get_arg_vars_w_types f_arg2 in
       let g_vars = get_arg_vars_w_types g_args in
       List.filter (fun (v,t) -> List.mem_assoc v f2_vars) g_vars
    in
    let renamings =
        List.map (fun (v,t) -> (v,(t,gen_var_sym()))) arg_collisions in
    let new_args = substitute_args renamings f_arg2 in
    let new_f_body =
        let non_coll_f_body = List.fold_left (fun e (v,(t,nv)) ->
          substitute (AVar(v,t), Var(nv,t)) e) f_body renamings
        in substitute (f_arg1, g_body) non_coll_f_body
    in AssocLambda(g_args, new_args, new_f_body)

(* collection_expr:
 * -- returns if expr is a collection type *)
let collection_expr expr = 
    begin match K3Typechecker.typecheck_expr expr with
        | Collection _ | Fn(_,Collection _) -> true
        | _ -> false
    end

let contains_collection_expr expr =
  let bu_f _ parts e = match e with
    | Const _ | Var _ -> false
    | SingletonPC _ | InPC _ | OutPC _ | PC _ -> true
    | _ -> List.exists (fun x -> x) (List.flatten parts)
  in fold_expr bu_f (fun x _ -> x) None false expr

(* selective_expr:
 * -- returns if expr is an aggregate, or a map with a selective lambda (i.e. a
 *    lambda contains a predicate dependent on the function argument)
 *)
let selective_expr expr =
    let vars = match expr with Map(f,_) -> get_fun_vars f | _ -> [] in
    let aux _ part_selvars e =
        let rv = 
           let selvars = List.flatten part_selvars in
           (List.mem true (List.map fst selvars),
            List.flatten (List.map snd selvars))
        in 
        match e with
        | Var(id,_) -> (false, [id])
        | Aggregate _ -> (true, [])
        | GroupByAggregate _ -> (true, [])
        (* For Maps we only care if this map_f is selective. If the collection
         * is not, then transformation of this Map will perform optimization *)
        | Map _ -> List.hd (List.hd part_selvars) 
        | IfThenElse0 _ ->
	        let (subsel,subvars) = rv in
	        let sel = (ListAsSet.inter subvars vars) <> []
	        in if subsel || sel then (true, []) else (false, subvars)  
	      | _ -> rv
    in fst (fold_expr aux (fun x _ -> x) None (false,[]) expr)

(* TODO: better nested map detection *)
let nested_map map_f = match map_f with
    | Lambda(_,Map _) -> true
    | _ -> false

let rec inline_collection_functions substitutions expr =
  Debug.print "INLINE-COLLECTION-FUNCTIONS" (fun () ->
    "---- Current substitutions ----\n"^
    (ListExtras.string_of_list (fun ((v,t),subst) -> v^"<="^(code_of_expr subst)) 
                    substitutions)^
    "\n---- Current expression ----\n"^
    (code_of_expr expr)^"\n"
  );
  let recur = inline_collection_functions in
  let rebind subs vt arg =
    let ns = 
      if List.mem_assoc vt subs 
      then List.remove_assoc vt subs 
      else subs
    in (vt,arg)::ns
  in
  begin match expr with
  | Apply(Lambda(AVar(v, (((Collection _) | TBase(_)) as t)), body), arg) ->
    Debug.print "INLINE-COLLECTION-FUNCTIONS" (fun () -> "*** APPLY ***");
    (recur (rebind substitutions (v,t) (recur substitutions arg))
           body)
  | Var(v,t) -> 
    Debug.print "INLINE-COLLECTION-FUNCTIONS" (fun () -> "*** VAR ***");
    if List.mem_assoc (v,t) substitutions
    then List.assoc (v,t) substitutions else expr
  | _ -> descend_expr (recur substitutions) expr
  end


(* Perform beta reduction for applications of base type arguments, and for
 * complex arguments that are used at most once.
 * For now, we'll also add some simple constant folding in here *)
let rec conservative_beta_reduction substitutions expr =
  let recur = conservative_beta_reduction in
  (* recursive identity fn, i.e. recur with same set of substitutions *)
  let recid e = match e with
    | Var _ -> 
      (* Force recursive application when directly used on vars, since
       * descend_expr does not invoke the descent function on terminals *)
      recur substitutions e
    | _ -> descend_expr (recur substitutions) e
  in  
  let rebind subs vt arg =
    let ns = if List.mem_assoc vt subs then List.remove_assoc vt subs else subs
    in (vt,arg)::ns
  in
  let beta_reduce free_vars (rem_acc, subs_acc, expr_acc) ((v,t), arg) =
    let red_arg = recid arg in
    let preserve () = (rem_acc@[(v,t), red_arg]), subs_acc, expr_acc in
    let reduce () =
      let new_subs = rebind subs_acc (v,t) red_arg
      in rem_acc, new_subs, descend_expr (recur new_subs) expr_acc
    in
    if not(contains_collection_expr arg) then reduce()
    else
      let occurrences = List.filter ((=) v) free_vars in
       (* first case preserves the variable even if it is not used, since the
        * argument may have side-effects, such as persistent collection updates. *)
       if List.length occurrences = 1 then reduce() else preserve()
  in
  begin match expr with
  
      (* Adding 0 to a number leaves it unchanged *)
	  | Add(Const(CInt(0)), x)     | Add(x, Const(CInt(0)))
	  | Add(Const(CFloat(0.0)), x) | Add(x, Const(CFloat(0.0))) -> recid x

      (* Adding two numbers can be inlined *)
	  | Add(Const(x), Const(y)) -> Const(Arithmetic.sum x y)

      (* Multiplying by 0 makes the number 0 *)	
	  | Mult(Const(CInt(0) as x), _)     | Mult(_, Const(CInt(0) as x)) 
	  | Mult(Const(CFloat(0.0) as x), _) | Mult(_, Const(CFloat(0.0) as x)) -> 	                                                                 Const(x)

      (* Multiplying by 1 leaves the number unchanged *)
	  | Mult(Const(CInt(1)), x)     | Mult(x, Const(CInt(1)))
	  | Mult(Const(CFloat(1.0)), x) | Mult(x, Const(CFloat(1.0))) -> recid x
	  
	   (* Multiplying two numbers can be inlined *)
	  | Mult(Const(x), Const(y)) -> Const(Arithmetic.prod x y)
	
	   (* External function application to constants *)
     | Apply(ExternalLambda(fn_name, AVar(_,_), TBase(fn_type)), Const(x)) 
          when Arithmetic.function_is_defined fn_name ->
       Const(Arithmetic.eval (
         Arithmetic.ValueRing.mk_val (
             Arithmetic.AFn(fn_name, [Arithmetic.mk_const x], fn_type))))
	
	   (* External function application to constant tuples *)
     | Apply(ExternalLambda(fn_name, ATuple(vt_l), TBase(fn_type)), Tuple(fields)) 
          when ((List.length vt_l) = (List.length fields)) &&
               (Arithmetic.function_is_defined fn_name) &&
               (List.for_all (function Const(_)->true | _->false) fields) ->
       Const (Arithmetic.eval (
         Arithmetic.ValueRing.mk_val (
             Arithmetic.AFn(fn_name, 
               List.map (function Const(x) -> Arithmetic.mk_const x
                  (* We should have filtered this case out in the case stmt. *)
                    | _ -> failwith "BUG: I thought I had only constants"
               ) fields,
               fn_type
            )
         )
      ))

	  | Apply(Lambda(AVar(v,t), body) as lambda_e, arg) ->
	    let free_vars = get_free_vars [] body in
	    let remaining, _, new_e =
	      beta_reduce free_vars ([], substitutions, lambda_e) ((v,t), arg) in
	    if remaining <> [] then recid expr else snd (get_fun_parts new_e)
	
	  | Apply(Lambda(ATuple(vt_l), body) as lambda_e, Tuple(fields)) ->
	    let free_vars = get_free_vars [] body in
	    let reml, _, new_e = List.fold_left (beta_reduce free_vars)
	        ([], substitutions, lambda_e) (List.combine vt_l fields) in
	    let rem_vt_l, rem_fields = List.split reml in
	    if rem_fields = fields then recid expr
	    else 
			begin match rem_fields with
				| [] -> snd (get_fun_parts new_e)
				| [rem_field] ->
					let (rem_v,rem_t) = List.hd rem_vt_l in
					Apply(Lambda(AVar(rem_v,rem_t), snd (get_fun_parts new_e)), rem_field)
				| _ -> Apply(Lambda(ATuple(rem_vt_l), snd (get_fun_parts new_e)), Tuple(rem_fields))
	 		end
	  (* TODO: handle Apply(AssocLambda(...)) *)
	
	  | Var(v,t) -> if List.mem_assoc (v,t) substitutions
	                then List.assoc (v,t) substitutions else expr
	  | _ -> recid expr
	  end	
  
(* Performs dependency tests at lambda-if boundaries *)
(* Avoids spinning on reordering at if-chains
 * -- if-reordering-spinning can be an arbitrarily deep problem, thus
 *    no pattern can match it (i.e. if we prevent an n-level reorder spin,
 *    we can still spin on a n+1-level if-chain)
 * -- the correct way to detect this is based on dependency sets of
 *    if-stmts, and only lift nested ifs when they have a strictly simpler
 *    dependency set.
 * -- a dependency set is the intersection of the bound variables, and
 *    variables used in the if's predicate. Thus we need to pass along
 *    bound variables, i.e. trigger vars and variables bound by lambdas.
 *)
let rec lift_ifs bindings expr =
   Debug.print "LOG-K3-OPT-LIFTIF" (fun () -> K3.string_of_expr expr);
    let lift_branches e =
        let rec expand_children acc_branches rem_branches
                                (branch : expr_t list) acc_branch : expr_t =
            let recex = expand_children acc_branches rem_branches in
            begin match branch with
            | [] -> expand_branch (acc_branches@[acc_branch]) rem_branches
            | h::t ->
                begin match h with
                | IfThenElse(pe,te,ee) ->  
	                let pe,te,ee = get_if_parts h in
	                let tev = recex t (acc_branch@[te]) in
	                let eev = recex t (acc_branch@[ee])
	                in IfThenElse(pe,tev,eev)
                | _ -> recex t (acc_branch@[h])
                end
            end
        and expand_branch (acc_branches : expr_t list list)
                          (rem_branches : expr_t list list) : expr_t =
            match rem_branches with
            | [] -> rebuild_expr e acc_branches
            | h::t -> expand_children acc_branches t h []
        in expand_branch [] (get_branches e) 
    in
    (* returns existing bindings (i.e. those passed to lift-ifs) used by e *)
    let get_dependencies e = 
        let vars = get_free_vars [] e in ListAsSet.inter vars bindings
    in
    (* returns if d1 is a strict subset of d2 *)  
    let simpler_dependencies pe pe2 =
        let d1 = get_dependencies pe in
        let d2 = get_dependencies pe2 in
        (ListAsSet.subset d1 d2) && not(ListAsSet.subset d2 d1)
    in
    (* if expression is dependent on arg, returns new bindings, otherwise nothing *)
    let arg_uses arg_l e =
        let vars = get_free_vars [] e in
        let bindings = List.flatten (List.map get_arg_vars arg_l) in
        if ListAsSet.inter bindings vars = []
        then ListAsSet.no_duplicates bindings else [] 
    in
    let simplify e =
        begin match e with
        | Lambda(arg_e, IfThenElse(pe,te,ee)) ->
            (* if pe is not dependent on arg_e, lift, otherwise nop *)
            if (arg_uses [arg_e] pe) = [] then e
            else IfThenElse(pe, Lambda(arg_e, te), Lambda(arg_e, ee))

        | AssocLambda(arg1_e,arg2_e,IfThenElse(pe,te,ee)) ->
            if (arg_uses [arg1_e; arg2_e] pe) = [] then e
            else IfThenElse(pe, AssocLambda(arg1_e,arg2_e,te),
                                AssocLambda(arg1_e,arg2_e,te))

        (* For if-chains, we just want the next level to have the same
         * predicate. *)
        | IfThenElse(pe,te,ee) ->
            begin match (te,ee) with
            | IfThenElse(pe2,te2,ee2), IfThenElse(pe3,te3,ee3) when pe2=pe3 ->
                if pe = pe2 then IfThenElse(pe,te2,ee3)
                else if simpler_dependencies pe2 pe then
                    let new_sub_e2 = IfThenElse(pe, te2, te3) in
                    let new_sub_e3 = IfThenElse(pe, ee2, ee3) 
                    in IfThenElse(pe2, new_sub_e2, new_sub_e3)
                else descend_expr (lift_ifs bindings) e
            | _ -> descend_expr (lift_ifs bindings) e
            end
        | _ ->
            let new_bindings = get_bindings e in
            let r = lift_branches e in
            (* if children have no ifs, descend and try w/ grandchildren *)
            if r <> e then r
            else descend_expr (lift_ifs (bindings@new_bindings)) e 
        end
    in
    let fixpoint old_expr new_expr =
      if old_expr = new_expr then new_expr else lift_ifs bindings new_expr
    in fixpoint expr (simplify expr)

(* predicates: list of upstream predicates and whether this expr is a
 * descendant of the "then" branch for each upstream predicate *)
let rec simplify_if_chains predicates expr =
    let rcr new_branch = simplify_if_chains (predicates@new_branch) in
    match expr with
    | IfThenElse(pe,te,ee) ->
        if List.mem_assoc pe predicates then
            rcr [] (if List.assoc pe predicates then te else ee)
        else IfThenElse(pe, rcr [pe,true] te, rcr [pe,false] ee)
    | _ -> descend_expr (simplify_if_chains []) expr 

(* TODO: pairwith2, deep flattens *)
let rec simplify_collections filter expr =
  let recur = simplify_collections filter in
  let simplify expr =
    let ne = descend_expr recur expr in
    begin match ne with
    | Map(map_f, Singleton(e)) -> Singleton(Apply(map_f, e))
    | Flatten(Singleton(c)) -> c
    | Map(map_f, Flatten c) ->
        let mapf_arg_t = match fst (get_fun_parts map_f) with
            | AVar(_,t) -> t
            | ATuple(vt_l) -> TTuple(List.map snd vt_l)
        in
        let v,v_t = gen_var_sym(), Collection(mapf_arg_t)
        in Flatten(Map(Lambda(AVar(v,v_t), Map(map_f, Var(v,v_t))), c))

    | Map(map_f, (Map(_,_) as rmap)) ->
        if filter && (collection_expr map_f || selective_expr rmap) then ne
        else
          let rmap_f, rmap_c = get_map_parts rmap in
          let new_map_f = compose map_f rmap_f
          in Map(new_map_f, rmap_c)

    | Aggregate((AssocLambda _ as agg_f),init, (Map _ as rmap)) ->
        (* v,w,fb *)
        let aggf_arg1, aggf_arg2, aggf_body = get_assoc_fun_parts agg_f in

        (* g,c1 *)
        let rmap_f, rmap_c = get_map_parts rmap in
        if nested_map rmap_f then
            
            (* create duplicate aggregate for nested colln *)
            (* TODO: handle decomposition errors, or move retrieval into 
             * nested_map function. *)
            (* x, map(h,c2) *)
            let rmapf_args, rmapf_body = get_fun_parts rmap_f in
            
            (* h,c2 *)
            let nmap_f, nmap_body = get_map_parts rmapf_body in
            
            (* y, hb *)
            let nmapf_args, nmapf_body = get_fun_parts nmap_f in
            
            let dup_agg_f =
                let dup_body = substitute (aggf_arg1, nmapf_body) aggf_body in
                AssocLambda(nmapf_args,aggf_arg2,dup_body) in
            let dup_init = Const(CFloat(0.0)) in

            (* mc2 *)
            let new_rmapf_body =
                recur (Aggregate(dup_agg_f, dup_init, nmap_body))
            in
            let new_rmapf = Lambda(rmapf_args, new_rmapf_body) in
            let new_agg_f = compose_assoc agg_f new_rmapf
            in Aggregate(new_agg_f, init, rmap_c)
        else
            let new_agg_f = compose_assoc agg_f rmap_f
            in Aggregate(new_agg_f, init, rmap_c)

    | GroupByAggregate((AssocLambda _ as agg_f),i,gb_f, (Map _ as rmap)) ->
        (* TODO: group by function optimizations *)
        (* For now, simply compose rhs map fn with group-by fn. Presumably
         * there are some nested optimizations of such group-bys we can perform
         * just as with aggregates above. *)
        let rmap_f, rmap_c = get_map_parts rmap in
        let new_agg_f = compose_assoc agg_f rmap_f in
        let new_gb_f = compose gb_f rmap_f
        in GroupByAggregate(new_agg_f,i,new_gb_f,rmap_c) 

    | Iterate(iter_f, (Map _ as rmap)) ->
        let rmap_f, rmap_c = get_map_parts rmap in
        let new_iter_f = compose iter_f rmap_f
        in Iterate(new_iter_f, rmap_c)
        
    | _ -> ne

    end
    in 
    (* Fixpoint *)
    let fixpoint expr new_expr =
        if new_expr = expr then expr else recur new_expr
    in fixpoint expr (simplify expr)



(* Common subexpression elimination, based on alpha equivalence of expressions. 
 * This method folds over a K3 expression, building up candidates to test for
 * equivalence in a bottom-up fashion. Currently, only collection operations
 * are considered as candidates (that is maps,aggregates,group-bys,flattens).
 * This CSE is conservative, candidates are materialized at:
 * i) lambdas, as a simple way to ensure variables do not escape their
 *    definition. This could be refined by allowing candidates to propagate up
 *    provided they do not use the variable bound by the lambda.
 * ii) if-then-elses, where only candidates common to both then- and else-branches
 *     are carried up. Candidates that appear in any combination of the predicate
 *     and one of the branches are materialized then and there.
 * 
 * Specifically, the fold method accumulates a set of equivalence classes,
 * represented as a nested list of expressions (each inner list represents
 * alpha equivalent expressions). This allows for easy substitution once we
 * decide to materialize candidates. Each equivalence class is kept as a list
 * rather than a set, to keep track of its usage count. Candidates that are
 * used at most once are not materialized.  
 *)

let common_subexpression_elim expr =
  let symref = ref 0 in
  let gensym () = incr symref; "__cse"^(string_of_int !symref) in
  let unique l = List.fold_left (fun acc e ->
    if List.mem e acc then acc else acc@[e]) [] l
  in
  let filter_contained eqvl =
    List.fold_left (fun acc cl ->
      (* Remove any equiv. classes in the accumulator contained by cl *)
      let nacc = List.filter (fun cl2 -> List.for_all
        (fun x -> not (List.exists (fun y -> contains_expr y x) cl)) cl2) acc
      in
      (* Only add cl if it is not contained within any equiv. classes in
       * the accumulator *)
      let prune = List.exists (List.exists
        (fun x -> List.exists (fun y -> contains_expr x y) cl)) nacc
      in if prune then nacc else nacc@[cl])
      [] eqvl
  in
  let rec substitute_expr substitutions expr =
    if List.mem_assoc expr substitutions then List.assoc expr substitutions
    else descend_expr (substitute_expr substitutions) expr
  in
  let sub_cse expr cse_var cl =
    List.fold_left (fun acc cse -> substitute_expr [cse, cse_var] acc) expr cl
  in
  let mk_cse expr_id expr eqv = List.fold_left (fun acc_expr cl ->
      let cse_id = gensym() in
      let arg = List.hd cl in
      let cse_ty =
        try K3Typechecker.typecheck_expr arg
        with Failure msg ->
          (print_endline ("invalid cse: "^msg);
           print_endline ("cse["^expr_id^"]: "^(string_of_expr arg));
           failwith ("invalid cse: "^msg))
      in
      let cse_var = Var(cse_id, cse_ty) in
        Apply(Lambda(AVar(cse_id, cse_ty),
              sub_cse acc_expr cse_var (unique cl)), arg))
    expr (List.rev (filter_contained eqv))
  in
  
  (* Alpha renaming based equivalence, and equivalence class helpers *)
  let alpha_rename expr =
    let sym = ref 0 in
    let renamed_var () = incr sym; "__alpha"^(string_of_int !sym) in
    let add_subs subs src dest =
      (if List.mem_assoc src subs then List.remove_assoc src subs else subs)@
      [src, dest] in
    let sub_of_id subs id =
      if List.mem_assoc id subs then List.assoc id subs else id in
    let subs_of_args subs a_l = List.fold_left (fun acc v ->
        add_subs acc v (renamed_var())) subs
      (List.flatten (List.map get_arg_vars a_l)) in
    let rebuild_arg subs arg = 
      let nv = List.map (sub_of_id subs) (get_arg_vars arg) in
      match arg,nv with
        | AVar(_,ty),[x] -> AVar(x,ty)
        | ATuple(vt_l), nvl -> ATuple(List.map2 (fun (_,ty) id -> id,ty) vt_l nvl)
        | _,_ -> failwith "invalid arg to rebuild"
    in
    let modify_subs subs expr = match expr with
      | Lambda(arg,_) -> subs_of_args subs [arg]
      | AssocLambda(arg1, arg2, _) -> subs_of_args subs [arg1; arg2]
      | _ -> subs
    in
    let apply_subs subs parts expr = match expr with
      | Var(id,ty) -> Var(sub_of_id subs id, ty)
      | Lambda(arg,_) -> Lambda(rebuild_arg subs arg, List.hd (List.hd parts))
      | AssocLambda(arg1,arg2,_) ->
        AssocLambda(rebuild_arg subs arg1,
          rebuild_arg subs arg2, List.hd (List.hd parts))
      | _ -> rebuild_expr expr parts
    in
    fold_expr apply_subs modify_subs [] (Const(CFloat(0.0))) expr
  in

  (* Test if two equivalence classes are the same by alpha equivalence of any
   * two members of the classes *)
  let alpha_eq eq1 eq2 =
    (alpha_rename (List.hd eq1)) = (alpha_rename (List.hd eq2))
  in
  
  (* Union, difference and intersection of two lists of equivalence classes *)  
  let union_eqv cl1 cl2 = List.fold_left (fun acc c ->
    if acc = [] then [c] else
    let eq,rest = List.partition (alpha_eq c) acc in
      match eq with
        | [] -> rest@[c]
        | [x] -> rest@[c@x]
        | _ -> failwith "multiple equivalences found")
    cl1 cl2  
  in
  let diff_eqv cl1 cl2 = List.fold_left (fun acc cl -> 
    if List.exists (alpha_eq cl) cl2 then acc else acc@[cl]) [] cl1
  in
  let inter_eqv cl1 cl2 = List.fold_left (fun acc cl ->
      let eqs = List.filter (alpha_eq cl) cl2 in
      if eqs = [] then acc else acc@[cl@(List.flatten eqs)])
    [] cl1
  in

  (* Misc helpers *)
  let union_eqvl acc l = List.fold_left union_eqv acc l in 
  let union_eqv_parts eqvll = List.fold_left union_eqvl [] eqvll in
  
  let name_of_map expr = match expr with
    | SingletonPC(id,_) | InPC(id,_,_) | OutPC(id,_,_) | PC(id,_,_,_) -> id
    | _ -> failwith "invalid map"
  in 
  let filter_maps expr =
    let filter_f pre acc expr =
      let r = List.flatten (List.flatten acc) in
      r@(match expr with
         | SingletonPC _ | InPC _ | OutPC _ | PC _ -> [expr]
         | _ -> [])
    in fold_expr filter_f (fun x _ -> x) None [] expr
  in

  let fold_f pre acc expr : expr_t list list * expr_t =
	 let parts = List.map (fun x -> List.map snd x) acc in
    let sub_parts = List.map (fun x -> List.map fst x) acc in
    let rebuilt_expr = rebuild_expr expr parts in
	
	 let lambda_common body f = 
      let r_expr =
        let body = List.hd (List.hd parts) in
        let cses = List.filter
          (fun cl -> List.length cl > 1) (List.hd (List.hd sub_parts))
        in f (mk_cse "lambda" body cses)
      in [], r_expr
    in

    let aggregate_common sub_part_idx =
      let agg_candidate = [[[[rebuilt_expr]]]] in
      let new_candidates = union_eqv_parts
        (agg_candidate@(List.map (List.nth sub_parts) sub_part_idx))
      in new_candidates, rebuilt_expr
    in

    let new_candidates, r_expr =
      match rebuilt_expr with
        | Const _ | Var _ 
        | SingletonPC _ | InPC _ | OutPC _ | PC _ ->
          [], rebuilt_expr (* no work to do here *)

        (* carry up candidates common to both branches only *)
        | IfThenElse (p,t,e) -> 
           
          (* Get maps tested in the predicate *)
          let map_names = match p with
            | Member(x,_) -> List.map name_of_map (filter_maps x)
            | _ -> []
          in

          (* Get then, else branch equivalences, and carry up intersection *)
          (* Note: flattening is fine for IfThenElse since this has singleton
           * lists per branches, guaranteeing unique candidates per branch.
           * In general this does not apply, e.g. for updates with key expr
           * lists considered a branch *)
          let branch_candidates = List.map
            (fun i -> List.flatten (List.nth sub_parts i)) [0; 1; 2] in
          let p_cand_exprs, t_cand_exprs, e_cand_exprs =
            List.nth branch_candidates 0,
            List.nth branch_candidates 1, List.nth branch_candidates 2 in

          (* Find intersection of CSEs in then and else branches, excluding
           * expressions containing any maps tested in the predicate. *)
          let common = List.filter (fun eqv ->
              let names = List.map name_of_map (filter_maps (List.hd eqv))
              in (ListAsSet.inter map_names names) = [])
            (inter_eqv t_cand_exprs e_cand_exprs) in
          let rest = diff_eqv (union_eqvl [] branch_candidates) common in

          (* Make CSEs for subexprs that are used more than once, but not passed up. *)
          let cses_here = List.filter (fun cl -> List.length cl > 1) rest in
          let p_cses, branch_cses =
            List.partition (fun cl -> List.exists (fun cl2 ->
              ListAsSet.inter cl cl2 <> []) p_cand_exprs) cses_here in
          let t_cses, e_cses =
            List.partition (fun cl -> List.exists (fun cl2 ->
              ListAsSet.inter cl cl2 <> []) t_cand_exprs) branch_cses
          in begin match rebuilt_expr with
            | IfThenElse(rp, rt, re) ->
              (* For the predicate, build funapps around the condition, to
               * enable sharing of predicate CSEs with branches *) 
              let r_expr =
                let cse_id_tys, np = List.fold_left
                  (fun (sub_acc, acc_expr) cl ->
                    let cse_id = gensym() in
                    let arg = List.hd cl in
                    let cse_t = K3Typechecker.typecheck_expr arg in
                    let cse_var = Var(cse_id,cse_t) in
                      List.fold_left (fun (sub_acc, acc_expr) cse ->
                          (sub_acc@[cse,cse_id,cse_t]),
                          substitute_expr [cse, cse_var] acc_expr)
                        (sub_acc, acc_expr) (unique cl))
                  ([],rp) p_cses
                in
                let nt = mk_cse "ite-then" rt t_cses in
                let ne = mk_cse "ite-else" re e_cses
                in List.fold_left (fun acc_expr (cse,id,ty) -> 
                    Apply(Lambda(AVar(id,ty), acc_expr), cse))
                  (IfThenElse(np, nt, ne)) cse_id_tys
              in common, r_expr
            | _ -> failwith "invaild rebuild of if-then-else expr"
          end

        (* keep common subexpressions inside functions, and do not carry
         * upwards. This could be generalized to handle commonalities that do
         * not depend on the vars bound by the lambda, and thus remain valid
         * outside the lambda. *)
        | Lambda (arg, _) ->
          lambda_common (List.hd (List.hd parts)) (fun x -> Lambda(arg, x))

        | AssocLambda (arg1, arg2, _) ->
          lambda_common (List.hd (List.hd parts)) (fun x -> AssocLambda(arg1, arg2, x))

        (* there will be no CSEs inside lambdas, leaving only the argument cses
         * to carry up. *)
        | Apply _ ->
          let arg_candidates = List.hd (List.nth sub_parts 1)
          in arg_candidates, rebuilt_expr

        | Iterate _ ->
          let coll_candidates = List.hd (List.nth sub_parts 1)
          in coll_candidates, rebuilt_expr 
        
        (* Keep common subexpressions inside functions, carry only collection
         * arg CSEs upwards. Add this expression as a potential CSE, building
         * up substitutions in larger -> smaller order. This tests matches of
         * larger exprs before smaller ones. *)
        | Map _ ->
          let coll_candidates = List.hd (List.nth sub_parts 1)
          in ([rebuilt_expr]::coll_candidates), rebuilt_expr

        | Aggregate _ -> aggregate_common [1;2]
        | GroupByAggregate _ -> aggregate_common [1;3]

        (* Add the flatten expression as a potential CSE, carrying up existing
         * candidates. *)
        | Flatten _ ->
          ([rebuilt_expr]::(List.hd (List.hd sub_parts))), rebuilt_expr

        (* Carry up the block, which contains inlined collection updates, to
         * preserve evaluation order w.r.t map operations, as well as the
         * map operations themselves. *) 
        | Block _ ->
          let candidate = mk_cse "block" rebuilt_expr
            (List.flatten (List.flatten sub_parts)) 
          in (union_eqv_parts ([[[[candidate]]]]@sub_parts)), candidate

        (* TODO/HACK: don't treat nested map lookups as CSEs for efficiency for now,
         * since nested map CSEs result in copying. This can be fixed by using
         * pointers in map entries for nested maps. *)
        | Member(PC _, _) | Lookup(PC _, _) ->
          (union_eqv_parts sub_parts), rebuilt_expr

        | Member _ | Lookup _ ->
          (union_eqv_parts ([[[[rebuilt_expr]]]]@sub_parts)), rebuilt_expr

        (* Everything else carries up CSE candidates. *)
        | _ -> (union_eqv_parts sub_parts), rebuilt_expr
    in
    new_candidates, r_expr
  in
  let global_cses, r_expr =
    fold_expr fold_f (fun x _ -> x) None ([], Const(CFloat(0.0))) expr
  in mk_cse "global" r_expr global_cses

(* Pruning optimizations *)

(* Lift-updates: moves updates up and sideways to their "defining" collection *)
let partition_updates l = List.partition
  (fun ce -> match ce with | PCValueUpdate _ -> true | _ -> false) l

let separate_updates e =
  let s,t = List.split (List.map (fun br ->
    let x,y = List.split (List.map (fun e -> match e with
      | Block(l) ->
        let updates, rest = partition_updates l
        in begin match rest with
           | [] -> failwith "invalid functional block"
           | [x] -> updates, x
           | y -> updates, Block(y)
           end
      | _ -> [], e) br)
    in List.flatten x, y) (get_branches e))
  in List.flatten s, (rebuild_expr e t)

let lift_updates bindings expr =
  let mk_arg_value a = match a with
    | AVar(i,t) -> mk_var (i,t)
    | ATuple vt_l -> Tuple(List.map mk_var vt_l)
  in
  let get_map_ids expr =
    let fold_f _ parts e = match e with
      | SingletonPC (id,t)               -> [id]
      | OutPC       (id,outs,t)          -> [id]
      | InPC        (id,ins,t)           -> [id]
      | PC          (id,ins,outs,t)      -> [id]
      | _ -> List.flatten (List.flatten parts)
    in fold_expr fold_f (fun x _ -> x) None [] expr
  in
  let lambda_common args lambda_e body_e =
    let arg_vars = List.flatten (List.map get_arg_vars args) in
    let updates, re = separate_updates lambda_e in
    let to_lift, rest = List.partition (fun ue -> 
      let u_vars = get_free_vars [] ue
      in ListAsSet.inter arg_vars u_vars = []) updates
    in match to_lift with
       | [] -> lambda_e
       | _ ->
        let new_lambda_e = 
          if rest = [] then re else
          let nbe = match re with
            | Lambda(_,b) -> b | AssocLambda(_,_,b) -> b
            | _ -> failwith "invalid lambda"
          in rebuild_expr lambda_e [[Block(rest@[nbe])]]
        in Block(to_lift@[new_lambda_e])
  in
  let flatten_common arg1 p else_updates fe =
    let pass = mk_arg_value arg1 in
    Map(Lambda(arg1,
      IfThenElse(p,pass,mk_block (else_updates@[pass]))), Flatten(fe))
  in
  let dependent_depth binding_depths e =
    let e_vars = get_free_vars [] e in
    let r = List.fold_left (fun d v ->
      if not (List.mem_assoc v binding_depths) then
        (print_endline ("dependent depth failure: "^(string_of_expr expr));
        failwith ("invalid dependent depth for "^v))
      else max d (List.assoc v binding_depths)) 0 e_vars
    in e, r
  in
  let rec lift_updates_aux binding_depths expr =
    let rcr b = descend_expr (lift_updates_aux b) in
    let add_bindings l arg_l =
      let current_max = List.fold_left max 0 (List.map snd l) in
      l@(List.fold_left (fun acc arg -> acc@
          (List.map (fun id -> (id,current_max+1)) (get_arg_vars arg))) [] arg_l)
    in
    let new_binding_depths = match expr with
      | Lambda(arg,_) -> add_bindings binding_depths [arg]
      | AssocLambda(arg1,arg2,_) -> add_bindings binding_depths [arg1; arg2]
      | _ -> binding_depths
    in
    let ne = rcr new_binding_depths expr in
    begin match ne with
    | Add _ | Mult _ | Eq _ | Neq _ | Lt _ | Leq _ | IfThenElse0 _
    | Tuple _->
      let updates, re = separate_updates ne in
      if updates = [] then re else Block(updates@[re])
      
    | Lambda(arg, be) -> lambda_common [arg] ne be
    | AssocLambda(arg1,arg2,be) -> lambda_common [arg1; arg2] ne be
    | Apply _ -> 
      let updates, re = separate_updates ne in
      if updates = [] then re else Block(updates@[re])

    | IfThenElse(p, Block(tb), Block(eb)) ->
      let tb_updates, tb_rest = partition_updates tb in
      let eb_updates, eb_rest = partition_updates eb in
      let common_updates = ListAsSet.inter tb_updates eb_updates in
      let ind_updates =
        let p_map_ids = get_map_ids p in
        List.filter (fun u ->
          ListAsSet.inter p_map_ids (get_map_ids u) = []) common_updates
      in
      let tbu_rest, ebu_rest = 
        let x = List.map2 ListAsSet.diff
                  [tb_updates; eb_updates] [ind_updates; ind_updates]
        in List.hd x, List.nth x 1 
      in
      if ind_updates = [] then ne else 
        Block(ind_updates@[
          IfThenElse(p, mk_block (tbu_rest@tb_rest),
                        mk_block (ebu_rest@eb_rest))])

    | IfThenElse(p, IfThenElse(p2,t2,e2), Block(l)) ->
      let l_updates, l_rest = partition_updates l in
      begin match l_rest with
        | [IfThenElse(p3,t3,e3)] when p2 = p3 ->
          begin match e2,e3 with
            | Block(l2), Block(l3) ->
              let e2_updates, e2_rest = partition_updates l2 in
              let e3_updates, e3_rest = partition_updates l3 in
              let common_updates = ListAsSet.inter e2_updates e3_updates in
              let lift_updates =
                let threshold_l_rank =
                  List.fold_left (fun acc (_,r) -> min acc r) max_int
                    (List.map (dependent_depth binding_depths) l_updates)
                in List.map fst 
                     (List.filter (fun (u,rnk) -> rnk < threshold_l_rank)
                       (List.map (dependent_depth binding_depths) common_updates))
              in
              if lift_updates = [] then ne else 
              let new_then = IfThenElse(p, t2, mk_block (l_updates@[t3])) in
              let new_else =
                let nested_then =
                  ((ListAsSet.diff e2_updates lift_updates)@e2_rest) in
                let nested_else =
                  ((ListAsSet.diff e3_updates lift_updates)@l_updates@e3_rest) 
                in lift_updates@[
                  IfThenElse(p, mk_block nested_then, mk_block nested_else)]
              in IfThenElse(p2, new_then, mk_block new_else)
            | _,_ -> ne
          end
        | _ -> ne
      end

    | Map(Block(l),c) ->
      let l_updates, l_rest = partition_updates l in
      mk_block (l_updates@[Map(mk_block l_rest, c)])

    | Aggregate(Block(l),i,c) ->
      let l_updates, l_rest = partition_updates l in
      mk_block (l_updates@[Aggregate(mk_block l_rest, i, c)])

    | GroupByAggregate(Block(l),i,g,c) ->
      let l_updates, l_rest = partition_updates l in
      mk_block (l_updates@[GroupByAggregate(mk_block l_rest, i, g, c)])

    | Map(Lambda(arg1, IfThenElse(p,t,Block(eb))), Map(fe,ce)) ->
      let eb_updates, eb_rest = partition_updates eb in
      let pass = mk_arg_value arg1 in
      let new_fe = compose
        (Lambda(arg1,IfThenElse(p,pass,mk_block (eb_updates@[pass])))) fe
      in Map(Lambda(arg1, IfThenElse(p, t, mk_block eb_rest)), Map (new_fe, ce))

    | Aggregate(AssocLambda(arg1,arg2,IfThenElse(p,t,Block(eb))),
        init_e, Flatten fe) ->
      let eb_updates, eb_rest = partition_updates eb in
      Aggregate(AssocLambda(arg1,arg2,IfThenElse(p,t,mk_block eb_rest)),
        init_e, flatten_common arg1 p eb_updates fe)

    | GroupByAggregate(AssocLambda(arg1,arg2,IfThenElse(p,t,Block(eb))),
        init_e, group_e, Flatten fe) ->
      let eb_updates, eb_rest = partition_updates eb in
      GroupByAggregate(AssocLambda(arg1,arg2,IfThenElse(p,t,mk_block eb_rest)),
        init_e, group_e, flatten_common arg1 p eb_updates fe)

    | e -> e
    end
  in lift_updates_aux (List.map (fun id -> id,0) bindings) expr

(* Lift-if0s *)
let rec lift_if0s expr =
  let rcr = descend_expr lift_if0s in
  let ne = rcr expr in
  begin match ne with
    | Mult(IfThenElse0(p1,t1), IfThenElse0(p2,t2)) ->
      rcr (IfThenElse0(Mult(p1, p2), Mult(t1, t2)))

    | Mult(IfThenElse0(p,t), o) -> rcr (IfThenElse0(p, Mult(t, o)))
    | Mult(o, IfThenElse0(p,t)) -> rcr (IfThenElse0(p, Mult(o, t))) 
    
    | IfThenElse0(p1,IfThenElse0(p2,t)) -> rcr (IfThenElse0(Mult(p1, p2), t))

    | IfThenElse(p1,IfThenElse0(p2,t2),IfThenElse0(p3,t3)) when p2 = p3 ->
      IfThenElse(Mult(p1,p2), t2, t3)

    | e -> e
  end

let optimize ?(optimizations=[]) trigger_vars expr =
  let apply_opts e =
      Debug.print "LOG-K3-OPT-DETAIL" (fun () -> "LIFT IFS");
    let e1 = (lift_ifs trigger_vars e) in
      Debug.print "LOG-K3-OPT-DETAIL" (fun () -> "SIMPLIFY COLLECTIONS");
    let e2 = (simplify_collections (not(List.mem NoFilter optimizations)) e1) in
      Debug.print "LOG-K3-OPT-DETAIL" (fun () -> "SIMPLIFY IF CHAINS");
    let e3 = (simplify_if_chains [] e2) in
      Debug.print "LOG-K3-OPT-DETAIL" (fun () -> "LIFT UPDATES");
    let e4 = (lift_updates trigger_vars e3) in
      e4
  in
  let rec fixpoint e =
    let new_e = apply_opts e in
    if e = new_e then e else fixpoint new_e    
  in 
  Debug.print "LOG-K3-OPT" (fun () -> "INLINE COLLECTION FNS");
  let r = fixpoint (inline_collection_functions [] expr) 
  in
  Debug.print "LOG-K3-OPT" (fun () -> "BETA OTIMIZATIONS");
  let r1 = if List.mem Beta optimizations
             then conservative_beta_reduction [] r else r 
  in
  Debug.print "LOG-K3-OPT" (fun () -> "CSE OPTIMIZATIONS");
     if not(List.mem CSE optimizations)  then r1
     else (lift_if0s
            (fixpoint (lift_ifs trigger_vars (common_subexpression_elim r1))))



(**** Datastructure optimization ****)

type ds_trig_decl = Schema.event_t * id_t * id_t list 
type ds_map_decls = map_t list * Patterns.pattern_map 

type ds_statement = ds_map_decls * K3.expr_t list
type ds_trigger = ds_trig_decl * ds_statement list
type ds_program = ds_map_decls * ds_trigger list

(* Returns a list of var, expression pairs of the partial key for the
 * collection with the given schema, and the remainder expression.
 * Assumes env and schema are disjoint variable lists.
 *)
let extract_predicates env schema e =
  let get_var_id_ty e = match e with
    | Var(id,ty) -> (id,ty) | _ -> failwith "invalid var in extract_predicates"
  in
  let is_bound var_list v = match v with
    | Var(id,ty) -> List.mem_assoc id schema 
    | _ -> false
  in
  let is_independent sch e =
    (ListAsSet.inter
      (get_free_vars (List.map fst env) e) (List.map fst sch)) = []
  in
  let bu_f c_sch parts e = match e with
    | Const _ | Var _ -> ([], e)
    | _ ->
      let re = rebuild_expr e (List.map (List.map snd) parts) in
      let bp_vars = List.map (fun l -> List.flatten (List.map fst l)) parts
      in begin match re with
        | Eq(x, y) ->
          begin match x,y with
            | a,b when is_bound c_sch a && is_independent c_sch b ->
              (List.flatten bp_vars)@[get_var_id_ty a, b], Const(CFloat 1.0)
            
            | b,a when is_bound c_sch a && is_independent c_sch b ->
              (List.flatten bp_vars)@[get_var_id_ty a, b], Const(CFloat 1.0)
            
            | _ -> (List.flatten bp_vars, re)
          end
        
        (* For disjunctions and conditions, only propagate up those pairs that
         * are common to both branches *)
        | Add _ -> 
          (ListAsSet.inter (List.hd bp_vars) (List.nth bp_vars 1)), re
        
        | IfThenElse _ ->
          (ListAsSet.inter (List.nth bp_vars 1) (List.nth bp_vars 2)), re
        
        | _ -> (List.flatten bp_vars, re)
      end
  in
  (* Top-down filtering of schema on rebinding *)
  let td_f sch e =
    let f = get_arg_vars_w_types in
    match e with
    | Lambda(arg, _) -> ListAsSet.diff sch (f arg)
    | AssocLambda(arg1, arg2, _) ->
      ListAsSet.diff (ListAsSet.diff sch (f arg1)) (f arg2)
    | _ -> sch
  in
  let unique l = List.fold_left
    (fun acc e -> if not(List.mem e acc) then acc@[e] else acc) [] l
  in 
  (* Bottom-up accumulator is a pair, of a list of build/probe vars, and
   * a rebuilt expression *)
  let bp, re = fold_expr bu_f td_f schema ([], Const(CFloat 1.0)) e in
  
  (* Only extract those predicates that have an unambiguous binder, i.e. 
   * appears in an equality with the same RHS in all instances *)
  let bp_groups = List.fold_left (fun acc (id, e) -> 
    if List.mem_assoc id acc then
      (List.remove_assoc id acc)@[id, (List.assoc id acc)@[e]]
    else acc@[id,[e]]) [] (unique bp)
  in
  let unambiguous_bp =
    List.map (fun (id, e_l) -> id, List.hd e_l)
      (List.filter (fun (_, e_l) -> List.length e_l = 1) bp_groups)
  in unambiguous_bp, conservative_beta_reduction [] re


let ds_sym = ref 0 
let gen_ds_sym () = incr ds_sym; "__ds"^(string_of_int !ds_sym)

let back l = let x = List.rev l in List.rev (List.tl x), List.hd x

let index e l =
  snd (List.fold_left (fun (found, pos) e2 ->
    if not found then (e=e2, if e=e2 then pos else pos+1)
    else (found,pos))
  (false, 0) l)

(* let m3_type_of_k3_type t = match t with
  | TInt -> VT_Int
  | TFloat -> VT_Float
  | _ -> failwith ("invalid m3 type from "^(string_of_type t)) *)

(* Returns a datastructure expression, declaration (map type and patterns),
 * and constructor expression *)
let create_datastructure arg partial_key e =
  let schema = get_arg_vars_w_types arg in
  if List.length schema = 1 then
    (print_endline ("singleton schema "^(String.concat "," (List.map fst schema)));
     failwith ("invalid tuple collection "^(string_of_expr e)))
  else
    let k_sch, _ = back schema in
    let kv_l = List.map (fun (id, ty) -> Var(id,ty)) schema in
    let oute_l, ve = back kv_l in
    let v_t = match ve with 
      | Var(_,ty) -> ty | _ -> failwith "invalid collection value" in
    let ds_id = gen_ds_sym() in
    let ds_e = OutPC(ds_id, k_sch, v_t) in
    let cons_e = Iterate(Lambda(arg, PCValueUpdate(ds_e, [], oute_l, ve)), e) in
    let patterns = 
      let schema_ids = List.map fst schema in
      let p = List.split
        (List.map (fun ((id,_),_) -> id, index id schema_ids) partial_key)
      in Patterns.singleton_pattern_map (ds_id, Patterns.Out p)
    in
    let out_tl = oute_l (* List.map (fun e -> match e with 
      | Var(id,ty) -> m3_type_of_k3_type ty 
      | _ -> failwith "invalid datastructure key var") oute_l *)
    in
    let decl = (ds_id, [], out_tl), patterns
    in ds_e, decl, cons_e

let datastructure_statement (schema, patterns) (pm, rel, args) stmt =
  let is_tuple_collection e = match K3Typechecker.typecheck_expr e with
    | Collection(TTuple _) -> true
    | _ -> false
  in
  let is_primitive_collection e = match e with
    | Slice _ | Lookup _ -> true | _ -> false
  in
  let rec get_collection_name e = match e with
    | Slice (e,_,_) -> get_collection_name e
    | Lookup (e,_) -> get_collection_name e
    | SingletonPC (n,_) | InPC (n,_,_) | OutPC (n,_,_) | PC (n,_,_,_) -> n
    | _ -> failwith "invalid collection name"
  in 
  let slice_collection e pkey = match e with
    | Slice (e,sch,pk) ->
      let npk = List.fold_left (fun kacc (id,e) ->
        if List.mem_assoc id kacc then kacc else kacc@[id,e]) pk pkey
      in npk, Slice(e,sch,npk)
    | _ -> [], e
  in

  (* Result schema and patterns that are mutated inline *)
  let r_sch = ref schema in
  let r_pats = ref patterns in

  (* Returns a list of datastructure signatures and construction statements,
   * and an expression using these datastructures *)
  let bu_f env parts e = match e with
    | Const _ -> [], e
    | Var _ -> [], e

    | Map(Lambda(arg, b), c) when is_tuple_collection c ->
      let sub_ds = List.flatten
        (List.map (fun l -> List.flatten (List.map fst l)) parts) in
      let new_ds_decl, new_e =
        let rlambda = snd (List.hd (List.hd parts)) in
        let rc = snd (List.hd (List.nth parts 1)) in
        match rlambda with
        | Lambda(narg, nb) ->
          let ds_sch, slice_sch =
            let k,v = back (get_arg_vars_w_types narg) in k@[v], k
          in
          (* narg can rebind vars in the env, thus exclude such bindings
           * from the inner env *)
          let inner_env = ListAsSet.diff env ds_sch in
          let bp_pairs, rest_b = extract_predicates inner_env ds_sch nb in
          let probe_key = List.map (fun ((id,_),e) -> id,e) bp_pairs in
          if is_primitive_collection rc then
            begin
              (* merged_probe_key combines the predicates with any existing
                 slice keys. We thus need to add a pattern for this new
                 partial key *)
              let c_name = get_collection_name rc in
              let merged_probe_key, slice_expr = slice_collection rc probe_key in
              let key_pat = List.split (List.map
                (fun (v,_) -> v, index v (List.map fst ds_sch)) merged_probe_key)
              in
              let existing_patterns = Patterns.get_out_patterns !r_pats c_name in
              let has_pattern = List.mem (snd key_pat) existing_patterns in 
              if not(probe_key = [] || has_pattern) then 
                r_pats := Patterns.add_pattern !r_pats
                            (c_name, Patterns.Out(key_pat));
              None,
              Map(Lambda(narg, rest_b), slice_expr)
            end
          else
            (* TODO: lift datastructure constructor if it is independent of narg. *)
            let ds, ds_decl, ds_cons = create_datastructure narg bp_pairs rc in
            let rebuilt_expr =
              (* Perform a lookup instead of a slice if all keys are probed. *)
              if (List.length probe_key) = (List.length slice_sch) then
                (* Rebind variables as needed *)
                let rebuilt_lookup = 
                   begin match narg with
                      | AVar _   -> Lookup(ds, List.map snd probe_key)
                      | ATuple(tl) -> 
                        (* Since we're doing a lookup, we need to match the
                           schema of the original map with a hand-constructed
                           tuple. *)
                        Tuple(   (List.map snd probe_key) @ 
                                 [Lookup(ds, List.map snd probe_key)] )
                   end in
                Singleton(Apply(Lambda(narg, rest_b), rebuilt_lookup))
              else 
                Map(Lambda(narg, rest_b), Slice(ds, slice_sch, probe_key))
            in Some(ds_decl), Block([ds_cons; rebuilt_expr])

        | _ -> failwith "invalid map lambda"
      in sub_ds@[new_ds_decl], new_e

    | _ ->
      let ds = List.flatten
        (List.map (fun l -> List.flatten (List.map fst l)) parts) in
      let e_parts = List.map (List.map snd) parts
      in ds, rebuild_expr e e_parts
  in
  (* Constructs a running environment to indicate bound variables *)
  let td_f env e =
    let f = get_arg_vars_w_types in 
    match e with
    | Lambda(arg, _) -> ListAsSet.union env (f arg)
    | AssocLambda(arg1, arg2, _) -> ListAsSet.multiunion [env; f arg1; f arg2]
    | _ -> env
  in
  let filter_opts l = List.map (function Some(x) -> x
                                | None -> failwith "invalid ds decl and pattern")
       (List.filter ((<>) None) l)
  in
  let decl_opts, new_stmt = fold_expr bu_f td_f args ([],Const(CFloat 1.0)) stmt in
  let decls, patterns = 
    let d, p_l = List.split (filter_opts decl_opts) in
    let p = match p_l with
      | [] -> Patterns.empty_pattern_map()
      | [pm] -> pm
      | _ -> List.fold_left Patterns.merge_pattern_maps (List.hd p_l) (List.tl p_l)
    in d, p
  in (!r_sch, !r_pats), ((decls, patterns), [new_stmt])

(* let k3_type_of_calc_type t = match t with
  | Calculus.TInt -> TInt
  | Calculus.TLong -> failwith "unsupported K3 type: long"
  | Calculus.TDouble -> TFloat
  | Calculus.TString -> failwith "unsupported K3 type: string" *)

let rewrite_with_datastructures dbschema (schema, patterns) k3trigs =
  List.fold_left (fun ((rs,rp),acc) (pm, rel, args, stmts) ->
    let k3args = List.map2 (fun v1 v2 ->
      v1, (snd v2)) args (List.assoc rel dbschema) in
    let (new_sch, new_pats), ds_stmts =
      List.fold_left (fun ((rs,rp),acc) (_,s) ->
        let ((ns,np),nds) = datastructure_statement (rs, rp) (pm, rel, k3args) s
        in ((ns,np), acc@[nds]))
      ((rs, rp), []) stmts 
    in (new_sch, new_pats), acc@[(pm, rel, args), ds_stmts])
    ((schema, patterns), []) k3trigs

let optimize_with_datastructures dbschema k3prog =
  let (g_schema, g_patterns, k3trigs) = k3prog in
  let global_ds = g_schema, g_patterns in
  let new_gds, ds_trigs = rewrite_with_datastructures dbschema global_ds k3trigs
  in (new_gds, ds_trigs)
  
