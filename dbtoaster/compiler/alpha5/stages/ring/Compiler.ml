open Util
open Calculus
open Common
open Common.Types

type 'a roly_t = 'a calc_t list

let rec compute_delta ?(mk_external = None) ?(calc_for_external = None)
                      (delta_name:string) (delta_vars:var_t list) 
                      (expr:'a calc_t): 'a calc_t =
   let rcr = compute_delta ~mk_external:mk_external 
                           ~calc_for_external:calc_for_external
                           delta_name delta_vars in
   match expr with
      | Sum(sl)            -> mk_sum (List.map rcr sl)
      | Prod([])           -> calc_zero (* d(const) = 0 *)
      | Prod(lhs :: rhs)   -> 
         let dlhs = rcr lhs and drhs = prod_list (rcr (Prod(rhs))) in
            mk_sum [
               mk_prod (lhs :: drhs);
               mk_prod (dlhs :: rhs);
               mk_prod (dlhs :: drhs)
            ]
      | Cmp(_,_,_)         -> calc_zero
      | AggSum(gb,subexpr) -> AggSum(gb,rcr subexpr)
      | Value(v)           -> calc_zero 
      | Relation(rel_name, rel_vars)    -> 
         if rel_name = delta_name then
            mk_prod (
               List.map (fun (rv,dv) -> 
                  Definition(rv, (Value(Var(dv))))
               ) (List.combine rel_vars delta_vars)
            )
         else calc_zero
      | External(en,ev,et,ei) -> (
            match calc_for_external with
               | None -> 
                  failwith "Don't know how to compute the delta of an external"
               | Some(c_for_ext) ->
                  rcr (c_for_ext (en,ev,et,ei)) 
         )
      | Definition(var,defn) ->
         (* The one tricky bit of this comes in with definitions... 
            the delta of a definition is not simpler than the definition itself
            so we provide the caller with a hook that rewrites the portion of
            the definition that doesn't get simpler -- generally with an
            external, although the entire expression may not need to be 
            rewritten. *)
         let defn_ext = (match mk_external with 
               | None -> defn
               | Some(mk_ext) -> mk_ext defn
            ) in
               mk_sum [
                  Definition(var, mk_sum [defn_ext; rcr defn]);
                  mk_prod [ calc_neg; (Definition(var, defn_ext)) ]
               ]
;;
let rec roly_poly (expr:'a calc_t): ('a roly_t) =
   match expr with
      | Sum(sl) -> List.flatten (List.map roly_poly sl)
      | Prod([]) -> [Value(Const(Integer(1)))]
      | Prod(term::rest) -> 
         let roly_term = roly_poly term in
         let roly_rest = roly_poly (Prod(rest)) in
            List.flatten (
               List.map (fun r -> List.map (fun t -> mk_prod [t;r]) roly_term)
                        roly_rest
            )
      | AggSum(gb,subexp) -> List.map (fun x -> AggSum(gb, x)) 
                                      (roly_poly subexp)
      | Definition(v,subexp) -> [Definition(v, mk_sum (roly_poly subexp))]
      | _ -> [expr]
;;

(* Simplify a list of monomials (returned earlier in the compilation process
   by roly_poly) by factorizing the expression (i.e. rewriting A * B + A * C 
   into A * (B + C).

   Factorization proceeds one term at a time:
   1 - Generate a list of candidate terms for factorization
   2 - Pick a 'best' term based on a heuristic cost metric
   3 - If no such term exists (All terms are not viable, or appear in
       only one monomial), ** terminate early ** by returning a sum of the input
       monomials.
   4 - Split the list of monomials into two: one for which 'best' can be 
       factorized out (with), and the other for which it can't (without).
   5 - Factorize term out of the 'with' sublist
   6 - Recursively factorize each sublist
   7 - Return factorize('without'); 'best' * factorize('with')

   Things to note: 
   - While A * B + A * C = A * (B + C) is valid, B * A + C * A *may* not be 
     rewritable to A * (B + C) (it is rewritable to (B + C) * A, but this is 
     going to make the factorization process even hairier, quite likely for no
     useful purpose).  The Calculus.commutes_with method is used to test for
     product commutativity.  This is relevant both for generating the list of
     candidate terms, as well as identifying which lists 'contains' the best
     candidate term.
   - The use of mk_sum and mk_prod ensures that we don't introduce any redundant 
     Sum or Prod nodes into the resulting calc_t tree.
   - This is not a complete factorization process.  Eventually we will want to
     replace this with a proper heuristic optimizer like A*.  

   The heuristic we currently use (defined in factorize_cost_metric):
   - Maximize the number of subexpressions the candidate term appears in
   - Do not factorize out terms that contain subsequent filtering predicates 
     (e.g. R(a) * (a > 5) * B + R(a) * C should NOT get factorized to 
     R(a) * (((a > 5) * B) + C)).  This is because the current code generation 
     process will actually end up materializing far more than it needs to in the 
     latter case.
 *)
let rec factorize_impl 
     (cost_metric:('a calc_t ->            (* prod of the terms to the left *)
                   'a calc_t ->            (* the candidate term *)
                   'a calc_t list ->       (* list of the terms to the right *)
                   'b))                    (* return: a cost metric *) 
     (aggregate_costs:'b ->                (* the cost metric being folded in *)
                      'b ->                (* the prior cost metric *)
                      'b)                  (* the merged (new) cost metric *)
     (sort_costs:'b ->                     (* Sort costs as per 'compare' *)
                 'b ->                     (* s.t. 'smaller' cost = better *)
                 int)                      
     (viable_candidate:('a calc_t ->       (* prod of the terms to the left *)
                        'a calc_t ->       (* the candidate term *)
                        'a calc_t list ->  (* list of the terms to the right *)
                        bool))             (* return: false if the term is not
                                              a valid candidate for 
                                              factorization *)
     (monomials: 'a calc_t list): 'a calc_t =
   let rcr = 
      factorize_impl cost_metric aggregate_costs sort_costs viable_candidate
   in
   let monomial_terms = List.map prod_list monomials in
   (* Step 1 and 2.1:Compute the costs *)
   let rec generate_candidates (prev:'a calc_t) (t_rest:'a calc_t list): 
      (('a calc_t * 'b) list) = 
      if t_rest = [] then [] else
         let t = List.hd t_rest in 
         let rest = List.tl t_rest in
            (  if viable_candidate prev t rest 
               then [t, cost_metric prev t rest] 
               else []
            ) @ (generate_candidates (mk_prod [prev; t]) rest)
   in
   let (costs:('a calc_t * 'b) list list) = 
      List.map (generate_candidates calc_one) monomial_terms in
   (* Step 2.2: Aggregate the costs together *)
   let candidates = List.sort (fun (_,(a,_)) (_,(b,_)) -> sort_costs a b) (
      (* Don't bother factoring out terms that only appear in one monomial *)
      List.filter (fun (_,(_,cnt)) -> cnt > 1) (
         (* Aggregate the costs from each monomial's candidate terms *)
         List.fold_left (fun agg candidates ->
            (* Filter out duplicate terms from a given monomial *)
            fst (List.fold_left (fun (agg, visited) (t, cost) ->
               if List.mem t visited then (agg, visited)
               else ((
                  if List.mem_assoc t agg then 
                     let (old_cost, old_cnt) = List.assoc t agg in
                     (t, (aggregate_costs cost old_cost, old_cnt + 1)) :: 
                        (List.remove_assoc t agg)
                  else
                     (t, (cost, 1)) :: agg
               ), t :: visited)
            ) (agg, []) candidates)
         ) [] costs
      )
   ) in
   (* Step 3: Recursion bottoms out here *)
   if candidates = [] then mk_sum monomials
   (* Finish up Step 2: Pick the *best* candidate out of the now sorted list *)
   else let (best, _) = List.hd candidates in
   (* Steps 4 and 5 *)
   let rec factor_out prev t_rest =
      if t_rest = [] then None else
      let t = List.hd t_rest in let rest = List.tl t_rest in
      if (t = best) && (viable_candidate prev t rest) then
         Some(mk_prod (prev :: rest))
      else factor_out (mk_prod [prev; t]) rest
   in
   let (with_term, without_term) = 
      List.fold_right (fun m (with_term, without_term) ->
         match factor_out calc_one m with
          | None    -> (with_term, (mk_prod m) :: without_term)
          | Some(f) -> (f :: with_term, without_term)
      ) monomial_terms ([],[])
   in
   (* Steps 6 and 7 *)
      mk_sum [rcr without_term; mk_prod [best; rcr with_term]]
;;
let factorize (monomials:'a calc_t list): 'a calc_t =
   let cost_metric _ _ _ = 
      -1
   in
   let aggregate_costs c old = 
      c + old
   in
   let viable_candidate prev t rest =
      if not (commutes_with prev t) then false
      else let sch = List.map fst (List.filter snd (get_schema t)) in
      let check_value a = 
         match a with Const(_) -> false | Var(v) -> List.mem v sch
      in
      (* We don't factorize out terms that define variables which are 
         subsequently filtered by a comparison predicate *)
         not (List.exists (fun rt ->
            match rt with 
               Cmp(a,_,b) -> (check_value a) || (check_value b)
               | _ -> false
         ) rest)
   in
      factorize_impl cost_metric
                     aggregate_costs
                     compare
                     viable_candidate 
                     monomials


module Opt = struct
   (* As a general rule, Calculus is simpler if all the AggSum()s are as
      close to the root as possible.  This essentially allows us to decide where
      the best place to run an aggregation loop is, potentially unnesting a 
      number of nested loops, or doing some other optimization across an aggsum
      boundary.
      
      lift_aggsums lifts all AggSums as high in the parse tree as possible and
      returns an accordingly modified version of its input.  *)
   let lift_aggsums (top_expr:'a calc_t): 'a calc_t =
      let merge_list merge_op list = 
         let (v,c) = List.fold_left (fun (old_v, old_c) (new_v, new_c) ->
            (ListAsSet.union old_v new_v, old_c @ [new_c])
         ) ([], []) list in
            (v, merge_op c)
      in
      let var_list_matches_schema vars expr = 
         ListAsSet.seteq 
            vars
            (fst (List.split (List.filter snd (get_schema expr))))
      in
      let rec rcr (expr:'a calc_t): (var_t list * 'a calc_t) =
         
         match expr with
            (* The list of variables returned is the schema of the 
               subexpression.  This list is restricted by aggsums, and 
               expanded by relations (the rel vars), externals (the bound vars),
               and definitions (the variable being defined).  
               
               Notes: 
                  Sum is correct, because the entire set of things being summed
                  must, by definition have the same schema.
            *)               
            | Sum(sl) ->  merge_list mk_sum  (List.map rcr sl)
            | Prod(pl) -> merge_list mk_prod (List.map rcr pl)
            | Cmp(_,_,_) -> ([], expr)
            | AggSum(gb,subexp) -> (gb, snd (rcr subexp))
            | Value(_) -> ([], expr)
            | Relation(_,rv) -> (rv, expr)
            | External(_,ev,_,_) -> (List.map fst (List.filter snd ev), expr)
            | Definition(dv,dd) -> 
               let (agg_vars,dd_pulled) = rcr dd in
                  (* See if we need to create a root level aggsum -- this is the
                     case if the native schema of the AggSum-free (i.e., pulled) 
                     expression contains at least one variable not in the group-
                     by schema defined by the pulling *)
                  let dd_wrapped =
                     if var_list_matches_schema agg_vars dd_pulled
                     then dd_pulled
                     else AggSum(agg_vars, dd_pulled)
                  in (dv :: agg_vars, (Definition(dv, dd_wrapped)))
      in 
      let (top_vars, simplified) = rcr top_expr in
         if var_list_matches_schema top_vars simplified
         then simplified
         else AggSum(top_vars, simplified)
   
   (* The second step of variable unification is to take definition terms that
      map one variable to another directly (e.g., (foo <- bar)) and apply the
      mapping directly (e.g., in the above case, replace all instances of foo
      with bar) to all the remaining terms in the expression.
      
      propagate_forward extracts the relevant definition terms, performs the
      replacements and returns a list of the mappings which it has discovered.
      (since the schema of the expression has now changed)
   *)
   let propagate_forward (top_expr:'a calc_t): (var_t * var_t) list * 'a calc_t=
      let sub (m:(var_t*var_t) list) (x:var_t): var_t = Function.apply m x x in
      let sub_val (m:(var_t*var_t) list) (x:value_t): value_t = match x with 
         | Const(_) -> x | Var(v) -> Var(sub m v) in
      let merge_mappings (a_list:(var_t*var_t) list)
                         (b_list:(var_t*var_t) list):(var_t*var_t) list =
         (* sanity check: b always occurs on the RHS of a; thus, we should
            never find a transitive closure from b to a, only visa versa *)
         if (ListAsSet.inter (List.map snd a_list)
                             (List.map fst b_list)) <> []
         then failwith "BUG: propagate_forward: closure from RHS to LHS"
         else ListAsSet.inter a_list
            (List.map (fun (o,n) -> (o, (Function.apply a_list n n))) b_list)
      in   
      let rec rcr (mapping:(var_t*var_t) list) 
                  (expr:'a calc_t): ((var_t*var_t) list * 'a calc_t) =
         match expr with
            | Sum(sl) -> 
               (* Sums are a bit delicate, since we need to preserve the
                  schema of everything inside.  We can apply mappings we've 
                  obtained outside the expression since those get applied to
                  the entire sum.  We can also apply new mappings nested inside
                  an AggSum (as long as they're not for group-by variables).
                  All the other nested mappings however, need to be reversed --
                  we need to re-instantiate the variables that we've gotten rid
                  of in order to preserve the schema of the expression.  In 
                  other words, we never export any new mappings. 
                  Either way, we make sure that we're not introducing new 
                  variables by wrapping each term in an aggsum to project away
                  any variables not in the original expression's schema.  
                  mk_aggsum is smart enough to figure out whether the AggSum is 
                  redundant.
                  *)
               ([], mk_sum (List.map (fun term ->
                  let (new_mapping, new_term) = rcr mapping term in
                     mk_aggsum (get_bound_schema term) (
                        mk_prod (new_term :: (List.map (fun (old_var,new_var) ->
                           Definition(old_var, Value(Var(new_var)))
                        ) new_mapping))
                     )
               ) sl))
            | Prod(pl) ->
               List.fold_left (fun (old_mapping, new_expr) (term) ->
                  let (new_mapping, new_term) = 
                     rcr (merge_mappings mapping old_mapping) term 
                  in
                     ((merge_mappings old_mapping new_mapping),
                      mk_prod [new_expr; new_term])
               ) (mapping, calc_one) pl
            | Cmp(a, op, b) -> 
               ([], Cmp(sub_val mapping a, op, sub_val mapping b))
            | AggSum(gb, subexp) -> 
               let (new_mapping, new_term) = rcr mapping expr in
               let merged_mapping = merge_mappings mapping new_mapping in
               (* Apply substitutions to the group by terms ... this includes
                  any new mappings we've discovered inside. *)
               let new_gb = List.map (sub merged_mapping) gb in
                  (
                     (* Don't propagate mappings filtered out by the aggsum *)
                     List.filter (fun (v,_) -> List.mem v new_gb) 
                                 merged_mapping,
                     (AggSum(new_gb, new_term))
                  )
            | Value(v) -> ([], Value(sub_val mapping v))
            | Relation(rn, rv) -> 
               ([], (Relation(rn, List.map (sub mapping) rv)))
            | External(en,ev,et,ei) ->
               ([], (External(en,
                              List.map (fun (v,bound) -> (sub mapping v,bound)) 
                                       ev,
                              et,ei)))
            | Definition(dv,(Value(Var(v)))) -> ([dv, v], calc_one)
            | Definition(dv, dd) ->
               let (new_mapping, new_term) = rcr mapping dd in
                  (new_mapping, Definition(dv, new_term))
      in (rcr [] top_expr)

   (* The first step in variable unification is replacement of equality 
      constraints with an equivalent definition term (if such a replacement is
      actually possible).  For example 
         (R(a) * S(b) * (a = b))
      becomes
         (R(a) * (b <- a) * S(b))
      This is a careful process -- We need to keep shifting the equality left
      until only one of the variables is bound.  Note that this may not always
      be possible, so at some point we might need to give up and put the 
      equality back into the expression 
      
      propagate_backward takes an expression and a set of input variables (which
      are never allowed to be unified), and returns a new expression with as 
      many of the equalities replaced by definition terms.
      *)
   let propagate_backwards (top_in_sch:var_t list) (top_expr:'a calc_t): 
                           'a calc_t =
      let rec rcr (in_sch:var_t list) (expr:'a calc_t): 'a calc_t =
         match expr with
            (* We could be a little more aggressive here and factorize common
               equalities out of the sum.  This seems like a job for an explicit 
               factorize_poly optimization though *)
         | Sum(sl) -> mk_sum (List.map (rcr in_sch) sl)
            (* Could also be a bit more aggressive here...  *)
         | AggSum(gb, subexp) -> AggSum(gb, rcr in_sch subexp)
         | Definition(dv, subexp) -> Definition(dv, rcr in_sch subexp)
         | Value(_) -> expr
         | Relation(_,_) -> expr
         | External(_,_,_,_) -> expr
         | Cmp(_,_,_) -> expr
         | Prod(pl) -> 
            mk_prod (List.fold_left (fun new_pl prod_term ->
               let try_sub a b =
                  if List.mem a in_sch then None
                  else let (ret,found) = 
                     (* iterate over the LHS terms until we find one where 
                        b is defined, but a isn't.  We tack a calc_one onto the
                        end of the list so that we can check for insertion at
                        the very end as well.  This is ok, because mk_prod will
                        eliminate the calc_one terms. *)
                     List.fold_left (fun (ret,found) term ->
                        if found then (ret @ [term],true) else
                        let sch = 
                           ListAsSet.union in_sch 
                                           (get_bound_schema (mk_prod ret))
                        in
                           if (List.mem b sch) && not (List.mem a sch) then
                              (  ret @ [Definition(a, Value(Var(b)));
                                        term],
                                 true)
                           else (ret @ [term], false)
                     ) ([calc_one], false) (new_pl@[calc_one])
                  in if found then Some(ret) else None
               in
               match prod_term with
               | Cmp(Var(a),Eq,Var(b)) -> (
                  (* Try to find a way to insert a <- b *)
                  match try_sub a b with 
                     | Some(ret) -> ret
                     | _ -> (
                        (* if that fails, try to insert b <- a *)
                        match try_sub b a with
                           Some(ret) -> ret
                           (* if that fails, just insert the equality back *)
                           | _ -> new_pl @ [prod_term]
                     )
                  )
               | _ -> new_pl @ [prod_term]
            ) [] pl) 
      in (rcr top_in_sch top_expr)

   (* merge_constants combines constants in sums/products.  Additionally, 
      multipliers in front of structurally equivalent terms in a sum are 
      combined (e.g., (X + 2X + Y) is turned into (3X+Y)).  merge_constants is
      not particularly intelligent about how it matches terms, and uses '='.
      Through the magic of mk_sum/mk_prod, this also happens to eliminate all 
      terms that cancel out (e.g., (X + (-1)*X) => 0) *)
   let merge_constants (top_expr: 'a calc_t): 'a calc_t =
      let one = Integer(1) in
      let rec rcr (expr: 'a calc_t): ('a calc_t * const_t) =
         match expr with
          | Sum(sl) -> 
            (  mk_sum (
                  List.map (fun (term, consts) ->
                     mk_prod [Value(Const(Arithmetic.add_list consts)); term]
                  ) (ListExtras.reduce_assoc (List.map rcr sl))
               ),
               one
            )
          | Prod(pl) ->
            let (terms, consts) = List.split (List.map rcr pl) in
               (  mk_prod terms,
                  Arithmetic.mult_list consts
               )
          | AggSum(gb,subexp) -> 
            let (simpler_subexp, const) = rcr subexp in
               (  (AggSum(gb, simpler_subexp)),
                  const
               )
          | Value(Const(c))     -> (calc_one, c)
          | Value(Var(v))       -> (expr, one)
          | Cmp(_,_,_)          -> (expr, one)
          | Relation(_,_)       -> (expr, one)
          | External(_,_,_,_)   -> (expr, one)
          | Definition(v, subexp) -> 
            let (simpler_subexp, const) = rcr subexp in
               (  (Definition(v, mk_prod [Value(Const(const)); 
                                          simpler_subexp])),
                  one
               )
      in 
      let (simpler_expr, const) = rcr top_expr in
         mk_prod [Value(Const(const)); simpler_expr]
   
   let lift_definitions (top_ivars: var_t list) (top_expr: 'a calc_t): 
                        'a calc_t =
      let inject_defns ((expr:'a calc_t),(defns:(var_t*'a calc_t) list)) =
         mk_prod ((List.map (fun (dv,dd) -> Definition(dv,dd)) defns) @ [expr])
      in
      let rec rcr (ivars:var_t list) (expr:'a calc_t): 
              ('a calc_t *((var_t*'a calc_t) list)) =
         match expr with
         | Sum(sl) -> 
            (mk_sum (List.map (fun x -> inject_defns (rcr ivars x)) sl), [])
         | Prod(term :: rest) ->
            (* Start with the leftmost term.  We might be able to extract some
               bindings out of it... those get propagated directly up (since
               there's nothing left for them to commute with).  *)
            let (term_exp, term_defns) = rcr ivars term in
            (* Now compute the input variables to anything left of the term.
               This includes the overall input variables, as well as any 
               bindings introduced by the definition terms extracted from the
               leftmost term *)
            let lhs_bindings = List.fold_left (fun old (dv,dd) ->
                  ListAsSet.multiunion [[dv];(get_bound_schema dd);old]
               ) ivars term_defns in
            (* Recur on anything else.  We can cheat and just use the overall
               schema of the term, since that should be the same as the union of
               the term_exp, and term_defn bindings *)
            let (sub_exp, sub_defns) = 
               rcr (ListAsSet.union (get_bound_schema term) ivars)
                   (mk_prod rest)
            in
            (* Now, repeatedly check to see if we can commute each definition
               term in our list with the new term *)
            let (ret_term, ret_defns, _) = 
               List.fold_left (fun (ret_term,ret_defns,lhs_bindings) (dv,dd) ->
                  if commutes_with ~ivars:ivars 
                                   ret_term 
                                   (Definition(dv,dd))
                  (* If it's commutable, then we can tack it onto our list of
                     definitions to lift up. *)
                  then (   ret_term, 
                           ret_defns @ [dv, dd],
                           ListAsSet.multiunion [[dv];(get_bound_schema dd);
                                                 lhs_bindings] )
                  (* Otherwise, we tack it back onto the main term *)
                  else (   mk_prod [ret_term;Definition(dv,dd)],
                           ret_defns,
                           lhs_bindings )
               ) (term_exp, term_defns, lhs_bindings) sub_defns
            in (mk_prod [ret_term; sub_exp], ret_defns)
         | Prod(pl) -> (mk_prod pl, [])
         | Cmp _ -> (expr, [])
         | Value _ -> (expr, [])
         | Relation _ -> (expr, [])
         | External _ -> (expr, [])
         | Definition(dv, dd) -> 
            (* Two things we need to do here.  First, it's possible to unnest
               definition terms: 
               e.g., 
                  A <- ((B <- foo) * bar)
               is the same as
                  (B <- foo) * (A <- bar)
               Note that this is not true for
                  A <- (bar * (B <- foo))
               but this case is handled by explicitly Prod() above *)
            let (dd_expr, dd_defns) = rcr (ListAsSet.union [dv] ivars) dd in
            (* Second, the definition term itself gets put onto the list. *)
               (calc_one, dd_defns @ [dv, dd_expr])
         | AggSum(gb, subexp) ->
            (* We can lift any definition terms who's bound variables are a 
               subset of the group by terms in the aggsum.  Note that there's
               a small gotcha... the definition term list is in order, so we 
               also need to make sure that the definition term can commute to
               the head of the list of terms that can't be lifted. *)
            let (sub_term, sub_defns) = rcr ivars subexp in
            let (lifted_defns, head_exp, _) = 
               List.fold_left (fun (lifted_defns, head_exp, bindings) (dv,dd) ->
                  let (defn_bindings) = 
                     (ListAsSet.union [dv] (get_bound_schema dd)) in
                  if (commutes_with ~ivars:bindings
                                    head_exp
                                    (Definition(dv,dd))) &&
                     (ListAsSet.subset defn_bindings gb)
                  then (   lifted_defns @ [dv,dd],
                           head_exp,
                           ListAsSet.union defn_bindings 
                                           bindings )
                  else (   lifted_defns, 
                           mk_prod [head_exp;Definition(dv,dd)],
                           bindings )
               ) ([], calc_one, ivars) sub_defns
            in
            let new_exp = mk_prod [head_exp; sub_term] in
               ((AggSum(
                  (* We might be able to eliminate some group by terms *)
                  ListAsSet.inter gb (get_bound_schema new_exp),
                  new_exp
                )), lifted_defns)
      in inject_defns (rcr top_ivars top_expr)
         
   
   type opt_t = LiftAggSums | PropagateFwd | PropagateBkwd | MergeConstants |
                LiftDefinitions

   let all_opts = [ 
      LiftAggSums ; PropagateBkwd ; PropagateFwd ; MergeConstants ;
      LiftDefinitions
   ]
   
   let rec optimize ?(opts = all_opts) 
                    ((ivars:var_t list),
                     (top_sch:var_t list),
                     (top_expr:'a calc_t)):
                    (var_t list * 'a calc_t) =
      let (opt_sch, opt_expr) = 
         List.fold_left (fun (sch,expr) opt ->
            match opt with 
               | LiftAggSums  -> (sch, lift_aggsums expr)
               | PropagateFwd -> (
                     let (subs,opt_expr) = propagate_forward expr in
                        (  (List.map (Function.apply_if_present subs) sch),
                           opt_expr
                        )
                  )
               | PropagateBkwd -> (sch, propagate_backwards ivars expr)
               | MergeConstants -> (sch, merge_constants expr)
               | LiftDefinitions -> (sch, lift_definitions ivars expr)
         ) (top_sch, top_expr) opts
      in if top_expr <> opt_expr 
         then optimize ~opts:opts (ivars, opt_sch, opt_expr)
         else (opt_sch, opt_expr)
end