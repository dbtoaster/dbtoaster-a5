(*************************************
 * K3 normalization and optimization
 *************************************)

(* Lifting (aka K3 expression normalization):
 * -- conditionals:
 *   ++ the point of lifting is to enable optimization of pre, post code in
 *      the context of each branch.
 *   ++ we lift conditionals to the top of their dependency block, pushing
 *      parent code into both branches as necessary.
 *   ++ lifting increases code size, thus we may want a lowering transformation
 *      at the very end if no other optimizations have been applied to either
 *      branch.
 *
 * -- functions: overall goal is to simplify any partially evaluted function to
 *    a normalized form, of lambda x -> lambda y -> ... , where x,y are the
 *    remaining variables to be bound before evaluation can complete 
 *
 * -- collection functions:
 *   ++ we lift collection operations applied to partially evaluated functions,
 *   ++ two cases:
 *     -- partially evaluated collections, e.g.:
 *        map(lambda y -> ..., lambda x -> collection...)
 *        => lambda x -> map(lambda y -> ..., collection ...)
 *     -- partially evaluated apply-each
 *        map(lambda a -> lambda b c -> ..., collection(b;c))
 *        => lambda a -> map(lambda b c -> ..., collection(b;c))
 *   ++ transformation order: pe-coll first, then pe-appeach 
 *   ++ when can we not lift? see notes on lifting in typechecking:
 *     i.  collections of functions, e.g.
 *         map(lambda x ..., collection(lambda y -> ...))
 *   ++ cases not handled:
 *     i. partially evaluted init aggregate values
 *     ii. partially evaluted group-by functions
 *
 * -- regular functions:
 *   ++ we lift lambdas above partial function applications, e.g.:
 *      apply(lambda a -> lambda b -> ..., x)
 *      => lambda b -> apply (lambda a -> ..., x)
 * 
 * -- what about interaction of conditionals and lifting?
 *   ++ must respect data dependencies, i.e. conditionals using bindings cannot
 *      be lifted above the relevant lambda
 *   ++ otherwise everything else is fair game, provided we duplicate code as
 *      necessary.
 * 
 * -- expression normalization algorithm:
 *  ++ recursively turn every sub expression into the form:
 *    lambda ... ->
 *        independent ifs
 *        then (... dependent ifs ... collection ops (..., collection) ... )
 *        else (... dependent ifs ... collection ops (..., collection) ... )
 *    where the outer most lambdas are for any free variables in the body
 *    and the independent ifs are those if statements that depend only on these
 *    free variables. Dependent ifs are those whose predicates are bound by
 *    some enclosed expression.
 *
 *  ++ lifting lambdas:
 *    -- cannot lift lambda above apply without changing typing of
 *       surrounding code
 *      ++ we could inline the application instead, but this will blow up
 *         code size, or could lead to multiple recomputations of the
 *         argument. In K3, since applies used for accessing maps, this would
 *         mean multiple conversions of maps to our internal temporary
 *         collections.
 *      ++ we'll only eliminate lambdas (i.e. inline) for flat arguments.
 * 
 *    -- lifting algorithm:
 *      ++ this is not standard lambda lifting, which aims at optimizing
 *         closure construction, etc, rather it is to create a normalized
 *         form for partially evaluated K3. This ensures other optimizations
 *         can fully apply to the curried function's body.
 *      ++ in general, lift any remaining lambdas from all child expressions in
 *         left-to-right AST order
 *      ++ pattern match on:
 *        -- collection ops, yield lifted form.
 *           this will fully lift lambdas out of collection operation chains.
 *        -- project, singletons
 *        -- binops: lambdas down each branch get lifted outside the binop, e.g.
 *           (lambda x -> ...) * (lambda y -> ...)
 *           => lambda x -> lambda y -> ... * ...
 *        -- blocks: lambdas for each stmt get lifted
 *        -- lambda, assoc lambda: lambdas from bodies get lifted to create
 *           nested form
 *        -- apply: partial evaluations get lifted
 *          ++ we must be careful not to change application order of args
 *          ++ apply binds from left-to-right, thus nested lambdas from partials
 *             must stay inner-most (i.e. right-most) until lifted outside 
 *             the apply
 *        -- tuple collection ops: map expr and key lambdas get lifted
 *        -- persistent collections: cannot contain lambdas
 *        -- updates: lift from all children
 *
 *    -- let's defer this since the M3 generated will not contain opportunities
 *       to apply this immediately... BUT can it occur in an intermediate
 *       form after some other optimization we apply?
 *
 *  ++ lifting ifs: TODO
 *
 *  ++ transformation ordering: what order do we normalize/optimize?
 *    1. lambdas first, since they may block condition lifting
 *    2. conditions second. Note this implies all normalization transformations
 *       before any optimizations.
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
 *      we compose functions, should we always inline?
 *   ++ this depends on what types of inlining we can do. In general what is
 *      the goal of inlining?
 *
 * -- function arity transforms
 *
 *)