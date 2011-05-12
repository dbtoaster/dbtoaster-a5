open Util
open Calculus
open Common.Types

let rec compute_delta ?(mk_external = None) ?(calc_for_external = None)
                      (delta_name:string) (delta_vars:var_t list) 
                      (expr:'a calc_t): 'a calc_t =
   let rcr = compute_delta ~mk_external:mk_external 
                           ~calc_for_external:calc_for_external
                           delta_name delta_vars in
   let c i = Value(Const(Integer(i))) in
   match expr with
      | Sum(sl)            -> mk_sum (List.map rcr sl)
      | Prod([])           -> c 0
      | Prod(lhs :: rhs)   -> 
         let dlhs = rcr lhs and drhs = prod_list (rcr (Prod(rhs))) in
            mk_sum [
               mk_prod (lhs :: drhs);
               mk_prod (dlhs :: rhs);
               mk_prod (dlhs :: drhs)
            ]
      | Neg(subexpr)       -> mk_neg (rcr subexpr)
      | Cmp(_,_,_)         -> c 0
      | AggSum(gb,subexpr) -> AggSum(gb,rcr subexpr)
      | Value(v)           -> c 0
      | Relation(rel_name, rel_vars)    -> 
         if rel_name = delta_name then
            mk_prod (
               List.map (fun (rv,dv) -> 
                  Definition(rv, (Value(Var(dv))))
               ) (List.combine rel_vars delta_vars)
            )
         else c 0
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
                  mk_neg (Definition(var, defn_ext))
               ]

;;

let rec roly_poly (expr:'a calc_t): ('a calc_t list) =
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
      | Neg(n) -> List.map mk_neg (roly_poly n)
      | AggSum(gb,subexp) -> List.map (fun x -> AggSum(gb, x)) 
                                      (roly_poly subexp)
      | Definition(v,subexp) -> [Definition(v, mk_sum (roly_poly subexp))]
      | _ -> [expr]

;;

let rec push_down_negs (neg:bool) (expr:'a calc_t) =
   match expr with 
      | Sum(sl) -> mk_sum (List.map (push_down_negs neg) sl)
      | Prod(pl) -> mk_prod (List.map (push_down_negs neg) pl)
      | Neg(n) -> push_down_negs (not neg) n
      | AggSum(gb,subexp) -> AggSum(gb, push_down_negs neg subexp)
      | Definition(dv, dd) -> 
         let new_expr = Definition(dv, push_down_negs false dd)
         in if neg then Neg(new_expr) else new_expr
      | _ -> if neg then Neg(expr) else expr

;;

let rec pull_up_aggsums (expr:'a calc_t): (var_t list * 'a calc_t) =
   let merge_list merge_op list = 
      let (v,c) = List.fold_left (fun (old_v, old_c) (new_v, new_c) ->
         (ListAsSet.union old_v new_v, old_c @ [new_c])
      ) ([], []) (List.map pull_up_aggsums list) in
         (v, merge_op c)
   in
   match expr with
      | Sum(sl) -> merge_list mk_sum sl
      | Prod(pl) -> merge_list mk_prod pl
      | Neg(n) -> let (vars, calc) = pull_up_aggsums n in (vars, (Neg(calc)))
      | Cmp(_,_,_) -> ([], expr)
      | AggSum(gb,subexp) -> (gb, snd (pull_up_aggsums subexp))
      | Value(_) -> ([], expr)
      | Relation(_,rv) -> (rv, expr)
      | External(_,ev,_,_) -> (List.map fst (List.filter snd ev), expr)
      | Definition(dv,dd) -> 
         let (agg_vars,dd_pulled) = pull_up_aggsums dd in
            (* See if we need to create a root level aggsum -- this is the
               case if the native schema of the AggSum-free (i.e., pulled) 
               expression contains at least one variable not in the group-by
               schema defined by the pulling *)
            let dd_wrapped =
               if not (List.exists (fun (var,_) ->
                     List.mem var agg_vars
                  ) (List.filter snd (Calculus.get_schema dd_pulled)))
               then AggSum(agg_vars, dd_pulled)
               else dd_pulled
            in (dv :: agg_vars, (Definition(dv, dd_wrapped)))

;;

let rec propagate_forward (mapping:(var_t * var_t) list) (expr:'a calc_t):
                          ((var_t * var_t) list * 'a calc_t) =
   let sub (x:var_t): var_t = Function.apply mapping x x in
   let sub_val (x:value_t): value_t = match x with | Const(_) -> x
                                                   | Var(v) -> Var(sub v) in
   let merge_mappings a b = 
      let new_a = List.map (fun (aov, anv) ->
         if List.mem_assoc anv b then (aov, List.assoc anv b)
                                 else (aov, anv)) a in
      let new_b = List.filter (fun (bv,_) -> not (List.mem_assoc bv a)) b in
         new_a @ new_b
   in
   match expr with
      | Sum(sl) -> 
         (* We can't really do much here without breaking things... simplify 
            should really only be used on monomials.  Still, we don't need
            to fail entirely.  We just tack on some definition terms to remap
            the removed elements of the schema back to their original values 
            so that the schema of each sum element matches correctly. *)
         ([], mk_sum (List.map (fun term ->
            let (mapping, new_term) = propagate_forward mapping term in
               if mapping = [] then new_term
               else mk_prod (
                  new_term :: (List.map (fun (old_var,new_value) -> 
                     Definition(old_var, Value(Var(new_value)))) mapping)
               )
            ) sl
         ))
      | Prod(pl) ->
         List.fold_left (fun (new_mapping, new_expr) (term) -> 
            let (term_mapping, new_term) = 
               propagate_forward (merge_mappings mapping new_mapping) term
            in (
                  (merge_mappings new_mapping term_mapping),
                  mk_prod [new_expr; term]
               )
         ) ([], (Value(Const(Integer(1))))) pl
      | Neg(term) -> 
         let (new_mapping, new_term) = propagate_forward mapping term
            in (new_mapping, (Neg(new_term)))
      | Cmp(a,op,b) -> ([], Cmp(sub_val a, op, sub_val b))
      | AggSum(gb,subexp) -> 
         let (mapping, new_term) = propagate_forward mapping subexp
            in (
               (* Don't propagate mappings filtered out by the aggsum *)
               List.filter (fun (v,_) -> List.mem v gb) mapping,
               (AggSum(List.map sub gb, new_term))
            )
      | Value(Const(_)) -> ([], expr)
      | Value(Var(v)) -> ([], (Value(Var(sub v))))
      | Relation(rn,rv) -> ([], (Relation(rn, List.map sub rv)))
      | External(en,ev,et,ei) ->
         ([], (External(en,List.map (fun (v,bound) -> (sub v,bound)) ev,et,ei)))
      | Definition(dv,(Value(Var(v)))) -> 
         ([dv, v], (Value(Const(Integer(1)))))
      | Definition(dv, dd) -> 
         let (new_mapping, new_term) = propagate_forward mapping dd
            in (new_mapping, (Definition(dv, new_term)))

;;

let rec propagate_backwards (in_schema:var_t list) (expr:'a calc_t): 
                            ('a calc_t) =
   let rcr = propagate_backwards in_schema in
   match expr with
      | Sum(sl) -> mk_sum (List.map rcr sl)
      | Prod(pl) -> 
         let (eq_terms, basic_terms) = 
            List.partition (fun t -> match t with | Cmp(_,Eq,_) -> true
                                                  | _ -> false) pl
         in
            mk_prod (List.map snd (List.fold_left 
               (fun (new_prod:(var_t list * 'a calc_t) list) 
                    (eq_term:'a calc_t) ->
               let find_bindpoint (v:value_t): int = 
                  match v with | Const(_) -> 0
                               | Var(var) -> 
                     if List.mem var in_schema then 0
                     else fst (List.fold_left (fun (i,found) (t_sch, t) ->
                        if found then (i, true)
                        else if (List.mem var t_sch)
                             then (i, true)
                             else (i+1, false)
                     ) (1,false) new_prod)
               in
               let inject_defn (dv:var_t) (dd:value_t) (index:int) =
                  if index = 0 
                  then 
                     [[dv],
                      (Definition(dv, (Value(dd))))] @ new_prod
                  else
                     snd (List.fold_left (fun (i, new_prod2) t ->
                        (i - 1, new_prod2 @ [t] @ 
                           (if i = 0 then
                              [[dv], 
                               (Definition(dv, (Value(dd))))]
                           else [])
                        )
                     ) (index-1, []) new_prod)
               in
               let (a,b) = match eq_term with Cmp(a,Eq,b) -> (a,b) 
                             | _ -> failwith "Bug in propagate_backwards (1)"
               in
                  let bp_a = find_bindpoint a and bp_b = find_bindpoint b in
                  if ((bp_a = 0) && (bp_b = 0)) || (bp_a = bp_b)
                  then new_prod @ [[],
                                   (Cmp(a, Eq, b))
                                  ]
                  else
                     if bp_a < bp_b 
                        then match b with Var(v) -> inject_defn v a bp_a
                        | Const(_) -> failwith "Bug in propagate_backwards (2)" 
                        else match a with Var(v) -> inject_defn v b bp_b
                        | Const(_) -> failwith "Bug in propagate_backwards (3)" 
            ) (snd (List.fold_left (fun (sch, terms) t ->
               let new_sch = 
                  (List.map fst (List.filter snd (Calculus.get_schema t))) 
               in
                  ( sch @ new_sch, 
                    terms @ [new_sch, propagate_backwards sch t]
                  )
               ) (in_schema, []) basic_terms))
              eq_terms
         ))
      | Neg(subexpr) -> Neg(rcr subexpr)
      | AggSum(gb, subexpr) -> AggSum(gb, rcr subexpr)
      | Definition(dv, subexpr) -> Definition(dv, rcr subexpr)
      | _ -> expr
;;
let simplify (in_sch:(var_t list)) (base:'a calc_t): 
             ((var_t*var_t) list * 'a calc_t) =
   let rec fixed_point (expr:'a calc_t) 
                       (mapping:(var_t * var_t) list) =
      let step1 = push_down_negs false expr in
      let (v,e) = pull_up_aggsums step1 in let step2 = AggSum(v,e) in
      let step3 = propagate_backwards in_sch step2 in
      let (new_mapping,new_expr) = propagate_forward mapping step3 in
         if new_expr = expr then (new_mapping,new_expr)
                            else fixed_point new_expr new_mapping
   in fixed_point base []
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
