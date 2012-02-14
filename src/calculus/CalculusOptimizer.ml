open Types
open Ring
open Arithmetic
open Calculus

module C = Calculus

type opt_t = 
   | OptCombineValues
   | OptLiftEqualities
   | OptUnifyLifts
   | OptNestingRewrites
   | OptFactorizePolynomial
;;
(******************************************************************************
 * combine_values expr
 *   Recursively merges together Value terms appearing in expr.  Ring operators 
 *   are pushed into the ValueRing, and expressions are evaluated to the fullest
 *   extent possible.  Additionally, negations are converted to multiplication
 *   by -1.
 *
 *   expr: The calculus expression to be processed
 ******************************************************************************)
let rec combine_values (expr:C.expr_t): C.expr_t =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Combine Values: "^(C.string_of_expr expr) 
   );
   let rcr = combine_values in
   let merge calc_op val_op e =
      let (val_list, calc_list) = List.fold_right (fun term (v, c) ->
         match term with
            | CalcRing.Val(Value(new_v)) -> (new_v :: v, c)
            | _                          -> (v, term :: c)
      ) e ([],[]) in
      if val_list = [] then calc_op calc_list
      else let val_term = 
         CalcRing.mk_val (Value(Arithmetic.eval_partial (val_op val_list))) in
      if calc_list = [] then val_term 
      else calc_op (val_term::calc_list)
   in
   CalcRing.fold
      (merge CalcRing.mk_sum  ValueRing.mk_sum)
      (merge CalcRing.mk_prod ValueRing.mk_prod)
      (fun x -> CalcRing.mk_prod [CalcRing.mk_val (Value(mk_int (-1))); x])
      (fun x -> CalcRing.mk_val (match x with
         | Value(_) | Rel(_,_,_) | External(_) | Cmp(_,_,_) -> x
         | AggSum(gb_vars, subexp) -> AggSum(gb_vars, rcr subexp)
         | Lift(lift_v, subexp)    -> Lift(lift_v,    rcr subexp)
      ))
      expr
;;
(******************************************************************************
 * lift_equalities scope expr
 *   Recursively commutes equality comparison terms as far left through the 
 *   expression as possible.  Once the term commutes all the way to the left, 
 *   if it is possible to commute it further by converting it to a lift, this
 *   function does so.
 *
 *   scope: Any variables defined outside of the expression being evaluated. 
 *          This includes trigger variables, input variables from the map on the
 *          lhs of the statement being evaluated.
 *   expr:  The calculus expression to be processed
 ******************************************************************************)
type lift_candidate_t = 
   (* A var=var equality that can be lifted in either direction *)
   | BidirectionalLift of var_t * var_t
   (* An equality that can only be lifted one way *)
   | UnidirectionalLift of var_t * value_t

let lift_equalities (scope:var_t list) (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Lift Equalities: "^(C.string_of_expr big_expr) 
   );
   let candidate_term (candidate:lift_candidate_t) = 
      CalcRing.mk_val (match candidate with
         | BidirectionalLift(x, y)  -> (Cmp(Eq, mk_var x, mk_var y))
         | UnidirectionalLift(x, y) -> (Cmp(Eq, mk_var x, y))
      )
   in
   let merge ((candidates:((lift_candidate_t) list list)),
              (terms:C.expr_t list)): C.expr_t = 
      CalcRing.mk_prod ((List.map candidate_term (List.flatten candidates)) @
                        terms)
   in
   let rec rcr (scope:var_t list) (expr:C.expr_t):
               ((lift_candidate_t) list * C.expr_t) = 
      let rcr_merge x = merge (List.split [rcr scope x]) in
      begin match expr with
         | CalcRing.Sum(sl) -> 
            ([], (CalcRing.mk_sum (List.map rcr_merge sl)))
         | CalcRing.Prod([])    -> ([], CalcRing.one)
         | CalcRing.Prod(pt::pl) ->
            let pt_ovars = (snd (schema_of_expr pt)) in
            let entering_scope = (ListAsSet.diff pt_ovars scope) in
            let rhs_scope = (ListAsSet.union scope pt_ovars) in
            let (rhs_eqs, rhs_term) = rcr rhs_scope (CalcRing.mk_prod pl) in
            let (lhs_eqs, lhs_term) = rcr scope pt in
            (* There are three possibilities for each equality x = y in rhs_eqs:
               1 - (x=y) commutes wiith lhs_term.  In this case, we keep 
                   shifting it left by returning it as an equality
               2 - (x=y) doesn't commute with lhs_term because:
                     a - x would fall out of scope, but all variables in y 
                         remain in scope.
                     b - y is a single variable and would fall out of scope 
                         while x remains in scope.
                   If either of these is true, we can convert the equality to a
                   lift expression appearing to the left of lhs_term in the 
                   product
               3 - All other cases.  The equality can not be converted into a
                   lift expression and must be re-inserted into the product 
                   terms to the right of lhs_term.  TODO: It might be possible  
                   to rewrite (x=y) in such a way that it falls into case 2.
                   
               There is, of course a shortcut.  If nothing enters scope in this
               expression, then we can always commute anything past it
            *)
            if entering_scope = [] then (lhs_eqs @ rhs_eqs, 
                                         CalcRing.mk_prod [lhs_term; rhs_term])
            else
            let (commuting_eqs, updated_lhs) = 
               List.fold_left (fun (commuting_eqs, updated_lhs) candidate ->
                  begin match candidate with
                  | UnidirectionalLift(x, y) ->
                     let y_enters_scope = 
                        let y_vars = (vars_of_value y) in
                           (List.exists (fun v -> List.mem v entering_scope)
                                        y_vars) 
                           && (y_vars <> [])
                     in
                     if y_enters_scope then
                        (* If y enters scope here, then it's not possible to
                           lift this expression (for now, this is true even if x 
                           doesn't enter scope here) *)
                        (  commuting_eqs, 
                           CalcRing.mk_prod [updated_lhs; 
                                             candidate_term candidate])
                     else if List.mem x entering_scope then
                        (* x enters scope here, but y does not.  We can
                           lift this equality *)
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              CalcRing.mk_val (
                                 Lift(x, CalcRing.mk_val (Value(y))));
                              updated_lhs])
                     else
                        (* Neither x nor y enter scope here.  We commute the
                           equality further. *)
                        (  commuting_eqs @ [candidate], updated_lhs  )
                  | BidirectionalLift(x, y) ->
                     begin match (List.mem x entering_scope, 
                            List.mem y entering_scope) with
                     | (true, true) -> (* x and y enter scope: abort *)
                        (  commuting_eqs, 
                           CalcRing.mk_prod [updated_lhs; 
                                             candidate_term candidate])
                     | (false, true) -> (* y enters scope, x does not: 
                                           lift into y *)
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              CalcRing.mk_val (
                                 Lift(y, CalcRing.mk_val (Value(mk_var x))));
                              updated_lhs])
                     | (true, false) -> (* x enters scope, y does not: 
                                           lift into x *)
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              CalcRing.mk_val (
                                 Lift(x, CalcRing.mk_val (Value(mk_var y))));
                              updated_lhs])
                     | (false, false) -> (* neither enters scope: commute *)
                        (  commuting_eqs @ [candidate], updated_lhs  )
                     end
                  end
               ) ([],lhs_term) rhs_eqs
            in
               (lhs_eqs @ commuting_eqs, 
                CalcRing.mk_prod [updated_lhs; rhs_term])
         | CalcRing.Neg(nt) -> 
            let (eqs, term) = rcr scope nt in
               (eqs, CalcRing.mk_neg nt)
         | CalcRing.Val(AggSum(gb_vars, term)) ->
            ([], CalcRing.mk_val (AggSum(gb_vars, rcr_merge term)))
         | CalcRing.Val(Lift(v, term)) ->
            ([], CalcRing.mk_val (Lift(v, rcr_merge term)))
         | CalcRing.Val(Cmp(Eq, ValueRing.Val(AVar(x)), 
                                ValueRing.Val(AVar(y)))) ->
            ([BidirectionalLift(x, y)], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, ValueRing.Val(AVar(x)), y)) ->
            ([UnidirectionalLift(x, y)], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, x, ValueRing.Val(AVar(y)))) ->
            ([UnidirectionalLift(y, x)], CalcRing.one)
         | _ -> 
            ([], expr)
      end
   in
      merge (List.split [rcr scope big_expr])
;;

(******************************************************************************
 * unify_lifts schema expr
 *   Where possible, variables defined by Lifts are replaced by the lifted 
 *   expression.  If possible, the Lift term is removed.  We do this by
 *   unfolding products, substituting the lifted expression throughout.  It's 
 *   possible that such a substitution will not be possible: e.g., if we're 
 *   lifting an aggregate expression and the lifted variable appears in a 
 *   comparison.  If so, then we do not remove the Lift term.
 *
 *   The other thing that can prevent the removal of a lift is if the variable
 *   being lifted into is present in the output schema of the enclosing 
 *   expression.  In short, this step only unifies lifted variables in the 
 *   context of a single expression (product, etc...), and doesn't propagate
 *   the unified variable up through the AST.  
 *
 *   schema: The expected output variables of the expression being unified.  
 *           These variables will never be unified away.
 *   expr:   The calculus expression being processed
 ******************************************************************************)
let split_ctx (split_fn: value_t -> 'a option) (ctx:(var_t * C.expr_t) list): 
              (var_t list * (var_t * 'a) list) =
   List.fold_left (fun (unusable_ctx, usable_ctx) (v,e) ->
      begin match e with
         | CalcRing.Val(Value(e_val)) -> 
            begin match split_fn e_val with
               | Some(s) -> (unusable_ctx, (v,s)::usable_ctx)
               | None -> (v::unusable_ctx, usable_ctx)
            end
         | _ -> (v::unusable_ctx, usable_ctx)
      end
   ) ([],[]) ctx
;;
let unify_lifts (big_schema:var_t list) (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Unify Lifts: "^(C.string_of_expr big_expr) 
   );
   let split_values (ctx:(var_t * C.expr_t) list): 
                    (var_t list * (var_t * value_t) list) = 
      split_ctx (fun v -> Some(v)) ctx
   in
   let split_vars (ctx:(var_t * C.expr_t) list):
                  (var_t list * (var_t * var_t) list) = 
      split_ctx (fun v -> match v with
       | ValueRing.Val(AVar(var)) -> Some(var)
       | _ -> None
      ) ctx
   in
   let deletable_vars term_vars unusable_vars usable_vars =
      ListAsSet.union (List.map fst usable_vars)
                      (ListAsSet.diff unusable_vars term_vars)
   in
   let sub x = List.map (Function.apply_if_present x) in
   let rec rcr (schema:var_t list) (ctx:(var_t * C.expr_t) list) 
               (expr:C.expr_t): (var_t list * C.expr_t) =
      let ctx_vars = (List.map fst ctx) in
      Debug.print "LOG-UNIFY-LIFTS" (fun () ->
         "Context: ["^(ListExtras.string_of_list (fun (v,e) ->
            (fst v)^" := "^(C.string_of_expr e)
         ) ctx)^"]\nExpression:"^(C.string_of_expr expr)
      );
      begin match expr with
         | CalcRing.Sum(sl) -> 
            let (deletable_lifts, sum_terms) =
               List.split (List.map (rcr schema ctx) sl)
            in
               (  ListAsSet.multiinter (ctx_vars::deletable_lifts),
                  CalcRing.mk_sum sum_terms  )
         | CalcRing.Prod([]) -> (ctx_vars, CalcRing.one)
         | CalcRing.Prod((CalcRing.Val(Lift(v, term)))::pl) ->
            (* Output variables contained within the lifted expression might be
               used somewhere within pl.  Regardless of whether or not the
               variables get projected away above, they must be considered as
               part of the schema of this term.  *)
            let rhs_schema = 
               (C.schema_of_expr (CalcRing.mk_prod pl)) in
            let schema_extensions = 
               ListAsSet.inter
                  (snd (C.schema_of_expr (term)))
                  (ListAsSet.union (fst rhs_schema) (snd rhs_schema))
            in
            let (lhs_deletable, lhs_nested_term) = 
               rcr (schema@schema_extensions) ctx term in
            let ((rhs_deletable, rhs_term), lhs_term) = 
               (* If the variable is part of the output schema, then we're not
                  allowed to delete the lift.  Additionally, we do not want to
                  unify away full expressions, since doing so breaks an 
                  optimization that we're able to perform in the delta 
                  computation.  See CalculusDeltas.delta_of_expr's Lift case. *)
               if (not (List.mem v schema)) && (
                   (Debug.active "UNIFY-EXPRESSIONS") ||
                   (match term with CalcRing.Val(Value(_))->true |_->false)
                  )
               then
                  (* If the term isn't deletable, then we shouldn't try to unify
                     this variable at all. *)
                  let (rhs_deletable_optimistic, rhs_term_optimistic) = 
                     rcr schema ((v, lhs_nested_term)::ctx) 
                                (CalcRing.mk_prod pl)
                  in if (Debug.active "AGGRESSIVE-UNIFY") ||
                        (List.mem v rhs_deletable_optimistic)
                  then (
                     Debug.print "LOG-UNIFY-LIFTS" (fun () -> 
                        "Unify of "^(string_of_var v)^" succeeded"
                     );
                     ((ListAsSet.diff rhs_deletable_optimistic [v], 
                         rhs_term_optimistic),
                         CalcRing.one)
                  ) else (
                     Debug.print "LOG-UNIFY-LIFTS" (fun () -> 
                        "Not unifying "^(string_of_var v)^
                        " because an RHS term decided it was a bad idea"
                     );
                     ((rcr schema ctx (CalcRing.mk_prod pl)), 
                        (CalcRing.mk_val (Lift(v,lhs_nested_term))))
                  )
               else (
                  Debug.print "LOG-UNIFY-LIFTS" (fun () -> 
                     "Not unifying "^(string_of_var v)^
                     " because Lift is an expression or in the schema"
                  );
                  ((rcr schema ctx (CalcRing.mk_prod pl)),
                   (CalcRing.mk_val (Lift(v,lhs_nested_term))))
               )
            in
               (  ListAsSet.inter lhs_deletable rhs_deletable,
                  CalcRing.mk_prod [lhs_term; rhs_term] )
         | CalcRing.Prod(pt::pl) -> 
            let rhs_schema = 
               (C.schema_of_expr (CalcRing.mk_prod pl)) in
            let schema_extensions = 
               ListAsSet.inter
                  (snd (C.schema_of_expr pt))
                  (ListAsSet.union (fst rhs_schema) (snd rhs_schema))
            in
            let (lhs_deletable, lhs_term) = 
               rcr (schema@schema_extensions) ctx pt in
            let (rhs_deletable, rhs_term) = rcr schema ctx (CalcRing.mk_prod pl)
            in
               (  ListAsSet.inter lhs_deletable rhs_deletable, 
                  CalcRing.mk_prod [lhs_term; rhs_term])
         | CalcRing.Neg(nt) ->
            let (deletable, term) = rcr schema ctx nt in
               (deletable, CalcRing.mk_neg term)
         | CalcRing.Val(AggSum(gb_vars, as_term)) ->
            let (deletable, subbed_term) = rcr gb_vars ctx as_term in
               (deletable, (CalcRing.mk_val (AggSum(gb_vars, subbed_term))))
         | CalcRing.Val(Lift(v, l_term)) ->
            (* Standalone lifts extend the schema of the current expression, so
               it's difficult to figure out whether or not the variable can 
               actually be unified.  However if the variable we're lifting into 
               is not part of the schema, AND the term being lifted doesn't 
               contain any relations, then we can safely delete this lift term 
            *)
            if ((C.rels_of_expr l_term) = []) &&
               (not (List.mem v schema))
            then (
               Debug.print "LOG-UNIFY-LIFTS" (fun () -> 
                  "Allowed to unify standalone lift into "^(string_of_var v)
               ); (ctx_vars, CalcRing.one)
            ) else (
               let (deletable, new_l_term) = rcr schema ctx l_term in
                  (deletable, (CalcRing.mk_val (Lift(v, new_l_term))))
            )
         | CalcRing.Val(Value(ValueRing.Val(AVar(v)))) ->
            (  ctx_vars, 
               if List.mem_assoc v ctx then List.assoc v ctx
                                       else expr )
         | CalcRing.Val(Value(v)) ->
            let (noval_ctx, value_subs) = split_values ctx in
            let simplified_v = eval_partial ~scope:value_subs v in
               (  (deletable_vars (vars_of_value v) noval_ctx value_subs),
                  CalcRing.mk_val (Value(simplified_v)))
         | CalcRing.Val(Cmp(cmp_op, lhs, rhs)) ->
            let (noval_ctx, value_subs) = split_values ctx in
            let simplified_lhs = eval_partial ~scope:value_subs lhs in
            let simplified_rhs = eval_partial ~scope:value_subs rhs in
               (  (deletable_vars (ListAsSet.union (vars_of_value lhs)
                                                   (vars_of_value rhs))
                                  noval_ctx value_subs),
                  CalcRing.mk_val (Cmp(cmp_op, simplified_lhs, simplified_rhs)))
         | CalcRing.Val(Rel(reln, relv, relt)) ->
            let (novar_ctx, var_subs) = split_vars ctx in
               (  (deletable_vars relv novar_ctx var_subs),
                  CalcRing.mk_val (Rel(reln, sub var_subs relv, relt)))
         | CalcRing.Val(External(en, eiv, eov, et, em)) ->
            let (novar_ctx, var_subs) = split_vars ctx in
               (  (deletable_vars (ListAsSet.union eiv eov) novar_ctx var_subs),
                  CalcRing.mk_val 
                     (External(en, sub var_subs eiv, sub var_subs eov, et, em)))
      end
   in
      snd (rcr big_schema [] big_expr)
;;

(******************************************************************************
 * nesting_rewrites
 *   A hodgepodge of simple rewrite rules for nested expressions (i.e., 
 *   expressions nested within a Lift or AggSum.  Many of these have to do with
 *   lifting expressions out of the nesting.
 *   
 *   AggSum([...], A) = A
 *       IF all of the output variables of A are in the group-by variables of
 *       the AggSum
 *   
 *   AggSum([...], A + B + ...) = 
 *       AggSum([...], A) + AggSum([...], B) + AggSum([...], ...)
 *   
 *   AggSum([...], A * B) = A * AggSum([...], B) 
 *       IF all of the output variables of A are in the group-by variables of
 *       the AggSum
 *   
 *   AggSum([...], A * B) = B * AggSum([...], A) 
 *       IF all of the output variables of B are in the group-by variables of
 *       the AggSum AND B commutes with A.
 * 
 *   AggSum(GB1, AggSum(GB2, A)) = AggSum(GB1, A) 
 *       IF all of the output variables of A are in the group-by variables of
 *       the AggSum.
 *   
 *   AggSum([...], A) = A 
 *       IF A is a constant term (i.e., has no output variables)
 *
 *   Lift(X, Lift(Y, A) * B) = Lift(Y,A) * Lift(X, B)
 ******************************************************************************)
let rec nesting_rewrites (expr:C.expr_t) = 
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Nesting Rewrites: "^(C.string_of_expr expr) 
   );
   CalcRing.fold (CalcRing.mk_sum) (CalcRing.mk_prod) 
      (fun x -> CalcRing.mk_prod [CalcRing.mk_val (Value(mk_int (-1))); x])
      (fun e -> begin match e with
         | AggSum(gb_vars, unprocessed_subterm) ->
            let subterm = nesting_rewrites unprocessed_subterm in
            if (snd (C.schema_of_expr subterm)) = [] then subterm
            else
               begin match subterm with
                  | CalcRing.Sum(sl) ->
                     CalcRing.mk_sum 
                        (List.map (fun x-> CalcRing.mk_val
                                             (AggSum(gb_vars, x))) sl)
                  | CalcRing.Prod(pl) ->
                     let (unnested,nested) = 
                        List.fold_left (fun (unnested,nested) term->
                           if (C.commutes nested term) && 
                              (ListAsSet.subset (snd (C.schema_of_expr term)) gb_vars)
                           then (CalcRing.mk_prod [unnested; term], nested)
                           else (unnested, CalcRing.mk_prod [nested; term])
                        ) (CalcRing.one, CalcRing.one) pl in
                        let new_gb_vars = 
                           ListAsSet.inter gb_vars 
                                           (snd (C.schema_of_expr nested))
                        in
                           CalcRing.mk_prod 
                              [  unnested; 
                                 CalcRing.mk_val (AggSum(new_gb_vars, nested)) ]
                  | CalcRing.Val(AggSum(_, term)) ->
                     CalcRing.mk_val (AggSum(gb_vars, term))
                  | _ -> 
                     if ListAsSet.subset (snd (C.schema_of_expr subterm))
                                         gb_vars 
                     then subterm
                     else CalcRing.mk_val e
               end
         | Lift(v, unprocessed_subterm) ->
            let subterm = nesting_rewrites unprocessed_subterm in
            let (unnested,nested) = List.fold_left (fun (unnested,nested) term->
               begin match term with
                  | CalcRing.Val(Lift(_,_)) when C.commutes term nested -> 
                     (CalcRing.mk_prod [unnested; term], nested)
                  | _ -> 
                     (unnested, CalcRing.mk_prod [nested; term])
               end
            ) (CalcRing.one, CalcRing.one) (CalcRing.prod_list subterm) in
               CalcRing.mk_prod 
                  [unnested; CalcRing.mk_val (Lift(v, nested))]
         | _ -> CalcRing.mk_val e
      end)
      expr
;;

(******************************************************************************
 * factorize_polynomial
 *   Given an expression (A * B) + (A * C), factor out the A to get A * (B + C)
 *   Note that this works bi-directionally, we can factor terms off the front, 
 *   or off the back.  All that matters is whether we can commute the term 
 *   to wherever it needs to get factored.
 * 
 *   Most of the work happens in factorize_one_polynomial, which takes a list
 *   of monomials representing a list of terms and identifies a Calculus 
 *   expression that is equivalent to their sum but which has terms factorized
 *   out as possible.
 * 
 *   This process happens in several stages. 
 *   - First, we identify a set of candidate subterms that may be factorized out 
 *     of the right or left hand side of each term.
 *   - For each candidate we count how many terms it can be factorized out of.
 *   - We pick the candidate that can be factorized out of the most terms and
 *     delete the term from those terms.
 *   - We recur twice, once on the set of terms that were factorized, and once
 *     on the set of terms that weren't.
 ******************************************************************************)
let rec factorize_one_polynomial (scope:var_t list) (term_list:C.expr_t list) =
   Debug.print "LOG-FACTORIZE" (fun () ->
      "Factorizing Expression: ("^
      (ListExtras.string_of_list ~sep:" + " C.string_of_expr term_list)^
      ")["^(ListExtras.string_of_list fst scope)^"]"
   );
   if List.length term_list < 2 then CalcRing.mk_sum term_list
   else
   let (lhs_candidates,rhs_candidates) = 
      List.fold_left (fun (lhs_candidates, rhs_candidates) term -> 
         let (new_lhs, new_rhs) = List.split (ListExtras.scan_map (fun p t n ->
            if (t = CalcRing.one) then ([],[])
            else
               (  (if C.commutes ~scope:scope (CalcRing.mk_prod p) t
                   then [t] else []),
                  (if C.commutes ~scope:scope t (CalcRing.mk_prod n)
                   then [t] else [])  )
            ) (CalcRing.prod_list term))
         in
            (  lhs_candidates @ (List.flatten new_lhs),
               rhs_candidates @ (List.flatten new_rhs)  )
      ) ([],[]) term_list
   in
   if ((List.length lhs_candidates) < 1) && ((List.length rhs_candidates) < 1)
   then
      (* If there are no candidates, we're done here *)
      CalcRing.mk_sum term_list
   else
   let incr_mem term list = 
      if List.mem_assoc term list then
         (term, (List.assoc term list) + 1)::(List.remove_assoc term list)
      else list
   in
   let (lhs_counts,rhs_counts) =
      List.fold_left (fun old_counts product_term -> 
         List.fold_left (fun (lhs_counts,rhs_counts) term ->
               (  incr_mem term lhs_counts, 
                  incr_mem term rhs_counts)
         ) old_counts (CalcRing.prod_list product_term)
      ) (   List.map (fun x->(x,0)) lhs_candidates, 
            List.map (fun x->(x,0)) rhs_candidates
         ) term_list
   in
   let lhs_sorted = List.sort (fun x y -> compare (snd y) (snd x)) lhs_counts in
   let rhs_sorted = List.sort (fun x y -> compare (snd y) (snd x)) rhs_counts in
   if ((List.length lhs_sorted < 1) || (snd (List.hd lhs_sorted) = 1)) &&
      ((List.length rhs_sorted < 1) || (snd (List.hd rhs_sorted) = 1))
   then
      (* If both candidate lists are empty, or all non-empty lists have each 
         candidate appearing exactly in one term, then we can't factorize 
         anything.  We're done here. *)
      CalcRing.mk_sum term_list
   else
   (* We should never get a candidate that doesn't appear at least once.  This
      is most definitely a bug if it happens. *)
   if ((List.length lhs_sorted >= 1) && (snd (List.hd lhs_sorted) < 1)) ||
      ((List.length rhs_sorted >= 1) && (snd (List.hd rhs_sorted) < 1))
   then failwith "BUG: Factorize got a vanishing candidate"
   else
   let factorize_and_split selected validate_fn =
      List.fold_left (fun (factorized,unfactorized) term ->
         try 
            let (lhs_of_selected,rhs_of_selected) =
               ListExtras.split_at_pivot selected (CalcRing.prod_list term) in
            Debug.print "LOG-FACTORIZE" (fun () ->
               "Split "^(C.string_of_expr term)^" into "^
               (C.string_of_expr (CalcRing.mk_prod lhs_of_selected))^
               " * [...] * "^
               (C.string_of_expr (CalcRing.mk_prod rhs_of_selected))         
            );
            if validate_fn lhs_of_selected rhs_of_selected 
            then (   factorized@
                        [CalcRing.mk_prod (lhs_of_selected@rhs_of_selected)],
                     unfactorized   )
            else (factorized, unfactorized@[term])
         with Not_found ->
            (factorized, unfactorized@[term])
      ) ([],[]) term_list
   in
   let ((factorized, unfactorized), merge_fn) = 
      if (snd (List.hd rhs_sorted)) > (snd (List.hd lhs_sorted)) then
         let selected = (fst (List.hd rhs_sorted)) in
            Debug.print "LOG-FACTORIZE" (fun () ->
               "Factorizing "^(C.string_of_expr selected)^" from the RHS"
            );
            (  (factorize_and_split selected (fun _ rhs ->
                  C.commutes ~scope:scope selected (CalcRing.mk_prod rhs))), 
               (fun factorized -> CalcRing.mk_prod [factorized; selected])
            )
      else
         let selected = (fst (List.hd lhs_sorted)) in
            Debug.print "LOG-FACTORIZE" (fun () ->
               "Factorizing "^(C.string_of_expr selected)^" from the LHS"
            );
            (  (factorize_and_split selected (fun lhs _ ->
                  C.commutes ~scope:scope (CalcRing.mk_prod lhs) selected)), 
               (fun factorized -> CalcRing.mk_prod [selected; factorized])
            )
   in
      Debug.print "LOG-FACTORIZE" (fun () ->
         "Factorized: "^
            (ListExtras.string_of_list C.string_of_expr factorized)^"\n"^
         "Unfactorized: "^
            (ListExtras.string_of_list C.string_of_expr unfactorized)
      );
      (* Finally, recur. We might be able to pull additional terms out of the
         individual expressions.  Note that we're no longer able to pull out
         terms across the factorized/unfactorized boundary *)
      CalcRing.mk_sum [
         (merge_fn (factorize_one_polynomial scope factorized));
         (factorize_one_polynomial scope unfactorized)
      ]

let factorize_polynomial (scope:var_t list) (expr:C.expr_t): C.expr_t =
   C.rewrite ~scope:scope 
      (fun (scope,_) sum_terms -> factorize_one_polynomial scope sum_terms)
      (fun _ -> CalcRing.mk_prod)
      (fun _ -> CalcRing.mk_neg)
      (fun _ -> CalcRing.mk_val)
      expr

(******************************************************************************
 * optimize_expr
 *   Given an expression apply the above optimizations to it until a fixed point
 *   is reached.
 ******************************************************************************)
let optimize_expr ?(optimizations = [OptCombineValues; OptLiftEqualities; 
                                     OptUnifyLifts; OptNestingRewrites;
                                     OptFactorizePolynomial])
                  ((scope,schema):C.schema_t) (expr:C.expr_t): C.expr_t =
   let include_opt in_fn o new_fn = 
      let old_in_fn = !in_fn in
      in_fn := (  if List.mem o optimizations 
                  then (fun x -> old_in_fn (new_fn x))
                  else !in_fn  )
   in
   let fp_1 = ref (fun x -> x) in
      include_opt fp_1 OptLiftEqualities      (lift_equalities scope);
      include_opt fp_1 OptUnifyLifts          (unify_lifts schema);
      include_opt fp_1 OptNestingRewrites     (nesting_rewrites);
      include_opt fp_1 OptFactorizePolynomial (factorize_polynomial scope);
   let fp_2 = ref (fun x -> x) in
      include_opt fp_2 OptCombineValues       (combine_values);
   let rec fixedpoint f x = 
      Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
         "OPTIMIZING: "^(C.string_of_expr x) 
      );
      let new_x = f x in
         if new_x <> x then fixedpoint f new_x
         else new_x

   (* The combine optimization pushes ring operators into values.  Often this is 
      a good idea, especially once we start graph decomposition.  However, this 
      optimization sometimes conflicts with Unification and Factorization, and 
      generally provides less benefit than either of these.  Thus, we apply it 
      as separate, second stage operation. *)

   in (fixedpoint !fp_2 (fixedpoint !fp_1 expr))
 