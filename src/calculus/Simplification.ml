open Types
open Ring
open Arithmetic
open Calculus

module C = BasicCalculus
open C

let commutes ?(scope = []) (e1:C.expr_t) (e2:C.expr_t): 
             bool =
   let (_,ovar1) = C.schema_of_expr e1 in
   let (ivar2,_) = C.schema_of_expr e2 in
      (* Commutativity within a product is possible if and only if all input 
         variables on the right hand side do not enter scope on the left hand 
         side.  A variable enters scope in an expression if it is an output 
         variable, and not already present in the scope (which we're given as a 
         parameter).  *)
   (ListAsSet.inter (ListAsSet.diff ovar1 scope) ivar2) = []
;;
(******************************************************************************
 * combine_values expr
 *   Recursively merges together Value terms appearing in expr.  Ring operators 
 *   are pushed into the ValueRing, and expressions are evaluated to the fullest
 *   extent possible.  
 *
 *   expr: The calculus expression to be processed
 ******************************************************************************)
let rec combine_values (expr:C.expr_t): C.expr_t =
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
      (CalcRing.mk_neg)
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
 *   expression.  If possible, the Lift expression is removed.
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
let unify_lifts (big_schema:var_t list) (big_expr:C.expr_t) =
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
      begin match expr with
         | CalcRing.Sum(sl) -> 
            let (deletable_lifts, sum_terms) =
               List.split (List.map (rcr schema ctx) sl)
            in
               (  ListAsSet.multiinter (ctx_vars::deletable_lifts),
                  CalcRing.mk_sum sum_terms  )
         | CalcRing.Prod([]) -> (ctx_vars, CalcRing.one)
         | CalcRing.Prod((CalcRing.Val(Lift(v, term)))::pl) ->
            let (lhs_deletable, lhs_term) = rcr schema ctx term in
            let (rhs_deletable, rhs_term) = 
               rcr schema ((v, term)::ctx) (CalcRing.mk_prod pl)
            in
               if (not (List.mem v schema)) && (List.mem v rhs_deletable)
               then (ListAsSet.inter lhs_deletable rhs_deletable,
                     rhs_term )
               else (ListAsSet.inter lhs_deletable rhs_deletable,
                     CalcRing.mk_prod [(CalcRing.mk_val (Lift(v,term)));
                                       rhs_term])
         | CalcRing.Prod(pt::pl) -> 
            let (lhs_deletable, lhs_term) = rcr schema ctx pt in
            let (rhs_deletable, rhs_term) = rcr schema ctx (CalcRing.mk_prod pl)
            in
               (  ListAsSet.inter lhs_deletable rhs_deletable, 
                  CalcRing.mk_prod [lhs_term; rhs_term])
         | CalcRing.Neg(nt) ->
            let (deletable, term) = rcr schema ctx nt in
               (deletable, CalcRing.mk_neg term)
         | CalcRing.Val(AggSum(gb_vars, as_term)) ->
            let (deletable, term) = rcr gb_vars ctx as_term in
               (deletable, (CalcRing.mk_val (AggSum(gb_vars, as_term))))
         | CalcRing.Val(Lift(v, l_term)) ->
            let (deletable, term) = rcr schema ctx l_term in
               (deletable, (CalcRing.mk_val (Lift(v, l_term))))
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
      rcr big_schema [] big_expr
;;



