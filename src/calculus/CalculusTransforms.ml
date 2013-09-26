(**
   Tools for optimizing/simplifying Calculus Expressions
*)

open Type
open Constants
open Ring
open Arithmetic
open Calculus

(**/**)
module C = Calculus

let next_temp_name = 
   FreshVariable.declare_class "calculus/CalculusTransforms"
                               "calc_transform_temp_var"
let mk_temp_var term = 
   (next_temp_name (), C.type_of_expr term)
(**/**)
;;
(**
 [combine_values expr]
 
   recursively merges together Value terms appearing in expr.  Ring operators 
   are pushed into the ValueRing, and expressions are evaluated to the fullest
   extent possible.  Additionally, negations are converted to multiplication
   by -1.
   @param expr The calculus expression to be processed
 *)
let rec combine_values ?(aggressive=false) (expr:C.expr_t): C.expr_t =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Combine Values: "^(CalculusPrinter.string_of_expr expr) 
   );
   let rcr = combine_values in
   let merge merge_consts calc_op val_op e =
      let (val_list, calc_list) = List.fold_right (fun term (v, c) ->
         match term with
            | CalcRing.Val(Value(new_v)) -> (new_v :: v, c)
            | _                          -> (v, term :: c)
      ) e ([],[]) in
      if val_list = [] then calc_op calc_list
      else let val_term = 
         if aggressive then 
            C.mk_value (Arithmetic.eval_partial (val_op val_list))
         else
         (* If we let it run free, combine values would be a little too 
            aggressive.  Specifically consider the expression 
               R(A) * S(B) * (A + B)
            The (A+B) makes our lives harder, since now we can't materialize
            these two terms separately.  That is to say, using << >> as a
            materialization operator, we want to materialize this expression as
               << R(A) * A >> * << S(B) >> + << R(A) >> * << S(B) * B >>.
            Which is something that we'd get with the standard decomposition.
            However, once the values are combined, this is not possible.  Thus
            When we combine variables, we always respect the hypergraph, and 
            only merge connected components (terms without variables can be
            safely merged into one). 
            
            Thus, the default is to be nonaggressive, as follows:
            *)
         let (number_values, variable_values) = 
            List.partition (fun x -> (Arithmetic.vars_of_value x) = [])
                           val_list
         in
         let partitioned_variable_values = 
            HyperGraph.connected_components Arithmetic.vars_of_value
                                            variable_values
         in
         let final_term_components = 
            if (List.length partitioned_variable_values > 0) && merge_consts
            then (number_values @ (List.hd partitioned_variable_values)) ::
                     (List.tl partitioned_variable_values)
            else number_values :: partitioned_variable_values
         in
            calc_op (List.map (fun val_list -> C.mk_value (
               Arithmetic.eval_partial (val_op val_list)
            )) (final_term_components))
      in      
      if calc_list = [] then val_term 
      else calc_op (calc_list @ [val_term])
   in
   CalcRing.fold
      (merge true  CalcRing.mk_sum  ValueRing.mk_sum)
      (merge false CalcRing.mk_prod ValueRing.mk_prod)
      (fun x -> CalcRing.mk_prod [ C.mk_value (Arithmetic.mk_int (-1)); 
                                   x ])
      (fun lf -> (match lf with
         | Cmp((Neq|Lt|Gt ),x,y) when x = y  -> 
         (* Zero is contageous.  This particular case following case wreaks 
            havoc on the schema of the expression being optimized.  Fortunately 
            though, the presence of Exists makes this point moot.  Since lifts
            can't extend the schema, and since Exists(0) = 0, the zero should
            either only affect subexpressions where the variables in question 
            are already in-scope, or should be able to propagate further up and
            kill anything that would have otherwise been able to bind the 
            variable.
            
            Even so, there's a flag to disable this.
         *)
            if Debug.active "CALC-DONT-CREATE-ZEROES" 
            then CalcRing.mk_val lf else CalcRing.zero
         
         | Cmp((Eq|Gte|Lte),x,y) when x = y -> CalcRing.one
         | Cmp(op,x,y) ->
            (* Pre-Evaluate comparisons wherever and as much as possible *)
            begin match ((Arithmetic.eval_partial x),
                         (Arithmetic.eval_partial y)) with
               | (ValueRing.Val(AConst(x_const)),
                  ValueRing.Val(AConst(y_const))) ->
                     begin match (Constants.Math.cmp op x_const y_const) with
                        | CBool(true) -> CalcRing.one
                        | CBool(false) -> 
                           if Debug.active "CALC-DONT-CREATE-ZEROES" 
                           then C.mk_cmp op 
                                   (Arithmetic.mk_const(x_const))
                                   (Arithmetic.mk_const(y_const))
                           else CalcRing.zero
                        | _ -> C.bail_out (CalcRing.mk_val lf)
                                 "Unexpected return value of comparison op"
                     end
               | (x_val, y_val) -> 
                  let ret = C.mk_cmp op x_val y_val in
                     Debug.print "LOG-COMBINE-VALUES" (fun () ->
                        "Combining "^(C.string_of_leaf lf)^" into "^
                        (C.string_of_expr ret)
                     ); ret
            end
         
         | Value(v) -> C.mk_value (Arithmetic.eval_partial v)
         | Rel(_,_) | External(_) -> CalcRing.mk_val lf
         | AggSum(gb_vars, subexp) -> 
            let new_subexp = rcr subexp in
            if new_subexp = CalcRing.zero then CalcRing.zero else
               C.mk_aggsum gb_vars new_subexp
         | Lift(lift_v, subexp)    -> 
            C.mk_lift lift_v (rcr subexp)
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp)    -> 
            let new_subexp = rcr subexp in
            if new_subexp = CalcRing.zero then CalcRing.zero else
               C.mk_exists new_subexp
(***** END EXISTS HACK *****)
      ))
      expr
;;

(**/**)
(** Type used internally by lift_equalities to describe liftable equalities. *)
type lift_candidate_t = 
   (* A var=var equality that can be lifted in either direction *)
   | BidirectionalLift of var_t * var_t
   (* An equality that can only be lifted one way *)
   | UnidirectionalLift of var_t * value_t
(**/**)
(**
  [lift_equalities scope expr]
  
    Recursively commutes equality comparison terms as far left through the 
    expression as possible.  Once the term commutes all the way to the left, 
    if it is possible to commute it further by converting it to a lift, this
    function does so.
 
    Because it's as good a place to do it as anywhere, lift_equalities will
    also delete obviously irrelevant equalities (i.e., equalities of the form 
    X=X will be replaced by 1).
 
    @param scope  Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
    @param expr   The calculus expression to be processed
*)
let lift_equalities (global_scope:var_t list) (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Lift Equalities: "^(CalculusPrinter.string_of_expr big_expr) 
   );
   let candidate_term (local_scope:var_t list) (candidate:lift_candidate_t) = 
      match candidate with
         | BidirectionalLift(x, y)  -> 
            if (List.mem x local_scope)
            then if (List.mem y local_scope)
                 then C.mk_cmp Eq (Arithmetic.mk_var x) 
                                         (Arithmetic.mk_var y)
                 else C.mk_lift y 
                         (C.mk_value (Arithmetic.mk_var x))
            else if (List.mem y local_scope)
                 then C.mk_lift x
                         (C.mk_value (Arithmetic.mk_var y))
                 else (Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
                        "Scope of error is : " ^
                        (ListExtras.ocaml_of_list string_of_var local_scope)
                      );
                      C.bail_out big_expr 
                         "Error: lifted equality past scope of both vars")
         | UnidirectionalLift(x, y) -> 
            if not (List.for_all (fun y_var -> List.mem y_var local_scope)
                                 (Arithmetic.vars_of_value y))
            then (Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
                        "Scope of error is : " ^
                        (ListExtras.ocaml_of_list string_of_var local_scope)
                      );
                 failwith "Error: lifted equality past scope of value")
            else if (List.mem x local_scope)
                 then C.mk_cmp Eq (Arithmetic.mk_var x) y
                 else C.mk_lift x (C.mk_value y)
   in
   let merge (local_scope:var_t list)
             ((candidates:((lift_candidate_t) list list)),
              (terms:C.expr_t list)): C.expr_t = 
      CalcRing.mk_prod ((List.map (candidate_term local_scope)
                                  (List.flatten candidates)) @
                        terms)
   in
   let rec rcr (scope:var_t list) (expr:C.expr_t):
               ((lift_candidate_t) list * C.expr_t) = 
      Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
         "Lift Equalities: "^
         (CalculusPrinter.string_of_expr expr)^
         "\n\t\tWith Scope: "^
         (ListExtras.ocaml_of_list string_of_var scope)
      );
      let rcr_merge x = merge scope (List.split [rcr scope x]) in
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
                     if y_enters_scope then (
                        (* If y enters scope here, then it's not possible to
                           lift this expression (for now, this is true even if x
                           doesn't enter scope here) *)
                        Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                           "Aborting search for unidirectional lift "^
                           (string_of_var x)^" ^= "^
                              (CalculusPrinter.string_of_value y)^
                           " due to scope"
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod 
                              [updated_lhs; 
                               candidate_term rhs_scope candidate])
                     ) else if List.mem x entering_scope then (
                        (* x enters scope here, but y does not.  We can
                           lift this equality *)
                        Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                           "Found lift location for unidirectional lift "^
                           (string_of_var x)^" ^= "^
                              (CalculusPrinter.string_of_value y)^" in:\n"^
                              (CalculusPrinter.string_of_expr updated_lhs)
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              C.mk_lift x (C.mk_value y);
                              updated_lhs])
                     ) else
                        (* Neither x nor y enter scope here.  We commute the
                           equality further. *)
                        (  commuting_eqs @ [candidate], updated_lhs  )
                  | BidirectionalLift(x, y) ->
                     begin match (List.mem x entering_scope, 
                            List.mem y entering_scope) with
                     | (true, true) -> (* x and y enter scope: abort *)
                        Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                           "Aborting search for bidirectional lift "^
                           (string_of_var x)^" ^=^ "^(string_of_var y)^
                           " due to simultaneous scope entrance"
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod 
                              [updated_lhs; 
                               candidate_term rhs_scope candidate])
                     | (false, true) -> (* y enters scope, x does not: 
                                           lift into y *)
                        Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                           "Found lift location for bidirectional lift "^
                           (string_of_var y)^" ^= "^(string_of_var x)^
                              " in:\n"^
                              (CalculusPrinter.string_of_expr updated_lhs)
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              C.mk_lift y 
                                 (C.mk_value (Arithmetic.mk_var x));
                              updated_lhs])
                     | (true, false) -> (* x enters scope, y does not: 
                                           lift into x *)
                        Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                           "Found lift location for bidirectional lift "^
                           (string_of_var x)^" ^= "^(string_of_var y)^
                              " in:\n"^
                              (CalculusPrinter.string_of_expr updated_lhs)
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              C.mk_lift x 
                                 (C.mk_value (Arithmetic.mk_var y));
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
            (** It's possible that we'll take advantage of a variable in-scope
                to unify some of the gb-vars away.  If this happens, we need
                to update the gb_vars *)
            let new_term = rcr_merge term in
            let new_gb_vars = ListAsSet.inter gb_vars
                                              (snd (C.schema_of_expr new_term))
            in
               ([], C.mk_aggsum new_gb_vars new_term)
         | CalcRing.Val(Lift(v, term)) ->
            ([], C.mk_lift v (rcr_merge term))
(***** BEGIN EXISTS HACK *****)
         | CalcRing.Val(Exists(term)) ->
            ([], C.mk_exists (rcr_merge term))
(***** END EXISTS HACK *****)
         | CalcRing.Val(Cmp(Eq, x, y)) when x = y ->
            (* X = X is a no-op *)
            ([], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, ValueRing.Val(AVar(x)), 
                                ValueRing.Val(AVar(y))))
               when (snd x) = (snd y) ->
            Debug.print "LOG-LIFT-EQUALITIES" (fun () -> 
               "Bidirectional lift candidate "^(string_of_var x)^" ^=^ "^
               (string_of_var y)
            );
            ([BidirectionalLift(x, y)], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, ValueRing.Val(AVar(x)), y)) 
               when (Type.can_escalate_type (type_of_value y) (snd x)) ->
            Debug.print "LOG-LIFT-EQUALITIES" (fun () -> 
               "Unidirectional lift candidate "^(string_of_var x)^" ^= "^
               (CalculusPrinter.string_of_value y)
            );
            ([UnidirectionalLift(x, y)], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, x, ValueRing.Val(AVar(y))))
               when (Type.can_escalate_type (type_of_value x) (snd y)) ->
            Debug.print "LOG-LIFT-EQUALITIES" (fun () -> 
               "Unidirectional lift candidate "^(string_of_var y)^" ^= "^
               (CalculusPrinter.string_of_value x)
            );
            ([UnidirectionalLift(y, x)], CalcRing.one)
         | CalcRing.Val(Cmp _) 
         | CalcRing.Val(External _)
         | CalcRing.Val(Rel _)
         | CalcRing.Val(Value _) -> 
            ([], expr)
      end
   in
      merge global_scope (List.split [rcr global_scope big_expr])
;;

(**/**)
(** Thrown when a potential unification would fail in order to back out of 
    unification and undo any changes that have been made.  The one field is the 
    reason for the failure. *)
exception CouldNotUnifyException of string;;
(**/**)

(**
  [unify_lifts scope schema expr]
  
   Where possible, variables defined by Lifts are replaced by the lifted 
   expression.  If possible, the Lift term is removed.  We do this by
   unfolding products, substituting the lifted expression throughout.  It's 
   possible that such a substitution will not be possible: e.g., if we're 
   lifting an aggregate expression and the lifted variable appears in a 
   comparison.  If so, then we do not remove the Lift term.

   The other thing that can prevent the removal of a lift is if the variable
   being lifted into is present in the output schema of the enclosing 
   expression.  In short, this step only unifies lifted variables in the 
   context of a single expression (product, etc...), and doesn't propagate
   the unified variable up through the AST.  

   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param schema  The expected output variables of the expression 
                  being unified.
                  These variables will never be unified away
   @param expr    The calculus expression being processed
*)
let unify_lifts (big_scope:var_t list) (big_schema:var_t list) 
                (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      let (in_sch, out_sch) = C.schema_of_expr big_expr in
      "Unify Lifts: "^
      (ListExtras.ocaml_of_list string_of_var in_sch)^
      (ListExtras.ocaml_of_list string_of_var out_sch)^": \n"^
      (CalculusPrinter.string_of_expr big_expr) 
   );
   let unify (force:bool) (lift_v:var_t) (expr_sub:C.expr_t) (scope:var_t list) 
             (schema:var_t list) (expr:C.expr_t): C.expr_t =
      Debug.print "LOG-UNIFY-LIFTS" (fun () ->
         "Attempting to unify ("^(string_of_var lift_v)^" ^= "^
         (CalculusPrinter.string_of_expr expr_sub)^") with scope/schema "^
         (ListExtras.ocaml_of_list string_of_var scope)^
         (ListExtras.ocaml_of_list string_of_var schema)^"\nin: "^
         (CalculusPrinter.string_of_expr expr)
      );
      let var_sub msg=
         match expr_sub with 
         | CalcRing.Val(Value(ValueRing.Val(AVar(v)))) -> v
         | _ when force -> lift_v
         | _ -> raise (CouldNotUnifyException("Could not put '"^
                   (CalculusPrinter.string_of_expr expr_sub)^"' into a"^msg))
      in
      let val_sub ?(aggressive = false) msg =
         match (combine_values ~aggressive:aggressive expr_sub) with 
         | CalcRing.Val(Value(v)) -> v
         | _ when force -> Arithmetic.mk_var lift_v
         | _ -> raise (CouldNotUnifyException("Could not put '"^
                   (CalculusPrinter.string_of_expr expr_sub)^"' into a"^msg))
      in
      let map_vars msg = 
         List.map (fun x -> if x = lift_v then var_sub msg else x)
      in
      let make_cmp msg =
         let subexp_v = var_sub msg in
         C.mk_cmp Eq (Arithmetic.mk_var lift_v) (Arithmetic.mk_var subexp_v)
      in
      (* If the lift variable is in the schema, then we can't unify it --
         Otherwise, we'd shrink the schema.  In some cases, a unification of 
         this sort is actually possible, but we don't handle them here.  
          - If the schema is defined by an AggSum, nested_rewrites will pull
            the Lift out of the AggSum (and update the schema accordingly)
          - If the schema is based on the expression itself, we have to handle
            things on a case-by-case basis, because whatever is going to be 
            evaluating the expression has to be made aware of the schema 
            changes.  For example, in Compiler, this is handled by 
            extract_renamings. *)
      if (not force) && List.mem lift_v schema then 
         raise (CouldNotUnifyException("Lift var is in the schema"));
      
      (* If the lift variable is already in-scope, then we can't unify it, 
         because it's serving the role of an equality predicate.  We could try 
         to turn it into an equality predicate here, but that's unnecessary.
          - If the term that brought the lift variable into scope is itself a
            lift, then we could only have gotten to this point if that lift was
            not unifiable for other reasons.
          - Otherwise, the next pass through advance_lifts and/or 
            nesting_rewrites will move this lift up and left, and then we should
            be able to unify it normally without trouble. *)
      if (not force) && List.mem lift_v scope then
         raise (CouldNotUnifyException("Lift var is in the scope"));

      (* We unify by replacing expressions with CalcRing.one.  This is only 
         correct if the lifted expression is a singleton.  For example, we 
         could implement COUNT(UNIQUE A) as 
            AggSum([], (foo ^= R(A)) * {foo > 0})
         The nested expression cannot be safely replaced.  However, if we get:
            (A ^= ...) * AggSum([], (foo ^= R(A)) * {foo > 0})
         then the nested aggregate is always over a single element. *)
      if not (C.expr_is_singleton ~scope:scope expr_sub) then (
         if force then expr else
         raise (CouldNotUnifyException("Can only unify lifts of singletons"));
      ) else
      
      let rewritten = 
      C.rewrite_leaves (fun _ x -> begin match x with
         (* No Arithmetic over lone values means they can take non-float/int
            values. *)
         | Value(ValueRing.Val(AVar(v))) when v = lift_v -> expr_sub
         | Value(v) when List.mem lift_v (Arithmetic.vars_of_value v) -> 
            C.mk_value(
               Arithmetic.eval_partial 
                  ~scope:[lift_v, val_sub " value expression"] v
            )
         | Value(_) -> CalcRing.mk_val x
         | AggSum(gb_vars, subexp) ->
            (* subexp is rewritten.  We just need to update gb_vars properly.
               This can only change (in this unify function) if the lhs variable
               appears in the GB terms.  
                  - If lift_v is bound in the expression, then subst may not be
                    bound if lift_v is bound by a lift.  E.g.  
                       (A ^= X) * AggSum([A], (A ^= X) * ...) -> 
                          AggSum([], ...)
                  - If the variable is not bound in the expression, but the 
                    subst is (and is a variable), then the new subst had better 
                    already be in the group-by variables.  
            *)
            let mapped_gb_vars = map_vars "n aggsum group-by var" gb_vars in
            let new_gb_vars = 
               ListAsSet.inter mapped_gb_vars (snd (C.schema_of_expr subexp))
            in            
               C.mk_aggsum new_gb_vars subexp
         | Rel(rn, rv) ->           
            let new_rv = map_vars " relation var" rv in
            if ListAsSet.has_no_duplicates new_rv
            then C.mk_rel rn new_rv
            else 
               (* In order to prevent expressions of the form R(dA,dA), *)
               (* we transform (B^=dA)*R(dA,B) into R(dA,B)*(B=dA).     *)
               CalcRing.mk_prod [ C.mk_rel rn rv; 
                                  make_cmp " relation var" ]
         | External(en, eiv, eov, et, em) ->
            (* The metadata is already rewritten *)
            let new_eiv = map_vars "n external input var" eiv in
            let new_eov = map_vars "n external output var" eov in
            if ListAsSet.has_no_duplicates new_eiv &&
               ListAsSet.has_no_duplicates new_eov
            then C.mk_external en new_eiv new_eov et em
            else
            
            (* In order to prevent expressions of the form M[][dA,dA], *)
            (* we transform (B^=dA)*M[][dA,B] into M[][dA,B]*(B=dA).   *)
            CalcRing.mk_prod [ 
               C.mk_external en (if ListAsSet.has_no_duplicates new_eiv 
                                 then new_eiv else eiv) 
                                (if ListAsSet.has_no_duplicates new_eov 
                                 then new_eov else eov) et em;
               make_cmp " n external var" ]
         | Cmp(op, lhs, rhs) when 
            (List.mem lift_v (Arithmetic.vars_of_value lhs)) ||
            (List.mem lift_v (Arithmetic.vars_of_value rhs)) ->
            let new_lhs = Arithmetic.eval_partial 
                     ~scope:[lift_v, val_sub " cmp expression"] lhs 
            in
            let new_rhs = Arithmetic.eval_partial 
                     ~scope:[lift_v, val_sub " cmp expression"] rhs 
            in
               C.mk_cmp op new_lhs new_rhs

         | Cmp(op, lhs, rhs) -> C.mk_cmp op lhs rhs


(***** BEGIN EXISTS HACK *****)
         | Exists(subexp) -> C.mk_exists subexp
            (* Subexp is rewritten.  No changes need to be made here *)
(***** END EXISTS HACK *****)

         | Lift(v, subexp) when (v = lift_v) && (subexp = expr_sub) ->
            CalcRing.one
         | Lift(v, subexp) when (v = lift_v) ->
            (* If the subexpressions aren't equivalent, then we should turn this
               into an equality test on the two.  lift_equalities will do 
               something more intelligent later if possible.  Note that here, 
               for the sake of code simplicity we take a slight hit: we cannot
               do comparisons over expressions. *)
            begin match combine_values ~aggressive:true subexp with
               | _ when force -> C.mk_lift v subexp
               | CalcRing.Val(Value(subexp_v)) ->
                  C.mk_cmp 
                     Eq subexp_v (val_sub ~aggressive:true " lift comparison")
               | _ -> raise (CouldNotUnifyException("Conflicting Lift"))
            end
            (* the subexp is already rewritten *)
         | Lift(v, subexp) -> C.mk_lift v subexp
            
      end) expr
      in 
         Debug.print "LOG-UNIFY-LIFTS" (fun () ->
            "Successfully unified ("^(string_of_var lift_v)^" ^= "^
            (CalculusPrinter.string_of_expr expr_sub)^") in: "^
            (CalculusPrinter.string_of_expr expr)^"\nto: "^
            (CalculusPrinter.string_of_expr rewritten)
         ); rewritten

   in
   let rec rcr (scope:var_t list) (schema:var_t list) (expr:C.expr_t):C.expr_t =
      Debug.print "LOG-UNIFY-LIFTS" (fun () ->
         "Attempting to unify lifts in: "^
         (ListExtras.ocaml_of_list string_of_var scope)^
         (ListExtras.ocaml_of_list string_of_var schema)^"\n"^
         (CalculusPrinter.string_of_expr expr)
      );
      begin match expr with 
      | CalcRing.Val(Value(_))
      | CalcRing.Val(Rel(_,_)) 
      | CalcRing.Val(External(_)) 
      | CalcRing.Val(Cmp(_,_,_)) -> expr
      
(***** BEGIN EXISTS HACK *****)
      | CalcRing.Val(Exists(subexp)) -> 
         C.mk_exists (rcr scope schema subexp)
(***** END EXISTS HACK *****)
      
      | CalcRing.Val(AggSum(gb_vars, subexp)) ->
         let new_subexp = rcr scope gb_vars subexp in
         let (orig_ivars,_) = C.schema_of_expr subexp in
         let (_,new_ovars) = C.schema_of_expr new_subexp in
         (* Unifying lifts can do some wonky things to the schema of subexp.
            Among other things, previously unbound variables can become bound,
            and previously bound variables can become unbound.  A variable in
            the schema of an expression will never be unified (although the lift
            can be propagated up and out of the aggsum via nesting_rewrites) *)
         let new_gb_vars = 
            ListAsSet.union
               (ListAsSet.inter gb_vars new_ovars)
               (ListAsSet.inter orig_ivars new_ovars)
         in
            Debug.print "LOG-UNIFY-LIFTS" (fun () ->
               "Updating group-by variables from "^
               (ListExtras.ocaml_of_list string_of_var gb_vars)^" to "^
               (ListExtras.ocaml_of_list string_of_var new_gb_vars)^
               " in AggSum of: "^
               (CalculusPrinter.string_of_expr new_subexp)
            );
            C.mk_aggsum new_gb_vars new_subexp
      
      | CalcRing.Val(Lift(lift_v, subexp)) ->
         let subexp_schema = ListAsSet.diff schema [lift_v] in
         let new_subexp = rcr scope subexp_schema subexp in (
            try 
               unify false lift_v new_subexp scope schema CalcRing.one;
            with CouldNotUnifyException(msg) ->
               Debug.print "LOG-UNIFY-LIFTS" (fun () ->
                  "Could not unify "^(string_of_var lift_v)^" because: "^msg^
                  " in terminal lift"
               );
               C.mk_lift lift_v new_subexp
         )

      | CalcRing.Sum(sum_terms) ->
         (* This is a bit of a hack for now.  The schema of every term in the
            sum needs to be kept identical.  Factorize will pull out any lifts 
            shared across terms in the sum, and poly decomposition will allow
            unification within a sum.  *)
         let (_,sum_schema) = C.schema_of_expr expr in
         let full_schema = ListAsSet.union sum_schema schema in
            CalcRing.mk_sum (List.map (rcr scope full_schema) sum_terms)
      
      | CalcRing.Neg(neg_term) ->
         CalcRing.mk_neg ((rcr scope schema) neg_term)
      
      | CalcRing.Prod(CalcRing.Val(Lift(lift_v, subexp))::rest) ->
         let subexp_schema = ListAsSet.diff schema [lift_v] in
         let new_subexp = rcr scope subexp_schema subexp in (
            try
               rcr scope schema (unify false lift_v new_subexp scope schema 
                                       (CalcRing.mk_prod rest))
            with CouldNotUnifyException(msg) -> (
               Debug.print "LOG-UNIFY-LIFTS" (fun () ->
                  "Could not unify "^(string_of_var lift_v)^" because: "^msg^
                  " in: \n"^(CalculusPrinter.string_of_expr 
                                    (CalcRing.mk_prod rest))
               );
               let simplified_rest = (
                  if Debug.active "AGGRESSIVE-UNIFICATION" then
                     unify true lift_v new_subexp scope schema 
                           (CalcRing.mk_prod rest)
                  else CalcRing.mk_prod rest
               ) in
               let (_,subexp_ovars) = C.schema_of_expr new_subexp in
               let new_scope = 
                  ListAsSet.multiunion [scope; [lift_v]; subexp_ovars]
               in
               let new_schema = 
                  ListAsSet.diff schema new_scope
               in
                  CalcRing.mk_prod [
                     C.mk_lift lift_v new_subexp;
                     rcr new_scope new_schema simplified_rest
                  ]
            )
         )

      | CalcRing.Prod(head::rest) ->
         let (rest_ivars, rest_ovars) = 
            C.schema_of_expr (CalcRing.mk_prod rest) 
         in
         let head_schema = 
            (ListAsSet.multiunion [schema; rest_ivars; rest_ovars]) 
         in
         let new_head = rcr scope head_schema head in
         let (_, head_ovars) = C.schema_of_expr new_head in
         let new_scope = ListAsSet.union scope head_ovars in
         let new_schema = ListAsSet.diff schema head_ovars in
            CalcRing.mk_prod [
               new_head; 
               rcr new_scope new_schema (CalcRing.mk_prod rest)
            ]

      | CalcRing.Prod([]) -> CalcRing.one
   end in 
      rcr big_scope big_schema big_expr

(**
  [advance_lifts scope expr]
  
    Move lifts as far to the left as possible -- The current implementation is 
    very heuristic, and can probably be replaced by something more effective.  
    For now though, something simple should be sufficient.

   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param expr    The calculus expression being processed
*)
let advance_lifts scope expr =
   Debug.print "LOG-CALCOPT-DETAIL" (fun () -> 
      "Advance Lifts: "^(CalculusPrinter.string_of_expr expr));
   C.rewrite ~scope:scope (fun _ x -> CalcRing.mk_sum x)
   (fun (scope, _) pl ->
      Debug.print "LOG-ADVANCE-LIFTS" (fun () -> 
         "Advance Lifts: processing " ^
         (CalculusPrinter.string_of_expr (CalcRing.mk_prod pl)));
      CalcRing.mk_prod (
         List.fold_left (fun curr_ret curr_term ->
            begin match curr_term with
               | CalcRing.Val(Lift(_,_)) -> 
                  begin match ( 
                     ListExtras.scan_fold (fun ret_term lhs rhs_hd rhs_tl ->
                        if ret_term <> None then ret_term else
                        let local_scope = 
                           ListAsSet.union scope
                              (snd (C.schema_of_expr (CalcRing.mk_prod lhs)))
                        in
                        if C.commutes ~scope:local_scope 
                                      (CalcRing.mk_prod (rhs_hd::rhs_tl))
                                      curr_term
                        then Some(lhs@[curr_term;rhs_hd]@rhs_tl)
                        else None
                     ) None curr_ret
                  ) with
                     |  Some(s) -> s
                     |  None    -> curr_ret @ [curr_term]
                  end
               | _ -> curr_ret @ [curr_term]
            end
         ) [] pl
      )
   )
   (fun _ x -> CalcRing.mk_neg x)
   (fun _ x ->
         begin match x with
            | AggSum(gb_vars, subexp) ->
               (* Advance lifts can turn output variables into input variables:
                  e.g., for R(A) * AggSum([A], S(A) * (A ^= 2)) *)
               let new_gb_vars = 
                  ListAsSet.inter gb_vars (snd (C.schema_of_expr subexp))
               in
                  C.mk_aggsum new_gb_vars subexp
            | _ -> CalcRing.mk_val x
         end)
   expr
;;

(**
  [nesting_rewrites expr]

    A hodgepodge of simple rewrite rules for nested expressions (i.e., 
    expressions nested within a Lift or AggSum.  Many of these have to do with
    lifting expressions out of the nesting.
    
    [AggSum([...], 0) = 0]
    
    [AggSum([...], A) = A]
        IF all of the output variables of A are in the group-by variables of
        the AggSum
    
    [AggSum([...], A + B + ...) = 
        AggSum([...], A) + AggSum([...], B) + AggSum([...], ...)]
    
    [AggSum([...], A * B) = A * AggSum([...], B)]
        IF all of the output variables of A are in the group-by variables of
        the AggSum
    
    [AggSum([...], A * B) = B * AggSum([...], A)]
        IF all of the output variables of B are in the group-by variables of
        the AggSum AND B commutes with A.
  
    [AggSum(GB1, AggSum(GB2, A)) = AggSum(GB1, A)]
        IF all of the output variables of A are in the group-by variables of
        the AggSum.
    
    [AggSum([...], A) = A]
        IF A is a constant term (i.e., has no output variables)
    
    [Lift(A, A) = 1]
    
    [Lift(A, f(A)) => [A = f(A)]] (or equivalent)

   @param expr    The calculus expression being processed
*)
let rec nesting_rewrites (expr:C.expr_t) = 
   Debug.print "LOG-CALCOPT-DETAIL" (fun () ->
      "Nesting Rewrites: "^(CalculusPrinter.string_of_expr expr) 
   );
   CalcRing.fold (CalcRing.mk_sum) (CalcRing.mk_prod) 
      (CalcRing.mk_neg)
      (fun e -> 
      Debug.print "LOG-NESTING-REWRITES" (fun () ->
         "Nesting Rewrites on Leaf: "^
         (CalculusPrinter.string_of_expr (CalcRing.mk_val e))
      );
      begin match e with
         | AggSum(gb_vars, x) when x = CalcRing.zero -> CalcRing.zero
         | AggSum(gb_vars, unprocessed_subterm) ->
            let subterm = nesting_rewrites unprocessed_subterm in
            if (snd (C.schema_of_expr subterm)) = [] then subterm
            else
               begin match subterm with
                  | CalcRing.Sum(sl) ->
                     (* Input variables might be bound some but not all of the 
                        sum terms.  We need to update the aggsum accordingly *)
                     let (sum_ivars, _) = C.schema_of_expr subterm in
                     let rewritten = 
                        CalcRing.mk_sum 
                           (List.map (fun term -> 
                              let (_,term_ovars) = C.schema_of_expr term in
                              Debug.print "LOG-NESTINGREWRITES-DETAIL" (fun ()->
                                 "Rewriting aggsum term with ovars:"^
                                 (ListExtras.ocaml_of_list string_of_var 
                                                            term_ovars)^":\n"^
                                 (CalculusPrinter.string_of_expr term)
                              );
                              let term_gb_vars = 
                                 ListAsSet.union gb_vars
                                    (ListAsSet.inter sum_ivars term_ovars)
                              in
                                 C.mk_aggsum term_gb_vars term
                           ) sl)
                     in
                     Debug.print "LOG-NESTINGREWRITES-DETAIL" (fun () ->
                        "Rewrote : AggSum("^
                        (ListExtras.ocaml_of_list string_of_var gb_vars)^", \n"^
                        (CalculusPrinter.string_of_expr subterm)^
                        "\n) with ivars: "^
                        (ListExtras.ocaml_of_list string_of_var sum_ivars)^
                        "\nto: "^
                        (CalculusPrinter.string_of_expr rewritten)
                     );
                     rewritten
                  | CalcRing.Prod(pl) ->
                     let (unnested,nested) = 
                        List.fold_left (fun (unnested,nested) term->
                           if (C.commutes nested term) && 
                              (ListAsSet.subset (snd (C.schema_of_expr term))
                                                gb_vars)
                           then (CalcRing.mk_prod [unnested; term], nested)
                           else (unnested, CalcRing.mk_prod [nested; term])
                        ) (CalcRing.one, CalcRing.one) pl in
                        let new_gb_vars = 
                           ListAsSet.inter gb_vars 
                                           (snd (C.schema_of_expr nested))
                        in
                           Debug.print "LOG-NESTINGREWRITES-DETAIL" (fun () ->
                              "Nesting rewrites lifting out:\n"^
                              (string_of_expr unnested)^
                              "\n\tand keeping in:\n"^
                              (string_of_expr nested)^
                              "\n\twith output variables : "^
                              (ListExtras.ocaml_of_list fst 
                                    (snd (C.schema_of_expr nested)))^
                              "\n\tMaking the new group-by variables:\n"^
                              (ListExtras.ocaml_of_list fst new_gb_vars)
                           );
                           CalcRing.mk_prod 
                              [  unnested; 
                                 C.mk_aggsum new_gb_vars nested ]
                  | CalcRing.Val(AggSum(_, term)) ->
                     C.mk_aggsum gb_vars term
                  | _ -> 
                     if ListAsSet.subset (snd (C.schema_of_expr subterm))
                                         gb_vars 
                     then subterm
                     else C.mk_aggsum gb_vars subterm
               end
         
(***** BEGIN EXISTS HACK *****)
         | Exists(unprocessed_subexp) -> 
            begin match (nesting_rewrites unprocessed_subexp) with 
               | CalcRing.Val(Value(ValueRing.Val(AConst(CInt(0))))) ->
                  CalcRing.zero
               | CalcRing.Val(Value(ValueRing.Val(AConst(CInt(_))))) ->
                  CalcRing.one
               | CalcRing.Val(Value(ValueRing.Val(AConst(_)))) ->
                  C.bail_out (CalcRing.mk_val e) 
                     "Exists with a non-integer value"
               | subexp -> C.mk_exists subexp
            end               
            
(***** END EXISTS HACK *****)
         
         | Lift(v, unprocessed_subterm) ->
            let nested = nesting_rewrites unprocessed_subterm in
            let (nested_ivars,nested_ovars) = C.schema_of_expr nested in
            (* Perform a few quick sanity checks.
               The variable being lifted should not occur in the nested 
               expression, especially not as an output variable (in which case, 
               the lift operation would overwrite that part of the schema).
               
               If the expression being lifted has the lifted variable as an 
               input, then what it's trying to express is an equality.  
               Technically this is an error, but there are certain cases 
               (unification in particular) where it can occur, so we simply 
               convert it into an equality.
            *)
            let lift_expr = 
               if List.mem v nested_ovars
               then failwith "Error: Overwriting schema of lifted expression"
               else if List.mem v nested_ivars
               then begin match nested with
                  | CalcRing.Val (Value(cmp_val)) ->
                     C.mk_cmp Eq (Arithmetic.mk_var v) cmp_val
                  | _ ->
                     let temp_var = mk_temp_var nested in
                        CalcRing.mk_prod [
                           C.mk_lift temp_var nested;
                           C.mk_cmp Eq (Arithmetic.mk_var v)
                                              (Arithmetic.mk_var temp_var)
                        ]
                  end
               else C.mk_lift v nested
            in lift_expr
         | _ -> CalcRing.mk_val e
      end)
      expr
;;

type selected_candidate_t = 
  | FactorizeLHS of C.expr_t
  | FactorizeRHS of C.expr_t
  | DoNotFactorize

(**
   [default_factorize_heuristic candidates scope term_list]
   
   where candidates is a 2-tuple of candidates that can be commuted to the lhs 
   of the terms they are extracted from, and the candidates that can be 
   commuted to the rhs.
   
   The default heuristic for factorization.  
      - Prefer non-value candidates
      - Prefer candidates that occur in the maximal number of terms
      - Prefer lhs candidates
      
*)
let default_factorize_heuristic (lhs_candidates,rhs_candidates)
                                scope term_list: (selected_candidate_t) =
   Debug.print "LOG-FACTORIZE" (fun () ->
      "Candidates for factorization: \n"^
      ListExtras.string_of_list ~sep:"\n" string_of_expr 
                               (lhs_candidates @ rhs_candidates)
   );
   let increment_count term list = 
      if List.mem_assoc term list then
         (term, (List.assoc term list) + 1)::(List.remove_assoc term list)
      else list
   in
   let (lhs_counts,rhs_counts) =
      List.fold_left (fun old_counts product_term -> 
         List.fold_left (fun (lhs_counts,rhs_counts) term ->
               (  increment_count term lhs_counts, 
                  increment_count term rhs_counts)
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
      DoNotFactorize
   else
      (* We should never get a candidate that doesn't appear at least once.  
         This is most definitely a bug if it happens. *)
   if ((List.length lhs_sorted >= 1) && (snd (List.hd lhs_sorted) < 1)) ||
      ((List.length rhs_sorted >= 1) && (snd (List.hd rhs_sorted) < 1))
   then failwith "BUG: Factorize got a vanishing candidate"
   else
      if (snd (List.hd rhs_sorted)) > (snd (List.hd lhs_sorted)) then
         FactorizeRHS(fst (List.hd rhs_sorted))
      else 
         FactorizeLHS(fst (List.hd lhs_sorted))

                                 

(**
  [factorize_one_polynomial scope term_list]
  
    Given an expression (A * B) + (A * C), factor out the A to get A * (B + C)
    Note that this works bi-directionally, we can factor terms off the front, 
    or off the back.  All that matters is whether we can commute the term 
    to wherever it needs to get factored.
  
    Most of the work happens in here in factorize_one_polynomial, which takes a 
    list of monomials representing a list of terms and identifies a Calculus 
    expression that is equivalent to their sum but which has terms factorized
    out as possible.
  
    This process happens in several stages. 
    - First, we identify a set of candidate subterms that may be factorized out 
      of the right or left hand side of each term.
    - For each candidate we count how many terms it can be factorized out of.
    - We pick the candidate that can be factorized out of the most terms and
      delete the term from those terms.
    - We recur twice, once on the set of terms that were factorized, and once
      on the set of terms that weren't.

   @param heuristic (optional) An optional factorization heuristic function
   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param term_list A list of terms to be treated as if they were part of a Sum
*)
let rec factorize_one_polynomial ?(heuristic = default_factorize_heuristic)
                                 (scope:var_t list) (term_list:C.expr_t list) =
   Debug.print "LOG-FACTORIZE" (fun () ->
      "Factorizing Expression: ("^
      (ListExtras.string_of_list ~sep:" + " 
          CalculusPrinter.string_of_expr term_list)^
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
   let factorize_and_split selected validate_fn =
      List.fold_left (fun (factorized,unfactorized) term ->
         try 
            let (lhs_of_selected,rhs_of_selected) =
               ListExtras.split_at_pivot selected (CalcRing.prod_list term) in
            Debug.print "LOG-FACTORIZE" (fun () ->
               "Split "^(CalculusPrinter.string_of_expr term)^" into "^
               (CalculusPrinter.string_of_expr 
                  (CalcRing.mk_prod lhs_of_selected))^
               " * [...] * "^
               (CalculusPrinter.string_of_expr 
                  (CalcRing.mk_prod rhs_of_selected))         
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
   let rcr ((factorized, unfactorized), merge_fn) =
      Debug.print "LOG-FACTORIZE" (fun () ->
         "Factorized: "^
            (ListExtras.string_of_list 
                CalculusPrinter.string_of_expr factorized)^"\n"^
         "Unfactorized: "^
            (ListExtras.string_of_list 
                CalculusPrinter.string_of_expr unfactorized)
      );
      (* Finally, recur. We might be able to pull additional terms out of the
         individual expressions.  Note that we're no longer able to pull out
         terms across the factorized/unfactorized boundary *)
      let ret =
         CalcRing.mk_sum [
            (merge_fn (factorize_one_polynomial scope factorized));
            (factorize_one_polynomial scope unfactorized)
         ]
      in Debug.print "LOG-FACTORIZE" (fun () ->
         "Final expression: \n"^(CalculusPrinter.string_of_expr ret)
      ); ret
   in
      match heuristic (lhs_candidates, rhs_candidates) scope term_list with
         | DoNotFactorize -> CalcRing.mk_sum term_list
         | FactorizeRHS(selected) ->
            Debug.print "LOG-FACTORIZE" (fun () ->
               "Factorizing "^
               (CalculusPrinter.string_of_expr selected)^
               " from the RHS"
            );
            rcr ((factorize_and_split selected (fun _ rhs ->
                  C.commutes ~scope:scope selected (CalcRing.mk_prod rhs))), 
                 (fun factorized -> CalcRing.mk_prod [factorized; selected])
                )
         | FactorizeLHS(selected) ->
            Debug.print "LOG-FACTORIZE" (fun () ->
               "Factorizing "^
               (CalculusPrinter.string_of_expr selected)^
               " from the LHS"
            );
            rcr ((factorize_and_split selected (fun lhs _ ->
                  C.commutes ~scope:scope (CalcRing.mk_prod lhs) selected)), 
                 (fun factorized -> CalcRing.mk_prod [selected; factorized])
                )

type term_map_t = {
   definition  : C.expr_t;
   valid       : bool ref
}

(**
   [cancel_term_list term_list]
   
   Removes terms from a list that cancel each other out when summed up.
   
   @param term_list The list of terms to process
*)
let cancel_term_list (term_list: C.expr_t list): C.expr_t list =
   let term_map = 
      List.map (fun term -> 
         { definition = term; valid = ref true }
      ) term_list
   in
   ListExtras.scan_fold (fun processed_terms
                             lhs_terms term rhs_terms ->
      if !(term.valid) then
         try
            let neg_term = CalcRing.mk_neg term.definition in
            let cancel_term =
               List.find (fun rhs_term ->
                  !(rhs_term.valid) &&
                  exprs_are_identical neg_term rhs_term.definition
               ) rhs_terms
            in
               term.valid := false;
               cancel_term.valid := false;
               Debug.print "LOG-CANCEL" (fun () ->
                  "Canceling "^
                  (CalculusPrinter.string_of_expr term.definition)^
                  " with " ^
                  (CalculusPrinter.string_of_expr cancel_term.definition)
               );
               processed_terms
         with Not_found ->
            processed_terms @ [ term.definition ]
      else
         processed_terms 
    ) [] term_map
   

(**
   [factorize_polynomial scope expr]
   
   Invokes polynomial factorization throughout an arbitrary Calculus expression 
   wherever possible as described above in factorize_one_polynomial.
   
   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param expr    The calculus expression being processed
*)
let factorize_polynomial (scope:var_t list) (expr:C.expr_t): C.expr_t =
   
   C.rewrite ~scope:scope 
      (fun (scope,_) sum_terms -> 
         factorize_one_polynomial scope (cancel_term_list sum_terms))
      (fun _ -> CalcRing.mk_prod)
      (fun _ -> CalcRing.mk_neg)
      (fun _ -> CalcRing.mk_val)
      (if Debug.active "AGGRESSIVE-FACTORIZE" 
         then combine_values (CalcRing.polynomial_expr expr)
         else expr)
;;
 

type opt_t = 
   | OptCombineValues
   | OptCombineValuesAggressive
   | OptLiftEqualities
   | OptAdvanceLifts
   | OptUnifyLifts
   | OptNestingRewrites
   | OptFactorizePolynomial
;;
let string_of_opt = (function
   | OptCombineValues           -> "OptCombineValues"
   | OptCombineValuesAggressive -> "OptCombineValuesAggressive"
   | OptLiftEqualities          -> "OptLiftEqualities"
   | OptAdvanceLifts            -> "OptAdvanceLifts"
   | OptUnifyLifts              -> "OptUnifyLifts"
   | OptNestingRewrites         -> "OptNestingRewrites"
   | OptFactorizePolynomial     -> "OptFactorizePolynomial"
)
;;
let default_optimizations = 
   [  OptLiftEqualities; OptAdvanceLifts; OptUnifyLifts; OptNestingRewrites;
      OptFactorizePolynomial; OptCombineValues]
;;
let private_time_hash:(opt_t, float) Hashtbl.t = Hashtbl.create 10;;
let dump_timings () = 
   Hashtbl.iter (fun opt time ->
      print_endline ("[CalculusTransforms] TIME: "^(string_of_opt opt)^" -> "^
                     (string_of_float time))
   ) private_time_hash
;;

(**
  optimize_expr
    Given an expression apply the above optimizations to it until a fixed point
    is reached.
*)
let optimize_expr ?(optimizations = default_optimizations)
                  ((scope,schema):C.schema_t) (expr:C.expr_t): C.expr_t =

   Debug.print "LOG-CALCOPT-STEPS" (fun () ->
      "CalculusTransforms asked to optimize: "^
      (ListExtras.ocaml_of_list string_of_var scope)^
      (ListExtras.ocaml_of_list string_of_var schema)^"\n"^
      (CalculusPrinter.string_of_expr expr)
   );
   if Debug.active "CALC-NO-OPTIMIZE" then expr else
      
   let fp_1 = ref (fun x -> sanity_check_variable_names x; x) in
   let include_opt_base o new_fn = 
     Fixpoint.build_if optimizations fp_1 o new_fn 
   in
   let include_opt o new_fn = 
      if not (Debug.active "TIME-CALCOPT") 
      then include_opt_base o new_fn 
      else
         let timed_fn x = 
            let start = Unix.time() in
            let result = new_fn x in
            let duration = Unix.time() -. start in 
            let tot = duration +. if (Hashtbl.mem private_time_hash o)
                                  then Hashtbl.find private_time_hash o
                                  else 0.0
            in Hashtbl.replace private_time_hash o tot; 
               result
         in include_opt_base o timed_fn
   in
      include_opt OptAdvanceLifts            (advance_lifts scope);
      include_opt OptUnifyLifts              (unify_lifts scope schema);
      include_opt OptLiftEqualities          (lift_equalities scope);
      include_opt OptNestingRewrites         (nesting_rewrites);
      include_opt OptFactorizePolynomial     (factorize_polynomial scope);
      include_opt OptCombineValues           (combine_values);
      include_opt OptCombineValuesAggressive (combine_values ~aggressive:true);
   if Debug.active "LOG-CALCOPT-STEPS" then Fixpoint.build fp_1 
      (fun x -> 
         let (fail,(ivars,ovars)) = 
            try 
               (None, (C.schema_of_expr x))
            with Failure(msg) ->
               ((Some(msg)), (scope,schema))
         in
            print_endline ("OPTIMIZING: "^
               (ListExtras.ocaml_of_list fst ivars)^
               (ListExtras.ocaml_of_list fst ovars)^
               " ::>> \n"^
               (CalculusPrinter.string_of_expr x)); 
            begin match fail with
             | None      -> x
             | Some(msg) -> failwith ("Schema error: "^msg)
            end);
   Fixpoint.compute_with_history !fp_1 expr
 