(** 
    A module for computing the initialization code of a given expression.  
		  This is required when the root-level expression is nonzero at the start
			(and barring changes in the deltas) will continue to be so throughout 
      execution.  There are two reasons that this might happen
         - The expression contains one or more Table relations, which will be
           nonzero at the start.
         - The expression contains one or more Lift expressions where the
           nested expression has no output variables (which will always have a
           value of 1).  
      ... and in both cases, there are no stream relations multiplying the 
      outermost terms to make the expression zero at the start *)

open Calculus

(** Compute the IVC of a given expression without input variables. *)
let derive_initializer ?(scope = [])
                       (table_rels:Schema.rel_t list) 
                       (expr:expr_t): expr_t =
   let table_names = List.map (fun (rn,_,_) -> rn) table_rels in
   let optimized_expr = (CalculusTransforms.optimize_expr 
                              (Calculus.schema_of_expr expr) expr)
   in 
   Debug.print "LOG-DERIVE-INITIALIZER" (fun () ->
      "[Initializer] Input is (scope: "^(ListExtras.ocaml_of_list fst scope)^
      "): "^(CalculusPrinter.string_of_expr optimized_expr)
   ); 
   let init_expr = 
      Calculus.rewrite_leaves ~scope:scope (fun lf_scope lf -> 
         Debug.print "LOG-DERIVE-INITIALIZER" (fun () -> 
            "[Initializer] Deriving Initializer for : "^(string_of_leaf lf)
         );
         match lf with
         | Rel(rn, rv) ->
            if List.mem rn table_names
            then CalcRing.mk_val (Rel(rn,rv))
            else CalcRing.zero
         | AggSum(gb_vars, subexp) ->
            (* If the subexpression is zero, then the aggsum is zero *)
            if subexp = CalcRing.zero then CalcRing.zero else
            
            (* It's also possible that the subexpression will have a narrower
               schema than before without becoming zero.  For example
                  AggSum([A], (B ^= R(A)) 
               would become
                  AggSum([A], (B ^= 0)) 
               In this case, there will be no rows in the output schema of this
               expression, and we can replace the nested expression by zero.
               
               Also note that this is not the case if any of those variables are
               bound on the outside (i.e., in the scope)
            *)
            let subexp_ovars = (snd (schema_of_expr subexp)) in
            let bound_schema = ListAsSet.union scope subexp_ovars in
            let unbound_schema = ListAsSet.diff gb_vars bound_schema in
               if unbound_schema = [] 
               then CalcRing.mk_val (AggSum(ListAsSet.inter gb_vars
                                                            subexp_ovars,
                                            subexp))
               else CalcRing.zero
         | _ -> CalcRing.mk_val lf
      ) (CalculusTransforms.optimize_expr (Calculus.schema_of_expr expr) expr)
   in
      Debug.print "LOG-DERIVE-INITIALIZER" (fun () ->
         "[Initializer] Initializer is "^
            (CalculusPrinter.string_of_expr init_expr)
      ); 
			if (Debug.active "IVC-OPTIMIZE-EXPR") then
	   		CalculusTransforms.optimize_expr (Calculus.schema_of_expr init_expr) init_expr
			else init_expr

(******************************************************************************)
