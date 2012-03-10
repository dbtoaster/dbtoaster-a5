open Types
open Arithmetic
open Calculus
open CalculusDecomposition
open CalculusTransforms
open Plan

type ds_history_t = ds_t list ref

(******************************************************************************)
		 
let extract_event_vars (event:Schema.event_t option) : (var_t list) =
	match event with 
		| Some(ev) ->  Schema.event_vars ev
		| None -> []

let extract_event_reln (event:Schema.event_t option) : (string option) =
	match event with
		| Some(Schema.InsertEvent((reln,_,_,_))) 
		| Some(Schema.DeleteEvent((reln,_,_,_))) -> Some(reln)
		| _  -> None

(******************************************************************************)

(* Returns todo list with the current expression *)
let rec materialize ?(scope = []) (history:ds_history_t) (prefix:string) 
							      (event:Schema.event_t option) (expr:expr_t) 
										: (ds_t list * expr_t) =
									
    let subexprs = decompose_poly expr in
		(* Note: all subexpressions have the same schema *)

		let scope_evars = ListAsSet.union scope (extract_event_vars event) in
			fst (List.fold_left ( fun ((todos, mat_expr), i) (schema, subexpr) ->
						let subexpr_opt = optimize_expr (scope_evars, schema) subexpr in
						let subexpr_name = (prefix^"_"^(string_of_int i)) in
						(*
						print_string ("subexpr_name: "^subexpr_name^
													"   Scope: "^(ListExtras.string_of_list string_of_var scope_evars)^
												  "   Schema: "^(ListExtras.string_of_list string_of_var schema)^"\n");
						*)
						let (todos_subexpr, mat_subexpr) = 
							materialize_expr history subexpr_name event (scope_evars, schema) subexpr_opt 
					  in	
						  	((todos @ todos_subexpr, CalcRing.mk_sum [mat_expr; mat_subexpr]), 
								i + 1)	
					) (([], CalcRing.zero), 1) subexprs)
	
(* Materialization of an expression of the form mk_prod [ ...] *)	
and materialize_expr (history:ds_history_t) (prefix:string)
										 (event:Schema.event_t option)
										 ((scope, schema):(var_t list * var_t list)) 
										 (expr:expr_t)
										 : (ds_t list * expr_t) =

		if (rels_of_expr expr) = [] then ([], expr) else

		(* Divide the expression into three pieces:                                    *)
		(* lift_exprs: lifts containing the event relation or input variables          *)
		(* rest_expr: subexpressions with input variables and the above lift variables *)
		(* mat_exprs: relations, irrelevant lifts, comparisons, variables, constants   *)
		let (mat_exprs, lift_exprs, rest_exprs) = fst (
			List.fold_left ( fun ((mat, lift, rest), scope_acc) term ->
					
					match CalcRing.get_val term with
						| Value(v) -> 
									let v_ivars = fst (schema_of_expr term) in
									if (ListAsSet.subset v_ivars scope_acc) then
											((CalcRing.mk_prod [mat; term], lift, rest), scope_acc)
									else 
											((mat, lift, CalcRing.mk_prod [rest; term]), scope_acc)
						| AggSum (v, subexp) ->
									failwith "[materialize_expr] Error: AggSums are supposed to be removed."																	
						| Rel (reln, relv, relt) ->								
									((CalcRing.mk_prod [mat; term], lift, rest), (ListAsSet.union scope_acc relv))
						| External (_) -> 
									((mat, lift, CalcRing.mk_prod [rest; term]), scope_acc) 
						| Cmp (_, v1, v2) -> 
									let cmp_ivars = fst (schema_of_expr term) in
									if (ListAsSet.subset cmp_ivars scope_acc) then
											((CalcRing.mk_prod [mat; term], lift, rest), scope_acc)
									else 
											((mat, lift, CalcRing.mk_prod [rest; term]), scope_acc)
						| Lift (v, subexpr) ->						
									if (ListAsSet.subset [v] scope_acc) then
											failwith"[materialize_expr] Error: The lift variable is in the scope"
									else
										let subexpr_rels = rels_of_expr subexpr in
										let subexpr_ivars = fst (schema_of_expr subexpr) in
										let subexpr_ivars_covered = ListAsSet.subset subexpr_ivars scope_acc in
										let lift_contains_event_rel = 
												match extract_event_reln event with 
													| Some(reln) -> List.mem reln subexpr_rels
													| None -> false
										in
										if ((not subexpr_ivars_covered) || lift_contains_event_rel) then										
											 	((mat, CalcRing.mk_prod [lift; term], rest), scope_acc)	
										else
												(* The expression does not include input variables *)
												(* and does not involve the relation for which the *)
												(* trigger function is being created.*)
   											((CalcRing.mk_prod [mat; term], lift, rest), scope_acc @ [v])							
															
			) ((CalcRing.one, CalcRing.one, CalcRing.one), scope) (CalcRing.prod_list expr)) 
		in 		
			
		(* Lifts are always materialized separately *)
		let scope_lift = ListAsSet.union scope (snd (schema_of_expr mat_exprs)) in
		let (todo_lifts, mat_lifts) = 
				List.fold_left (fun (todos, mats) lift ->
						match (CalcRing.get_val lift) with
								| Lift(v, subexp) ->
									let (todo, mat) = materialize ~scope:scope_lift 
											history prefix event subexp in
									(todos @ todo, CalcRing.mk_prod [mats; CalcRing.mk_val (Lift(v, mat))])			
								| _  -> (todos, mats)
				) ([], CalcRing.one) (CalcRing.prod_list lift_exprs)
		in																
		
		let (mat_exprs_ivars, mat_exprs_ovars) = schema_of_expr mat_exprs in
			Debug.print "LOG-COMPILE-DETAIL" (fun () -> 
					"Materializing expression: "^(string_of_expr mat_exprs)^"\n"^
	 				"Output Variables: ["^
						(ListExtras.string_of_list string_of_var mat_exprs_ovars)^"]"
			);
		(*
		print_string ("Output variables: "^(ListExtras.string_of_list string_of_var mat_exprs_ovars)^
									"    Schema: "^(ListExtras.string_of_list string_of_var schema)^
									"    Intersection: "^(ListExtras.string_of_list string_of_var (ListAsSet.inter mat_exprs_ovars schema))^"\n");
		*)
		let (found_ds,mapping_if_found) = 
				List.fold_left (fun result i ->
						if (snd result) <> None then result else
						(i, (cmp_exprs i.ds_definition mat_exprs))
      		) ({ds_name=CalcRing.one;ds_definition=CalcRing.one}, None) !history
		in begin match mapping_if_found with
			| None -> 
					let new_ds = {
							ds_name = CalcRing.mk_val (
									External(
			                prefix,
			                ListAsSet.diff mat_exprs_ivars scope,
			                ListAsSet.inter mat_exprs_ovars schema,
			                type_of_expr mat_exprs,
			                None
			            )
			        );
			        ds_definition = mat_exprs;
			    } in
						history := new_ds :: !history;
			      ([new_ds] @ todo_lifts, CalcRing.mk_prod ( [new_ds.ds_name; mat_lifts; rest_exprs] ))
			         
			| Some(mapping) ->
					Debug.print "LOG-COMPILE-DETAIL" (fun () -> 
   						"Found Mapping to : "^(string_of_expr found_ds.ds_name)^
   						"\nWith: "^
							(ListExtras.ocaml_of_list (fun ((a,_),(b,_))->a^"->"^b) 
                                        mapping)
        		);
        		(todo_lifts, CalcRing.mk_prod [ (rename_vars mapping found_ds.ds_name); 
																				  (rename_vars mapping mat_lifts); 
																					(rename_vars mapping rest_exprs)])
		end