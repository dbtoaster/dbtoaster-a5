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

(*
let schema_of_expr ?(scope = []) (expr:expr_t) : (var_t list * var_t list) =
	let (expr_ivars, expr_ovars) = Calculus.schema_of_expr expr in
	let new_ovars = ListAsSet.inter expr_ivars scope in
		(ListAsSet.diff expr_ivars new_ovars, ListAsSet.union expr_ovars new_ovars) 
*)
let string_of_vars = ListExtras.string_of_list string_of_var

(******************************************************************************)

(* Returns a todo list with the current expression *)
let rec materialize (history:ds_history_t) (prefix:string) 
							      (event:Schema.event_t option) (expr:expr_t) 
										: (ds_t list * expr_t) = 
									
    let subexprs = decompose_poly expr in
		(* Note: all subexpressions have the same schema *)

		let event_vars = extract_event_vars event in 
		fst (
			List.fold_left ( fun ((todos, mat_expr), i) (subexpr_schema, subexpr) ->
				(* Subexpression optimization *)
				(* TODO: double check the scope and schema in the optimization call*)
				let subexpr_opt = optimize_expr (event_vars, subexpr_schema) subexpr in
				let subexpr_name = (prefix^"_t"^(string_of_int i)) in						
		
				print_string ("Materializing expression: "^(string_of_expr subexpr)^"    "^
					"Scope: ["^(string_of_vars event_vars)^"]    "^
		 			"Schema: ["^	(string_of_vars subexpr_schema)^"]"
				);
		
				Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
					"Materializing expression: "^(string_of_expr subexpr)^"    "^
					"Scope: ["^(string_of_vars event_vars)^"]    "^
		 			"Schema: ["^	(string_of_vars subexpr_schema)^"]"
				);
		
				let (todos_subexpr, mat_subexpr) = 
					materialize_expr history subexpr_name event subexpr_schema subexpr_opt 
			  in	
				  	((todos @ todos_subexpr, CalcRing.mk_sum [mat_expr; mat_subexpr]), 
						i + 1)	
			) (([], CalcRing.zero), 1) subexprs
		)
	
(* Materialization of an expression of the form mk_prod [ ...] *)	
and materialize_expr (history:ds_history_t) (prefix:string)
										 (event:Schema.event_t option)
										 (schema:var_t list) (expr:expr_t)
										 : (ds_t list * expr_t) =

		if (rels_of_expr expr) = [] then ([], expr) else

		let event_vars = extract_event_vars event in 

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
															
			) ((CalcRing.one, CalcRing.one, CalcRing.one), event_vars) (CalcRing.prod_list expr)) 
		in 		
		
		(* Sanity check - mat_exprs should not contain any input variables *)
		let (mat_exprs_ivars, mat_exprs_ovars) = schema_of_expr mat_exprs in
		if mat_exprs_ivars <> [] then 
			failwith "The materialized expression has input variables" 
		else
		
		(* Extended the schema with the input variables of other expressions *) 
		let expected_schema = (ListAsSet.multiunion [ 
			schema; fst (schema_of_expr lift_exprs); fst (schema_of_expr rest_exprs)]) in
		
		let (todo_lifts, mat_lifts) = ([], CalcRing.one) in
		
(*		
		(* Lifts are always materialized separately *)
		(* let scope_lift = ListAsSet.union scope mat_exprs_ovars in *)
		let (todo_lifts, mat_lifts) = 
				List.fold_left (fun (todos, mats) lift ->
						match (CalcRing.get_val lift) with
								| Lift(v, subexp) ->
									let (todo, mat) = materialize history prefix event subexp in
									(todos @ todo, CalcRing.mk_prod [mats; CalcRing.mk_val (Lift(v, mat))])			
								| _  -> (todos, mats)
				) ([], CalcRing.one) (CalcRing.prod_list lift_exprs)
		in																
*)
	
		(* Graph Decomposition *)
		let mat_subexprs = snd (decompose_graph event_vars (expected_schema, mat_exprs)) in		
		let (mat_subexprs_todos, mat_subexprs_mats) = fst (
			List.fold_left ( fun ((todos, mats), i) (schema_mat_expr, mat_expr) ->
			
				if (rels_of_expr mat_expr) = [] then 
					((todos, CalcRing.mk_prod [mats; mat_expr]), i) 
				else
			
				let (mat_expr_ivars, mat_expr_ovars) = schema_of_expr mat_expr in
  				let full_mat_expr = 
					if ListAsSet.seteq mat_expr_ovars schema_mat_expr then
						mat_expr
					else CalcRing.mk_val (AggSum(schema_mat_expr, mat_expr)) in
	
				(* Sanity check - mat_exprs should not contain any input variables *)
				if mat_exprs_ivars <> [] then 
					failwith "The materialized subexpression has input variables" 
				else
					
				let (found_ds, mapping_if_found) = 
						List.fold_left (fun result i ->
								if (snd result) <> None then result else
								(i, (cmp_exprs i.ds_definition full_mat_expr))
		      		) ({ds_name = CalcRing.one; ds_definition = CalcRing.one}, None) !history
				in begin match mapping_if_found with
					| None -> 
							let new_ds = {
									ds_name = CalcRing.mk_val (
											External(
					                (prefix^"_g"^(string_of_int i)),
					                mat_expr_ivars,
													ListAsSet.inter mat_expr_ovars expected_schema,
					                type_of_expr full_mat_expr,
					                None
					            )
					        );
					        ds_definition = full_mat_expr;
					    } in
								history := new_ds :: !history;
					      ((todos @ [new_ds], CalcRing.mk_prod [mats; new_ds.ds_name]), i+1)
					         
					| Some(mapping) ->
							Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
		   						"Found Mapping to: "^(string_of_expr found_ds.ds_name)^
		   						"      With: "^
									(ListExtras.ocaml_of_list (fun ((a,_),(b,_))->a^"->"^b) mapping)
		        		);
		        		((todos, CalcRing.mk_prod [mats; (rename_vars mapping found_ds.ds_name)]), i+1)
				end		
			) (([], CalcRing.one), 1) mat_subexprs
		) in
		(mat_subexprs_todos @ todo_lifts, CalcRing.mk_prod [mat_subexprs_mats; mat_lifts; rest_exprs])
