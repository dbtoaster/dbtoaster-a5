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

let schema_of_expr ?(scope:var_t list = []) (expr:expr_t) : 
                    (var_t list * var_t list) =
	let (expr_ivars, expr_ovars) = Calculus.schema_of_expr expr in
	let new_ovars = ListAsSet.inter expr_ivars scope in
		(ListAsSet.diff expr_ivars new_ovars, ListAsSet.union expr_ovars new_ovars) 

let string_of_vars = ListExtras.string_of_list string_of_var


(* Divide the expression into three parts:                                     *)
(* lift_exprs: lifts containing the event relation or input variables          *)
(* rest_expr: subexpressions with input variables and the above lift variables *)
(* rel_exprs: relations, irrelevant lifts, comparisons, variables, constants   *)
let split_expr (event:Schema.event_t option) 
               (scope:var_t list) (expr:expr_t) :
							 (expr_t * expr_t * expr_t) =
		fst (
			List.fold_left ( fun ((rel, lift, rest), scope_acc) term ->
					
					match CalcRing.get_val term with
						| Value(v) -> 
									let v_ivars = fst (schema_of_expr ~scope:scope_acc term) in
									if (v_ivars = []) then
											((CalcRing.mk_prod [rel; term], lift, rest), scope_acc)
									else 
											((rel, lift, CalcRing.mk_prod [rest; term]), scope_acc)
						| AggSum (v, subexp) ->
									failwith "[materialize_expr] Error: AggSums are supposed to be removed."																	
						| Rel (reln, relv, relt) ->								
									((CalcRing.mk_prod [rel; term], lift, rest), (ListAsSet.union scope_acc relv))
						| External (_) -> 
									((rel, lift, CalcRing.mk_prod [rest; term]), scope_acc) 
						| Cmp (_, v1, v2) -> 
									let cmp_ivars = fst (schema_of_expr ~scope:scope_acc term) in
									if (cmp_ivars = []) then
											((CalcRing.mk_prod [rel; term], lift, rest), scope_acc)
									else 
											((rel, lift, CalcRing.mk_prod [rest; term]), scope_acc)
						| Lift (v, subexpr) ->						
									if (ListAsSet.subset [v] scope_acc) then
											failwith"[materialize_expr] Error: The lift variable is in the scope"
									else
										let subexpr_rels = rels_of_expr subexpr in
										let subexpr_ivars = fst (schema_of_expr ~scope:scope_acc subexpr) in
										let subexpr_ivars_covered = (subexpr_ivars = []) in
										let lift_contains_event_rel = 
												match extract_event_reln event with 
													| Some(reln) -> List.mem reln subexpr_rels
													| None -> false
										in
										if ((not subexpr_ivars_covered) || lift_contains_event_rel) then										
											 	((rel, CalcRing.mk_prod [lift; term], rest), scope_acc)	
										else
												(* The expression does not include input variables *)
												(* and does not involve the relation for which the *)
												(* trigger function is being created.*)
   											((CalcRing.mk_prod [rel; term], lift, rest), scope_acc @ [v])							
															
			) ((CalcRing.one, CalcRing.one, CalcRing.one), scope) (CalcRing.prod_list expr)
		) 


(******************************************************************************)

(* Returns a todo list with the current expression *)
let rec materialize ?(scope:var_t list = [])
                    (history:ds_history_t) (prefix:string) 
							      (event:Schema.event_t option) (expr:expr_t) 
										: (ds_t list * expr_t) = 
		
(*		print_string "\n\nMATERIALIZE\n"; *)	
																							
    let expr_scope = ListAsSet.union scope (extract_event_vars event) in
		fst (
			(* Polynomial decomposition. Note: all the terms have the same schema *)
			List.fold_left ( fun ((term_todos, term_mats), i) (term_schema, term) ->

    	    let term_opt = optimize_expr (expr_scope, term_schema) term in

(*        print_string ("SUM: "^(string_of_expr term_opt)^
                "\n     Scope: ["^(string_of_vars expr_scope)^"]"^
                "\n     Schema: ["^(string_of_vars term_schema)^"]\n"
        ); *)

				(* Graph decomposition *)
				let ((new_term_todos, new_term_mats), k) =  
					List.fold_left ( fun ((todos, mat_expr), j) (subexpr_schema, subexpr) ->
						
						(* Subexpression optimization *)					
						let subexpr_opt = optimize_expr (expr_scope, subexpr_schema) subexpr in
						
						let subexpr_name = (prefix^(string_of_int j)) in
				
(*				    print_string ("GRAPH: "^(string_of_expr subexpr)^
                    "\n      Scope: ["^(string_of_vars expr_scope)^"]"^
                    "\n      Schema: ["^    (string_of_vars subexpr_schema)^"]\n"
            ); *)
				
						Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
							"Materializing expression: "^(string_of_expr subexpr)^"    "^
							"Scope: ["^(string_of_vars expr_scope)^"]    "^
				 			"Schema: ["^	(string_of_vars subexpr_schema)^"]"
						);
				
						let (todos_subexpr, mat_subexpr) = 
							materialize_expr history subexpr_name event expr_scope subexpr_schema subexpr_opt 
					  in
(*              print_string ("MATERIALIZED GRAPH: "^(string_of_expr mat_subexpr)^"\n"); *)
	
						  	((todos @ todos_subexpr, CalcRing.mk_prod [mat_expr; mat_subexpr]), 	j + 1)	
	
					) (([], CalcRing.one), i)
					 	(snd (decompose_graph expr_scope (term_schema, term_opt)))
				in
					((term_todos @ new_term_todos, CalcRing.mk_sum [term_mats; new_term_mats]), k)
					
			) (([], CalcRing.zero), 1) (decompose_poly expr)
		)
	
(* Materialization of an expression of the form mk_prod [ ...] *)	
and materialize_expr (history:ds_history_t) (prefix:string)
										 (event:Schema.event_t option)
                     (scope:var_t list) (schema:var_t list) 
										 (expr:expr_t) : (ds_t list * expr_t) =

		if (rels_of_expr expr) = [] then ([], expr) else

		(* Divide the expression into three parts *)
		let (rel_exprs, lift_exprs, rest_exprs) = split_expr event scope expr in 		

    let (rel_exprs_ivars, rel_exprs_ovars) = schema_of_expr ~scope:scope rel_exprs in
		let (lift_exprs_ivars, _) = schema_of_expr ~scope:scope lift_exprs in
		let (rest_exprs_ivars, _) = schema_of_expr ~scope:scope rest_exprs in
					
		(* Sanity check - mat_exprs should not contain any input variables *)
		if rel_exprs_ivars <> [] then 
		    failwith "The materialized expression has input variables" 
		else
										
		(* Lifts are always materialized separately *)
	  let scope_lifts = ListAsSet.union scope rel_exprs_ovars in
		let (todo_lifts, mat_lifts) = 
				List.fold_left (fun (todos, mats) lift ->
						match (CalcRing.get_val lift) with
								| Lift(v, subexp) ->
									let (todo, mat) = materialize ~scope:scope_lifts history (prefix^"_") event subexp in
									(todos @ todo, CalcRing.mk_prod [mats; CalcRing.mk_val (Lift(v, mat))])			
								| _  -> (todos, mats)
				) ([], CalcRing.one) (CalcRing.prod_list lift_exprs)
		in	

		if (rels_of_expr rel_exprs) = [] then 
			(todo_lifts, CalcRing.mk_prod [ rel_exprs; mat_lifts; rest_exprs])
		else
				
			(* Extended the schema with the input variables of other expressions *) 
			let expected_schema = ListAsSet.inter rel_exprs_ovars 
															(ListAsSet.multiunion [scope; schema; lift_exprs_ivars; 
																										 rest_exprs_ivars]) in
			(* Add an aggregation if necessary *)
			let agg_rel_expr = if ListAsSet.seteq rel_exprs_ovars expected_schema then rel_exprs
													else CalcRing.mk_val (AggSum(expected_schema, rel_exprs)) in
			
(*			print_string ("AGG_EXPR: "^(string_of_expr agg_rel_expr)^
			              "\n       SCHEMA: "^(string_of_vars rel_exprs_ovars)^
										"\n       EXPECTED SCHEMA: "^(string_of_vars expected_schema)^"\n"); *)
			
			(* Try to found an already existing map *)
			let (found_ds, mapping_if_found) = 
				List.fold_left (fun result i ->
            (*print_string ("  DS_DEFINITION: "^(string_of_expr i.ds_definition)^"\n");*)
						if (snd result) <> None then result else
						(i, (cmp_exprs i.ds_definition agg_rel_expr))
	  			) ({ds_name = CalcRing.one; ds_definition = CalcRing.one}, None) !history
			in begin match mapping_if_found with
				| None -> 
					let new_ds = {
							ds_name = CalcRing.mk_val (
									External(
					            prefix,
			                rel_exprs_ivars,
											expected_schema,
			                type_of_expr agg_rel_expr,
			                None
			            )
			        );
			        ds_definition = agg_rel_expr;
			    } in
						history := new_ds :: !history;
						Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
			   						"Materializing: "^(string_of_expr agg_rel_expr)
						);
	
			      ([new_ds] @ todo_lifts, CalcRing.mk_prod [new_ds.ds_name; mat_lifts; rest_exprs])
			         
				| Some(mapping) ->
					Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
			   						"Found Mapping to: "^(string_of_expr found_ds.ds_name)^
			   						"      With: "^
										(ListExtras.ocaml_of_list (fun ((a,_),(b,_))->a^"->"^b) mapping)
					);
					(todo_lifts, CalcRing.mk_prod [(rename_vars mapping found_ds.ds_name); mat_lifts; rest_exprs])
			end
		