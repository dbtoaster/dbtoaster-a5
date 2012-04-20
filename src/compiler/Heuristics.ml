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
		| Some(Schema.InsertEvent((reln,_,_))) 
		| Some(Schema.DeleteEvent((reln,_,_))) -> Some(reln)
		| _  -> None


let schema_of_expr ?(scope:var_t list = []) (expr:expr_t) : 
                    (var_t list * var_t list) =
	let (expr_ivars, expr_ovars) = Calculus.schema_of_expr expr in
	let new_ovars = ListAsSet.inter expr_ivars scope in
		(ListAsSet.diff expr_ivars new_ovars, ListAsSet.union expr_ovars new_ovars) 


let string_of_expr = CalculusPrinter.string_of_expr
let string_of_vars = ListExtras.string_of_list string_of_var

(* Push rels to the left, cmps and values to the right *)
let reorganize (expr:expr_t) : (expr_t) =
	let (rels, lifts, rest) = 
		List.fold_left ( fun (rel,lift,rest) term ->
        match CalcRing.get_val term with
            | Value(_) | Cmp (_, _, _) -> (rel, lift, rest @ [term])
            | Rel (_, _) | External (_) -> (rel @ [term], lift, rest)
						| Lift (_, _) -> (rel, lift @ [term], rest)                              
						| AggSum (_, _) ->
							failwith "[reorganize] Error: AggSums are supposed to be removed."
    ) ([], [], []) (CalcRing.prod_list expr)
	in
	   CalcRing.mk_prod (rels @ lifts @ rest)

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
									failwith "[split_expr] Error: AggSums are supposed to be removed."																	
						| Rel (reln, relv) ->								
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
(*									if (List.mem v (ListAsSet.union scope_acc scope)) then
											failwith "[split_expr] Error: The lift variable is in the scope"
									else *)
										let subexpr_ivars = fst (schema_of_expr ~scope:scope_acc subexpr) in
										let subexpr_rels = rels_of_expr subexpr in
										let lift_contains_event_rel =
											match extract_event_reln event with 
												| Some(reln) -> List.mem reln subexpr_rels
												| None -> false
										in
										if ((lift_contains_event_rel) || (subexpr_ivars <> [])) then
											((rel, CalcRing.mk_prod [lift; term], rest), scope_acc)
										else
											(* The expression does not include input variables *)
											(* and does not involve the relation for which the *)
											(* trigger function is being created.*)
											((CalcRing.mk_prod [rel; term], lift, rest), scope_acc @ [v])													
			) ((CalcRing.one, CalcRing.one, CalcRing.one), []) 
			  (CalcRing.prod_list (reorganize expr))
		) 


(******************************************************************************)
(*let eval_as_update_expr (event:Schema.event_t option) 
                        (scope:var_t list) (schema:var_t list) 
                        (expr:expr_t) : bool =

    if (rels_of_expr expr) = [] then (true) else

		(* Divide the expression into three parts *)
    let (rel_exprs, lift_exprs, rest_exprs) = split_expr event scope expr in        
    
		let (rel_exprs_ivars, rel_exprs_ovars) = schema_of_expr ~scope:scope rel_exprs in
    let (lift_exprs_ivars, _) = schema_of_expr ~scope:scope lift_exprs in

    (* Iterate over all lift expressions *)
      let scope_lifts = ListAsSet.union scope rel_exprs_ovars in
        let (todo_lifts, mat_lifts) = fst ( 
                List.fold_left (fun ((todos, mats), j) lift ->
                        match (CalcRing.get_val lift) with
                                | Lift(v, subexp) ->
                                    let (todo, mat) = materialize ~scope:scope_lifts history 
                                                     (prefix^"_L"^(string_of_int j)^"_") event subexp in
                                    ((todos @ todo, CalcRing.mk_prod [mats; CalcRing.mk_val (Lift(v, mat))]), j + 1)            
                                | _  -> ((todos, mats), j)
                ) (([], CalcRing.one), 1) (CalcRing.prod_list lift_exprs)
        )   in  


    (true)

let eval_as_update (event:Schema.event_t option) (expr:expr_t) : bool =
	
    let expr_scope = extract_event_vars event in
	
    (* Polynomial decomposition. Note: all the terms have the same schema *)
    List.fold_left ( fun update (term_schema, term) ->
        (update && eval_as_update_expr event expr_scope term_schema term)
    ) (true) (decompose_poly expr_scope expr)
*)

(* Returns a todo list with the current expression *)
let rec materialize ?(scope:var_t list = [])
                    (history:ds_history_t) (prefix:string) 
							      (event:Schema.event_t option) (expr:expr_t) 
										: (ds_t list * expr_t) = 
		

		Debug.activate "LOG-HEURISTICS-DETAIL"; 
		Debug.activate "IGNORE-FINAL-OPTIMIZATION"; 
		
		Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
			 "[Heuristics] Raw expression: "^(string_of_expr expr)^
       "\n\t Scope: ["^(string_of_vars scope)^"]"
		);
																							
    let expr_scope = ListAsSet.union scope (extract_event_vars event) in
		let (todos, mat_expr) = fst (
			(* Polynomial decomposition. Note: all the terms have the same schema *)
			List.fold_left ( fun ((term_todos, term_mats), i) (term_schema, term) ->

    	    let term_opt = optimize_expr (expr_scope, term_schema) term in

        Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
            "[Heuristics] PolyDecomposition + Optimization: "^(string_of_expr term_opt)^
            "\n\t Scope: ["^(string_of_vars expr_scope)^"]"^
            "\n\t Schema: ["^(string_of_vars term_schema)^"]"
        ); 

				(* Graph decomposition *)
				let ((new_term_todos, new_term_mats), k) =  
					List.fold_left ( fun ((todos, mat_expr), j) (subexpr_schema, subexpr) ->
						
						(* Subexpression optimization *)					
						let subexpr_opt = optimize_expr (expr_scope, subexpr_schema) subexpr in
						
						let subexpr_name = (prefix^(string_of_int j)) in
								
						Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
							"[Heuristics] Graph decomposition: "^(string_of_expr subexpr)^
							"\n\t Scope: ["^(string_of_vars expr_scope)^"]"^
				 			"\n\t Schema: ["^	(string_of_vars subexpr_schema)^"]"
						);
				
						let (todos_subexpr, mat_subexpr) = 
							materialize_expr history subexpr_name event expr_scope subexpr_schema subexpr_opt 
					  in
						  Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                  "[Heuristics] Materialized form: "^(string_of_expr mat_subexpr)
              );
						  	((todos @ todos_subexpr, CalcRing.mk_prod [mat_expr; mat_subexpr]), 	j + 1)	
	
					) (([], CalcRing.one), i)
					 	(snd (decompose_graph expr_scope (term_schema, term_opt)))
				in
					((term_todos @ new_term_todos, CalcRing.mk_sum [term_mats; new_term_mats]), k)
					
			) (([], CalcRing.zero), 1) (decompose_poly expr_scope expr)
		)
		in
		(if (Debug.active "IGNORE-FINAL-OPTIMIZATION") then
		  (todos, mat_expr)
		else 
			let schema = snd (schema_of_expr ~scope:expr_scope mat_expr) in
			(todos, optimize_expr (expr_scope, schema) mat_expr))
	
(* Materialization of an expression of the form mk_prod [ ...] *)	
and materialize_expr (history:ds_history_t) (prefix:string)
										 (event:Schema.event_t option)
                     (scope:var_t list) (schema:var_t list) 
										 (expr:expr_t) : (ds_t list * expr_t) =

		(*if (rels_of_expr expr) = [] then ([], expr) else*)

		(* Divide the expression into three parts *)
		let (rel_exprs, lift_exprs, rest_exprs) = split_expr event scope expr in 		

    let (rel_exprs_ivars, rel_exprs_ovars) = schema_of_expr ~scope:scope rel_exprs in
		let (lift_exprs_ivars, lift_exprs_ovars) = schema_of_expr ~scope:scope lift_exprs in
		let (rest_exprs_ivars, _) = schema_of_expr ~scope:scope rest_exprs in
					
		(* Sanity check - mat_exprs should not contain any input variables *)
		if rel_exprs_ivars <> [] then 
			  (print_string ((string_of_expr rel_exprs)^"\nScope:"^(string_of_vars scope)^"\nInpVars: "^(string_of_vars rel_exprs_ivars));
		    failwith "The materialized expression has input variables") 
		else
										
		(* Lifts are always materialized separately *)
	  let scope_lifts = ListAsSet.union scope rel_exprs_ovars in
		let (todo_lifts, mat_lifts) = fst ( 
				List.fold_left (fun ((todos, mats), j) lift ->
						match (CalcRing.get_val lift) with
								| Lift(v, subexp) ->
									let (todo, mat) = materialize ~scope:scope_lifts history 
									                 (prefix^"_L"^(string_of_int j)^"_") event subexp in
									((todos @ todo, CalcRing.mk_prod [mats; CalcRing.mk_val (Lift(v, mat))]), j + 1)			
								| _  -> ((todos, mats), j)
				) (([], CalcRing.one), 1) (CalcRing.prod_list lift_exprs)
		)	in	
		
		(* Extended the schema with the input variables of other expressions *) 
		let rel_expected_schema = ListAsSet.inter rel_exprs_ovars 
		                                      (ListAsSet.multiunion [ schema; 
																					                       lift_exprs_ivars;
																																 (* handling a special case when (A ^= dB) * (A ^= 5) *)
																																 lift_exprs_ovars;
																																 rest_exprs_ivars]) in
		(* If necessary, add aggregation to the relation term *)
		let agg_rel_expr = 
			if ListAsSet.seteq rel_exprs_ovars rel_expected_schema then 
				rel_exprs
			else CalcRing.mk_val (AggSum(rel_expected_schema, rel_exprs)) 
		in
		Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
			"[Heuristics]  Relation AggSum expression: "^(string_of_expr agg_rel_expr)^
	    "\n\t Scope: ["^(string_of_vars scope)^"]"^
	    "\n\t Lift InpVars: ["^(string_of_vars lift_exprs_ivars)^"]"^
	    "\n\t Rest InpVars: ["^(string_of_vars rest_exprs_ivars)^"]"^
      "\n\t Schema org: ["^(string_of_vars schema)^"]"^
	    "\n\t Schema: ["^(string_of_vars rel_exprs_ovars)^"]"^
	    "\n\t Relation expected schema: ["^(string_of_vars rel_expected_schema)^"]"
	  ); 
		
	  let (todos, complete_mat_exprs) =					 
		  if (rels_of_expr rel_exprs) = [] then 
	        (todo_lifts, CalcRing.mk_prod [ agg_rel_expr; mat_lifts; rest_exprs])
	    else		
				
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
												rel_expected_schema,
				                type_of_expr agg_rel_expr,
				                None
				            )
				        );
				        ds_definition = agg_rel_expr;
				    } in
							history := new_ds :: !history;
				      ([new_ds] @ todo_lifts, CalcRing.mk_prod [new_ds.ds_name; mat_lifts; rest_exprs])
				         
					| Some(mapping) ->
						Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
				   						"[Heuristics] Found Mapping to: "^(string_of_expr found_ds.ds_name)^
				   						"      With: "^
											(ListExtras.ocaml_of_list (fun ((a,_),(b,_))->a^"->"^b) mapping)
						);
						(todo_lifts, CalcRing.mk_prod [(rename_vars mapping found_ds.ds_name); mat_lifts; rest_exprs])
				end
		in
    (* If necessary, add aggregation to the relation term *)
		let (mat_expr_ivars, mat_expr_ovars) = schema_of_expr ~scope:scope complete_mat_exprs in
    let agg_mat_expr = 
			if ListAsSet.seteq mat_expr_ovars schema then 
				complete_mat_exprs
      else CalcRing.mk_val (AggSum(schema, complete_mat_exprs)) 
		in
		  (todos, agg_mat_expr) 

		