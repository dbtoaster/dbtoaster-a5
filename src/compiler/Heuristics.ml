(**
   A module for deciding on how to materialize a given (delta) query. It is used
	 by the Compiler module to produce a Plan.plan_t datastructure.
	 	
	 Driven by a set of heuristics rules, this module provides two main functions: 
	 {ul
	   {- {b should_update} decides whether it is more efficient to incrementally 
		    maintain the given query, or to reevaluate it upon each trigger call }  
	   {- {b materialize} transforms the input (delta) expression by replacing all
		    relations with data structures (maps) }
	 }
  *)

open Types
open Arithmetic
open Calculus
open CalculusDecomposition
open CalculusTransforms
open Plan

type ds_history_t = ds_t list ref

(******************************************************************************)

(** A helper function for obtaining the variable list from the optional event parameter. 
    It calls Schema.event_vars if the event is passed. *)
let extract_event_vars (event:Schema.event_t option) : (var_t list) =
  match event with 
		| Some(ev) ->  Schema.event_vars ev
	  | None -> []

(** A helper function for extracting the relations from the optional event parameter. *)
let extract_event_reln (event:Schema.event_t option) : (string option) =
	match event with
		| Some(Schema.InsertEvent((reln,_,_))) 
		| Some(Schema.DeleteEvent((reln,_,_))) -> Some(reln)
		| _  -> None

(** Obtain the human-readable representation of the optional event parameter. 
    It calls Schema.string_of_event if the event is passed. *) 
let string_of_event (event:Schema.event_t option) : string =
	match event with
		| Some(evt) -> Schema.string_of_event evt
    | _  -> "<None>" 

let string_of_expr = CalculusPrinter.string_of_expr
let string_of_vars = ListExtras.string_of_list string_of_var

(** Reorganize a given expression such that the relation terms are pushed to 
    the left while comparisons and values are pushed to the right. The partial 
		order inside each of the group (relations, lifts, and rests) is preserved. 
		The method assumes an aggsum-free product-only representation of the expression. *)
let reorganize_expr (expr:expr_t) : (expr_t list * expr_t list * expr_t list) =
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
	   (rels, lifts, rest)


(** Split an expression into three parts: 
	  {ol
	    {li base relations, irrelevant lift expressions with respect to the 
	        event relation that also contain no input variables, and subexpressions 
	        with no input variables (comparisons, variables and constants) }  
	    {li lift expressions containing the event relation or input variables }
	    {li subexpressions with input variables and the above lift variables } 
	   } 
		 Note: In order to minimize the need for IVC, if there is no relation at the 
		 root level, then lift subexpressions containing irrelevant relations are 
		 materialized separately. *)
let partition_expr (event:Schema.event_t option) (expr:expr_t) :
							 (expr_t * expr_t * expr_t) =

    let schema_of_expr ?(scope:var_t list = []) (expr:expr_t) :  
                        (var_t list * var_t list) =
	    let (expr_ivars, expr_ovars) = Calculus.schema_of_expr expr in
	    let new_ovars = ListAsSet.inter expr_ivars scope in
	        (ListAsSet.diff expr_ivars new_ovars, ListAsSet.union expr_ovars new_ovars) 
    in
		let (rel_part, lift_part, rest_part) = reorganize_expr expr in
		let minimize_ivc = Debug.active "HEURISTICS-MINIMIZE-IVC" in
		let has_root_relation = (rel_part <> []) in 
		
		(* Relations, irrelevant lifts, relevant lifts, and the remainder *)
		let (rels, lifts, rests) = fst (
			List.fold_left ( fun ((rel, lift, rest), scope_rel) term ->
					
					match CalcRing.get_val term with
						| Value(v) -> 
									let v_ivars = fst (schema_of_expr ~scope:scope_rel term) in
									if (v_ivars = []) then
											((rel @ [term], lift, rest), scope_rel)
									else 
											((rel, lift, rest @ [term]), scope_rel)
						| AggSum (v, subexp) ->
									failwith "[partition_expr] Error: AggSums are supposed to be removed."																	
						| Rel (reln, relv) ->								
									((rel @ [term], lift, rest), (ListAsSet.union scope_rel relv))
						| External (_) -> 
									((rel, lift, rest @ [term]), scope_rel) 
						| Cmp (_, v1, v2) -> 
									let cmp_ivars = fst (schema_of_expr ~scope:scope_rel term) in
									if (cmp_ivars = []) then
											((rel @ [term], lift, rest), scope_rel)
									else 
											((rel, lift, rest @ [term]), scope_rel)
						| Lift (v, subexpr) ->						
									let subexpr_ivars = fst (schema_of_expr ~scope:scope_rel subexpr) in
									let subexpr_rels = rels_of_expr subexpr in
									let lift_contains_event_rel =
										match extract_event_reln event with 
											| Some(reln) -> List.mem reln subexpr_rels
											| None -> false
									in
									if ((lift_contains_event_rel) || (subexpr_ivars <> []) ||
									    (minimize_ivc && (not has_root_relation))) then
										((rel, lift @ [term], rest), scope_rel)
									else
										(* The expression does not include input variables *)
										(* and does not involve the relation for which the *)
										(* trigger function is being created.*)
										((rel @ [term], lift, rest), scope_rel @ [v])		
																					
			) (([], [], []), []) (rel_part @ lift_part @ rest_part)
		) 
    in 
		  (CalcRing.mk_prod rels, CalcRing.mk_prod lifts, CalcRing.mk_prod rests)
				

(******************************************************************************)
(** For a given expression and a trigger event, decides whether it is more 
    efficient to incrementally maintain the expression or to reevaluate it 
		upon each trigger call. 
		
		The reason for making this decision is the following: If an expression 
		contains 	a lift subexpression (nested aggregate) with nonzero delta, 
		the overall delta expression is not simpler than the original expression.
		In such cases, it might be beneficial to maintain the expression 
		non-incrementally, recomputing it on every update.
				
		In general, the subexpressions of the given expression can be divided into 
		three categories:  
		  {ol
        {li base relations, irrelevant lift expressions with respect to the 
            event relation that also contain no input variables, and subexpressions 
            with no input variables (comparisons, variables and constants) }  
        {li lift expressions containing the event relation or input variables }
        {li subexpressions with input variables and the above lift variables } 
       }
		The rule for making the decision is the following:
		If there is an equality constraint between the base relations (category I) 
		and lift subexpressions (category II), i.e. the variables of the lift 
		subexpressions are also output variables of the base relations, then 
		the expression should be incrementally maintained. The rationale	behind 
		this decision is that deltas of the lift subexpressions affect only a subset
		of the tuples, thus bounding the variables used in the outside expression
		and avoiding the need to iterate over the whole domain of values.
		Otherwise, if there is no overlapping between the set of variables used 
		inside the lift subexpressions and the rest of the query, the expression 
		is more efficient to reevalute on each trigger call.
		
		Two special cases arise when the given expression does not contain lift
		subexpressions, or it does not contain relation subexpressions. In both 
		cases, this method suggests the incremental maintenance.
*)
let should_update (event:Schema.event_t) (expr:expr_t)  : bool =
	
	  if (Debug.active "HEURISTICS-ALWAYS-UPDATE") then true
		else 
			if (Debug.active "HEURISTICS-ALWAYS-REPLACE") then false
		else
	    let expr_scope = Schema.event_vars event in
			let (do_update, do_replace) = 
		    (* Polynomial decomposition *)
		    List.fold_left ( fun (do_update, do_replace) (term_schema, term) ->
		
		        let term_opt = optimize_expr (expr_scope, term_schema) term in
	
			      (* Graph decomposition *)
		        let (do_update_graphs, do_replace_graphs) =  
		          List.fold_left ( fun (do_update_graph, do_replace_graph) (subexpr_schema, subexpr) ->
			                
				        (* Subexpression optimization *)                    
				        let subexpr_opt = optimize_expr (expr_scope, subexpr_schema) subexpr in
	
	              (* Split the expression into three parts *)
								let (rel_exprs, lift_exprs, _) = partition_expr (Some(event)) subexpr_opt in
		            let rel_exprs_ovars = snd (schema_of_expr rel_exprs) in
								(* TODO: Get variables appearing in the lift expressions and not all variables *) 
		            let lift_exprs_vars = all_vars lift_exprs in
								let local_update_graph = ((rel_exprs_ovars = []) || (lift_exprs_vars = []) ||
								                          ((ListAsSet.inter rel_exprs_ovars lift_exprs_vars) <> [])) in
								(do_update_graph || local_update_graph, 
								 do_replace_graph || (not local_update_graph))
			
		          ) (false, false)
		            (snd (decompose_graph expr_scope (term_schema, term_opt)))
		        in
		          (do_update || do_update_graphs, do_replace || do_replace_graphs)
			                
		    ) (false, false) (decompose_poly expr)
			in
			  if (do_update && do_replace) || (not do_update && not do_replace) then
				  (Debug.active "HEURISTICS-PREFER-UPDATE")
				else do_update	
    
(******************************************************************************)

(** Perform partial materialization of a given expression according to the following rules:
      {ol
        {li {b Polynomial expansion}: Before materializing, an expression is expanded into 
				    a flat ("polynomial") form with unions at the top. The materialization procedure 
						is performed over each individual term. }
			  {li {b Graph decomposition}: If some parts of the expression are disconnected in 
				    the join graph, it is always better to materialise them piecewise.}
				{li {b Decorrelation of nested subexpressions}: Lift expressions with non-zero 
				    delta are always materialized separetely. } 
        {li {b No input variables}: No map includes input variables in order to prevent 
				    creation of large expensive-to-maintain domains. } 
				{li {b Factorization} is performed after the partial materialization in order to 
				    maximise reuse of common subexpressions. }
       }
   @param scope     The scope in which [expr] is materialized
	 @param db_schema Schema of the database
   @param history   The history of used data structures
	 @param prefix    A prefix string used to name newly created maps
	 @param event     A trigger event
	 @param expr      A calculus expression
   @return          A list of data structures that needs to be materialized afterwards 
	                  (a todo list) together with the materialized form of the expression. 
*)									
let rec materialize ?(scope:var_t list = []) (db_schema:Schema.t) 
                    (history:ds_history_t) (prefix:string) 
							      (event:Schema.event_t option) (expr:expr_t) 
										: (ds_t list * expr_t) = 
		
		(* Debug.activate "LOG-HEURISTICS-DETAIL"; *) 
		(* Debug.activate "HEURISTICS-IGNORE-FINAL-OPTIMIZATION";  *)
		
		Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
			 "[Heuristics] "^(string_of_event event)^
			 "\n\t Expr: "^(string_of_expr expr)^
       "\n\t Scope: ["^(string_of_vars scope)^"]"
		);
																							
    let expr_scope = ListAsSet.union scope (extract_event_vars event) in
		let (todos, mat_expr) = fst (
			(* Polynomial decomposition *)
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
							materialize_expr db_schema history subexpr_name event 
							                 expr_scope subexpr_schema subexpr_opt 
					  in
						  Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                  "[Heuristics] Materialized form: "^(string_of_expr mat_subexpr)
              );
						  	((todos @ todos_subexpr, CalcRing.mk_prod [mat_expr; mat_subexpr]), 	j + 1)	
	
					) (([], CalcRing.one), i)
					 	(snd (decompose_graph expr_scope (term_schema, term_opt)))
				in
					((term_todos @ new_term_todos, CalcRing.mk_sum [term_mats; new_term_mats]), k)
					
			) (([], CalcRing.zero), 1) (decompose_poly expr)
		)
		in
		(if (Debug.active "HEURISTICS-IGNORE-FINAL-OPTIMIZATION") then
		  (todos, mat_expr)
		else 
			let schema = snd (schema_of_expr mat_expr) in
			(todos, optimize_expr (expr_scope, schema) mat_expr))
	
(* Materialization of an expression of the form mk_prod [ ...] *)	
and materialize_expr (db_schema:Schema.t) (history:ds_history_t) 
                     (prefix:string) (event:Schema.event_t option)
                     (scope:var_t list) (schema:var_t list) 
										 (expr:expr_t) : (ds_t list * expr_t) =

		(* Divide the expression into three parts *)
		let (rel_exprs, lift_exprs, rest_exprs) = partition_expr event expr in 		

    let (rel_exprs_ivars, rel_exprs_ovars) = schema_of_expr rel_exprs in
		let (lift_exprs_ivars, lift_exprs_ovars) = schema_of_expr lift_exprs in
		let (rest_exprs_ivars, _) = schema_of_expr rest_exprs in
					
		(* Sanity check - rel_exprs should not contain any input variables *)
		if rel_exprs_ivars <> [] then  
        (print_endline (string_of_expr rel_exprs);
		     failwith ("The materialized expression has input variables")) 
		else
										
		(* Lifts are always materialized separately *)
	  let scope_lifts = ListAsSet.union scope rel_exprs_ovars in
		let (todo_lifts, mat_lifts) = fst ( 
				List.fold_left (fun ((todos, mats), (j, scope_acc)) lift ->
						match (CalcRing.get_val lift) with
								| Lift(v, subexp) ->
									let (todo, mat) = materialize ~scope:scope_acc db_schema history 
									                 (prefix^"_L"^(string_of_int j)^"_") event subexp in
									let mat_ovars = snd(schema_of_expr mat) in 
									   ((todos @ todo, CalcRing.mk_prod [mats; CalcRing.mk_val (Lift(v, mat))]), 
									    (j + 1, (ListAsSet.union scope_acc mat_ovars)))		
								| _  -> ((todos, mats), (j, scope_acc))
				) (([], CalcRing.one), (1, scope_lifts)) (CalcRing.prod_list lift_exprs)
		)	in	
		
		(* Extended the schema with the input variables of other expressions *) 
		let rel_expected_schema = ListAsSet.inter rel_exprs_ovars 
	      (ListAsSet.multiunion [ (* when materializing a lift subexpression, variables
				                           that are used outside the lift need to be preserved *)
					                      scope;
					                      schema; 
				                        lift_exprs_ivars;
															  (* handling a special case of type (A ^= 0) * (A ^= R(dB)) *)
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
      "\n\t Relation OutVars: ["^(string_of_vars rel_exprs_ovars)^"]"^
      "\n\t Original schema: ["^(string_of_vars schema)^"]"^
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
						(* Compute the IVC expression *) 
						let ivc = IVC.derive_initializer ~scope:scope 
						                                 (Schema.table_rels db_schema) 
																						agg_rel_expr 
            in
            let ivc_expr = if (ivc = CalcRing.zero) then None else Some(ivc) in
						
            Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
               begin match ivc_expr with
                 | None -> "[Heuristics]  ===> NO IVC <==="
                 | Some(s) -> "[Heuristics]  IVC: \n"^(string_of_expr s)
               end
            );
						let new_ds = {
								ds_name = CalcRing.mk_val (
										External(
						            prefix,
				                rel_exprs_ivars,
												rel_expected_schema,
				                type_of_expr agg_rel_expr,
				                ivc_expr
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
		let (_, mat_expr_ovars) = schema_of_expr complete_mat_exprs in
    let agg_mat_expr = 
			if ListAsSet.seteq mat_expr_ovars schema then 
				complete_mat_exprs
      else CalcRing.mk_val (AggSum(schema, complete_mat_exprs)) 
		in
		  (todos, agg_mat_expr) 

		