(**
   The heart of the DBToaster frontend: Where we do delta processing. 

   The primary entry into this module is Compiler.compile, which takes a list
   of named queries and transforms them into a Plan.plan_t.  
   
   Compiler makes extensive use of Calculus functionality, and in particular
   CalculusTransforms and CalculusDeltas.
  *)

open Ring
open Arithmetic
open Types
open Calculus
open Plan

(******************************************************************************)

type todo_t = ds_t * bool (* bool is true if we can skip TLQ computation *)
type todo_list_t = todo_t list
type tlq_list_t = (string * expr_t) list

(******************************************************************************)

let extract_renamings ((scope,schema):schema_t) (expr:expr_t): 
                      ((var_t * var_t) list * expr_t) =
   let (raw_mappings, expr_terms) = 
      List.fold_left (fun (mappings, expr_terms) term ->
         begin match term with
         | CalcRing.Val(Lift(v1, 
            CalcRing.Val(Value(ValueRing.Val(AVar(v2))))))->
            if (List.mem v2 scope) && (List.mem v1 schema) then
               ((v1, v2)::mappings, expr_terms)
            else (mappings, expr_terms @ [term])
         | _ ->  (mappings, expr_terms @ [term])
         end
      ) ([], []) (CalcRing.prod_list expr)
   in
   let (mappings, mapping_conditions) =
      (* It's possible that we might receive multiple renamings for the same
         output variables (e.g. for the delta of R(A,B)*(A=B)).  In this case,
         we need to pick (an arbitrary) one of the renamings, and put the
         remaining renamings back in as equality predicates over the inputs.
         
         We do this by doing an associative reduce to pair each original name
         with the full set of names that it can take.  We pick the head of
         that list, and add back in an equality predicate between the chosen
         name and the remaining names.
       *)
      List.split (
         List.map (fun (orig_var, new_var_list) ->
               match new_var_list with
                | [] -> failwith "BUG: reduce returned an empty list"
                | x::rest -> 
                  (  (orig_var, x), 
                     CalcRing.mk_prod (List.map (fun y -> 
                        CalcRing.mk_val (Cmp(Eq, mk_var x, mk_var y))
                     ) rest)
                  )
            )
            (ListExtras.reduce_assoc raw_mappings)
      )
   in
   let rec fix_schemas expr =
      (* We can't just leave the result of the standard rename_vars approach 
         here, because we're actually mucking with the schema of the 
         subexpressions.  In particular it's possible for us to remove Lifts (if 
         we get X ^= X), and the schema of AggSums may need to be updated. *)
      CalcRing.fold CalcRing.mk_sum CalcRing.mk_prod CalcRing.mk_neg
         (fun leaf -> (begin match leaf with
            | AggSum(gb_vars, subexp) ->
               let new_subexp = fix_schemas subexp in
               CalcRing.mk_val (AggSum(
                  ListAsSet.inter gb_vars (snd (schema_of_expr new_subexp)),
                  new_subexp
               ))
            | Lift(var1, CalcRing.Val(Value(ValueRing.Val(AVar(var2)))))
                  when var1 = var2 -> CalcRing.one
            | Lift(var, subexp) -> 
               CalcRing.mk_val (Lift(var, fix_schemas subexp))
            | _ -> CalcRing.mk_val leaf
         end))
         expr
   in
      (  mappings, 
         fix_schemas (Calculus.rename_vars mappings 
                         (CalcRing.mk_prod (mapping_conditions@expr_terms))))

(******************************************************************************)

let compile_map (db_schema:Schema.t) (history:Heuristics.ds_history_t)
                ((todo,skip_ivc):todo_t) : todo_list_t * compiled_ds_t =
   (* Sanity check: Deltas of non-numeric types don't make sense and it doesn't
      make sense to compile a map that doesn't support deltas of some form. *)
   let (todo_name, todo_ivars, todo_ovars, todo_base_type, _) =
      begin match todo.ds_name with
         | CalcRing.Val(External(e)) -> e
         | _ -> failwith "Error: Invalid datastructure name, not an external"
      end
   in
   let todo_type = 
      begin match todo_base_type with
         | TInt | TFloat -> todo_base_type
         | TBool -> TInt
         | _ -> failwith "Error: Compiling map with unsupported type"
      end
   in
   Debug.print "LOG-COMPILE-DETAIL" (fun () ->
      "Optimizing: \n"^(CalculusPrinter.string_of_expr todo.ds_definition)
   );
   let optimized_defn = 
      CalculusTransforms.optimize_expr (todo_ivars, todo_ovars) 
                                      todo.ds_definition
   in
   Debug.print "LOG-COMPILE-DETAIL" (fun () ->
      "Optimized: \n"^(CalculusPrinter.string_of_expr optimized_defn)
   );
   let rels = 
      try List.map (Schema.rel db_schema) (rels_of_expr optimized_defn)
      with Not_found -> 
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Expected rels: "^(ListExtras.string_of_list (fun x->x)
                                 (rels_of_expr optimized_defn))^
            "\nDatabase Schema: "^(Schema.string_of_schema db_schema)
         );
         failwith "Compiling expression with undefined relation.";
   in
   let (table_rels, stream_rels) = 
      List.partition (fun (_,_,t) -> t = Schema.TableRel) rels
   in
   let triggers = ref [] in
   let trigger_todos = ref [] in
   let events = (if (Debug.active "IGNORE-DELETES")
                 then [(fun x -> Schema.InsertEvent(x)), ""]
                 else [(fun x -> Schema.DeleteEvent(x)), "_m";
                       (fun x -> Schema.InsertEvent(x)), "_p"])
   in
   
   List.iter (fun (reln,relv,_) -> List.iter (fun (mk_evt,evt_prefix) ->
               
      (***** THE FUN STUFF HAPPENS HERE *****)
      
      let map_prefix = todo_name^evt_prefix^reln in
      let prefixed_relv = List.map (fun (n,t) -> (map_prefix^n, t)) relv in         
      let delta_event = mk_evt (reln, prefixed_relv, Schema.StreamRel) in
            
      if (Heuristics.should_update delta_event optimized_defn) 
      then begin
                 
         (* The expression is to be incrementally maintained *)
         let delta_expr_unoptimized = 
            (CalculusDeltas.delta_of_expr delta_event optimized_defn)
         in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Unoptimized Delta: "^(Schema.string_of_event delta_event)^
            " DO \n"^(CalculusPrinter.string_of_expr delta_expr_unoptimized)
         );
         let delta_expr_unextracted = 
            CalculusTransforms.optimize_expr 
               (todo_ivars @ prefixed_relv,todo_ovars) 
               delta_expr_unoptimized
         in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Optimized, Unextracted Delta: \n" ^ 
            (Schema.string_of_event delta_event) ^ 
            " DO "^(CalculusPrinter.string_of_expr delta_expr_unextracted)
         );
         let (delta_renamings, delta_expr) = 
            extract_renamings (prefixed_relv, todo_ovars) delta_expr_unextracted
         in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Optimized Delta: \n"^(Schema.string_of_event delta_event)^
            " DO "^(CalculusPrinter.string_of_expr delta_expr)
         );
    
         let (new_todos, materialized_delta) = 
            Heuristics.materialize db_schema history map_prefix 
                                   (Some(delta_event)) delta_expr
         in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Materialized delta expr: \n"^
            (CalculusPrinter.string_of_expr materialized_delta)
         );
             
         let (ivc_todos,todo_ivc) = 
            if skip_ivc then ([], None) else
            if (todo_ivars <> []) then ([], None)
(*          then failwith "TODO: Implement IVC for maps with ivars."*)
            else if (todo_ovars <> [])
            then if IVC.naive_needs_runtime_ivc (Schema.table_rels db_schema)
                                                todo.ds_definition
                 then  let (ivc_todos, todo_ivc) =
                          Heuristics.materialize db_schema 
                                                 history 
                                                 (map_prefix^"_IVC")
                                                 (Some(delta_event))
                                                 todo.ds_definition
                       in (ivc_todos, Some(todo_ivc))
                 else ([], None)
            else ([], None)
         in
             
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            begin match todo_ivc with
               | None -> "===> NO IVC <==="
               | Some(s) -> "IVC: \n"^(CalculusPrinter.string_of_expr s)
            end
         );
             
         trigger_todos := 
            (** The materializer (presently) produces maps guaranteed not to
                need IVC *)
            (List.map (fun x -> (x, true)) (ivc_todos @ new_todos)) 
               @ !trigger_todos;
         triggers      := 
            (delta_event, {
               Plan.target_map = 
                  CalcRing.mk_val (External(
                     todo_name, 
                     List.map (Function.apply_if_present delta_renamings) 
                              todo_ivars,
                     List.map (Function.apply_if_present delta_renamings) 
                              todo_ovars,
                     todo_base_type,
                     todo_ivc  
                  ));
               Plan.update_type = Plan.UpdateStmt;
               Plan.update_expr = materialized_delta
            }) :: !triggers
      end
      else begin 
         (* The expression is to be reevaluated *)
         let (new_todos, materialized_expr) = 
            Heuristics.materialize db_schema history map_prefix 
                                   (Some(delta_event)) optimized_defn
         in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Materialized expr: \n"^
            (CalculusPrinter.string_of_expr materialized_expr)
         );
         trigger_todos := (List.map (fun x -> (x, true)) new_todos)
                               @ !trigger_todos;
         triggers      := 
            (delta_event, {
               Plan.target_map = todo.ds_name;
               Plan.update_type = Plan.ReplaceStmt;
               Plan.update_expr = materialized_expr
            }) :: !triggers
      end
      
      (**************************************)

   ) events) stream_rels;
   
   (* Compute the initialization code to run at system start.  This is 
      required when the root-level expression is nonzero at the start (and
      barring changes in the deltas) will continue to be so throughout 
      execution.  There are two reasons that this might happen
         - The expression contains one or more Table relations, which will be
           nonzero at the start.
         - The expression contains one or more Lift expressions where the
           nested expression has no output variables (which will always have a
           value of 1).  
      ... and in both cases, there are no stream relations multiplying the 
      outermost terms to make the expression zero at the start. *)
   begin match IVC.derive_initializer (Schema.table_rels db_schema)
                                      todo.ds_definition
   with
      | x when x = CalcRing.zero -> ()
      | system_init_expr ->
         triggers := 
            (Schema.SystemInitializedEvent, {
               Plan.target_map = todo.ds_name;
               Plan.update_type = Plan.ReplaceStmt;
               Plan.update_expr = system_init_expr
            }) :: !triggers
   end;
   
   
   let (init_todos, init_expr) = 
      if todo_ivars <> [] then
         (* If the todo has input variables, it needs a default initializer *)
         let (init_todos,init_expr) =
            Heuristics.materialize db_schema history (todo_name^"_init") 
						                       None optimized_defn
         in (init_todos, Some(init_expr))
      else
         ([], None)
   
   in
      (((!trigger_todos) @ (List.map (fun x -> (x, true)) init_todos)), {
         description = {
            ds_name = CalcRing.mk_val (External(
                  todo_name, todo_ivars, todo_ovars, todo_type, init_expr
               ));
            ds_definition = optimized_defn
         };
         ds_triggers = !triggers
      })

(******************************************************************************)
let compile_table ((reln, relv, relut):Schema.rel_t): compiled_ds_t = 
   let map_name = (Plan.mk_ds_name ("_"^reln) ([],relv) TInt) in {
      Plan.description = {
         Plan.ds_name       = map_name ;
         Plan.ds_definition = (CalcRing.mk_val (Rel(reln, relv)))
      };
      Plan.ds_triggers = (List.map (fun (event, update_expr) ->
         (event, {
            Plan.target_map  = map_name;
            Plan.update_type = UpdateStmt;
            Plan.update_expr = update_expr
         }
      )) [(Schema.InsertEvent(reln, relv, relut)), 
               CalcRing.one; 
          (Schema.DeleteEvent(reln, relv, relut)), 
               CalcRing.mk_neg CalcRing.one]
      )
   }

(******************************************************************************)
let compile_tlqs (db_schema:Schema.t) (history:Heuristics.ds_history_t) 
                 (calc_queries:tlq_list_t) : todo_list_t * tlq_list_t =
	 	
   let todo_lists, toplevel_queries = List.split ( 			
      if (Debug.active "EXPRESSIVE-TLQS") then
         List.map ( fun (qname, qexpr) ->
            let qschema = Calculus.schema_of_expr qexpr in
            let optimized_qexpr = CalculusTransforms.optimize_expr qschema 
                                                                   qexpr in
            let (todos, mat_expr) = Heuristics.materialize db_schema history
                                                           (qname^"_") None 
                                                           optimized_qexpr in
               (List.map (fun x -> (x, true)) todos, (qname, mat_expr))
         ) calc_queries
      else
         List.map (fun (qname, qexpr) ->
            let qschema = Calculus.schema_of_expr qexpr in
            let qtype   = Calculus.type_of_expr qexpr in
            let ds_name = Plan.mk_ds_name qname qschema qtype in
               ( [ { Plan.ds_name = ds_name; 
                     Plan.ds_definition = qexpr }, false ], 
                 (qname, ds_name) )
         ) calc_queries
    ) in 
      (List.flatten todo_lists, toplevel_queries)
			
(******************************************************************************)

let compile (db_schema:Schema.t) (calc_queries:tlq_list_t): 
            (plan_t * tlq_list_t) =
	 
	 (* First process the toplevel queries *)					
   let history:Heuristics.ds_history_t   = ref [] in
   let plan   :plan_t ref                = ref [] in
	 let todo_list, tlq_list = compile_tlqs db_schema history calc_queries in	
	 let todos  :todo_list_t ref           = ref todo_list in
	 let toplevel_queries : tlq_list_t ref = ref tlq_list in
				
   while List.length !todos > 0 do (
      let next_ds = List.hd !todos in todos := List.tl !todos;
      Debug.print "LOG-COMPILE-DETAIL" (fun () ->
         "Compiling: "^
         (if (snd next_ds) then "(skipping todos)" else "")^
         (string_of_ds (fst next_ds))
      );
      let new_todos, compiled_ds = 
         compile_map db_schema history next_ds
      in
      (* The order in which we concatenate new_todos decides whether compilation 
      is performed 'depth-first' or 'breadth-first' with respect to the map 
      heirarchy.  Only depth-first is correct without a topological sort 
      postprocessing step. 
      
      OAK: I'm not 100% convinced that this is the case.  It might be safer just 
      to do a topo sort postprocessing step regardless.
      *)
      todos     := new_todos @ !todos;
      plan      := (!plan) @ [compiled_ds]
   ) done; (!plan, !toplevel_queries)

(******************************************************************************)

let string_of_ds ds = 
   "DECLARE "^(string_of_ds ds.description)^"\n"^(String.concat "\n" 
      (List.map (fun x -> "   "^x) (
         (List.map (fun (evt, stmt) ->
            (Schema.string_of_event evt)^" DO "^(string_of_statement stmt))
            ds.ds_triggers)
      )))

let string_of_plan (plan:plan_t): string =
   ListExtras.string_of_list ~sep:"\n\n" string_of_ds plan