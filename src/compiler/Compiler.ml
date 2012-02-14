open Ring
open Arithmetic
open Types
open Calculus
open Statement

module C = Calculus

module TemporaryMaterializer = struct
   (* Temporary hack to test the rest of the code.  This materializer doesn't
      do anything interesting, it just materializes the entire expression, doing
      some limited expression deduplication.  It will cause an infinite loop on 
      queries with nested subqueries. *)
   type ds_history_t = ds_t list ref
   
   let materialize_one (history:ds_history_t) (prefix:string) (expr:C.expr_t) =
      if (C.rels_of_expr expr) = [] then ([], expr) else
      let (expr_ivars, expr_ovars) = C.schema_of_expr expr in
      Debug.print "LOG-COMPILE-DETAIL" (fun () -> 
         "Materializing expression: "^(C.string_of_expr expr)^"\n"^
         "Output Variables: ["^
            (ListExtras.string_of_list string_of_var expr_ovars)^"]"
      );
      let (found_ds,mapping_if_found) = 
         List.fold_left (fun result i ->
            if (snd result) <> None then result else
               (i, (C.cmp_exprs i.ds_definition expr))
            ) ({ds_name=CalcRing.one;ds_definition=CalcRing.one}, None) !history
      in begin match mapping_if_found with
         | None -> 
            let new_ds = {
               ds_name = CalcRing.mk_val (
                     External(
                        prefix,
                        expr_ivars,
                        expr_ovars,
                        C.type_of_expr expr,
                        None
                     )
                  );
               ds_definition = expr;
            } in
               history := new_ds :: !history;
               ([new_ds], new_ds.ds_name)
         
         | Some(mapping) ->
            Debug.print "LOG-COMPILE-DETAIL" (fun () -> 
               "Found Mapping to : "^(C.string_of_expr found_ds.ds_name)^
               "\nWith: "^
                  (ListExtras.ocaml_of_list (fun ((a,_),(b,_))->a^"->"^b) 
                                            mapping)
            );
            ([], C.rename_vars mapping found_ds.ds_name)
      end
   
   let materialize (history:ds_history_t) (prefix:string) (expr:C.expr_t) =
      let (expr_ivars, expr_ovars) = C.schema_of_expr expr in
      let subexprs = 
         List.map (fun (subexp_schema, subexp) ->
            let (_, subexp_ovars) = C.schema_of_expr subexp in
            if ListAsSet.seteq subexp_ovars subexp_schema then
               subexp
            else C.CalcRing.mk_val (C.AggSum(subexp_schema, subexp))
         ) 
         (snd 
            (CalculusDecomposition.decompose_graph expr_ivars (expr_ovars,expr))
         )  
      in
         fst (List.fold_left (fun (ret, i) subexp ->
            let (new_todos, new_term) = 
               materialize_one history (prefix^"_"^(string_of_int i)) subexp
            in
            ((new_todos @ (fst ret), 
             CalcRing.mk_prod [snd ret; new_term]), 
             i+1)
         ) (([], CalcRing.one), 1) subexprs)
      
end

module Materializer = TemporaryMaterializer

type todo_list_t = ds_t list
type ds_metadata_t = {
   init_at_start  : bool;
   init_on_access : bool
}
type compiled_ds_t = {
   definition : ds_t;
   metadata   : ds_metadata_t;
   triggers   : (Schema.event_t * stmt_t) list
}
type plan_t = compiled_ds_t list

(******************************************************************************)

let compile_map (db_schema:Schema.t) (history:Materializer.ds_history_t)
                (todo: ds_t): todo_list_t * compiled_ds_t =
   (* Sanity check: Deltas of non-numeric types don't make sense and it doesn't
      make sense to compile a map that doesn't support deltas of some form. *)
   let (todo_name, todo_ivars, todo_ovars, todo_type, _) =
      begin match todo.ds_name with
         | CalcRing.Val(External(e)) -> e
         | _ -> failwith "Error: Invalid datastructure name, not an external"
      end
   in
   begin match todo_type with
      | TInt | TFloat -> ()
      | _ -> failwith "Error: Compiling map with unsupported type"
   end;
   Debug.print "LOG-COMPILE-DETAIL" (fun () ->
      "Optimizing: "^(C.string_of_expr todo.ds_definition)
   );
   let optimized_defn = 
      CalculusOptimizer.optimize_expr (todo_ivars, todo_ovars) 
                                      todo.ds_definition
   in
   Debug.print "LOG-COMPILE-DETAIL" (fun () ->
      "Optimized: "^(C.string_of_expr optimized_defn)
   );
   let rels = 
      try List.map (Schema.rel db_schema) (C.rels_of_expr optimized_defn)
      with Not_found -> 
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Expected rels: "^(ListExtras.string_of_list (fun x->x)
                                 (C.rels_of_expr optimized_defn))^
            "\nDatabase Schema: "^(Schema.string_of_schema db_schema)
         );
         failwith "Compiling expression with undefined relation.";
   in
   let (table_rels, stream_rels) = 
      List.partition (fun (_,_,t,_) -> t = Schema.TableRel) rels
   in
   let triggers = ref [] in
   let trigger_todos = ref [] in
   let ds_meta = {
      init_at_start  = table_rels <> [];
      init_on_access = todo_ivars <> []
   } in
   let events = (if (Debug.active "IGNORE DELETES")
                 then [Schema.InsertEvent]
                 else [Schema.DeleteEvent; Schema.InsertEvent])
   in
   List.iter (fun (reln,relv,_,relt) -> List.iter (fun event ->
               
      (***** THE FUN STUFF HAPPENS HERE *****)
      
      let map_prefix = 
         todo_name^"_"^
            (if event == Schema.InsertEvent then "p" else "m")^
            "_"^reln
      in
      let prefixed_relv = List.map (fun (n,t) -> (map_prefix^n, t)) relv in
      let delta_event = (event,(reln, prefixed_relv, Schema.StreamRel, relt)) in
      let delta_expr = 
         CalculusDeltas.delta_of_expr delta_event optimized_defn
      in
      Debug.print "LOG-COMPILE-DETAIL" (fun () ->
         "Delta: "^(Schema.string_of_event 
            (event,(reln,relv,Schema.StreamRel,relt)))^
            " DO "^(C.string_of_expr delta_expr)
      );
      let (new_todos, materialized_delta) = 
         Materializer.materialize history map_prefix 
            (CalculusOptimizer.optimize_expr (todo_ivars @ prefixed_relv,
                                              todo_ovars) 
                                             delta_expr)
      in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Materialized: "^(C.string_of_expr materialized_delta)
         );
         trigger_todos := new_todos @ !trigger_todos;
         triggers      := 
            (delta_event, {
               Statement.target_map = todo.ds_name;
               Statement.update_type = Statement.UpdateStmt;
               Statement.update_expr = materialized_delta
            }) :: !triggers
      
      (**************************************)

   ) events) stream_rels;
   
   let (init_todos, init_expr) = 
      if ds_meta.init_at_start || ds_meta.init_on_access then
         let (init_todos,init_expr) =
            Materializer.materialize history (todo_name^"_init") optimized_defn
         in (init_todos, Some(init_expr))
      else
         ([], None)
   
   in
      (((!trigger_todos) @ init_todos), {
         definition = {
            ds_name = CalcRing.mk_val (External(
                  todo_name, todo_ivars, todo_ovars, todo_type, init_expr
               ));
            ds_definition = optimized_defn
         };
         metadata = ds_meta;
         triggers = !triggers
      })


(******************************************************************************)

let compile (db_schema:Schema.t) (queries:todo_list_t): plan_t =
   let todos  :todo_list_t ref           = ref queries in
   let history:Materializer.ds_history_t = ref [] in
   let plan   :plan_t ref                = ref [] in
      
   while List.length !todos > 0 do (
      let next_ds = List.hd !todos in todos := List.tl !todos;
      Debug.print "LOG-COMPILE-DETAIL" (fun () ->
         "Compiling: "^(string_of_ds next_ds)
      );
      let new_todos, compiled_ds = 
         compile_map db_schema history next_ds
      in
      (* The order in which we concatenate new_todos decides whether compilation 
      is performed 'depth-first' or 'breadth-first' with respect to the map 
      heirarchy.  Both of these approaches are correct (even in the absence of a 
      topological sort postprocessing step).  The real question is how and where 
      duplicate map elimination takes place: depth-first will encounter most of 
      its duplicate maps in the 'completed' list, while breadth first will 
      encounter most of its duplicate maps in the 'todos' list.  Without the 
      topo sort, we can't re-use maps in the 'completed' table, but there are 
      cases where breadth first is going to re-use maps it encounters in 
      'completed' as well.  So, in general, depth-first should be a touch more 
      efficient *)
      todos     := new_todos @ !todos;
      plan      := compiled_ds :: !plan
   ) done; !plan

(******************************************************************************)

let string_of_ds ds = 
   "DECLARE "^(string_of_ds ds.definition)^"\n"^(String.concat "\n" 
      (List.map (fun x -> "   "^x) (
         (if ds.metadata.init_at_start 
            then ["WITH init_at_start"] else [])@
         (if ds.metadata.init_on_access 
            then ["WITH init_on_access"] else [])@
         (List.map (fun (evt, stmt) ->
            (Schema.string_of_event evt)^" DO "^(string_of_statement stmt))
            ds.triggers)
      )))

let string_of_plan (plan:plan_t): string =
   ListExtras.string_of_list ~sep:"\n\n" string_of_ds plan