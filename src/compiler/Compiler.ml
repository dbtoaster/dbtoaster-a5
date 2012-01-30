open Ring
open Arithmetic
open Types
open Calculus
open Materializer

module C = BasicCalculus

(******************************************************************************)

let compile_map (history:ds_history_t) (db_schema:Schema.t)
                ((map_ref, map_defn): todo_ds_t):
                todo_ds_t list * compiled_ds_t =
   (* Sanity check: Deltas of non-numeric types don't make sense and it doesn't
      make sense to compile a map that doesn't support deltas of some form. *)
   begin match map_ref.value_type with
      | TInt | TFloat -> ()
      | _ -> failwith "Error: Compiling map with unsupported type"
   end;
   let optimized_defn = 
      CalculusOptimizer.optimize_expr (map_ref.input_vars, 
                                       map_ref.output_vars) 
                                      map_defn
   in
   let rels = List.map (Schema.rel db_schema) (C.rels_of_expr optimized_defn) in
   let (table_rels, stream_rels) = 
      List.split (fun (_,_,t,_) -> t = Schema.TableRel) rels
   in
   let triggers = ref [] in
   let trigger_todos = ref [] in
   let    init_at_start  =         table_rels <> [] 
      and init_on_access = map_ref.input_vars <> [] in
   let events = (if (Debug.active "IGNORE DELETES")
                 then [Schema.InsertEvent]
                 else [Schema.InsertEvent; Schema.DeleteEvent])
   in
   List.iter (fun (reln,relv,_,_) -> List.iter (fun event ->
               
      (***** THE FUN STUFF HAPPENS HERE *****)
      
      let delta_expr = 
         CalculusDeltas.delta_of_expr (event,reln) relv optimized_defn
      in
      let (new_todos, materialized_delta) = 
         materialize_expr history
                          (map_ref.name^"_"^
                           (if event == Schema.InsertEvent then "+" else "-")^
                           "_"^reln)
                          delta_expr
      in
         trigger_todos := new_todos @ !trigger_todos;
         triggers      := (
            (event, reln), 
            (map_ref, Statement.UpdateStmt, materialized_delta)
         ) :: !triggers
      
      (**************************************)

   ) events) stream_rels;
   
   let (init_todos, init_expr) = 
      if init_at_start || init_on_access then
         materialize_expr ((!trigger_todos)@known_ds)
                          (map_ref.name^"_init")
                          optimized_defn
      else
         ([], optimized_defn)
   
   in
      ((trigger_todos @ init_todos), 
      { datastructure: (map_ref, map_defn),
        init_expr:     init_expr,
        triggers:      triggers
        meta:          {
           Datastructure.init_at_start:  init_at_start,
           Datastructure.init_on_access: init_on_access
        }
      })

(******************************************************************************)

let topological_sort (plan: plan_t): plan_t =
   failwith "TODO: topo_sort"


(******************************************************************************)

let compile (queries:(Datastructure.t * C.expr_t) list)
            (db_schema:Schema.t): plan_t =
   let todos:todo_ds_t list ref     = ref queries in
   let history:ds_history_t         = empty_history () in
   let plan:plan_t ref              = ref [] in
   while List.length !todos > 0 do (
      let next_ds = List.hd !todos in todos := List.tl !todos;
      let new_todos, compiled_ds = 
         compile_map history
                     db_schema
                     next_ds
      in
      completed := next_ds :: !completed;
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
   ) done; topological_sort !plan

