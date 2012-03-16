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

(*
module TemporaryMaterializer = struct
   (* Temporary hack to test the rest of the code.  This materializer doesn't
      do anything interesting, it just materializes the entire expression, doing
      some limited expression deduplication.  It will cause an infinite loop on 
      queries with nested subqueries. *)
   type ds_history_t = ds_t list ref
   
   let materialize_one (history:ds_history_t) (prefix:string) (expr:expr_t) =
      if (rels_of_expr expr) = [] then ([], expr) else
      let (expr_ivars, expr_ovars) = schema_of_expr expr in
      Debug.print "LOG-COMPILE-DETAIL" (fun () -> 
         "Materializing expression: "^(string_of_expr expr)^"\n"^
         "Output Variables: ["^
            (ListExtras.string_of_list string_of_var expr_ovars)^"]"
      );
      let (found_ds,mapping_if_found) = 
         List.fold_left (fun result i ->
            if (snd result) <> None then result else
               (i, (cmp_exprs i.ds_definition expr))
            ) ({ds_name=CalcRing.one;ds_definition=CalcRing.one}, None) !history
      in begin match mapping_if_found with
         | None -> 
            let new_ds = {
               ds_name = CalcRing.mk_val (
                     External(
                        prefix,
                        expr_ivars,
                        expr_ovars,
                        type_of_expr expr,
                        None
                     )
                  );
               ds_definition = expr;
            } in
               history := new_ds :: !history;
               ([new_ds], new_ds.ds_name)
         
         | Some(mapping) ->
            Debug.print "LOG-COMPILE-DETAIL" (fun () -> 
               "Found Mapping to : "^(string_of_expr found_ds.ds_name)^
               "\nWith: "^
                  (ListExtras.ocaml_of_list (fun ((a,_),(b,_))->a^"->"^b) 
                                            mapping)
            );
            ([], rename_vars mapping found_ds.ds_name)
      end
   
   let materialize (history:ds_history_t) (prefix:string) 
                   (_:Schema.event_t option) (expr:expr_t) =
      let (expr_ivars, expr_ovars) = schema_of_expr expr in
      let subexprs = 
         List.map (fun (subexp_schema, subexp) ->
            let (_, subexp_ovars) = schema_of_expr subexp in
            if ListAsSet.seteq subexp_ovars subexp_schema then
               subexp
            else CalcRing.mk_val (AggSum(subexp_schema, subexp))
         ) 
         (snd 
            (CalculusDecomposition.decompose_graph expr_ivars (expr_ovars,expr))
         )  
      in
         fst (List.fold_left (fun (ret, i) subexp ->
            let (new_todos, new_term) = 
               materialize_one history (prefix^(string_of_int i)) subexp
            in
            ((new_todos @ (fst ret), 
             CalcRing.mk_prod [snd ret; new_term]), 
             i+1)
         ) (([], CalcRing.one), 1) subexprs)
      
end

module Materializer = TemporaryMaterializer
*)
(******************************************************************************)

type todo_list_t = ds_t list

(******************************************************************************)

let extract_renamings ((scope,schema):schema_t) (expr:expr_t): 
                      ((var_t * var_t) list * expr_t) =
   let (mappings, expr_terms) = 
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
      (mappings, Calculus.rename_vars mappings (CalcRing.mk_prod expr_terms))

(******************************************************************************)

let compute_init_at_start (table_rels:Schema.rel_t list) (expr:expr_t): expr_t =
   let table_names = List.map (fun (rn,_,_,_) -> rn) table_rels in
   Calculus.rewrite_leaves (fun _ lf -> match lf with
      | Rel(rn, rv, rt) ->
         if List.mem rn table_names
         then CalcRing.mk_val (Rel(rn,rv,rt))
         else CalcRing.zero
      | _ -> CalcRing.mk_val lf
   ) (CalculusTransforms.optimize_expr (Calculus.schema_of_expr expr) expr)


(******************************************************************************)

let compile_map (db_schema:Schema.t) (history:Heuristics.ds_history_t)
                (todo: ds_t): todo_list_t * compiled_ds_t =
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
      "Optimizing: "^(string_of_expr todo.ds_definition)
   );
   let optimized_defn = 
      CalculusTransforms.optimize_expr (todo_ivars, todo_ovars) 
                                      todo.ds_definition
   in
   Debug.print "LOG-COMPILE-DETAIL" (fun () ->
      "Optimized: "^(string_of_expr optimized_defn)
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
      List.partition (fun (_,_,t,_) -> t = Schema.TableRel) rels
   in
   let triggers = ref [] in
   let trigger_todos = ref [] in
   let events = (if (Debug.active "IGNORE-DELETES")
                 then [(fun x -> Schema.InsertEvent(x)), ""]
                 else [(fun x -> Schema.DeleteEvent(x)), "_m";
                       (fun x -> Schema.InsertEvent(x)), "_p"])
   in
   List.iter (fun (reln,relv,_,relt) -> List.iter (fun (mk_evt,evt_prefix) ->
               
      (***** THE FUN STUFF HAPPENS HERE *****)
      
      let map_prefix = todo_name^evt_prefix^reln in
      let prefixed_relv = List.map (fun (n,t) -> (map_prefix^n, t)) relv in
      let delta_event = mk_evt (reln, prefixed_relv, Schema.StreamRel, relt) in
      let delta_expr_unextracted = 
         CalculusTransforms.optimize_expr 
            (todo_ivars @ prefixed_relv,todo_ovars) 
            (CalculusDeltas.delta_of_expr delta_event optimized_defn)
      in
      let (delta_renamings, delta_expr) = 
         extract_renamings (prefixed_relv, todo_ovars) delta_expr_unextracted
      in
      Debug.print "LOG-COMPILE-DETAIL" (fun () ->
         "Delta: "^(Schema.string_of_event delta_event)^
            " DO "^(string_of_expr delta_expr)
      );

      let (new_todos, materialized_delta) = 
         Heuristics.materialize history map_prefix (Some(delta_event)) delta_expr
      in
         Debug.print "LOG-COMPILE-DETAIL" (fun () ->
            "Materialized: "^(string_of_expr materialized_delta)
         );
         trigger_todos := new_todos @ !trigger_todos;
         triggers      := 
            (delta_event, {
               Plan.target_map = 
                  Calculus.rename_vars delta_renamings todo.ds_name;
               Plan.update_type = Plan.UpdateStmt;
               Plan.update_expr = materialized_delta
            }) :: !triggers
      
      (**************************************)

   ) events) stream_rels;
   
   if table_rels <> [] then ( 
      (* If the expression contains tables, we need to initialize it at 
         startup *)
      let system_init_expr =
         compute_init_at_start (Schema.table_rels db_schema)
                               todo.ds_definition
      in
      if system_init_expr <> CalcRing.zero
      then triggers := 
         (Schema.SystemInitializedEvent, {
            Plan.target_map = todo.ds_name;
            Plan.update_type = Plan.ReplaceStmt;
            Plan.update_expr = system_init_expr
         }) :: !triggers
   );
   
   
   let (init_todos, init_expr) = 
      if todo_ivars <> [] then
         (* If the todo has input variables, it needs a default initializer *)
         let (init_todos,init_expr) =
            Heuristics.materialize history (todo_name^"_init") None 
                                     optimized_defn
         in (init_todos, Some(init_expr))
      else
         ([], None)
   
   in
      (((!trigger_todos) @ init_todos), {
         description = {
            ds_name = CalcRing.mk_val (External(
                  todo_name, todo_ivars, todo_ovars, todo_type, init_expr
               ));
            ds_definition = optimized_defn
         };
         ds_triggers = !triggers
      })

(******************************************************************************)
let compile_table ((reln, relv, relut, relt):Schema.rel_t): compiled_ds_t = 
   let map_name = (Plan.mk_ds_name ("_"^reln) ([],relv) relt) in {
      Plan.description = {
         Plan.ds_name       = map_name ;
         Plan.ds_definition = (CalcRing.mk_val (Rel(reln, relv, relt)))
      };
      Plan.ds_triggers = (List.map (fun (event, update_expr) ->
         (event, {
            Plan.target_map  = map_name;
            Plan.update_type = UpdateStmt;
            Plan.update_expr = update_expr
         }
      )) [(Schema.InsertEvent(reln, relv, relut, relt)), 
               CalcRing.one; 
          (Schema.DeleteEvent(reln, relv, relut, relt)), 
               CalcRing.mk_neg CalcRing.one]
      )
   }

(******************************************************************************)

let compile (db_schema:Schema.t) (queries:todo_list_t): plan_t =
   let todos  :todo_list_t ref           = ref queries in
   let history:Heuristics.ds_history_t = ref [] in
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
      heirarchy.  Only depth-first is correct without a topological sort 
      postprocessing step. 
      
      OAK: I'm not 100% convinced that this is the case.  It might be safer just 
      to do a topo sort postprocessing step regardless.
      *)
      todos     := new_todos @ !todos;
      plan      := (!plan) @ [compiled_ds]
   ) done; !plan

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