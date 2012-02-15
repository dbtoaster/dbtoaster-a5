open Ring
open Arithmetic
open Types
open Calculus
open Plan

type trigger_t = {
   event      : Schema.event_t;
   statements : Plan.stmt_t list ref
}

type incremental_view_t = {
   view_description : Plan.ds_t;
   view_metadata    : Plan.ds_metadata_t
}

type map_t = 
   | DSView  of incremental_view_t
   | DSTable of Schema.rel_t

type prog_t = {
   queries   : (string * expr_t) list ref;
   maps      : map_t list ref;
   triggers  : trigger_t list ref;
   db        : Schema.t
}

(************************* Stringifiers *************************)

let string_of_map ?(is_query=false) (map:map_t): string = begin match map with
   | DSView(view) -> 
      "DECLARE "^
      (if view.view_metadata.init_on_access then "PARTIAL " else "")^
      (if view.view_metadata.init_at_start then "INITIALIZED " else "")^
      (if is_query then "QUERY " else "")^
      "MAP "^(Calculus.string_of_expr (
         Calculus.strip_calc_metadata view.view_description.ds_name))^
      " := \n    "^
      (Calculus.string_of_expr view.view_description.ds_definition)^";"
   | DSTable(rel) -> Schema.code_of_rel rel
   end

let string_of_trigger (trigger:trigger_t): string = 
   (Schema.string_of_event trigger.event)^" {"^
   (ListExtras.string_of_list ~sep:"" (fun stmt ->
      "\n   "^(Plan.string_of_statement stmt)^";"
   ) !(trigger.statements))^"\n}"

let string_of_m3 (prog:prog_t): string = 
   "-------------------- SOURCES --------------------\n"^
   (Schema.code_of_schema prog.db)^"\n\n"^
   "--------------------- MAPS ----------------------\n"^
   (* Skip Table maps -- these are already printed above in the schema *)
   (ListExtras.string_of_list ~sep:"\n\n" string_of_map (List.filter (fun x ->
      match x with DSTable(_) -> false | _ -> true) !(prog.maps)))^"\n\n"^
   "-------------------- QUERIES --------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (qname,qdefn) ->
      "DECLARE QUERY "^qname^" := "^(Calculus.string_of_expr qdefn)^";"
   ) !(prog.queries))^"\n\n"^
   "------------------- TRIGGERS --------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" string_of_trigger !(prog.triggers))


(************************* Accessors/Mutators *************************)

let get_trigger (prog:prog_t) 
                (event_type:Schema.event_type_t) 
                (event_name:string): trigger_t =
   List.find (fun trig ->
               let (trig_type, (trig_name, _,_,_)) = trig.event in
                  (trig_type = event_type) && (trig_name = event_name)
             ) !(prog.triggers)
;;

let add_rel (prog:prog_t) ?(source = Schema.NoSource)
                          ?(adaptor = ("", []))
                          (rel:Schema.rel_t): unit = 
   Schema.add_rel prog.db ~source:source ~adaptor:adaptor rel;
   let (_,_,t,_) = rel in if t = Schema.TableRel then
      prog.maps     := (DSTable(rel)) :: !(prog.maps)
   else
      prog.triggers := { event = (Schema.InsertEvent, rel); statements=ref [] }
                    :: { event = (Schema.DeleteEvent, rel); statements=ref [] }
                    :: !(prog.triggers)
;;

let add_query (prog:prog_t) (name:string) (expr:expr_t): unit =
   prog.queries := (name, expr) :: !(prog.queries)
;;

let add_view (prog:prog_t) (view:incremental_view_t): unit =
   prog.maps := (DSView(view)) :: !(prog.maps)
;;

let add_stmt (prog:prog_t) ((event_type,event_rel):Schema.event_t)
                           (stmt:stmt_t): unit =
   let (reln, relv, relvt, relt) = event_rel in
   try
      let trigger = get_trigger prog event_type reln in
      let (_,(_,trig_relv,_,_)) = trigger.event in
      (* We need to ensure that we're not clobbering any existing variable names
         with these rewrites.  This includes not just the update expression, 
         but also any IVC computations present in the target map reference *)
      let safe_mapping = 
         (find_safe_var_mapping 
            (find_safe_var_mapping 
               (List.combine relv trig_relv) 
               stmt.update_expr)
            stmt.target_map)
      in
      trigger.statements := !(trigger.statements) @ [{
         target_map = rename_vars safe_mapping stmt.target_map;
         update_type = stmt.update_type;
         update_expr  = rename_vars safe_mapping stmt.update_expr
      }]
   with Not_found -> 
      failwith "Adding statement for an event that has not been established"

(************************* Initializers *************************)

let init (db:Schema.t): prog_t = 
   let (db_tables, db_streams) = 
      List.partition (fun (_,_,t,_) -> t = Schema.TableRel)
                     (Schema.rels db)
   in
   {  queries = ref [];
      maps    = ref (List.map (fun x -> DSTable(x)) db_tables);
      triggers = 
         ref (List.map (fun x -> { event = x; statements = ref [] })
                       (List.flatten 
                          (List.map (fun x -> 
                                       [  Schema.InsertEvent,x; 
                                          Schema.DeleteEvent,x])
                                    db_streams)));
      db = db
   }
;;

let empty_prog (): prog_t = 
   {  queries = ref []; maps = ref []; triggers = ref []; 
      db = Schema.empty_db () }
;;

let plan_to_m3 (db:Schema.t) (plan:Plan.plan_t):prog_t =
   let prog = init db in
      List.iter (fun (ds:Plan.compiled_ds_t) ->
         add_view prog {
            view_description = ds.description;
            view_metadata    = ds.metadata
         };
         List.iter (fun (event, stmt) ->
            add_stmt prog event stmt
         ) (ds.ds_triggers)
      ) plan;
   prog
;;

