open Ring
open Arithmetic
open Types
open Calculus
open Statement

module C = Calculus

type trigger_t = {
   event      : Schema.event_t;
   statements : stmt_t list ref
}

type view_t = {
   name       : string;
   schema     : C.schema_t;
   definition : C.expr_t;
}

type ds_t = 
   | DSView  of view_t
   | DSTable of Schema.rel_t

type prog_t = {
   queries   : (string * C.expr_t) list ref;
   tables    : ds_t list ref;
   triggers  : trigger_t list ref;
   db        : Schema.t
}

let init (db:Schema.t): prog_t = 
   let (db_tables, db_streams) = 
      List.partition (fun (_,_,t,_) -> t = Schema.TableRel)
                     (Schema.rels db)
   in
   {  queries = ref [];
      tables   = ref (List.map (fun x -> DSTable(x)) db_tables);
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
   {  queries = ref []; tables = ref []; triggers = ref []; 
      db = Schema.empty_db () }
;;

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
      prog.tables   := (DSTable(rel)) :: !(prog.tables)
   else
      prog.triggers := { event = (Schema.InsertEvent, rel); statements=ref [] }
                    :: { event = (Schema.DeleteEvent, rel); statements=ref [] }
                    :: !(prog.triggers)
;;

let add_query (prog:prog_t) (name:string) (expr:C.expr_t): unit =
   prog.queries := (name, expr) :: !(prog.queries)
;;

let add_view (prog:prog_t) (view:view_t): unit =
   prog.tables := (DSView(view)) :: !(prog.tables)
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
         (C.find_safe_var_mapping 
            (C.find_safe_var_mapping 
               (List.combine relv trig_relv) 
               stmt.update_expr)
            stmt.target_map)
      in
      trigger.statements := !(trigger.statements) @ [{
         target_map = C.rename_vars safe_mapping stmt.target_map;
         update_type = stmt.update_type;
         update_expr  = C.rename_vars safe_mapping stmt.update_expr
      }]
   with Not_found -> 
      failwith "Adding statement for an event that has not been established"
      