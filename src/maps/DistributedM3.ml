(**
   Tools and functions for distributed execution of M3 programs.
   
   The core functionality supported here at the moment is supporting eventual 
   consistency via Schema.event_t's CorrectiveUpdate.  The idea is that we want 
   to evaluate statements before we're guaranteed that the data that the 
   statements are being evaluated on is consistent.  If it's not consistent, 
   then we need to have a mechanism to correct any mistakes that we later 
   discover.  
   
   Corrective update triggers are this mechanism.  Each corrective update is 
   associated with an existing trigger, as well as an external that is read from
   by that trigger.  If we discover an error, we run the corrective update to 
   fix the mistake (and propagate any further corrections as-needed).
*)

open Types
open Schema
open Plan
open M3

(**
   Compute the corrective trigger for a specific external and a specific base 
   event.
*)
let corrective_trigger (ext_name:string) (base_event:event_t) 
                       (base_stmts:stmt_t list): trigger_t =
   let reserved_names =
      List.flatten (List.map (fun { target_map = tgt; update_expr = update } -> 
         (Calculus.externals_of_expr tgt) @ (Calculus.externals_of_expr update)
      ) base_stmts)
   in
   let delta_ext_name = 
      FreshVariable.mk_safe_name reserved_names ("delta_"^ext_name)
   in
   let update_event = CorrectiveUpdate(ext_name, delta_ext_name, base_event) in
   {
      event = update_event;
      statements = ref (List.flatten (List.map (fun { target_map = tgt; 
                                    update_type = upd_type;
                                    update_expr = expr } ->
         let delta_expr = (CalculusDeltas.delta_of_expr update_event expr) in
         if delta_expr = Calculus.CalcRing.zero then []
         else 
            [{ target_map = tgt; 
               update_type = upd_type;
               update_expr = delta_expr
            }]
      ) base_stmts))
   }

(**
   Compute all the corrective triggers for a specific base trigger 
*)
let corrective_triggers_for_event ({ event = base_event;
                                     statements = base_stmts }: trigger_t):
                                  trigger_t list =
   let externals_in_event = 
      List.flatten (List.map (fun { update_expr = update } -> 
         (Calculus.externals_of_expr update)
      ) !base_stmts)
   in
      List.map (fun ext_name ->
         corrective_trigger ext_name base_event !base_stmts
      ) externals_in_event

(**
   Extend a specified M3 program with corrective update triggers.  This will 
   allow it to be run in a distributed setting.
*)
let distributed_m3_of_m3 (prog:prog_t): prog_t =
   {
      queries = prog.queries;
      
      maps = prog.maps;
      
      db = prog.db;
      
      triggers = ref (
         (!(prog.triggers)) @
         (List.flatten (List.map corrective_triggers_for_event 
                                 !(prog.triggers)))
      )
   }