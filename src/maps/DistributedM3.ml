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

open Type
open Schema
open Plan
open M3

(**
   Compute the corrective trigger for a specific external and a specific base 
   event.
*)
let corrective_trigger ((en,eiv,eov,et,_):Calculus.external_t) 
                       (base_event:event_t) 
                       (base_stmts:stmt_t list): trigger_t =
   let reserved_names = ref (List.map fst (
      List.flatten (List.map (fun { target_map = tgt; update_expr = update } -> 
         (Calculus.all_vars tgt) @ (Calculus.all_vars update)
      ) base_stmts))
   ) in
   let mk_delta_name ((vn,vt):var_t) =
      let new_vn = FreshVariable.mk_safe_name !reserved_names ("delta_"^vn) in
         reserved_names := new_vn :: !reserved_names;
         (new_vn, vt)
   in
   let div = List.map mk_delta_name eiv in
   let dov = List.map mk_delta_name eov in
   let dval   = mk_delta_name (en,et) in
   let update_event = CorrectiveUpdate(en, div, dov, dval, base_event) in
   let all_delta_vars = Schema.event_vars update_event in
   {
      event = update_event;
      statements = ref (List.flatten (List.map (fun { target_map = tgt; 
                                    update_type = upd_type;
                                    update_expr = expr } ->
         Debug.print "LOG-CORRECTIVE-TRIGGER" (fun () ->
            "[DistributedM3] "^(Schema.string_of_event update_event)^"\n"^
            "vars: "^(ListExtras.ocaml_of_list string_of_var all_delta_vars)
         );
         let delta_expr = (CalculusDeltas.delta_of_expr update_event expr) in
         if delta_expr = Calculus.CalcRing.zero then []
         else 
         let expr_schema =
            (  all_delta_vars,
               ListAsSet.diff (snd (Calculus.schema_of_expr expr)) 
                              all_delta_vars
            ) in
         let opt_delta_expr = 
            CalculusTransforms.optimize_expr expr_schema delta_expr
         in
         let (renamings, extracted_delta_expr) = 
            Compiler.extract_renamings expr_schema opt_delta_expr
         in
            [{ target_map = Calculus.rename_vars renamings tgt; 
               update_type = upd_type;
               update_expr = extracted_delta_expr
            }]
      ) base_stmts))
   }

(**
   Compute all the corrective triggers for a specific base trigger 
*)
let corrective_triggers_for_event
       (maps_by_name:(string * Calculus.external_t) list)
       ({ event = base_event; statements = base_stmts }: trigger_t): 
            trigger_t list =
   let externals_in_event = 
      List.map (fun x -> List.assoc x maps_by_name) (
         List.flatten (List.map (fun { update_expr = update } -> 
            (Calculus.externals_of_expr update)
         ) !base_stmts)
      )
   in
      List.map (fun ext_name ->
         corrective_trigger ext_name base_event !base_stmts
      ) externals_in_event

(**
   Extend a specified M3 program with corrective update triggers.  This will 
   allow it to be run in a distributed setting.
*)
let distributed_m3_of_m3 (prog:prog_t): prog_t =
   let maps_by_name = List.flatten (List.map (fun m ->
      match m with 
       | DSTable _ -> []
       | DSView({ds_name = ds}) -> 
         let external_info = expand_ds_name ds in
         let (en, _, _, _, _) = external_info in
            [en, external_info]
      ) !(prog.maps))
   in
   {
      queries = prog.queries;
      
      maps = prog.maps;
      
      db = prog.db;
      
      triggers = ref (
         (!(prog.triggers)) @
         (List.flatten (List.map (corrective_triggers_for_event maps_by_name)
                                 !(prog.triggers)))
      )
   }