open Ring
open Arithmetic
open Types
open Calculus
open Plan
open M3

type prog_t = {
   maps      : map_t list ref; (**
      The set of datastructures (Maps) that we store.  These include:
      Views: Mutable datastructures representing the result of a query.  Views
             may appear on the left-hand side of a trigger statement, and are
             referenced in queries using '[External[...][...]]'
      Tables: Immutable datastructures loaded in at the start of processing.
              Tables may not be updated once processing has started, and are
              referenced in queries using [Rel(...)]
   *)
   
   triggers  : trigger_t list ref; (**
      Triggers for each event that can be handled by this program.  These
      use event times as defined in Schema, and statements (as defined in Plan)
      of the form [External[...][...]] ([:=] OR [+=]) CalculusExpression.  When 
      the triggerring event occurs, the specified CalculusExpression will be 
      evaluated (with the trigger's parameters in-scope), and used to update the
      specified view (which again, is sliced with the trigger's parameters in-
      scope).  
      
      The calculus expression may only contain [Rel(...)] terms referencing 
      [TableRel]s.
      
      Every possible trigger that can occur in this program *must* be defined,
      even if it has no accompanying statements.  This means both an Insert
      and a Delete trigger for every [StreamRel].
   *)
}

let empty_prog (): prog_t = 
   {  maps = ref []; triggers = ref (default_triggers ()) }

let get_map_information_from_expr (expr: Calculus.expr_t): Calculus.external_t = 
   begin match expr with
   | CalcRing.Val(v) -> 
      begin match v with
      | External(ename,eins,eouts,etype,emeta) ->
         (ename,eins,eouts,etype,emeta)
      | _ -> failwith "Expression should be map!"
      end
   | _ -> failwith "Expression should be leaf!"
   end

let get_map_postfix postfix (expr: Calculus.expr_t): Calculus.expr_t =
   let (ename,eins,eouts,etype,emeta) = get_map_information_from_expr expr in
      CalcRing.Val(External(ename^postfix, eins, eouts, Types.TInt, None))

let get_map_status (expr: Calculus.expr_t): Calculus.expr_t = 
   get_map_postfix "_status" expr

let get_map_input_output_domain ?(input = false) (expr: Calculus.expr_t): Calculus.expr_t = 
   let (ename,eins,eouts,etype,emeta) = get_map_information_from_expr expr in
   let out_vars = if input then  []       else eouts     in
   let in_vars  = if input then eins      else  []       in
   let postfix  = if input then "_input"  else "_output" in
      CalcRing.Val(External(ename^postfix, in_vars, out_vars, Types.TInt, None))

let get_map_output_domain (expr: Calculus.expr_t): Calculus.expr_t =
   get_map_input_output_domain ~input:false expr
        
let get_map_input_domain (expr: Calculus.expr_t): Calculus.expr_t =
   get_map_input_output_domain ~input:true expr

let get_relation_of_event (event_input: Schema.event_t): Schema.rel_t =
    begin match event_input with
        | Schema.InsertEvent(rel) -> rel
        | Schema.DeleteEvent(rel) -> rel
        | _ -> failwith ("incorrect event!")
    end
        
let get_singleton_tuple (relation: Schema.rel_t): Calculus.expr_t =
    let (rname, rvars, _) = relation in
        CalcRing.Val(External(rname^"_singleton", [], rvars, Types.TInt, None))

let singleton_tuple_is_for_event (event_input: Schema.event_t) (test_singleton_tuple: Calculus.expr_t): bool = 
   let single_tuple = get_singleton_tuple (get_relation_of_event event_input) in
   Debug.print "CHECK-M3DM" (fun () -> (Calculus.string_of_expr single_tuple)^" =?= "^(Calculus.string_of_expr test_singleton_tuple) );
   single_tuple = test_singleton_tuple

let get_calc_for_event (event_input: Schema.event_t) = 
   begin match event_input with 
      | Schema.InsertEvent(_) -> CalcRing.one
      | Schema.DeleteEvent(_) -> CalcRing.Neg(CalcRing.one)
      | _ -> failwith ("incorrect event!")
   end
        
let get_relation_vars (expr: Calculus.expr_t): var_t list = 
    snd (Calculus.schema_of_expr expr)
        
let is_calculus_relation (expr: Calculus.expr_t): bool = 
    let leaf = 
            begin match expr with
            | CalcRing.Val(x) -> x
            | _ -> CalcRing.get_val CalcRing.one
            end in
        begin match leaf with
        | Rel(_, _) ->
            true
        | _ -> false
        end

      
let rec simplify_formula (event_input: Schema.event_t) (expr: Calculus.expr_t): Calculus.expr_t * bool = (* First return is the simplified query and the second one is whether it should be removed or not! *)
    Debug.print "CHECK-M3DM" (fun () -> "simplifying "^(Calculus.string_of_expr expr) );
    let should_be_removed = (CalcRing.zero, false) in
    let rec aux e = 
       begin match e with
           | CalcRing.Sum([q1;q2]) -> 
               let (rq1, rb1) = aux(q1) in
               let (rq2, rb2) = aux(q2) in
               if rb1 = true then 
                   if rb2 = true then
                       (CalcRing.mk_sum([rq1; rq2]), true)
                   else
                       (rq1, true)
               else
                   if rb2 = true then
                       (rq2, true)
                   else
                       should_be_removed 
           | CalcRing.Sum(q1::qo) -> 
               let (rq1, rb1) = aux(q1) in
               let (rq2, rb2) = aux(CalcRing.mk_sum(qo)) in
               if rb1 = true then 
                   if rb2 = true then
                       (CalcRing.mk_sum([rq1; rq2]), true)
                   else
                       (rq1, true)
               else
                   if rb2 = true then
                       (rq2, true)
                   else
                       should_be_removed
           | CalcRing.Prod([q1;q2]) -> 
               let (rq1, rb1) = aux(q1) in
               let (rq2, rb2) = aux(q2) in
               if rb1 = true then 
                   if rb2 = true then
                       (CalcRing.mk_prod([rq1; rq2]), true)
                   else
                       (rq1, true)
               else
                   if rb2 = true then
                       (rq2, true)
                   else
                       should_be_removed
           | CalcRing.Prod(q1::qo) -> 
               let (rq1, rb1) = aux(q1) in
               let (rq2, rb2) = aux(CalcRing.mk_prod(qo)) in
               if rb1 = true then 
                   if rb2 = true then
                       (CalcRing.mk_prod([rq1; rq2]), true)
                   else
                       (rq1, true)
               else
                   if rb2 = true then
                       (rq2, true)
                   else
                       should_be_removed         
           | CalcRing.Val(leaf) ->
               begin match leaf with
               | AggSum(gb_vars, subexp) -> 
                   if gb_vars = [] then 
                       should_be_removed
                   else 
                       let (rq, rb) = aux(subexp) in
                           if rb = true then 
                               (CalcRing.Val(AggSum(gb_vars, rq)), true) 
                           else 
                               should_be_removed
               | Rel(rname, rvars)    -> 
                   if rvars = [] then should_be_removed else (e, true)
               | External(ename,eins,eouts,etype,emeta) ->
                   if eouts = [] then 
                        should_be_removed 
                   else 
                        if (singleton_tuple_is_for_event event_input e) then
                           (get_calc_for_event event_input, true)
                        else
(*
                           let trigger_args = Schema.event_vars event_input in
                              if (ListAsSet.diff eouts trigger_args = []) then
                                 should_be_removed
                              else
*)
                                 (e, true)
               | _ -> failwith ("Incorrect leaf")
               end
           | _ -> failwith ("Incorrect formula")
       end
      in
         aux expr

let mk_dm_trigger (left_domain: Calculus.expr_t)
                    (update_domain: Calculus.expr_t):Plan.stmt_t= 
    {
        target_map = left_domain;
        update_type = Plan.UpdateStmt;
        update_expr = update_domain
    }

        
let simplify_dm_trigger (event_input: Schema.event_t) (trigger: Plan.stmt_t): Plan.stmt_t list = 
    let left_domain = trigger.target_map in
    let update_domain = trigger.update_expr in 
    let schema_left_domain = snd (Calculus.schema_of_expr left_domain) in
    if schema_left_domain = [] then [] else
        let (simplified_update_domain, _) = simplify_formula event_input (update_domain) in
            [mk_dm_trigger (left_domain) (simplified_update_domain)]
    

let simplify_dm_triggers (event_input: Schema.event_t) (trigger_list: Plan.stmt_t list): Plan.stmt_t list = 
   List.fold_left (fun (x) (y) -> x@(simplify_dm_trigger event_input y)) [] trigger_list
   (*trigger_list*)
    
        


let rec maintain (context: Calculus.expr_t)
                (formula: Calculus.expr_t) : Plan.stmt_t list * Calculus.expr_t =
    begin match formula with
    | CalcRing.Sum([q1;q2]) -> 
        let (trlist1, context1) = maintain(context)(q1) in
            let (trlist2, context2) = maintain(context)(q2) in 
                (trlist1 @ trlist2, CalcRing.mk_sum([context1; context2]))
    | CalcRing.Sum(q1::qo) -> 
        let (trlist1, context1) = maintain(context)(q1) in
            let (trlist2, context2) = maintain(context)(CalcRing.mk_sum qo) in 
                (trlist1 @ trlist2, CalcRing.mk_sum([context1; context2]))
    | CalcRing.Prod([q1;q2]) -> 
        let (trlist1, context1) = maintain(context)(q1) in
            let (trlist2, context2) = maintain(context1)(q2) in 
                (trlist1 @ trlist2, context2)
    | CalcRing.Prod(q1::qo) -> 
        let (trlist1, context1) = maintain(context)(q1) in
            let (trlist2, context2) = maintain(context1)(CalcRing.mk_prod qo) in 
                (trlist1 @ trlist2, context2)
    | CalcRing.Neg(q1) ->
        maintain(context)(q1)
    | CalcRing.Val(leaf) ->
        begin match leaf with
        | Value(v) -> ([], context)
        | External(ename,eins,eouts,etype,emeta) ->
            let input_domain = get_map_input_domain (formula) in
            let output_domain = get_map_output_domain (formula) in
            let input_vars = get_relation_vars input_domain in
            let context1 = CalcRing.mk_prod ([context; output_domain]) in
            let update_domain = CalcRing.Val(AggSum(input_vars, context))  in
            let dm_statement = mk_dm_trigger (input_domain) (update_domain) in
                ([dm_statement], context1)
        | AggSum(gb_vars, subexp) -> 
            let (trlist, context1) = maintain (context) (subexp) in
                let right_context = CalcRing.Val(AggSum(gb_vars, context1)) in
                    (trlist, CalcRing.mk_prod ([context; right_context]))
        | Rel(rname, rvars)    -> 
            ([], CalcRing.mk_prod ([context; formula]))
        | Cmp(op,subexp1,subexp2) -> 
            let right_context = CalcRing.Val(leaf) in
                ([], CalcRing.mk_prod ([context; right_context]))
           (* failwith ("comparison not supported!") *) (*TODO*)
        | Lift(target, subexp)    -> 
            let (trlist, context1) = maintain(context)(subexp) in
                (trlist, context1) (*FIXME*)
        end
    | _ -> failwith ("Incorrect formula")
    end

let maintain_statement (statement:Plan.stmt_t)
                        (relation: Schema.rel_t):Plan.stmt_t list = 
      let singleton_tuple = get_singleton_tuple (relation) in 
(*    let singleton_tuple = CalcRing.one in *)
    let left_input_domain = get_map_input_domain (statement.target_map) in
    let left_output_domain = get_map_output_domain (statement.target_map) in
    let left_output_vars = get_relation_vars left_output_domain in
    let (trigger_list, update_domain) = 
        let context = CalcRing.mk_prod ([singleton_tuple; left_input_domain]) in
        let (trigger_list, query_domain) = maintain (context) (statement.update_expr) in
        let left_output_vars_wo_triggers = ListAsSet.diff left_output_vars (get_relation_vars singleton_tuple) in
         (trigger_list, 
            if left_output_vars_wo_triggers <> [] then 
               CalcRing.Val(AggSum(left_output_vars_wo_triggers, query_domain)) 
            else
               query_domain
         ) in
    let dm_statement = mk_dm_trigger (left_output_domain) (update_domain) in
        dm_statement :: trigger_list


let maintain_all (relation: Schema.rel_t)
                (stmts: Plan.stmt_t list)
                (event_input: Schema.event_t): trigger_t = 
    let dm_statements = ref [] in
    let dm_trigger: trigger_t = {event = event_input; statements = dm_statements} in
    let singleton_tuple = get_singleton_tuple relation in
    if not (Debug.active "DEBUG-DM") then
      dm_statements := [mk_dm_trigger singleton_tuple singleton_tuple]
    else
      ();
    List.iter (fun (statement:Plan.stmt_t) ->
            let dm_statement = maintain_statement (statement) (relation) in
            dm_statements := !dm_statements@dm_statement
        ) stmts;
        if not (Debug.active "NOOPT-M3DM") then
            dm_statements := simplify_dm_triggers event_input !dm_statements
        else 
            ();
    dm_trigger



let make_DM_triggers (m3_triggers: trigger_t list): trigger_t list =
    let dm_triggers = ref [] in
        List.iter (fun (trigger:trigger_t) ->
            try
                let relation = get_relation_of_event (trigger.event) in
            	let dm_trigger = maintain_all (relation) (!(trigger.statements)) (trigger.event) in
            	dm_triggers := !dm_triggers@[dm_trigger]
            with _ -> ()
        ) m3_triggers;
    !dm_triggers

let make_DM_maps (m3_db: Schema.t) (m3_maps: map_t list): map_t list = 
   let dm_maps = ref [] in
   let add_map expr = (dm_maps := DSView({ds_name = expr; ds_definition = expr})::(!dm_maps)) in
      List.iter ( 
         fun (map: map_t) ->
         match map with
         | DSView(ds) ->
            let (ename,eins,eouts,etype,emeta) = Plan.expand_ds_name ds.ds_name in
               if eins <> [] then
                  let map_input = get_map_input_domain ds.ds_name in
                  add_map map_input; 
                  if Debug.active "DEBUG-DM" then
                     add_map (get_map_status map_input)
                  else ()
               else
                  ();
               if eouts <> [] then
                  let map_output = get_map_output_domain ds.ds_name in
                  add_map map_output;
                  if Debug.active "DEBUG-DM" then
                     add_map (get_map_status map_output)
                  else ()
               else 
                  ();
         | DSTable(rel) ->
            (*add_map (get_singleton_tuple rel)*)
            ()
      ) m3_maps;
      List.iter (
         fun (rel: Schema.rel_t) ->
            add_map (get_singleton_tuple rel)
      ) (Schema.rels m3_db);

      !dm_maps

let m3_to_m3dm (m3_prog: M3.prog_t): prog_t = 
   let dm_triggers = make_DM_triggers !(m3_prog.M3.triggers) in
   let dm_maps = make_DM_maps (m3_prog.M3.db) !(m3_prog.M3.maps) in
   let dm_prog: prog_t = {triggers = ref dm_triggers; maps = ref dm_maps} in
      dm_prog


let string_of_m3DM (prog:prog_t): string =
   "--------------------- DM MAPS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" string_of_map !(prog.maps))^"\n\n"^
   "------------------- DM TRIGGERS --------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" string_of_trigger !(prog.triggers))
