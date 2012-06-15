(** Module for translating a M3DM program into a K3 program. *)

(**/**)
open Arithmetic
open Calculus
open M3ToK3

module T = Types
module V = Arithmetic.ValueRing
module C = Calculus.CalcRing
module K = K3

type stmt_info_t = string * K.expr_t list * K.expr_t * K.expr_t list * K.expr_t list * T.type_t * K.expr_t list * K.expr_t * K.expr_t

let is_ivc_constant = one_int_val
let is_gc_constant  = minus_one_int_val
let is_normal_constant = zero_int_val

let stmt_info_list:((Plan.stmt_t, stmt_info_t) Hashtbl.t) = Hashtbl.create 10

let get_stmt_info_from_dm_stmt (dm_stmt: Plan.stmt_t) : (stmt_info_t) = 
    Hashtbl.find stmt_info_list dm_stmt

let get_stmt_info (meta: meta_t) (dm_stmt: Plan.stmt_t) trig_args : (stmt_info_t * meta_t) = 
    let (mapn, lhs_ins, lhs_outs, map_type, init_calc_opt) = Plan.expand_ds_name dm_stmt.Plan.target_map in
    let {Plan.update_type = update_type; Plan.update_expr = incr_calc} = dm_stmt in 

    let lhs_collection  = map_to_expr mapn lhs_ins lhs_outs map_type in

    let map_k3_type = K.TBase(map_type) in

    let lhs_ins_el = varIdType_to_k3_expr lhs_ins in
    let lhs_outs_el = varIdType_to_k3_expr lhs_outs in
    let trig_args_el = varIdType_to_k3_expr trig_args in

    let incr_result, new_meta = calc_to_k3_expr ~generate_init:true meta trig_args_el incr_calc in
    let (rhs_outs_el, rhs_ret_ve, incr_expr) = incr_result in
    assert (ListAsSet.seteq (ListAsSet.diff lhs_outs_el trig_args_el) rhs_outs_el);
    assert (compatible_types (type_of_kvar rhs_ret_ve) map_k3_type);
    let stmt_info = 
        (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr)
    in
    Hashtbl.replace stmt_info_list dm_stmt stmt_info;
        stmt_info, new_meta

let get_map_from_schema (map_name: string) (k3_prog_schema: K.map_t list): K.map_t =
    let (mapn, lhs_ins, lhs_outs, map_type) = 
    List.find 
    (  
        fun (mapn, input_vars, output_vars, map_type ) -> 
        mapn = map_name
    ) k3_prog_schema
    in 
        (mapn, lhs_ins, lhs_outs, map_type)
        
let get_map_expr_from_schema (collection_name: string) (k3_prog_schema: K.map_t list): K.expr_t =
    let (mapn, lhs_ins, lhs_outs, map_type) = 
          get_map_from_schema (collection_name) (k3_prog_schema) 
    in
        map_to_expr mapn lhs_ins lhs_outs map_type

let get_collection_status (collection_name: string) (k3_prog_schema: K.map_t list): K.expr_t =
   let collection_status_name = collection_name ^ "_status" in
      let (mapn, lhs_ins, lhs_outs, map_type) = 
          get_map_from_schema (collection_status_name) (k3_prog_schema) 
      in
         map_to_expr mapn lhs_ins lhs_outs map_type
         
let get_map_vars (collection_name: string) (k3_prog_schema: K.map_t list): T.var_t list * T.var_t list =
    let (mapn, lhs_ins, lhs_outs, map_type) = 
        get_map_from_schema (collection_name) (k3_prog_schema) 
    in
        lhs_ins, lhs_outs
        
let string_ends_with (source: string) (test: string): bool = 
    let source_length = String.length source in
    let test_length     = String.length test in
    if source_length < test_length then
        false
    else
        let source_last_part = String.sub source (source_length - test_length) test_length in
        source_last_part = test
        
let collection_ivc_gc_var stmt_info =
    let (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr) = stmt_info in
    let domain_type = K.TBase(Types.TInt) in
    let free_lhs_outs_el = ListAsSet.diff lhs_outs_el trig_args_el in
    let collection_ivc_gc_t  = 
        if free_lhs_outs_el = [] then
            K.TTuple([domain_type;domain_type])
        else
            K.Collection(K.Unknown, K.TTuple( (k3_expr_to_k3_type (free_lhs_outs_el))@[domain_type; domain_type] )) 
    in
        K.Var("cig_"^mapn, collection_ivc_gc_t)

let collection_ivc_gc_generate stmt_info =
    let (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr) = stmt_info in

    let zero_value = init_val_from_type map_type in
    let previous_value = 
        K.IfThenElse(  K.Member(lhs_collection, lhs_outs_el),
                    K.Lookup(lhs_collection, lhs_outs_el),
                    zero_value 
         ) in
    let delta_value = rhs_ret_ve in
    let new_value = K.Add(previous_value, delta_value) in
    let is_ivc_result = K.Tuple(rhs_outs_el@[is_ivc_constant;new_value]) in
    let is_normal_result = K.Tuple(rhs_outs_el@[is_normal_constant;new_value]) in
    let is_gc_result = K.Tuple(rhs_outs_el@[is_gc_constant;new_value]) in
    let ivc_gc_specifier =
        K.IfThenElse(  K.Eq(previous_value, zero_value), 
                       K.IfThenElse(  K.Neq(delta_value, zero_value), 
                                      is_ivc_result,
                                      is_normal_result
                       ),
                       K.IfThenElse(  K.Eq(new_value, zero_value), 
                                      is_gc_result,
                                      is_normal_result
                       )
        )
        in
    let collection_ivc_gc = 
        let inner_loop_body = lambda (rhs_outs_el@[rhs_ret_ve]) ivc_gc_specifier in
        if rhs_outs_el <> [] then
            K.Map(inner_loop_body, incr_expr)
        else
            K.Apply(inner_loop_body, incr_expr)
    in
        (collection_ivc_gc)
      
let update_status_statement_generate k3_prog_schema stmt_info =
    let (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr) = stmt_info in
        
    let collection_ivc_gc_var = 
        collection_ivc_gc_var stmt_info in

    let collection_status = get_collection_status mapn k3_prog_schema in
    let update_status_lambda_body = 
        let new_status_value = rhs_ret_ve in
        K.PCValueUpdate(collection_status, [], lhs_outs_el, 
            new_status_value
        )
    in
    let multi_ret = K.Var("multi_ret",K.TBase(T.TInt))
    in
    let update_status_lambda = lambda (rhs_outs_el@[rhs_ret_ve; multi_ret]) update_status_lambda_body in
    if rhs_outs_el = [] then   
        K.Apply(  update_status_lambda,collection_ivc_gc_var)    
    else                       
        K.Iterate(update_status_lambda,collection_ivc_gc_var)  
         
let ivc_do (map_name_domain: string) (vars: K.expr_t list)  (k3_prog_schema: K.map_t list): K.expr_t =
    Debug.print "DEBUG-DM-LOG" (fun () -> "ivc_statement_generate: "^map_name_domain);
    let input_postfix = "_input" in let output_postfix = "_output" in
    (* in_out is true if it's an input, otherwise false*)  
    let in_out = (string_ends_with map_name_domain input_postfix) in
    let postfix = if in_out then input_postfix else output_postfix in
    let map_name = 
        String.sub map_name_domain 0 
            (
                (String.length map_name_domain) -
                (String.length postfix)
            ) in
    let map_schema          =   get_map_vars                (map_name)  (k3_prog_schema) in
    let (_, _, _, map_type) =   get_map_from_schema         (map_name)  (k3_prog_schema) in
    let map_expr            =   get_map_expr_from_schema    (map_name)  (k3_prog_schema) in
    let ivc_statement = 
	    match (pair_map varIdType_to_k3_expr map_schema) with
	    | x,    []    -> K.PCValueUpdate(map_expr, vars, [], init_val_from_type map_type) 
	    | [],    y    -> K.PCValueUpdate(map_expr, [], vars, init_val_from_type map_type)
	    | x,    y     -> failwith "Map with both variables is not supported"
	in
	   (* in order to solve the problem when something is initiliazed in ON SYSTEM *)
	   K.IfThenElse(K.Member(map_expr, vars),
	       get_unit_statement (),
	       ivc_statement
	   )
	    (*** FIXME ***) (*only supports maps with output *)
	    (* that's not a good idea! another way should be used to handle this *)
	   
let ivc_statement_generate k3_prog_schema stmt_info =
    let (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr) = stmt_info in
     
    let dummy_statement = 
        get_unit_statement ()
    in
    
    let collection_ivc_gc_var = 
        collection_ivc_gc_var stmt_info in
    
    let ivc_statement_main = ivc_do mapn lhs_outs_el k3_prog_schema in
    let ivc_statement_lambda_body = 
        let new_status_value = rhs_ret_ve in
        K.IfThenElse(
            K.Eq(new_status_value, is_ivc_constant), 
            ivc_statement_main,
            dummy_statement
        )
    in
    let multi_ret = K.Var("multi_ret",K.TBase(T.TInt))
    in
    let ivc_statement_lambda = lambda (rhs_outs_el@[rhs_ret_ve; multi_ret]) ivc_statement_lambda_body in
    if rhs_outs_el = [] then   
        K.Apply(  ivc_statement_lambda,collection_ivc_gc_var)    
    else                       
        K.Iterate(ivc_statement_lambda,collection_ivc_gc_var)
        
let gc_do (map_name_domain: string) (vars: K.expr_t list)  (k3_prog_schema: K.map_t list): K.expr_t =
    Debug.print "DEBUG-DM-LOG" (fun () -> "gc_statement_generate: "^map_name_domain);
    let input_postfix = "_input" in let output_postfix = "_output" in
    (* in_out is true if it's an input, otherwise false*)  
    let in_out = (string_ends_with map_name_domain input_postfix) in
    let postfix = if in_out then input_postfix else output_postfix in
    let map_name = 
        String.sub map_name_domain 0 
            (
                (String.length map_name_domain) -
                (String.length postfix)
            ) in
    let map_schema          =   get_map_vars                (map_name)  (k3_prog_schema) in
    let map_expr            =   get_map_expr_from_schema    (map_name)  (k3_prog_schema) in
    let gc_statement = 
        match (pair_map varIdType_to_k3_expr map_schema) with
        | x,    []    -> K.PCElementRemove(map_expr, vars, []) 
        | [],    y    -> K.PCElementRemove(map_expr, [], vars)
        | x,    y     -> failwith "Map with both variables is not supported"
    in
       gc_statement
        (*** FIXME ***) (*only supports maps with output *)
       
let gc_statement_generate k3_prog_schema stmt_info =
    let (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr) = stmt_info in
     
    let dummy_statement = 
        get_unit_statement ()
    in
    
    let collection_ivc_gc_var = 
        collection_ivc_gc_var stmt_info in
    
    let gc_statement_main = gc_do mapn lhs_outs_el k3_prog_schema in
    let gc_statement_lambda_body = 
        let new_status_value = rhs_ret_ve in
        K.IfThenElse(
            K.Eq(new_status_value, is_gc_constant), 
            K.Block([gc_statement_main; K.PCElementRemove(lhs_collection, [], lhs_outs_el)]),
            dummy_statement
        )
    in
    let multi_ret = K.Var("multi_ret",K.TBase(T.TInt))
    in
    let gc_statement_lambda = lambda (rhs_outs_el@[rhs_ret_ve; multi_ret]) gc_statement_lambda_body in
    if rhs_outs_el = [] then   
        K.Apply(  gc_statement_lambda,collection_ivc_gc_var)    
    else                       
        K.Iterate(gc_statement_lambda,collection_ivc_gc_var)
        
let update_domain_statement_generate stmt_info = 
    let (mapn, 
        trig_args_el,
        lhs_collection, 
        lhs_ins_el, 
        lhs_outs_el, 
        map_type, 
        rhs_outs_el, 
        rhs_ret_ve, 
        incr_expr) = stmt_info in
    let collection_ivc_gc_var = 
        collection_ivc_gc_var stmt_info in

    let multi_ret = K.Var("multi_ret",K.TBase(T.TInt))
    in
    let update_domain_lambda_body = 
      K.IfThenElse(
                     K.Neq(multi_ret, zero_int_val),
                   K.PCValueUpdate(lhs_collection, [], lhs_outs_el, multi_ret),
                   get_unit_statement ()
                  )
	in
	let update_domain_lambda = lambda (rhs_outs_el@[rhs_ret_ve; multi_ret]) update_domain_lambda_body in
	if rhs_outs_el = [] then
	    K.Apply( update_domain_lambda, collection_ivc_gc_var )
	else
	    K.Iterate( update_domain_lambda, collection_ivc_gc_var )

(*
let dm_stmt_to_k3_stmt (meta: meta_t) trig_args (dm_stmt: Plan.stmt_t) (k3_prog_schema: K.map_t list)
     : K.statement_t * meta_t =
    let stmt_info, nm0 =
        get_stmt_info meta dm_stmt trig_args
    in
    let collection_ivc_gc       = 
        collection_ivc_gc_generate  stmt_info in 
    let collection_ivc_gc_var   = 
        collection_ivc_gc_var       stmt_info in

    let statement_expr =
        let lambda_arg = collection_ivc_gc_var in
        let lambda_body = 
            let dummy_statement = 
	            get_unit_statement ()
            in
            let update_domain_statement =
                update_domain_statement_generate stmt_info
            in
            let update_status = 
                if Debug.active "DEBUG-DM-STATUS" then
                    update_status_statement_generate stmt_info k3_prog_schema 
                else
                    dummy_statement 
            in
            let ivc_statement =
                if Debug.active "DEBUG-DM-IVC" then
                    ivc_statement_generate stmt_info k3_prog_schema
                else
                    dummy_statement 
            in
                K.Block([update_domain_statement; update_status; ivc_statement; dummy_statement])
        in
        let outer_loop_body = lambda [lambda_arg] lambda_body in
            K.Apply(outer_loop_body, collection_ivc_gc)
    in
        (statement_expr, nm0)


let dm_trig_to_k3_trig (meta: meta_t) (m3dm_trig: M3.trigger_t) (k3_prog_schema: K.map_t list) (m3_to_k3_trig: K.trigger_t) : K.trigger_t * meta_t =
    Debug.print "DEBUG-DM-LOG" (fun () -> "dm_trig_to_k3_trig for event: "^(Schema.name_of_event m3dm_trig.M3.event));
    let trig_args = Schema.event_vars m3dm_trig.M3.event in
    let m3_to_k3_stmts = if (Debug.active "DEBUG-DM-WITH-M3") then snd m3_to_k3_trig else [] in
    let k3_trig_stmts, new_meta = 
        List.fold_left 
        (
            fun (old_stms, om) m3dm_stmt -> 
                    let k3_stmt, nm = dm_stmt_to_k3_stmt om trig_args m3dm_stmt k3_prog_schema in 
                    (old_stms@[k3_stmt], nm) 
        )
        ([],([],snd meta))
        !(m3dm_trig.M3.statements) 
    in
        ((m3dm_trig.M3.event, k3_trig_stmts@m3_to_k3_stmts), new_meta)
*)

let dm_trig_to_k3_trig (meta: meta_t) (m3dm_trig: M3.trigger_t) (k3_prog_schema: K.map_t list) (m3_to_k3_trig: K.trigger_t) : K.trigger_t * meta_t =
    Debug.print "DEBUG-DM-LOG" (fun () -> "dm_trig_to_k3_trig for event: "^(Schema.name_of_event m3dm_trig.M3.event));
    let trig_args = Schema.event_vars m3dm_trig.M3.event in
    let m3_to_k3_stmts = if (Debug.active "DEBUG-DM-WITH-M3") then snd m3_to_k3_trig else [] in
    let stmt_infos, new_meta = 
        List.fold_left 
        (
            fun (old_stmt_infos, om) dm_stmt ->
                let stmt_info, nm0 = get_stmt_info om dm_stmt trig_args in
                    (old_stmt_infos@[stmt_info], nm0)
        )
        ([],([],snd meta))
        !(m3dm_trig.M3.statements) 
    in
    if List.length stmt_infos = 0 then
        ((m3dm_trig.M3.event, m3_to_k3_stmts), meta)
    else    
	    let m3_part = K.Block(m3_to_k3_stmts) in
	    let update_domain_part =
	        K.Comment("Updating domains", 
		        K.Block(
		            List.map (update_domain_statement_generate) stmt_infos
		        )
		    )
	    in
	    Debug.print "DEBUG-DM-LOG" (fun () -> "update domain part finished!");
	    let update_status_part =
	        if Debug.active "DEBUG-DM-STATUS" then
	            K.Comment("Updating status",  
	                K.Block(
		                List.map (update_status_statement_generate k3_prog_schema) stmt_infos 
		            )
		        )
	        else
	            get_unit_statement ()
	    in
	    Debug.print "DEBUG-DM-LOG" (fun () -> "update status part finished!");
	    let ivc_part = 
	        if Debug.active "DEBUG-DM-IVC" then
	            K.Comment("IVC computation",
		            K.Block(
		                List.map (ivc_statement_generate k3_prog_schema) stmt_infos 
		            )
	            )
	        else
	            get_unit_statement ()
	    in
	    Debug.print "DEBUG-DM-LOG" (fun () -> "IVC part finished!");
	    let gc_part = 
            if not (Debug.active "DEBUG-DM-NO-GC") then
                K.Comment("Garbage Collecting",
	                K.Block(
	                    List.map (gc_statement_generate k3_prog_schema) stmt_infos 
	                )
	            )
            else
                get_unit_statement ()
        in
        Debug.print "DEBUG-DM-LOG" (fun () -> "GC part finished!");
	    let collection_ivc_gc_stmts = 
	        if List.length stmt_infos = 1 then
	            collection_ivc_gc_generate (List.hd stmt_infos)
	        else
		        K.Tuple(
		            List.map (collection_ivc_gc_generate) stmt_infos
		        )
	    in
	    let collection_ivc_gc_vars = 
	        List.map (collection_ivc_gc_var) stmt_infos
	    in
	    let k3_trig_stmt =
	        let lambda_arg = collection_ivc_gc_vars in
	        let lambda_body = 
	            K.Block([update_domain_part; update_status_part; ivc_part; m3_part; gc_part; get_unit_statement ()])
	        in
	        let apply_body = lambda lambda_arg lambda_body in
	            K.Apply(apply_body, collection_ivc_gc_stmts)
	    in
	        ((m3dm_trig.M3.event, [k3_trig_stmt]), new_meta)

(** Transforms an existing K3 program with its corresponding M3DM program into a K3 program. *)
let m3dm_to_k3 (m3tok3_program : K.prog_t) (m3dm_prog: M3DM.prog_t) : (K.prog_t) =
    let with_m3 = Debug.active "DEBUG-DM-WITH-M3" in
    if with_m3 then ();(* begin Debug.activate "DEBUG-DM-IVC"; Debug.activate "K3-NO-OPTIMIZE"(* Debug.activate "K3-NO-OPTIMIZE-LIFT-UPDATES" *) end; *)
    let ( k3_database, (old_k3_prog_schema, old_patterns_map), m3tok3_prog_trigs, old_k3_prog_tlqs) = m3tok3_program in
    let k3_prog_tlqs = if (with_m3) then old_k3_prog_tlqs else [] in
    let k3_prog_schema = (if (with_m3) then old_k3_prog_schema else [])@(List.map m3_map_to_k3_map !(m3dm_prog.M3DM.maps)) in
    let patterns_map = (if (with_m3) then old_patterns_map else [])@Patterns.extract_patterns !(m3dm_prog.M3DM.triggers) in
    let k3_prog_trigs, (_,sum_maps) = 
        let dynamic_triggers, metas = 
	        List.fold_left
	            (fun (old_trigs,om) m3dm_trig -> 
	                let m3_to_k3_trig =
	                    let ev = m3dm_trig.M3.event in
	                        List.find 
	                            (
	                                fun (e, sl) -> e = ev
	                            ) m3tok3_prog_trigs
	                in
                        let k3_trig,nm = dm_trig_to_k3_trig om m3dm_trig k3_prog_schema m3_to_k3_trig in
                            (old_trigs@[k3_trig], nm) 
	            )
	            ([],empty_meta) 
	            !(m3dm_prog.M3DM.triggers)
        in
        let static_trigger = 
            List.find 
            (
                fun (e, _) -> 
                    match e with 
                    | Schema.SystemInitializedEvent -> true
                    | _ -> false
            ) m3tok3_prog_trigs
        in
            (dynamic_triggers@[static_trigger], metas)
    in
    ( k3_database, 
      (k3_prog_schema@sum_maps, patterns_map), 
      k3_prog_trigs, 
      k3_prog_tlqs 
   )
(**/**)
