open Algebra

(* datastructure to track compilations for eliminating duplicate maps *)
let terms_compiled = ref []

let add_compilation t mapn = terms_compiled := (!terms_compiled)@[t,mapn]
let is_compiled t = List.mem_assoc t (!terms_compiled)
let get_compilation t = List.assoc t (!terms_compiled)
let clear_compilation () = terms_compiled := []

let debug_compilation () = 
    List.iter
        (fun (x,y) ->
            print_endline ("compiled("^y^"): "^(term_as_string x [])))
        (!terms_compiled)

(* TODO: move this to Algebra module *)
(* readable_relalg_t -> readable_relalg_lf_t list *)
let rec get_base_relations_plan r =
    let relalg_lf lf = match lf with
        | Rel(_) -> [lf]
        | AtomicConstraint(_, t1, t2) ->
              (get_base_relations t1)@(get_base_relations t2)
        | _ -> []
    in
        fold_relalg Util.ListAsSet.multiunion 
            Util.ListAsSet.multiunion relalg_lf r

(* readable_term_t -> readable_relalg_lf_t list *)
and get_base_relations t =
    let term_lf lf = match lf with
        | AggSum(f,r) ->
              (get_base_relations f)@(get_base_relations_plan r)
        | _ -> []
    in
        fold_term Util.ListAsSet.multiunion 
            Util.ListAsSet.multiunion term_lf t

(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: var_t list)
                          (mapn:   string)         (* map name *)
                          (params: var_t list)
                          (bigsum_vars: var_t list)
                          (negate_f: term_t -> term_t)
                          (term: term_t) =
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      x_mR_A1, ..., x_mR_Ak. *)
   let bound_vars = (List.map (fun (x,y) -> ("x_"^mapn^reln^"_"^x, y)) relsch)
   in
   (* compute the delta and simplify. *)
   let s = List.filter (fun (_, t) -> t <> term_zero)
              (simplify (term_delta negate_f reln bound_vars term)
                            (bound_vars @ bigsum_vars) params)
   in
   let todos =
      let f (new_params, new_ma) =
         let mk x =
            let t_params = (Util.ListAsSet.inter (term_vars x)
                              (Util.ListAsSet.union new_params
                                 (Util.ListAsSet.union bound_vars bigsum_vars)))
            in (t_params, x)
         in
         List.map mk (extract_aggregates new_ma)
      in
      let add_name ((p, x), i) = (mapn^reln^(string_of_int i), p, x)
      in
      List.map add_name (Util.add_positions (Util.ListAsSet.no_duplicates
         (List.flatten (List.map f s))) 1)
   in
   let g (new_params, new_ma) =
      (reln, bound_vars, new_params, bigsum_vars, mapn, new_ma)
   in
   ((List.map g s), todos)




(* auxiliary for compile.
   Writes (= generates string for) one increment statement. For example,

   generate_code ("R", ["x_R_A"; "x_R_B"], ["x_C"], "m",
                  Prod[Val(Var(x_R_A)); <mR1>],
                  [("mR1", ["x_R_B"; "x_C"], <mR1>)]) =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)
let generate_code db_schema
                  (reln, bound_vars, params, bigsum_vars, mapn, new_ma)
                  new_ma_aggs =
   let loop_vars = Util.ListAsSet.diff params bound_vars in
   let bv_names = List.map fst bound_vars in
   let param_names = List.map fst params in
   let bs_names = List.map fst bigsum_vars in
   let lv_names = List.map fst loop_vars in
   let fn (mapname, params, mapstructure) =
          (mapstructure, mapname^"["^(
              Util.string_of_list ", " (List.map fst params)^"]"))
   in
      "+"^reln^"("^(Util.string_of_list ", " bv_names)^ "): "^
        (if (loop_vars = []) then ""
         else "foreach "^(Util.string_of_list ", " lv_names)^" do ")
        ^mapn^"["^(Util.string_of_list ", " param_names)^"] += "^
        (if (bigsum_vars = []) then ""
         else "bigsum_{"^(Util.string_of_list ", " bs_names)^"} ")
        ^(term_as_string new_ma (List.map fn new_ma_aggs))

let generate_spread db_schema (reln, bound_vars, params, _, mapn, new_ma) new_ma_aggs =
   let mk_map_entry n keys =
       let key_names = String.concat "," (List.map fst keys) in
           "Map "^n^"["^key_names^"]"
   in
   let arg_bindings = 
       List.combine bound_vars (List.assoc reln db_schema)
   in
   let get_arg_binding v =
       if List.mem_assoc v arg_bindings
       then List.assoc v arg_bindings else v
   in
   let substitute_args t = apply_variable_substitution_to_term arg_bindings t in
   let bind_agg_fn (mapname, params, mapstructure) =
       (substitute_args mapstructure,
           mk_map_entry mapname (List.map get_arg_binding params))
   in
   let bound_var_names = String.concat ";"
       (List.map (fun x -> fst (List.assoc x arg_bindings)) bound_vars)
   in
   let get_entry_bindings = List.map bind_agg_fn new_ma_aggs in
   let put_entry = mk_map_entry mapn (List.map get_arg_binding params) in
      reln^"\t"^bound_var_names^"\t"^put_entry^"\t\t"^
          (term_as_string (substitute_args new_ma) get_entry_bindings)


(* recursive compilation implementation *)
let rec compile_messages
        (db_schema: (string * var_t list) list)
        (mapn: string)
        (params: var_t list)
        (bigsum_vars: var_t list)
        (negate_f: term_t -> term_t)
        (string_of_message:
            (string * var_t list) list ->
            string * var_t list * var_t list * var_t list * string * term_t ->
            (string * var_t list * term_t) list -> string)
        (term: term_t) : (string list) =
   let cdfr (reln, relsch) =
      compile_delta_for_rel reln relsch mapn params bigsum_vars negate_f term
   in

   let partition_f ((x,y,z), (a,b,c)) = not(is_compiled(z)) in 

   (* Reset any bound or loop vars to their original name *)
   let normalize_term (n,p,t) =
       let br = Util.ListAsSet.no_duplicates
           (get_base_relations (readable_term t))
       in

       (* TODO: parser should disallow attributes with prefix "x_" *)
       let is_bound_var ((a,b),x) = 
           ((String.length a) > 2) && ((String.sub a 0 2) = "x_")
       in
       let bindings = List.flatten (List.map
           (function
               | Rel(reln,f) ->
                     List.filter is_bound_var
                         (List.combine f (List.assoc reln db_schema))
               | _ -> raise (Failure ("Expected a relation.")))
           br)
       in
           (n,p,apply_variable_substitution_to_term bindings t)
   in

   let (l1, l2) = (List.split (List.map cdfr db_schema))
   in
   let normalized_l2 = List.flatten (List.map2
       (fun todo_l (reln, relsch) -> List.map normalize_term todo_l)
       l2 db_schema)
   in
   let ((normalized_todo, todo), (normalized_dups, todo_dups)) =
       let (a,b) = List.partition partition_f
           (List.combine normalized_l2 (List.flatten l2))
       in
           (List.split a, List.split b)
   in

   (* Track normalized targets for next step prior to recurring *)
   List.iter (fun (y,_,x) -> add_compilation x y) normalized_todo;

   (* Use correct map names for duplicate terms *)
   let new_ma_aggs = todo@
       (List.map2 (fun (_,_,dt) (_,p,t) -> (get_compilation dt,p,t))
           normalized_dups todo_dups)
   in
   let ready = List.map
       (fun x -> string_of_message db_schema x new_ma_aggs) (List.flatten l1)
   in

   (* Recur over non-duplicate terms *)
   ready @
   (List.flatten (List.map (fun (x,y,z) ->
       compile_messages db_schema x y [] negate_f string_of_message z) todo))


(* compiles to ck's original format *)
let compile_readable_messages db_schema mapn params bigsum_vars negate_f term =
    (* Reset compilations *)
    clear_compilation();
    compile_messages
        db_schema mapn params bigsum_vars negate_f generate_code term

(* compiles to spread message templates *)
let compile_spread_messages db_schema mapn params bigsum_vars negate_f term =
    (* Reset compilations *)
    clear_compilation();
    compile_messages
        db_schema mapn params bigsum_vars negate_f generate_spread term

(* the main compile function. call this one, not the others. *)
let compile_terms (db_schema: (string * var_t list) list)
                  (mapn: string)
                  (params: var_t list)
                  (term_l: term_t list)
                  (compile_fn:
                      (string * var_t list) list -> string -> var_t list 
                      -> var_t list -> (term_t -> term_t) -> term_t -> string list)
                  : (string list) =

    let flat_term_l = List.map
        (fun x -> let (tc,bsv) = flatten_term params x in
            (mk_cond_agg tc, bsv))
        (List.map readable_term (List.map roly_poly term_l))
    in
    let insert_negate_f = fun x -> x in
    let delete_negate_f = Algebra.negate_term in
    let messages_l = 
        List.map (fun (t, bigsum_vars_and_rels) ->
            let bigsum_vars =
                List.map (fun (x,_,_) -> x) bigsum_vars_and_rels in
            let insert_messages =
                compile_fn db_schema mapn params bigsum_vars insert_negate_f t
            in
            let delete_messages =
                let invert_event s =
                    (if (String.get s 0) = '+' then s.[0] <- '-'); s
                in
                    List.map invert_event 
                        (compile_fn db_schema
                            mapn params bigsum_vars delete_negate_f t)
            in
                insert_messages@delete_messages)
            (List.map (fun (t,v) -> (make_term t, v)) flat_term_l)
    in
        List.flatten messages_l


module BytecodeCompiler =
struct
    module A = Algebra
    module SA = Bytecode.SimplifiedAlgebra
    module BG = Bytecode.Generator
    module BI = Bytecode.InstructionSet
    module BS = Bytecode.InstructionSet.NestedBlockSet
    module CPP = Codegen.CPPCodeGenerator
    module CPPI = Codegen.CPPInstrumentation

    (* handler name, arg names and types, code *)
    type handler = (string * ((string * string) list) * string)

    (* event type (insert or delete), event relation *)
    type event = (string * string)

    let generate_bytecode db_schema bigsum_vars_and_rels
                          (reln, bound_vars, params, mapn, new_ma)
                          new_ma_aggs
                          : (string * BI.nested_block_t)
            =
        let loop_vars = Util.ListAsSet.diff params bound_vars in
        let fn (mapname, params, mapstructure) =
            (mapstructure, (mapname, params)) in
        let env = bound_vars in
        let map_bindings = (List.map fn new_ma_aggs) in
        let arg_bindings = 
            List.combine bound_vars (List.assoc reln db_schema)
        in
        let rma = A.readable_term new_ma in
        let ma_block = 
            BG.term_as_bytecode mapn rma params
                loop_vars bigsum_vars_and_rels map_bindings arg_bindings
        in
            (reln, (mapn, env, ma_block))


    let rec compile
            (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (bigsum_vars_and_rels: (var_t * var_t * readable_relalg_lf_t) list)
            (negate_f: term_t -> term_t)
            (term: A.term_t) : BG.handler_blocks
            =
        (*
        let debug_dupcheck x =
            print_endline ("dupcheck: "^(term_as_string x []));
            debug_compilation() in
        let debug_filter r x = if not r then
            print_endline ("found duplicate "^(term_as_string x [])) in

        let debug_partition_f ((x,y,z), (a,b,c)) =
            debug_dupcheck z;
            let r = not(is_compiled(z)) in
                debug_filter r z; r in
        *)
        let partition_f ((x,y,z), (a,b,c)) = not(is_compiled(z)) in 

        (* Reset any bound or loop vars to their original name *)
        let normalize_term (n,p,t) =
            let br = Util.ListAsSet.no_duplicates
                (get_base_relations (readable_term t))
            in

            (* TODO: parser should disallow attributes with prefix "x_" *)
            let is_bound_var ((a,b),x) = 
                ((String.length a) > 2) && ((String.sub a 0 2) = "x_")
            in
            let bindings = List.flatten (List.map
                (function
                    | Rel(reln,f) ->
                          List.filter is_bound_var
                              (List.combine f (List.assoc reln db_schema))
                    | _ -> raise (Failure ("Expected a relation.")))
                    br)
            in
                (n,p,apply_variable_substitution_to_term bindings t)
        in

        let bigsum_vars = List.map (fun (x,_,_) -> x) bigsum_vars_and_rels in
        let cdfr (reln, relsch) =
            compile_delta_for_rel
                reln relsch mapn params bigsum_vars negate_f term in
        let (l1,l2) = (List.split (List.map cdfr db_schema)) in

        let normalized_l2 = List.flatten (List.map2
            (fun todo_l (reln, relsch) -> List.map normalize_term todo_l)
            l2 db_schema)
        in
        let ((normalized_todo, todo), (normalized_dups, todo_dups)) =
            let (a,b) =
                List.partition partition_f
                    (List.combine normalized_l2 (List.flatten l2))
            in
                (List.split a, List.split b)
        in

        (* Track normalized targets for next step prior to recurring *)
        List.iter (fun (y,_,x) -> add_compilation x y) normalized_todo;

        (* Use correct map names for duplicate terms *)
        let new_ma_aggs = todo@
            (List.map2 (fun (_,_,dt) (_,p,t) -> (get_compilation dt,p,t))
                normalized_dups todo_dups)
        in
        let ready_in =
            (List.map
                (fun (reln, bound_vars, params, _, mapn, new_ma) ->
                    generate_bytecode db_schema bigsum_vars_and_rels
                        (reln, bound_vars, params, mapn, new_ma)
                        new_ma_aggs)
                (List.flatten l1))
        in
        let ready = BG.create_dependent_handler_block ready_in in

        (* Recur over non-duplicate terms *)
        let next =
            List.flatten (List.map
                (fun (x,y,z) -> compile db_schema x y [] negate_f z) todo)
        in
            BG.merge_handler_blocks ready next


    let compile_terms_to_bytecode
            (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (term_l: A.term_t list)
            =
        (* Reset bytecode generator for new compilation *)
        BG.clear_declarations();

        let debug_flattening l = 
            List.iter (fun (t,v) ->
                print_endline "Flattened: ";
                print_endline (term_as_string (make_term t) []);
                print_endline ("Bigsum vars: "^
                    (String.concat "," (List.map (fun ((n,t),_,_) -> n) v))))
                l
        in
        let flat_term_l = List.map
            (fun x -> let (tc,bsv) = flatten_term params x in
                (mk_cond_agg tc, bsv))
            (List.map readable_term (List.map roly_poly term_l))
        in

        debug_flattening flat_term_l;

        (* Recursively compile for the schema, creating bytecode *)
        let compile_for_event event_type =

            clear_compilation();

            let negate_f = match event_type with
                | "insert" -> (fun x -> x)
                | "delete" -> Algebra.negate_term
                | _ -> raise (Failure ("Invalid event type: "^event_type))
            in
            (* event_bc_l : handler_blocks list *)
            let event_bc_l = List.map (fun (t, bigsum_vars_and_rels) ->
                let bc = compile db_schema
                    mapn params bigsum_vars_and_rels negate_f t
                in

                let bsv_maintenance_bc =
                    List.map (fun (bsv,v,r) -> match r with
                        | Rel(n,_) ->
                              BG.generate_bigsum_maintenance event_type bsv v n
                        | _ -> raise (Failure "Invalid bigsum datastructure"))
                        bigsum_vars_and_rels
                in
                    BG.append_handler_blocks
                        (List.flatten bsv_maintenance_bc) bc)
                (List.map (fun (t,v) -> (make_term t, v)) flat_term_l)
            in

            debug_compilation();

            (* Create handlers, i.e. group compilations by input relation *)
            let create_handler event_type rel blocks =
                let rel_vars =
                    if List.mem_assoc rel db_schema
                    then (List.assoc rel db_schema) else []
                in
                    ((event_type, rel),
                        ("on_"^event_type^"_"^rel, rel_vars, blocks))
            in

            (* event_handlers_bc:
             *     (event * (hndl name, hndl args, ordered_blocks list)) list
             * where event : (event type : string) * (relation : string) *)
            let event_handlers_bc = List.map
                (fun (rel, blocks) -> create_handler event_type rel blocks)
                (List.flatten event_bc_l)
            in
                event_handlers_bc
        in
        let event_types = ["insert"; "delete"] in
            List.flatten (List.map compile_for_event event_types)

    (* TODO: ensure disjoint input relations for term_l *)
    let compile_terms  (db_schema: (string * var_t list) list)
                       (mapn: string)
                       (params: var_t list)
                       (term_l: A.term_t list)
                       : string * ((handler * event) list)
            =
        let events_and_handlers = 
            compile_terms_to_bytecode db_schema mapn params term_l
        in

        (* Generate C++ source *)
        let bc_decls = BG.get_declarations() in
        let (decls_str, events_names_args_handlers_l) =
            CPP.generate_code mapn bc_decls events_and_handlers
        in
        let instrumentation_str =
            CPPI.generate_declarations_instrumentation
                bc_decls events_names_args_handlers_l
        in
            (decls_str^"\n\n"^instrumentation_str,
            List.map
                (fun (event, (name, args_and_decls, handler_code)) ->
                    ((name, args_and_decls, handler_code), event))
                events_names_args_handlers_l)


    (* Helper function to dump out bytecode in the toplevel *)
    let compile_terms_in_toplevel
            (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (term_l: A.term_t list)
            =
        let events_and_handlers =
            compile_terms_to_bytecode db_schema mapn params term_l
        in
            List.flatten (List.map 
                (fun (_, (_,_,ordered_blocks)) ->
                    List.flatten (List.map
                        (fun bs -> BS.fold (fun el acc -> acc@[el]) bs [])
                        ordered_blocks))
                events_and_handlers)
end
