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

(* Returns a mapping from variables in the delta, to their original names *)
let get_bindings db_schema relations =
    (* TODO: parser should disallow attributes with prefix "x_" *)
    let is_bound_var ((a,b),x) = 
        ((String.length a) > 2) && ((String.sub a 0 2) = "x_")
    in
        Util.ListAsSet.multiunion (List.map (function
            | Rel(reln,f) -> List.filter is_bound_var
                  (List.combine f (List.assoc reln db_schema))
            | _ -> raise (Failure ("Expected a relation.")))
            relations)

(* Reset any bound or loop vars to their original name *)
let normalize_term db_schema ((n,p,t) as orig) =
    let br = Util.ListAsSet.no_duplicates (get_base_relations (readable_term t))
    in
        (*debug_compile_bindings br (get_bindings db_schema br);*)
        ((n,p,apply_variable_substitution_to_term
            (get_bindings db_schema br) t), orig)


(* One-step delta compilation, used by both message and bytecode compiler. *)
let compile_delta_for_rel (reln:   string)
        (relsch: var_t list)
        (mapn:   string)         (* map name *)
        (params: var_t list)
        (bigsum_vars: var_t list)
        (negate: bool)
        (term: term_t) =
    (* on insert into a relation R with schema A1, ..., Ak, to update map m,
       we assume that the input tuple is given by variables
       x_mR_A1, ..., x_mR_Ak. *)
    let bound_vars = (List.map (fun (x,y) -> ("x_"^mapn^reln^"_"^x, y)) relsch)
    in
        (* compute the delta and simplify. *)
    let s = List.filter (fun (_, t) -> t <> term_zero)
        (simplify (term_delta negate reln bound_vars term)
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


(* A module to output message-oriented query processing *)
module MessageCompiler =
struct

    (* Message generation/stringification.
       Writes (= generates string for) one increment statement. For example,

       generate_code ("R", ["x_R_A"; "x_R_B"], ["x_C"], "m",
       Prod[Val(Var(x_R_A)); <mR1>],
       [("mR1", ["x_R_B"; "x_C"], <mR1>)]) =
       "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
    *)
    let generate_code db_schema negate
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
        let evt = if negate then "-" else "+" in
            evt^reln^"("^(Util.string_of_list ", " bv_names)^ "): "^
                (if (loop_vars = []) then ""
                else "foreach "^(Util.string_of_list ", " lv_names)^" do ")
            ^mapn^"["^(Util.string_of_list ", " param_names)^"] += "^
                (if (bigsum_vars = []) then ""
                else "bigsum_{"^(Util.string_of_list ", " bs_names)^"} ")
            ^(term_as_string new_ma (List.map fn new_ma_aggs))

    let generate_remaining db_schema mapn params bigsum_vars negate term =
        let base_rels = List.map (function
            | Rel(n,f) -> (n,f)
            | _ -> raise (Failure "Expected a relation."))
            (get_base_relations (readable_term term))
        in
        let string_of_vars l = String.concat "," (List.map fst l) in
        let evt = if negate then "-" else "+" in
        let t_str = term_as_string term []
        in
            List.map (fun (rel, relsch) -> 
                let bound_vars =
                    List.map (fun (x,y) -> ("x_"^mapn^rel^"_"^x, y)) relsch
                in
                let loop_vars = Util.ListAsSet.diff params bound_vars in
                let assign = rel^"("^(string_of_vars relsch)^"): "^
                    (if (loop_vars = []) then ""
                    else "foreach "^(string_of_vars loop_vars)^" do ")^
                    mapn^"["^(string_of_vars params)^"]"
                in
                    evt^assign^" = "^
                    (if (bigsum_vars = []) then ""
                    else "bigsum_{"^(string_of_vars bigsum_vars)^"} ")^
                    t_str)
                (Util.ListAsSet.no_duplicates base_rels)

    (* Spread message generation (see Spread docs for more info on message template):
     *     "<relation name>\t<relation variables>\t<put entry>\t\t<arithmetic map term>"
     * where:
     *     entry = "Map <name>[<key names>]"
     *     arithmetic map term =
     *         constant | var | entry | arith map term op arith map term
     *)
    let generate_spread db_schema negate
            (reln, bound_vars, params, _, mapn, new_ma) new_ma_aggs =
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
        let evt = if negate then "-" else "+" in
            evt^reln^"\t"^bound_var_names^"\t"^put_entry^"\t\t"^
                (term_as_string (substitute_args new_ma) get_entry_bindings)


    (* Recursive compilation implementation *)
    let rec compile_messages
            (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (bigsum_vars: var_t list)
            (negate: bool)
            (string_of_message:
                (string * var_t list) list -> bool -> 
                string * var_t list * var_t list * var_t list * string * term_t ->
                (string * var_t list * term_t) list -> string)
            (string_of_remaining_message:
                (string * var_t list) list -> string -> var_t list -> var_t list ->
                    bool -> term_t -> string list)
            (level: int)
            (term: term_t) : (string list) =
        if level = 0 then
            string_of_remaining_message
                db_schema mapn params bigsum_vars negate term
        else begin
            let recur (x,y,z) = compile_messages db_schema
                x y [] negate string_of_message string_of_remaining_message (level-1) z
            in
            let cdfr (reln, relsch) =
                compile_delta_for_rel reln relsch mapn params bigsum_vars negate term
            in

            let partition_f ((x,y,z), (a,b,c)) = not(is_compiled(z)) in 

            let (l1, l2) = (List.split (List.map cdfr db_schema)) in
            let normalized_l2 = List.map (normalize_term db_schema) (List.flatten l2) in
                (* dup_todos: normalized dup term * dup term params * dup term *)
            let ((normalized_todo, todo), dup_todos) =
                let (a,b) = List.partition partition_f normalized_l2 in
                let dt = List.map (fun ((_,_,dt), (_,p,t)) -> (get_compilation dt,p,t)) b
                in
                    (List.split a, dt)
            in

                (* Track normalized targets for next step prior to recurring *)
                List.iter (fun (y,_,x) -> add_compilation x y) normalized_todo;

                (* Use correct map names for duplicate terms *)
                let new_ma_aggs = todo@dup_todos in
                let ready = List.map
                    (fun x -> string_of_message db_schema negate x new_ma_aggs) (List.flatten l1)
                in

                    (* Recur over non-duplicate terms *)
                    ready @ (List.flatten (List.map recur todo))
        end

    (* auxiliary function to compile to ck's original format *)
    let compile_readable_messages db_schema mapn params bigsum_vars negate level term =
        (* Reset compilations *)
        clear_compilation();
        compile_messages db_schema mapn params
            bigsum_vars negate generate_code generate_remaining level term

    (* auxiliary function to compile to spread message templates *)
    let compile_spread_messages db_schema mapn params bigsum_vars negate level term =
        let remaining_failure_fn = fun dbs mapn p bsv neg t ->
            raise (Failure ("Spread does not support partial compilation."))
        in
            (* Reset compilations *)
            clear_compilation();
            compile_messages db_schema mapn params
                bigsum_vars negate generate_spread remaining_failure_fn level term

    (* The main compile function. call this one, not the others. *)
    let compile_terms (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (compile_fn:
                (string * var_t list) list -> string -> var_t list 
                -> var_t list -> bool -> int -> term_t -> string list)
            (compilation_level: int)
            (term_l: term_t list)
            : (string list) =

        let flat_term_l = List.map
            (fun x -> let (tc,bsv) = flatten_term params x in
                (mk_cond_agg tc, bsv))
            (List.map readable_term (List.map roly_poly term_l))
        in
        let insert = false in
        let delete = true in
        let messages_l = 
            List.map (fun (t, bigsum_vars_and_rels) ->
                let bigsum_vars =
                    List.map (fun (x,_,_) -> x) bigsum_vars_and_rels in
                let insert_messages = compile_fn db_schema
                    mapn params bigsum_vars insert compilation_level t
                in
                let delete_messages =
                    compile_fn db_schema
                        mapn params bigsum_vars delete compilation_level t
                in
                    insert_messages@delete_messages)
                (List.map (fun (t,v) -> (make_term t, v)) flat_term_l)
        in
            List.flatten messages_l
end


module BytecodeCompiler =
struct
    module A = Algebra
    module SA = Bytecode.SimplifiedAlgebra
    module BG = Bytecode.Generator
    module BI = Bytecode.InstructionSet
    module BS = Bytecode.InstructionSet.NestedBlockSet
    module CPP = Codegen.CPPCodeGenerator
    module CPPI = Codegen.CPPInstrumentation

    module Debug =
    struct
        let debug_partition_f ((x,y,z), (a,b,c)) =
            print_endline ("dupcheck: "^(term_as_string z []));
            debug_compilation();
            let r = not(is_compiled(z)) in
                if not r then print_endline
                    ("found duplicate "^(term_as_string z []));
                r

        let debug_compile_bindings br bindings =
            let string_of_var (n,t) = n in
            let string_of_vars l = String.concat ","
                (List.map (fun (x,_) -> string_of_var x) l) in
            let ambiguous =
                List.map (fun (x,y) ->
                    ((x,y), List.filter (fun (u,v) -> x=u && y<>v) bindings))
                    bindings in
            let duplicates =
                List.map (fun (x,y) ->
                    ((x,y), List.filter (fun (y,_) -> x=y) bindings))
                    bindings
            in
                List.iter (function
                    | Rel(reln,_) -> print_endline ("rel: "^reln)
                    | _ -> raise (Failure ("Expected a relation."))) br;
                List.iter
                    (fun ((x,_), al) -> print_endline
                        ("Ambiguous bindings for "^(string_of_var x)^
                            ": "^(string_of_vars al)))
                    (List.filter (fun (_, al) -> List.length al <> 0) ambiguous);
                List.iter
                    (fun ((x,_), dl) -> print_endline 
                        ("Duplicate bindings for "^(string_of_var x)^
                            ": "^(string_of_vars dl)))
                    (List.filter (fun (_, dl) -> List.length dl > 1) duplicates)

    end

    (* handler name, arg names and types, code *)
    type handler = (string * ((string * string) list) * string)

    (* event type (insert or delete), event relation *)
    type event = (string * string)

    (* Bytecode generation *)
    let generate_bytecode db_schema bigsum_vars_and_rels
                          (reln, bound_vars, params, mapn, new_ma)
                          new_ma_aggs
                          : (string * BG.maintenance_terms_and_deps) *
                            (string * BI.nested_block_t)
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
        let (maint, ma_block) = 
            BG.term_as_bytecode mapn rma params
                loop_vars bigsum_vars_and_rels map_bindings arg_bindings
        in
            ((reln, maint), (reln, (mapn, env, ma_block)))


    (* Recursive compilation *)
    let rec compile
            (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (bigsum_vars_and_rels: (var_t * var_t * readable_relalg_lf_t) list)
            (negate: bool)
            (level: int)
            (term: A.term_t)
            : (string * BG.maintenance_terms_and_deps) list * BG.handler_blocks *
              (string * var_t list * A.term_t) list
            =
        if level = 0 then ([], [], [(mapn, params, term)])
        else begin
            (* returns if a compilation target has been seen before,
             * for preventing duplicate maps *)
            let partition_f ((x,y,z), (a,b,c)) = not(is_compiled(z)) in 


            let bigsum_vars = List.map (fun (x,_,_) -> x) bigsum_vars_and_rels in
            let cdfr (reln, relsch) =
                compile_delta_for_rel
                    reln relsch mapn params bigsum_vars negate term in
            let (l1,l2) = (List.split (List.map cdfr db_schema)) in

            let normalized_l2 = List.map (normalize_term db_schema) (List.flatten l2) in

            (* dup_todos: normalized dup term * dup term params * dup term *)
            let ((normalized_todo, todo), dup_todos) =
                let (a,b) = List.partition partition_f normalized_l2 in
                let dt = List.map (fun ((_,_,dt), (_,p,t)) -> (get_compilation dt,p,t)) b
                in
                    (List.split a, dt)
            in

                (* Track normalized targets for next step prior to recurring *)
                List.iter (fun (y,_,x) -> add_compilation x y) normalized_todo;

                (* Use correct map names for duplicate terms *)
                let new_ma_aggs = todo@dup_todos in
                let (maint_l, ready_in) = List.split (List.map
                    (fun (reln, bound_vars, params, _, mapn, new_ma) ->
                        generate_bytecode db_schema bigsum_vars_and_rels
                            (reln, bound_vars, params, mapn, new_ma)
                            new_ma_aggs)
                    (List.flatten l1))
                in
                let ready = BG.create_dependent_handler_block ready_in in

                (* Recur over non-duplicate terms *)
                let (next_maint_l, next_l, rem_l) =
                    let nr = List.map (fun (x,y,z) ->
                        compile db_schema x y [] negate (level-1) z) todo
                    in
                    let mnt = List.map (fun (x,_,_) -> x) nr in
                    let nxt = List.map (fun (_,x,_) -> x) nr in
                    let rem = List.map (fun (_,_,x) -> x) nr in
                        (mnt,nxt,rem)
                in
                    ((List.flatten next_maint_l)@maint_l,
                        BG.merge_handler_blocks ready (List.flatten next_l),
                        (List.flatten rem_l))
        end


    (* TODO: ensure disjoint input relations for term_l *)
    (* Bytecode compilation for both insert and delete events *)
    let compile_terms_to_bytecode
            (db_schema: (string * var_t list) list)
            (mapn: string)
            (params: var_t list)
            (compilation_level: int)
            (term_l: A.term_t list)
            : (event * (string * var_t list * BG.ordered_blocks)) list
            =
        (* Reset bytecode generator for new compilation *)
        BG.clear_declarations();
        BG.clear_relations();

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

            let negate_term = match event_type with
                | "insert" -> false
                | "delete" -> true
                | _ -> raise (Failure ("Invalid event type: "^event_type))
            in
            (* event_bc_l : handler_blocks list *)
            let event_bc_l = List.map (fun (t, bigsum_vars_and_rels) ->
                let (maint_by_rel, bc, remaining_terms) =
                    compile db_schema mapn params
                        bigsum_vars_and_rels negate_term compilation_level t
                in


                (* Create bigsum var bindings for initial value computation *)
                let constraint_bindings = 
                    List.map (fun (x,y,_) -> (x,y)) bigsum_vars_and_rels
                in
                let schema_bindings = Util.ListAsSet.multiunion
                    (List.map (fun (r,f) -> List.map
                        (fun (n,t) -> ((n, t), ("protect_"^n,t))) f) db_schema)
                in

                (* Remove init val and final dep duplicates first *)
                let filtered_maint =
                    let eliminate_dups l = List.fold_left
                        (fun acc k -> if List.mem k acc then acc else acc@[k])
                        [] l
                    in
                        List.fold_left (fun acc (rel, maint) ->
                            let dup_elim_maint = eliminate_dups maint in
                                if List.mem_assoc rel acc then
                                    let existing_maint = List.assoc rel acc in
                                    let new_maint = 
                                        eliminate_dups (existing_maint@dup_elim_maint)
                                    in
                                        (List.remove_assoc rel acc)@[(rel, new_maint)]
                                else
                                    acc@[(rel, dup_elim_maint)])
                            [] maint_by_rel
                in

                (* TODO: only compute the bytecode needed for this handler, and not its delete.
                 * Right now we do this since generation uses common metadata, but this should
                 * be separated more cleanly in the bytecode generation *)
                let (init_compute_bc, init_maintain_bc, final_compute_bc, final_maintain_bc) =
                    let icm_fcm_bc =
                        List.map (fun (rel, maint) ->
                            let r = BG.generate_map_maintenance rel event_type db_schema
                                constraint_bindings schema_bindings maint
                            in
                                r)
                            filtered_maint
                    in
                    let ic = List.map (fun (x,_,_,_) -> x) icm_fcm_bc in
                    let im = List.map (fun (_,x,_,_) -> x) icm_fcm_bc in
                    let fc = List.map (fun (_,_,x,_) -> x) icm_fcm_bc in
                    let fm = List.map (fun (_,_,_,x) -> x) icm_fcm_bc in
                        (List.flatten ic, List.flatten im, List.flatten fc, List.flatten fm)
                in

                (* TODO: ensure bigsum var maintenance is done only once per relation *)
                let bsv_maintenance_bc = List.flatten (List.map
                    (fun (bsv,v,r) -> match r with
                        | Rel(n,_) ->
                              BG.generate_bigsum_maintenance
                                  event_type bsv v n (List.assoc n db_schema)
                        | _ -> raise (Failure "Invalid bigsum datastructure"))
                    bigsum_vars_and_rels)
                in
                let (init_rem_rel_bc, final_rem_rel_bc, remaining_bc) =
                    BG.remaining_terms_as_bytecode event_type db_schema
                        constraint_bindings schema_bindings remaining_terms
                in
                let bc_order = match event_type with
                    | "insert" -> [init_compute_bc; bsv_maintenance_bc; init_maintain_bc; init_rem_rel_bc]
                    | "delete" ->
                          [final_maintain_bc; final_rem_rel_bc;
                              bsv_maintenance_bc; final_compute_bc]
                    | _ -> raise (Failure "Invalid event type")
                in
                    List.fold_left
                        (fun acc bl -> BG.append_handler_blocks acc bl)
                        [] (bc_order@[bc; remaining_bc]))
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
             *     (event * (hndl name, hndl args, ordered_blocks)) list
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
    (* The main bytecode compilation function *)
    let compile_terms  (db_schema: (string * var_t list) list)
                       (mapn: string)
                       (params: var_t list)
                       (compilation_level: int)
                       (term_l: A.term_t list)
                       : string * ((handler * event) list)
            =
        let events_and_handlers = compile_terms_to_bytecode
            db_schema mapn params compilation_level term_l
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
            (compilation_level: int)
            (term_l: A.term_t list)
            =
        let events_and_handlers = compile_terms_to_bytecode
            db_schema mapn params compilation_level term_l
        in
            List.flatten (List.map 
                (fun (_, (_,_,ordered_blocks)) ->
                    List.flatten (List.map
                        (fun bs -> BS.fold (fun el acc -> acc@[el]) bs [])
                        ordered_blocks))
                events_and_handlers)
end
