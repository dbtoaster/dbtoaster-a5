module A = Algebra

open Bytecode
open Bytecode.SimplifiedAlgebra
open Bytecode.InstructionSet

exception CodegenException of string

(* Code terminal interface *)
module type LeafGenerator =
sig
    type incr_op = OpSum | OpProd | OpMin

    (* readable, i.e. can only appear on RHS of statements. *)
    type value

    (* readable and writeable, i.e. can appear on LHS or RHS of statements. *)
    type accumulator

    (* list of statements *)
    type code

    val string_of_code : code -> string

    val empty_value : value
    val empty_code  : code
    val merge_code  : code list -> code

    val get_value             : code -> value
    val get_accumulator_value : accumulator -> value

    (* Typing stringification helpers *)
    val gc_type          : A.type_t -> string
    val gc_var_type      : A.var_t -> string
    val gc_entry_type    : A.type_t list -> string
    val gc_decl_type     : datastructure -> string
    val gc_iterator_type : datastructure -> string

    (* returns readable values (i.e. names) for constants,
     * variables and datastructures *)
    val gc_const     : A.const_t   -> value
    val gc_var       : A.var_t     -> value
    val gc_ds_var    : ds_var_t    -> value

    (* returns a readable and writable name for a map entry *)
    val gc_map_entry : map_entry_t -> accumulator

    (* returns a tuple value for a list of variables *)
    val gc_tuple     : A.var_t list  -> value

    (* returns a freshly allocated accumulator of the given type, along
     * with its declaration. This accumulator will be incremented by
     * incr_op (used to determine an initial value). *)
    val gc_temporary : A.type_t -> incr_op -> (accumulator * code)

    (* returns the code enclosed in a basic block *)
    val gc_block     : code -> code

    (* returns a comparison implemented functionally with a ternary
     * conditional operator *)
    val gc_ternary_conditional :
        (A.comp_t * value * value) list -> value -> value -> value

    val gc_map_predicate:
        (A.comp_t * value * value) list -> code -> code -> code

    val gc_for_loop : datastructure -> ds_var_t -> A.var_t list -> code -> code

    val gc_constraint : code -> A.comp_t -> value -> value -> code

    val gc_relation : code -> datastructure -> ds_var_t -> A.var_t list -> code

    val gc_union : code list -> code

    val gc_arith_sum : value -> value -> value
    val gc_arith_prod: value -> value -> value

    val gc_multiset_insert: ds_var_t -> value -> code
    val gc_multiset_delete: ds_var_t -> value -> code

    val gc_set_insert: ds_var_t -> value -> code
    val gc_set_delete: ds_var_t -> value -> (A.var_t * datastructure * ds_var_t) list -> code

    val gc_incr : accumulator -> incr_op -> value -> code

    val gc_map_set    : map_entry_t -> value -> code
    val gc_map_update : map_entry_t -> value -> code
    val gc_map_insert : map_entry_t -> code -> code
    val gc_map_delete : map_entry_t -> (A.var_t * datastructure * ds_var_t) list -> code

    val gc_declarations: (ds_var_t * datastructure) list -> code

    val gc_arg_list : A.var_t list -> code list
    val gc_fun_decl: string -> A.var_t list -> code -> ds_var_t * datastructure -> code

    (* HACK *)
    val gc_init_delete_args : A.var_t list -> A.var_t list -> code
end


(* Code generation backends *)
module CPPGenOptions :
sig
    val use_pool_allocator : bool ref
    val simple_profiler_enabled : bool ref
    val log_results : bool ref
end =
struct
    let use_pool_allocator = ref true
    let simple_profiler_enabled = ref true

    (* TODO: implement result logging for maps *)
    let log_results = ref true
end

module CPPLeafGen : LeafGenerator =
struct
    type incr_op = OpSum | OpProd | OpMin

    type accumulator = string
    type value = string
    type condition = string
    type code = string list

    (* Code stringification helpers *)
    let tab = "    "
    let rec indent i s = if i = 0 then s else indent (i-1) (tab^s)

    let gc_stmt s = s^";"

    let wrap_lines c =
        let rec break_line s len level acc = 
            let indent_break l =
                if acc = [] then [l] else (acc@[indent level l])
            in
            if (String.length s) <= len then indent_break s
            else
                (* Break at nearest whitespace to len *)
                let delims = ['?'; '('; ' '] in
                let last_pos =
                    List.fold_left
                        (fun pos d -> if pos <> len then pos else
                            try String.rindex_from s len d
                            with Not_found -> len)
                        len delims
                in
                let (hd, tl) = (String.sub s 0 (last_pos+1),
                    String.sub s (last_pos+1)
                        ((String.length s) - (last_pos+1)))
                in
                    break_line tl len level (indent_break hd)
        in
            List.fold_left
                (fun acc stmt ->
                    let indent_level = 
                        let pos = ref 0 in
                            while (String.get stmt (!pos)) = ' ' do
                                incr pos done;
                            ((!pos) / 4)+1
                    in
                    acc@(break_line stmt 80 indent_level [])) [] c

    let string_of_code c = String.concat "\n" (wrap_lines c)
    let indent_code c = List.map (fun s -> tab^s) c

    let empty_value = ""
    let empty_code = []
    let merge_code cl = List.flatten cl

    let get_value code =
        if code = empty_code then ""
        else if (List.length code) = 1 then (List.hd code)
        else
            raise (CodegenException
                ("Invalid value: "^(string_of_code code)))

    let get_accumulator_value a = a

    (* Type stringifiers *)
    let gc_type type_t =
        begin match type_t with
            | A.TInt -> "int"
            | A.TLong -> "int64_t"
            | A.TDouble -> "double"
            | A.TString -> "string"
        end

    let gc_var_type (var_name, type_t) = gc_type type_t

    let gc_entry_type type_l =
        let (prefix,suffix) =
            if List.length type_l = 1 then ("", "") else ("tuple<", ">")
        in
            prefix^(String.concat "," (List.map gc_type type_l))^suffix

    (* Note: CPPGenOptions.use_pool_allocator determines if we use
     * Boost's pool allocator or the standard STL allocator *)
    let gc_decl_type decl =
        begin match decl with
            | Multiset (type_l) ->
                  let tuple_type = gc_entry_type type_l in
                  let suffix = if List.length type_l > 1 then " >" else ">" in
                      if !CPPGenOptions.use_pool_allocator then
                          let compare_type = "std::less<"^tuple_type^suffix in
                          let allocator_type =
                              "boost::pool_allocator<"^tuple_type^suffix
                          in
                              "multiset<"^tuple_type^", "^compare_type^", "^
                                  allocator_type^" >"

                      else "multiset<"^tuple_type^suffix

            | Set (type_l) ->
                  let tuple_type = gc_entry_type type_l in
                  let suffix = if List.length type_l > 1 then " >" else ">" in
                      if !CPPGenOptions.use_pool_allocator then
                          let compare_type = "std::less<"^tuple_type^suffix in
                          let allocator_type =
                              "boost::pool_allocator<"^tuple_type^suffix
                          in
                              "set<"^tuple_type^", "^compare_type^", "^
                                  allocator_type^" >"

                      else "set<"^tuple_type^suffix

            | Map(type_l, value_type) ->
                  if type_l = [] then gc_type value_type else
                      let key_type = gc_entry_type type_l in
                      let val_type = gc_type value_type in
                          if !CPPGenOptions.use_pool_allocator then
                              let kv_type = "pair<"^key_type^","^val_type^">" in
                              let compare_type = "std::less<"^key_type^" >" in
                              let allocator_type =
                                  "boost::pool_allocator<"^kv_type^" >"
                              in
                                  "map<"^key_type^","^val_type^","^compare_type^","^
                                      allocator_type^" >"
                          else "map<"^key_type^","^(gc_type value_type)^">"
        end

    let gc_iterator_type datastructure =
        (gc_decl_type datastructure)^"::iterator"

    (* Operator, and value stringifiers *)
    let gc_comp_op op =
        match op with
            | A.Eq -> "==" | A.Lt -> "<" | A.Le -> "<=" | A.Neq -> "<>"

    let gc_const (v) = 
        match v with
            | A.Int(i) -> string_of_int(i)
            | A.Long(l) -> Int64.to_string(l)
            | A.Double(d) ->
                  (* string_of_float 0.0 = "0." *)
                  if d = 0.0 then "0.0" else string_of_float(d)

            | A.String(s) -> "\""^s^"\""

    let gc_var v = fst v

    let gc_ds_var dsv = dsv

    let rec gc_key k =
        let fold_binop op l =
            List.fold_left
                (fun acc k ->
                    if (String.length acc) = 0 then "("^k^")"
                    else acc^" "^op^" ("^k^")")
                "" (List.map gc_key l)
        in
            begin match k with
                | MKConst(v) -> gc_const v
                | MKVar(v) -> gc_var v
                | MKSum(k_l) -> fold_binop "+" k_l
                | MKProduct(k_l) -> fold_binop "*" k_l
            end

    let gc_map_keys map_keys =
        let (prefix, suffix) =
            if (List.length map_keys) = 1 then ("", "")
            else ("make_tuple(", ")")
        in
            prefix^(String.concat "," (List.map gc_key map_keys))^suffix

    let gc_map_entry (map_var, map_keys) =
        (gc_ds_var map_var)^
            (if map_keys = [] then ""
            else ("["^(gc_map_keys map_keys)^"]"))

    let gc_tuple fields =
        let fields_val = (String.concat "," (List.map gc_var fields)) in
            if List.length fields = 1 then fields_val
            else "make_tuple("^fields_val^")"

    (* Declaration stringifiers *)
    let gc_basic_decl var_t =
        (gc_var_type var_t)^" "^(gc_var var_t)

    let gc_decl (var_t, val_const_t) =
        (gc_basic_decl var_t)^
            " = "^(gc_const val_const_t)^";"

    let gc_iterator_value datastructure iterator counter = 
        let msg dstype =
            "Invalid "^dstype^" tuple access to field "^
                (string_of_int counter) in
        let prefix_suffix t_l =
            if List.length t_l = 1 then ("","")
            else (("get<"^(string_of_int counter)^">("), ")")
        in
        match datastructure with
            | Multiset(f_t) ->
                  let (prefix, suffix) = prefix_suffix f_t in
                      if counter < (List.length f_t) then
                          prefix^"*"^iterator^suffix
                      else raise (CodegenException (msg "multiset"))

            | Set(f_t) ->
                  let (prefix, suffix) = prefix_suffix f_t in
                      if counter < (List.length f_t) then
                          prefix^"*"^iterator^suffix
                      else raise (CodegenException (msg "set"))

            | Map(k_t,v_t) ->
                  let (prefix, suffix) = prefix_suffix k_t in
                      if counter < (List.length k_t) then
                          prefix^iterator^"->first"^suffix
                      else raise (CodegenException (msg "map"))

    let gc_ds_decl datastructure var_t iterator counter =
        (gc_basic_decl var_t)^" = "^
            (gc_iterator_value datastructure iterator counter)^";"

    (* Data structure declarations *)
    let gc_declarations decls =
        merge_code (List.map
            (fun (n, ds) -> [gc_stmt((gc_decl_type ds)^" "^n)]) decls)

    (* Temporaries name generation *)
    let iterator_counter = ref 0
    let gc_iterator ds iterator_name = 
        incr iterator_counter;
        ds^"_"^iterator_name^(string_of_int (!iterator_counter))

    let temporary_counter = ref 0
    let gc_temporary temp_type_t incr_op =
        let init_val = match incr_op with
            | OpSum -> A.Int(0)
            | OpProd -> A.Int(1)
            | OpMin -> A.Int(max_int)
        in
            incr temporary_counter;
            let r = "var"^(string_of_int (!temporary_counter)) in
            let var = (r, temp_type_t) in
                (r, [gc_decl (var, init_val)])

    (* Basic control flow primitive stringifiers:
     * -- if statements, blocks, for loops
     *)
    let gc_block code = ["{"]@(indent_code code)@["}"]
    let gc_value_block value : value = "( "^value^" )"

    let gc_comparison (op, l, r) : value =
        gc_value_block (l^" "^(gc_comp_op op)^" "^r)

    let gc_if_cond cond then_code else_code =
        ["if "^cond]@
            (gc_block then_code)@
            (if else_code = empty_code then []
            else ["else "]@(gc_block else_code))

    let gc_conjunctive_predicate conjunct_val_l =
        (String.concat " && " conjunct_val_l)

    let gc_ternary_conditional conjunct_l then_val else_val =
        let cond = 
            let r = gc_conjunctive_predicate
                (List.map gc_comparison conjunct_l)
            in
                if (List.length conjunct_l) = 1 then r
                else (gc_value_block r)
        in
        let r =
            cond^"? "^
                (gc_value_block then_val)^
                (" : ")^
                (if else_val = empty_value then "assert(0)"
                else (gc_value_block else_val))
        in
            gc_value_block r

    let gc_map_predicate conjunct_l then_code else_code =
        let cond = 
            let r = gc_conjunctive_predicate
                (List.map gc_comparison conjunct_l)
            in
                if (List.length conjunct_l) = 1 then r
                else (gc_value_block r)
        in
            gc_if_cond cond then_code else_code

    let gc_for_loop datastructure dom_var loop_vars inner_code =
        let it_type_t = gc_iterator_type datastructure in
        let (begin_it, end_it) =
            (gc_iterator dom_var "it", gc_iterator dom_var "end") in
        let loop_var_decls =
            fst (List.fold_left
                (fun (decl_acc,counter) var_t ->
                    (decl_acc@
                        [gc_ds_decl datastructure var_t begin_it counter],
                    counter+1))
                ([], 0) loop_vars)
        in
        let ds_name = gc_ds_var dom_var in
        let loop_code =
            [gc_stmt (it_type_t^" "^begin_it^" = "^ds_name^".begin()");
             gc_stmt (it_type_t^" "^end_it^" = "^ds_name^".end()");
            "for (; "^begin_it^" != "^end_it^"; ++"^begin_it^")"]
        in
            loop_code@(gc_block (loop_var_decls@inner_code))

    let gc_constraint inner_code op lv rv =
        (["if "^(gc_comparison (op, lv, rv))])@(gc_block inner_code)

    let gc_relation inner_code ds ds_var loop_vars =
        gc_for_loop ds ds_var loop_vars inner_code

    (* Notes:
     * -- we assume an accumulator model of computing aggregates, where
          unions can be defined as sequential accumulation. 
          It does not work for a functional rel. alg. model where we actually
          need to perform the union and return the resulting set.
     * -- this only works when aggregates distribute over unions, i.e. sum/min.
          It does not work for holistic aggregates such as median *)
    let gc_union l = merge_code l

    let gc_arith_binop op lv rv =
        if lv = empty_value then rv
        else if rv = empty_value then lv
        else lv^op^rv

    let gc_arith_sum l r = gc_arith_binop "+" l r
    let gc_arith_prod l r = gc_arith_binop "*" l r

    let gc_multiset_op set_var op tuple =
        if tuple <> "" then
            [set_var^"."^op^"("^tuple^");"]
        else
            let msg = "Invalid tuple code: "^tuple in
                raise (CodegenException msg)

    let gc_multiset_insert set_var tuple =
        gc_multiset_op set_var "insert" tuple

    let gc_multiset_delete set_var tuple =
        gc_multiset_op set_var "erase" tuple

    let gc_set_op set_var op tuple =
        if tuple <> "" then
            [set_var^"."^op^"("^tuple^");"]
        else
            let msg = "Invalid tuple code: "^tuple in
                raise (CodegenException msg)

    let gc_set_insert set_var tuple = gc_set_op set_var "insert" tuple

    (* TODO: loop over datastructure checking for existing of dependent vars
     * -- this should really be indexed for efficiency, but this complicates other
     *    parts of the code. *)
    let gc_set_delete set_var tuple deps =
        (*
        let delete_conjuncts = List.map (fun ((vn,vt), ds, dsv) ->
            match ds with
                | Multiset _ -> gc_value_block (dsv^".find("^vn^") == "^dsv^".end()")
                | Set _ -> gc_value_block (dsv^".find("^vn^") == "^dsv^".end()")
                | _ -> raise (Failure
                      ("gc_set_delete: unexpected set datastructure dependency.")))
            deps
        in
        let delete_pred = gc_conjunctive_predicate delete_conjuncts in
        let delete_code = gc_set_op set_var "delete" tuple in
            gc_if_cond delete_pred delete_code empty_code
        *)
        empty_code

    let gc_incr accumulator op rv =
        let (acc_op, acc_val) = 
            match op with
                | OpSum -> ("+=", rv)
                | OpProd -> ("*=", rv)
                | OpMin -> ("=", "min("^accumulator^", "^rv^")")
        in
            [gc_stmt (accumulator^" "^acc_op^" "^acc_val)]

    let gc_map_set map_entry set_value =
        let assign_val = gc_map_entry map_entry in
            [gc_stmt (assign_val^" = "^set_value)]

    let gc_map_update map_entry update_value =
        let assign_val = gc_map_entry map_entry in
            [gc_stmt (assign_val^" += "^update_value)]

    let gc_map_insert map_entry insert_code =
        let (map_var, map_keys) = map_entry in
        let map_key = gc_map_keys map_keys in
        let cond = gc_value_block
            ((gc_ds_var map_var)^
                ".find("^map_key^") == "^map_var^".end()")
        in
            gc_if_cond cond insert_code empty_code

    (* Note: for now, all bigsum var domains are single field multisets *)
    let gc_map_delete (map_var, map_keys) deps =
        let delete_conjuncts = List.map (fun ((vn,vt), ds, dsv) -> 
            match ds with
                | Multiset([t]) -> gc_value_block (dsv^".find("^vn^") == "^dsv^".end()")
                | Set([t]) -> gc_value_block (dsv^".find("^vn^") == "^dsv^".end()")
                | _ -> raise (Failure
                      ("gc_map_delete: unexpected map datastructure dependency.")))
            deps
        in
        let delete_pred = gc_conjunctive_predicate delete_conjuncts in
        let delete_code = 
            [gc_stmt ((gc_ds_var map_var)^
                ".erase("^(gc_map_keys map_keys)^")")]
        in
            gc_if_cond delete_pred delete_code empty_code
                

    let gc_arg_list v_l = List.map (fun v -> [gc_basic_decl v]) v_l

    let gc_fun_decl handler_name handler_vars handler_body (result_n, result_ds) = 
        let handler_decl =
            (* Note: file args must be kept synced with function object declaration in
             *     Runtime.generate_stream_engine_file_decl_and_init
             *     streamengine.h:
             *         type FileStreamDispatcher::Handler,
             *         FileStreamDispatcher.dispatch()  *)
            let file_args = "ofstream* results, ofstream* log, ofstream* stats" in
            let var_args =
                String.concat "," (List.flatten (gc_arg_list handler_vars)) in
            let arg_sep = (if file_args = "" || handler_vars = [] then "" else ", ")
            in
                ["void "^handler_name^"("^file_args^arg_sep^var_args^")"]
        in
        let profiled_body =
            if !CPPGenOptions.simple_profiler_enabled then
                (* Only generate this code if the simple profiler is enabled. *)
                ["struct timeval hstart, hend;";
                "gettimeofday(&hstart, NULL);"]@
                handler_body@
                ["gettimeofday(&hend, NULL);";
                "DBToaster::Profiler::accumulate_time_span(hstart, hend, "^
                    handler_name^"_sec_span, "^handler_name^"_usec_span);"]
            else handler_body
        in
        let body_with_logging =
            if !CPPGenOptions.log_results then
                (* TODO: support map output for queries with params *)
                let r = match result_ds with
                    | Map([],t) ->
                          ["(*results) << \""^handler_name^
                              "\" << \",\" << "^result_n^" << endl;"]
                    | _ -> []
                in
                    profiled_body@r
            else profiled_body
        in
            merge_code [handler_decl; (gc_block body_with_logging);]

    let gc_init_delete_args orig_args handler_args =
        List.map2
            (fun ov ((hn,ht) as hv) ->
                let negate = if ht = A.TString then "" else "-" in
                    gc_stmt ((gc_basic_decl ov)^" = "^negate^(gc_var hv)))
            orig_args handler_args
end


(* TODO: write up module signature *)

module BlockGenerator =
    functor (L : LeafGenerator) ->
struct
    module RB = RelAlgBase
    module R = RelAlg

    module MB = MapAlgBase
    module M = MapAlg

    module AMB = ArithMapAlgBase
    module AM = ArithMapAlg

    let fold_first f init l = List.fold_left f (init (List.hd l)) (List.tl l)

    (*
     * Nested relational and map algebra code generation
     *)

    (* RelAlgBase.lf_t -> code -> code *)
    let rec gc_relalg_lf_t lf outer_code : L.code =
        (* generates code to recursively accumulate a map term, returning
         * the code, and the accumulator.
         * TODO: the recursively compiled code may be extremely simple,
         * e.g. a constant or var if the code is not nested. Right now
         * we rely on the C++ compiler to optimize this away for efficiency *)
        (* MapAlg.term_t -> code * accumulator *)
        let gc_term t =
            let t_type = term_type t [] in
            let (t_acc, acc_decl_code) = L.gc_temporary t_type L.OpSum in
            let t_code =
                let r = gc_mapalg_t t_acc t_type L.OpSum t in
                    L.merge_code [acc_decl_code; r]
            in
                (t_code, t_acc)
        in
        match lf with
            | Empty -> L.empty_code
            | ConstantNullarySingleton -> outer_code
            | AtomicConstraint(op, t1, t2) ->
                  let (l_code, l_acc) = gc_term t1 in
                  let (r_code, r_acc) = gc_term t2 in
                  let constraint_code =
                      let lv = L.get_accumulator_value l_acc in
                      let rv = L.get_accumulator_value r_acc in
                          L.gc_constraint outer_code op lv rv
                  in
                      L.merge_code [l_code; r_code; constraint_code]

            | Rel(n,f,ds) ->
                  L.gc_relation outer_code ds n f

    (* Naive implementation processes constraints inside loops *)
    (* code -> RelAlgBase.term_t list -> code *)
    and gc_relalg_prod_t outer_code prod_list : L.code =
        let (rels, constraints) =
            let is_rel = function | Rel (_,_,_) -> true | _ -> false
            in
                List.partition is_rel prod_list
        in
            List.fold_left
                (fun acc lf -> gc_relalg_lf_t lf acc)
                outer_code (constraints@rels)

    (* code -> RelAlg.expr_t -> code *)
    and gc_relalg_t outer_code r : L.code =
        let f_prod = function
            | R.PProduct(prod_l) -> gc_relalg_prod_t outer_code prod_l
        in
            match r with
                | R.PTVal(lf) -> gc_relalg_lf_t lf outer_code
                | R.PTProduct(prod_l) -> f_prod prod_l
                | R.PSum(sum_l) -> L.gc_union (List.map f_prod sum_l)

    (* Naive code generation for an arbitrarily nested map term *)
    (* accumulator -> type_t -> incr_op -> mapalg_t -> code *)
    and gc_mapalg_t accumulator acc_type accum_op t  : L.code =

        (* wraps a nested accumulation, including:
         * -- generating a temporary for the nested accumulation if it
         *    uses a different increment operator than the current
         * -- generates code for:
         *    ++ i) the nested accumulation using the gc_f argument
         *    ++ ii) incrementing the current accumulation
        *)
        let wrap_accumulator inner_accum_op gc_f =
            let (recur_acc, recur_type, recur_decl_opt, recur_op) =
                if accum_op = inner_accum_op then
                    (accumulator, acc_type, None, accum_op)
                else
                    let (acc,decl) = L.gc_temporary acc_type inner_accum_op in
                        (acc, acc_type, Some(decl), inner_accum_op)
            in
            let next_code = gc_f recur_acc recur_type recur_op in
            let code_for_merging =
                (match recur_decl_opt with None -> [] | Some(c) -> [c])@
                (if recur_acc <> accumulator then
                    let accum_code =
                        L.gc_incr accumulator accum_op
                            (L.get_accumulator_value recur_acc)
                    in
                        [next_code; accum_code]
                else [next_code])
            in
                L.merge_code code_for_merging

        in

        (* generates an accumulator for Agg(f,r):
         * -- recursively generates code to accumulate f
         * -- uses code for f as the inner code for r *) 
        let gc_agg agg_op f r =
            let gc_f recur_acc recur_type recur_op =
                let inner_code = gc_mapalg_t recur_acc recur_type recur_op f in
                    gc_relalg_t inner_code r
            in
                wrap_accumulator agg_op gc_f
        in
        let gc_recur_and_acc_list acc_op l =
            let gc_f recur_acc recur_type recur_op =
                List.fold_left
                    (fun acc s ->
                        let sc = gc_mapalg_t recur_acc recur_type recur_op s in
                            L.merge_code [acc; sc])
                    L.empty_code l
            in
                if (List.length l) = 1 then
                    gc_mapalg_t accumulator acc_type accum_op (List.hd l)
                else
                    wrap_accumulator acc_op gc_f
        in
        let gc_mapalg_lf_t accumulator acc_type accum_op lf =
            begin match lf with
                | Const(c) -> L.gc_incr accumulator accum_op (L.gc_const c)
                | Var(v) -> L.gc_incr accumulator accum_op (L.gc_var v)
                | AggSum(f, r) -> gc_agg L.OpSum f r
            end
        in
        let prod_f = function | M.PProduct(l) ->
            (* TODO: simply call rather than recur *)
            gc_recur_and_acc_list L.OpProd
                (List.map (fun lf -> M.PTVal(lf)) l)
        in
            begin match t with
                | M.PTVal(lf) -> gc_mapalg_lf_t accumulator acc_type accum_op lf
                | M.PTProduct(prod_l) -> prod_f prod_l
                | M.PSum(l) ->
                      gc_recur_and_acc_list L.OpSum
                          (List.map (fun x -> M.PTProduct(x)) l)
            end


    (*
     * Code generation for arithmetic map expression
     *)

    (* arith_mapalg_term_t -> value *)
    let gc_arith_mapalg_lf_t a : L.value =
        match a with
            | AMConst(c) -> L.gc_const c
            | AMVar(v) -> L.gc_var v
            | AMMapEntry(e) -> L.get_accumulator_value (L.gc_map_entry e)

    (* ArithMapAlgBase.t -> value *)
    let rec gc_arith_mapalg_lf_expr_t a : L.value =
        match a with
            | AMVal(v) -> gc_arith_mapalg_lf_t v
            | AMIfThen(constraint_l,v) ->
                  let constraint_val_l = List.map
                      (fun (op,l,r) ->
                          let lv = gc_arith_mapalg_t l in
                          let rv = gc_arith_mapalg_t r in
                              (op, lv, rv))
                      constraint_l
                  in
                      L.gc_ternary_conditional constraint_val_l
                          (gc_arith_mapalg_t v) (L.gc_const (A.Int(0)))

    (* ArithMapAlgBase.t list -> value *)
    and gc_arith_mapalg_prod_t prod_list : L.value =
        let gc_prod acc lf =
            L.gc_arith_prod acc (gc_arith_mapalg_lf_expr_t lf)
        in
            fold_first gc_prod gc_arith_mapalg_lf_expr_t prod_list

    (* ArithMapAlg.expr_t -> value *)
    and gc_arith_mapalg_t arith_expr : L.value =
        let f_prod =
            function | AM.PProduct(prod_l) -> gc_arith_mapalg_prod_t prod_l
        in
        let gc_sum acc prod = L.gc_arith_sum acc (f_prod prod) in
            begin match arith_expr with
                | AM.PTVal(term) -> gc_arith_mapalg_lf_expr_t term
                | AM.PTProduct(prod_l) -> f_prod prod_l
                | AM.PSum(sum_l) -> fold_first gc_sum f_prod sum_l
            end

    (* Assignment, block code generation *)

    (* rel_assign_t -> code *)
    let gc_rel_assign_t s : L.code =
        let gc_relalg f sv tv ra = gc_relalg_t (f sv (L.gc_tuple tv)) ra in
            begin match s with
                | MultisetInsert(ds, sv, tv, ra) ->
                      gc_relalg L.gc_multiset_insert sv tv ra

                | MultisetDelete(ds, sv, tv, ra) ->
                      gc_relalg L.gc_multiset_delete sv tv ra

                | SetInsert(ds, sv, tv) -> L.gc_set_insert sv (L.gc_tuple tv)
                | SetDelete(ds, sv, tv, deps) -> L.gc_set_delete sv (L.gc_tuple tv) deps
            end

    (* map_assign_t -> code *)
    let gc_map_assign_t s : L.code =
        begin match s with
            | MapSet (ds, entry, arith_expr) ->
                  L.gc_map_set entry (gc_arith_mapalg_t arith_expr)

            | MapUpdate (ds, entry, arith_expr) ->
                  L.gc_map_update entry (gc_arith_mapalg_t arith_expr)

            | MapInsert (ds, entry, map_expr) ->
                  let val_type = match ds with
                      | Map(_,vt) -> vt
                      | _ -> raise (CodegenException
                            "Expected a map datastructure.")
                  in
                  let insert_code =
                      gc_mapalg_t (L.gc_map_entry entry)
                          val_type L.OpSum map_expr
                  in
                      L.gc_map_insert entry insert_code

            | MapDelete (ds, entry, deps) -> L.gc_map_delete entry deps
        end

    (* flat_assign_t -> code *)
    let gc_flat_assign_t s : L.code =
        match s with
            | RelAssign s_l -> L.merge_code (List.map gc_rel_assign_t s_l)
            | MapAssign s_l -> L.merge_code (List.map gc_map_assign_t s_l)

    (* nested_assign_t -> code *)
    let rec gc_nested_assign_t s : L.code =
        match s with
            | Assign (fs_l) ->
                  L.merge_code (List.map gc_flat_assign_t fs_l)

            | IfThen (pred, ns_l) ->
                  let pred_val_l = List.map
                      (fun (op,l,r) ->
                          let lv = gc_arith_mapalg_t l in
                          let rv = gc_arith_mapalg_t r in
                              (op, lv, rv))
                      pred
                  in
                      L.gc_map_predicate pred_val_l
                          (L.merge_code
                              (List.map gc_nested_assign_t ns_l)) L.empty_code

            | ForEach (ds, ds_var, loop_v, ch_s_l) ->
                  L.gc_for_loop ds ds_var loop_v
                      (L.merge_code (List.map gc_nested_assign_t ch_s_l))

    (* nested_block_t -> code *)
    let gc_nested_block_t b : L.code =
        let (_,_,s_l) = b in
            L.merge_code (List.map gc_nested_assign_t s_l)

    (* (nested_block_t set) list -> code *)
    let generate_nested_block_code assignments : L.code =
        let gc_nb_set bs = NestedBlockSet.fold
            (fun b acc -> L.merge_code [acc; (gc_nested_block_t b)])
            bs L.empty_code
        in
            (List.fold_left
                (fun acc bs ->
                    L.merge_code [acc; (gc_nb_set bs)])
                L.empty_code assignments)


    (* Code generation API *)
    (* (event * (string * var_t list * ordered_blocks)) list
     *     -> string * (code list) *)
    let generate_code result_map declarations handler_assignments =
        let find_var_marker varname =
            try (Str.search_backward (Str.regexp (Str.quote "__"))
                varname (String.length varname))+2
            with Not_found -> 0
        in
        let create_arg_mapping args =
            List.map (fun (n,t) ->
                let marker_pos = find_var_marker n in
                let orig_var = String.sub
                    n marker_pos ((String.length n) - marker_pos)
                in
                    ((n, t), (orig_var, t)))
                args
        in
        let rename_handler_args ord_blocks mapping =
            List.map (fun bs ->
                NestedBlockSet.fold
                    (fun (mapn,env,assigns) acc ->
                        let new_assigns = substitute_variables assigns mapping
                        in
                            NestedBlockSet.add (mapn,env,new_assigns) acc)
                    bs NestedBlockSet.empty) ord_blocks
        in
        (* Handler profiling declarations, defined here as handler declarations. 
         * TODO: cleanly generate profiling code here, rather than in L.gc_fun_decl
         * Note: the code to display the total execution time per handler is
         * generated in the CPPInstrumentation module. *)
        let handler_decls =
            if !CPPGenOptions.simple_profiler_enabled then
                String.concat "\n" (List.map 
                    (fun (_, (handler_name, _, _)) ->
                        let r = 
                            ["double "^handler_name^"_sec_span = 0.0;";
                            "double "^handler_name^"_usec_span = 0.0;"]
                        in
                            String.concat "\n" r)
                    handler_assignments)
            else ""
        in
        let handlers =
            List.map (fun (event, (handler_name, handler_args, ord_blocks)) -> 
                (*let (event_type, rel) = event in*)

                (* Rename args and body to match user specified args *)
                let handler_arg_mapping = create_arg_mapping handler_args in
                let renamed_args = List.map snd handler_arg_mapping in
                let renamed_ord_blocks =
                    rename_handler_args ord_blocks handler_arg_mapping
                in
                let handler_body = 
                    generate_nested_block_code renamed_ord_blocks in
                let handler_code =
                    L.gc_fun_decl handler_name renamed_args handler_body
                        (result_map, List.assoc result_map declarations)
                in
                let arg_decls = 
                    List.map L.string_of_code (L.gc_arg_list renamed_args)
                in
                let handler_spec =
                    (handler_name,
                    (List.combine (List.map fst renamed_args) arg_decls),
                    L.string_of_code handler_code)
                in
                    (event, handler_spec))
                handler_assignments
        in
        let all_decls =
            (L.string_of_code (L.gc_declarations declarations))^"\n\n"^
                handler_decls^"\n\n"
        in
            (all_decls, handlers)
end

module CPPCodeGenerator = BlockGenerator(CPPLeafGen)

module CPPInstrumentation =
struct
    let rec indent i s = if i = 0 then s else indent (i-1) ("   "^s)

    let generate_memory_analysis_fn declarations =
        let complex_declarations = 
            List.filter
                (fun (_, ds) -> match ds with
                    | Multiset(l) -> true
                    | Set(l) -> true
                    | Map(k_l,_) -> List.length k_l > 0)
                declarations
        in
        let print_expression msg expr =
            "cout << \""^msg^"\" << ("^expr^") << endl;\n"
        in
        (* Note: assumes the method has an argument: ofstream* stats *)
        let log_expression msg expr =
            "(*stats) << \"m,\" << \""^msg^"\" << \",\" << ("^expr^") << endl;\n"
        in
        let get_size_estimate dsvar ds dstype =
            match ds with
                | Multiset _
                | Set _ ->
                      (* (val sz + elem sz) * #elems + container sz *)
                      let key_type = dstype^"::key_type" in
                      let val_type = dstype^"::value_type" in
                      let compare_type = dstype^"::key_compare" in
                      let val_size = "sizeof("^val_type^")" in
                      let elem_size = "sizeof(struct _Rb_tree_node_base)" in
                      let container_size = "(sizeof(struct _Rb_tree<"^
                          key_type^", "^val_type^", "^
                          "_Identity<"^val_type^">, "^compare_type^">))" in
                      let num_elems = dsvar^".size()" in
                          String.concat "\n"
                              (["(("^val_size;
                              (indent 2 (" + "^elem_size^")"));
                              (indent 2
                                  (" * "^num_elems^")  + "^container_size) )])

                | Map _ ->
                      (* (key sz + val sz + elem sz) * #elems + container sz *)
                      let key_type = dstype^"::key_type" in
                      let val_type = dstype^"::mapped_type" in
                      let kv_type = dstype^"::value_type" in
                      let compare_type = dstype^"::key_compare" in
                      let key_size = "sizeof("^key_type^")" in
                      let val_size = "sizeof("^val_type^")" in
                      let elem_size = "sizeof(struct _Rb_tree_node_base)" in
                      let container_size = "(sizeof(struct _Rb_tree<"^
                          key_type^", "^kv_type^", "^
                          "_Select1st<"^kv_type^">, "^compare_type^">))"
                      in
                      let num_elems = dsvar^".size()" in
                          String.concat "\n"
                              (["(("^key_size;
                              (indent 2 (" + "^val_size));
                              (indent 2 (" + "^elem_size^")"));
                              (indent 2
                                  (" * "^num_elems^")  + "^container_size))])
        in
        let output_exprs =
            List.flatten (List.map
                (fun (dsvar, ds) -> 
                    let ds_type = CPPLeafGen.gc_decl_type ds in
                    let size_expr = get_size_estimate dsvar ds ds_type in
                        [print_expression (dsvar^" size: ") size_expr;
                         log_expression dsvar size_expr])
                complex_declarations)
        in
        let body =
            (* Note: method name be synced with its usage in Runtime *)
            ["void analyse_mem_usage(ofstream* stats)"; "{"]@
                (List.map (indent 1) output_exprs)@["}"]
        in
            (String.concat "\n" body)^"\n\n"

    let generate_handler_analysis_fn handlers =
        let print_expression msg cost_expr =
            "cout << "^msg^" << "^cost_expr^" << endl;" in
        (* Note: assumes the method has an argument: ofstream* stats *)
        let log_expression msg cost_expr = 
            "(*stats) << \"h,\" << "^msg^" << \",\" << "^cost_expr^" << endl;"
        in
        if !CPPGenOptions.simple_profiler_enabled then
            let output_exprs =
                List.flatten (List.map
                    (fun (_, (handler_name, _, _)) ->
                        let print_msg = "\""^handler_name^" cost: \"" in
                        let log_msg = "\""^handler_name^"\"" in
                        let cost_expr = "("^handler_name^"_sec_span + ("^
                            handler_name^"_usec_span / 1000000.0))"
                        in
                            [print_expression print_msg cost_expr;
                             log_expression log_msg cost_expr])
                    handlers)
            in
            let body =
                (* Note: method name be synced with its usage in Runtime *)
                ["void analyse_handler_usage(ofstream* stats)"; "{"]@
                    (List.map (indent 1) output_exprs)@["}"]
            in
                (String.concat "\n" body)^"\n\n"
        else ""

    (* declarations -> string *)
    let generate_declarations_instrumentation declarations handlers =
        let mem_analysis_fn = generate_memory_analysis_fn declarations in
        let handler_analysis_fn = generate_handler_analysis_fn handlers in
            mem_analysis_fn^handler_analysis_fn
end

;;

module Test =
struct
    module A = Algebra
    
    module RB = RelAlgBase
    module R = RelAlg

    module MB = MapAlgBase
    module M = MapAlg

    module AMB = ArithMapAlgBase
    module AM = ArithMapAlg
        
    module S = NestedBlockSet

    module CPP = CPPCodeGenerator

    let declarations = [("m", Map([A.TInt], A.TInt))]

    let mp2_incr_init_block =
        let bids_ds = Multiset([A.TInt; A.TInt]) in
        let bids1 = Rel("bids", [("P1", A.TInt); ("V1", A.TInt)], bids_ds) in
        let bids2 = Rel("bids", [("P2", A.TInt); ("V2", A.TInt)], bids_ds) in
        let init_code =
            M.PSum([
                M.PProduct[Const(A.Double(0.25));
                    AggSum(M.PTVal(Var("V1", A.TInt)),
                        R.PTProduct(R.PProduct[bids1]))];
                M.PProduct[
                    AggSum(M.PTVal(Var("V2", A.TInt)),
                        R.PTProduct(
                            R.PProduct([AtomicConstraint(A.Lt,
                                M.PTVal(Var("P", A.TInt)),
                                M.PTVal(Var("P2", A.TInt)));
                                bids2])))]])
        in
        let mp2_ds = Map([A.TInt], A.TInt) in
        let mp2_incr =
            MapUpdate(mp2_ds, ("m", [MKVar("P", A.TInt)]),
                AM.PSum([
                    AM.PProduct([AMVal(AMConst(A.Double(0.25)));
                        AMVal(AMVar("V1", A.TInt))]);
                    AM.PProduct([AMIfThen([(A.Lt,
                        AM.PTVal(AMVal(AMVar("P", A.TInt))),
                        AM.PTVal(AMVal(AMVar("P2", A.TInt))))],
                        AM.PTVal(AMVal(AMVar("V2", A.TInt))))])]))
        in
        let mp2_init =
            MapInsert(mp2_ds, ("m", [MKVar("P", A.TInt)]), init_code) in
        let map_assigns = [
            ForEach(mp2_ds, "m", [("P", A.TInt)],
                [Assign([MapAssign([mp2_incr])])]);
            Assign([MapAssign([mp2_init])]); ]
        in
            [S.add
                ("m", [("P", A.TInt); ("V", A.TInt)], map_assigns)
                S.empty]

    let main () =
        let (decls, names_args_handlers_l) =
            let event = ("insert", "BIDS") in
            let handler = 
                ("on_insert_bids",
                [("P", A.TInt); ("V", A.TInt)], mp2_incr_init_block)
            in
            (CPP.generate_code "m" declarations [(event, handler)])
        in
        let output = decls^"\n\n"^
            (String.concat "\n"
                (List.map (fun (_,(_,_,h)) -> h) names_args_handlers_l))
        in
            print_endline output
end;;
