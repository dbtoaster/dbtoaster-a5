open Algebra
open Codegen
open Codegen.CPPLeafGen


type stream_type = string
type source_type = string
type source_args = string
type tuple_type = string
type adaptor_type = string
type adaptor_bindings = (string * string) list
type source_instance = string
type source_info =
        string *
            (stream_type * source_type * source_args * tuple_type *
                adaptor_type * adaptor_bindings * string * source_instance)

type stream_source_type = File | Socket


module RuntimeOptions :
sig
    val reset_frequency : int ref
end = 
struct
    let reset_frequency = ref 10000
end

(************************************
 *
 * Testing / standalone engine generation
 *
 ************************************)

let generate_includes out_chan =
    let list_code l = String.concat "\n" l in
    let includes =
        ["// DBToaster includes.";
        "#include <cmath>";
        "#include <cstdio>";
        "#include <cstdlib>\n";
        "#include <iostream>";
        "#include <map>";
        "#include <list>";
        "#include <set>\n";
        "#include <tr1/cstdint>";
        "#include <tr1/tuple>";
        "#include <tr1/unordered_set>\n";
        "using namespace std;";
        "using namespace tr1;\n\n"]
    in
        output_string out_chan (list_code includes)

(* TODO: move id generation to algebra.ml *)

(* unit -> stream name * stream id *)
let stream_ids = Hashtbl.create 10
let get_stream_id_name stream_name = "stream"^stream_name^"Id"

let generate_stream_id stream_name =
    let (id_name, id) =
        (get_stream_id_name stream_name, Hashtbl.length stream_ids)
    in
        Hashtbl.add stream_ids id_name id;
        (id_name, id)

let fun_obj_id_counter = ref 0
let generate_function_object_id handler_name =
    let r = !fun_obj_id_counter in
        incr fun_obj_id_counter;
        "fo_"^handler_name^"_"^(string_of_int r)


let get_handler_metadata (name, arg_names_and_decls, _) adaptor_type adaptor_bindings =
    let (arg_names, arg_decls) = List.split arg_names_and_decls in
    let input_metadata =
        let get_binding input_instance id =
            if (List.mem_assoc id adaptor_bindings) then
                let in_field = List.assoc id adaptor_bindings in
                    input_instance^"."^in_field
            else raise (CodegenException
                ("Could not find input arg binding for "^id))
        in
        let ifa =
            (fun input_instance ->
                String.concat ","
                    (List.map (fun id -> get_binding input_instance id) arg_names))
        in
            (adaptor_type^"::Result", ifa)
    in
        (name, input_metadata, arg_decls)


let validate_stream_types streams_handlers_and_events check_type =
    let valid =
        List.fold_left
            (fun valid_acc (stream_type_info, stream_name, _, _) ->
                let (stream_type, _,_,_,_,_, _) = stream_type_info in
                    print_endline ("stream "^stream_name^": "^
                        (match stream_type with
                            | File -> "file" | Socket -> "sock"));
                    valid_acc && (stream_type = check_type))
            true streams_handlers_and_events
    in
        if not valid then
            raise (CodegenException
                ("Invalid stream types, "^
                    "streams must all be from files or sockets"))
        else ()


let generate_noop_main out_chan =
    let indent s = "    "^s in
    let list_code l = String.concat "\n" l in
    let noop_main =
        ["int main(int argc, char** argv)";
        "{";
        (indent "cout << \"No sources found.\" << endl;");
        (indent "return 0;");
        "}"]
    in
        output_string out_chan (list_code noop_main)

(*
 * Code generation common to both engine and debugger.
 * -- Standalone components (multiplexers, dispatchers,
 *       init/main function common code).
 *)
let generate_stream_engine_includes out_chan =
    let list_code l = String.concat "\n" l in
    let includes =
        ["\n\n// Stream engine includes.";
        "#include \"standalone/streamengine.h\"";
        "#include \"profiler/profiler.h\"";
        "#include \"datasets/adaptors.h\"";
        "#include <boost/bind.hpp>";
        "#include <boost/shared_ptr.hpp>";
        "#include <boost/pool/pool.hpp>";
        "#include <boost/pool/pool_alloc.hpp>\n\n";
        "using namespace DBToaster::Profiler;\n"; ]
    in
        output_string out_chan (list_code includes)

let generate_socket_stream_engine_common io_service_name =
    let indent s = "    "^s in
    let common_decl =
        ["DBToaster::StandaloneEngine::SocketMultiplexer sources;";
        "void runReadLoop()\n{\n"^
            (indent "sources.read(boost::bind(&runReadLoop));\n")^"}\n\n";]
    in
    let common_main =
        ["init(sources);";
        "runReadLoop();";
        "boost::thread t(boost::bind(";
        (indent "&boost::asio::io_service::run, &"^io_service_name^"));")]
    in
        (common_decl, common_main)


(* Returns declarations and code to add to init() method
 *
 * Type assumptions: 
 *  -- adaptor constructor: adaptor()
 *
 * Add streams to multiplexer
 * -- instantiate input stream sources.
 * -- instantiate adaptors.
 * -- initialize (i.e. buffer data for) input streams
 * -- generate stream id, register with multiplexer via
 *      add_stream(stream id, input stream)
 *
 * add handlers to dispatcher
 *     -- create function object with operator()(boost::any data)
 *     -- cast from boost::any to expected struct, and invoke handler
 *        with struct fields.
 *     -- register handler with dispatcher for a given stream, via
 *          add_handler(stream id, dml type, function object)
 *)
(* TODO: allocation model for inputs? *)
let generate_stream_engine_file_decl_and_init
        stream_type_info stream_name handler event existing_decls =
    let indent s = ("    "^s) in

    let (_, source_type, source_args, tuple_type,
         adaptor_type, adaptor_bindings, _) = stream_type_info in

    (* declare_stream: <stream type> <stream inst>;
     *     static int <stream id name> = <stream id val>;
     * register_stream: sources.addStream< <tuple_type> >(
     *     &<stream inst>, <stream id name>);
     * stream_id: <stream id name>
     *)
    let (declare_stream, register_stream, stream_id) =
        let new_decl = source_type^" "^stream_name^"("^source_args^");\n" in
            if List.mem new_decl existing_decls then
                ([], [], get_stream_id_name stream_name)
            else
                let (id_name, id) = generate_stream_id stream_name in
                let adaptor_name = stream_name^"_adaptor" in
                let new_adaptor =
                    "boost::shared_ptr<"^adaptor_type^"> "^
                        adaptor_name^"(new "^adaptor_type^"());"
                in
                let new_id = ("static int "^id_name^" = "^
                    (string_of_int id)^";\n") in
                let stream_pointer = "&"^stream_name in
                let adaptor_deref = "*"^adaptor_name in
                let reg =
                    "sources.addStream<"^tuple_type^">("^
                        stream_pointer^", "^adaptor_deref^", "^id_name^");"
                in
                    ([ new_decl; new_adaptor; new_id ], [reg], id_name)
    in

    let (handler_name, handler_input_metadata, _) =
        get_handler_metadata handler adaptor_type adaptor_bindings
    in
    let (handler_input_type_name, handler_input_args) =
        handler_input_metadata
    in
    let handler_dml_type = match event with
        | ("insert", _) -> "DBToaster::StandaloneEngine::insertTuple"
        | ("delete", _) -> "DBToaster::StandaloneEngine::deleteTuple"
        | (t,_) -> raise (CodegenException ("Invalid event type: "^t))
    in

    (* declare_handler_fun_obj:
       struct <fun obj type name>
       { <handler ret type> operator() { cast; invoke; } }
       * fun_obj_type: <fun obj type name> *)
    let (declare_handler_fun_obj, fun_obj_type) = 
        let input_instance = "input" in
        let fo_type = handler_name^"_fun_obj" in
            ([("struct "^fo_type^" { ");
            (indent ("void operator()(boost::any data) { "));
            (indent (indent
                (handler_input_type_name^" "^input_instance^" = ")));
            (indent (indent (indent ("boost::any_cast<"^
                handler_input_type_name^">(data); "))));
            (indent (indent
                (handler_name^"("^
                    (handler_input_args input_instance)^");")));
            (indent "}"); "};\n"],
            fo_type)
    in

    (* declare_handler_fun_obj_inst: <fo_type> <fo instance name>; *)
    let (declare_handler_fun_obj_inst, handler_fun_obj_inst) =
        let h_inst = generate_function_object_id handler_name in
        let decl_code = [ handler_name^"_fun_obj "^h_inst^";\n" ] in
            (decl_code, h_inst)
    in

    (* register_handler:
     *     router.addHandler(<stream id name>,
     *         handler dml type, <fo instance name>)
    *)
    let register_handler =
        "router.addHandler("^stream_id^","^
            handler_dml_type^","^handler_fun_obj_inst^");"
    in

    let decl_code =
        declare_stream@declare_handler_fun_obj@declare_handler_fun_obj_inst
    in

    let init_code = register_stream@[register_handler] in

        (decl_code, init_code)


(* Generates init() and main() methods for standalone stream engine for
 * compiled queries.
 * source metadata:
 *   (stream type * tuple type * stream name * handler * event) list
 * source metadata -> unit
*)
let generate_file_stream_engine_init out_chan streams_handlers_and_events =
    let indent s = ("    "^s) in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in

    validate_stream_types streams_handlers_and_events File;

    let (init_decls, init_body) =
        List.fold_left
            (fun (decls_acc, init_body_acc)
                (stream_type_info, stream_name, handler, event) ->
                let (decl_code, init_code) =
                    generate_stream_engine_file_decl_and_init
                        stream_type_info stream_name handler event decls_acc
                in
                    (decls_acc@decl_code, init_body_acc@init_code))
            ([], []) streams_handlers_and_events
    in
    (* void init(multiplexer, sources) { register stream, handler;} *)
    let init_defn =
        let init_code =
            ["\n\nvoid init("^
                "DBToaster::StandaloneEngine::FileMultiplexer& sources,";
             (indent
                 "DBToaster::StandaloneEngine::FileStreamDispatcher& router)");
             "{";]@
                (List.map indent init_body)@[ "}\n\n"; ]
        in
            list_code init_code
    in
        output_string out_chan (list_code init_decls);
        output_string out_chan init_defn


(*
 * Declarations:
 * -- stream dispatcher class
 * -- stream dispatcher instance 
 * -- network data sources 
 *
 * Init code:
 * -- register stream with multiplexer
 *)

let validate_socket_handlers_and_events handlers_and_events =
    if List.length handlers_and_events <> 2 then
        let msg = "Invalid number of handlers and events for socket source,"^
            " expected insert and delete handler/event"
        in
            raise (CodegenException msg)
    else
        begin match (List.hd handlers_and_events,
            List.hd (List.tl handlers_and_events))
        with
            | ((_, ("insert", r1)), (_, ("delete", r2))) when r1 = r2 -> ()
            | ((_, ("delete", r1)), (_, ("insert", r2))) when r1 = r2 -> ()
            | _ ->
                  let msg =
                      "Invalid handlers and events types for socket source"^
                          " expected insert and delete handler/event"^
                          " on the same base relation"
                  in
                      raise (CodegenException msg)
        end


let generate_stream_engine_socket_decl_and_init
        stream_type_info stream_name handlers_and_events io_service_name =
    let indent s = ("    "^s) in
    let (_, source_type, source_args, tuple_type,
         adaptor_type, adaptor_bindings, _) = stream_type_info in

    validate_socket_handlers_and_events handlers_and_events;

    let handlers_metadata =
        List.map
            (fun (h,_) -> get_handler_metadata h adaptor_type adaptor_bindings)
            handlers_and_events
    in

    let get_sd_handler_metadata hm =
        let (h_name, h_input_metadata, h_arg_signatures) = hm in
        let (h_input_type_name, h_input_args_gen) = h_input_metadata in
        let h_args_sig = String.concat "," h_arg_signatures in
        let fn_sig = "void ("^(h_args_sig)^")" in
        let h_fn_ptr_name_and_decl =
            let fn_ptr_name = h_name^"_ptr" in
            let fn_decl = "boost::function<"^fn_sig^"> "^fn_ptr_name^";" in
                (fn_ptr_name, fn_decl)
        in
        let h_fn_name_arg_sigs_gen =
            (h_name, h_arg_signatures, h_input_args_gen)
        in
            ((h_fn_ptr_name_and_decl, h_fn_name_arg_sigs_gen),
            h_input_type_name)
    in
    let get_handler_fn_ptr_init_body (ptr_name, _) (fn_name, arg_sigs, _) =
        let fn_ptr_val = "&"^fn_name in
        let bind_args = 
            let (_,bindings) =
                List.fold_left (fun (cnt,acc) arg ->
                    let new_acc = 
                        (if String.length acc = 0 then ""else (acc^","))^
                            ("_"^(string_of_int cnt))
                    in
                        (cnt+1, new_acc))
                    (1,"") arg_sigs
            in
                if (String.length bindings) = 0 then "" else (","^bindings)
        in
            ptr_name^" = boost::bind("^fn_ptr_val^bind_args^");"
    in
    let decl_code =
        let (stream_dispatcher_type_name, stream_dispatcher_class_decl) =
            let sd_type_name = "dispatch_"^stream_name^"_tuple" in
            let sd_class =
                let adaptor_name = "adaptor" in
                let adaptor_input_name = "inTuple" in
                let adaptor_output_name = "tuple" in
                let handler_input_name = "input" in
                let cast_adaptor_result handler_input_type_name =
                    List.map indent 
                        ([handler_input_type_name^" "^handler_input_name^" = ";
                        (indent ("boost::any_cast<"^
                            handler_input_type_name^">("^
                            adaptor_output_name^".data);"))])
                in
                let sd_handler_metadata =
                    List.map get_sd_handler_metadata handlers_metadata
                in
                let ((handler_fn_ptr_names_and_decls,
                    handler_fn_names_arg_sigs_gen), handler_input_type_names) =
                    let (d,c) = List.split sd_handler_metadata in
                    let (a,b) = List.split d in
                        ((a,b),c)
                in
                let (_,handler_fn_ptr_decls) =
                    List.split handler_fn_ptr_names_and_decls in
                let handler_fn_ptrs_init_body =
                    List.map2
                        get_handler_fn_ptr_init_body
                        handler_fn_ptr_names_and_decls
                        handler_fn_names_arg_sigs_gen
                in
                let handler_fn_ptr_invocations =
                    List.map
                        (fun (fn_name, _, arg_gen) ->
                            fn_name^"("^(arg_gen handler_input_name)^");")
                        handler_fn_names_arg_sigs_gen
                in
                let sd_constructor =
                    List.map indent 
                        ([sd_type_name^"()"; "{";]@
                            (List.map indent handler_fn_ptrs_init_body)@["}\n"])
                in
                (* define function operator:
                 * -- apply adaptor
                 * -- check DML type
                 * -- apply any_cast
                 * -- dispatch *)
                let sd_fn_operator =
                    let body =
                        ["DBToaster::StandaloneEngine::DBToasterTuple "^
                            adaptor_output_name^";";
                        (adaptor_name^"("^adaptor_output_name^", "^
                            adaptor_input_name^");");
                        "if ( "^adaptor_output_name^".type == "^
                            "DBToaster::StandaloneEngine::insertTuple )"; "{"]@
                        (cast_adaptor_result
                            (List.hd handler_input_type_names))@
                        [(indent (List.hd handler_fn_ptr_invocations)); "}";
                        "else if ( "^adaptor_output_name^".type == "^
                            "DBToaster::StandaloneEngine::deleteTuple )"; "{"]@
                        (cast_adaptor_result (List.hd
                            (List.tl handler_input_type_names)))@
                        [(indent (List.hd
                            (List.tl handler_fn_ptr_invocations))); "}";
                        "else { cerr << \"Invalid DML type!\" << endl; }";]
                    in
                        List.map indent
                            (["void operator()(boost::any& "^
                                adaptor_input_name^")"; "{"]@
                                (List.map indent body)@["}"])
                in

                (* declare:
                 * -- adaptor
                 * -- handler function pointers
                 * -- constructor, setting function pointers
                 * -- function operator *)
                    ["struct "^sd_type_name; "{";
                    (indent adaptor_type^" "^adaptor_name^";\n")]@
                    (List.map indent handler_fn_ptr_decls)@["\n"]@
                    sd_constructor@sd_fn_operator@["};\n"]
            in
                (sd_type_name, sd_class)
        in
        let (stream_dispatcher_name, stream_dispatcher_instance_decl) = 
            let sd_name = stream_name^"_dispatcher" in
                (sd_name, [stream_dispatcher_type_name^" "^sd_name^";"])
        in
        let stream_source_decl =
            [source_type^" "^stream_name^"("^
                io_service_name^","^stream_dispatcher_name^");"]
        in
            stream_dispatcher_class_decl@
                stream_dispatcher_instance_decl@stream_source_decl
    in

    let init_code =
        let stream_pointer = "&"^stream_name in
            [ "sources.addStream(static_cast<"^
                "DBToaster::StandaloneEngine::SocketStream*>("^
                stream_pointer^"));" ]
    in
        (decl_code, init_code)


let generate_socket_stream_engine_init out_chan streams_handlers_and_events =
    let indent s = ("    "^s) in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in

    validate_stream_types streams_handlers_and_events Socket;

    (* declarations:
     * -- io_service
     * -- stream dispatcher class
     * -- stream dispatcher instance 
     * -- network data sources *)
    let (io_service_name, io_service_decl) =
        ("io_service", "boost::asio::io_service io_service;\n")
    in

    let (init_decls, init_body) =
        let stream_typeinfos_and_names =
            List.map
                (fun (stream_type_info,stream_name,_,_) ->
                    (stream_type_info, stream_name))
                streams_handlers_and_events
        in
        let unique_stn =
            List.fold_left
                (fun acc stn -> if List.mem stn acc then acc else acc@[stn])
                [] stream_typeinfos_and_names
        in
        let handlers_and_events_per_stream =
            List.fold_left
                (fun he_acc (sti, sn) ->
                    let handlers_and_events =
                        List.map (fun (_,_,h,e) -> (h,e))
                            (List.filter
                                (fun (sti2,sn2,_,_) -> sti = sti2 && sn = sn2)
                                streams_handlers_and_events)
                    in
                        he_acc@[((sti, sn), handlers_and_events)])
                [] unique_stn
        in
        List.fold_left
            (fun (decls_acc, init_body_acc)
                ((stream_type_info, stream_name), handlers_and_events) ->
                let (decl_code, init_code) =
                    generate_stream_engine_socket_decl_and_init
                        stream_type_info stream_name
                        handlers_and_events io_service_name
                in
                    (decls_acc@decl_code, init_body_acc@init_code))
            ([], []) handlers_and_events_per_stream
    in

    (* void init(multiplexer) { register stream;} *)
    let init_defn =
        let init_code =
            ["\n\nvoid init("^
                "DBToaster::StandaloneEngine::SocketMultiplexer& sources)";
            "{";]@
                (List.map indent init_body)@[ "}\n\n"; ]
        in
            list_code init_code
    in
        output_string out_chan (list_code ([io_service_decl]@init_decls));
        output_string out_chan init_defn


(*
 * Top-level standalone engine code generation functions
 *)

(* main() { declare multiplexer, dispatcher; loop over multiplexer, dispatching; }  *)
let generate_file_stream_testing_main query_id code_out_chan global_decls =
    let indent s = ("    "^s) in
    let list_code l = String.concat "\n" l in
    let block l = ["{"]@(List.map indent l)@["}"] in

    let main_defn =
        let loop_body =
            (if !Codegen.CPPGenOptions.simple_profiler_enabled then
                ["struct timeval tups, tupe;";
	        "gettimeofday(&tups, NULL);";]
            else [])@
            ["DBToaster::StandaloneEngine::DBToasterTuple t = sources.nextInput();";
            "router.dispatch(t);";]@
            (if !Codegen.CPPGenOptions.simple_profiler_enabled then
                ["++tuple_counter;";
                "gettimeofday(&tupe, NULL);";
                "DBToaster::Profiler::accumulate_time_span("^
                    "tups, tupe, tup_sec_span, tup_usec_span);";
                "if ( (tuple_counter % "^
                    (string_of_int !RuntimeOptions.reset_frequency)^") == 0 )";
                "{";
                (indent ("DBToaster::Profiler::reset_time_span("^
                    (string_of_int !RuntimeOptions.reset_frequency)^
                    ", tvs, tuple_counter, tup_sec_span, tup_usec_span);")); ]
            else [])@
            [indent "analyse_mem_usage();"]@
            (if !Codegen.CPPGenOptions.simple_profiler_enabled then
                [(indent "analyse_handler_usage();"); "}"]
            else [])
        in

        let run_body =
            (if !CPPGenOptions.simple_profiler_enabled then
                ["unsigned long tuple_counter = 0;";
	        "double tup_sec_span = 0.0;";
	        "double tup_usec_span = 0.0;";
                "struct timeval tvs, tve;";
                "gettimeofday(&tvs, NULL);";]
            else [])@
            ["while ( sources.streamHasInputs() ) {";]@
            (List.map indent loop_body)@
            ["}";]@
            (if !CPPGenOptions.simple_profiler_enabled then
                ["DBToaster::Profiler::reset_time_span("^
                    "(tuple_counter%"^
                    (string_of_int !RuntimeOptions.reset_frequency)^"), "^
                    "tvs, tuple_counter, tup_sec_span, tup_usec_span);";
                "analyse_handler_usage();";]
            else [])@
            ["analyse_mem_usage();" ]
        in
        let main_code =
            ["DBToaster::StandaloneEngine::FileMultiplexer sources(12345, 20);";
            "DBToaster::StandaloneEngine::FileStreamDispatcher router;"; ""; "";
            "void runMultiplexer()"; "{"]@
            (List.map indent run_body)@["}"; ""; ""]@
            ["int main(int argc, char** argv)"]@
                (block
                    (["init(sources, router);";
                    "runMultiplexer();"]))
        in
            list_code main_code
    in
        output_string code_out_chan main_defn


(* declare multiplexer; main() { start read loop; run io_service in a thread, join thread }  *)
let generate_socket_stream_testing_main query_id code_out_chan global_decls =
    let indent s = ("    "^s) in
    let list_code l = String.concat "\n" l in
    let block l = ["{"]@(List.map indent l)@["}"] in

    let main_defn =

        (* TODO: get io_service_name as an argument *)
        let io_service_name = "io_service" in

        let (common_decl, common_main) =
            generate_socket_stream_engine_common io_service_name in

        let main_code =
            common_decl@
            ["int main(int argc, char** argv)"]@
            (block (common_main@["t.join();"]))
        in
            list_code main_code
    in
        output_string code_out_chan main_defn
