open Algebra
open Thriftgen
open Runtime

(*******************************
 *
 * Standalone stepper/debugger
 *
 *******************************)

(*
 * Debugger code generation, common to file and network sources
 *)

(* Assumes each tuple type has a matching thrift definition named:
   Thrift<tuple type>, e.g. ThriftOrderbookTuple *)
(* TODO: define and use structs as complex map keys rather than tuples,
   or convert tuples to structs here in Thrift *)

let generate_stream_debugger_includes out_chan =
    let list_code l = String.concat "\n" l in
    let includes =
        ["#include \"Debugger.h\"";]
    in
        generate_thrift_includes out_chan;
        output_string out_chan (list_code includes)


let generate_debugger_thrift_server_main stream_debugger_class stream_debugger_constructor_args =
    [ "int port = 20000;";
    "boost::shared_ptr<"^stream_debugger_class^"> handler(new "^stream_debugger_class^"("^stream_debugger_constructor_args^"));";
    "boost::shared_ptr<TProcessor> processor(new DebuggerProcessor(handler));";
    "boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));";
    "boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());";
    "boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());";
    "TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);";
    "server.serve();" ]


(*
 * Debugger handler generation
 *)

(* Generate Thrift protocol spec file
   -- void step(tuple)
   -- void stepn(int n)
   -- state/map accessors
*)
let generate_file_stream_debugger_thrift_declarations streams =
    let strip_namespace type_name =
        let ns_and_type = Str.split (Str.regexp "::") type_name in
            List.nth ns_and_type (List.length ns_and_type - 1)
    in
    let indent s = "    "^s in
    let list_code l = String.concat "\n" l in
    let (tuple_decls, steps_decls, stepns_decls) =
        List.fold_left
            (fun (td_acc, step_acc, stepn_acc)
                (stream_name,
                    (_, source_type, _, tuple_type,
                    adaptor_type, adaptor_bindings,
                    thrift_tuple_namespace))
                ->
                let (thrift_tuple_decl, thrift_tuple_type) =
                    let normalized_tuple_type = (strip_namespace tuple_type) in
                    let new_type = "Thrift"^normalized_tuple_type in
                        if (List.mem_assoc new_type td_acc) then
                            ([], new_type)
                        else
                            let tt_prefix =
                                if (String.length thrift_tuple_namespace) > 0
                                then (thrift_tuple_namespace^".") else ""
                            in
                            let new_decl = list_code
                                ([ "struct "^new_type^" {"]@
                                    (List.map indent
                                        (["1: DmlType type,";
                                        "2: DBToasterStreamId id,";
                                        "3: "^tt_prefix^normalized_tuple_type^" data"]))@
                                    [ "}\n"])
                            in
                                ([(new_type, new_decl)], new_type)
                in
                let step_stream = "void step_"^(stream_name)^"(1:"^thrift_tuple_type^" input)," in
                let stepn_stream = "void stepn_"^(stream_name)^"(1:i32 n)," in
                    (td_acc@thrift_tuple_decl, step_acc@[step_stream], stepn_acc@[stepn_stream]))
        ([], [], []) streams
    in
        (tuple_decls, steps_decls, stepns_decls)

(* Generate Thrift debugger service implementation
   -- instantiate + initialize multiplexer, dispatcher
   -- void step(tuple) { dispatch(tuple); }
   -- void stepn(int n) { for i=0:n-1 { dispatch(multiplexer->nextInput()); } }
   -- state/map accessors
   -- Copy Thrift service main from skeleton
*)
let generate_file_stream_debugger_thrift_implementations streams =
    let strip_namespace type_name =
        let ns_and_type = Str.split (Str.regexp "::") type_name in
            List.nth ns_and_type (List.length ns_and_type - 1)
    in
    let indent s = "    "^s in
    let block l = ["{"]@(List.map indent l)@["}"] in
    let (steps_defns, stepns_defns) = 
        List.fold_left
            (fun (step_acc, stepn_acc)
                (stream_name,
                    (_, source_type, _, tuple_type,
                    adaptor_type, adaptor_bindings, _))
                ->
                let normalized_tuple_type = (strip_namespace tuple_type) in
                let thrift_tuple_type = "Thrift"^normalized_tuple_type in
                 (* Create a DBToasterTuple from argument, and invoke dispatch *)
                let step_stream_body = 
                    ["void step_"^stream_name^"(const "^thrift_tuple_type^"& input)"]@
                        (block
                            (["DBToaster::StandaloneEngine::DBToasterTuple dbtInput;";
                            "dbtInput.id = input.id;";
                            "dbtInput.type = static_cast<DBToaster::StandaloneEngine::DmlType>(input.type);";
                            "dbtInput.data = boost::any(input.data);";
                            "router.dispatch(dbtInput);" ]))
                in
                (* Read n tuples from the stream *)
                let stepn_stream_body =
                    ["void stepn_"^stream_name^"(const int32_t n)"]@
                        (block
                            (["for (int32_t i = 0; i < n; ++i)"]@
                                (block 
                                    (["if ( !sources.streamHasInputs() ) break;";
                                    "DBToaster::StandaloneEngine::DBToasterTuple t "^
                                        "= sources.nextInput();";
                                    "router.dispatch(t);"]))))
                in
                    (step_acc@step_stream_body, stepn_acc@stepn_stream_body))
            ([], []) streams
    in
        (steps_defns, stepns_defns)


let generate_file_stream_debugger_class
        decl_out_chan impl_out_chan less_out_chan
        query_id global_decls streams_handlers_and_events
    =
    (* Helpers *)
    let indent s = ("    "^s) in

    (* Generate stream engine preamble: stream definitions and dispatcher function objects,
     * multiplexer, dispatcher 
     * Note: this initializes stream dispatching identifiers which are need for the
     * Thrift protocol spec file, hence the invocation prior to protocol generation. *)
    generate_file_stream_engine_init impl_out_chan streams_handlers_and_events;

    let streams = List.fold_left
        (fun acc (stream_type_info, stream_name, handler, event) ->
            if (List.mem_assoc stream_name acc) then acc
            else acc@[(stream_name, stream_type_info)])
        [] streams_handlers_and_events
    in

    let stream_id_decls = 
        Hashtbl.fold
            (fun stream_id_name stream_id acc ->
                acc@["const i32 "^stream_id_name^" = "^(string_of_int stream_id)])
            stream_ids []
    in

    (* Thrift debugger declarations for file sources *)
    let (tuple_decls, steps_decls, stepns_decls) =
        generate_file_stream_debugger_thrift_declarations streams
    in

    let (struct_decls, key_decls, accessors_decls, less_opers) =
        generate_thrift_accessor_declarations global_decls
    in

    let service_namespace =
        "DBToaster.Debugger"^(if query_id = "" then "" else ("."^query_id))
    in

    (* Thrift debugger implementation for file sources *)
    let class_name = "DebuggerHandler" in
    let (steps_defns, stepns_defns) =
        generate_file_stream_debugger_thrift_implementations streams
    in
    let accessor_defns =
        generate_thrift_accessor_implementations global_decls class_name
    in

    let thrift_service_name = "Debugger" in
    let thrift_service_handler_name = class_name in
    let thrift_service_handler_interface = "DebuggerIf" in

    let thrift_includes =  ["datasets.thrift"; "profiler.thrift"] in
    let thrift_languages = [ "cpp"; "java" ] in
    let thrift_global_decls =
        stream_id_decls@
        (snd (List.split tuple_decls))@
        struct_decls@
        (snd (List.split key_decls))
    in
    let debugger_decls = 
        List.map indent (steps_decls@stepns_decls@accessors_decls)
    in

    (* Local datastructures and constructor *)
    let debugger_constructor_args =
        ["DBToaster::StandaloneEngine::FileMultiplexer& s,";
        "DBToaster::StandaloneEngine::FileStreamDispatcher& r)"]
    in
    let debugger_constructor_member_init = ["sources(s)"; "router(r)"] in
    let debugger_constructor_init = [ "PROFILER_INITIALIZATION" ] in

    let debugger_internals =
        List.map indent
            (["DBToaster::StandaloneEngine::FileMultiplexer& sources;";
            "DBToaster::StandaloneEngine::FileStreamDispatcher& router;\n"])
    in

    (* Method bodies *)
    let debugger_interface_impls =
        (List.map indent
            (steps_defns@["\n"]@
            stepns_defns@["\n"]@
            accessor_defns@
            ["PROFILER_SERVICE_METHOD_IMPLEMENTATION"]))
    in

        generate_dbtoaster_thrift_module decl_out_chan
            thrift_includes service_namespace thrift_languages
            thrift_service_name ["profiler.Profiler"]
            thrift_global_decls debugger_decls;

        generate_dbtoaster_thrift_module_implementation impl_out_chan
            service_namespace thrift_service_handler_name thrift_service_handler_interface
            debugger_constructor_args debugger_constructor_member_init debugger_constructor_init
            debugger_internals debugger_interface_impls;

        generate_dbtoaster_thrift_less_operators less_out_chan
            less_opers query_id "Debugger";

        class_name


let generate_file_stream_debugger_main impl_out_chan stream_debugger_class =
    let indent s = "    "^s in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let block l = ["{"]@(List.map indent l)@["}"] in
    let main_defn =
        ["int main(int argc, char **argv) "]@
            (block 
                (["DBToaster::StandaloneEngine::FileMultiplexer sources(12345, 20);";
                "DBToaster::StandaloneEngine::FileStreamDispatcher router;";
                "init(sources, router);\n"]@
                (generate_debugger_thrift_server_main stream_debugger_class "sources, router")@
                ["return 0;";]))
    in
        output_string impl_out_chan (list_code main_defn)


(*
 * Network debugger 
 *)

let generate_socket_stream_debugger_thrift_declarations streams =
    let strip_namespace type_name =
        let ns_and_type = Str.split (Str.regexp "::") type_name in
            List.nth ns_and_type (List.length ns_and_type - 1)
    in
    let (steps_decls, stepns_decls) =
        List.fold_left
            (fun (step_acc, stepn_acc)
                (stream_name, (_, _, _, tuple_type, _, _, thrift_tuple_namespace))
                ->
                let normalized_tuple_type =
                    (if String.length thrift_tuple_namespace = 0 then ""
                    else (thrift_tuple_namespace^"."))^
                        (strip_namespace tuple_type)
                in
                let step_stream = "void step_"^(stream_name)^"(1:"^normalized_tuple_type^" input)," in
                let stepn_stream = "void stepn_"^(stream_name)^"(1:i32 n)," in
                    (step_acc@[step_stream], stepn_acc@[stepn_stream]))
        ([], []) streams
    in
        (steps_decls, stepns_decls)


let generate_socket_stream_debugger_thrift_implementations class_name streams =
    let strip_namespace type_name =
        let ns_and_type = Str.split (Str.regexp "::") type_name in
            List.nth ns_and_type (List.length ns_and_type - 1)
    in
    let indent s = "    "^s in
    let block l = ["{"]@(List.map indent l)@["}"] in
    let (steps_defns, stepns_defns) = 
        List.fold_left
            (fun (step_acc, stepn_acc)
                (stream_name,
                    (_, source_type, _, tuple_type,
                    adaptor_type, adaptor_bindings, _))
                ->
                (* HACK for now.
                 * TODO: pass in stream dispatchers as an argument *)
                let stream_dispatcher = stream_name^"_dispatcher" in

                let normalized_tuple_type = (strip_namespace tuple_type) in
                let step_stream_body = 
                    (* Create a DBToasterTuple from argument, and invoke dispatch *)
                    ["void step_"^stream_name^"(const DBToaster::DemoDatasets::Protocol::"^normalized_tuple_type^"& input)"]@
                        (block
                            (["boost::any anyInput(input);"; stream_dispatcher^"(anyInput);"]))
                in
                let stepn_stream_body =
                    (* Read n tuples from the stream *)
                    ["void stepn_"^stream_name^"(const int32_t n)"]@
                        (block (
                            ["sources.setNumberReads(n);";
                             "sources.read(boost::bind(&"^class_name^"::recursive_stepn, this));"]))
                in
                    (step_acc@step_stream_body, stepn_acc@stepn_stream_body))
            ([], []) streams
    in
        (steps_defns, stepns_defns)


let generate_socket_stream_debugger_class
        decl_out_chan impl_out_chan less_out_chan
        query_id global_decls streams_handlers_and_events
    =
    let indent s = "    "^s in

    (* Generate stream engine preamble: stream definitions and dispatcher function objects,
     * multiplexer, dispatcher 
     * Note: this initializes stream dispatching identifiers which are need for the
     * Thrift protocol spec file, hence the invocation prior to protocol generation. *)
    generate_socket_stream_engine_init impl_out_chan streams_handlers_and_events;

    let streams = List.fold_left
        (fun acc (stream_type_info, stream_name, handler, event) ->
            if (List.mem_assoc stream_name acc) then acc
            else acc@[(stream_name, stream_type_info)])
        [] streams_handlers_and_events
    in

    (* Thrift debugger declarations for network sources *)
    let (steps_decls, stepns_decls) =
        generate_socket_stream_debugger_thrift_declarations streams
    in

    let (struct_decls, key_decls, accessors_decls, less_opers) =
        generate_thrift_accessor_declarations global_decls
    in

    let service_namespace =
        "DBToaster.Debugger"^(if query_id = "" then "" else ("."^query_id))
    in

    (* Thrift debugger implementation for network sources *)
    let class_name = "DebuggerHandler" in
    let (steps_defns, stepns_defns) =
        generate_socket_stream_debugger_thrift_implementations class_name streams
    in
    let accessor_defns =
        generate_thrift_accessor_implementations global_decls class_name
    in
    let thrift_service_name = "Debugger" in
    let thrift_service_handler_name = class_name in
    let thrift_service_handler_interface = "DebuggerIf" in

    let thrift_includes =  ["datasets.thrift"; "profiler.thrift"] in
    let thrift_languages = [ "cpp"; "java" ] in
    let thrift_global_decls = struct_decls@(snd (List.split key_decls)) in
    let debugger_decls = 
        List.map indent (steps_decls@stepns_decls@accessors_decls)
    in

    (* Local datastructures and constructor *)
    let debugger_constructor_args =
        ["DBToaster::StandaloneEngine::SocketMultiplexer& s"]
    in
    let debugger_constructor_member_init = ["sources(s)"] in
    let debugger_constructor_init = [ "PROFILER_INITIALIZATION" ] in

    let debugger_internals =
        List.map indent
            (["DBToaster::StandaloneEngine::SocketMultiplexer& sources;\n";])
    in

    (* Method bodies *)
    let recursive_stepn_method = ["\nvoid recursive_stepn() {}\n"] in
    let debugger_interface_impls =
        (List.map indent
            (steps_defns@["\n"]@
            stepns_defns@["\n"]@
            accessor_defns@
            recursive_stepn_method@
            ["PROFILER_SERVICE_METHOD_IMPLEMENTATION"]))
    in

        generate_dbtoaster_thrift_module decl_out_chan
            thrift_includes service_namespace thrift_languages
            thrift_service_name ["profiler.Profiler"]
            thrift_global_decls debugger_decls;

        generate_dbtoaster_thrift_module_implementation impl_out_chan
            service_namespace thrift_service_handler_name thrift_service_handler_interface
            debugger_constructor_args debugger_constructor_member_init debugger_constructor_init
            debugger_internals debugger_interface_impls;
 
        generate_dbtoaster_thrift_less_operators less_out_chan
            less_opers query_id "Debugger";

        class_name


let generate_socket_stream_debugger_main impl_out_chan stream_debugger_class =
    let indent s = "    "^s in
    let list_code l = String.concat "\n" l in
    let block l = ["{"]@(List.map indent l)@["}"] in

    let main_defn =

        (* TODO: get io_service_name as an argument *)
        let io_service_name = "io_service" in

        let (common_decl, common_main) =
            generate_socket_stream_engine_common io_service_name
        in

        common_decl@
        ["int main(int argc, char **argv) "]@
            (block 
                (common_main@
                (generate_debugger_thrift_server_main stream_debugger_class "sources")@
                ["return 0;";]))
    in
        output_string impl_out_chan (list_code main_defn)
