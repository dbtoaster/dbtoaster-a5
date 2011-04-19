open Arg

exception BuildException of string
exception DriverException of string

module type DriverSig =
sig
    val input_file_type : string ref
    val input_files : string list ref
    val data_sources_config_file : string ref 
    val dbtoaster_arg_spec : (key * spec * doc) list
    val dbtoaster_usage : string

    val add_source : string -> unit
    val run : (string * (Algebra.var_t list)) list ->
              Algebra.var_t list ->
              Algebra.term_t list ->
              (Runtime.source_info list)
              -> unit
end

module type ModeOptionSig =
sig
    type mode
    val compile_mode : mode ref
    val get_extension : unit -> string
    val set_compile_mode : string -> unit
    val requires_thrift : unit -> bool 
end

module BasicModeOption =
struct
    type mode = [ `Code | `Query | `Test ]

    let compile_mode = ref `Test
    let get_extension () =
        match !compile_mode with
            | `Code -> ".cc"
            | `Query -> ".o"
            | `Test -> ".dbtt"

    let set_compile_mode m_str = 
        let m = match m_str with
            | "code" -> `Code
            | "query" -> `Query
            | "test" | _ -> `Test
        in
            compile_mode := m

    let requires_thrift () = false
end

module type CompileOptionsSig =
sig
    module Modes : ModeOptionSig

    val input_file_type          : string ref
    val input_files              : string list ref
    val data_sources_config_file : string ref 
    val dbtoaster_arg_spec       : (key * spec * doc) list
    val dbtoaster_usage          : string

    val add_source               : string -> unit

    val compilation_level   : int ref

    val source_output_file  : string ref
    val binary_output_file  : string ref

    val dbtoaster_build_dir : string ref
    val boost_dir           : string ref
 
    val cpp_compiler        : string ref

    val cxx_flags           : string ref
    val cxx_linker_flags    : string ref
    val cxx_linker_libs     : string ref

    val append_cxx_flags         : string -> unit
    val append_cxx_include_flags : string -> unit
    val append_cxx_linker_flags  : string -> unit
    val append_cxx_linker_libs   : string -> unit
end

module CompileOptions :
    (functor (ModeOption : ModeOptionSig) ->
        CompileOptionsSig with type Modes.mode = ModeOption.mode)
=
    functor (ModeOption : ModeOptionSig) ->
struct
    module Modes = ModeOption

    let dbtoaster_usage =
        "dbtoaster <options> <SQL files>\n"^
        "Recognized options:\n"

    let dbtoaster_build_dir = ref "_dbtbuild"

    (* Software dependencies *)
    let boost_dir = ref ""
    let set_boost_dir d = boost_dir := d

    (* Required parameters w/ defaults *)
    let input_file_type = ref "sql"
    let set_input_file_type s = input_file_type := s

    let input_files = ref []
    let add_source filename = input_files := (!input_files)@[filename]

    let source_output_file = ref ("query.cc")
    let binary_output_file = ref ("query.dbte")
    let data_sources_config_file = ref "datasources.xml"

    let set_source_output s =
        source_output_file := (Filename.chop_extension s)^".cc"

    let set_binary_output s =
        binary_output_file :=
            (Filename.chop_extension s)^(ModeOption.get_extension ())

    let set_data_sources_config s =
        data_sources_config_file := s

    let set_compile_mode mode =
        ModeOption.set_compile_mode mode;
        binary_output_file :=
            (Filename.chop_extension !binary_output_file)^
            (ModeOption.get_extension ())

    (* Optional parameters *)
    let compilation_level = ref max_int
    let set_compilation_level i = compilation_level := i

    let cpp_compiler = ref "g++"
    let set_cpp_compiler s = cpp_compiler := s

    let cxx_flags = ref ""
    let cxx_linker_flags = ref ""
    let cxx_linker_libs = ref ""

    let append_ref_delim r s delim =
        r := (if (String.length !r) = 0 then "" else (!r^delim))^s

    let append_ref r s = append_ref_delim r s " "

    let append_cxx_flags s = append_ref cxx_flags s

    let append_cxx_include_flags s = append_ref cxx_flags ("-I"^s)
    let append_cxx_linker_flags s = append_ref cxx_linker_flags ("-L"^s)
    let append_cxx_linker_libs s = append_ref cxx_linker_libs ("-l"^s)

    (* Code generation parameters *)
    let set_allocator alloc =
        Codegen.CPPGenOptions.use_pool_allocator := (alloc <> "stl")

    let set_simple_profiler state =
        Codegen.CPPGenOptions.simple_profiler_enabled := (state = "on")

    let set_result_logging state =
        Codegen.CPPGenOptions.log_results := (state = "on")

    let dbtoaster_arg_spec = [
        ("-s", Symbol ([(*"tml";*) "sql"], set_input_file_type),
            " input file type");
        ("-o", String (set_source_output), "output file prefix");
        ("-d", String (set_data_sources_config), "data sources configuration");
        ("-m", Symbol (["code"; "query"; "test";],
            set_compile_mode), " compilation mode");
        ("-r", Int (set_compilation_level), "recursive compilation level");
        ("-compiler", String (set_cpp_compiler), "C++ compiler");
        ("-cflags", String (append_cxx_flags), "C++ compiler flags");
        ("-cI", String (append_cxx_include_flags), "C++ include flags");
        ("-cL", String (append_cxx_linker_flags), "C++ linker flags");
        ("-cl", String (append_cxx_linker_libs), "C++ linker libraries");
        ("-with-boost", String (set_boost_dir), "Boost directory");
        ("-allocator", Symbol (["stl"; "boost"], set_allocator), " stl::map allocator");
        ("-simple-profiler", Symbol (["on"; "off"], set_simple_profiler), " enable simple profiling");
        ("-progress-counter", Set_int Runtime.RuntimeOptions.reset_frequency,
            "progress reporting frequency for simple profiling");
        ("-log-results", Symbol (["on"; "off"], set_result_logging), " enable result logging");
    ]

end;;

module Build =
    functor (CO : CompileOptionsSig) ->
struct

    module Find =
    struct
        let get_path_parts path =
            let split_path = Str.full_split (Str.regexp "/") path in
            let abs_path = (List.hd split_path) = (Str.Delim "/") in
            let parts = 
                List.filter
                    (function | Str.Text _ -> true | _ -> false)
                    split_path
            in
                (abs_path,
                List.map
                    (function
                        | Str.Text x -> x
                        | _ -> raise (Failure ("Invalid split result type")))
                    parts)

        let append_path path append =
            let (abs_path, path_parts) = get_path_parts path in
            let append_parts = snd (get_path_parts append) in
                (if abs_path then "/" else "")^
                    (String.concat "/" (path_parts@append_parts))

        let append_path_parts path append_parts =
            let (abs_path, path_parts) = get_path_parts path in
                (if abs_path then "/" else "")^
                    (String.concat "/" (path_parts@append_parts))

        let trim_path_suffix path suffix =
            let rec trim_aux l acc =
                if l = [] then acc
                else if l = suffix then acc
                else trim_aux (List.tl l) (acc@[List.hd l])
            in
            let (abs_path, path_parts) = get_path_parts path in
            let parts_without_suffix = trim_aux path_parts [] in
                (if abs_path then "/" else "")^
                    (String.concat "/" parts_without_suffix)

        let any_file_exists file_list path =
            print_string ("Checking path: "^path^" for: "^
                (String.concat "; " file_list)^" ... ");
            let r = List.exists
                (fun f -> Sys.file_exists (append_path path f)) file_list
            in
                print_endline (if r then "found" else "failed");
                r

        let any_file_prefix_exists prefix_list path =
            print_string ("Checking path: "^path^" for: "^
                (String.concat ";" prefix_list)^" ... ");
            let check_dir path prefix =
                let dir_contents = Array.to_list (Sys.readdir path) in
                let prefix_regexp = Str.regexp (Str.quote prefix) in
                    List.exists (fun dir_entry ->
                        Str.string_match prefix_regexp dir_entry 0)
                        dir_contents
            in
            let r =
                List.exists
                    (fun prefix ->
                        let prefix_dir = Filename.dirname prefix in
                        let test_path = append_path path prefix_dir in
                            if Sys.is_directory test_path then
                                check_dir test_path prefix
                            else
                                false)
                    prefix_list
            in
                print_endline (if r then "found" else "failed");
                r

        let find_include_and_lib_dirs
                additional_dirs include_ext include_tests lib_ext lib_tests =
            let path = Sys.getenv "PATH" in
            let bin_path_list = Str.split (Str.regexp ":") path in
            let path_list = 
                additional_dirs@
                    (List.map (fun p ->
                        trim_path_suffix p ["bin"]) bin_path_list)
            in
            let include_paths_to_test = 
                path_list@(List.flatten
                    (List.map
                        (fun ext -> 
                            (List.map (fun p -> append_path p ext) path_list))
                        include_ext))
            in
            let lib_paths_to_test =
                path_list@(List.flatten
                    (List.map
                        (fun ext ->
                            (List.map (fun p -> append_path p ext) path_list))
                        lib_ext))
            in
            let include_path =
                try List.find (any_file_exists include_tests)
                    include_paths_to_test
                with Not_found -> raise (BuildException "include dir")
            in
            let lib_path =
                try List.find (any_file_prefix_exists lib_tests)
                    lib_paths_to_test
                with Not_found -> raise (BuildException "lib dir")
            in
                (include_path, lib_path)

        let get_libraries lib_dir lib_prefixes lib_suffix =
            if Sys.is_directory lib_dir then
                let dir_contents = Array.to_list (Sys.readdir lib_dir) in
                    List.map
                        (fun prefix ->
                            print_string ("Finding library for "^prefix^": ");
                            let lib_regexp =
                                Str.regexp ((Str.quote prefix)^".*"^
                                    (Str.quote lib_suffix))
                            in
                            let lib_matches =
                                List.find_all
                                    (fun dir_entry ->
                                        Str.string_match lib_regexp dir_entry 0)
                                    dir_contents
                            in
                            let (_, best_match) =
                                List.fold_left
                                    (fun (len, res) lib ->
                                        let liblen = String.length lib in
                                            if liblen < len then
                                                (liblen, lib) 
                                            else (len,res))
                                    (max_int, "") lib_matches
                            in
                                if best_match = "" then
                                    raise (BuildException
                                        ("Failed to find library for prefix: "^
                                            prefix));
                                print_endline best_match;
                                (Str.global_replace (Str.regexp ("lib")) ""
                                    (Str.global_replace (Str.regexp
                                        (Str.quote lib_suffix)) "" best_match)))
                        lib_prefixes
            else raise (BuildException ("Invalid lib directory: "^lib_dir))

        let get_os_lib_suffix () =
            match Sys.os_type with
                | "Unix" ->
                      let uname_chan = Unix.open_process_in "uname" in
                      let uname_str =
                          try input_line uname_chan
                          with End_of_file ->
                              raise (BuildException
                                  "Could not get operating system type")
                      in
                      let os_suffixes = ["Linux", ".so"; "Darwin", ".dylib"] in
                      let uname_matches =
                          let uname_regexp = Str.regexp (Str.quote uname_str) in
                              List.filter
                                  (fun (os, _) ->
                                      Str.string_match uname_regexp os 0)
                                  os_suffixes
                      in
                          begin match uname_matches with
                              | [] -> raise (BuildException
                                    ("Unknown operating system '"^
                                        uname_str^"'"))
                              | [(x, y)] -> y
                              | _ -> raise (BuildException
                                    ("Ambiguous operating system: "^uname_str))
                          end

                | "Cygwin" -> ".so"
                | "Win32" -> raise (BuildException
                      ("DBToaster does not support Native Win32 compilation"))
                | _ -> raise (BuildException ("Invalid OS type: "^Sys.os_type))

        (* Returns boost include and lib dir *)
        let find_boost () =
            let boost_user_dir =
                if (!CO.boost_dir) = "" then []
                else begin
                    print_endline ("Adding "^(!CO.boost_dir)^
                        " to boost search list...");
                    [!CO.boost_dir] 
                end
            in
            let boost_versions = ["1_35"; "1_36"; "1_37"; "1_38"; "1_39"] in
            let boost_include_ext =
                ["include"; "include/boost"]@
                    (List.map (fun v -> "include/boost-"^v) boost_versions)
            in
            let boost_include_tests = ["boost/smart_ptr.hpp"] in
            let boost_lib_ext = ["lib"] in
            let boost_lib_tests = ["libboost_system"] in
                try 
                    find_include_and_lib_dirs boost_user_dir
                        boost_include_ext boost_include_tests
                        boost_lib_ext boost_lib_tests
                with BuildException s ->
                    raise (BuildException ("Could not find boost "^s))

        let get_boost_libraries boost_lib_dir =
            let prefixes = ["libboost_system"; "libboost_thread"; "libboost_program_options" ] in
                get_libraries boost_lib_dir prefixes (get_os_lib_suffix())

        let test_find () =
            let (boost_inc, boost_lib) = find_boost() in
            let boost_libs = get_boost_libraries boost_lib in
                print_endline ("Found boost at: "^boost_inc^", "^boost_lib);
                print_endline ("Boost libraries: "^
                    (String.concat ", " boost_libs))
    end

    (* TODO: can we use ocamlbuild API to do invocation of external tools? *)
    (* Very basic command construction, should replace with at least a Makefile,
     *  if not ocamlbuild *)

    let get_output_file f =
        let r =
            if Filename.is_relative f && (Filename.dirname f) = "." then
                Filename.concat (!CO.dbtoaster_build_dir) f
            else if Sys.file_exists (Filename.dirname f) &&
                Sys.is_directory (Filename.dirname f)
            then f
            else raise (BuildException ("Invalid output file: "^f))
        in
            print_endline ("Using output file "^
                r^" from "^f^" dir "^(Filename.dirname f));
            r

    let get_binary_output_file f =
        if (Filename.dirname f) = "" then f
        else if Sys.file_exists (Filename.dirname f) &&
            Sys.is_directory (Filename.dirname f)
        then f
        else raise (BuildException ("Invalid output file: "^f))

    let get_object_file source_file =
        (Filename.chop_extension source_file)^".o"

    let get_testing_binary_file source_file =
        (Filename.chop_extension source_file)^".dbtt"

    let build_query_cpp_compile_cmd_with_output source_file output_file =
        let output = get_output_file output_file in
            ((!CO.cpp_compiler)^" -o "^output^" -c "^(!CO.cxx_flags)^" "^
                " "^source_file, output)

    let build_query_cpp_compile_cmd source_file =
        build_query_cpp_compile_cmd_with_output source_file
            (get_object_file source_file)

    let build_cpp_linker_cmd output_file object_files =
        (!CO.cpp_compiler)^" -o "^(get_binary_output_file output_file)^" "^
            (List.fold_left
                (fun acc o ->
                    (if (String.length acc) = 0 then "" else acc^" ")^o)
                "" object_files)^" "^
            (!CO.cxx_linker_flags)^" "^(!CO.cxx_linker_libs)

    let check_status cmd =
        print_endline ("Running command: "^cmd);
        match Unix.system cmd with
            | Unix.WEXITED id when id = 0 -> ()
            | _ ->
                  print_endline ("Failed to run command "^cmd);
                  raise (BuildException ("Failed to run command "^cmd))

    (* TODO: should we move the object file to the top level project dir? *)
    let build_query_object query_source =
        let (cmd,out) = build_query_cpp_compile_cmd query_source in
            check_status cmd;
            out

    module Runtime =
    struct
        (* Testing engine compilation for file sources only *)
        let build_testing_engine query_source =
            let binary_file = get_testing_binary_file query_source in

            (* Invoke g++ -c *)
            let query_object = build_query_object query_source in

            (* Invoke g++ -o *)
            let link_cmd = build_cpp_linker_cmd binary_file [query_object] in
                check_status link_cmd
    end
end;;


module BasicRun =
    functor(CO : CompileOptionsSig with type Modes.mode = BasicModeOption.mode) ->
struct
    module C = Compiler.BytecodeCompiler
    module B = Build(CO)
    module BF = B.Find
    module R = Runtime

    (* Set up C++ software dependencies *)
    let compile_with_boost() =
        let (boost_inc_dir, boost_lib_dir) = BF.find_boost() in
        let boost_libs = BF.get_boost_libraries boost_lib_dir in
            CO.append_cxx_include_flags boost_inc_dir;
            CO.append_cxx_linker_flags boost_lib_dir;
            List.iter CO.append_cxx_linker_libs boost_libs

    (* Add any compilation flags from the code generator *)
    let setup_code_generator() =
        if !Codegen.CPPGenOptions.simple_profiler_enabled then
            CO.append_cxx_flags "-DENABLE_SIMPLE_PROFILER"

    (*
     * Object file compilation, i.e. handlers only.
     *)
    let compile_query db_schema params terms_l out_file_name =
        let (global_decls, handlers_and_events) =
            C.compile_terms db_schema "q" params !CO.compilation_level terms_l in
        let out_chan =
            if out_file_name = "" then stdout else open_out out_file_name
        in
            R.generate_includes out_chan;

            (* output declarations *) 
            output_string out_chan (global_decls^"\n\n\n");
            
            (* output handlers *)
            output_string out_chan (
                (String.concat "\n\n"
                    (List.map (fun ((_,_,h),_) -> h)
                        handlers_and_events)^"\n\n"));

            if out_chan <> stdout then close_out out_chan

    let create_stream_type_info event_rel relation_sources =
        if List.mem_assoc event_rel relation_sources then
            let (stream_type_string,
                 source_type, source_args, tuple_type,
                 adaptor_type, adaptor_bindings,
                 thrift_tuple_namespace, stream_name)
                    =
                (List.assoc event_rel relation_sources)
            in
            let stream_type =
                match stream_type_string with
                    | "file" -> R.File
                    | "socket" -> R.Socket
                    | _ -> raise (DriverException
                          ("Invalid stream type string: "^stream_type_string))
            in
                (stream_name,
                (stream_type, source_type, source_args, tuple_type,
                adaptor_type, adaptor_bindings, thrift_tuple_namespace))
        else
            raise (DriverException
                ("Could not find relation source for "^event_rel))

    (*
     * Testing engine compilation: handlers, stream multiplexer,
     * dispatcher, main
     *)
    let compile_testing_engine db_schema params terms_l
            relation_sources out_file_name
            =
        let query_base_path = Filename.chop_extension out_file_name in
        let query_id = Filename.basename query_base_path in
        let code_out_chan = open_out out_file_name in

        let (global_decls, handlers_and_events) =
            C.compile_terms db_schema "q" params !CO.compilation_level terms_l
        in
        let streams_handlers_and_events = List.map
            (fun (h, ((evt_type, evt_rel) as e)) ->
                let (stream_name, stream_type_info) =
                    create_stream_type_info evt_rel relation_sources
                in
                    (stream_type_info, stream_name, h, e))
            handlers_and_events
        in

            R.generate_includes code_out_chan;
            R.generate_stream_engine_includes code_out_chan;

            (* output declarations *) 
            output_string code_out_chan (global_decls^"\n");
            
            (* output handlers *)
            output_string code_out_chan (
                (String.concat "\n\n"
                    (List.map (fun ((_,_,h),_) -> h) handlers_and_events))^
                    "\n\n");

            let file_init_and_main =
                (R.generate_file_stream_engine_init,
                R.generate_file_stream_testing_main)
            in
            let socket_init_and_main =
                (R.generate_socket_stream_engine_init,
                R.generate_socket_stream_testing_main)
            in
            let generate_init_and_main stream_type h =
                let (gen_init_fn, gen_main_fn) =
                    match stream_type with
                        | "file" -> file_init_and_main
                        | "socket" -> socket_init_and_main
                        | _ -> raise
                              (DriverException
                                  ("Invalid stream source type: "^stream_type))
                in

                    (* init *)
                    gen_init_fn code_out_chan streams_handlers_and_events;

                    (* main *)
                    gen_main_fn query_id code_out_chan global_decls;
            in
                begin match relation_sources with
                    | [] ->
                          begin
                              print_endline
                                  "No sources found, using no-op main.";
                              R.generate_noop_main code_out_chan
                          end

                    | h::t ->
                          let (_, (stream_type, _, _, _, _, _, _, _)) = h in
                              generate_init_and_main stream_type h
                end;

                close_out code_out_chan

    let run (db_schema : (string * (Algebra.var_t list)) list)
            (params : Algebra.var_t list)
            (terms_l : Algebra.term_t list)
            (relation_sources : Runtime.source_info list) =

        let create_build_dir () =
            if not(Sys.file_exists !CO.dbtoaster_build_dir) then
                begin
                    print_endline ("Creating build directory "^
                        (!CO.dbtoaster_build_dir));
                    Unix.mkdir !CO.dbtoaster_build_dir 0o755;
                end
        in
            begin match !CO.Modes.compile_mode with
                | `Code -> compile_query db_schema params terms_l !CO.source_output_file

                | `Query ->
                      begin
                          compile_query db_schema
                              params terms_l !CO.source_output_file;
                          ignore(B.build_query_object !CO.source_output_file)
                      end

                | `Test ->
                      begin
                          create_build_dir();
                          setup_code_generator();
                          compile_with_boost();
                          compile_testing_engine
                              db_schema params terms_l relation_sources
                              !CO.source_output_file;
                          B.Runtime.build_testing_engine !CO.source_output_file
                      end
            end
end

module CO : CompileOptionsSig
    with type Modes.mode = BasicModeOption.mode
    = CompileOptions(BasicModeOption)

module B = Build(CO)
module BR = BasicRun(CO)

let input_file_type = CO.input_file_type
let input_files = CO.input_files
let data_sources_config_file = CO.data_sources_config_file
let dbtoaster_arg_spec = CO.dbtoaster_arg_spec
let dbtoaster_usage = CO.dbtoaster_usage

let add_source = CO.add_source
let run = BR.run;;
