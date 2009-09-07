open Arg
open Unix

open Xml

open Algebra
open Sqllexer
open Sqlparser

open Codegen
open Thriftgen
open Compile
open Gui

exception ThriftException of string
exception BuildException of string


(*
 * Compiler main
 *)
module CompileOptions =
struct
    type mode = Code | Query | Test | Engine | Debugger

    let dbtoaster_usage =
        "dbtoaster <options> <TML files>\n"^
        "Recognized options:\n"

    (* Software dependencies *)
    let boost_dir = ref ""
    let set_boost_dir d = boost_dir := d

    let thrift_dir = ref ""
    let set_thrift_dir d = thrift_dir := d

    (* Required parameters w/ defaults *)
    let input_file_type = ref "sql"
    let set_input_file_type s = input_file_type := s

    let input_files = ref []
    let add_tml_source filename = input_files := (!input_files)@[filename]

    let source_output_file = ref ("query.cc")
    let binary_output_file = ref ("query.dbte")
    let compile_mode = ref Test
    let data_sources_config_file = ref "datasources.xml"

    let get_extension mode =
        match mode with
            | Code -> ".cc"
            | Query -> ".o"
            | Test -> ".dbtt"
            | Engine -> ".dbte"
            | Debugger -> ".dbtd"

    let dbtoaster_build_dir = ref "_dbtbuild"

    let set_source_output s =
        source_output_file := (Filename.chop_extension s)^".cc"

    let set_binary_output s =
        binary_output_file :=
            (Filename.chop_extension s)^(get_extension !compile_mode)

    let set_compile_mode m_str = 
        let m = match m_str with
            | "code" -> Code
            | "query" -> Query
            | "debugger" -> Debugger
            | "engine" -> Engine
            | "test" | _ -> Test
        in
            compile_mode := m;
            binary_output_file :=
                (Filename.chop_extension !binary_output_file)^
                (get_extension !compile_mode)

    let requires_thrift () =
        ((!compile_mode) = Debugger || (!compile_mode) = Engine)

    let set_data_sources_config s =
        data_sources_config_file := s

    (* Optional parameters *)
    let cxx_flags = ref ""
    let cxx_linker_flags = ref ""
    let cxx_linker_libs = ref ""

    let thrift_binary = ref "thrift"
    let thrift_output_dir = ref "thrift"
    let thrift_languages = ref "-gen cpp -gen java"
    let thrift_flags = ref ""

    let thrift_cxx_flags = ref ""
    let thrift_java_classpath = ref ""

    let thrift_modules = ref []

    let append_ref_delim r s delim =
        r := (if (String.length !r) = 0 then "" else (!r^delim))^s

    let append_ref r s = append_ref_delim r s " "

    let append_cxx_flags s = append_ref cxx_flags s
    let append_cxx_include_flags s = append_ref cxx_flags ("-I"^s)
    let append_cxx_linker_flags s = append_ref cxx_linker_flags ("-L"^s)
    let append_cxx_linker_libs s = append_ref cxx_linker_libs ("-l"^s)

    let set_thrift_binary s = thrift_binary := s
    let set_thrift_language s = thrift_languages := ("-gen "^s)
    let append_thrift_languages s = append_ref thrift_languages ("-gen "^s)

    let append_thrift_flags s =
        let flag_and_val = Str.split (Str.regexp ",") s in
            if (List.length flag_and_val) = 2 then
                append_ref thrift_flags
                    (" -"^(List.hd flag_and_val)^" "^(List.hd (List.tl flag_and_val)))
            else
                print_endline ("Invalid Thrift flag argument: "^s^" ... skipping.")

    let append_thrift_cxx_flags s = append_ref thrift_cxx_flags ("-I"^s)
    let append_thrift_java_classpath s = append_ref_delim thrift_java_classpath s ":"

    let append_thrift_module s =
        let mod_name_and_path = Str.split (Str.regexp ",") s in
            if (List.length mod_name_and_path = 2) then
                begin
                    let thrift_module_base = List.hd (List.tl mod_name_and_path) in
                        thrift_modules := (!thrift_modules@
                            [(List.hd mod_name_and_path, thrift_module_base)]);
                        append_thrift_cxx_flags
                            (Filename.concat thrift_module_base "gen-cpp")
                end
            else
                print_endline ("Invalid module argument: "^s^" ... skipping.")

    let dbtoaster_arg_spec = [
        ("-s", Symbol (["tml"; "sql"], set_input_file_type), "input file type (tml|sql)");
        ("-o", String (set_source_output), "output file prefix");
        ("-d", String (set_data_sources_config), "data sources configuration");
        ("-m", Symbol (["code"; "query"; "test"; "engine"; "debugger"], set_compile_mode), "compilation mode");
        ("-cI", String (append_cxx_include_flags), "C++ include flags");
        ("-cL", String (append_cxx_linker_flags), "C++ linker flags");
        ("-cl", String (append_cxx_linker_libs), "C++ linker libraries");
        ("-thrift", String (set_thrift_binary), "Set Thrift binary");
        ("-t", String (set_thrift_language), "Set Thrift output language");
        ("-tl", String (append_thrift_languages), "Add Thrift output language");
        ("-tI", String (append_thrift_flags), "Thrift include flags");
        ("-tm", String (append_thrift_module), "Add Thrift module.");
        ("-tCI", String (append_thrift_cxx_flags), "Add include path for compiling Thrift C++ sources");
        ("-tcp", String (append_thrift_java_classpath), "Add classpath for compiling Thrift java sources");
        ("-with-boost", String (set_boost_dir), "Boost directory");
        ("-with-thrift", String (set_thrift_dir), "Thrift directory");
    ]
end;;

open CompileOptions;;

module Build =
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
            print_string ("Checking path: "^path^" for: "^(String.concat "; " file_list)^" ... ");
            let r = List.exists (fun f -> Sys.file_exists (append_path path f)) file_list in
                print_endline (if r then "found" else "failed");
                r

        let any_file_prefix_exists prefix_list path =
            print_string ("Checking path: "^path^" for: "^(String.concat ";" prefix_list)^" ... ");
            let r =
                List.exists
                    (fun prefix ->
                        let prefix_dir = Filename.dirname prefix in
                        let test_path = append_path path prefix_dir in
                            if Sys.is_directory test_path then
                                let dir_contents = Array.to_list (Sys.readdir test_path) in
                                let prefix_regexp = Str.regexp (Str.quote prefix) in
                                    List.exists
                                        (fun dir_entry -> Str.string_match prefix_regexp dir_entry 0)
                                        dir_contents
                            else
                                false)
                    prefix_list
            in
                print_endline (if r then "found" else "failed");
                r

        let find_include_and_lib_dirs additional_dirs include_ext include_tests lib_ext lib_tests =
            let path = Sys.getenv "PATH" in
            let bin_path_list = Str.split (Str.regexp ":") path in
            let path_list = 
                additional_dirs@
                    (List.map (fun p -> trim_path_suffix p ["bin"]) bin_path_list)
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
                try List.find (any_file_exists include_tests) include_paths_to_test
                with Not_found -> raise (BuildException "include dir")
            in
            let lib_path =
                try List.find (any_file_prefix_exists lib_tests) lib_paths_to_test
                with Not_found -> raise (BuildException "lib dir")
            in
                (include_path, lib_path)

        (* Returns boost include and lib dir *)
        let find_boost () =
            let boost_user_dir =
                if (!boost_dir) = "" then []
                else begin
                    print_endline ("Adding "^(!boost_dir)^" to boost search list...");
                    [!boost_dir] 
                end
            in
            let boost_versions = ["1_35"; "1_36"; "1_37"; "1_38"; "1_39"] in
            let boost_include_ext =
                ["include"]@(List.map (fun v -> "include/boost-"^v) boost_versions)
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

        (* Returns Thrift include and lib dir *)
        let find_thrift () =
            let thrift_user_dir =
                if (!thrift_dir) = "" then []
                else begin
                    print_endline ("Adding "^(!thrift_dir)^" to thrift search list...");
                    [!thrift_dir]
                end
            in
            let thrift_include_ext = ["include"; "include/thrift"] in
            let thrift_include_tests = ["Thrift.h"] in
            let thrift_lib_ext = ["lib"] in
            let thrift_lib_tests = ["libthrift"] in
                try 
                    find_include_and_lib_dirs thrift_user_dir
                        thrift_include_ext thrift_include_tests
                        thrift_lib_ext thrift_lib_tests 
                with BuildException s ->
                    raise (BuildException ("Could not find Thrift "^s))

        let get_libraries lib_dir lib_prefixes lib_suffix =
            if Sys.is_directory lib_dir then
                let dir_contents = Array.to_list (Sys.readdir lib_dir) in
                    List.map
                        (fun prefix ->
                            print_string ("Finding library for "^prefix^": ");
                            let lib_regexp =
                                Str.regexp ((Str.quote prefix)^".*"^(Str.quote lib_suffix))
                            in
                            let lib_matches =
                                List.find_all
                                    (fun dir_entry -> Str.string_match lib_regexp dir_entry 0)
                                    dir_contents
                            in
                            let (_, best_match) =
                                List.fold_left
                                    (fun (len, res) lib ->
                                        let liblen = String.length lib in
                                            if liblen < len then (liblen, lib) else (len,res))
                                    (max_int, "") lib_matches
                            in
                                if best_match = "" then
                                    raise (BuildException ("Failed to find library for prefix: "^prefix));
                                print_endline best_match;
                                (Str.global_replace (Str.regexp ("lib")) ""
                                    (Str.global_replace (Str.regexp (Str.quote lib_suffix)) "" best_match)))
                        lib_prefixes
            else raise (BuildException ("Invalid lib directory: "^lib_dir))

        let get_os_lib_suffix () =
            match Sys.os_type with
                | "Unix" ->
                      let uname_chan = Unix.open_process_in "uname" in
                      let uname_str =
                          try input_line uname_chan
                          with End_of_file ->
                              raise (BuildException "Could not get operating system type")
                      in
                      let os_suffixes = ["Linux", ".so"; "Darwin", ".dylib"] in
                      let uname_matches =
                          let uname_regexp = Str.regexp (Str.quote uname_str) in
                              List.filter
                                  (fun (os, _) -> Str.string_match uname_regexp os 0)
                                  os_suffixes
                      in
                          begin match uname_matches with
                              | [] -> raise (BuildException
                                    ("Unknown operating system '"^uname_str^"'"))
                              | [(x, y)] -> y
                              | _ -> raise DuplicateException
                          end

                | "Cygwin" -> ".so"
                | "Win32" -> raise (BuildException
                      ("DBToaster does not support Native Win32 compilation"))
                | _ -> raise (BuildException ("Invalid OS type: "^Sys.os_type))

        let get_boost_libraries boost_lib_dir =
            let prefixes = ["libboost_system"; "libboost_thread"] in
                get_libraries boost_lib_dir prefixes (get_os_lib_suffix())

        let get_thrift_libraries thrift_lib_dir =
            let prefixes = ["libthrift"] in
                get_libraries thrift_lib_dir prefixes (get_os_lib_suffix())

        let test_find () =
            let (boost_inc, boost_lib) = find_boost() in
            let boost_libs = get_boost_libraries boost_lib in
            let (thrift_inc, thrift_lib) = find_thrift() in
            let thrift_libs = get_thrift_libraries thrift_lib in
                print_endline ("Found boost at: "^boost_inc^", "^boost_lib);
                print_endline ("Boost libraries: "^(String.concat ", " boost_libs));
                print_endline ("Found thrift at: "^thrift_inc^", "^thrift_lib);
                print_endline ("Thrift libraries: "^(String.concat ", " thrift_libs))
    end

    (* TODO: can we use ocamlbuild API to do invocation of external tools? *)
    (* Very basic command construction, should replace with at least a Makefile,
     *  if not ocamlbuild *)

    let get_output_file f =
        let r =
            if Filename.is_relative f && (Filename.dirname f) = "." then
                Filename.concat (!dbtoaster_build_dir) f
            else if Sys.file_exists (Filename.dirname f) &&
                Sys.is_directory (Filename.dirname f)
            then f
            else raise (BuildException ("Invalid output file: "^f))
        in
            print_endline ("Using output file "^r^" from "^f^" dir "^(Filename.dirname f));
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
            ("g++ -o "^output^" -c "^(!cxx_flags)^" "^(!thrift_cxx_flags)^" "^source_file, output)

    let build_query_cpp_compile_cmd source_file =
        build_query_cpp_compile_cmd_with_output source_file (get_object_file source_file)

    let build_cpp_linker_cmd output_file object_files =
        "g++ -o "^(get_binary_output_file output_file)^" "^
            (List.fold_left
                (fun acc o ->
                    (if (String.length acc) = 0 then "" else acc^" ")^o)
                "" object_files)^" "^
            (!cxx_linker_flags)^" "^(!cxx_linker_libs)

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

    (* Testing engine compilation for file sources only *)
    let build_testing_engine query_source =
        let binary_file = get_testing_binary_file query_source in

        (* Invoke g++ -c *)
        let query_object = build_query_object query_source in

        (* Invoke g++ -o *)
        let link_cmd = build_cpp_linker_cmd binary_file [query_object] in
            check_status link_cmd

    module Thrift =
    struct
        let get_thrift_service_sources service_name = service_name^".cpp"

        (* Gets the java namespace specified in the Thrift module *)
        let get_java_namespace thrift_module =
            let module_lines = 
                let module_chan = open_in thrift_module in
                let rec input_lines acc =
                    try input_lines (acc@[input_line module_chan])
                    with End_of_file -> acc
                in
                    input_lines []
            in
            let java_namespace_lines =
                List.filter
                    (fun x -> Str.string_match (Str.regexp "^namespace\\ *java") x 0)
                    module_lines
            in
                match (List.length java_namespace_lines) with
                    | 0 -> ""
                    | 1 ->
                          begin
                              let namespace_str = List.hd java_namespace_lines in
                              let matched =
                                  Str.string_match
                                      (Str.regexp "namespace\\ *java\\ *\\([^\\ ]*\\)")
                                      namespace_str 0
                              in
                                  if matched then (Str.matched_group 1 namespace_str)
                                  else
                                      raise (ThriftException
                                          ("Invalid java namespace '"^namespace_str^"'"))
                          end

                    | x -> raise (ThriftException
                          ("Found "^(string_of_int x)^" java namespaces in "^thrift_module))

        (* Gets the java namespace specified in the Thrift module as a directory *)
        let get_java_namespace_dir thrift_module =
            Str.global_replace
                (Str.regexp "\\.") "/" (get_java_namespace thrift_module)

        (* Returns sources files produced for thrift modules, recurring over dependents. *)
        let rec get_thrift_sources thrift_module known_dependents =
            let module_name = Filename.chop_extension (Filename.basename thrift_module) in
            let module_lines =
                let module_chan = open_in thrift_module in
                let rec input_lines acc =
                    try input_lines (acc@[input_line module_chan])
                    with End_of_file -> acc
                in
                    input_lines []
            in
            let includes =
                let include_lines = 
                    List.filter
                        (fun x -> Str.string_match (Str.regexp "^include") x 0)
                        module_lines
                in
                let included_files =
                    List.map
                        (fun x ->
                            let matched = Str.string_match (Str.regexp "^include\\ *\"\\([^\"]*\\)\"") x 0 in
                                try
                                    if matched then Str.matched_group 1 x else ""
                                with Not_found ->
                                    begin
                                        print_endline ("No include filename found for "^x);
                                        raise (ThriftException ("No include filename found for "^x))
                                    end)
                        include_lines
                in
                    (* Skip known dependents *)
                    List.filter
                        (fun x -> not (x = "") && not(List.mem x known_dependents))
                        included_files
            in
            let services = 
                let service_lines =
                    List.filter
                        (fun x -> Str.string_match (Str.regexp "^service") x 0)
                        module_lines
                in
                let service_decls =
                    List.map
                        (fun x ->
                            let matched =
                                Str.string_match (Str.regexp "service\\ *\\([^{\\ ]*\\)\\ .*{") x 0
                            in
                                try
                                    if matched then
                                        let s = Str.matched_group 1 x in
                                            print_endline ("Found service "^s); s
                                    else ""
                                with Not_found ->
                                    begin
                                        print_endline ("No service name found for "^x);
                                        raise (ThriftException ("No service name found for "^x))
                                    end)
                        service_lines
                in
                    List.map get_thrift_service_sources
                        (List.filter (fun x -> not (x = "")) service_decls)
            in
                (List.flatten (List.map (fun i -> get_thrift_sources i known_dependents) includes))@
                    [module_name^"_constants.cpp"; module_name^"_types.cpp"; module_name^"_operators.cpp"]@
                    services

        let get_thrift_objects thrift_module known_dependents =
            let thrift_sources = get_thrift_sources thrift_module known_dependents in
                List.map (fun x -> (Filename.chop_extension x)^".o") thrift_sources

        (* absolute module source path * module base path -> sources list * objects list
         * Gets sources that need to be compiled, and existing objects for Thrift modules *)
        let get_thrift_module_sources_and_objects thrift_modules =
            (* Build Thrift compile commands for any Thrift module sources *)
            let thrift_object_file_names =
                List.flatten
                    (List.map (fun (module_name, module_base) ->
                        let interface_base = Filename.concat module_base "gen-cpp" in
                            List.map 
                                (Filename.concat interface_base)
                                (get_thrift_objects module_name []))
                        thrift_modules)
            in
            let (thrift_objects_from_source, thrift_module_object_files) =
                List.partition (fun x -> not (Sys.file_exists x)) thrift_object_file_names
            in
            let thrift_module_source_files =
                List.filter 
                    (fun (_,x) -> Sys.file_exists x)
                    (List.map 
                        (fun o -> (Filename.basename o, (Filename.chop_extension o)^".cpp"))
                        thrift_objects_from_source)
            in
                (thrift_module_source_files, thrift_module_object_files)

        (* Builds Thrift compilation command, checking and create output dir as necessary *)
        let build_thrift_cmd thrift_file_name =
            let cmd =
                (!thrift_binary)^" -o "^(!thrift_output_dir)^" "^(!thrift_flags)^" "^
                    (!thrift_languages)^" "^thrift_file_name
            in
            let out_exists = Sys.file_exists !thrift_output_dir in
                begin
                    if out_exists && not(Sys.is_directory !thrift_output_dir)
                    then
                        (print_endline ("Invalid output directory: "^(!thrift_output_dir));
                        exit 1)
                    else if not out_exists then
                        (print_endline ("Creating Thrift output directory: "^(!thrift_output_dir));
                        Unix.mkdir !thrift_output_dir 0o755)
                end;
                (cmd, !thrift_output_dir)

        let build_thrift_cpp_compile_cmd_with_output source_file output_file =
            let output = get_output_file output_file in
                ("g++ -o "^output^" -c "^(!thrift_cxx_flags)^" "^source_file, output)

        let build_thrift_cpp_compile_cmd source_file =
            build_thrift_cpp_compile_cmd_with_output source_file (get_object_file source_file)

        let build_thrift_java_compile_cmd package_dir =
            "javac -cp "^(!thrift_java_classpath)^" "^package_dir^"/*.java"

        let build_thrift_java_jar_cmd output_file package_base =
            "jar cf "^output_file^" -C "^package_base^" ."
    end

    module Runtime =
    struct
        open Thrift

        let get_engine_binary_file source_file =
            (Filename.chop_extension source_file)^".dbte"

        let get_debugger_binary_file source_file =
            (Filename.chop_extension source_file)^".dbtd"

        let get_debugger_java_client_file () = "debugger.jar"

        let get_viewer_java_client_file () = "viewer.jar"

        (* Engine compilation supporting file/network sources, and Thrift viewer *)
        let build_standalone_engine query_source thrift_modules =
            let known_thrift_modules =
                List.map (fun (x,_) -> Filename.basename x) thrift_modules in

            (* Build Thrift compile commands for any Thrift module sources *)
            let (thrift_module_source_files, thrift_module_object_files) =
                get_thrift_module_sources_and_objects thrift_modules
            in

            (* Process viewer's Thrift file *)
            let thrift_file = (Filename.chop_extension query_source)^".thrift" in
            let (thrift_cmd, thrift_output_dir) = build_thrift_cmd thrift_file in

                (* Invoke thrift before building compile cmds for thrift sources *)
                check_status thrift_cmd;
                
                (* cp operator.cpp file under /thrift/gen-cpp *) 
                check_status ("cp "^(Filename.chop_extension query_source)^"_operators.cpp"
                ^" "^(Filename.concat thrift_output_dir "gen-cpp")^"/");

                let thrift_source_dir = Filename.concat thrift_output_dir "gen-cpp" in

                (* Build Java client jar file *)
                let thrift_java_package_base_dir = Filename.concat thrift_output_dir "gen-java" in
                let thrift_java_package_dir = get_java_namespace_dir thrift_file in
                let thrift_java_source_dir =
                    Filename.concat thrift_java_package_base_dir thrift_java_package_dir
                in
                let thrift_java_compile_cmd =
                    build_thrift_java_compile_cmd thrift_java_source_dir
                in
                let thrift_jar_cmd =
                    build_thrift_java_jar_cmd (get_viewer_java_client_file())
                        thrift_java_package_base_dir
                in
                    check_status thrift_java_compile_cmd;
                    check_status thrift_jar_cmd;

                    let prepend_thrift_dir s = Filename.concat thrift_source_dir s in
                    let thrift_source_files =
                        List.filter Sys.file_exists
                            (List.map prepend_thrift_dir
                                (get_thrift_sources thrift_file known_thrift_modules))
                    in

                    (* Track all Thrift compilations, and objects for linking *)
                    let thrift_compile_cmds_and_outputs =
                        (List.map
                            (fun (target, src) -> build_thrift_cpp_compile_cmd_with_output src target)
                            thrift_module_source_files)@
                            (List.map
                                (fun x ->
                                    let target = Filename.basename (get_object_file x) in
                                        build_thrift_cpp_compile_cmd_with_output x target)
                                thrift_source_files)
                    in
                    let (thrift_compile_cmds, thrift_object_files) =
                        List.split thrift_compile_cmds_and_outputs
                    in

                        (* Add local Thrift generated source to includes *)
                        append_cxx_include_flags thrift_source_dir;

                        let thrift_object_files = thrift_module_object_files@thrift_object_files in
                        let binary_file = get_engine_binary_file query_source in

                            (* Invoke g++ -c *)
                            List.iter check_status thrift_compile_cmds;

                            let query_object = build_query_object query_source in

                            (* Invoke g++ -o *)
                            let link_cmd =
                                build_cpp_linker_cmd binary_file (thrift_object_files@[query_object])
                            in
                                check_status link_cmd


        let build_debugger query_source thrift_modules =
            let known_thrift_modules =
                List.map (fun (x,_) -> Filename.basename x) thrift_modules in
            let (thrift_module_source_files, thrift_module_object_files) =
                get_thrift_module_sources_and_objects thrift_modules
            in

            (* Process debugger's Thrift file *)
            let thrift_file = (Filename.chop_extension query_source)^".thrift" in
            let (thrift_cmd, thrift_output_dir) = build_thrift_cmd thrift_file in

                (* Invoke thrift before building compile cmds for thrift sources *)
                check_status thrift_cmd;

                let thrift_source_dir = Filename.concat thrift_output_dir "gen-cpp" in

                (* Build Java client jar file *)
                let thrift_java_package_base_dir = Filename.concat thrift_output_dir "gen-java" in
                let thrift_java_package_dir = get_java_namespace_dir thrift_file in
                let thrift_java_source_dir =
                    Filename.concat thrift_java_package_base_dir thrift_java_package_dir
                in
                let thrift_java_compile_cmd =
                    build_thrift_java_compile_cmd thrift_java_source_dir
                in
                let thrift_jar_cmd =
                    build_thrift_java_jar_cmd (get_debugger_java_client_file())
                        thrift_java_package_base_dir
                in
                    check_status thrift_java_compile_cmd;
                    check_status thrift_jar_cmd;

                    let prepend_thrift_dir s = Filename.concat thrift_source_dir s in
                    let thrift_source_files =
                        List.filter Sys.file_exists
                            (List.map prepend_thrift_dir
                                (get_thrift_sources thrift_file known_thrift_modules))
                    in
                        (* Track all Thrift compilations, and objects for linking *)
                    let thrift_compile_cmds_and_outputs =
                        (List.map
                            (fun (target, src) -> build_thrift_cpp_compile_cmd_with_output src target)
                            thrift_module_source_files)@
                            (List.map
                                (fun x ->
                                    let target = Filename.basename (get_object_file x) in
                                        build_thrift_cpp_compile_cmd_with_output x target)
                                thrift_source_files)
                    in
                    let (thrift_compile_cmds, thrift_object_files) =
                        List.split thrift_compile_cmds_and_outputs
                    in

                        (* Add local Thrift generated source to includes *)
                        append_cxx_include_flags thrift_source_dir;

                        let thrift_object_files = thrift_module_object_files@thrift_object_files in
                        let binary_file = get_debugger_binary_file query_source in

                            (* Invoke g++ -c *)
                            List.iter check_status thrift_compile_cmds;

                            let query_object = build_query_object query_source in

                            (* Invoke g++ -o *)
                            let link_cmd =
                                build_cpp_linker_cmd binary_file (thrift_object_files@[query_object])
                            in
                                check_status link_cmd
    end
end;;


(* Set up C++ software dependencies *)
let compile_with_boost() =
    let (boost_inc_dir, boost_lib_dir) = Build.Find.find_boost() in
    let boost_libs = Build.Find.get_boost_libraries boost_lib_dir in
        append_cxx_include_flags boost_inc_dir;
        append_cxx_linker_flags boost_lib_dir;
        List.iter append_cxx_linker_libs boost_libs

let compile_with_thrift() =
    let (thrift_inc_dir, thrift_lib_dir) = Build.Find.find_thrift() in
    let thrift_libs = Build.Find.get_thrift_libraries thrift_lib_dir in
        append_cxx_include_flags thrift_inc_dir;
        append_cxx_linker_flags thrift_lib_dir;
        List.iter append_cxx_linker_libs thrift_libs


(* Parses MeTML + SourceML, or SQL.
 * arguments:
 *     file_list : a list of MeTML or SQL files.
 * returns:
 *     map expression list list * source configuration list
 *     list of map expressions per file * sources in all files
 *
 * source configuration:
 *    (relation name,
 *      (stream type, source type, source constructor args, tuple type,
 *       adaptor type, adaptor bindings, thrift namespace, source instance name))
 * stream type: "file" or "network"
 * source type: C type of source
 * tuple type: C type of tuples produced by source
 * adaptor type: C type of adaptor
 * adaptor bindings: (string * string) list, association from relation field name to C tuple field name
 * thrift namespace: Thrift namespace defining Thrift equivalent of tuple type
 * source instance name: variable name of source in C code
 *
 * TODO: rethink decoupling of MeTML and SourceML to localize sources per file
 *)

let parse_input_files file_list =
    match !input_file_type with
        | "tml" ->
              let file_exprs_l =
                  try List.map parse_treeml_file file_list
                  with InvalidTreeML x ->
                      print_endline ("Parsing TreeML failed: "^x);
                      exit 1
              in
              let relation_sources =
                  try parse_data_sources_config !data_sources_config_file
                  with ConfigException s ->
                      print_endline ("Failed reading data sources config "^s);
                      exit 1
              in
                  (file_exprs_l, relation_sources)
                      
        | "sql" ->
              let parse_sql_file fn =
                  let lexbuf = Lexing.from_channel (open_in fn) in
                      init_line lexbuf;
                      Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuf
              in
              let (file_exprs_l, source_list) = 
                  List.split (List.map parse_sql_file file_list)
              in
              let relation_sources =
                  List.fold_left
                      (fun acc h ->
                          Hashtbl.fold
                              (fun n si acc -> if List.mem si acc then acc else (acc@[si]))
                              h acc)
                      [] source_list
              in
                  List.iter
                      (fun me -> print_endline
                          ("Parsed SQL query:\n"^(indented_string_of_map_expression me 0)))
                      (List.flatten file_exprs_l);
                  (file_exprs_l, relation_sources)

        | _ -> raise (BuildException ("Invalid input file type: "^(!input_file_type)))

let main () =

    (* Parse options *)
    parse dbtoaster_arg_spec add_tml_source dbtoaster_usage;

    (* Invoke compiler on TML file according to mode *)
    let (file_exprs_l, relation_sources) = match !input_files with
        | [] ->
              print_endline "No queries specified for compilation!";
              usage dbtoaster_arg_spec dbtoaster_usage;
              exit 1

        | fn -> parse_input_files fn
    in
    let trace_out_file_opt = Some((Filename.chop_extension !source_output_file)^".tc") in
    let create_build_dir () =
        if not(Sys.file_exists !dbtoaster_build_dir) then
            begin
                print_endline ("Creating build directory "^(!dbtoaster_build_dir));
                Unix.mkdir !dbtoaster_build_dir 0o755;
            end
    in
        begin
            match !compile_mode with
                | Code -> compile_query file_exprs_l !source_output_file

                | Query ->
                      begin
                          compile_query file_exprs_l !source_output_file;
                          ignore(Build.build_query_object !source_output_file)
                      end

                | Test ->
                      begin
                          create_build_dir();
                          compile_with_boost();
                          compile_testing_engine file_exprs_l relation_sources
                              !source_output_file trace_out_file_opt;
                          Build.build_testing_engine !source_output_file
                      end
                          
                | Engine ->
                      begin
                          create_build_dir();
                          (*
                          compile_with_boost();
                          compile_with_thrift();
                          *)
                          append_cxx_flags "-DENABLE_PROFILER";
                          compile_standalone_engine file_exprs_l relation_sources
                              !source_output_file trace_out_file_opt;
                          Build.Runtime.build_standalone_engine !source_output_file !thrift_modules
                      end
                          
                | Debugger ->
                      begin
                          create_build_dir();
                          (*
                          compile_with_boost();
                          compile_with_thrift();
                          *)
                          append_cxx_flags "-DENABLE_PROFILER";                          
                          compile_standalone_debugger file_exprs_l relation_sources
                              !source_output_file trace_out_file_opt;
                          Build.Runtime.build_debugger !source_output_file !thrift_modules
                      end
        end
;;

main()
