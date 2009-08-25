open Xml
open Algebra
open Codegen
open Compile
open Gui
open Arg
open Unix

exception ConfigException of string
exception ThriftException of string
exception BuildException of string

(* Source config file DTD:
 * <!ELEMENT sources ( relation* )>
 * <!ELEMENT relation ( stream | source | tuple | adaptor | thrift )* >
 * <!ELEMENT stream type CDATA #REQUIRED >
 * <!ELEMENT source ( arg* ) >
 * <!ATTLIST source
 *     type CDATA #REQUIRED
 *     instance CDATA #REQUIRED >
 * <!ATTLIST arg
 *     pos CDATA #REQUIRED
 *     val CDATA #REQUIRED >
 * <!ATTLIST tuple
 *     type CDATA #REQUIRED >
 * <!ELEMENT adaptor ( binding* ) >
 * <!ATTLIST adaptor type CDATA #REQUIRED >
 * <!ATTLIST binding
 *     queryname CDATA #REQUIRED
 *     tuplename CDATA #REQUIRED >
 * <!ATTLIST thrift namespace CDATA #REQUIRED >
 *)
let get_stream_type relation ch =
    let stream_nodes = List.filter (fun c -> (tag c) = "stream") ch in
        match stream_nodes with
            | [] -> raise (ConfigException ("Missing source type for relation "^relation))
            | [x] -> attrib x "type"
            | _ -> raise (ConfigException
                  ("Multiple stream types specified for relation "^relation))

let get_source_info relation ch =
    let source_nodes = List.filter (fun c -> (tag c) = "source") ch in
        match source_nodes with
            | [] -> raise (ConfigException ("Missing source type for relation "^relation))
            | [x] ->
                  let source_type = attrib x "type" in
                  let source_instance_name = attrib x "instance" in
                  let arg_nodes = List.filter (fun c -> (tag c) = "arg") (Xml.children x) in
                  let source_args =
                      let positioned_args =
                          List.sort (fun (p,v) (p2,v2) -> compare p p2)
                              (List.map (fun a ->
                                  let val_data =
                                      let v = (attrib a "val") in
                                          match Xml.parse_string v with
                                              | PCData b -> b
                                              | _ -> raise (ConfigException ("Failed to parse arg val "^v))
                                  in
                                      (attrib a "pos", val_data)) arg_nodes)
                      in
                          List.fold_left
                              (fun acc (_,v) ->
                                  (if (String.length acc) = 0 then "" else acc^",")^v)
                              "" positioned_args
                  in
                      (source_type, source_instance_name, source_args)

            | _ -> raise (ConfigException
                  ("Multiple source types specified for relation "^relation))

let get_tuple_type relation ch =
    let tuple_nodes = List.filter (fun c -> (tag c) = "tuple") ch in
        match tuple_nodes with
            | [] -> raise (ConfigException ("Missing tuple type for relation "^relation))
            | [x] -> attrib x "type"
            | _ -> raise (ConfigException
                  ("Multiple tuple types specified for relation "^relation))

let get_adaptor_info relation ch =
    let adaptor_nodes = List.filter (fun c -> (tag c) = "adaptor") ch in
        match adaptor_nodes with
            | [] -> raise (ConfigException ("Missing adaptor type for relation "^relation))
            | [x] ->
                  let adaptor_type = attrib x "type" in
                  let binding_nodes = List.filter (fun c -> (tag c) = "binding") (Xml.children x) in
                  let adaptor_bindings =
                      List.map (fun x -> (attrib x "queryname", attrib x "tuplename")) binding_nodes
                  in
                      (adaptor_type, adaptor_bindings)
            | _ -> raise (ConfigException
                  ("Multiple adaptor types specified for relation "^relation))

let get_thrift_tuple_namespace relation ch =
    let ns_nodes = List.filter (fun c -> (tag c) = "thrift") ch in
        match ns_nodes with
            | [] -> ""
            | [x] -> attrib x "namespace"
            | _ -> raise (ConfigException
                  ("Multiple Thrift namespaces specified for relation "^relation))

let get_relation_source node =
    match (tag node) with
        | "relation" ->
              let rel_name = attrib node "name" in
              let ch = Xml.children node in
              let stream_type = get_stream_type rel_name ch in
              let (source_type, source_instance_name, source_constructor_args) =
                  get_source_info rel_name ch in
              let tuple_type = get_tuple_type rel_name ch in
              let (adaptor_type, adaptor_bindings) = get_adaptor_info rel_name ch in
              let thrift_ns = get_thrift_tuple_namespace rel_name ch in
                  (rel_name,
                  (stream_type, source_type, source_constructor_args, tuple_type,
                  adaptor_type, adaptor_bindings, thrift_ns, source_instance_name))

        | _ -> raise (ConfigException ("Invalid relation node "^(tag node)))

let get_source_configuration tree =
    let start_nodes =
        if (tag tree)  = "sources" then
            List.filter (fun x -> (tag x) = "relation") (Xml.children tree)
        else
            raise (ConfigException ("Invalid data source config file root: "^(tag tree)))
    in
        List.map get_relation_source start_nodes
        
let parse_data_sources_config fn =
    try
        let config_xml = parse_file fn in
        let relation_sources = get_source_configuration config_xml in
            relation_sources
    with
        | Error e -> raise (ConfigException (Xml.error e))

(*
 * Compiler main
 *)
module CompileOptions =
struct
    type mode = Handlers | Engine | Debugger

    let dbtoaster_usage =
        "dbtoaster <options> <TML files>\n"^
        "Recognized options:\n"

    (* Required parameters w/ defaults *)
    let tml_sources = ref []
    let add_tml_source filename = tml_sources := (!tml_sources)@[filename]

    let source_output_file = ref ("query.cc")
    let binary_output_file = ref ("query.dbte")
    let compile_mode = ref Engine
    let data_sources_config_file = ref "datasources.xml"

    let get_extension mode =
        match mode with
            | Handlers -> ".o"
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
            | "handlers" -> Handlers
            | "debugger" -> Debugger
            | "engine" | _ -> Engine
        in
            compile_mode := m;
            binary_output_file :=
                (Filename.chop_extension !binary_output_file)^
                (get_extension !compile_mode)

    let requires_thrift () = (!compile_mode) = Debugger

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

    let append_cxx_flags s = append_ref cxx_flags ("-I"^s)
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
        ("-o", String (set_source_output), "output file prefix");
        ("-d", String (set_data_sources_config), "data sources configuration");
        ("-m", Symbol(["handlers"; "engine"; "debugger"], set_compile_mode), "compilation mode");
        ("-cI", String (append_cxx_flags), "C++ include flags");
        ("-cL", String (append_cxx_linker_flags), "C++ linker flags");
        ("-cl", String (append_cxx_linker_libs), "C++ linker libraries");
        ("-thrift", String (set_thrift_binary), "Set Thrift binary");
        ("-t", String (set_thrift_language), "Set Thrift output language");
        ("-tl", String (append_thrift_languages), "Add Thrift output language");
        ("-tI", String (append_thrift_flags), "Thrift include flags");
        ("-tm", String (append_thrift_module), "Add Thrift module.");
        ("-tCI", String (append_thrift_cxx_flags), "Add include path for compiling Thrift C++ sources");
        ("-tcp", String (append_thrift_java_classpath), "Add classpath for compiling Thrift java sources");
    ]
end;;

open CompileOptions

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

let get_binary_file source_file =
    (Filename.chop_extension source_file)^".dbte"

let get_debugger_binary_file source_file =
    (Filename.chop_extension source_file)^".dbtd"

let get_debugger_java_client_file () = "debugger.jar"

let get_viewer_java_client_file () = "viewer.jar"

let get_thrift_service_sources service_name = service_name^".cpp"

(* Gets the java namespace specified in the Thrift module *)
let get_java_namespace thrift_module =
    let module_lines = Std.input_list (open_in thrift_module) in
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
    let module_lines = Std.input_list (open_in thrift_module) in
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

let build_thrift_java_compile_cmd package_dir =
    "javac -cp "^(!thrift_java_classpath)^" "^package_dir^"/*.java"

let build_thrift_java_jar_cmd output_file package_base =
    "jar cf "^output_file^" -C "^package_base^" ."

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
    append_cxx_flags thrift_source_dir;

    let thrift_object_files = thrift_module_object_files@thrift_object_files in
    let binary_file = get_binary_file query_source in

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
    append_cxx_flags thrift_source_dir;

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
        

let main () =

    (* Parse options *)
    parse dbtoaster_arg_spec add_tml_source dbtoaster_usage;

    (* Invoke compiler on TML file according to mode *)
    let m_expr_l = match !tml_sources with
        | [] ->
              begin
                  print_endline "No queries specified for compilation!";
                  usage dbtoaster_arg_spec dbtoaster_usage;
                  exit 1
              end
        | fn ->
              begin
                  try
                      List.flatten (List.map parse_treeml_file fn)
                  with InvalidTreeML x ->
                      print_endline ("Parsing TreeML failed: "^x);
                      exit 1
              end

    in
    let trace_out_file_opt = Some((Filename.chop_extension !source_output_file)^".tc") in
    let relation_sources =
        try parse_data_sources_config !data_sources_config_file
        with ConfigException s ->
            print_endline ("Failed reading data sources config "^s);
            exit 1
    in
        begin
            match !compile_mode with
                | Handlers ->
                      begin
                          compile_query m_expr_l !source_output_file;
                          ignore(build_query_object !source_output_file)
                      end
                          
                | Engine ->
                      begin
                          if not(Sys.file_exists !dbtoaster_build_dir) then
                              begin
                                  print_endline ("Creating build directory "^(!dbtoaster_build_dir));
                                  Unix.mkdir !dbtoaster_build_dir 0o755;
                              end;
                          compile_standalone_engine m_expr_l relation_sources
                              !source_output_file trace_out_file_opt;
                          build_standalone_engine !source_output_file !thrift_modules
                      end
                          
                | Debugger ->
                      begin
                          if not(Sys.file_exists !dbtoaster_build_dir) then
                              begin
                                  print_endline ("Creating build directory "^(!dbtoaster_build_dir));
                                  Unix.mkdir !dbtoaster_build_dir 0o755;
                              end;
                          compile_standalone_debugger m_expr_l relation_sources
                              !source_output_file trace_out_file_opt;
                          build_debugger !source_output_file !thrift_modules
                      end
        end
;;

main()
