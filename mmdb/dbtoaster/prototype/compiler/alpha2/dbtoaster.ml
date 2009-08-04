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
 * <!ELEMENT relation ( source | tuple | adaptor | thrift )* >
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
              let (source_type, source_instance_name, source_constructor_args) =
                  get_source_info rel_name ch in
              let tuple_type = get_tuple_type rel_name ch in
              let (adaptor_type, adaptor_bindings) = get_adaptor_info rel_name ch in
              let thrift_ns = get_thrift_tuple_namespace rel_name ch in
                  (rel_name,
                  (source_type, source_constructor_args, tuple_type,
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

    let source_output_file = ref "query.cc"
    let binary_output_file = ref "query.dbte"
    let compile_mode = ref Engine
    let data_sources_config_file = ref "datasources.xml"

    let get_extension mode =
        match mode with
            | Handlers -> ".o"
            | Engine -> ".dbte"
            | Debugger -> ".dbtd"

    let set_source_output s =
        source_output_file := (Filename.chop_extension s)^".cc"

    let set_binary_output s =
        binary_output_file := (Filename.chop_extension s)^(get_extension !compile_mode)

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

    let thrift_output_dir = ref "thrift"
    let thrift_languages = ref "-gen cpp -gen java"
    let thrift_flags = ref ""

    let append_ref r s =
        r := (if (String.length !r) = 0 then "" else (!r^" "))^s

    let append_cxx_flags s = append_ref cxx_flags ("-I"^s)
    let append_cxx_linker_flags s = append_ref cxx_linker_flags ("-L"^s)
    let append_cxx_linker_libs s = append_ref cxx_linker_libs ("-l"^s)

    let set_thrift_language s = thrift_languages := ("-gen "^s)
    let append_thrift_languages s = append_ref thrift_languages ("-gen "^s)
    let append_thrift_flags s = append_ref thrift_flags s

    let dbtoaster_arg_spec = [
        ("-o", String (set_source_output), "output file prefix");
        ("-d", String (set_data_sources_config), "data sources configuration");
        ("-m", Symbol(["handlers"; "engine"; "debugger"], set_compile_mode), "compilation mode");
        ("-cI", String (append_cxx_flags), "C++ include flags");
        ("-cL", String (append_cxx_linker_flags), "C++ linker flags");
        ("-cl", String (append_cxx_linker_libs), "C++ linker libraries");
        ("-t", String (set_thrift_language), "Set Thrift output language");
        ("-tl", String (append_thrift_languages), "Add Thrift output language");
        ("-tI", String (append_thrift_flags), "Thrift include flags");
    ]
end;;

open CompileOptions

(* TODO: can we use ocamlbuild API to do invocation of external tools? *)
(* Very basic command construction, should replace with at least a Makefile,
 *  if not ocamlbuild *)

let get_object_file source_file =
    (Filename.chop_extension source_file)^".o"

let get_binary_file source_file =
    (Filename.chop_extension source_file)^".dbte"

let get_thrift_service_sources service_name = service_name^".cpp"

(* Returns sources files produced for thrift modules, recurring over dependents. *)
let rec get_thrift_sources thrift_module =
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
            List.filter (fun x -> not (x = "")) included_files
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
                        Str.string_match (Str.regexp "service\\ *\\([^{\\ ]*\\)\\ *{") x 0
                    in
                        try
                            if matched then Str.matched_group 1 x else ""
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
        (List.flatten (List.map get_thrift_sources includes))@
            [module_name^"_constants.cpp"; module_name^"_types.cpp"]@
            services


(* TODO: check and create thrift output dir as necessary *)
let build_thrift_cmd thrift_file_name =
    let cmd =
        "thrift -o "^(!thrift_output_dir)^" "^(!thrift_flags)^" "^
            (!thrift_languages)^" "^thrift_file_name
    in
        (cmd, !thrift_output_dir)

let build_cpp_compile_cmd source_file =
    "g++ -o "^(get_object_file source_file)^
        " -c "^(!cxx_flags)^" "^source_file

let build_cpp_linker_cmd output_file object_files =
    "g++ -o "^output_file^" "^
        (List.fold_left
            (fun acc o ->
                (if (String.length acc) = 0 then "" else acc^" ")^o)
            "" object_files)^" "^
        (!cxx_linker_flags)^" "^(!cxx_linker_libs)


let check_status cmd =
    match Unix.system cmd with
        | Unix.WEXITED id when id = 0 -> ()
        | _ ->
              print_endline ("Failed to run command "^cmd);
              raise (BuildException ("Failed to run command "^cmd))

let build_query_object query_source =
    check_status (build_cpp_compile_cmd query_source)
    
let build_standalone_engine query_source =
    let object_files = [get_object_file query_source] in
    let binary_file = get_binary_file query_source in
        build_query_object query_source;
        check_status (build_cpp_linker_cmd binary_file object_files)

let build_debugger query_source =
    let thrift_file = (Filename.chop_extension query_source)^".thrift" in
    let (thrift_cmd, thrift_output_dir) = build_thrift_cmd thrift_file in
    let prepend_thrift_dir s = Filename.concat thrift_output_dir s in
    let source_files = 
        (List.map prepend_thrift_dir (get_thrift_sources thrift_file))@
            [query_source]
    in
    let object_files = List.map get_object_file source_files in
    let binary_file = get_binary_file query_source in
    let compile_cmds = List.map build_cpp_compile_cmd source_files in
    let linker_cmd = build_cpp_linker_cmd binary_file object_files in

        (* Invoke thrift *)
        check_status thrift_cmd;

        (* Invoke g++ -c *)
        List.iter check_status compile_cmds;

        (* Invoke g++ -o *)
        check_status linker_cmd
        

let main () =

    (* Parse options *)
    parse dbtoaster_arg_spec add_tml_source dbtoaster_usage;

    (* Invoke compiler on TML file according to mode *)
    let m_expr = match !tml_sources with
        | [] ->
              begin
                  print_endline "No queries specified for compilation!";
                  usage dbtoaster_arg_spec dbtoaster_usage;
                  exit 1
              end
        | [fn] ->
              begin
                  try
                      let m_expr_l = parse_treeml_file fn in
                          if (List.length m_expr_l) > 1 then
                              begin
                                  print_endline "Unable to compile multiple queries.";
                                  exit 1
                              end
                          else
                              List.hd m_expr_l
                  with InvalidTreeML x ->
                      print_endline ("Parsing TreeML failed: "^x);
                      exit 1
              end

        | _ ->
              begin
                  print_endline "Unable to compile multiple queries.";
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
                          compile_query m_expr !source_output_file;
                          build_query_object !source_output_file
                      end
                          
                | Engine ->
                      begin
                          compile_standalone_engine m_expr relation_sources
                              !source_output_file trace_out_file_opt;
                          build_standalone_engine !source_output_file
                      end
                          
                | Debugger ->
                      begin
                          compile_standalone_debugger m_expr relation_sources
                              !source_output_file trace_out_file_opt;
                          build_debugger !source_output_file
                      end
        end
;;

main()
