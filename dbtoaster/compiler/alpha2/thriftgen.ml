open Algebra
open Codegen

(*
 * Thrift code generation helpers
 *)

(* Note these two functions are asymmetric *)
let thrift_type_of_base_type t =
    match t with
        | "int" -> "i32"
        | "float" -> "double"
        | "double" -> "double"
        | "long" -> "i64"
        | "string" -> "string"
        | _ -> raise (CodegenException ("Unsupported base type "^t))

(* TODO: extend to support map, list, set *)
let ctype_of_thrift_type t =
    match t with
        | "i32" -> "int32_t"
        | "double" -> "double"
        | "i64" -> "long long"
        | "string" -> "string"
        | "bool" -> "bool"
        | "byte" | "i16" | "binary"
        | _ ->
              raise (CodegenException ("Unsupported Thrift type: "^t))

let thrift_type_of_datastructure d =
    match d with
        | `Variable(n,typ) -> thrift_type_of_base_type typ

        | `Tuple (n,f) -> n^"_tuple"

	| `Map (n,f,r) ->
	      let key_type = 
                  match f with
                      | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                      | [(x,y)] -> thrift_type_of_base_type y
                      | _ -> n^"_key"
	      in
		  "map<"^key_type^","^(thrift_type_of_base_type (ctype_of_type_identifier r))^">"

        | (`Set(n, f) as ds) 
	| (`Multiset (n,f) as ds) ->
	      let el_type = 
		  if (List.length f) = 1
                  then thrift_type_of_base_type (let (_,ty) = List.hd f in ty)
                  else n^"_elem"
	      in
              let ds_type =
                  match ds with | `Set _ -> "set" | `Multiset _ -> "list"
              in
		  ds_type^"<"^el_type^">"


let ctype_of_thrift_datastructure d =
    match d with
        | `Variable(n,typ) -> thrift_type_of_base_type typ

        | `Tuple(n,f) -> n^"_tuple"

	| `Map (n,f,r) ->
	      let key_type = 
                  match f with
                      | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                      | [(x,y)] -> ctype_of_thrift_type (thrift_type_of_base_type y)
                      | _ -> n^"_key"
	      in
              let rt_thrift_type =
                  thrift_type_of_base_type (ctype_of_type_identifier r)
              in
		  "map<"^key_type^","^(ctype_of_thrift_type rt_thrift_type)^">"

        | (`Set(n, f) as ds) 
	| (`Multiset (n,f) as ds) ->
	      let el_type = 
		  if (List.length f) = 1
                  then ctype_of_thrift_type (thrift_type_of_base_type (let (_,ty) = List.hd f in ty))
                  else n^"_elem"
	      in
              let ds_type =
                  match ds with | `Set _ -> "set" | `Multiset _ -> "vector"
              in
		  ds_type^"<"^el_type^">"

let thrift_element_type_of_datastructure d =
    match d with
        | `Variable(n,_) 
        | `Tuple (n,_)
            -> raise (CodegenException
                ("Invalid datastructure for elements: "^n))

	| `Map (n,f,r) ->
	      let key_type = 
                  match f with
                      | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                      | [(x,y)] -> thrift_type_of_base_type y
                      | _ -> n^"_key"
	      in
		  "pair<"^key_type^","^(ctype_of_type_identifier r)^">"

        | `Set(n, f)
	| `Multiset (n,f) ->
	      let el_type = 
		  if (List.length f) = 1
                  then thrift_type_of_base_type (let (_,ty) = List.hd f in ty)
                  else n^"_elem"
	      in
                  el_type


let thrift_inner_declarations_of_datastructure d =
    let indent s = ("    "^s) in
    let field_declarations f =
        let (_, r) =
            List.fold_left (fun (counter, acc) (id,ty) ->
                let field_decl =
                    ((string_of_int counter)^": "^
                        (thrift_type_of_base_type (ctype_of_type_identifier ty))^
                        " "^id^",")
                in
                    (counter+1, acc@[field_decl]))
                (1, []) f
        in
            r
    in
        match d with
            | `Variable(n,_)
            | `Tuple(n,_)
                -> raise (CodegenException
                    ("Datastructure has no inner declarations: "^n))

	    | (`Map (n,f,_) as ds)
            | (`Set(n, f) as ds)
	    | (`Multiset (n,f) as ds) ->
                  if (List.length f) = 1 then
                      ("", [], "")
                  else
	              let field_decls = field_declarations f in
                      let decl_name = match ds with
                          | `Map _ -> n^"_key" | `Set _ | `Multiset _ -> n^"_elem"
                      in
                      let struct_decl =
                          ["struct "^decl_name^" {"]@(List.map indent field_decls)@["}\n"]
                      in
                      let less_oper = 
                          let rec be fields = 
                              (match fields with
                                  | [(x, y)] -> ( x ^ " < r."^x)
                                  | (hd,y)::tl -> "(("^hd^" < r."^hd^") || ("^hd^" == r."^hd^" && \n"^
                                        "                "^(be tl)^"))"
                                  | _ -> raise InvalidExpression)
                          in
                          "    bool "^decl_name^"::operator<(const "^decl_name^"& r) const\n"^
                          "    {\n"^
                          "        return"^(be f)^";\n"^
                          "    }\n"
                      in
(*                          List.iter (fun x -> print_endline x) struct_decl;
                          print_endline lessoperator; *)
		          (decl_name, struct_decl, less_oper) 


let copy_element_for_thrift d dest src =
    let copy_tuple f dest src = 
        List.fold_left (fun (counter,acc) (id, ty) ->
            (counter+1, acc@[dest^"."^id^" = get<"^(string_of_int counter)^">("^src^");"]))
            (0, []) f
    in
        match d with
            | `Variable(n,_)
            | `Tuple(n,_)
                -> raise (CodegenException "Invalid element copy.")

	    | `Map (n,f,r) ->
                  (* Copy from pair< tuple<>, <ret type> > -> pair< struct, <ret type> > *)
                  let (key_dest_decl, key_dest) = ([n^"_key key;"], "key") in
                  let (_, copy_tuple_defn) = copy_tuple f key_dest (src^".first") in
                  let ret_src = src^".second" in
                      key_dest_decl@
                      copy_tuple_defn@
                      [dest^" = make_pair("^key_dest^","^ret_src^");"]

            | `Set(n, f)
	    | `Multiset (n,f) ->
                  (* Copy from tuple<> -> struct *)
                  let (_,r) = copy_tuple f dest src in r


let thrift_inserter_of_datastructure d =
    let indent s = ("    "^s) in
        match d with
            | `Tuple (n,_) -> raise (CodegenException ("Cannot insert into tuple "^n))
	    | `Map (n,f,_)
            | `Set(n, f)
	    | `Multiset (n,f) ->
                  let inserter_body =
                      if (List.length f) = 1 then
                          [(indent "dest.insert(dest.begin(), src);")]
                      else
                          let elem_type = thrift_element_type_of_datastructure d in
                          let copy_fields dest_var src_var =
                              copy_element_for_thrift d dest_var src_var
                          in
                              (List.map indent
                                  ([elem_type^" r;"]@
                                      (copy_fields "r" "src")@
                                      ["dest.insert(dest.begin(), r);"]))
                  in
                  let inserter_name = "insert_thrift_"^n in
                  let inserter_defn =
                      let ttype = ctype_of_thrift_datastructure d in
                      let elem_ctype = element_ctype_of_datastructure d in
                          [("inline void "^inserter_name^"("^
                              ttype^"& dest, "^elem_ctype^"& src)"); "{" ]@
                              inserter_body@
                              ["}\n"]
                  in
                      (inserter_defn, inserter_name)


(*
 * Thrift common code
 *)
let generate_thrift_includes out_chan =
    let list_code l = String.concat "\n" l in
    let includes =
        ["// Thrift includes";
        "#include <protocol/TBinaryProtocol.h>";
        "#include <server/TSimpleServer.h>";
        "#include <transport/TServerSocket.h>";
        "#include <transport/TBufferTransports.h>\n";
        "using namespace apache::thrift;";
        "using namespace apache::thrift::protocol;";
        "using namespace apache::thrift::transport;";
        "using namespace apache::thrift::server;\n";
        "using namespace boost;";
        "using boost::shared_ptr;\n\n"]
    in
        output_string out_chan (list_code includes)

let generate_dbtoaster_thrift_module
        out_chan
        module_includes module_namespace module_languages
        service_name service_inheritance global_decls service_decls
  =
    let list_code l = String.concat "\n" l in
    let includes = List.map (fun i -> "include \""^i^"\"\n") module_includes in
    let namespaces =
        List.map (fun l -> "namespace "^l^" "^module_namespace) module_languages
    in
    let service =
        ["service "^service_name^
            (if List.length service_inheritance = 0 then ""
            else " extends "^(String.concat "," service_inheritance))^"{"]@
        service_decls@["}"]
    in
    let thrift_module =
        includes@["\n"]@namespaces@
        ["\n";
        "typedef i32 DBToasterStreamId";
        "enum DmlType { insertTuple = 0, deleteTuple = 1 }\n"]@
        global_decls@["\n"]@
        service
    in
        output_string out_chan (list_code thrift_module)

let generate_dbtoaster_thrift_module_implementation
        out_chan module_namespace class_name class_interface
        constructor_args constructor_member_init constructor_init
        internals interface_impls
  =
    let indent s = "    "^s in
    let rec indent_n n s = match n with 0 -> s | _ -> indent_n (n-1) (indent s) in
    let list_code l = String.concat "\n" l in
    let cpp_module_namespace =
        Str.global_replace (Str.regexp "\\.") "::" module_namespace
    in
    let constructor =
        let (constructor_name_and_parens, constructor_arg_lines) = 
            if List.length constructor_args = 0 then
                (class_name^"()", [])
            else
                (class_name^"(", (List.map (indent_n 2) constructor_args)@[")"])
        in
        let mem_init =
            if List.length constructor_member_init = 0 then []
            else 
                [(indent (": "^(String.concat "," constructor_member_init)))]
        in
            List.map indent
                (["public:";
                constructor_name_and_parens]@
                constructor_arg_lines@mem_init@
                ["{"]@
                (List.map indent constructor_init)@
                ["}"])
    in
    let module_impl =
        ["using namespace "^cpp_module_namespace^";\n";
        ("class "^class_name^" : virtual public "^class_interface); "{"]@
            internals@
            constructor@
            (List.map indent interface_impls)@
            ["};\n\n"]
    in
        output_string out_chan (list_code module_impl)

(*
 * Code generation common to both engine and debugger.
 * -- Thrift accessors for query results and internal maps.
 *)

let generate_thrift_accessor_declarations global_decls =
    let list_code l = String.concat "\n" l in
    let (struct_decls, key_decls, accessors_decls, less_opers) =
        List.fold_left (fun (struct_acc, key_acc, accessor_acc, less_acc ) d ->
            let (struct_decls, key_decl, accessor_decl, less_opers ) =
                match d with
                    | `Declare x ->
                          begin
                              match x with
		                  | `Variable(n, typ) -> 
                                        let ttype = thrift_type_of_base_type typ in
                                            ([], [], ttype^" get_"^n^"(),", [])

                                  | (`Tuple (n,f) as y) ->
                                        let ds = datastructure_of_declaration y in
                                        let n = identifier_of_datastructure ds in
                                        let ttype = thrift_type_of_datastructure ds in
                                        let acs_decl = ttype^" get_"^n^"()" in

                                        let field_declarations f =
                                            let (_, r) =
                                                List.fold_left (fun (counter, acc) ((id,ty),_) ->
                                                    let field_decl =
                                                        ((string_of_int counter)^": "^
                                                            (thrift_type_of_base_type
                                                                (ctype_of_type_identifier ty))^" "^id^",")
                                                    in
                                                        (counter+1, acc@[field_decl]))
                                                    (1, []) f
                                            in
                                                r
                                        in

                                        let struct_decl = 
                                            ["struct "^ttype^" {"]@(field_declarations f)@["}"]
                                        in
                                            (struct_decl, [], acs_decl, [])

                                  | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                        let ds = datastructure_of_declaration y in
                                        let n = identifier_of_datastructure ds in
                                        let ttype = thrift_type_of_datastructure ds in
                                        let (inner_decl_name, inner_decl, less_oper) =
                                            thrift_inner_declarations_of_datastructure ds
                                        in
                                        let acs_decl = ttype^" get_"^n^"()," in
                                            if (List.length inner_decl) > 0 then
                                                ([], [(inner_decl_name, list_code inner_decl)], acs_decl, [less_oper])
                                            else
                                                ([], [], acs_decl, [])

                                  | `ProfileLocation _ -> ([], [], "", [])
                          end
                    | _ -> raise (CodegenException
                          ("Invalid declaration: "^(indented_string_of_code_expression d)))
            in
                if key_decl = [] && accessor_decl = "" then
                    (struct_acc, key_acc, accessor_acc, less_acc)
                else
                    (struct_acc@struct_decls, key_acc@key_decl, accessor_acc@[accessor_decl], less_acc@less_opers))
            ([], [], [], []) global_decls
    in
        (struct_decls, key_decls, accessors_decls, less_opers )

let generate_thrift_accessor_implementations global_decls service_class_name =
    let indent s = "    "^s in
    let accessor_defns =
        List.fold_left (fun acc d ->
            let new_defn =
                match d with
                    | `Declare x ->
                          begin
                              match x with
		                  | `Variable(n, typ) -> 
                                        (* Return declared variable *)
                                        let ttype = thrift_type_of_base_type typ in
                                        let dtype = ctype_of_thrift_type ttype in
                                            [dtype^" get_"^n^"()"; "{" ]@
                                                (List.map indent
                                                    ([dtype^" r = static_cast<"^dtype^">("^n^");";
                                                    "return r;"]))@
                                                [ "}\n" ]

                                  | (`Tuple (_,f)) as y ->
                                        let ds = datastructure_of_declaration y in
                                        let n = identifier_of_datastructure ds in
                                        let ttype = ctype_of_thrift_datastructure ds in
                                            ["void get_"^n^"("^ttype^"& _return)"; "{"]@
                                            (List.map (fun ((id,ty),rv) ->
                                                let rv_str = match rv with
                                                    | `Arith a -> string_of_arith_code_expression a
                                                    | `Map mid -> mid
                                                in
                                                    indent ("_return."^id^" = "^rv_str^";")) f)@
                                            ["}\n"]

                                  | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                        let ds = datastructure_of_declaration y in
                                        let n = identifier_of_datastructure ds in
                                        let ttype = ctype_of_thrift_datastructure ds in
                                        let (inserter_defn, inserter_name) = thrift_inserter_of_datastructure ds in
                                        let inserter_mem_fn =
                                            "boost::bind(&"^service_class_name^"::"^inserter_name^", this, _return, _1)"
                                        in
                                        let (begin_it, end_it, begin_decl, end_decl) =
		                            range_iterator_declarations_of_datastructure ds
                                        in
                                            (* Copy datastructure into _return *)
                                            inserter_defn@
                                            ["void get_"^n^"("^ttype^"& _return)"; "{"]@
                                                (List.map indent
                                                    ([(begin_decl^" = "^(identifier_of_datastructure ds)^".begin();");
                                                    (end_decl^" = "^(identifier_of_datastructure ds)^".end();");
                                                    "for_each("^begin_it^", "^end_it^",";
                                                    (indent inserter_mem_fn)^");"]))@
                                                ["}\n"]

                                  | `ProfileLocation _ -> []
                          end
                    | _ -> raise (CodegenException
                          ("Invalid declaration: "^(indented_string_of_code_expression d)))
            in
                acc@new_defn)
            [] global_decls
    in
        accessor_defns


let generate_dbtoaster_thrift_less_operators out_chan less_opers query_id eng_debug = 
    let includes = 
        "#include \""^query_id^"_types.h\"\n\n" 
    in
    let namespace = 
        "namespace DBToaster { namespace "^eng_debug^" { namespace "^query_id^" {\n"
    in
    let tail = "}; }; };" in 
    let functions = 
        List.fold_left (fun acc x -> acc^"\n"^x ) "" less_opers
    in 
    let result = includes^namespace^functions^tail
    in
        print_endline result;
        output_string out_chan result

(*
 * Query result viewer
 *)
let generate_stream_engine_viewer_includes out_chan =
    let list_code l = String.concat "\n" l in
    let includes =
        ["\n\n// Viewer includes.";
        "#include \"AccessMethod.h\"\n\n";]
    in
        generate_thrift_includes out_chan;
        output_string out_chan (list_code includes)

let generate_stream_engine_viewer thrift_out_chan code_out_chan less_out_chan query_id global_decls =
    let indent s = "    "^s in
    let service_namespace =
        "DBToaster.Viewer"^(if query_id = "" then "" else ("."^query_id))
    in
    let thrift_service_name = "AccessMethod" in
    let thrift_service_class = thrift_service_name^"Handler" in
    let thrift_service_interface = thrift_service_name^"If" in

    let (struct_decls, key_decls, accessors_decls, less_opers) = generate_thrift_accessor_declarations global_decls in
    let accessor_impls = generate_thrift_accessor_implementations global_decls thrift_service_class in

    let thrift_includes =  ["profiler.thrift"] in
    let thrift_service_inheritance = ["profiler.Profiler"] in
    let thrift_languages = [ "cpp"; "java" ] in
    let thrift_global_decls = struct_decls@(snd (List.split key_decls)) in
    let access_method_decls = List.map indent accessors_decls in

    let service_constructor_args = [] in
    let service_constructor_member_init = [] in
    let service_constructor_init = [ "PROFILER_INITIALIZATION" ] in

    let service_internals = [] in

    (* Method bodies *)
    let service_interface_impls =
        (List.map indent
            (accessor_impls@["PROFILER_SERVICE_METHOD_IMPLEMENTATION"]))
    in
        generate_dbtoaster_thrift_module thrift_out_chan
            thrift_includes service_namespace thrift_languages
            thrift_service_name thrift_service_inheritance
            thrift_global_decls access_method_decls;

        generate_dbtoaster_thrift_module_implementation code_out_chan
            service_namespace thrift_service_class thrift_service_interface
            service_constructor_args service_constructor_member_init service_constructor_init
            service_internals service_interface_impls;

        generate_dbtoaster_thrift_less_operators less_out_chan 
            less_opers query_id "Viewer" 

(*
 * Top-level standalone engine code generation functions
 *)

let generate_thrift_server_main viewer_class viewer_constructor_args =
    [ "int port = 20000;";
    "boost::shared_ptr<"^viewer_class^"> handler(new "^viewer_class^"("^viewer_constructor_args^"));";
    "boost::shared_ptr<TProcessor> processor(new AccessMethodProcessor(handler));";
    "boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));";
    "boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());";
    "boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());";
    "TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);";
    "server.serve();" ]
