(* Standard ocaml modules *)
open Arg
open Unix

(* XML-light, will be needed when using TML *)
(*open Xml*)

(* DBToaster *)
open Algebra
open Sqllexer
open Sqlparser
open Driver


exception BuildException of string

module Make =
    functor (D : DriverSig) ->
struct


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
        match !D.input_file_type with
            | "tml" ->
                  (* TODO: import gui.ml from alpha2 *)
                  (*
                  let file_exprs_l =
                      try List.map parse_treeml_file file_list
                      with InvalidTreeML x ->
                          print_endline ("Parsing TreeML failed: "^x);
                          exit 1
                  in
                  let relation_sources =
                      try parse_data_sources_config !D.data_sources_config_file
                      with ConfigException s ->
                          print_endline ("Failed reading data sources config "^s);
                          exit 1
                  in
                      (file_exprs_l, relation_sources)
                  *)
                  raise (Failure "TML not implemented.")
                          
            | "sql" ->
                  let parse_sql_file fn =
                      let lexbuf = Lexing.from_channel (open_in fn) in
                          init_line lexbuf;
                          Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuf
                  in
                  (* file_exprs_l :
                   *     ((Algebra.readable_term_t list) *
                   *          (Algebra.var_t list)) list list
                   * source_list :
                   *     (string, Runtime.source_info) Hashtbl.t list
                   *)
                  let (file_exprs_l, source_list) = 
                      List.split (List.map parse_sql_file file_list)
                  in
                  let relation_sources =
                      List.fold_left
                          (fun acc h ->
                              Hashtbl.fold
                                  (fun n si acc ->
                                      if List.mem si acc then acc
                                      else (acc@[si]))
                                  h acc)
                          [] source_list
                  in
                      (file_exprs_l, relation_sources)

            | _ -> raise (BuildException
                  ("Invalid input file type: "^(!D.input_file_type)))

    (*
     * Compiler main
     *)
    let print_input file_exprs_l =
        let print_schema schema =
            let string_of_vars v =
                String.concat ","
                    (List.map (fun (n,t) -> n^":"^(type_as_string t)) v)
            in
                print_endline "Schema:";
                List.iter
                    (fun (r,v) ->
                        print_endline (r^": "^(string_of_vars v)))
                    schema
        in
        let print_params params = 
            print_endline
                ("Params: "^(String.concat "," (List.map fst params)))
        in
        let print_t_l t_l =
            List.iter
                (fun t -> print_endline
                    ("Input:\n"^(term_as_string (make_term t) [])))
                t_l
        in
            List.iter
                (fun (t_l, db_schema, params) ->
                    print_schema db_schema;
                    print_params params;
                    print_t_l t_l)
                (List.flatten file_exprs_l)

    let main () =

        (* Parse options *)
        parse D.dbtoaster_arg_spec D.add_source D.dbtoaster_usage;

        (* Invoke compiler on TML file according to mode *)
        let (file_exprs_l, relation_sources) = match !D.input_files with
            | [] ->
                  print_endline "No queries specified for compilation!";
                  usage D.dbtoaster_arg_spec D.dbtoaster_usage;
                  exit 1

            | fn -> parse_input_files fn
        in
            print_input file_exprs_l;

            List.iter
                (fun (t_l, db_schema, params) ->
                    D.run db_schema params
                        (List.map make_term t_l) relation_sources)
                (List.flatten file_exprs_l)
end
;;

module Main = Make(Driver);;
(*
module Main = Make(ThriftDriver);;
*)

Main.main()

