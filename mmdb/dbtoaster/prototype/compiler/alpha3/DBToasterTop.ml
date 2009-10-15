open Algebra

module C=Compiler
module BC=Compiler.BytecodeCompiler

open Sqllexer
open Sqlparser;;

(* returns: expr_l * source_ht 
 * where,
 * expr_l : ((Algebra.readable_term_t list) * 
 *     (string * Algebra.var_t list) list * (Algebra.var_t list)) list
 * source_ht : (string, Runtime.source_info) Hashtbl.t *)
let parse_sql_string str =
    let lexbuf = Lexing.from_string str in
        init_line lexbuf;
        Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuf

(* Simple wrapper around the parser to dump out a readable term in a toplevel *)
let parse_sql_to_term sql_str =
    let (expr_l, _) = parse_sql_string sql_str in
        List.flatten (List.map (fun (t_l,_,_) -> t_l) expr_l)

(* Simple wrapper around the parser, compiler, bytecode generator
 * to print ck's format in a toplevel *)
let compile_sql_to_messages sql_str =
    let (expr_l, _) = parse_sql_string sql_str in
    let messages =
        List.flatten (List.map
            (fun (t_l,db_schema,params) ->
                C.compile_terms db_schema "q" params
                    (List.map make_term t_l) C.compile_readable_messages)
            expr_l)
    in
        print_endline (String.concat "\n" messages)

(* Simple wrapper around the parser, compiler, bytecode generator
 * to print spread messages in a toplevel *)
let compile_sql_to_spread sql_str =
    let (expr_l, _) = parse_sql_string sql_str in
    let spread_messages =
        List.flatten (List.map
            (fun (t_l,db_schema,params) ->
                C.compile_terms db_schema "q" params
                    (List.map make_term t_l) C.compile_spread_messages)
            expr_l)
    in
        print_endline (String.concat "\n" spread_messages)

(* Simple wrapper around the parser, compiler, bytecode generator
 * to directly dump out bytecode in a toplevel *)
let compile_sql_to_bytecode sql_str =
    let (expr_l, _) = parse_sql_string sql_str
    in
        List.flatten (List.map
            (fun (t_l, db_schema, params) ->
                BC.compile_terms_in_toplevel
                    db_schema "q" params (List.map make_term t_l))
            expr_l)
;;
