open Algebra

module C=Compiler.MessageCompiler
module BC=Compiler.BytecodeCompiler

open Sqllexer
open Sqlparser;;

(* returns: expr_l * source_ht 
 * where,
 * expr_l : ((Algebra.readable_term_t list) * 
 *     (string * Algebra.var_t list) list * (Algebra.var_t list)) list
 * source_ht : (string, Runtime.source_info) Hashtbl.t *)
let parse_sql fn str =
    let lexbuf = fn str in
        init_line lexbuf;
        Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuf

let parse_sql_string = parse_sql Lexing.from_string
let parse_sql_file = parse_sql (fun s -> Lexing.from_channel (open_in s))

(* Simple wrapper around the parser to dump out a readable term in a toplevel *)
let parse_sql_to_term sql_str =
    let (expr_l, _) = parse_sql_string sql_str in
        List.flatten (List.map (fun (t_l,_,_) -> t_l) expr_l)

(* Simple wrapper around the parser, compiler, bytecode generator
 * to print ck's format in a toplevel *)
let compile_sql_to_messages_aux compilation_level parse_fn =
    let (expr_l, _) = parse_fn () in
    let messages =
        List.flatten (List.map
            (fun (t_l,db_schema,params) ->
                C.compile_terms db_schema "q" params
                    C.compile_readable_messages compilation_level
                    (List.map make_term t_l))
            expr_l)
    in
        messages

let compile_sql_to_messages_l l str =
    compile_sql_to_messages_aux l (fun () -> parse_sql_string str)

let compile_sql_file_to_message_l l str =
    compile_sql_to_messages_aux l (fun () -> parse_sql_file str)

let compile_sql_to_messages = compile_sql_to_messages_l max_int
let compile_sql_file_to_messages = compile_sql_to_messages_l max_int


(* Simple wrapper around the parser, compiler, bytecode generator
 * to print spread messages in a toplevel *)
let compile_sql_to_spread_aux compilation_level parse_fn =
    let (expr_l, _) = parse_fn() in
    let spread_messages =
        List.flatten (List.map
            (fun (t_l,db_schema,params) ->
                C.compile_terms db_schema "q" params
                    C.compile_spread_messages compilation_level 
                    (List.map make_term t_l))
            expr_l)
    in
        spread_messages

let compile_sql_to_spread str =
    compile_sql_to_spread_aux max_int (fun () -> parse_sql_string str)

let compile_sql_file_to_spread str =
    compile_sql_to_spread_aux max_int (fun () -> parse_sql_file str)


(* Simple wrapper around the parser, compiler, bytecode generator
 * to directly dump out bytecode in a toplevel *)
let compile_sql_to_bytecode sql_str compilation_level =
    let (expr_l, _) = parse_sql_string sql_str
    in
        List.flatten (List.map
            (fun (t_l, db_schema, params) ->
                BC.compile_terms_in_toplevel db_schema "q"
                    params compilation_level (List.map make_term t_l))
            expr_l)
;;

let test_flatten sql_str params =
    let x = List.map make_term (parse_sql_to_term sql_str) in
    let flat_term_l = List.map
        (fun x -> let (tc,bsv) = flatten_term params x in
            (mk_cond_agg tc, bsv))
        (List.map readable_term (List.map roly_poly x))
    in
        flat_term_l

let test_simplify sql_str rel relsch mapn params bigsum_vars =
    let flat_terms = test_flatten sql_str params in
        Compiler.compile_delta_for_rel rel relsch mapn params bigsum_vars false
            (make_term (fst (List.hd flat_terms)))
;;

