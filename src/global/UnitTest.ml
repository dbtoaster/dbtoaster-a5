(**
  Generic tools for implementing the unit tests that live in
  test/unit.  This includes both a set of generic logging functions
  as well as a few utility functions for calling into things like
  parsers, etc...
*)

open Types
open Ring
open Arithmetic

let showdiff exp_str fnd_str = 
   print_string ("--Expected--\n"^exp_str^
                 "\n\n--Result--\n"^fnd_str^"\n\n"); 
   if not (Debug.active "NO-VISUAL-DIFF") then
      match (Debug.os ()) with
       | "Darwin" -> (
         let (exp,exp_fd) = Filename.open_temp_file "expected" ".diff" in
         let (fnd,fnd_fd) = Filename.open_temp_file "found" ".diff" in
            output_string exp_fd exp_str; close_out exp_fd;
            output_string fnd_fd fnd_str; close_out fnd_fd;
            let _ = Unix.system("opendiff "^exp^" "^fnd) in
            Unix.unlink exp;
            Unix.unlink fnd
         )
       | _ -> ()
;;
let log_test (title:string) (to_s:'a -> string) 
                  (result:'a) (expected:'a) : unit =
   if result = expected then print_endline (title^": Passed")
   else (
      print_endline (title^": Failed");
      showdiff (to_s expected) (to_s result);
      exit 1
   )
;;
let log_boolean (title:string) (result:bool) (expected:bool) : unit =
   log_test title (function true -> "TRUE" | false -> "FALSE") result expected
;;
let log_list_test (title:string) (to_s:'a -> string) 
                       (result:'a list) (expected:'a list) : unit =
   if result = expected then print_endline (title^": Passed")
   else (
      let rec diff_lists rlist elist = 
         let recurse rstr estr new_rlist new_elist =
            let (new_rstr, new_estr) = diff_lists new_rlist new_elist in
               (rstr^"\n"^new_rstr,estr^"\n"^new_estr)
         in   
            if List.length rlist > 0 then
              if List.length elist > 0 then
                recurse (to_s (List.hd rlist)) (to_s (List.hd elist)) 
                        (List.tl rlist) (List.tl elist)
              else
                recurse (to_s (List.hd rlist)) "--empty line--" 
                        (List.tl rlist) []
            else
              if List.length elist > 0 then
                recurse "--empty line--" (to_s (List.hd elist)) 
                        [] (List.tl elist)
              else
                ("","")
     in
        let (result_diffs, expected_diffs) = diff_lists result expected in
           print_endline (title^": Failed");
           showdiff (expected_diffs) (result_diffs);
           exit 1
   );;

let log_collection_test (title:string) (result:Values.K3Value.t)
                        (expected:(Values.K3Value.t list * 
                                   Values.K3Value.t) list): unit = 
   let collection_entries = 
      match result with
         | Values.K3Value.SingleMap(m) ->
            Values.K3ValuationMap.to_list m
         | Values.K3Value.DoubleMap(dm) ->
					  List.flatten
							(List.map (fun (k1,m) -> 
										List.map
												(fun (k2,v) -> (k1@k2,v) ) 
												(Values.K3ValuationMap.to_list m) )
		        	(Values.K3ValuationMap.to_list dm) )
         | Values.K3Value.TupleList(tlist) -> 
            List.map (fun tuple ->
               match tuple with
                  | Values.K3Value.Tuple(telems) ->
                     if telems = [] then failwith "Invalid tuple"
                     else let revelems = List.rev telems in
                        (  List.rev (List.tl revelems),
                           List.hd revelems )
                  | _ -> failwith "Invalid tuple list"
            ) tlist
         | _ ->
            print_endline (title^": Failed");
            showdiff "-- A collection --" 
                     (Values.K3Value.string_of_value result);
            exit 1
   in 
   let dom = ListAsSet.union (fst (List.split expected))
                             (fst (List.split collection_entries))
   in
   let (expected_strings,found_strings) = 
      List.fold_left (fun (expected_strings,found_strings) k ->
      let k_string = 
         ListExtras.string_of_list Values.K3Value.string_of_value k in
      if not (List.mem_assoc k expected) then
         (  (k_string^" -> n/a")::expected_strings,
            (k_string^" -> "^(Values.K3Value.string_of_value 
                                 (List.assoc k collection_entries)))::
                                    found_strings
         )
      else if not (List.mem_assoc k collection_entries) then
         (  (k_string^" -> "^(Values.K3Value.string_of_value 
                                 (List.assoc k expected)))::
                                    expected_strings,
            (k_string^" -> n/a")::found_strings
         )
      else 
         let expected_val = List.assoc k expected in
         let found_val    = List.assoc k collection_entries in
         if found_val = expected_val
            then (expected_strings,found_strings)
            else (   (k_string^" -> "^(Values.K3Value.string_of_value
                                       expected_val))::expected_strings,
                     (k_string^" -> "^(Values.K3Value.string_of_value
                                       found_val))::found_strings)
   ) ([], []) dom
   in
      if (expected_strings <> []) then (
         let suffix = if List.length expected_strings < List.length dom
                      then "\n... and the remaining values match"
                      else ""
         in
         print_endline (title^": Failed");
         showdiff ((String.concat "\n" expected_strings)^suffix)
                  ((String.concat "\n" found_strings)^suffix);
         exit 1
      ) else (
         print_endline (title^": Passed")
      );;

(*************************** String parsers ****************************)

let parse_sql_expr (expr:string):Sql.expr_t = 
   try  
      Sqlparser.expression Sqllexer.tokenize 
                           (Lexing.from_string expr)
   with Parsing.Parse_error -> (
      print_endline ("Error parsing :'"^expr^"'");
      let _ = Parsing.set_trace true in
      Sqlparser.expression Sqllexer.tokenize 
                           (Lexing.from_string expr)
   )
;;     

let parse_calc ?(opt=false) (expr:string):Calculus.expr_t = 
   try  
      let ret = 
         Calculusparser.calculusExpr Calculuslexer.tokenize 
                                     (Lexing.from_string expr)
      in if opt then CalculusTransforms.optimize_expr 
                        (Calculus.schema_of_expr ret) ret
                else ret
   with Parsing.Parse_error -> (
      print_endline ("Error parsing :'"^expr^"'");
      let _ = Parsing.set_trace true in
      Calculusparser.calculusExpr Calculuslexer.tokenize 
                                     (Lexing.from_string expr)
   )
;;      

let parse_stmt (m3_stmt:string) = 
   try  
      Calculusparser.mapTriggerStmt Calculuslexer.tokenize 
                                     (Lexing.from_string m3_stmt)
   with Parsing.Parse_error -> (
      print_endline ("Error parsing :'"^m3_stmt^"'");
      let _ = Parsing.set_trace true in
      Calculusparser.mapTriggerStmt Calculuslexer.tokenize 
                                     (Lexing.from_string m3_stmt)
   )
;;      

let parse_k3 (expr:string):K3.expr_t =
   try  
      K3parser.statement K3lexer.tokenize 
                         (Lexing.from_string expr)
   with Parsing.Parse_error -> (
      print_endline ("Error parsing :'"^expr^"'");
      let _ = Parsing.set_trace true in
      K3parser.statement K3lexer.tokenize 
                         (Lexing.from_string expr)
   )
;;   

(*************************** Type constructors ****************************)
let var v = (v,Types.TFloat);;
let rel rn rv = (Calculus.Rel(rn, List.map var rv));;
let schema_rel (reln:string) (relv:string list) = 
   (reln, List.map var relv, Schema.StreamRel);;
let event ins reln relv = (
   (if ins then Schema.InsertEvent(schema_rel reln relv)
           else Schema.DeleteEvent(schema_rel reln relv)) 
   
);;
let mk_db rels = (
   let ret_db = Schema.empty_db() in
   List.iter (fun (reln, relv) -> 
      Schema.add_rel ret_db (schema_rel reln relv)
   ) rels;
   ret_db
);;

let mk_float_collection elems = 
   List.map (fun (k,v) ->
      (  List.map 
            (fun x -> Values.K3Value.BaseValue(CFloat(x))) k,
            Values.K3Value.BaseValue(CFloat(v))
      )
   ) elems

(*************************** Shorthand Operations ****************************)
let compile (db:Schema.t) (name:string) (expr:string) =
   Compiler.compile db [(name, parse_calc expr)]

;;
(*************************** Standard Debug Prefs ****************************)
FreshVariable.set_prefix "";;


