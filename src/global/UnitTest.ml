open Types
open Ring
open Arithmetic


(****************************************************************
 * Generic tools for implementing the unit tests that live in
 * test/unit.  This includes both a set of generic logging functions
 * as well as a few utility functions for calling into things like
 * parsers, etc...
 ****************************************************************)

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

let log_test (title:string) (to_s:'a -> string) 
                  (result:'a) (expected:'a) : unit =
   if result = expected then print_endline (title^": Passed")
   else (
      print_endline (title^": Failed");
      showdiff (to_s expected) (to_s result);
      exit 1
   );;

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
                        (expected:(float list * float) list): unit = 
   let k3_expected = 
      List.map (fun (k,v) -> 
                  (  List.map (fun x -> Values.K3Value.Float(x)) k, 
                     Values.K3Value.Float(v))) 
               expected 
   in
   let collection_entries = 
      match result with
         | Values.K3Value.SingleMap(m) ->
            Values.K3ValuationMap.to_list m
         | _ ->
            print_endline (title^": Failed");
            showdiff "-- A collection --" 
                     (Values.K3Value.string_of_value result);
            exit 1
   in 
   let dom = ListAsSet.union (fst (List.split k3_expected))
                             (fst (List.split collection_entries))
   in
   let (expected_strings,found_strings) = 
      List.fold_left (fun (expected_strings,found_strings) k ->
      let k_string = 
         ListExtras.string_of_list Values.K3Value.string_of_value k in
      if not (List.mem_assoc k k3_expected) then
         (  (k_string^" -> n/a")::expected_strings,
            (k_string^" -> "^(Values.K3Value.string_of_value 
                                 (List.assoc k collection_entries)))::
                                    found_strings
         )
      else if not (List.mem_assoc k collection_entries) then
         (  (k_string^" -> "^(Values.K3Value.string_of_value 
                                 (List.assoc k k3_expected)))::
                                    expected_strings,
            (k_string^" -> n/a")::found_strings
         )
      else 
         let expected_val = List.assoc k k3_expected in
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

(*************************** Shorthand Operations ****************************)
let compile (db:Schema.t) (name:string) (expr:string) =
   let q_expr = parse_calc expr in
   let q_schema = Calculus.schema_of_expr q_expr in
   let q_type = Calculus.type_of_expr q_expr in
      Compiler.compile db [{
         Plan.ds_name = Plan.mk_ds_name name q_schema q_type;
         Plan.ds_definition = q_expr
      }]
