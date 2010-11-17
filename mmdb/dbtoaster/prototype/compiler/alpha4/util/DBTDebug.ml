open Util
open CalcToM3

module K3OC = K3Compiler.Make(K3OCamlgen.K3CG)

let compiled_k3_for_script script_file = 
   let f = open_in script_file in 
   let lexbuff = Lexing.from_channel f in
   let (calc,sources) = Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff in
   let dbschema = List.flatten (List.map (fun (_,x,_) -> x) calc) in
   let program = CalcToM3.M3InProgress.finalize
      (List.fold_left 
         (fun accum (qlist,dbschema,qvars) -> 
            (List.fold_left 
               (fun accum q  ->
                  (CalcToM3.compile
                     dbschema 
                     ((Calculus.make_term q) ,
                     (Calculus.map_term "QUERY" qvars))
                     accum)
               )
               accum
               qlist
            )
         )
         CalcToM3.M3InProgress.init
         calc
      )
   in
      K3OC.compile_query_to_string dbschema (program,sources) ["QUERY"]

;;

let ocaml_compile_unit_test ?(libs=[])
                            ?(libdirs=["lib/ocaml";"util";"stages/maps";
                                       "stages/functional"])
                            ?(post_out=(fun x->x))
                            ?(post_err=(fun x->x))
                            name script expected_out expected_err =
   let text = (Util.string_of_list ""
      (List.map (fun x -> "open "^x^"\n") libs))^script in
   let cmd = 
      ("./dbtoaster_top -noprompt "^
      (Util.string_of_list " " (List.map (fun x -> "-I "^x) libdirs))^
      " tmp.ml")
   in
      (*print_string (cmd^"\n");*)
   let (in_c,pout_c,err_c) = Unix.open_process_full cmd (Array.make 0 "") in
   let out_c = open_out "tmp.ml" in
      output_string out_c text;
      flush out_c;
      close_out out_c;
   let std_output = post_out (Util.complete_read in_c) in
   let err_output = post_err (Util.complete_read err_c) in
   let _ = Unix.close_process_full (in_c,pout_c,err_c) in
      (if expected_out <> "" then
       if expected_err <> "" then
         Debug.log_unit_test
            name
            (fun (out,err) -> "--Output--\n"^out^"\n--Error--\n"^err)
            (std_output,err_output)
            (expected_out,expected_err)
      else
         Debug.log_unit_test
            name
            (fun x->x)
            std_output
            expected_out
      else 
         Debug.log_unit_test
            name
            (fun x -> x)
            err_output
            expected_err
      );
      Unix.unlink("tmp.ml");