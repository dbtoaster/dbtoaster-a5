open Util
open CalcToM3

module K3OC = K3Compiler.Make(K3OCamlgen.K3CG)

let parse_script script_file = 
   let f = open_in script_file in 
   let lexbuff = Lexing.from_channel f in
      Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff


;;

let compiled_m3_for_script script_file =
   let (calc,sources) = parse_script script_file in
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
      (program, dbschema, sources)
;;

let k3_for_script script_file = 
   let ((schema,prog),dbschema,sources) = compiled_m3_for_script script_file in
   let (m3ptrigs,patterns) = M3Compiler.prepare_triggers prog in
   let (_,_,trigs) = K3Builder.collection_prog (schema,m3ptrigs) patterns in
      List.map (fun (event, rel, args, cs) ->
         let stmts = List.map K3OC.compile_k3_expr (List.map fst cs) in
            (event, rel, args, stmts)
         ) trigs

;;

let compiled_k3_for_script script_file = 
   let (program,dbschema,sources) = compiled_m3_for_script script_file in
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
      (match (expected_out,expected_err) with
      | ((Some(eo)),(Some(ee))) -> 
         Debug.log_unit_test 
            name 
            (fun (out,err) -> cmd^"\n....Output....\n"^out^
                                  "\n....Error....\n"^err)
            (std_output,err_output)
            (eo,ee)
      | (None,(Some(ee))) ->
         Debug.log_unit_test name (fun x->cmd^"\n"^x) err_output ee
      | ((Some(eo)),None) -> 
         Debug.log_unit_test 
            name 
            (fun (out,err) -> cmd^"\n"^(if err <> "" then "'"^err^"'" else out))
            (std_output,err_output) 
            (eo,"")
      | (None,None) ->
         print_string ("Skipping unnecessary test: "^name)
      );
      Unix.unlink("tmp.ml")

;;

let k3_test_compile ?(expected = "") name args program =
   let code = K3OC.compile_k3_expr program in
   let text = 
      "module MC = K3ValuationMap\n"^"module DB = NamedK3Database\n"^
      "let "^name^" dbtoaster_db "^
      (Util.string_of_list " " (List.map (fun x -> "var_"^x) args))^" = "^
      (K3OCamlgen.K3CG.to_string code) 
   in
      ocaml_compile_unit_test
         ~libs:["Database";"Values"]
         ("K3 Ocaml RST "^name)
         text
         None (Some(expected))

;;
