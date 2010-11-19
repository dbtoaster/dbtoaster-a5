open Util
open K3OCamlgen
open K3
open K3.SR

;;

let tcode ?(t=K3O.Float) txt = (IP.Leaf("<<"^txt^">>"),t);;
let code_list = List.map tcode ["x";"y";"z"];;
let arg_list = List.map (fun x -> ("<<"^x^">>",K3.SR.TFloat)) ["x";"y";"z"];;

module K3OC = K3Compiler.Make(K3OCamlgen.K3CG)

;;

let test_compile script result =
   (DBTDebug.ocaml_compile_unit_test
      ~post_out:(fun x -> 
         try 
         let timer = (Str.search_forward (Str.regexp("Tuples: ")) x 0) in
         let index = (Str.search_forward (Str.regexp("QUERY: ")) x timer) in
            print_string ("Run time "^script^": "^
                          (String.sub x (timer + 8) (index - timer - 8)));
            String.sub x index ((String.length x) - index)
         with Not_found -> ""
      )
      ("K3 Ocaml Compile "^script)
      (DBTDebug.compiled_k3_for_script script)
      (Some("QUERY: "^result^"\n")) (None)
   )
   
;;


test_compile "test/sql/sgl.sql" "";;
test_compile "test/sql/rst.sql" "1.87533670489e+13";;
test_compile "test/sql/vwap.sql" "31230008700.";;
