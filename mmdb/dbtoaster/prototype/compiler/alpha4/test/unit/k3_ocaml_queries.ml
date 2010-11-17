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
   DBTDebug.ocaml_compile_unit_test
      ~post_out:(fun x -> 
         let start = (String.index x '\n')+1 in
            String.sub x start (String.length x - start))
      ("K3 Ocaml Compile "^script)
      (DBTDebug.compiled_k3_for_script script)
      ("QUERY: "^result^"\n") ""

;;

test_compile "test/sql/rst.sql" "1.87533670489e+13";;
test_compile "test/sql/vwap.sql" "1.87533670489e+13";;


exit (-1)