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
         let timer = (Str.search_forward (Str.regexp("Processing time: ")) x 0) in
         let index = (Str.search_forward (Str.regexp("QUERY: ")) x 0) in
            print_string ("Run time "^script^": "^
                          (String.sub x (timer + 17) (index - timer - 17)));
            String.sub x index ((String.length x) - index)
         with Not_found -> "[[malformed runtime output]]\n"^x
      )
      ("K3 Ocaml Compile "^script)
      (DBTDebug.compiled_k3_for_script script)
      (Some("QUERY: "^result^"\n")) (None)
   )
   
;;


test_compile "test/sql/sgl.sql" "QUERY: [[ 28. ]->6.; [ 0. ]->0.; [ 45. ]->11.; [ 3. ]->0.; [ 19. ]->4.; [ 13. ]->2.; [ 56. ]->10.; [ 6. ]->1.; [ 38. ]->5.; [ 26. ]->7.; [ 41. ]->10.; [ 17. ]->4.; [ 12. ]->3.; [ 52. ]->7.; [ 34. ]->8.; [ 55. ]->11.; [ 24. ]->0.; [ 37. ]->0.; [ 11. ]->2.; [ 48. ]->10.; [ 5. ]->1.; [ 31. ]->5.; [ 51. ]->11.; [ 22. ]->5.; [ 33. ]->3.; [ 10. ]->2.; [ 44. ]->5.; [ 29. ]->4.; [ 47. ]->8.; [ 20. ]->5.; [ 2. ]->0.; [ 9. ]->2.; [ 1. ]->1.; [ 40. ]->1.; [ 4. ]->0.; [ 27. ]->0.; [ 43. ]->1.; [ 18. ]->4.; [ 54. ]->12.; [ 8. ]->2.; [ 36. ]->0.; [ 57. ]->15.; [ 25. ]->4.; [ 16. ]->5.; [ 39. ]->4.; [ 50. ]->12.; [ 32. ]->7.; [ 53. ]->11.; [ 23. ]->7.; [ 15. ]->5.; [ 35. ]->1.; [ 46. ]->2.; [ 7. ]->1.; [ 30. ]->5.; [ 49. ]->3.; [ 21. ]->2.; [ 14. ]->2.; [ 42. ]->10.;]<pat= >";;
test_compile "test/sql/rst.sql"           "1.87533670489e+13";;
test_compile "test/sql/finance/vwap.sql"  "31230008700.";;
test_compile "test/sql/rs_inequality.sql" "1.3525254564e+15";;