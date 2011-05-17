open Util
open Calculus

let test_reverse name readable_query =
   let query = make_term readable_query in
   Debug.log_unit_test 
      ("Roly Reversibility: "^name)
      string_of_term
      (un_roly_poly (roly_poly_plural query))
      query

let vt v = (v, TInt);;
let var v = RVal(Var(vt v));;

test_reverse "Simple Sum of Terms"
   (RVal(AggSum(
      (RSum[var "a"; var "b"]), 
      (RA_Leaf(Rel("R", [vt "a"; vt "b"])))
   )))
;;
test_reverse "Sum of Terms with Product"
   (RVal(AggSum(
      (RProd[var "c"; (RSum[var "a"; var "b"])]), 
      (RA_Leaf(Rel("R", [vt "a"; vt "b"; vt "c"])))
   )))
