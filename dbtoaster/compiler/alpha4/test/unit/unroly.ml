open Util
open Calculus

let test_reverse ?((f,fn) = (poly_factorize,"post")) name readable_query =
   let query = make_term readable_query in
   Debug.log_unit_test 
      ("Roly Reversibility ("^fn^"): "^name)
      string_of_term
      (f (roly_poly_plural query))
      query;

;;
let test_all_reverse name readable_query
   test_reverse ~f:(un_roly_poly,"basic") name readable_query;
   test_reverse ~f:(poly_factorize,"post") name readable_query

;;
let test_unrolly name r_mlist r_factored = 
   let mlist = List.map make_term r_mlist
   and factored = make_term r_factored
   in Debug.log_unit_test 
      ("Unrolly: "^name)
      string_of_term
      (un_roly_poly mlist)
      factored
;;

let vt v = (v, TInt);;
let var v = RVal(Var(vt v));;

test_all_reverse "Simple Sum of Terms"
   (RVal(AggSum(
      (RSum[var "a"; var "b"]), 
      (RA_Leaf(Rel("R", [vt "a"; vt "b"])))
   )))
;;
test_all_reverse "Sum of Terms with Product"
   (RVal(AggSum(
      (RProd[var "c"; (RSum[var "a"; var "b"])]), 
      (RA_Leaf(Rel("R", [vt "a"; vt "b"; vt "c"])))
   )))
