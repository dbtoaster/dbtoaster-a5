open GlobalTypes
open Calculus.UnitCalculus

let test_expr = 
   mk_prod [
      mk_int 1;
      mk_rel "R" ["a",TInt; "b", TInt] (TInt)
   ]
;;

Debug.log_unit_test "Basic Stringification"
   (fun x -> x)
   ("(1 * R(<a:int>; <b:int>))")
   (string_of_expr test_expr)