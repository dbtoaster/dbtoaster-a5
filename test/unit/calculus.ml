open Types
open Arithmetic
open Calculus
open Calculus.BasicCalculus
open Calculus.BasicCalculus.CalcRing


let parse (expr:string):expr_t = 
   Calculusparser.calculusExpr Calculuslexer.tokenize (Lexing.from_string expr)
let var v = (v,TFloat)
let rel rn rv = (Rel(rn, rv, TInt))
;;


let test_expr = 
   mk_prod [
      mk_val (Value(mk_int 1));
      mk_val (rel "R" ["a",TInt; "b", TInt])
   ]
;;

Debug.log_unit_test "Stringification"
   (fun x -> x)
   (string_of_expr test_expr)
   ("R(a; b)")
;;

Debug.log_unit_test "Parsing"
   BasicCalculus.string_of_expr
   (parse "R(A,B) * S(B,C) * #(A+C)#")
   (CalcRing.mk_prod [
      CalcRing.mk_val (rel "R" [var "A"; var "B"]);
      CalcRing.mk_val (rel "S" [var "B"; var "C"]);
      CalcRing.mk_val (Value(
         ValueRing.mk_sum [
            Arithmetic.mk_var (var "A");
            Arithmetic.mk_var (var "C")
         ]
      ))
   ])
;;