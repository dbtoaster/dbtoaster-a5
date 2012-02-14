open Types
open Arithmetic
open Calculus
open Calculus
open Calculus.CalcRing


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
   string_of_expr
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

let test_delta (name:string) ins reln relv expr delta =
   Debug.log_unit_test ("Deltas: "^name)
      Calculus.string_of_expr
      (CalculusDeltas.delta_of_expr
         (  (if ins then Schema.InsertEvent else Schema.DeleteEvent),
            (reln, List.map var relv, Schema.StreamRel, TInt))
         (parse expr))
      (parse delta)
;;

test_delta "2-way Delta" true "R" ["dA"; "dB"]
   "AggSum([], R(A, B)*S(B, C))"
   "AggSum([], (A ^= dA)*(B ^= dB)*S(B,C))"

;;

Debug.log_unit_test "Renaming an external"
   string_of_expr
   (Calculus.rename_vars [var "A", var "B"] 
                         (parse "FOO(int)[][A]"))
   (parse "FOO(int)[][B]")