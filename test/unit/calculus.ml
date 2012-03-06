open Types
open Arithmetic
open Calculus
open Calculus
open Calculus.CalcRing
open UnitTest


let test_expr = 
   mk_prod [
      mk_val (Value(mk_int 1));
      mk_val (rel "R" ["a";"b"])
   ]
;;

log_test "Stringification"
   (fun x -> x)
   (string_of_expr test_expr)
   ("R(a, b)")
;;

log_test "Parsing"
   string_of_expr
   (parse_calc "R(A,B) * S(B,C) * [A+C] * [A < C]")
   (CalcRing.mk_prod [
      CalcRing.mk_val (rel "R" ["A"; "B"]);
      CalcRing.mk_val (rel "S" ["B"; "C"]);
      CalcRing.mk_val (Value(
         ValueRing.mk_sum [
            Arithmetic.mk_var (var "A");
            Arithmetic.mk_var (var "C")
         ]
      ));
      CalcRing.mk_val (Cmp(Lt,
         Arithmetic.mk_var (var "A"),
         Arithmetic.mk_var (var "C")
      ))
   ])
;;

let test_delta (name:string) ins reln relv expr delta =
   log_test ("Deltas: "^name)
      Calculus.string_of_expr
      (CalculusDeltas.delta_of_expr (event ins reln relv) (parse_calc expr))
      (parse_calc delta)
;;

test_delta "2-way Delta" true "R" ["dA"; "dB"]
   "AggSum([], R(A, B)*S(B, C))"
   "AggSum([], (A ^= dA)*(B ^= dB)*S(B,C))"

;;

log_test "Renaming an external"
   string_of_expr
   (Calculus.rename_vars [var "A", var "B"] 
                         (parse_calc "FOO(int)[][A]"))
   (parse_calc "FOO(int)[][B]")