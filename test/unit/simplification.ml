open Types
open Ring
open Arithmetic
open Calculus
open Calculus.BasicCalculus

let parse (expr:string):expr_t = 
   Calculusparser.calculusExpr Calculuslexer.tokenize (Lexing.from_string expr)
let var v = (v,TFloat)
let rel rn rv = (Rel(rn, rv, TInt))
;;

Debug.log_unit_test "Invalid Commute"
   string_of_bool
   (Simplification.commutes (parse "R(A,B) * #A#") (parse "#B#"))
   false
;;
Debug.log_unit_test "Invalid Commute (Made valid by scope)"
   string_of_bool
   (Simplification.commutes ~scope:[var "B"] (parse "R(A,B) * #A#") 
                                             (parse "#B#"))
   true
;;
Debug.log_unit_test "Valid Commute"
   string_of_bool
   (Simplification.commutes (parse "R(A,B) * #A#") (parse "#C#"))
   true
;;
Debug.log_unit_test "Combine Values"
   BasicCalculus.string_of_expr
   (Simplification.combine_values 
      (parse "(R(A,B) * #-1#) * (#22-1# + #3#) * #A#"))
   (parse "#((-24) * A)# * R(A,B)")
;;

Debug.log_unit_test "Lift Equalities (Empty Scope, Unliftable)"
   BasicCalculus.string_of_expr
   (Simplification.lift_equalities []
      (parse "R(A,B) * (A = B)"))
   (parse "R(A,B) * (A = B)")
;;
Debug.log_unit_test "Lift Equalities (Empty Scope, Liftable)"
   BasicCalculus.string_of_expr
   (Simplification.lift_equalities []
      (parse "R(A,B) * S(B,C) * (A = C)"))
   (parse "R(A,B) * (C ^= #A#) * S(B,C)")
;;