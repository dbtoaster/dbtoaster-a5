open Types
open Ring
open Arithmetic
open Calculus
open Calculus.BasicCalculus

let parse (expr:string):expr_t = 
   try
      Calculusparser.calculusExpr 
         Calculuslexer.tokenize (Lexing.from_string expr)
   with Parsing.Parse_error ->
      let _ = Parsing.set_trace true in
      Calculusparser.calculusExpr 
         Calculuslexer.tokenize (Lexing.from_string expr)
;;      
let var v = (v,TFloat)
let rel rn rv = (Rel(rn, rv, TInt))
;;

Debug.log_unit_test "Invalid Commute"
   string_of_bool
   (CalculusOptimizer.commutes (parse "R(A,B) * #A#") (parse "#B#"))
   false
;;
Debug.log_unit_test "Invalid Commute (Made valid by scope)"
   string_of_bool
   (CalculusOptimizer.commutes ~scope:[var "B"] (parse "R(A,B) * #A#") 
                                             (parse "#B#"))
   true
;;
Debug.log_unit_test "Valid Commute"
   string_of_bool
   (CalculusOptimizer.commutes (parse "R(A,B) * #A#") (parse "#C#"))
   true
;;
Debug.log_unit_test "Combine Values"
   BasicCalculus.string_of_expr
   (CalculusOptimizer.combine_values 
      (parse "(R(A,B) * #-1#) * (#22-1# + #3#) * #A#"))
   (parse "#((-24) * A)# * R(A,B)")
;;

let test msg scope input output =
   Debug.log_unit_test ("Lift Equalities ("^msg^")")
      BasicCalculus.string_of_expr
      (CalculusOptimizer.lift_equalities scope (parse input))
      (parse output)
in 
   test "Empty Scope, Unliftable" [] 
      "R(A,B) * (A = B)"
      "R(A,B) * (A = B)";
   test "Empty Scope, Liftable" []
      "R(A,B) * S(B,C) * (A = C)"
      "R(A,B) * (C ^= #A#) * S(B,C)";
   test "Liftable due to scope" [var "B"]
      "R(A,B) * (A = B)"
      "(A ^= #B#) * R(A,B)";
   test "Unliftable due to scope" [var "A"; var "B"]
      "R(A,B) * (A = B)"
      "(A = B) * R(A,B)";
   test "Liftable due to nested scope" []
      "S(B) * AggSum([], R(A,B) * (A = B))"
      "S(B) * AggSum([], (A ^= #B#) * R(A,B))";
   test "Unliftable due to nested scope" []
      "S(A, B) * AggSum([], R(A,B) * (A = B))"
      "S(A, B) * AggSum([], (A = B) * R(A,B))"
;;

let test msg schema input output =
   Debug.log_unit_test ("Unify Lifts ("^msg^")")
      BasicCalculus.string_of_expr
      (CalculusOptimizer.unify_lifts schema (parse input))
      (parse output)
in
   test "Empty schema, Full expression into variable" []
      "(A ^= AggSum([C],R(B,C)*#B#)) * #A#"
      "AggSum([C],R(B,C)*#B#)";
   test "Empty schema, Full expression into value" []
      "(A ^= AggSum([C],R(B,C)*#B#)) * #2*A#"
      "(A ^= AggSum([C],R(B,C)*#B#)) * #2*A#";
   test "Empty schema, Full expression into relation" []
      "(A ^= AggSum([C],R(B,C)*#B#)) * R(A)"
      "(A ^= AggSum([C],R(B,C)*#B#)) * R(A)";
   test "Empty schema, Full expression into external" []
      "(A ^= AggSum([C],R(B,C)*#B#)) * E[A][]"
      "(A ^= AggSum([C],R(B,C)*#B#)) * E[A][]";
   test "Empty schema, Full expression into all" []
      "(A ^= AggSum([C],R(B,C)*#B#)) * #A# * #2*A# * R(A)"
      "(A ^= AggSum([C],R(B,C)*#B#)) * #A# * #2*A# * R(A)";
   test "Empty schema, Value into variable" []
      "(A ^= #2*B#) * #A#"
      "#2*B#";
   test "Empty schema, Value into value" []
      "(A ^= #2*B#) * #2*A#"
      "#2*2*B#";
   test "Empty schema, Value into relation" []
      "(A ^= #2*B#) * R(A)"
      "(A ^= #2*B#) * R(A)";
   test "Empty schema, Value into external" []
      "(A ^= #2*B#) * E[A][]"
      "(A ^= #2*B#) * E[A][]";
   test "Empty schema, Value into all" []
      "(A ^= #2*B#) * #A# * #2*A# * R(A)"
      "(A ^= #2*B#) * #A# * #2*A# * R(A)";
   test "Empty schema, Variable into variable" []
      "(A ^= #B#) * #A#"
      "#B#";
   test "Empty schema, Variable into value" []
      "(A ^= #B#) * #2*A#"
      "#2*B#";
   test "Empty schema, Variable into relation" []
      "(A ^= #B#) * R(A)"
      "R(B)";
   test "Empty schema, Variable into external" []
      "(A ^= #B#) * E[A][]"
      "E[B][]";
   test "Empty schema, Variable into all" []
      "(A ^= #B#) * #A# * #2*A# * R(A)"
      "#B# * #2*B# * R(B)";
   test "Variable into all made invalid by schema" [var "A"]
      "(A ^= #B#) * #A# * #2*A# * R(A)"
      "(A ^= #B#) * #A# * #2*A# * R(A)";
   test "Cascading pair with nesting" []
      "(A ^= #B#) * AggSum([], (C ^= #A*2#)*#C#)"
      "AggSum([], #2*B#)";
   test "Cascading pair made partly invalid by nested schema" []
      "(A ^= #B#) * AggSum([C], (C ^= #A*2#)*#C#)"
      "AggSum([C], (C ^= #2*B#)*#C#)";
   test "Full expression and value into comparison" []
      "(A ^= #2*B#) * (C ^= AggSum([], R(D))) * (A < C)"
      "(C ^= AggSum([], R(D))) * (2*B < C)"