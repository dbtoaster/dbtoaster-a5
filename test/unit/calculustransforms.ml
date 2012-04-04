open Types
open Ring
open Arithmetic
open Calculus
open UnitTest
;;
log_test "Invalid Commute"
   string_of_bool
   (commutes (parse_calc "R(A,B) * A") (parse_calc "B"))
   false
;;
log_test "Invalid Commute (Made valid by scope)"
   string_of_bool
   (commutes ~scope:[var "B"] (parse_calc "R(A,B) * A") 
                                             (parse_calc "B"))
   true
;;
log_test "Valid Commute"
   string_of_bool
   (commutes (parse_calc "R(A,B) * A") (parse_calc "C"))
   true
;;
let test msg input output =
   log_test ("Combine Values ("^msg^")")
      string_of_expr
      (CalculusTransforms.combine_values (parse_calc input))
      (parse_calc output)
in
   test "Constants and Var"
      "(R(A,B) * [-1]) * ([22-1] + [3]) * [A]"
      "R(A,B)* [-24] * A";
   test "Vars across relations"
      "R(A) * S(B) * A * B"
      "R(A) * S(B) * A * B";
   test "Vars already combined"
      "R(A) * S(B) * [A+B] * A * B"
      "R(A) * S(B) * [(A+B)* A * B]";
   test "Self-comparison"
      "R(A) * [A = A]"
      "R(A)";
   test "Self-anticomparison"
      "R(A) * ([A != A] + [A < A])"
      "0"
;;

let test msg scope input output =
   log_test ("Lift Equalities ("^msg^")")
      string_of_expr
      (CalculusTransforms.lift_equalities scope (parse_calc input))
      (parse_calc output)
in 
   test "Empty Scope, Unliftable" [] 
      "R(A,B) * [A = B]"
      "R(A,B) * [A = B]";
   test "Empty Scope, Liftable" []
      "R(A,B) * S(B,C) * [A = C]"
      "R(A,B) * (C ^= [A]) * S(B,C)";
   test "Liftable due to scope" [var "B"]
      "R(A,B) * [A = B]"
      "(A ^= [B]) * R(A,B)";
   test "Unliftable due to scope" [var "A"; var "B"]
      "R(A,B) * [A = B]"
      "[A = B] * R(A,B)";
   test "Liftable due to nested scope" []
      "S(B) * AggSum([], R(A,B) * [A = B])"
      "S(B) * AggSum([], (A ^= [B]) * R(A,B))";
   test "Unliftable due to nested scope" []
      "S(A, B) * AggSum([], R(A,B) * [A = B])"
      "S(A, B) * AggSum([], [A = B] * R(A,B))";
   test "TPCH17" [var "dPK"]
      "LI(PK, QTY) * (nested ^= AggSum([PK],((LI(PK, QTY2) * [QTY2])))) * 
         [PK = dPK] * [QTY < (0.5 * nested)]"
      "(PK ^= dPK) * LI(PK, QTY) * 
         (nested ^= AggSum([PK],((LI(PK, QTY2) * [QTY2])))) * 
         [QTY < (0.5 * nested)]"
;;

let test msg scope input output =
   log_test ("Advance Lifts ("^msg^")")
      string_of_expr
      (CalculusTransforms.advance_lifts scope (parse_calc input))
      (parse_calc output)
in
   test "Basic Advancement" []
      "C * (A ^= B)"
      "(A ^= B) * C";
   test "Schema-Forbidden" []
      "R(A) * (B ^= A)"
      "R(A) * (B ^= A)";
   test "Schema-Forbidden with Nested Agg" []
      "R(A) * (B ^= AggSum([], R(C) * [C < A]))"
      "R(A) * (B ^= AggSum([], R(C) * [C < A]))";
   test "Schema-Permitted with Nested Agg" [var "A"]
      "R(A) * (B ^= AggSum([], R(C) * [C < A]))"
      "(B ^= AggSum([], R(C) * [C < A])) * R(A)";
;;

let test ?(scope=[]) msg schema input output =
   log_test ("Unify Lifts ("^msg^")")
      string_of_expr
      (CalculusTransforms.unify_lifts scope schema (parse_calc input))
      (parse_calc output)
in
   (* We don't unify expressions for now 
   test "Empty schema, Full expression into variable" []
      "(A ^= AggSum([C],R(B,C)*[B])) * A"
      "AggSum([C],R(B,C)*B)";
   *)
   test "Empty schema, Full expression into value" []
      "(A ^= AggSum([C],R(B,C)*B)) * [2*A]"
      "(A ^= AggSum([C],R(B,C)*B)) * [2*A]";
   test "Empty schema, Full expression into relation" []
      "(A ^= AggSum([C],R(B,C)*B)) * R(A)"
      "(A ^= AggSum([C],R(B,C)*B)) * R(A)";
   test "Empty schema, Full expression into external" []
      "(A ^= AggSum([C],R(B,C)*B)) * E[A][]"
      "(A ^= AggSum([C],R(B,C)*B)) * E[A][]";
   test "Empty schema, Full expression into all" []
      "(A ^= AggSum([C],R(B,C)*B)) * A * [2*A] * R(A)"
      "(A ^= AggSum([C],R(B,C)*B)) * A * [2*A] * R(A)";
   test "Empty schema, Value into variable" []
      "(A ^= [2*B]) * A"
      "[2*B]";
   test "Empty schema, Value into value" []
      "(A ^= [2*B]) * [2*A]"
      "[2*2*B]";
   test "Empty schema, Value into relation" []
      "(A ^= [2*B]) * R(A)"
      "(A ^= [2*B]) * R(A)";
   test "Empty schema, Value into external" []
      "(A ^= [2*B]) * E[A][]"
      "(A ^= [2*B]) * E[A][]";
   test "Empty schema, Value into all" []
      "(A ^= [2*B]) * A * [2*A] * R(A)"
      "(A ^= [2*B]) * A * [2*A] * R(A)";
   test "Empty schema, Variable into variable" []
      "(A ^= B) * A"
      "B";
   test "Empty schema, Variable into value" []
      "(A ^= B) * [2*A]"
      "[2*B]";
   test "Empty schema, Variable into relation" []
      "(A ^= B) * R(A)"
      "R(B)";
   test "Empty schema, Variable into external" []
      "(A ^= B) * E[A][]"
      "E[B][]";
   test "Empty schema, Variable into all" []
      "(A ^= B) * A * [2*A] * R(A)"
      "B * [2*B] * R(B)";
   test "Variable into all made invalid by schema" [var "A"]
      "(A ^= B) * A * [2*A] * R(A)"
      "(A ^= B) * A * [2*A] * R(A)";
   test "Cascading pair with nesting" []
      "(A ^= B) * AggSum([], (C ^= [A*2])*C)"
      "AggSum([], [2*B])";
   test "Cascading pair made partly invalid by nested schema" []
      "(A ^= B) * AggSum([C], (C ^= [A*2])*C)"
      "AggSum([C], (C ^= [2*B])*C)";
   test "Full expression and value into comparison" []
      "(A ^= [2*B]) * (C ^= AggSum([], R(D))) * [A < C]"
      "(C ^= AggSum([], R(D))) * [2*B < C]";
   test "Dropping unnecessary lifts 1" []
      "AggSum([], (A ^= B))"
      "AggSum([], 1)"; (* The aggsum would be deleted by nesting_rewrites *)
   test "Dropping unnecessary lifts 2" []
      "AggSum([], (A ^= B)*(C ^= D))"
      "AggSum([], 1)";
   test "Dropping some unnecessary lifts 2" []
      "AggSum([], (A ^= AA)*(B ^= BB)*S(B))"
      "AggSum([BB], S(BB))";
   test "Lifts in non-normal form" []
      "AggSum([], S(B)*(B ^= BB))"
      "AggSum([], S(B)*(B ^= BB))" (* This should get shifted left by 
                                      nesting_rewrites *)
;;
let test msg input output =
   log_test ("Nesting Rewrites ("^msg^")")
      string_of_expr
      (CalculusTransforms.nesting_rewrites (parse_calc input))
      (parse_calc output)
in
   test "Lift of two Lifts"
      "(A ^= (B ^= (C ^= D)))"
      "(C ^= D) * (B ^= 1) * (A ^= 1)";
   test "AggSum of a partitionable expression"
      "AggSum([B,C], R(A, B) * S(B, C))"
      "S(B,C) * AggSum([B], R(A, B))";
   test "AggSum of a nested AggSum"
      "AggSum([B], AggSum([B,C], R(A,B,C)))"
      "AggSum([B], R(A,B,C))";
   test "AggSum of zero"
      "AggSum([B,C], 0)"
      "0";
   test "A Lifted Value"
      "(A ^= A)"
      "[A = A]";
   test "A Lifted More Complex Value"
      "(A ^= [A*B*C])"
      "[A = (A*B*C)]"
;;
let test msg scope input output =
   log_test ("Factorize One Polynomial ("^msg^")")
      string_of_expr
      (CalculusTransforms.factorize_one_polynomial scope (List.map parse_calc input))
      (parse_calc output)
in
(*   Debug.activate "LOG-FACTORIZE";*)
   test "Basic" []
      ["(A * B)";"(A * C)"]
      "A * (B + C)";
   test "Front and Back" []
      ["(A * R(C) * C)";"(A * S(C) * C)"]
      "A * (R(C) + S(C)) * C";
   test "Multiple Possibilities" []
      ["R(A) * A" ; "R(A) * 2" ; "S(A) * A"]
      "(R(A) * (A + 2)) + (S(A) * A)";
   test "Multiple Decreasing Possibilities" []
      [  "R(A) * A * 2" ; "R(A) * C" ; 
         "R(A) * A * D" ; "S(A) * A"]
      "(R(A) * ((A * (2 + D)) + C)) + (S(A) * A)"
;;
let test msg scope input output =
   log_test ("Factorize Sums ("^msg^")")
      string_of_expr
      (CalculusTransforms.factorize_polynomial scope (parse_calc input))
      (parse_calc output)
in
(*   Debug.activate "LOG-FACTORIZE";*)
   test "Basic" []
      "(A * B)+(A * C)"
      "A * (B + C)";
   test "Front and Back" []
      "(A * R(C) * C)+(A * S(C) * C)"
      "A * (R(C) + S(C)) * C";
   test "Multiple Possibilities" []
      "(R(A) * A)+(R(A) * 2)+(S(A) * A)"
      "(R(A) * (A + 2)) + (S(A) * A)";
   test "Multiple Decreasing Possibilities" []
      "(R(A) * A * 2)+(R(A) * C)+(R(A) * A * D)+(S(A) * A)"
      "(R(A) * ((A * (2 + D)) + C)) + (S(A) * A)";
   test "Multiple Decreasing With Negs" []
      "(R(A) * A * 2)+(R(A) * C)-((R(A) * A * D)+(S(A) * A))"
      "(R(A) * ((A * 2) + C))-(((R(A) * D)+S(A)) * A)"
;;
let test ?(opts = CalculusTransforms.default_optimizations)
         ?(skip_opts = [])
         msg scope schema input output =
   log_test ("End-to-end ("^msg^")")
      string_of_expr
      (CalculusTransforms.optimize_expr 
         ~optimizations:(ListAsSet.diff opts skip_opts)
         (List.map var scope, List.map var schema) 
         (parse_calc input)
      )
      (parse_calc output)
in
   test "TPCH17 simple raw" [] []
      "AggSum([], (
         P(PPK) * LI(LPK, LQTY) * 
         AggSum([], ([PPK = LPK])) *
         AggSum([], (
            (nested ^= (0.5 * AggSum([], LI(LPK2, LQTY2) * 
               AggSum([], [LPK2 = PPK]) * [LQTY2]))) *
            [LQTY < nested]
         ))
      ))"
      "AggSum([], (
         P(PPK) * LI(PPK, LQTY) * 
         AggSum([PPK], (
            (nested ^= AggSum([PPK], LI(PPK, LQTY2) * [LQTY2]) * 
               0.5) *
            [LQTY < nested]
         ))
      ))";
   test "TPCH17 full raw" [] []
      "AggSum([],(
         (  LINEITEM(L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE) * 
            PART(P_PARTKEY) * 
            AggSum([],([P_PARTKEY = L_PARTKEY])) * 
            AggSum([],(
               (  (__sql_agg_tmp_1 ^= ([0.005] * AggSum([],(
                     (LINEITEM(L2_PARTKEY, L2_QUANTITY, L2_EXTENDEDPRICE) * 
                     AggSum([],([L2_PARTKEY = P_PARTKEY])) * 
                     [L2_QUANTITY]
                  ))))) * 
                  [L_QUANTITY < __sql_agg_tmp_1]
               )
            )) * 
            [L_EXTENDEDPRICE]
         )
      ))"
      "AggSum([],(
         (  LINEITEM(L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE) * 
            PART(L_PARTKEY) * 
            AggSum([L_PARTKEY],(
               (  (__sql_agg_tmp_1 ^= (AggSum([L_PARTKEY],(
                     (LINEITEM(L_PARTKEY, L2_QUANTITY, L2_EXTENDEDPRICE) * 
                     [L2_QUANTITY]
                  )))) * [0.005] ) * 
                  [L_QUANTITY < __sql_agg_tmp_1]
               )
            )) * 
            [L_EXTENDEDPRICE]
         )
      ))";
   test "TPCH17 dPart Tricky Term" ["dPK"] []
      "(PK ^= dPK) * LI(PK, QTY) * (alpha ^= 0) * (
         (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2) + alpha))
         - (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2)))
       ) * [QTY < 0.5 * nested]"
      "0";
   test "TPCH17 dPart Full" ["dPK"] [] 
      "( ((PK ^= dPK) * LI(PK, QTY) *
            (nested ^= (AggSum([PK], LI(PK,QTY2) * QTY2))))
         +
         (P(PK) * LI(PK, QTY) * (alpha ^= 0) * (
            (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2) + alpha))
            - (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2)))
         ))
         +
         ((PK ^= dPK) * LI(PK, QTY) * (alpha ^= 0) * (
            (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2) + alpha))
            - (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2)))
         ))
      ) * [QTY < 0.5 * nested]"
      "(nested ^= (AggSum([dPK], LI(dPK,QTY2) * QTY2))) * LI(dPK,QTY) *
         [QTY < 0.5 * nested]";
   test "TPCH17 dLineitem" ["dPK"; "dQTY"] []
      "P(PK) * (((PK ^= dPK) * (QTY ^= dQTY) * 
                  (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))
            + 
            ((L(PK, QTY) + (PK ^= dPK) * (QTY ^= dQTY)) * 
            (delta_2 ^= (AggSum([PK],(
               ((PK ^= [dPK]) * (QTY2 ^= [dQTY]) * [QTY2]))))) * 
            (
               (nested ^= AggSum([PK], L(PK, QTY2) * QTY2) + delta_2)
                - (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))
            )))"
      "P(dPK) * (
         ((QTY ^= dQTY) * (nested ^= AggSum([dPK], L(dPK, QTY2) * QTY2)))
            + 
            ((L(dPK, QTY) + (QTY ^= dQTY)) * 
            (
               (nested ^= AggSum([dPK], L(dPK, QTY2) * QTY2) + dQTY) +
               (nested ^= AggSum([dPK], L(dPK, QTY2) * QTY2)) * [-1]
            )))"