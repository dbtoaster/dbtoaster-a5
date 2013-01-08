open Type
open Ring
open Arithmetic
open Calculus
open UnitTest
;;
Debug.activate "PARSE-CALC-WITH-FLOAT-VARS";
Debug.activate "AGGRESSIVE-FACTORIZE";
Debug.activate "AGGRESSIVE-UNIFICATION";
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
      "(R(A,B) * {-1}) * ({22-1} + {3}) * {A}"
      "R(A,B)* {-24} * A";
   test "Vars across relations"
      "R(A) * S(B) * A * B"
      "R(A) * S(B) * A * B";
   test "Vars already combined"
      "R(A) * S(B) * {A+B} * A * B"
      "R(A) * S(B) * {(A+B)* A * B}";
   test "Self-comparison"
      "R(A) * {A = A}"
      "R(A)";
(* This optimization temporarilly disabled, until we figure out what to do about
   the change in schema
   test "Self-anticomparison"
      "R(A) * ({A != A} + {A < A})"
      "0" *)
;;

let test msg scope input output =
   log_test ("Lift Equalities ("^msg^")")
      string_of_expr
      (CalculusTransforms.lift_equalities scope (parse_calc input))
      (parse_calc output)
in 
   test "Empty Scope, Unliftable" [] 
      "R(A,B) * {A = B}"
      "R(A,B) * {A = B}";
   test "Empty Scope, Liftable" []
      "R(A,B) * S(B,C) * {A = C}"
      "R(A,B) * (C ^= A) * S(B,C)";
   test "Liftable due to scope" [var "B"]
      "R(A,B) * {A = B}"
      "(A ^= B) * R(A,B)";
   test "Unliftable due to scope" [var "A"; var "B"]
      "R(A,B) * {A = B}"
      "{A = B} * R(A,B)";
   test "Liftable due to nested scope" []
      "S(B) * AggSum([], R(A,B) * {A = B})"
      "S(B) * AggSum([], (A ^= B) * R(A,B))";
   test "Unliftable due to nested scope" []
      "S(A, B) * AggSum([], R(A,B) * {A = B})"
      "S(A, B) * {A = B} * R(A,B)";
   test "TPCH17" [var "dPK"]
      "LI(PK, QTY) * (nested ^= AggSum([PK],((LI(PK, QTY2) * QTY2)))) * 
         {PK = dPK} * {QTY < (0.5 * nested)}"
      "(PK ^= dPK) * LI(PK, QTY) * 
         (nested ^= AggSum([PK],((LI(PK, QTY2) * QTY2)))) * 
         {QTY < (0.5 * nested)}"
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
      "R(A) * (B ^= AggSum([], R(C) * {C < A}))"
      "R(A) * (B ^= AggSum([], R(C) * {C < A}))";
   test "Schema-Permitted with Nested Agg" [var "A"]
      "R(A) * (B ^= AggSum([], R(C) * {C < A}))"
      "(B ^= AggSum([], R(C) * {C < A})) * R(A)";
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
      "(A ^= AggSum([C],R(B,C)*B)) * {2*A}"
      "(A ^= AggSum([C],R(B,C)*B)) * {2*A}";
   test "Empty schema, Full expression into relation" []
      "(A ^= AggSum([C],R(B,C)*B)) * R(A)"
      "(A ^= AggSum([C],R(B,C)*B)) * R(A)";
   test "Empty schema, Full expression into external" []
      "(A ^= AggSum([C],R(B,C)*B)) * E[A][]"
      "(A ^= AggSum([C],R(B,C)*B)) * E[A][]";
   test "Empty schema, Full expression into all" []
      "(A ^= AggSum([C],R(B,C)*B)) * A * {2*A} * R(A)"
      "(A ^= AggSum([C],R(B,C)*B)) * A * {2*A} * R(A)";
   test "Empty schema, Value into variable" []
      "(A ^= {2*B}) * A"
      "{2*B}";
   test "Empty schema, Value into value" []
      "(A ^= {2*B}) * {2*A}"
      "{2*2*B}";
   test "Empty schema, Value into relation" []
      "(A ^= {2*B}) * R(A)"
      "(A ^= {2*B}) * R(A)";
   test "Empty schema, Value into external" []
      "(A ^= {2*B}) * E[A][]"
      "(A ^= {2*B}) * E[A][]";
   test "Empty schema, Value into all" []
      "(A ^= {2*B}) * A * {2*A} * R(A)"
      "(A ^= {2*B}) * {2*B} * {2*2*B} * R(A)";
   test "Empty schema, Variable into variable" []
      "(A ^= B) * A"
      "B";
   test "Empty schema, Variable into value" []
      "(A ^= B) * {2*A}"
      "{2*B}";
   test "Empty schema, Variable into relation" []
      "(A ^= B) * R(A)"
      "R(B)";
   test "Empty schema, Variable into external" []
      "(A ^= B) * E[A][]"
      "E[B][]";
   test "Empty schema, Variable into all" []
      "(A ^= B) * A * {2*A} * R(A)"
      "B * {2*B} * R(B)";
   test "Variable into all made invalid by schema" [var "A"]
      "(A ^= B) * A * {2*A} * R(A)"
      "(A ^= B) * B * {2*B} * R(B)";
   test "Cascading pair with nesting" []
      "(A ^= B) * AggSum([], (C ^= {A*2})*C)"
      "{2*B}";
   test "Cascading pair made partly invalid by nested schema" [var "C"]
      "(A ^= B) * AggSum([C], (C ^= {A*2})*C)"
      "(C ^= {2*B})*{2*B}";
   test "Full expression and value into comparison" []
      "(A ^= {2*B}) * (C ^= AggSum([], R(D))) * {A < C}"
      "(C ^= AggSum([], R(D))) * {2*B < C}";
   test "Dropping unnecessary lifts 1" []
      "AggSum([], (A ^= B))"
      "1";
   test "Dropping unnecessary lifts 2" []
      "AggSum([], (A ^= B)*(C ^= D))"
      "1";
   test "Dropping some unnecessary lifts 2" []
      "AggSum([], (A ^= AA)*(B ^= BB)*S(B))"
      "S(BB)"; (* BB goes from being an ivar to being an ovar *)
   test "Lifts in non-normal form" []
      "AggSum([], S(B)*(B ^= BB))"
      "AggSum([], S(B)*(B ^= BB))"; (* This should get shifted left by 
                                       nesting_rewrites *)
   test "Duplicate lifts" []
      "(A ^= 0) * (A ^= AggSum([], S(B)))"
      "(A ^= 0) * (A ^= AggSum([], S(B)))";
   test "Duplicate lifts (not terminal)" []
      "(A ^= 0) * (A ^= AggSum([], S(B))) * 2"
      "(A ^= 0) * (A ^= AggSum([], S(B))) * 2";
   test "Query18 funny business" [var "query18_mLINEITEMLINEITEM_ORDERKEY"; 
                                  var "query18_mLINEITEMLINEITEM_QUANTITY"]
      "(O_OK ^= dOK) * AggSum([O_OK],(
          (AggSum([O_OK],(LINEITEM(O_OK, L3_Q)))) +
          ( (O_OK ^= dOK) * AggSum([O_OK],(LINEITEM(O_OK, L3_Q)))
       )))"
      "(AggSum([dOK],(LINEITEM(dOK, L3_Q)))) +
       (AggSum([dOK],(LINEITEM(dOK, L3_Q))))";
   test "Double lifts into an equality" [var "B"; var "C"]
      "AggSum([],(A ^= B) * (A ^= C))"
      "{C = B}";
   test "TPCH3 lifts" [var "CUSTOMER_MKTSEGMENT"]
      "AggSum([],((CUSTOMER_MKTSEGMENT_1 ^= CUSTOMER_MKTSEGMENT) * 
                  (CUSTOMER_MKTSEGMENT_1 ^= 'BUILDING')))"
      "{'BUILDING' = CUSTOMER_MKTSEGMENT}";
   test "Lift into a lifted aggsum of a lift" [var "C"]
      "(A ^= B) * (C ^= (AggSum([A], R(D) * (A ^= D))))"
      "(C ^= (AggSum([], R(D) * {D = B})))";
   test "TPCH11 nested query" [var "P_NATIONKEY"; var "N_VALUE"]
      "(N_NATIONKEY ^= P_NATIONKEY) * 
       (N_VALUE ^= AggSum([N_NATIONKEY],(
           PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, 
                    PS_COMMENT) * 
           (S_SUPPKEY ^= PS_SUPPKEY) * 
           SUPPLIER(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                    S_ACCTBAL, S_COMMENT) * 
           (N_NATIONKEY ^= S_NATIONKEY) * 
           PS_SUPPLYCOST * PS_AVAILQTY
      )))"
      "(N_VALUE ^= AggSum([],(
           PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, 
                    PS_COMMENT) * 
           SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, 
                    S_ACCTBAL, S_COMMENT) * 
           {S_NATIONKEY = P_NATIONKEY} * 
           PS_SUPPLYCOST * PS_AVAILQTY
      )))";
   test "Materialized terminal lift" [var "B"]
      "AggSum([A], (M1_L1_1(int)[][A,B] * (E ^= M1_L1_1(int)[][] * B)))"
      "AggSum([A], M1_L1_1(int)[][A,B])";
   test "Aggregation over a non-agg lift" [var "A"]
      "AggSum([A], R(A,B) * (E ^= R(C,D) * B))"
      "AggSum([A], R(A,B) * (E ^= R(C,D) * B))";
   test "Lift in a sum, ivar on RHS" []
      "(R(A) + (A ^= B)) * {A < 3}"
      "(R(A) + (A ^= B)) * {A < 3}";
   test "Lift in a sum, ovar on RHS" []
      "(R(A) + (A ^= B)) * S(A)"
      "(R(A) + (A ^= B)) * S(A)";
   test ~scope:[var "A"; var "B"]  
        "Empty schema, Lift order" [] 
   "(BETA ^= {B = 'FRANCE' }) * (ALPHA ^= (A + 1)) * -1 * ALPHA * BETA"
   "(-1 * ((A + 1)) * ({B = 'FRANCE'}))";
   test "Relation with duplicate entries" []
      "(B ^= dA) * R(dA,B)"
      "R(dA,B) * {B = dA}"   

;;
let test msg input output =
   log_test ("Nesting Rewrites ("^msg^")")
      string_of_expr
      (CalculusTransforms.nesting_rewrites (parse_calc input))
      (parse_calc output)
in
   test "Lift of two Lifts"
      "(A ^= (B ^= (C ^= D)))"
      "(A ^= (B ^= (C ^= D)))";
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
      "{A = A}";
   test "A Lifted More Complex Value"
      "(A ^= {A*B*C})"
      "{A = (A*B*C)}";
   test "Aggsum of a lifted value - schema changed"
      "AggSum([A], (A ^= B))"
      "(A ^= B)";
   test "Aggsum of a lifted value - schema unchanged"
      "AggSum([A], (A ^= B) * R(A,C))"
      "(A ^= B) * AggSum([A], R(A,C))";
;;
let test msg scope input output =
   log_test ("Factorize One Polynomial ("^msg^")")
      string_of_expr
      (CalculusTransforms.factorize_one_polynomial 
          scope (List.map parse_calc input))
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
   log_test ("Term cancelation ("^msg^")")
      string_of_expr
      (CalcRing.mk_sum (
          CalculusTransforms.cancel_term_list (List.map parse_calc input)))
      (parse_calc output)
in
(*   Debug.activate "LOG-CANCEL";*)
   test "Basic" []
     ["A"; "-A"]
     "0";
   test "Multiple terms" []
     ["A"; "-A"; "A"]
     "A";
   test "Complex terms" []
     ["A*B"; "-A"; "-A*B"]
     "(-1 * A)";
(*   test "Complex terms different order" []*)
(*     ["A*B"; "-A"; "-B*A"]                *)
(*     "(-1 * A)";                          *)
   test "Multiple expressions" []
     ["A"; "-B"; "B"; "A"]
     "A + A";
   test "Multiple expression cancelation" []
     ["A"; "-B"; "B"; "-A"]
     "0";  
   test "PriceSpread" []
     ["(-1 * ( 
         AggSum([],
            (((__sql_inline_agg_2 ^= (PSP_mBIDS1_L1_1(float)[][] * 0.0001)) * 
              PSP_mBIDS4(float)[][B_VOLUME] * 
              {B_VOLUME > __sql_inline_agg_2}))) * 
         AggSum([],
            (((__sql_inline_agg_1 ^= (PSP_mBIDS2_L1_1(float)[][] * 0.0001)) * 
            {ASKS_VOLUME > __sql_inline_agg_1})))))";
      "(AggSum([],
           (((__sql_inline_agg_2 ^= (PSP_mBIDS1_L1_1(float)[][] * 0.0001)) * 
             PSP_mBIDS4(float)[][B_VOLUME] * 
             {B_VOLUME > __sql_inline_agg_2}))) *
        AggSum([],
           (((__sql_inline_agg_1 ^= (PSP_mBIDS2_L1_1(float)[][] * 0.0001)) * 
             {ASKS_VOLUME > __sql_inline_agg_1}))))" ]
     "0";
;;

let test msg scope input output =
   log_test ("Factorize Sums ("^msg^")")
      CalculusPrinter.string_of_expr
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
      "(R(A) * (A * (2+({-1}*D)))+C)+({-1} * S(A) * A)";
;;
let test ?(opts = CalculusTransforms.default_optimizations)
         ?(skip_opts = [])
         msg scope schema input output =
   log_test ("End-to-end ("^msg^")")
      CalculusPrinter.string_of_expr
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
          AggSum([], ({PPK = LPK})) *
          AggSum([], (
             (nested ^= (0.5 * AggSum([], LI(LPK2, LQTY2) * 
                 AggSum([], {LPK2 = PPK}) * LQTY2))) *
             {LQTY < nested}
          ))
       ))"
      "AggSum([], (
          P(PPK) * LI(PPK, LQTY) * 
          AggSum([PPK], (
             (nested ^= AggSum([PPK], LI(PPK, LQTY2) * LQTY2) * 0.5) *
             {LQTY < nested}
          ))
       ))";
   test "TPCH17 full raw" [] []
      "AggSum([],(
          ( LINEITEM(L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE) * 
            PART(P_PARTKEY) * 
            AggSum([],({P_PARTKEY = L_PARTKEY})) * 
            AggSum([],(
               (  (__sql_agg_tmp_1 ^= (0.005 * AggSum([],(
                      (LINEITEM(L2_PARTKEY, L2_QUANTITY, L2_EXTENDEDPRICE) * 
                      AggSum([],({L2_PARTKEY = P_PARTKEY})) * 
                      L2_QUANTITY
                  ))))) * 
                  {L_QUANTITY < __sql_agg_tmp_1}
               )
            )) * 
            L_EXTENDEDPRICE
         )
      ))"
      "AggSum([],(
          ( LINEITEM(L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE) * 
            PART(L_PARTKEY) * 
            AggSum([L_PARTKEY],(
               (  (__sql_agg_tmp_1 ^= (AggSum([L_PARTKEY],(
                      (LINEITEM(L_PARTKEY, L2_QUANTITY, L2_EXTENDEDPRICE) * 
                      L2_QUANTITY
                  )))) * 0.005 ) * 
                  {L_QUANTITY < __sql_agg_tmp_1}
               )
            )) * 
            L_EXTENDEDPRICE
         )
      ))";
   test "TPCH17 dPart Tricky Term" ["dPK"] []
      "(PK ^= dPK) * LI(PK, QTY) * (alpha ^= 0) * (
          (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2) + alpha)) -
          (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2)))
       ) * {QTY < 0.5 * nested}"
      "0";
   test "TPCH17 dPart Full" ["dPK"] [] 
      "( ((PK ^= dPK) * LI(PK, QTY) *
          (nested ^= (AggSum([PK], LI(PK,QTY2) * QTY2))))
         +
         (P(PK) * LI(PK, QTY) * (alpha ^= 0) * (
             (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2) + alpha)) -
             (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2)))
         ))
         +
         ((PK ^= dPK) * LI(PK, QTY) * (alpha ^= 0) * (
             (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2) + alpha)) -
             (nested ^= (AggSum([PK], LI(PK, QTY2) * QTY2)))
         ))
       ) * {QTY < 0.5 * nested}"
      "(nested ^= (AggSum([dPK], LI(dPK,QTY2) * QTY2))) * LI(dPK,QTY) *
       {QTY < 0.5 * nested}";
   Debug.activate "NO-VISUAL-DIFF";
   test "TPCH17 dLineitem" ["dPK"; "dQTY"] ["QTY"; "nested"]
      "P(PK) * 
       (((PK ^= dPK) * (QTY ^= dQTY) * 
         (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))
        + 
        ((L(PK, QTY) + (PK ^= dPK) * (QTY ^= dQTY)) * (PK ^= dPK) *
         (
             (nested ^= AggSum([PK], L(PK, QTY2) * QTY2) + 
                        (AggSum([],((QTY2 ^= dQTY) * QTY2)))) -
             (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))
         )))"
      "P(dPK) * (
          (L(dPK, QTY) * (
            (nested ^= AggSum([dPK], L(dPK, QTY2) * QTY2) + dQTY) +
            (nested ^= AggSum([dPK], L(dPK, QTY2) * QTY2)) * {-1}
          )) +
          ((QTY ^= dQTY) * 
            (nested ^= AggSum([dPK], L(dPK, QTY2) * QTY2) + dQTY)
          )
       )";
   test "SumNestedInTarget Delta" ["dA"; "dB"] []
      "AggSum([], 
          (((R1_A ^= dA) *
            (R1_B ^= dB) * AggSum([R1_A], R(R1_A, R2_B))) +
           ((R(R1_A, R1_B) +
             ((R1_A ^= dA) * (R1_B ^= dB))) *
            AggSum([R1_A], 
               ((R1_A ^= dA) * (R2_B ^= dB))))))"
      "AggSum([dA], R(dA, R2_B)) + AggSum([dA], R(dA, R1_B)) + 1";
   test "Employee37 dEmployee dLineitem" ["dLID"; "dRG"] ["COUNT_DID"]
      "AggSum([COUNT_DID], 
          (AggSum([__sql_inline_agg_1, D_LOCATION_ID], 
              (L_REGIONAL_GROUP ^= 'CHICAGO') * (D_LOCATION_ID ^= dLID) *
              (L_REGIONAL_GROUP ^= dRG) *
              ((__sql_inline_agg_1 ^=
                   (AggSum([D_LOCATION_ID], 
                       ((L_REGIONAL_GROUP ^= 'CHICAGO') *
                        LOCATION(D_LOCATION_ID, L_REGIONAL_GROUP))) +
                    AggSum([], 1))) +
               (-1 *
                (__sql_inline_agg_1 ^=
                    AggSum([D_LOCATION_ID], 
                       ((L_REGIONAL_GROUP ^= 'CHICAGO') *
                        LOCATION(D_LOCATION_ID, L_REGIONAL_GROUP)))))) *
              DEPARTMENT(COUNT_DID, D_NAME, D_LOCATION_ID) * 
              {__sql_inline_agg_1 > 0})))"
      "({dRG = 'CHICAGO'} *
        AggSum([], 
           (((__sql_inline_agg_1 ^=
                 (AggSum([dLID], 
                     ((L_REGIONAL_GROUP ^= 'CHICAGO') *
                      LOCATION(dLID, L_REGIONAL_GROUP))) +
                  1)) +
             ((__sql_inline_agg_1 ^=
                  AggSum([dLID], 
                     ((L_REGIONAL_GROUP ^= 'CHICAGO') *
                      LOCATION(dLID, L_REGIONAL_GROUP)))) *
                     {-1})) *
            {__sql_inline_agg_1 > 0} *
            AggSum([dLID], DEPARTMENT(COUNT_DID, D_NAME, dLID)))))";
   test "Zeus Query 96434723 dS" ["COUNT_mSSB"; "COUNT_mSSC"] 
                                 ["B1"; "C1"; "LB71Y"; "GKKEOF"; "QKKF7PYI"]
      "(AggSum([B1, C1, LB71Y, GKKEOF], 
           (COUNTS1(int)[][NPZL_7DKV_B,NPZL_7DKV_C] * 
            (__sql_inline_not_1 ^= 0) *
            (NPZL_7DKV_B ^= 0) *
            (LB71Y ^= ((COUNTSS_B * COUNTSS_C) + NPZL_7DKV_B)) *
            (B1 ^= NPZL_7DKV_B) * (__sql_inline_not_1 ^= {NPZL_7DKV_C = 2}) *
            (GKKEOF ^= (NPZL_7DKV_C * {(NPZL_7DKV_B * NPZL_7DKV_B)})) *
            (C1 ^= NPZL_7DKV_C))) *
        (QKKF7PYI ^= COUNTSS_C))"
      "(LB71Y ^= (COUNTSS_B * COUNTSS_C)) * (B1 ^= 0) * (GKKEOF ^= 0) *
        (QKKF7PYI ^= COUNTSS_C) *
        AggSum([C1], 
           (__sql_inline_not_1 ^= 0) * (NPZL_7DKV_B ^= 0) *
           COUNTS1(int)[][NPZL_7DKV_B, NPZL_7DKV_C] *
           (__sql_inline_not_1 ^= {NPZL_7DKV_C = 2}) *
           (C1 ^= NPZL_7DKV_C)
         )";
   test "Factorizing deltas of products of lifts" ["dA"; "dB"] 
                                                  ["A"; "B"; "ALPHA"; "BETA"]
      "( (  (A ^= dA) * (B ^= dB) *
            (  (ALPHA ^= (R(A, B) + 1)) + 
               (-1 * (ALPHA ^= R(A, B)))
            ) *
            (BETA ^= R(A, B))
         ) +
         (  (  (ALPHA ^= R(A, B)) +
               (  (A ^= dA) * (B ^= dB) *
                  (  (ALPHA ^= (R(A, B) + 1)) + 
                     (-1 * (ALPHA ^= R(A, B)))
                  )
               )
            ) *
            (A ^= dA) * (B ^= dB) *
            (  (BETA ^= (R(A, B) + 1)) + 
               (-1 * (BETA ^= R(A, B)))
            )
         )
      )"
      "(A ^= dA) * (B ^= dB) * (
         ((BETA ^= R(dA, dB)) * (ALPHA ^= R(dA,dB)) * {-1}) 
            + 
         ((BETA ^= R(dA, dB) + 1) * (ALPHA ^= R(dA,dB) + 1))
       )";
    test "Consistent handling of output vars in lifts" [] []
        "AggSum([B], (R(B, C) * 
                      AggSum([], (CMPVAR ^= AggSum([], (S(A) * {B = A}))) *
                                 {0 = CMPVAR})
                ))))"
        "AggSum([B], R(B, C) * AggSum([], (CMPVAR ^= S(B)) * (CMPVAR ^= 0)))";
