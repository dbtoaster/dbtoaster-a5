open UnitTest

let tables = [
   "T1", [var "W"; var "X"], Schema.TableRel;
   "T2", [var "Y"; var "Z"], Schema.TableRel;
];;

Debug.activate "NO-VISUAL-DIFF";;
Debug.activate "PARSE-CALC-WITH-FLOAT-VARS";;

let test msg expected expr = 
   log_boolean ("Needs Runtime IVC ( "^msg^" )")
      (IVC.needs_runtime_ivc tables (parse_calc expr))
      expected
in
   test "Relation"       false   "R(A)";
   test "Lift of Const"  false   "(A ^= 1)";
   test "Lift of Rel"    true    "(A ^= R(B))";
   test "Rel and Lift 1" false   "(A ^= 1) * R(A)";
   test "Rel and Lift 2" false   "(A ^= 1) * R(B)";
   test "Rel and Lift 3" false   "(A ^= S(C)) * R(B)";
   Debug.activate "LOG-IVC-TEST";
   test "r_count_of_one" true
      "AggSum([S_C], 
        ((S_C ^= AggSum([S_A], (R(R_A, R_B) * (S_A ^= R_A)))) * (S_A ^= 3)))";
(*   test "R_Multinest"    true
      "(Z___SQL_SUM_AGGREGATE_1 ^=
        AggSum([Z_D], 
          ((Y_D ^=
             AggSum([Y_C], 
               ((X_C ^= AggSum([X_A], (R(R1_A, R1_B) * (X_A ^= R1_A) * R1_B))) *
                 (Y_C ^= X_C) * X_A))) *
            (Z_D ^= Y_D) * Y_C)))";
   test "TPCH11" true
      "AggSum([P_NATIONKEY, P_PARTKEY], 
        ((N_VALUE ^=
           AggSum([P_NATIONKEY], 
             (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST,
                         PS_COMMENT) *
               SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, P_NATIONKEY, S_PHONE,
                          S_ACCTBAL, S_COMMENT) *
               PS_SUPPLYCOST * PS_AVAILQTY))) *
          (P_VALUE ^=
            AggSum([P_PARTKEY, P_NATIONKEY], 
              (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST,
                          PS_COMMENT) *
                (P_PARTKEY ^= PS_PARTKEY) *
                SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                           S_ACCTBAL, S_COMMENT) *
                (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) *
          {P_VALUE > (0.001 * N_VALUE)} * P_VALUE))";
*)

let test msg expr expected =
   log_test ("IVC Derivation ( "^msg^" )")
      (CalculusPrinter.string_of_expr)
      (IVC.derive_initializer tables (parse_calc expr))
      (parse_calc expected)
in
   Debug.activate "LOG-IVC-DERIVATION";
   test "R_Multinest"
      "(Z___SQL_SUM_AGGREGATE_1 ^=
        AggSum([Z_D], 
          ((Y_D ^=
             AggSum([Y_C], 
               ((X_C ^= AggSum([X_A], (R(R1_A, R1_B) * (X_A ^= R1_A) * R1_B))) *
                 (Y_C ^= X_C) * X_A))) *
            (Z_D ^= Y_D) * Y_C)))"
      "0";
      