open Type
open Arithmetic
open Calculus
open Calculus.CalcRing
open UnitTest


let test_expr = 
   CalcRing.mk_prod [
      Calculus.mk_value (Arithmetic.mk_int 1);
      Calculus.mk_rel "R" (List.map var ["a";"b"])
   ]
;;

let test msg op ret = 
   log_test ("Variable comparisons ( "^msg^" )")
      (function true -> "TRUE" | false -> "FALSE")
      op
      ret
in
   test "Equality with same type"
      (("A",TInt) = ("A",TInt))  true;
   (* test "Equality with different type"
      (("A",TInt) = ("A",TFloat)) true; *)
   test "Nonequality with same type"
      (("A",TInt) <> ("A",TInt)) false;
   (* test "Nonequality with different type"
      (("A",TInt) <> ("A",TFloat)) false; *)
   test "List.mem"
      (List.mem ("A",TFloat) [var "A"; var "B"; var "C"]) true
;;

log_test "Stringification"
   (fun x -> x)
   (string_of_expr test_expr)
   ("R(a, b)")
;;

log_test "Parsing"
   string_of_expr
   (parse_calc "R(A,B) * S(B,C) * {A+C} * {A < C}")
   (CalcRing.mk_prod [
      Calculus.mk_rel "R" (List.map var ["A"; "B"]);
      Calculus.mk_rel "S" (List.map var ["B"; "C"]);
      Calculus.mk_value (
         ValueRing.mk_sum [
            Arithmetic.mk_var (var "A");
            Arithmetic.mk_var (var "C")
         ]
      );
      Calculus.mk_cmp Lt (Arithmetic.mk_var (var "A"))
                         (Arithmetic.mk_var (var "C"))
   ])
;;

let test_delta (name:string) ins reln relv expr delta =
   log_test ("Deltas ("^name^")")
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
;;

let test (name:string) expr ivar ovar =
   log_test ("Schemas ("^name^")")
      (fun (iv,ov) -> (ListExtras.ocaml_of_list fst iv)^
                      (ListExtras.ocaml_of_list fst ov))
      (schema_of_expr (parse_calc expr))
      ((List.map var ivar), (List.map var ovar))
in 
   test "TPCH18 simplified funny business"
      "( ( AggSum([],(
         ( (delta_143 ^= 
             (
               {-1} * 
               query18_mLINEITEMLINEITEM_QUANTITY
             )
           ) * 
           ( (__sql_agg_tmp_1 ^= 
               ( AggSum([O_ORDERKEY],(
                   ( LINEITEM(O_ORDERKEY, L3_QUANTITY) * 
                     L3_QUANTITY
                   )
                 )) + 
                 delta_143
               )
             ) + 
             ( (__sql_agg_tmp_1 ^= 
                 AggSum([O_ORDERKEY],(
                   ( LINEITEM(O_ORDERKEY, L3_QUANTITY) * 
                     L3_QUANTITY
                   )
                 ))
               ) * 
               {-1}
             )
           ) * 
           {100 < __sql_agg_tmp_1}
         )
       )) * 
       AggSum([O_ORDERKEY],(
         LINEITEM(O_ORDERKEY, L2_QUANTITY)
       ))
      ))"
      ["O_ORDERKEY"; "query18_mLINEITEMLINEITEM_QUANTITY"] [];
   test "TCPH 16" "AggSum([], 
        (AggSum([SUPPLIER_CNT_pPARTPART_SIZE, SUPPLIER_CNT_pPARTPART_TYPE,
                   SUPPLIER_CNT_pPARTPART_BRAND], 
           EXISTS(
             ( SUPPLIER_CNT_pPART1_E1_1(int)[]
               [PS_SUPPKEY, SUPPLIER_CNT_pPARTPART_BRAND,
                  SUPPLIER_CNT_pPARTPART_TYPE, SUPPLIER_CNT_pPARTPART_SIZE] *
               ({SUPPLIER_CNT_pPARTPART_SIZE = 49} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 14} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 23} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 45} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 19} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 3} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 36} +
                 {SUPPLIER_CNT_pPARTPART_SIZE = 9}) * 
               {0 =
              [regexp_match:int]
                 ('^MEDIUM POLISHED.*$', SUPPLIER_CNT_pPARTPART_TYPE)} *
               {SUPPLIER_CNT_pPARTPART_BRAND != 'Brand#45'} 
              ))) *
    -1))" [] [];
;;


let test_cmp_exprs title ?(cmp_opts = default_cmp_opts) input1 input2 expected =
  let e1 = parse_calc input1 in
  let e2 = parse_calc input2 in
  log_test ("Compare expressions - ("^title^")")
    (string_of_bool)
    ((Calculus.cmp_exprs ~cmp_opts:cmp_opts e1 e2) <> None)
    (expected)
in
  test_cmp_exprs "Case #1" "R(A,B)" "R(A,B)" true;
  test_cmp_exprs "Case #2" "R(A,B)" "S(A,B)" false;

  test_cmp_exprs "Sum case #1" 
                 "R(A,B) + S(C,D)" "S(C,D) + R(A,B)" true;
  test_cmp_exprs "Sum case #1-1" ~cmp_opts:[] 
                 "R(A,B) + S(C,D)" "S(C,D) + R(A,B)" false;
  test_cmp_exprs "Sum case #2" 
                 "R(A,B) + S(C,D)" "S(C,D) + R(A,B)" true;
  test_cmp_exprs "Sum case #2-1" ~cmp_opts:[] 
                 "R(A,B) + S(C,D)" "S(C,D) + R(A,B)" false;
  test_cmp_exprs "Sum case #3" 
                 "R(A,B) + S(C,D) + T(E)" "T(E,F) + S(C,D) + R(A,B)" false;
  test_cmp_exprs "Sum case #4" 
                 "R(A,B) + S(C,D) + T(E)" "T(E) + R(A,B) + S(C,D)" true;
  test_cmp_exprs "Sum case #5-1" ~cmp_opts:[OptProdOrderIndependent] 
                 "R(A,B) + S(C,D) + T(E)" "T(E) + R(A,B) + S(C,D)" false;

  test_cmp_exprs "Prod case #1" 
                 "R(A,B) * S(C,D)" "S(C,D) * R(A,B)" true;
  test_cmp_exprs "Prod case #1-1" ~cmp_opts:[OptSumOrderIndependent] 
                 "R(A,B) * S(C,D)" "S(C,D) * R(A,B)" false;
  test_cmp_exprs "Prod case #2" 
                 "R(A,B) * {A > 5}" "{A > 5} * R(A,B)" true;
  test_cmp_exprs "Prod case #2-1" ~cmp_opts:[] 
                 "R(A,B) * {A > 5}" "{A > 5} * R(A,B)" false;
  test_cmp_exprs "Prod case #3" 
                 "R(A,B) * S(A,D) * {A > 5}" "R(A,B) * {A > 5} * S(A,D)" true;
  test_cmp_exprs "Prod case #3-1" ~cmp_opts:[OptSumOrderIndependent] 
                 "R(A,B) * S(A,D) * {A > 5}" "R(A,B) * {A > 5} * S(A,D)" false;
  test_cmp_exprs "Prod case #4" 
                 "R(A) * (S(B) * T(C))" "R(A) * S(B) * T(C)" true;
  test_cmp_exprs "Prod case #4-1" ~cmp_opts:[] 
                 "R(A) * (S(B) * T(C))" "R(A) * S(B) * T(C)" true;
  test_cmp_exprs "Prod case #5" 
                 "R(A) * (T(B) * S(C))" "R(A) * S(B) * T(C)" true;
  test_cmp_exprs "Prod case #5-1" ~cmp_opts:[OptSumOrderIndependent] 
                 "R(A) * (T(B) * S(C))" "R(A) * S(B) * T(C)" false;

  test_cmp_exprs "Prod case #5" 
                  ("AggSum([A,B],(ALPHA ^= AggSum([A],R(A,C))) * " ^ 
                   "(ALPHA ^= {0}) * S(A,B) * {B})")
                  ("AggSum([A,B],((ALPHA ^= {0}) * " ^ 
                   "(ALPHA ^= AggSum([A],R(A,C)))) * S(A,B) * {B})") true;
  test_cmp_exprs "Prod case #5-1" ~cmp_opts:[OptSumOrderIndependent] 
                 ("AggSum([A,B],(ALPHA ^= AggSum([A],R(A,C))) * " ^ 
                  "(ALPHA ^= {0}) * S(A,B) * {B})")
                 ("AggSum([A,B],((ALPHA ^= {0}) * " ^ 
                  "(ALPHA ^= AggSum([A],R(A,C)))) * S(A,B) * {B})") false;

  test_cmp_exprs "Sum/Prod case #1" 
                 "R(A) * (S(B) + T(C))" "(T(C) + S(B)) * R(A)" true;
  test_cmp_exprs "Sum/Prod case #1-1" ~cmp_opts:[] 
                 "R(A) * (S(B) + T(C))" "(T(C) + S(B)) * R(A)" false;
  test_cmp_exprs "Sum/Prod case #1-2" ~cmp_opts:[OptSumOrderIndependent] 
                 "R(A) * (S(B) + T(C))" "(T(C) + S(B)) * R(A)" false;
  test_cmp_exprs "Sum/Prod case #1-3" ~cmp_opts:[OptProdOrderIndependent] 
                 "R(A) * (S(B) + T(C))" "(T(C) + S(B)) * R(A)" false;

  test_cmp_exprs "Scope case #1"
                 "EXISTS(AggSum([A],R(A,B))) * R(A,B)"
                 "EXISTS(AggSum([E],R(E,C))) * R(E,D)" true;                 
  test_cmp_exprs "Scope case #2 with input vars"
                 "AggSum([A],R(A,B) * {F > 0}) * R(A,B) * {F = B}"
                 "AggSum([E],R(E,C) * {G > 0}) * R(E,D) * {G = D}" true;
  test_cmp_exprs "Scope case #3 with input vars"
                 "AggSum([A],R(A,B) * {F > 0}) * R(A,B) * {F = B}"
                 "AggSum([E],R(E,C) * {G > 0}) * R(E,D) * {F = D}" false;

  test_cmp_exprs "Repeated sum terms #1" ~cmp_opts:[]
                 "A + A" "B + B" true;
  test_cmp_exprs "Repeated sum terms #2" ~cmp_opts:[OptSumOrderIndependent]
                 "A + A" "B + B" true;                 
  test_cmp_exprs "Repeated prod terms #1" ~cmp_opts:[]
                 "A * A" "B * B" true;
  test_cmp_exprs "Repeated prod terms #2" ~cmp_opts:[OptProdOrderIndependent]
                 "A * A" "B * B" true;               

  test_cmp_exprs "Problem 1" ~cmp_opts:[]
          "((AggSum([COUNT_pRR_A],(R(COUNT_pRR_A, R2_B))) * (1 + 1)) + 1)"
          "((AggSum([COUNT_pRR_A],(R(COUNT_pRR_A, R2_B))) * (1 + 1)) + 1)"
          true; 
          
  test_cmp_exprs "Multiple mappings #1" ~cmp_opts:[]
                 "R(A) + R(C)" "R(B) + R(A)" true;
  test_cmp_exprs "Multiple mappings #2" ~cmp_opts:[OptSumOrderIndependent]
                 "R(A) + R(C)" "R(B) + R(A)" true;
  test_cmp_exprs "Multiple mappings #3" ~cmp_opts:[]
                 "R(A) + R(C) + S(A)" "R(A) + R(B) + S(B)" false;
  test_cmp_exprs "Multiple mappings #4" 
                 ~cmp_opts:[OptSumOrderIndependent; OptMultipleMappings]
                 "R(A) + R(C) + S(A)" "R(A) + R(B) + S(B)" true;  
  (* We don't propagate multiple mappings through expression trees,
     so the following equivalence is not detected. *)                 
  (* test_cmp_exprs "Multiple mappings #5" ~cmp_opts:[OptSumOrderIndependent]
                 "(R(A) + R(C)) * S(C)" "(R(B) + R(A)) * S(B)" true; *)
  ()
;;
let test_identical title ?(expected = true) ?(cmp_opts = default_cmp_opts)
                         input1 input2 =
  let e1 = parse_calc input1 in
  let e2 = parse_calc input2 in
  log_boolean ("Expression Identity - ("^title^")")
    (Calculus.exprs_are_identical ~cmp_opts:cmp_opts e1 e2)
    (expected)
in
   test_identical "Same and same"
                  "A * B" "A * B";
   test_identical "Same out of order (opt off)" ~cmp_opts:[] 
                                            ~expected:false
                  "B * A" "A * B";
   test_identical "Same out of order (opt on)" 
                  "B * A" "A * B";
   test_identical "Different, but mappable" ~expected:false
                  "A * B" "A * C";
   test_identical "Comparisons #1"
                  "{S_B = 20}" "{S_B = 20}";
   test_identical "Comparisons #2"
                  "{S_B = (20 + 16)}" "{S_B = (20 + 16)}";
   test_identical "Comparisons #3"
                  "{S_B = (20 + 20)}" "{S_B = (20 + 20)}";
   test_identical "Scope-aware mapping"
                  "AggSum([A], R(A,B))" "AggSum([A], R(A,C))";

   test_identical "Scope-aware mapping 2"
      "DOMAIN(AggSum([O_ORDERKEY], 
          DELTA(LINEITEM(O_ORDERKEY, L2_PARTKEY, L2_SUPPKEY, L2_LINENUMBER,
                  L2_QUANTITY, L2_EXTENDEDPRICE, L2_DISCOUNT, L2_TAX,
                  L2_RETURNFLAG, L2_LINESTATUS, L2_SHIPDATE,
                  L2_COMMITDATE, L2_RECEIPTDATE, L2_SHIPINSTRUCT,
                  L2_SHIPMODE, L2_COMMENT))))"
      "DOMAIN(AggSum([O_ORDERKEY], 
         DELTA(LINEITEM(O_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
                  L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
                  L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,
                  L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT,
                  L_SHIPMODE, L_COMMENT))))";

  test_identical "Simple Zeus 48183500"
    "AggSum([BV0ZRLQKV_A, BV0ZRLQKV_B, NKMBTT1VV, S_B, S_C, OYXB_E_B, 
             OYXB_E_C, WCWZWIFH, Q8EHFQVS3, RB1M0UL], 
      (COUNT_mS1(int)[][BV0ZRLQKV_A, BV0ZRLQKV_B, OYXB_E_B, 
                        OYXB_E_C, NKMBTT1VV] *
      (S_C ^= COUNT_mSS_C) * (RB1M0UL ^= (0.014679976512 * S_C)) *
      (Q8EHFQVS3 ^= S_C) * (WCWZWIFH ^= (17890.08 * S_C)) *
      (S_B ^= COUNT_mSS_B)))"
    "AggSum([BV0ZRLQKV_A, BV0ZRLQKV_B, NKMBTT1VV, S_B, S_C, OYXB_E_B, 
             OYXB_E_C, WCWZWIFH, Q8EHFQVS3, RB1M0UL], 
      (COUNT_mS1(int)[][BV0ZRLQKV_A, BV0ZRLQKV_B, OYXB_E_B, 
                        OYXB_E_C, NKMBTT1VV] *
      (S_C ^= COUNT_mSS_C) * (RB1M0UL ^= (0.014679976512 * S_C)) *
      (Q8EHFQVS3 ^= S_C) * (WCWZWIFH ^= (17890.08 * S_C)) *
      (S_B ^= COUNT_mSS_B)))";

  test_identical "Simple Zeus 48183500 -- comparisons only"
    "({S_B = (20 + (-1 * S_C) + BV0ZRLQKV_A + BV0ZRLQKV_A)})"
    "({S_B = (20 + (-1 * S_C) + BV0ZRLQKV_A + BV0ZRLQKV_A)})";

  ;;

  let test_decomposition msg input output = 
    let input_calc = parse_calc input in
    log_list_test ("Decomposition ( "^msg^" )")
      CalculusPrinter.string_of_expr
      (List.map snd (CalculusDecomposition.decompose_poly input_calc))
      (List.map parse_calc output)
  ;;

  test_decomposition "Polynomial decomposition"
    "R(A,B) * AggSum([], R(B,C))"
    ["R(A,B) * R(B_1, C)"];

  test_decomposition "Polynomial decomposition -- unsafe mapping"
    "R(A,B) * AggSum([], R(B, B_2))"
    ["R(A,B) * R(B_2, B_2_1)"]; 
