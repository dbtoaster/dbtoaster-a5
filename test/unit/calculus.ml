open Types
open Arithmetic
open Calculus
open Calculus.CalcRing
open UnitTest


let test_expr = 
   mk_prod [
      mk_val (Value(mk_int 1));
      mk_val (rel "R" ["a";"b"])
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
              [regexp_match:int]('^MEDIUM POLISHED.*$', SUPPLIER_CNT_pPARTPART_TYPE)} *
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
  ()