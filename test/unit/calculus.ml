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

let test_schema (name:string) expr ivar ovar =
   log_test ("Schemas ("^name^")")
      (fun (iv,ov) -> (ListExtras.ocaml_of_list fst iv)^
                      (ListExtras.ocaml_of_list fst ov))
      (schema_of_expr (parse_calc expr))
      ((List.map var ivar), (List.map var ovar))
;;

test_schema "TPCH18 simplified funny business"
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
  ) 
)"
   ["O_ORDERKEY"; "query18_mLINEITEMLINEITEM_QUANTITY"] []
;;


(*let test_cmp_exprs title cmp_fun input1 input2 expected =                                                                 *)
(*  let e1 = parse_calc input1 in                                                                                           *)
(*	let e2 = parse_calc input2 in                                                                                           *)
(*	log_test ("Compare expressions - ("^title^")")                                                                          *)
(*	   (string_of_bool)                                                                                                     *)
(*	   ((cmp_fun e1 e2) <> None)                                                                                            *)
(*		 (expected)                                                                                                           *)
(*in                                                                                                                        *)
(*  test_cmp_exprs "Sum case #1" Calculus.cmp_exprs "R(A,B)" "R(A,B)" true;                                                 *)
(*	test_cmp_exprs "Sum case #2" Calculus.cmp_exprs "R(A,B)" "S(A,B)" false;                                                *)
(*	test_cmp_exprs "Sum case #3" Calculus.cmp_exprs "R(A,B) + S(C,D)" "S(C,D) + R(A,B)" false;                              *)
(*	                                                                                                                        *)
(*	test_cmp_exprs "Sum case #4" Calculus.cmp_exprs_sum "R(A,B) + S(C,D)" "S(C,D) + R(A,B)" true;                           *)
(*	test_cmp_exprs "Sum case #5" Calculus.cmp_exprs_sum "R(A,B) + S(C,D) + T(E)" "T(E,F) + S(C,D) + R(A,B)" false;          *)
(*	test_cmp_exprs "Sum case #6" Calculus.cmp_exprs_sum "R(A,B) + S(C,D) + T(E)" "T(E) + R(A,B) + S(C,D)" true;             *)
(*	                                                                                                                        *)
(*	test_cmp_exprs "Prod case #7" Calculus.cmp_exprs_prod "R(A,B) * S(C,D)" "S(C,D) * R(A,B)" true;                         *)
(*	test_cmp_exprs "Prod case #8" (Calculus.cmp_exprs_prod ~scope:[("A", TInt)]) "R(A,B) * [A > 5]" "[A > 5] * R(A,B)" true;*)
(*	test_cmp_exprs "Prod case #9" Calculus.cmp_exprs_prod "R(A,B) * S(A,D) * [A > 5]" "R(A,B) * [A > 5] * S(A,D)" true;     *)
	
(*	test_cmp_exprs "Prod case #10" Calculus.cmp_exprs_prod "R(A) * (S(B) * T(C))" "R(A) * S(B) * T(C)" true;*)
(*	                                                                                                        *)
(*	test_cmp_exprs "Prod case #11" Calculus.cmp_exprs_prod                                                  *)
(*	       "AggSum([A,B],(ALPHA ^= AggSum([A],R(A,C))) * (ALPHA ^= [0]) * S(A,B) * [B])"                    *)
(*				 "AggSum([A,B],((ALPHA ^= [0]) * (ALPHA ^= AggSum([A],R(A,C)))) * S(A,B) * [B])" true;            *)

	
