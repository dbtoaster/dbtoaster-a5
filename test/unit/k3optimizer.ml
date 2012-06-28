open UnitTest
;;
(****** Collection Contents ******)
let test msg expr expected =
   log_boolean ((if not expected then "(Not) " else "")^ 
                  "Has Collection ( "^msg^" )")
      (K3Optimizer.contains_collection_expr (parse_k3 expr))
      expected
in
   test "A" "Apply(
     ExternalLambda(date_part, <foo:string; bar:date>, int),
        <\"DAY\";ORDERS_ORDERDATE:date>)"
      false;
   test "B" "
     ExternalLambda(date_part, <foo:string; bar:date>, int)"
     false;
;;

(****** Beta reduction ******)

k3_map "QUERY22(float)[][C1_NATIONKEY:int]";
k3_map "QUERY22_mCUSTOMER1_L3_1(int)[][ORDERS_CUSTKEY:int]";
k3_map "QUERY22_mCUSTOMER1_L2_1(float)[][]";
k3_map "QUERY22_mCUSTOMER1(float)[]
               [C1_CUSTKEY:int,C1_NATIONKEY:int,C1_ACCTBAL:float]";
k3_map "COUNT(int)[][S_C : int]";
k3_map "COUNT_mR1_L2_1(int)[][S_A : int]";

let test msg expr expected =
   log_test ("Beta Reduction ( "^msg^" )")
      (fun x -> K3.nice_string_of_expr x [])
      (K3Optimizer.conservative_beta_reduction [] (parse_k3 expr))
      (parse_k3 expected)
in
   test "Date Extraction" "
      Apply(
        Lambda(<D:int; M:int; __prod_ret_1_3:int>) {
          <  D:int;
             M:int;
             Apply(ExternalLambda(
                date_part,<date_part_arg_1:string;date_part_arg_2:date>,int
             ),<\"YEAR\";ORDERS_ORDERDATE:date>);
             (__prod_ret_1_3:int * 1)
          >
        },
        <
          Apply(ExternalLambda(
             date_part,<date_part_arg_1:string;date_part_arg_2:date>,int
          ),<\"DAY\";ORDERS_ORDERDATE:date>);
          Apply(ExternalLambda(
             date_part,<date_part_arg_1:string;date_part_arg_2:date>,int
          ),<\"MONTH\";ORDERS_ORDERDATE:date>);
          1
        >
      )"
   "< Apply(ExternalLambda(
          date_part,<date_part_arg_1:string;date_part_arg_2:date>,int
      ),<\"DAY\";ORDERS_ORDERDATE:date>);
      Apply(ExternalLambda(
          date_part,<date_part_arg_1:string;date_part_arg_2:date>,int
      ),<\"MONTH\";ORDERS_ORDERDATE:date>);
      Apply(ExternalLambda(
          date_part,<date_part_arg_1:string;date_part_arg_2:date>,int
      ),<\"YEAR\";ORDERS_ORDERDATE:date>);
      1
    >";
   test "Q22-1"
      "Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
         (0 + prod:int)
       },<0;(1 * ((__cse_var_1:int == 0)))>)"
      "(1 * ((__cse_var_1:int == 0)))";
   test "Q22-2"
      "(Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
         (0 + prod:int)
       },<0;(1 * ((__cse_var_1:int == 0)))>) * (-1))"
      "((1 * ((__cse_var_1:int == 0))) * (-1))";
   test "Q22-3"
      "(Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
         (0 + prod:int)
       },<0;(1 * ((((__cse_var_1:int + 1)) == 0)))>))"
      "(1 * ((((__cse_var_1:int + 1)) == 0)))";
   test "Q22-4"
      "((Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
         (0 + prod:int)
       },<0;(1 * ((((__cse_var_1:int + 1)) == 0)))>))
        + 
       (((Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
         (0 + prod:int)
       },<0;(1 * ((__cse_var_1:int == 0)))>)) * -1)))"
      "((1 * ((((__cse_var_1:int + 1)) == 0))) 
         + ((1 * ((__cse_var_1:int == 0))) * (-1)))";
   test "Q22-5"
      "(__prod_ret_1_11:float * 
          (((Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
               (0 + prod:int)
             },<0;(1 * ((((__cse_var_1:int + 1)) == 0)))>))
              + 
          (((Apply(Lambda(<__sql_inline_agg_1:int; prod:int>) {
               (0 + prod:int)
             },<0;(1 * ((__cse_var_1:int == 0)))>)) * -1)))))"
      "(__prod_ret_1_11:float * 
         ((1 * ((((__cse_var_1:int + 1)) == 0))) 
            + ((1 * ((__cse_var_1:int == 0))) * (-1))))";
   test "Q22-6"
      "PCValueUpdate($QUERY22,[],[C1_NATIONKEY:int],
        (__prod_ret_1_11:float * 
          (((Apply(
               Lambda(<__sql_inline_agg_1:int; prod:int>) {(0 + prod:int)
                 },<0;(1 * ((((__cse_var_1:int + 1)) == 0)))>)) + 
             (((Apply(
                  Lambda(<__sql_inline_agg_1:int; prod:int>) {
                    (0 + prod:int)},<0;(1 * ((__cse_var_1:int == 0)))>))
                * -1))))))"
      "PCValueUpdate($QUERY22,[],[C1_NATIONKEY:int],
         (__prod_ret_1_11:float * 
            ((1 * ((((__cse_var_1:int + 1)) == 0))) 
               + ((1 * ((__cse_var_1:int == 0))) * (-1)))))";
   test "Q22-7"
      "Apply(
          Lambda(__cse_var_1:int) {
            PCValueUpdate($QUERY22,[],[C1_NATIONKEY:int],
              (__prod_ret_1_11:float * 
                (((Apply(
                     Lambda(<__sql_inline_agg_1:int; prod:int>) {(0 + prod:int)
                       },<0;(1 * ((((__cse_var_1:int + 1)) == 0)))>)) + 
                   (((Apply(
                        Lambda(<__sql_inline_agg_1:int; prod:int>) {
                          (0 + prod:int)},<0;(1 * ((__cse_var_1:int == 0)))>))
                      * -1))))))},
          Lookup($QUERY22_mCUSTOMER1_L3_1,[ORDERS_CUSTKEY:int]))"
      "Apply(Lambda(__cse_var_1:int) {
         PCValueUpdate($QUERY22,[],[C1_NATIONKEY:int],
            (__prod_ret_1_11:float * 
               ((1 * ((((__cse_var_1:int + 1)) == 0))) 
                  + ((1 * ((__cse_var_1:int == 0))) * (-1)))))
       }, Lookup($QUERY22_mCUSTOMER1_L3_1,[ORDERS_CUSTKEY:int]))";
