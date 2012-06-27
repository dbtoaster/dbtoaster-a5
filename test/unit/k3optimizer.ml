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
let test msg expr expected =
   log_test ("Beta Reduction ( "^msg^" )")
      (fun x -> K3.nice_string_of_expr x [])
      (K3Optimizer.conservative_beta_reduction [] (parse_k3 expr))
      (parse_k3 expected)
in
   test "Date Extraction" "
      Apply(
          Lambda(<D:int; M:int; __prod_ret_1_3:int>) {
            <D:int;M:int;
              Apply(
                ExternalLambda(date_part,<date_part_arg_1:string;date_part_arg_2:date>,int),
                <\"YEAR\";ORDERS_ORDERDATE:date>);(__prod_ret_1_3:int * 1)>},
          <
            Apply(
              ExternalLambda(date_part,<date_part_arg_1:string;date_part_arg_2:date>,int),
              <\"DAY\";ORDERS_ORDERDATE:date>);
            Apply(
              ExternalLambda(date_part,<date_part_arg_1:string;date_part_arg_2:date>,int),
              <\"MONTH\";ORDERS_ORDERDATE:date>);
              
            1>)
   "
   "{ <
            Apply(
              ExternalLambda(date_part,<date_part_arg_1:string;date_part_arg_2:date>,int),
              <\"DAY\";ORDERS_ORDERDATE:date>);
            Apply(
              ExternalLambda(date_part,<date_part_arg_1:string;date_part_arg_2:date>,int),
              <\"MONTH\";ORDERS_ORDERDATE:date>);
            Apply(
                ExternalLambda(date_part,<date_part_arg_1:string;date_part_arg_2:date>,int),
              <\"YEAR\";ORDERS_ORDERDATE:date>);
              
              1>}";
