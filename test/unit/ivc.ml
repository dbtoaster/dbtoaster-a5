open Types
open UnitTest
open IVC
;;

Debug.activate "NO-VISUAL-DIFF";

let test msg expr exp_val_domain exp_var_domains =
   log_list_test ("Schema Domains ( "^msg^" )")
                 (fun (v, source) ->
                     (Types.string_of_var v)^":"^
                     (IVC.string_of_domain_source source))
                 (let (var_domains,val_domain) =
                    (schema_domains ["T1",[],Schema.TableRel; 
                                     "T2",[],Schema.TableRel; 
                                     "T3",[],Schema.TableRel; 
                                     "T4",[],Schema.TableRel]
                                    (parse_calc expr))
                  in (("::VALUE::", TAny), val_domain)::var_domains)
                 ((("::VALUE::", TAny), exp_val_domain)::
                     (List.map (fun (vn,dt) -> (var vn, dt)) exp_var_domains))
in
   test "rel"  "R(A)" StreamSource [
      "A", StreamSource
   ];
   test "lift" "(X ^= R(A))" InlineSource [
      "X", StreamSource;
      "A", StreamSource;
   ];
   test "value" "A" InlineSource [
   ];
   test "product" "R(A)*A" StreamSource [
      "A", StreamSource
   ];
   test "sum of stream v table" "R(A)+T1(A)" TableSource [
      "A", TableSource
   ];
   test "sum of stream v lift" "R(A)+(A ^= 1)" InlineSource [
      "A", InlineSource
   ];