open Types
open UnitTest
open IVC
;;

Debug.activate "NO-VISUAL-DIFF";

let test expected expr =
   log_test ("Startup IVC ( "^expr^" )")
            (function true -> "YES" | false -> "NO")
            (IVC.needs_startup_ivc ["T1",[],Schema.TableRel; 
                                    "T2",[],Schema.TableRel; 
                                    "T3",[],Schema.TableRel; 
                                    "T4",[],Schema.TableRel]
                                   (parse_calc expr))
            expected
in
   test false "R(A)";
   test false "A";
   test true  "(A ^= R(A))";
   test true  "T1[]";