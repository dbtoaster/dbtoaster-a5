open Util
open Calculus

;;

let calc_pricespread = make_term
   (RVal(AggSum(
      (RSum([
         (RVal(Var("A___P", TDouble)));
         (RVal(Var("B___P", TDouble)))
      ])),
      (RA_MultiNatJoin([
         (RA_Leaf(Rel("BIDS", [
            ("B__T",TDouble);
            ("B__ID",TDouble);
            ("B__BROKER_ID",TDouble);
            ("B__P",TDouble);
            ("B__V",TDouble)
         ])));
         (RA_Leaf(Rel("ASKS", [
            ("A__T",TDouble);
            ("A__ID",TDouble);
            ("A__BROKER_ID",TDouble);
            ("A__P",TDouble);
            ("A__V",TDouble)
         ])));
         (RA_Leaf(AtomicConstraint(Lt,
            (RProd([
               (RVal(Const(Calculus.Double(0.0001))));
               (RVal(AggSum(
                  (RVal(Var("B1__V",TDouble))),
                  (RA_Leaf(Rel("BIDS",[
                     ("B1__T",TDouble);
                     ("B1__ID",TDouble);
                     ("B1__BROKER_ID",TDouble);
                     ("B1__P",TDouble);
                     ("B1__V",TDouble)
                  ])))
               )))
            ])),
            (RVal(Var("B__V",TDouble)))
         )))
      ]))
   )))
in
let (bigsum_vars, bsrw_theta, bsrw_term) =
   (Calculus.bigsum_rewriting        
      Calculus.ModeOpenDomain 
      (Calculus.roly_poly calc_pricespread) 
      [] 
      ("QUERY_")
   )
in
   Debug.log_unit_test 
      "Sum of varsum with bigsum"
      (Util.string_of_list0 "\n"
         (fun (md,mt) -> (Calculus.term_as_string mt)^" := "^
                         (Calculus.term_as_string md)))
      bsrw_theta
      []
