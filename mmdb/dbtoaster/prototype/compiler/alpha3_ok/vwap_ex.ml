open Calculus

let query = 
  RVal(AggSum(RProd[RVal(Var("P0")); RVal(Var("V0"))],
  RA_MultiNatJoin[
    RA_Leaf(Rel("B", ["P0"; "V0"]));
    RA_Leaf(
      AtomicConstraint(Le,
        RProd[RVal(Const (Double 0.25)); 
              RVal(AggSum(RVal(Var("V1")), RA_Leaf(Rel("B", ["P1"; "V1"]))))
             ],
        RVal(AggSum(RVal(Var("V2")), 
                    RA_MultiNatJoin[RA_Leaf(Rel("B", ["P2"; "V2"]));
                                    RA_Leaf(AtomicConstraint(Lt,
                                            RVal(Var("P0")), RVal(Var("P2"))
                                           ))
                                   ]
                   ))
      )
    )
  ]));;


print_string
  (M3.pretty_print_prog
    (Compiler.compile_m3
      [("B", ["P"; "V"])]
      (Compiler.mk_external "vwap" [])
      (Calculus.make_term query)
    )
  );;
