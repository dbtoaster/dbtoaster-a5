open Util
open Sql.Types
open Calculus

let delta_test (test:string) (input:unit calc_t) 
               (rel:string) (relvars:var_t list)
               (expected:unit calc_t): unit =
   Debug.log_unit_test 
      ("(d/d"^rel^") ["^test^"]")
      string_of_calc
      (Compiler.compute_delta rel relvars input)
      expected
;;

let calc_test_one (test:string) = 
   Debug.log_unit_test test string_of_calc
;;
let calc_test_list (test:string) = 
   Debug.log_unit_test test (string_of_list0 "\n" string_of_calc)
;;


let v n = (n,IntegerT) in
let rst_test = delta_test "RST"
   (AggSum([],
      (Prod[
         Relation("R", [v "RA";v "RB"]);
         Relation("S", [v "SB";v "SC"]);
         Relation("T", [v "TC";v "TD"]);
         Cmp(Var(v "RB"),Eq,Var(v "SB"));
         Cmp(Var(v "SC"),Eq,Var(v "TC"));
         Value(Var(v "RA"));
         Value(Var(v "TD"))
      ])
   ))
in
   rst_test "R" [v "IRA"; v "IRB"] 
      (AggSum([],
         (Prod[
            Definition(v "RA", (Value(Var(v "IRA"))));
            Definition(v "RB", (Value(Var(v "IRB"))));
            Relation("S", [v "SB";v "SC"]);
            Relation("T", [v "TC";v "TD"]);
            Cmp(Var(v "RB"),Eq,Var(v "SB"));
            Cmp(Var(v "SC"),Eq,Var(v "TC"));
            Value(Var(v "RA"));
            Value(Var(v "TD"))
         ])
      ))
;;
let p i = (("B"^(string_of_int i)^"_PRICE"), DoubleT) in
let v i = (("B"^(string_of_int i)^"_VOLUME"), IntegerT) in
let t i = (("TMP_"^(string_of_int i), if i = 1 then IntegerT else DoubleT)) in
let lhs = 
   (Definition(("TMP_0", DoubleT), (Prod[
      Value(Const(Double(0.25)));
      AggSum([],
         (Prod[
            Relation("BIDS", [p 3; v 3]);
            Value(Var(v 3))
         ])
      )
   ]))) in
let d_lhs = 
   (Sum[Definition(t 0, (Sum[
         (Prod[Value(Const(Double(0.25)));
               AggSum([], (Prod[Relation("BIDS", [p 3; v 3]); Value(Var(v 3))]))
              ]);
         (Prod[Value(Const(Double(0.25)));
               AggSum([], (Prod[Definition(p 3, (Value(Var(p 10))));
                                Definition(v 3, (Value(Var(v 10))));
                                Value(Var(v 3))]))
              ])
      ]));
      Neg(Definition(t 0, (Prod[Value(Const(Double(0.25)));
                                AggSum([],(Prod[Relation("BIDS", [p 3; v 3]);
                                                Value(Var(v 3))]))
                               ])))
   ]) in
let rhs = 
   (Definition(("TMP_1", IntegerT), (AggSum([], (Prod[
      Relation("BIDS", [p 2; v 2]);
      Cmp((Var(p 2)), Gt, (Var(p 1)));
      Value(Var(v 2))
   ]))))) in
let d_rhs = 
   (Sum[Definition(t 1, (Sum[
         (AggSum([], (Prod[Relation("BIDS", [p 2; v 2]);
                           Cmp((Var(p 2)), Gt, (Var(p 1)));
                           Value(Var(v 2))
                          ])));
         (AggSum([], (Prod[Definition(p 2, (Value(Var(p 10))));
                           Definition(v 2, (Value(Var(v 10))));
                           Cmp((Var(p 2)), Gt, (Var(p 1)));
                           Value(Var(v 2))])))
      ]));
      Neg(Definition(t 1, (AggSum([], (Prod[Relation("BIDS", [p 2; v 2]);
                                            Cmp((Var(p 2)), Gt, (Var(p 1)));
                                            Value(Var(v 2))])))));
   ]) in
let base = (Relation("BIDS", [p 1; v 1])) in
let d_base = 
   (Prod[
      Definition(p 1, (Value(Var(p 10))));
      Definition(v 1, (Value(Var(v 10))))
   ]) in
let rest = 
   (Prod[
      Cmp((Var(t 0)), Gt, (Var(t 1)));
      Value(Var(p 1));
      Value(Var(v 1))
   ]) in
let vwap_test = delta_test "VWAP" (AggSum([], mk_prod [base; lhs; rhs; rest])) 
in
   vwap_test "BIDS" [p 10; v 10] (
      AggSum([], (mk_sum [
         (mk_prod [ base; 
            (mk_sum [ 
               (mk_prod [ lhs; d_rhs; rest ]);
               (mk_prod [ d_lhs; rhs; rest ]);
               (mk_prod [ d_lhs; d_rhs; rest])
            ])]);
         (mk_prod [ d_base; lhs; rhs; rest ]);
         (mk_prod [ d_base; 
            (mk_sum [ 
               (mk_prod [ lhs; d_rhs; rest ]);
               (mk_prod [ d_lhs; rhs; rest ]);
               (mk_prod [ d_lhs; d_rhs; rest])
            ])])
         ]))
      )

;;

let v n = Value(Var(n, IntegerT)) in
   calc_test_list "Roly 1" 
      (Compiler.roly_poly (Sum[v "A"; v "B"]))
      [v "A"; v "B"];
   calc_test_list "Roly 2"
      (Compiler.roly_poly (Prod[Sum[v "A"; v "B"]; Sum[v "C"; v "D"]]))
      [  Prod[v "A"; v "C"]; 
         Prod[v "B"; v "C"]; 
         Prod[v "A"; v "D"];
         Prod[v "B"; v "D"] ];
   calc_test_list "Roly 3"
      (Compiler.roly_poly (Prod[Sum[v "A"; v "B"]; 
                                Sum[v "C"; Prod[v "D"; Sum[v "E"; v "F"]]]]))
      [  Prod[v "A"; v "C"]; 
         Prod[v "B"; v "C"]; 
         Prod[v "A"; v "D"; v "E"];
         Prod[v "B"; v "D"; v "E"];
         Prod[v "A"; v "D"; v "F"];
         Prod[v "B"; v "D"; v "F"] ];

   calc_test_one "Pushdown Negs"
      (Compiler.push_down_negs false 
         (Neg(Prod[Sum[v "A"; v "B"]; Sum[v "C"; v "D"]])))
      (Prod[Sum[Neg(v "A"); Neg(v "B")]; Sum[Neg(v "C"); Neg(v "D")]]);
      