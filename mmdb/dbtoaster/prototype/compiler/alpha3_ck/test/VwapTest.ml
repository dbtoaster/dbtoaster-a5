open Calculus;;


let vwap_term = RProd[RVal(Var "p0"); RVal(Var "v0")]
;;

let vwap_calc =
RA_MultiNatJoin[
   RA_Leaf(Rel("B", ["p0"; "v0"]));
   RA_Leaf(AtomicConstraint(Le, RVal(Const (Int 0)),
      RSum[RProd[RVal(Const (Double 0.25));
                      RVal(AggSum(RVal(Var "v1"),
                                  RA_Leaf(Rel("B", ["p1"; "v1"]))))];
                RVal(AggSum(RVal(Var "v2"),
                            RA_MultiNatJoin[RA_Leaf(Rel("B", ["p2"; "v2"]));
                                            RA_Leaf(AtomicConstraint(Lt,
                                               RVal(Var "p0"), RVal(Var "p2")))
                                        ]))]))
]
;;

(*
let vwap = make_term(RVal(AggSum(vwap_term, vwap_calc)));;
*)

let (bigsum_vars, t, r) =
   bigsum_rewriting (make_term vwap_term) (make_relcalc vwap_calc) []
;;

let vwap2 = make_term (RVal(AggSum(readable_term t, readable_relcalc r)));;


readable_term vwap2 =
  RVal
   (AggSum
     (RVal
       (AggSum
         (RProd [RVal (Var "p0"); RVal (Var "v0")],
          RA_Leaf (Rel ("B", ["p0"; "v0"])))),
      RA_Leaf
       (AtomicConstraint (Le, RVal (Const (Int 0)),
         RSum
          [RProd
            [RVal (Const (Double 0.25));
             RVal
              (AggSum (RVal (Var "v1"), RA_Leaf (Rel ("B", ["p1"; "v1"]))))];
           RVal
            (AggSum
              (RVal (Var "v2"),
               RA_MultiNatJoin
                [RA_Leaf (Rel ("B", ["p2"; "v2"]));
                 RA_Leaf
                  (AtomicConstraint (Lt, RVal (Var "p0"), RVal (Var "p2")))]))]))))
;;



let s0 = "(0.25*AggSum(v1, B(p1, v1)))+AggSum(v2, B(p2, v2) and p0<p2)"
in
let s1 = "("^s0^"+(0.25*v)+(v*(if p0<p then 1 else 0)))"
in
List.map (fun (x,y) -> (x, term_as_string y))
(simplify (term_delta Compiler.externals_forbidden false "B" ["p"; "v"] vwap2) ["p"; "v"; "p0"] [])
=
[([],
  "(if 0<="^s1^" then ((if p0=p then p0 else 0)*v) else 0)");
 ([],
  "(if 0<="^s1^" and ("^s0^")<0 then AggSum((p0*v0), B(p0, v0)) else 0)");
 ([],
  "(-1*(if "^s1^"<0 and 0<=("^s0^") then AggSum((p0*v0), B(p0, v0)) else 0))")]
;;




let c_new = "((0.25*vwapB1[])+vwapB2[p0]+(0.25*x_vwapB_V)+(x_vwapB_V*(if p0<x_vwapB_P then 1 else 0)))"
in
let c_old = "((0.25*vwapB1[])+vwapB2[p0])"
in
Compiler.compile [("B", ["P"; "V"])]
                 (Compiler.mk_external "vwap" [])
                 bigsum_vars vwap2 =
["+B(x_vwapB_P, x_vwapB_V): vwap[] += bigsum_{p0} (if 0<="^c_new^" then ((if p0=x_vwapB_P then p0 else 0)*x_vwapB_V) else 0)";
 "+B(x_vwapB_P, x_vwapB_V): vwap[] += bigsum_{p0} (if 0<="^c_new^" and "^c_old^"<0 then vwapB3[p0] else 0)";
 "+B(x_vwapB_P, x_vwapB_V): vwap[] += bigsum_{p0} (-1*(if "^c_new^"<0 and 0<="^c_old^" then vwapB3[p0] else 0))";
 "+B(x_vwapB1B_P, x_vwapB1B_V): vwapB1[] += x_vwapB1B_V";
 "+B(x_vwapB2B_P, x_vwapB2B_V): foreach p0 do vwapB2[p0] += (x_vwapB2B_V*(if p0<x_vwapB2B_P then 1 else 0))";
 "+B(x_vwapB3B_P, x_vwapB3B_V): vwapB3[x_vwapB3B_P] += (x_vwapB3B_P*x_vwapB3B_V)"]
;;

 

