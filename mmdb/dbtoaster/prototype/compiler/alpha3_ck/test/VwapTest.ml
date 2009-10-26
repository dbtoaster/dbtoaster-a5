open Calculus;;

let vwap1r = RVal(AggSum(RVal(Var "v1"), RA_Leaf(Rel("B", ["p1"; "v1"]))));;
let vwap2r = RVal(AggSum(RVal(Var "v2"),
                       RA_MultiNatJoin[RA_Leaf(Rel("B", ["p2"; "v2"]));
                                       RA_Leaf(AtomicConstraint(Lt,
                                               RVal(Var "p0"), RVal(Var "p2")))
                                        ]));;
let vwap = make_term(
RVal(AggSum(RProd[RVal(Var "p0"); RVal(Var "v0")],
RA_MultiNatJoin[
   RA_Leaf(Rel("B", ["p0"; "v0"]));
   RA_Leaf(AtomicConstraint(Le, RProd[RVal(Const (Double 0.25)); vwap1r],
           vwap2r))
])))
;;



let r = (make_relcalc (RA_Leaf(Rel("B", ["p1"; "v1"]))));;

let (flat, nested) = split_nested r ;;
readable_relcalc nested;;

((not (constraints_only r)) && ((snd (split_nested r)) <> relcalc_one))
= false;;


let (bs_vars, vwap_theta, vwap_bsrw) =
   bigsum_rewriting Calculus.ModeIntroduceDomain vwap [] "aux" ;;

term_as_string vwap_bsrw =
"AggSum(aux1[p0], Dom_{p0}(p0) and (0.25*aux2[])<=aux3[p0])";;


List.map (fun (x,y) -> (readable_term x, readable_term y)) vwap_theta =
[(RVal
   (AggSum
     (RProd [RVal (Var "p0"); RVal (Var "v0")],
      RA_Leaf (Rel ("B", ["p0"; "v0"])))),
  RVal (External ("aux1", ["p0"])));
 (vwap1r, RVal (External ("aux2", [])));
 (vwap2r, RVal (External ("aux3", ["p0"])))]
;;



let (bs_vars4, vwap_theta4, vwap4) =
   bigsum_rewriting Calculus.ModeIntroduceDomain (make_term vwap1r) [] "aux" ;;

term_as_string vwap4 = "AggSum(v1, B(p1, v1))";;


Compiler.compile Calculus.ModeIntroduceDomain [("B", ["P"; "V"])]
                 (Compiler.mk_external "vwap" [])
                 vwap =
["+On Lookup(): vwap[] := AggSum(vwap$1[p0], Dom_{p0}(p0) and (0.25*vwap$2[])<=vwap$3[p0])";
 "+B(x_vwap$1B_P, x_vwap$1B_V): vwap$1[x_vwap$1B_P] += (x_vwap$1B_P*x_vwap$1B_V)";
 "+B(x_vwap$2B_P, x_vwap$2B_V): vwap$2[] += x_vwap$2B_V";
 "+B(x_vwap$3B_P, x_vwap$3B_V): foreach p0 do vwap$3[p0] += (x_vwap$3B_V*(if p0<x_vwap$3B_P then 1 else 0))"]
;;
 

