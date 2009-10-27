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



let (bs_vars, vwap_theta, vwap_bsrw) =
   bigsum_rewriting Calculus.ModeExtractFromCond vwap [] "aux" ;;

term_as_string vwap_bsrw =
"AggSum((p0*v0), B(p0, v0) and (0.25*aux1[])<=aux2[p0])";;

let (bs_vars, vwap_theta, vwap_bsrw) =
   bigsum_rewriting Calculus.ModeOpenDomain vwap [] "aux" ;;

term_as_string vwap_bsrw =
"(if (0.25*aux2[])<=aux3[p0] then aux1[p0] else 0)";;

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


Compiler.compile Calculus.ModeExtractFromCond [("B", ["P"; "V"])]
                 (Compiler.mk_external "vwap" [])
                 vwap =
["+On Lookup(): vwap[] := AggSum((p0*v0), B(p0, v0) and (0.25*vwap__1[])<=vwap__2[p0])";
 "+B(x_vwap__1B_P, x_vwap__1B_V): vwap__1[] += x_vwap__1B_V";
 "+B(x_vwap__2B_P, x_vwap__2B_V): foreach p0 do vwap__2[p0] += (x_vwap__2B_V*(if p0<x_vwap__2B_P then 1 else 0))"]
;;

Compiler.compile Calculus.ModeIntroduceDomain [("B", ["P"; "V"])]
                 (Compiler.mk_external "vwap" [])
                 vwap =
["+On Lookup(): vwap[] := AggSum(vwap__1[p0], Dom_{p0}(p0) and (0.25*vwap__2[])<=vwap__3[p0])";
 "+B(x_vwap__1B_P, x_vwap__1B_V): vwap__1[x_vwap__1B_P] += (x_vwap__1B_P*x_vwap__1B_V)";
 "+B(x_vwap__2B_P, x_vwap__2B_V): vwap__2[] += x_vwap__2B_V";
 "+B(x_vwap__3B_P, x_vwap__3B_V): foreach p0 do vwap__3[p0] += (x_vwap__3B_V*(if p0<x_vwap__3B_P then 1 else 0))"]
;;
 
Compiler.compile Calculus.ModeOpenDomain [("B", ["P"; "V"])]
                 (Compiler.mk_external "vwap" [])
                 vwap =
["+B(x_vwapB_P, x_vwapB_V): vwap[] += bigsum_{p0} (if ((0.25*vwap__2[])+(0.25*x_vwapB_V))<=(vwap__3[p0]+(x_vwapB_V*(if p0<x_vwapB_P then 1 else 0))) then ((if p0=x_vwapB_P then p0 else 0)*x_vwapB_V) else 0)";
 "+B(x_vwapB_P, x_vwapB_V): vwap[] += bigsum_{p0} (vwap__1[p0]*(if ((0.25*vwap__2[])+(0.25*x_vwapB_V))<=(vwap__3[p0]+(x_vwapB_V*(if p0<x_vwapB_P then 1 else 0))) then 1 else 0)*(if vwap__3[p0]<(0.25*vwap__2[]) then 1 else 0))";
 "+B(x_vwapB_P, x_vwapB_V): vwap[] += bigsum_{p0} (-1*vwap__1[p0]*(if (vwap__3[p0]+(x_vwapB_V*(if p0<x_vwapB_P then 1 else 0)))<((0.25*vwap__2[])+(0.25*x_vwapB_V)) then 1 else 0)*(if (0.25*vwap__2[])<=vwap__3[p0] then 1 else 0))";
 "+B(x_vwap__1B_P, x_vwap__1B_V): vwap__1[x_vwap__1B_P] += (x_vwap__1B_P*x_vwap__1B_V)";
 "+B(x_vwap__2B_P, x_vwap__2B_V): vwap__2[] += x_vwap__2B_V";
 "+B(x_vwap__3B_P, x_vwap__3B_V): foreach p0 do vwap__3[p0] += (x_vwap__3B_V*(if p0<x_vwap__3B_P then 1 else 0))"]
;;


