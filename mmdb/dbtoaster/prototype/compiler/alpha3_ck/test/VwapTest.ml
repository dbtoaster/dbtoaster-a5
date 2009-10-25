open Calculus;;

let vwap =
make_term(RVal(AggSum(
RProd[RVal(Var "p0"); RVal(Var "v0")],
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
])))
;;

let vwap2 =
make_term(RVal(AggSum(
RProd[RVal(Var "p0"); RVal(Var "v0")],
RA_MultiNatJoin[
   RA_Leaf(Rel("B", ["p0"; "v0"]));
   RA_Leaf(AtomicConstraint(Le, RVal(Const (Int 0)),
                                RVal(External("m", ["p0"]))))
])))
;;


let map_deltas =
[
   ("m", (["p0"], make_term (RVal(External("Delta m", ["p0"])))))
];;


let (_, d) =
Calculus.simplify_roly true
   (Calculus.term_delta (Compiler.delta_for_named_map map_deltas)
                        false "B" ["x"; "y"] vwap2) ["x"; "y"];;

term_as_string d =
"((if 0<=(m[x]+Delta m[x]) then (x*y) else 0)+AggSum((p0*v0), B(p0, v0) and 0<=(m[p0]+Delta m[p0]) and m[p0]<0)+AggSum((-1*(p0*v0)), B(p0, v0) and (m[p0]+Delta m[p0])<0 and 0<=m[p0]))"
;;


let (bs_vars, vwap_theta, vwap3) =
   bigsum_rewriting Calculus.ModeIntroduceDomain vwap [] "aux" ;;

readable_term vwap3 =
RVal
 (AggSum
   (RVal
     (AggSum
       (RProd [RVal (Var "p0"); RVal (Var "v0")],
        RA_Leaf (Rel ("B", ["p0"; "v0"])))),
    RA_MultiNatJoin
     [RA_Leaf (Rel ("Dom_{p0}", ["p0"]));
      RA_Leaf
       (AtomicConstraint (Le, RVal (Const (Int 0)),
         RSum
          [RProd [RVal (Const (Double 0.25)); RVal (External ("aux1", []))];
           RVal (External ("aux2", ["p0"]))]))]))
;;

List.map (fun (x,y) -> (readable_term x, readable_term y)) vwap_theta =
[(RVal (AggSum (RVal (Var "v1"), RA_Leaf (Rel ("B", ["p1"; "v1"])))),
  RVal (External ("aux1", [])));
 (RVal
   (AggSum
     (RVal (Var "v2"),
      RA_MultiNatJoin
       [RA_Leaf (Rel ("B", ["p2"; "v2"]));
        RA_Leaf (AtomicConstraint (Lt, RVal (Var "p0"), RVal (Var "p2")))])),
  RVal (External ("aux2", ["p0"])))]
;;





Compiler.compile Calculus.ModeIntroduceDomain [("B", ["P"; "V"])]
                 (Compiler.mk_external "vwap" [])
                 vwap =
["+On Lookup(): vwap[] := AggSum(AggSum((p0*v0), B(p0, v0)), Dom_{p0}(p0) and 0<=((0.25*vwap$1[])+vwap$2[p0]))";
 "+B(x_vwap$1B_P, x_vwap$1B_V): vwap$1[] += x_vwap$1B_V";
 "+B(x_vwap$2B_P, x_vwap$2B_V): foreach p0 do vwap$2[p0] += (x_vwap$2B_V*(if p0<x_vwap$2B_P then 1 else 0))"]
;;
 

