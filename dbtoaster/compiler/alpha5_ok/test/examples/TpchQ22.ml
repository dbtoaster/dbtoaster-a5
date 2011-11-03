open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;
let test_query s = Debug.log_unit_test s (String.concat "\n");;


(* TPCH Query 22

select c1.nationkey, sum(c1.acctbal) from customer c1
where c1.acctbal <
    (select sum(c2.acctbal) from customer c2 where c2.acctbal > 0)
and 0 = (select sum(1) from orders o where o.custkey = c1.custkey)
group by c1.nationkey
*)
let tpch_sch22 =
[
   ("C", [("custkey", TInt); ("nationkey", TInt); ("acctbal", TInt)]);
   ("O", [("custkey", TInt)])
];;


let tpch_q22 =
(make_term(RVal(AggSum(RVal(Var(("c1.acctbal", TInt))),
   RA_MultiNatJoin
   [RA_Leaf(Rel("C", [("c1.custkey", TInt); ("c1.nationkey", TInt); ("c1.acctbal", TInt)]));
    RA_Leaf(AtomicConstraint(Lt, RVal(Var(("c1.acctbal", TInt))),
       RVal(AggSum(RVal(Var(("c2.acctbal", TInt))),
          RA_MultiNatJoin
          [RA_Leaf(Rel("C", [("c2.custkey", TInt); ("c2.nationkey", TInt); ("c2.acctbal", TInt)]));
           RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
                                        RVal(Var(("c2.acctbal", TInt)))))
          ]))));
    RA_Leaf(AtomicConstraint(Eq, RVal(Const (Int 0)),
       RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin [
                      RA_Leaf(Rel("O", [("o.custkey", TInt)]));
                      RA_Leaf(AtomicConstraint(Eq, RVal(Var(("o.custkey", TInt))),
                                                   RVal(Var(("c1.custkey", TInt)))))
                   ]))))
   ]))))
;;


let (_, q22_theta, q22_b) =
   Calculus.bigsum_rewriting Calculus.ModeIntroduceDomain
                             tpch_q22 [("c1.nationkey", TInt)] "foo";;

term_as_string q22_b =
"AggSum(foo1[c1.acctbal, c1.custkey], Dom_{c1.acctbal, c1.custkey}(c1.acctbal, c1.custkey) and c1.acctbal<foo2[] and 0=foo3[c1.custkey])";;

let (_, q22_theta, q22_b) =
   Calculus.bigsum_rewriting Calculus.ModeExtractFromCond
                             tpch_q22 [("c1.nationkey", TInt)] "foo";;

term_as_string q22_b =
"AggSum(c1.acctbal, C(c1.custkey, c1.nationkey, c1.acctbal) and c1.acctbal<foo1[] and 0=foo2[c1.custkey])";;

let (_, q22_theta, q22_b) =
   Calculus.bigsum_rewriting Calculus.ModeGroupCond
                             tpch_q22 [("c1.nationkey", TInt)] "foo";;

term_as_string q22_b =
"AggSum(c1.acctbal, C(c1.custkey, c1.nationkey, c1.acctbal) and 1=(if c1.acctbal<foo1[] and 0=foo2[c1.custkey] then 1 else 0))";;

let (_, q22_theta, q22_b) =
   Calculus.bigsum_rewriting Calculus.ModeOpenDomain
                             tpch_q22 [("c1.nationkey", TInt)] "foo";;

term_as_string q22_b =
"(if c1.acctbal<foo2[] and 0=foo3[c1.custkey] then foo1[c1.acctbal, c1.custkey] else 0)";;


readable_term q22_b =
RVal
 (AggSum
   (RVal (External ("foo1", [("c1.acctbal", TInt); ("c1.custkey", TInt)])),
    RA_MultiNatJoin
     [RA_Leaf
       (AtomicConstraint (Lt, RVal (Var ("c1.acctbal", TInt)),
         RVal (External ("foo2", []))));
      RA_Leaf
       (AtomicConstraint (Eq, RVal (Const (Int 0)),
         RVal (External ("foo3", [("c1.custkey", TInt)]))))]))
;;

List.map (fun (x,y) -> (readable_term x, readable_term y)) q22_theta =
[(RVal
   (AggSum
     (RVal (Var ("c1.acctbal", TInt)),
      RA_Leaf (Rel ("C", [("c1.custkey", TInt); ("c1.nationkey", TInt); ("c1.acctbal", TInt)])))),
  RVal (External ("foo1", [("c1.acctbal", TInt); ("c1.custkey", TInt)])));
 (RVal
   (AggSum
     (RVal (Var ("c2.acctbal", TInt)),
      RA_MultiNatJoin
       [RA_Leaf (Rel ("C", [("c2.custkey", TInt); ("c2.nationkey", TInt); ("c2.acctbal", TInt)]));
        RA_Leaf
         (AtomicConstraint (Lt, RVal (Const (Int 0)),
           RVal (Var ("c2.acctbal", TInt))))])),
  RVal (External ("foo2", [])));
 (RVal
   (AggSum
     (RVal (Const (Int 1)),
      RA_MultiNatJoin
       [RA_Leaf (Rel ("O", [("o.custkey", TInt)]));
        RA_Leaf
         (AtomicConstraint (Eq, RVal (Var ("o.custkey", TInt)),
           RVal (Var ("c1.custkey", TInt))))])),
  RVal (External ("foo3", [("c1.custkey", TInt)])))]
;;




(Compiler.compile Calculus.ModeExtractFromCond tpch_sch22
                 (tpch_q22, (map_term "q" [("c1.nationkey", TInt)])) cg [])
=
["+On Lookup(): q[c1.nationkey] := AggSum(c1.acctbal, C(c1.custkey, c1.nationkey, c1.acctbal) and c1.acctbal<q__1[] and 0=q__2[c1.custkey])";
 "+C(x_q__1C_custkey, x_q__1C_nationkey, x_q__1C_acctbal): q__1[] += (if 0<x_q__1C_acctbal then x_q__1C_acctbal else 0)";
 "+O(x_q__2O_custkey): q__2[x_q__2O_custkey] += 1"]
;;

(Compiler.compile Calculus.ModeIntroduceDomain tpch_sch22
                 (tpch_q22, (map_term "q" [("c1.nationkey", TInt)])) cg [])
=
["+On Lookup(): q[c1.nationkey] := AggSum(q__1[c1.acctbal, c1.custkey], Dom_{c1.acctbal, c1.custkey}(c1.acctbal, c1.custkey) and c1.acctbal<q__2[] and 0=q__3[c1.custkey])";
 "+C(x_q__1C_custkey, x_q__1C_nationkey, x_q__1C_acctbal): q__1[x_q__1C_acctbal, x_q__1C_custkey] += x_q__1C_acctbal";
 "+C(x_q__2C_custkey, x_q__2C_nationkey, x_q__2C_acctbal): q__2[] += (if 0<x_q__2C_acctbal then x_q__2C_acctbal else 0)";
 "+O(x_q__3O_custkey): q__3[x_q__3O_custkey] += 1"]
;;


(* monstrous
Compiler.compile Calculus.ModeOpenDomain tpch_sch22
                 (Compiler.mk_external "q" [("c1.nationkey", TInt)])
                 tpch_q22 cg []
;;
*)







(* a simplified version of q22, using a bigsum variable *)
let q22_simpl =
(make_term(
RVal
 (AggSum
   (RVal
     (AggSum
       (RVal (Var ("c1.acctbal", TInt)),
        RA_Leaf (Rel ("C", [("c1.custkey", TInt); ("c1.acctbal", TInt)])))),
    RA_Leaf
     (AtomicConstraint (Eq, RVal (Const (Int 0)),
       RVal
        (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("O", [("c1.custkey", TInt)]))))))))
))
in
(Compiler.compile Calculus.ModeExtractFromCond
                 [ ("C", [("custkey", TInt); ("acctbal", TInt)]); ("O", [("custkey", TInt)]) ]
                 (q22_simpl, (Compiler.mk_external "q" [])) cg [])
=
["+C(x_qC_custkey, x_qC_acctbal): q[] += (x_qC_acctbal*(if 0=qC1[] then (if c1.custkey=x_qC_custkey then 1 else 0) else 0))";
 "+C(x_qC_custkey, x_qC_acctbal): q[] += (if 0=qC1[] and qC1[]<>0 then qC2[] else 0)";
 "+C(x_qC_custkey, x_qC_acctbal): q[] += (-1*(if qC1[]<>0 and 0=qC1[] then qC2[] else 0))";
 "+O(x_qO_custkey): q[] += (if 0=(qO2[]+1) and qO2[]<>0 then qO1[] else 0)";
 "+O(x_qO_custkey): q[] += (-1*(if (qO2[]+1)<>0 and 0=qO2[] then qO1[] else 0))";
 "+O(x_qC1O_custkey): qC1[] += 1";
 "+C(x_qC2C_custkey, x_qC2C_acctbal): qC2[] += x_qC2C_acctbal";
 "+C(x_qO1C_custkey, x_qO1C_acctbal): qO1[] += x_qO1C_acctbal";
 "+O(x_qO2O_custkey): qO2[] += 1"]
;;

