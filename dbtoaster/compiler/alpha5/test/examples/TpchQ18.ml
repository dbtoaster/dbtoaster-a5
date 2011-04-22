open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;
let test_query s = Debug.log_unit_test s (String.concat "\n");;

(* TPCH Query 18

select c.custkey, sum(l1.quantity)
from customer c, orders o, lineitem l1
where 1 <=
      (select sum(1) from lineitem l2
       where l1.orderkey = l2.orderkey
       and 100 < (select sum(l3.quantity) from lineitem l3
                  where l2.orderkey = l3.orderkey))
and c.custkey = o.custkey
and o.orderkey = l1.orderkey
group by c.custkey;
*)

let tpch_sch18 =
[
   ("C", [("custkey", TInt)]);
   ("O", [("custkey", TInt); ("orderkey", TInt)]);
   ("L", [("quantity", TInt); ("orderkey", TInt)])
];;

let q18 = make_term (
   RVal(AggSum(RVal(Var(("l1.quantity", TInt))),
      RA_MultiNatJoin[
         RA_Leaf(Rel("C", [("c.custkey", TInt)]));
         RA_Leaf(Rel("O", [("o.custkey", TInt); ("o.orderkey", TInt)]));
         RA_Leaf(Rel("L", [("l1.quantity", TInt); ("l1.orderkey", TInt)]));
         RA_Leaf(AtomicConstraint(Eq, RVal(Var ("c.custkey", TInt)),
                                      RVal(Var ("o.custkey", TInt))));
         RA_Leaf(AtomicConstraint(Eq, RVal(Var ("o.orderkey", TInt)),
                                      RVal(Var ("l1.orderkey", TInt))));
         RA_Leaf(AtomicConstraint(Le, RVal(Const(Int 1)),
            RVal(AggSum(RVal(Const(Int 1)),
            RA_MultiNatJoin[
               RA_Leaf(Rel("L", [("l2.quantity", TInt); ("l2.orderkey", TInt)]));
               RA_Leaf(AtomicConstraint(Eq, RVal(Var ("l1.orderkey", TInt)),
                                            RVal(Var ("l2.orderkey", TInt))));
               RA_Leaf(AtomicConstraint(Lt, RVal(Const(Int 100)),
                  RVal(AggSum(RVal(Var(("l3.quantity", TInt))),
                     RA_MultiNatJoin[
                        RA_Leaf(Rel("L", [("l3.quantity", TInt); ("l3.orderkey", TInt)]));
                        RA_Leaf(AtomicConstraint(Eq, RVal(Var ("l2.orderkey", TInt)),
                                                     RVal(Var ("l3.orderkey", TInt))))
                     ]))))
            ]))))
      ]
)));;

(* simplify *)
let q18s = (snd (Calculus.simplify_roly true q18 [("c.custkey", TInt)] []));;

readable_term q18s =
   RVal(AggSum(RVal(Var(("l1.quantity", TInt))),
      RA_MultiNatJoin[
         RA_Leaf(Rel("C", [("c.custkey", TInt)]));
         RA_Leaf(Rel("O", [("c.custkey", TInt); ("o.orderkey", TInt)]));
         RA_Leaf(Rel("L", [("l1.quantity", TInt); ("o.orderkey", TInt)]));
         RA_Leaf(AtomicConstraint(Le, RVal(Const(Int 1)),
            RVal(AggSum(RVal(Const(Int 1)),
            RA_MultiNatJoin[
               RA_Leaf(Rel("L", [("l2.quantity", TInt); ("o.orderkey", TInt)]));
               RA_Leaf(AtomicConstraint(Lt, RVal(Const(Int 100)),
                  RVal(AggSum(RVal(Var(("l3.quantity", TInt))),
                     RA_Leaf(Rel("L", [("l3.quantity", TInt); ("o.orderkey", TInt)]))))))
            ]))))
      ]
));;

(* alternatively, do this:
let q18s = snd(List.hd (Calculus.simplify q18 [("c.custkey", TInt)] [("c.custkey", TInt)]));;
*)


let (q18_bsvars, q18_theta, q18_b) =
   (Calculus.bigsum_rewriting Calculus.ModeIntroduceDomain q18s [] "foo");;

readable_term q18_b =
RVal
 (AggSum
   (RVal (External ("foo1", [("o.orderkey", TInt)])),
    RA_MultiNatJoin
     [RA_Leaf (Rel ("Dom_{o.orderkey}", [("o.orderkey", TInt)]));
      RA_Leaf
       (AtomicConstraint (Le, RVal (Const (Int 1)),
         RVal (External ("foo2", [("o.orderkey", TInt)]))))]))
;;


Compiler.compile Calculus.ModeExtractFromCond tpch_sch18 (q18, (map_term "q" [])) cg [] =
["+On Lookup(): q[] := AggSum(l1.quantity, L(l1.quantity, o.orderkey) and 1<=q__1[o.orderkey] and O(c.custkey, o.orderkey) and C(c.custkey))";
 "+On Lookup(): q__1[l1.orderkey] := AggSum(1, L(l2.quantity, l1.orderkey) and 100<q__1__1[l1.orderkey])";
 "+L(q__1__1L_quantity, q__1__1L_orderkey): q__1__1[q__1__1L_orderkey] += q__1__1L_quantity"]
;;

Compiler.compile Calculus.ModeGroupCond tpch_sch18 (q18, (map_term "q" [])) cg [] =
["+On Lookup(): q[] := AggSum(l1.quantity, L(l1.quantity, o.orderkey) and O(c.custkey, o.orderkey) and C(c.custkey) and 1=(if 1<=q__1[o.orderkey] then 1 else 0))";
 "+On Lookup(): q__1[l1.orderkey] := AggSum(1, L(l2.quantity, l1.orderkey) and 1=(if 100<q__1__1[l1.orderkey] then 1 else 0))";
 "+L(q__1__1L_quantity, q__1__1L_orderkey): q__1__1[q__1__1L_orderkey] += q__1__1L_quantity"]
;;

(Compiler.compile Calculus.ModeIntroduceDomain tpch_sch18 (q18, (map_term "q" [])) cg []
=
["+On Lookup(): q[] := AggSum(q__1[l1.orderkey], Dom_{l1.orderkey}(l1.orderkey) and 1<=q__2[l1.orderkey])";
 "+C(q__1C_custkey): foreach l1.orderkey do q__1[l1.orderkey] += q__1C1[l1.orderkey, q__1C_custkey]";
 "+O(q__1O_custkey, q__1O_orderkey): q__1[q__1O_orderkey] += (q__1O1[q__1O_orderkey]*q__1O2[q__1O_custkey])";
 "+L(q__1L_quantity, q__1L_orderkey): q__1[q__1L_orderkey] += (q__1L_quantity*q__1L1[q__1L_orderkey])";
 "+O(q__1C1O_custkey, q__1C1O_orderkey): q__1C1[q__1C1O_orderkey, q__1C1O_custkey] += q__1C1O1[q__1C1O_orderkey]";
 "+L(q__1C1L_quantity, q__1C1L_orderkey): foreach q__1C_custkey do q__1C1[q__1C1L_orderkey, q__1C_custkey] += (q__1C1L_quantity*q__1C1L1[q__1C_custkey, q__1C1L_orderkey])";
 "+L(q__1C1O1L_quantity, q__1C1O1L_orderkey): q__1C1O1[q__1C1O1L_orderkey] += q__1C1O1L_quantity";
 "+O(q__1C1L1O_custkey, q__1C1L1O_orderkey): q__1C1L1[q__1C1L1O_custkey, q__1C1L1O_orderkey] += 1";
 "+L(q__1O1L_quantity, q__1O1L_orderkey): q__1O1[q__1O1L_orderkey] += q__1O1L_quantity";
 "+C(q__1O2C_custkey): q__1O2[q__1O2C_custkey] += 1";
 "+C(q__1L1C_custkey): foreach q__1L_orderkey do q__1L1[q__1L_orderkey] += q__1L1C1[q__1L1C_custkey, q__1L_orderkey]";
 "+O(q__1L1O_custkey, q__1L1O_orderkey): q__1L1[q__1L1O_orderkey] += q__1L1O1[q__1L1O_custkey]";
 "+O(q__1L1C1O_custkey, q__1L1C1O_orderkey): q__1L1C1[q__1L1C1O_custkey, q__1L1C1O_orderkey] += 1";
 "+C(q__1L1O1C_custkey): q__1L1O1[q__1L1O1C_custkey] += 1";
 "+On Lookup(): q__2[l1.orderkey] := AggSum(q__2__1[l2.orderkey], Dom_{l2.orderkey}(l2.orderkey) and 100<q__2__2[l2.orderkey])";
 "+L(q__2__1L_quantity, q__2__1L_orderkey): q__2__1[q__2__1L_orderkey] += 1";
 "+L(q__2__2L_quantity, q__2__2L_orderkey): q__2__2[q__2__2L_orderkey] += q__2__2L_quantity"]
;;



(*
Compiler.compile Calculus.ModeOpenDomain tpch_sch18 (map_term "q" []) q18;;
(* a monster *)
*)


