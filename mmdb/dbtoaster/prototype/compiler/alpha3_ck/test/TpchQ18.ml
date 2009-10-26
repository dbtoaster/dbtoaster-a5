open Calculus;;

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
   ("C", ["custkey"]);
   ("O", ["custkey"; "orderkey"]);
   ("L", ["quantity"; "orderkey"])
];;

let q18 = make_term (
   RVal(AggSum(RVal(Var("l1.quantity")),
      RA_MultiNatJoin[
         RA_Leaf(Rel("C", ["c.custkey"]));
         RA_Leaf(Rel("O", ["o.custkey"; "o.orderkey"]));
         RA_Leaf(Rel("L", ["l1.quantity"; "l1.orderkey"]));
         RA_Leaf(AtomicConstraint(Eq, RVal(Var "c.custkey"),
                                      RVal(Var "o.custkey")));
         RA_Leaf(AtomicConstraint(Eq, RVal(Var "o.orderkey"),
                                      RVal(Var "l1.orderkey")));
         RA_Leaf(AtomicConstraint(Le, RVal(Const(Int 1)),
            RVal(AggSum(RVal(Const(Int 1)),
            RA_MultiNatJoin[
               RA_Leaf(Rel("L", ["l2.quantity"; "l2.orderkey"]));
               RA_Leaf(AtomicConstraint(Eq, RVal(Var "l1.orderkey"),
                                            RVal(Var "l2.orderkey")));
               RA_Leaf(AtomicConstraint(Lt, RVal(Const(Int 100)),
                  RVal(AggSum(RVal(Var("l3.quantity")),
                     RA_MultiNatJoin[
                        RA_Leaf(Rel("L", ["l3.quantity"; "l3.orderkey"]));
                        RA_Leaf(AtomicConstraint(Eq, RVal(Var "l2.orderkey"),
                                                     RVal(Var "l3.orderkey")))
                     ]))))
            ]))))
      ]
)));;

(* simplify *)
let q18s = (snd (Calculus.simplify_roly true q18 ["c.custkey"]));;

readable_term q18s =
   RVal(AggSum(RVal(Var("l1.quantity")),
      RA_MultiNatJoin[
         RA_Leaf(Rel("C", ["c.custkey"]));
         RA_Leaf(Rel("O", ["c.custkey"; "o.orderkey"]));
         RA_Leaf(Rel("L", ["l1.quantity"; "o.orderkey"]));
         RA_Leaf(AtomicConstraint(Le, RVal(Const(Int 1)),
            RVal(AggSum(RVal(Const(Int 1)),
            RA_MultiNatJoin[
               RA_Leaf(Rel("L", ["l2.quantity"; "o.orderkey"]));
               RA_Leaf(AtomicConstraint(Lt, RVal(Const(Int 100)),
                  RVal(AggSum(RVal(Var("l3.quantity")),
                     RA_Leaf(Rel("L", ["l3.quantity"; "o.orderkey"]))))))
            ]))))
      ]
));;

(* alternatively, do this:
let q18s = snd(List.hd (Calculus.simplify q18 ["c.custkey"] ["c.custkey"]));;
*)


let (q18_bsvars, q18_theta, q18_b) =
   (Calculus.bigsum_rewriting Calculus.ModeIntroduceDomain q18s [] "foo");;

readable_term q18_b =
RVal
 (AggSum
   (RVal (External ("foo1", ["o.orderkey"])),
    RA_MultiNatJoin
     [RA_Leaf (Rel ("Dom_{o.orderkey}", ["o.orderkey"]));
      RA_Leaf
       (AtomicConstraint (Le, RVal (Const (Int 1)),
         RVal (External ("foo2", ["o.orderkey"]))))]))
;;


Compiler.compile Calculus.ModeExtractFromCond tpch_sch18 (Compiler.mk_external "q" []) q18 =
["+On Lookup(): q[] := AggSum(l1.quantity, L(l1.quantity, o.orderkey) and 1<=q__1[o.orderkey] and O(c.custkey, o.orderkey) and C(c.custkey))";
 "+On Lookup(): foreach l1.orderkey do q__1[l1.orderkey] := AggSum(1, L(l2.quantity, l1.orderkey) and 100<q__1__1[l1.orderkey])";
 "+L(x_q__1__1L_quantity, x_q__1__1L_orderkey): q__1__1[x_q__1__1L_orderkey] += x_q__1__1L_quantity"]
;;

Compiler.compile Calculus.ModeGroupCond tpch_sch18 (Compiler.mk_external "q" []) q18 =
["+On Lookup(): q[] := AggSum(l1.quantity, L(l1.quantity, o.orderkey) and O(c.custkey, o.orderkey) and C(c.custkey) and 1=(if 1<=q__1[o.orderkey] then 1 else 0))";
 "+On Lookup(): foreach l1.orderkey do q__1[l1.orderkey] := AggSum(1, L(l2.quantity, l1.orderkey) and 1=(if 100<q__1__1[l1.orderkey] then 1 else 0))";
 "+L(x_q__1__1L_quantity, x_q__1__1L_orderkey): q__1__1[x_q__1__1L_orderkey] += x_q__1__1L_quantity"]
;;

Compiler.compile Calculus.ModeIntroduceDomain tpch_sch18 (Compiler.mk_external "q" []) q18
=
["+On Lookup(): q[] := AggSum(q__1[l1.orderkey], Dom_{l1.orderkey}(l1.orderkey) and 1<=q__2[l1.orderkey])";
 "+C(x_q__1C_custkey): foreach l1.orderkey do q__1[l1.orderkey] += q__1C1[l1.orderkey, x_q__1C_custkey]";
 "+O(x_q__1O_custkey, x_q__1O_orderkey): q__1[x_q__1O_orderkey] += (q__1O1[x_q__1O_orderkey]*q__1O2[x_q__1O_custkey])";
 "+L(x_q__1L_quantity, x_q__1L_orderkey): q__1[x_q__1L_orderkey] += (x_q__1L_quantity*q__1L1[x_q__1L_orderkey])";
 "+O(x_q__1C1O_custkey, x_q__1C1O_orderkey): q__1C1[x_q__1C1O_orderkey, x_q__1C1O_custkey] += q__1C1O1[x_q__1C1O_orderkey]";
 "+L(x_q__1C1L_quantity, x_q__1C1L_orderkey): foreach x_q__1C_custkey do q__1C1[x_q__1C1L_orderkey, x_q__1C_custkey] += (x_q__1C1L_quantity*q__1C1L1[x_q__1C_custkey, x_q__1C1L_orderkey])";
 "+L(x_q__1C1O1L_quantity, x_q__1C1O1L_orderkey): q__1C1O1[x_q__1C1O1L_orderkey] += x_q__1C1O1L_quantity";
 "+O(x_q__1C1L1O_custkey, x_q__1C1L1O_orderkey): q__1C1L1[x_q__1C1L1O_custkey, x_q__1C1L1O_orderkey] += 1";
 "+L(x_q__1O1L_quantity, x_q__1O1L_orderkey): q__1O1[x_q__1O1L_orderkey] += x_q__1O1L_quantity";
 "+C(x_q__1O2C_custkey): q__1O2[x_q__1O2C_custkey] += 1";
 "+C(x_q__1L1C_custkey): foreach x_q__1L_orderkey do q__1L1[x_q__1L_orderkey] += q__1L1C1[x_q__1L1C_custkey, x_q__1L_orderkey]";
 "+O(x_q__1L1O_custkey, x_q__1L1O_orderkey): q__1L1[x_q__1L1O_orderkey] += q__1L1O1[x_q__1L1O_custkey]";
 "+O(x_q__1L1C1O_custkey, x_q__1L1C1O_orderkey): q__1L1C1[x_q__1L1C1O_custkey, x_q__1L1C1O_orderkey] += 1";
 "+C(x_q__1L1O1C_custkey): q__1L1O1[x_q__1L1O1C_custkey] += 1";
 "+On Lookup(): foreach l1.orderkey do q__2[l1.orderkey] := AggSum(q__2__1[l2.orderkey], Dom_{l2.orderkey}(l2.orderkey) and 100<q__2__2[l2.orderkey])";
 "+L(x_q__2__1L_quantity, x_q__2__1L_orderkey): q__2__1[x_q__2__1L_orderkey] += 1";
 "+L(x_q__2__2L_quantity, x_q__2__2L_orderkey): q__2__2[x_q__2__2L_orderkey] += x_q__2__2L_quantity"]
;;



(*
Compiler.compile Calculus.ModeOpenDomain tpch_sch18 (Compiler.mk_external "q" []) q18;;
(* a monster *)
*)


