open Calculus;;

(* SQL query:
select b.broker_id, sum(a.v+-1*(b.p*b.p))
from bids b, asks a
where b.broker_id = a.broker_id
and ( (a.p+-1*b.p > 1000) or (b.p+-1*a.p > 1000) )
group by b.broker_id
*)

let axf = make_term(
RVal(AggSum(
    RSum[RVal(Var("A__V"));
    RProd([
        RVal(Const(Int(-1)));
        RSum([RVal(Var("B__P"));
            RVal(Var("B__P"))])])],
    RA_MultiNatJoin[
        RA_Leaf(Rel("A",["A__BROKER_ID"; "A__P"; "A__V"]));
        RA_Leaf(Rel("B",["B__BROKER_ID"; "B__P"; "B__V"]));
        RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("B__BROKER_ID")),
            RVal(Var("A__BROKER_ID"))));
        RA_MultiUnion([
            RA_Leaf(AtomicConstraint(Lt,
                RVal(Const(Double(1000.0))),
                RSum([RVal(Var("A__P"));
                RProd([RVal(Const(Int(-1))); RVal(Var("B__P"))])])));
            RA_Leaf(AtomicConstraint(Lt,
                RVal(Const(Double(1000.0))),
                RSum([RVal(Var("B__P"));
                RProd([RVal(Const(Int(-1))); RVal(Var("A__P"))])])))])])))
;;

Compiler.compile Calculus.ModeOpenDomain
    [("A", ["A__BROKER_ID"; "A__P"; "A__V"]);
     ("B", ["B__BROKER_ID"; "B__P"; "B__V"])]
    (Compiler.mk_external "axf" ["B__BROKER_ID"]) axf
;;

Compiler.compile Calculus.ModeOpenDomain
    [("A", ["A__BROKER_ID"; "A__P"; "A__V"]);
     ("B", ["B__BROKER_ID"; "B__P"; "B__V"])]
    (Compiler.mk_external "axf" ["B__BROKER_ID"]) axf
=
["+A(x_axfA_A__BROKER_ID, x_axfA_A__P, x_axfA_A__V): axf[x_axfA_A__BROKER_ID] += (x_axfA_A__V*axfA1[x_axfA_A__BROKER_ID, x_axfA_A__P])";
 "+A(x_axfA_A__BROKER_ID, x_axfA_A__P, x_axfA_A__V): axf[x_axfA_A__BROKER_ID] += (-1*axfA2[x_axfA_A__BROKER_ID, x_axfA_A__P])";
 "+A(x_axfA_A__BROKER_ID, x_axfA_A__P, x_axfA_A__V): axf[x_axfA_A__BROKER_ID] += (-1*axfA2[x_axfA_A__BROKER_ID, x_axfA_A__P])";
 "+A(x_axfA_A__BROKER_ID, x_axfA_A__P, x_axfA_A__V): axf[x_axfA_A__BROKER_ID] += (x_axfA_A__V*axfA3[x_axfA_A__BROKER_ID, x_axfA_A__P])";
 "+A(x_axfA_A__BROKER_ID, x_axfA_A__P, x_axfA_A__V): axf[x_axfA_A__BROKER_ID] += (-1*axfA4[x_axfA_A__BROKER_ID, x_axfA_A__P])";
 "+A(x_axfA_A__BROKER_ID, x_axfA_A__P, x_axfA_A__V): axf[x_axfA_A__BROKER_ID] += (-1*axfA4[x_axfA_A__BROKER_ID, x_axfA_A__P])";
 "+B(x_axfB_B__BROKER_ID, x_axfB_B__P, x_axfB_B__V): axf[x_axfB_B__BROKER_ID] += axfB1[x_axfB_B__BROKER_ID, x_axfB_B__P]";
 "+B(x_axfB_B__BROKER_ID, x_axfB_B__P, x_axfB_B__V): axf[x_axfB_B__BROKER_ID] += (-1*axfB2[x_axfB_B__P, x_axfB_B__BROKER_ID])";
 "+B(x_axfB_B__BROKER_ID, x_axfB_B__P, x_axfB_B__V): axf[x_axfB_B__BROKER_ID] += (-1*axfB2[x_axfB_B__P, x_axfB_B__BROKER_ID])";
 "+B(x_axfB_B__BROKER_ID, x_axfB_B__P, x_axfB_B__V): axf[x_axfB_B__BROKER_ID] += axfB3[x_axfB_B__BROKER_ID, x_axfB_B__P]";
 "+B(x_axfB_B__BROKER_ID, x_axfB_B__P, x_axfB_B__V): axf[x_axfB_B__BROKER_ID] += (-1*axfB4[x_axfB_B__P, x_axfB_B__BROKER_ID])";
 "+B(x_axfB_B__BROKER_ID, x_axfB_B__P, x_axfB_B__V): axf[x_axfB_B__BROKER_ID] += (-1*axfB4[x_axfB_B__P, x_axfB_B__BROKER_ID])";
 "+B(x_axfA1B_B__BROKER_ID, x_axfA1B_B__P, x_axfA1B_B__V): foreach x_axfA_A__P do axfA1[x_axfA1B_B__BROKER_ID, x_axfA_A__P] += (if 1000.<(x_axfA_A__P+(-1*x_axfA1B_B__P)) then 1 else 0)";
 "+B(x_axfA2B_B__BROKER_ID, x_axfA2B_B__P, x_axfA2B_B__V): foreach x_axfA_A__P do axfA2[x_axfA2B_B__BROKER_ID, x_axfA_A__P] += (if 1000.<(x_axfA_A__P+(-1*x_axfA2B_B__P)) then x_axfA2B_B__P else 0)";
 "+B(x_axfA3B_B__BROKER_ID, x_axfA3B_B__P, x_axfA3B_B__V): foreach x_axfA_A__P do axfA3[x_axfA3B_B__BROKER_ID, x_axfA_A__P] += (if 1000.<(x_axfA3B_B__P+(-1*x_axfA_A__P)) then 1 else 0)";
 "+B(x_axfA4B_B__BROKER_ID, x_axfA4B_B__P, x_axfA4B_B__V): foreach x_axfA_A__P do axfA4[x_axfA4B_B__BROKER_ID, x_axfA_A__P] += (if 1000.<(x_axfA4B_B__P+(-1*x_axfA_A__P)) then x_axfA4B_B__P else 0)";
 "+A(x_axfB1A_A__BROKER_ID, x_axfB1A_A__P, x_axfB1A_A__V): foreach x_axfB_B__P do axfB1[x_axfB1A_A__BROKER_ID, x_axfB_B__P] += (x_axfB1A_A__V*(if 1000.<(x_axfB1A_A__P+(-1*x_axfB_B__P)) then 1 else 0))";
 "+A(x_axfB2A_A__BROKER_ID, x_axfB2A_A__P, x_axfB2A_A__V): foreach x_axfB_B__P do axfB2[x_axfB_B__P, x_axfB2A_A__BROKER_ID] += (if 1000.<(x_axfB2A_A__P+(-1*x_axfB_B__P)) then x_axfB_B__P else 0)";
 "+A(x_axfB3A_A__BROKER_ID, x_axfB3A_A__P, x_axfB3A_A__V): foreach x_axfB_B__P do axfB3[x_axfB3A_A__BROKER_ID, x_axfB_B__P] += (x_axfB3A_A__V*(if 1000.<(x_axfB_B__P+(-1*x_axfB3A_A__P)) then 1 else 0))";
 "+A(x_axfB4A_A__BROKER_ID, x_axfB4A_A__P, x_axfB4A_A__V): foreach x_axfB_B__P do axfB4[x_axfB_B__P, x_axfB4A_A__BROKER_ID] += (if 1000.<(x_axfB_B__P+(-1*x_axfB4A_A__P)) then x_axfB_B__P else 0)"]
;;

