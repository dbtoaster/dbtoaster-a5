open Calculus
open Util;;

(* SQL query:
select b.broker_id, sum(a.v+-1*(b.p*b.p))
from bids b, asks a
where b.broker_id = a.broker_id
and ( (a.p+-1*b.p > 1000) or (b.p+-1*a.p > 1000) )
group by b.broker_id
*)

let cg = Compiler.generate_unit_test_code;;

let axf = make_term(
RVal(AggSum(
    RSum[RVal(Var(("A__V", TInt)));
    RProd([
        RVal(Const(Int(-1)));
        RSum([RVal(Var(("B__P", TInt)));
            RVal(Var(("B__P", TInt)))])])],
    RA_MultiNatJoin[
      RA_Leaf(Rel("A",[("A__BROKER_ID", TInt); ("A__P", TInt); ("A__V", TInt)]));
      RA_Leaf(Rel("B",[("B__BROKER_ID", TInt); ("B__P", TInt); ("B__V", TInt)]));
      RA_Leaf(AtomicConstraint(Eq,
            RVal(Var(("B__BROKER_ID", TInt))),
            RVal(Var(("A__BROKER_ID", TInt)))));
      RA_MultiUnion([
            RA_Leaf(AtomicConstraint(Lt,
                RVal(Const(Double(1000.0))),
                RSum([RVal(Var(("A__P", TInt)));
                RProd([RVal(Const(Int(-1))); RVal(Var(("B__P", TInt)))])])));
            RA_Leaf(AtomicConstraint(Lt,
              RVal(Const(Double(1000.0))),
              RSum([RVal(Var(("B__P", TInt)));
              RProd([RVal(Const(Int(-1))); RVal(Var(("A__P", TInt)))])])))])])))
;;

(*
Compiler.compile Calculus.ModeOpenDomain
    [("A", [("A__BROKER_ID", TInt); ("A__P", TInt); ("A__V", TInt)]);
     ("B", [("B__BROKER_ID", TInt); ("B__P", TInt); ("B__V", TInt)])]
    (axf, (map_term "axf" [("B__BROKER_ID", TInt)])) cg []
;;
*)

Debug.log_unit_test "Ax finder" (String.concat "\n")
(Compiler.compile Calculus.ModeOpenDomain
    [("A", [("A__BROKER_ID", TInt); ("A__P", TInt); ("A__V", TInt)]);
     ("B", [("B__BROKER_ID", TInt); ("B__P", TInt); ("B__V", TInt)])]
    (axf, (map_term "axf" [("B__BROKER_ID", TInt)])) cg [])
(
["+A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(axfA_A__V*axfA1[axfA_A__BROKER_ID, axfA_A__P])";
 "+A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA2[axfA_A__BROKER_ID, axfA_A__P])";
 "+A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA2[axfA_A__BROKER_ID, axfA_A__P])";
 "+A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(axfA_A__V*axfA3[axfA_A__BROKER_ID, axfA_A__P])";
 "+A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA4[axfA_A__BROKER_ID, axfA_A__P])";
 "+A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA4[axfA_A__BROKER_ID, axfA_A__P])";
 "-A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(axfA_A__V*axfA1[axfA_A__BROKER_ID, axfA_A__P]*-1)";
 "-A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA2[axfA_A__BROKER_ID, axfA_A__P]*-1)";
 "-A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA2[axfA_A__BROKER_ID, axfA_A__P]*-1)";
 "-A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(axfA_A__V*axfA3[axfA_A__BROKER_ID, axfA_A__P]*-1)";
 "-A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA4[axfA_A__BROKER_ID, axfA_A__P]*-1)";
 "-A(axfA_A__BROKER_ID, axfA_A__P, axfA_A__V): axf[axfA_A__BROKER_ID] += "^
    "(-1*axfA4[axfA_A__BROKER_ID, axfA_A__P]*-1)";
 "+B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "axfB1[axfB_B__BROKER_ID, axfB_B__P]";
 "+B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB2[axfB_B__P, axfB_B__BROKER_ID])";
 "+B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB2[axfB_B__P, axfB_B__BROKER_ID])";
 "+B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "axfB3[axfB_B__BROKER_ID, axfB_B__P]";
 "+B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB4[axfB_B__P, axfB_B__BROKER_ID])";
 "+B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB4[axfB_B__P, axfB_B__BROKER_ID])";
 "-B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(axfB1[axfB_B__BROKER_ID, axfB_B__P]*-1)";
 "-B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB2[axfB_B__P, axfB_B__BROKER_ID]*-1)";
 "-B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB2[axfB_B__P, axfB_B__BROKER_ID]*-1)";
 "-B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(axfB3[axfB_B__BROKER_ID, axfB_B__P]*-1)";
 "-B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB4[axfB_B__P, axfB_B__BROKER_ID]*-1)";
 "-B(axfB_B__BROKER_ID, axfB_B__P, axfB_B__V): axf[axfB_B__BROKER_ID] += "^
    "(-1*axfB4[axfB_B__P, axfB_B__BROKER_ID]*-1)";
 "+A(axfB4A_A__BROKER_ID, axfB4A_A__P, axfB4A_A__V): "^
    "foreach axfB_B__P do axfB4[axfB_B__P, axfB4A_A__BROKER_ID] += "^
    "(if 1000.<(axfB_B__P+(-1*axfB4A_A__P)) then axfB_B__P else 0)";
 "-A(axfB4A_A__BROKER_ID, axfB4A_A__P, axfB4A_A__V): "^
    "foreach axfB_B__P do axfB4[axfB_B__P, axfB4A_A__BROKER_ID] += "^
    "((if 1000.<(axfB_B__P+(-1*axfB4A_A__P)) then axfB_B__P else 0)*-1)";
 "+A(axfB3A_A__BROKER_ID, axfB3A_A__P, axfB3A_A__V): "^
    "foreach axfB_B__P do axfB3[axfB3A_A__BROKER_ID, axfB_B__P] += "^
    "(axfB3A_A__V*(if 1000.<(axfB_B__P+(-1*axfB3A_A__P)) then 1 else 0))";
 "-A(axfB3A_A__BROKER_ID, axfB3A_A__P, axfB3A_A__V): "^
    "foreach axfB_B__P do axfB3[axfB3A_A__BROKER_ID, axfB_B__P] += "^
    "(axfB3A_A__V*(if 1000.<(axfB_B__P+(-1*axfB3A_A__P)) then 1 else 0)*-1)";
 "+A(axfB2A_A__BROKER_ID, axfB2A_A__P, axfB2A_A__V): "^
    "foreach axfB_B__P do axfB2[axfB_B__P, axfB2A_A__BROKER_ID] += "^
    "(if 1000.<(axfB2A_A__P+(-1*axfB_B__P)) then axfB_B__P else 0)";
 "-A(axfB2A_A__BROKER_ID, axfB2A_A__P, axfB2A_A__V): "^
    "foreach axfB_B__P do axfB2[axfB_B__P, axfB2A_A__BROKER_ID] += "^
    "((if 1000.<(axfB2A_A__P+(-1*axfB_B__P)) then axfB_B__P else 0)*-1)";
 "+A(axfB1A_A__BROKER_ID, axfB1A_A__P, axfB1A_A__V): "^
    "foreach axfB_B__P do axfB1[axfB1A_A__BROKER_ID, axfB_B__P] += "^
    "(axfB1A_A__V*(if 1000.<(axfB1A_A__P+(-1*axfB_B__P)) then 1 else 0))";
 "-A(axfB1A_A__BROKER_ID, axfB1A_A__P, axfB1A_A__V): "^
    "foreach axfB_B__P do axfB1[axfB1A_A__BROKER_ID, axfB_B__P] += "^
    "(axfB1A_A__V*(if 1000.<(axfB1A_A__P+(-1*axfB_B__P)) then 1 else 0)*-1)";
 "+B(axfA4B_B__BROKER_ID, axfA4B_B__P, axfA4B_B__V): "^
    "foreach axfA_A__P do axfA4[axfA4B_B__BROKER_ID, axfA_A__P] += "^
    "(if 1000.<(axfA4B_B__P+(-1*axfA_A__P)) then axfA4B_B__P else 0)";
 "-B(axfA4B_B__BROKER_ID, axfA4B_B__P, axfA4B_B__V): "^
    "foreach axfA_A__P do axfA4[axfA4B_B__BROKER_ID, axfA_A__P] += "^
    "((if 1000.<(axfA4B_B__P+(-1*axfA_A__P)) then axfA4B_B__P else 0)*-1)";
 "+B(axfA3B_B__BROKER_ID, axfA3B_B__P, axfA3B_B__V): "^
    "foreach axfA_A__P do axfA3[axfA3B_B__BROKER_ID, axfA_A__P] += "^
    "(if 1000.<(axfA3B_B__P+(-1*axfA_A__P)) then 1 else 0)";
 "-B(axfA3B_B__BROKER_ID, axfA3B_B__P, axfA3B_B__V): "^
    "foreach axfA_A__P do axfA3[axfA3B_B__BROKER_ID, axfA_A__P] += "^
    "((if 1000.<(axfA3B_B__P+(-1*axfA_A__P)) then 1 else 0)*-1)";
 "+B(axfA2B_B__BROKER_ID, axfA2B_B__P, axfA2B_B__V): "^
    "foreach axfA_A__P do axfA2[axfA2B_B__BROKER_ID, axfA_A__P] += "^
    "(if 1000.<(axfA_A__P+(-1*axfA2B_B__P)) then axfA2B_B__P else 0)";
 "-B(axfA2B_B__BROKER_ID, axfA2B_B__P, axfA2B_B__V): "^
    "foreach axfA_A__P do axfA2[axfA2B_B__BROKER_ID, axfA_A__P] += "^
    "((if 1000.<(axfA_A__P+(-1*axfA2B_B__P)) then axfA2B_B__P else 0)*-1)";
 "+B(axfA1B_B__BROKER_ID, axfA1B_B__P, axfA1B_B__V): "^
    "foreach axfA_A__P do axfA1[axfA1B_B__BROKER_ID, axfA_A__P] += "^
    "(if 1000.<(axfA_A__P+(-1*axfA1B_B__P)) then 1 else 0)";
 "-B(axfA1B_B__BROKER_ID, axfA1B_B__P, axfA1B_B__V): "^
    "foreach axfA_A__P do axfA1[axfA1B_B__BROKER_ID, axfA_A__P] += "^
    "((if 1000.<(axfA_A__P+(-1*axfA1B_B__P)) then 1 else 0)*-1)"]
);;
