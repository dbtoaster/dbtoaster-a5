open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;
let test_query s = Debug.log_unit_test s (String.concat "\n");;

(* Person P *)
let sch = [("P", [("id", TInt); ("prop", TInt)])];;

(* matching people; prop1*prop2 >= 5 stands for
   score(\vec{prop1}, \vec{prop2}) >= threshold *)
let q = make_term(
   RVal(AggSum(RVal(Const(Int 1)),
               RA_MultiNatJoin[
   RA_Leaf(Rel("P", [("id1", TInt); ("prop1", TInt)]));
   RA_Leaf(Rel("P", [("id2", TInt); ("prop2", TInt)]));
   RA_Leaf(AtomicConstraint(Neq, RVal(Var ("id1", TInt)), RVal(Var ("id2", TInt))));
   RA_Leaf(AtomicConstraint(Le, RVal(Const(Int 5)),
      RProd[RVal(Var ("prop1", TInt)); RVal(Var ("prop2", TInt))]))
])))
in
test_query "select count() P P1, P P2 where P1.id <> P2.id and P1.prop * P2.prop >= 5"
(Compiler.compile Calculus.ModeOpenDomain sch
   (q, (map_term "q" [("id1", TInt)])) cg [])
(
["+P(qP_id, qP_prop): q[qP_id] += qP1[qP_id, qP_prop]";
 "+P(qP_id, qP_prop): foreach id1 do q[id1] += qP2[id1, qP_id, qP_prop]";
 "+P(qP_id, qP_prop): q[qP_id] += ((if qP_id<>qP_id then 1 else 0)*(if 5<=(qP_prop*qP_prop) then 1 else 0))";
 "-P(qP_id, qP_prop): q[qP_id] += (qP1[qP_id, qP_prop]*-1)";
 "-P(qP_id, qP_prop): foreach id1 do q[id1] += (qP2[id1, qP_id, qP_prop]*-1)";
 "-P(qP_id, qP_prop): q[qP_id] += ((if qP_id<>qP_id then 1 else 0)*(if 5<=(qP_prop*qP_prop) then 1 else 0))";
 "+P(qP2P_id, qP2P_prop): foreach qP_id, qP_prop do qP2[qP2P_id, qP_id, qP_prop] += ((if qP2P_id<>qP_id then 1 else 0)*(if 5<=(qP2P_prop*qP_prop) then 1 else 0))";
 "-P(qP2P_id, qP2P_prop): foreach qP_id, qP_prop do qP2[qP2P_id, qP_id, qP_prop] += ((if qP2P_id<>qP_id then 1 else 0)*(if 5<=(qP2P_prop*qP_prop) then 1 else 0)*-1)";
 "+P(qP1P_id, qP1P_prop): foreach qP_id, qP_prop do qP1[qP_id, qP_prop] += ((if qP_id<>qP1P_id then 1 else 0)*(if 5<=(qP_prop*qP1P_prop) then 1 else 0))";
 "-P(qP1P_id, qP1P_prop): foreach qP_id, qP_prop do qP1[qP_id, qP_prop] += ((if qP_id<>qP1P_id then 1 else 0)*(if 5<=(qP_prop*qP1P_prop) then 1 else 0)*-1)"]
);;



(* (insert only...)
+Person(id, prop):
 q[id] += qP1[id, prop];

 ($ID2 in distinct(pi_1(Person)))
 q[$ID2] += qP2[$ID2, id, prop];

 (($ID2, $PROP2) in distinct(Person))
 qP1[$ID2, $PROP2] +=
   if id<>$ID2 and 5<=(prop*$PROP2) then 1 else 0;

 (($ID2, $PROP2) in distinct(Person))
 qP1[id, $ID2, $PROP2] +=
   if id<>$ID2 and 5<=(prop*$PROP2) then 1 else 0;
*)


