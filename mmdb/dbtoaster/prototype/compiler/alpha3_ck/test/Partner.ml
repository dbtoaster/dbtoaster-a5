open Calculus;;

(* Person P *)
let sch = [("P", ["id"; "prop"])];;

(* matching people; prop1*prop2 >= 5 stands for
   score(\vec{prop1}, \vec{prop2}) >= threshold *)
let q = make_term(
   RVal(AggSum(RVal(Const(Int 1)),
               RA_MultiNatJoin[
   RA_Leaf(Rel("P", ["id1"; "prop1"]));
   RA_Leaf(Rel("P", ["id2"; "prop2"]));
   RA_Leaf(AtomicConstraint(Neq, RVal(Var "id1"), RVal(Var "id2")));
   RA_Leaf(AtomicConstraint(Le, RVal(Const(Int 5)),
      RProd[RVal(Var "prop1"); RVal(Var "prop2")]))
])));;


Compiler.compile Calculus.ModeOpenDomain sch
   (Compiler.mk_external "q" ["id1"]) q =
["+P(x_qP_id, x_qP_prop): q[x_qP_id] += qP1[x_qP_id, x_qP_prop]";
 "+P(x_qP_id, x_qP_prop): foreach id1 do q[id1] += qP2[id1, x_qP_id, x_qP_prop]";
 "+P(x_qP_id, x_qP_prop): q[x_qP_id] += ((if x_qP_id<>x_qP_id then 1 else 0)*(if 5<=(x_qP_prop*x_qP_prop) then 1 else 0))";
 "+P(x_qP1P_id, x_qP1P_prop): foreach x_qP_id, x_qP_prop do qP1[x_qP_id, x_qP_prop] += ((if x_qP_id<>x_qP1P_id then 1 else 0)*(if 5<=(x_qP_prop*x_qP1P_prop) then 1 else 0))";
 "+P(x_qP2P_id, x_qP2P_prop): foreach x_qP_id, x_qP_prop do qP2[x_qP2P_id, x_qP_id, x_qP_prop] += ((if x_qP2P_id<>x_qP_id then 1 else 0)*(if 5<=(x_qP2P_prop*x_qP_prop) then 1 else 0))"]
;;



(*
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


