open Calculus;;

let sch = [("C", ["cid"; "nation"])];;

(* for each supplier, the number of customers of the same nation *)
let q = make_term(RVal(AggSum(RVal(Const (Int 1)),
   RA_MultiNatJoin[
      RA_Leaf(Rel("C", ["cid"; "nation1"]));
      RA_Leaf(Rel("C", ["cid2"; "nation2"]));
      RA_Leaf(AtomicConstraint(Neq, RVal (Var "nation1"), RVal (Var "nation2"))) ])));;

Compiler.compile Calculus.ModeOpenDomain sch
   (Compiler.mk_external "q" ["cid"]) q
;;



(*
+C(cid, nation):
 q[cid] += qC1[nation];
 foreach cid2 do q[cid2] += qC2[cid2, nation];

 q[cid] += (if nation<>nation then 1 else 0);

 foreach nation2 do qC1[nation2] += if nation2<>nation then 1 else 0;
 foreach nation2 do qC2[cid, nation2] += if nation<>nation2 then 1 else 0
*)



