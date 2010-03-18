open Calculus;;

let sch = [("C", ["cid"; "nation"])];;

(* for each supplier, the number of customers of the same nation *)
let q = make_term(RVal(AggSum(RVal(Const (Int 1)),
   RA_MultiNatJoin[
      RA_Leaf(Rel("C", ["cid"; "nation"]));
      RA_Leaf(Rel("C", ["cid2"; "nation"])) ])));;

Compiler.compile Calculus.ModeOpenDomain sch
   (Compiler.mk_external "q" ["cid"]) q
;;



(*
+C(cid, nation) {
 q[cid] += qC1[nation];
 foreach cid2 do q[cid2] += qC2[cid2, nation];
 q[cid] += 1;
 qC1[nation] += 1;
 qC2[cid, nation] += 1
}

-C(cid, nation) {
 q[cid] -= qC1[nation];
 foreach cid2 do q[cid2] -= qC2[cid2, nation];
 q[cid] += 1;
 qC1[nation] -= 1;
 qC2[cid, nation] -= 1
}

*)



