open Calculus;;

let sch =
[("C", ["cid"; "nation"]);
 ("S", ["sid"; "nation"])];;

(* for each supplier, the number of customers of the same nation *)
let q = make_term( RVal(AggSum(
   RVal(Const (Int 1)),
   RA_MultiNatJoin[
      RA_Leaf(Rel("C", ["cid"; "nation"]));
      RA_Leaf(Rel("S", ["sid"; "nation"]))
]
)));;


Compiler.compile Calculus.ModeOpenDomain sch
   (Compiler.mk_external "q" ["sid"]) q
;;



(*
+C(cid, nation):
 foreach sid do q[sid] += qC1[sid, nation];
 qS1[nation] += 1

+S(sid, nation):
 q[sid] += qS1[nation];
 qC1[sid, nation] += 1
*)



