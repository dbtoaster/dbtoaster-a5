open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;

let sch =
[("C", [("cid", TInt); ("nation", TInt)]);
 ("S", [("sid", TInt); ("nation", TInt)])];;

(* for each supplier, the number of customers of the same nation 
  select cid, sum(1) from Customer,Supplier where cid=sid  *)
let q = make_term( RVal(AggSum(
   RVal(Const (Int 1)),
   RA_MultiNatJoin[
      RA_Leaf(Rel("C", [("cid", TInt); ("nation", TInt)]));
      RA_Leaf(Rel("S", [("sid", TInt); ("nation", TInt)]))
]
)));;

Debug.log_unit_test "Customers/suppliers in nation" (String.concat "\n")
(Compiler.compile Calculus.ModeOpenDomain sch
   (q, (map_term "q" [("sid", TInt)])) cg [])
(
["+C(qC_cid, qC_nation): foreach sid do q[sid] += qC1[sid, qC_nation]";
 "-C(qC_cid, qC_nation): foreach sid do q[sid] += (qC1[sid, qC_nation]*-1)";
 "+S(qS_sid, qS_nation): q[qS_sid] += qS1[qS_nation]";
 "-S(qS_sid, qS_nation): q[qS_sid] += (qS1[qS_nation]*-1)";
 "+C(qS1C_cid, qS1C_nation): qS1[qS1C_nation] += 1";
 "-C(qS1C_cid, qS1C_nation): qS1[qS1C_nation] += -1";
 "+S(qC1S_sid, qC1S_nation): qC1[qC1S_sid, qC1S_nation] += 1";
 "-S(qC1S_sid, qC1S_nation): qC1[qC1S_sid, qC1S_nation] += -1"]
);; 



