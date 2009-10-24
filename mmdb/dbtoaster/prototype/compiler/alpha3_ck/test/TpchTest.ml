open Calculus;;


let schP  = ("P",  ["PK"; "PN"]);;
let schPS = ("PS", ["PK"; "SK"]);;
let schS  = ("S",  ["SK"; "SN"]);;
let schC  = ("C",  ["CK"; "CN"]);;
let schO  = ("O",  ["OK"; "CK"; "DA"; "XCH"]);;
let schLI = ("LI", ["OK"; "PK"; "SK"; "PR"]);;

let tpch_sch = [schP; schPS; schS; schC; schO; schLI];;

let relP  = RA_Leaf(Rel("P", ["PK"; "PN"]));;
let relPS = RA_Leaf(Rel("PS", ["PK"; "SK"]));;
let relS = RA_Leaf(Rel("S", ["SK"; "SN"]));;
let relC  = RA_Leaf(Rel("C", ["CK"; "CN"]));;
let relO  = RA_Leaf(Rel("O", ["OK"; "CK"; "DA"; "XCH"]));;
let relLI = RA_Leaf(Rel("LI", ["OK"; "PK"; "SK"; "PR"]));;


(* select sum(LI.Price) from LineItem LI group by LI.PK *)
Compiler.compile tpch_sch (Compiler.mk_external "q" ["PK"]) []
   (make_term(RVal(AggSum(RVal(Var("PR")), RA_MultiNatJoin [relLI]))))
= ["+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): q[x_qLI_PK] += x_qLI_PR"]
;;

(* select sum(O.ExchangeRate * LI.Price)
   from   Order O, LineItem LI
   where  O.OK = LI.OK *)
Compiler.compile tpch_sch (Compiler.mk_external "q" []) []
   (make_term(RVal(AggSum(RProd[RVal(Var("PR")); RVal(Var("XCH"))],
                          RA_MultiNatJoin [relO; relLI]))))
=
["+O(x_qO_OK, x_qO_CK, x_qO_DA, x_qO_XCH): q[] += (qO1[x_qO_OK]*x_qO_XCH)";
 "+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): q[] += (x_qLI_PR*qLI1[x_qLI_OK])";
 "+LI(x_qO1LI_OK, x_qO1LI_PK, x_qO1LI_SK, x_qO1LI_PR): qO1[x_qO1LI_OK] += x_qO1LI_PR";
 "+O(x_qLI1O_OK, x_qLI1O_CK, x_qLI1O_DA, x_qLI1O_XCH): qLI1[x_qLI1O_OK] += x_qLI1O_XCH"]
;;



(* number of parts supplied by supplier, by supplier name *)
Compiler.compile tpch_sch (Compiler.mk_external "q" ["SN"]) []
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                          RA_MultiNatJoin [relS; relPS]))))
=
["+PS(x_qPS_PK, x_qPS_SK): foreach SN do q[SN] += qPS1[x_qPS_SK, SN]";
 "+S(x_qS_SK, x_qS_SN): q[x_qS_SN] += qS1[x_qS_SK]";
 "+S(x_qPS1S_SK, x_qPS1S_SN): qPS1[x_qPS1S_SK, x_qPS1S_SN] += 1";
 "+PS(x_qS1PS_PK, x_qS1PS_SK): qS1[x_qS1PS_SK] += 1"]
;;

(* profits by customer *)
Compiler.compile tpch_sch (Compiler.mk_external "q" ["CK"]) []
   (make_term(RVal(AggSum(RVal(Var "PR"),
                          RA_MultiNatJoin [relO; relLI]))))
=
["+O(x_qO_OK, x_qO_CK, x_qO_DA, x_qO_XCH): q[x_qO_CK] += qO1[x_qO_OK]";
 "+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): foreach CK do q[CK] += (x_qLI_PR*qLI1[x_qLI_OK, CK])";
 "+LI(x_qO1LI_OK, x_qO1LI_PK, x_qO1LI_SK, x_qO1LI_PR): qO1[x_qO1LI_OK] += x_qO1LI_PR";
 "+O(x_qLI1O_OK, x_qLI1O_CK, x_qLI1O_DA, x_qLI1O_XCH): qLI1[x_qLI1O_OK, x_qLI1O_CK] += 1"]
;;


(* a monstrous cyclic query *)
Compiler.compile tpch_sch (Compiler.mk_external "q" []) []
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                          RA_MultiNatJoin [relO; relLI; relPS]))))
=
["+PS(x_qPS_PK, x_qPS_SK): q[] += qPS1[x_qPS_PK, x_qPS_SK]";
 "+O(x_qO_OK, x_qO_CK, x_qO_DA, x_qO_XCH): q[] += qO1[x_qO_OK]";
 "+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): q[] += (qLI1[x_qLI_OK]*qLI2[x_qLI_PK, x_qLI_SK])";
 "+O(x_qPS1O_OK, x_qPS1O_CK, x_qPS1O_DA, x_qPS1O_XCH): foreach x_qPS_PK, x_qPS_SK do qPS1[x_qPS_PK, x_qPS_SK] += qPS1O1[x_qPS1O_OK, x_qPS_PK, x_qPS_SK]";
 "+LI(x_qPS1LI_OK, x_qPS1LI_PK, x_qPS1LI_SK, x_qPS1LI_PR): qPS1[x_qPS1LI_PK, x_qPS1LI_SK] += qPS1LI1[x_qPS1LI_OK]";
 "+LI(x_qPS1O1LI_OK, x_qPS1O1LI_PK, x_qPS1O1LI_SK, x_qPS1O1LI_PR): qPS1O1[x_qPS1O1LI_OK, x_qPS1O1LI_PK, x_qPS1O1LI_SK] += 1";
 "+O(x_qPS1LI1O_OK, x_qPS1LI1O_CK, x_qPS1LI1O_DA, x_qPS1LI1O_XCH): qPS1LI1[x_qPS1LI1O_OK] += 1";
 "+PS(x_qO1PS_PK, x_qO1PS_SK): foreach x_qO_OK do qO1[x_qO_OK] += qO1PS1[x_qO_OK, x_qO1PS_PK, x_qO1PS_SK]";
 "+LI(x_qO1LI_OK, x_qO1LI_PK, x_qO1LI_SK, x_qO1LI_PR): qO1[x_qO1LI_OK] += qO1LI1[x_qO1LI_PK, x_qO1LI_SK]";
 "+LI(x_qO1PS1LI_OK, x_qO1PS1LI_PK, x_qO1PS1LI_SK, x_qO1PS1LI_PR): qO1PS1[x_qO1PS1LI_OK, x_qO1PS1LI_PK, x_qO1PS1LI_SK] += 1";
 "+PS(x_qO1LI1PS_PK, x_qO1LI1PS_SK): qO1LI1[x_qO1LI1PS_PK, x_qO1LI1PS_SK] += 1";
 "+O(x_qLI1O_OK, x_qLI1O_CK, x_qLI1O_DA, x_qLI1O_XCH): qLI1[x_qLI1O_OK] += 1";
 "+PS(x_qLI2PS_PK, x_qLI2PS_SK): qLI2[x_qLI2PS_PK, x_qLI2PS_SK] += 1"]
;;





