open Algebra;;


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
Compiler.compile tpch_sch "q" ["PK"]
   (make_term(RVal(AggSum(RVal(Var("PR")), RA_MultiNatJoin [relLI]))))
= ["+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): q[x_qLI_PK] += x_qLI_PR"]
;;

(* select sum(O.ExchangeRate * LI.Price)
   from   Order O, LineItem LI
   where  O.OK = LI.OK *)
Compiler.compile tpch_sch "q" []
   (make_term(RVal(AggSum(RProd[RVal(Var("PR")); RVal(Var("XCH"))],
                          RA_MultiNatJoin [relO; relLI]))))
=
["+O(x_qO_OK, x_qO_CK, x_qO_DA, x_qO_XCH): q[] += (qO1_1[x_qO_OK]*x_qO_XCH)";
 "+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): q[] += (x_qLI_PR*qLI1_1[x_qLI_OK])";
 "+LI(x_qO1_1LI_OK, x_qO1_1LI_PK, x_qO1_1LI_SK, x_qO1_1LI_PR): qO1_1[x_qO1_1LI_OK] += x_qO1_1LI_PR";
 "+O(x_qLI1_1O_OK, x_qLI1_1O_CK, x_qLI1_1O_DA, x_qLI1_1O_XCH): qLI1_1[x_qLI1_1O_OK] += x_qLI1_1O_XCH"]
;;

(* number of parts supplied by supplier, by supplier name *)
Compiler.compile tpch_sch "q" ["SN"]
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                          RA_MultiNatJoin [relS; relPS]))))
=
["+PS(x_qPS_PK, x_qPS_SK): foreach SN do q[SN] += qPS1_1[x_qPS_SK, SN]";
 "+S(x_qS_SK, x_qS_SN): q[x_qS_SN] += qS1_1[x_qS_SK]";
 "+S(x_qPS1_1S_SK, x_qPS1_1S_SN): qPS1_1[x_qPS1_1S_SK, x_qPS1_1S_SN] += 1";
 "+PS(x_qS1_1PS_PK, x_qS1_1PS_SK): qS1_1[x_qS1_1PS_SK] += 1"]
;;

(* profits by customer *)
Compiler.compile tpch_sch "q" ["CK"]
   (make_term(RVal(AggSum(RVal(Var "PR"),
                          RA_MultiNatJoin [relO; relLI]))))
=
["+O(x_qO_OK, x_qO_CK, x_qO_DA, x_qO_XCH): q[x_qO_CK] += qO1_1[x_qO_OK]";
 "+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): foreach CK do q[CK] += (x_qLI_PR*qLI1_1[x_qLI_OK, CK])";
 "+LI(x_qO1_1LI_OK, x_qO1_1LI_PK, x_qO1_1LI_SK, x_qO1_1LI_PR): qO1_1[x_qO1_1LI_OK] += x_qO1_1LI_PR";
 "+O(x_qLI1_1O_OK, x_qLI1_1O_CK, x_qLI1_1O_DA, x_qLI1_1O_XCH): qLI1_1[x_qLI1_1O_OK, x_qLI1_1O_CK] += 1"]
;;


(* a monstrous cyclic query *)
Compiler.compile tpch_sch "q" []
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                          RA_MultiNatJoin [relO; relLI; relPS]))))
=
["+PS(x_qPS_PK, x_qPS_SK): q[] += qPS1_1[x_qPS_PK, x_qPS_SK]";
 "+O(x_qO_OK, x_qO_CK, x_qO_DA, x_qO_XCH): q[] += qO1_1[x_qO_OK]";
 "+LI(x_qLI_OK, x_qLI_PK, x_qLI_SK, x_qLI_PR): q[] += (qLI1_1[x_qLI_OK]*qLI1_2[x_qLI_PK, x_qLI_SK])";
 "+O(x_qPS1_1O_OK, x_qPS1_1O_CK, x_qPS1_1O_DA, x_qPS1_1O_XCH): foreach x_qPS_PK, x_qPS_SK do qPS1_1[x_qPS_PK, x_qPS_SK] += qPS1_1O1_1[x_qPS1_1O_OK, x_qPS_PK, x_qPS_SK]";
 "+LI(x_qPS1_1LI_OK, x_qPS1_1LI_PK, x_qPS1_1LI_SK, x_qPS1_1LI_PR): qPS1_1[x_qPS1_1LI_PK, x_qPS1_1LI_SK] += qPS1_1LI1_1[x_qPS1_1LI_OK]";
 "+LI(x_qPS1_1O1_1LI_OK, x_qPS1_1O1_1LI_PK, x_qPS1_1O1_1LI_SK, x_qPS1_1O1_1LI_PR): qPS1_1O1_1[x_qPS1_1O1_1LI_OK, x_qPS1_1O1_1LI_PK, x_qPS1_1O1_1LI_SK] += 1";
 "+O(x_qPS1_1LI1_1O_OK, x_qPS1_1LI1_1O_CK, x_qPS1_1LI1_1O_DA, x_qPS1_1LI1_1O_XCH): qPS1_1LI1_1[x_qPS1_1LI1_1O_OK] += 1";
 "+PS(x_qO1_1PS_PK, x_qO1_1PS_SK): foreach x_qO_OK do qO1_1[x_qO_OK] += qO1_1PS1_1[x_qO_OK, x_qO1_1PS_PK, x_qO1_1PS_SK]";
 "+LI(x_qO1_1LI_OK, x_qO1_1LI_PK, x_qO1_1LI_SK, x_qO1_1LI_PR): qO1_1[x_qO1_1LI_OK] += qO1_1LI1_1[x_qO1_1LI_PK, x_qO1_1LI_SK]";
 "+LI(x_qO1_1PS1_1LI_OK, x_qO1_1PS1_1LI_PK, x_qO1_1PS1_1LI_SK, x_qO1_1PS1_1LI_PR): qO1_1PS1_1[x_qO1_1PS1_1LI_OK, x_qO1_1PS1_1LI_PK, x_qO1_1PS1_1LI_SK] += 1";
 "+PS(x_qO1_1LI1_1PS_PK, x_qO1_1LI1_1PS_SK): qO1_1LI1_1[x_qO1_1LI1_1PS_PK, x_qO1_1LI1_1PS_SK] += 1";
 "+O(x_qLI1_1O_OK, x_qLI1_1O_CK, x_qLI1_1O_DA, x_qLI1_1O_XCH): qLI1_1[x_qLI1_1O_OK] += 1";
 "+PS(x_qLI1_2PS_PK, x_qLI1_2PS_SK): qLI1_2[x_qLI1_2PS_PK, x_qLI1_2PS_SK] += 1"]
;;





