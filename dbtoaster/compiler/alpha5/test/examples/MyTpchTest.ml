open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;
let test_query s = Debug.log_unit_test s (String.concat "\n");;

let schP  = ("P",  [("PK", TInt); ("PN", TInt)]);;
let schPS = ("PS", [("PK", TInt); ("SK", TInt)]);;
let schS  = ("S",  [("SK", TInt); ("SN", TInt)]);;
let schC  = ("C",  [("CK", TInt); ("CN", TInt)]);;
let schO  = ("O",  [("OK", TInt); ("CK", TInt); ("DA", TInt); ("XCH", TInt)]);;
let schLI = ("LI", [("OK", TInt); ("PK", TInt); ("SK", TInt); ("PR", TInt)]);;

let tpch_sch = [schP; schPS; schS; schC; schO; schLI];;

let relP  = RA_Leaf(Rel("P", [("PK", TInt); ("PN", TInt)]));;
let relPS = RA_Leaf(Rel("PS", [("PK", TInt); ("SK", TInt)]));;
let relS = RA_Leaf(Rel("S", [("SK", TInt); ("SN", TInt)]));;
let relC  = RA_Leaf(Rel("C", [("CK", TInt); ("CN", TInt)]));;
let relO  = RA_Leaf(Rel("O", [("OK", TInt); ("CK", TInt); ("DA", TInt); ("XCH", TInt)]));;
let relLI = RA_Leaf(Rel("LI", [("OK", TInt); ("PK", TInt); ("SK", TInt); ("PR", TInt)]));;


(* select sum(LI.Price) from LineItem LI group by LI.PK *)
let q =
   (make_term(RVal(AggSum(RVal(Var(("PR", TInt))), RA_MultiNatJoin [relLI])))) 
in
test_query "select sum(LI.Price) from LineItem LI group by LI.PK"
(Compiler.compile Calculus.ModeExtractFromCond tpch_sch
   (q, (map_term "q" [("PK", TInt)])) cg [])   
(["+LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): q[qLI_PK] += qLI_PR";
  "-LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): q[qLI_PK] += (qLI_PR*-1)";])
;;

(* select sum(O.ExchangeRate * LI.Price)
   from   Order O, LineItem LI
   where  O.OK = LI.OK *)
let q = 
   (make_term(RVal(AggSum(RProd[RVal(Var(("PR", TInt))); RVal(Var(("XCH", TInt)))],
                          RA_MultiNatJoin [relO; relLI]))))
in
test_query "select sum(O.XCH*LI.P) from O,LI where O.OK = LI.OK"
(Compiler.compile Calculus.ModeExtractFromCond
                 tpch_sch (q,(map_term "q" [])) cg [])
(
["+O(qO_OK, qO_CK, qO_DA, qO_XCH): q[] += (qO1[qO_OK]*qO_XCH)";
 "-O(qO_OK, qO_CK, qO_DA, qO_XCH): q[] += (qO1[qO_OK]*qO_XCH*-1)";
 "+LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): q[] += (qLI_PR*qLI1[qLI_OK])";
 "-LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): q[] += (qLI_PR*qLI1[qLI_OK]*-1)";
 "+O(qLI1O_OK, qLI1O_CK, qLI1O_DA, qLI1O_XCH): qLI1[qLI1O_OK] += qLI1O_XCH";
 "-O(qLI1O_OK, qLI1O_CK, qLI1O_DA, qLI1O_XCH): qLI1[qLI1O_OK] += (qLI1O_XCH*-1)";
 "+LI(qO1LI_OK, qO1LI_PK, qO1LI_SK, qO1LI_PR): qO1[qO1LI_OK] += qO1LI_PR";
 "-LI(qO1LI_OK, qO1LI_PK, qO1LI_SK, qO1LI_PR): qO1[qO1LI_OK] += (qO1LI_PR*-1)"]
);;


(* number of parts supplied by supplier, by supplier name *)
let q =
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                          RA_MultiNatJoin [relS; relPS]))))
in
test_query "select SN,count() from S,PS where S.SK=PS.SK group by SN"
(Compiler.compile Calculus.ModeExtractFromCond
                 tpch_sch (q, (map_term "q" [("SN", TInt)])) cg [])
(
["+PS(qPS_PK, qPS_SK): foreach SN do q[SN] += qPS1[qPS_SK, SN]";
 "-PS(qPS_PK, qPS_SK): foreach SN do q[SN] += (qPS1[qPS_SK, SN]*-1)";
 "+S(qS_SK, qS_SN): q[qS_SN] += qS1[qS_SK]";
 "-S(qS_SK, qS_SN): q[qS_SN] += (qS1[qS_SK]*-1)";
 "+PS(qS1PS_PK, qS1PS_SK): qS1[qS1PS_SK] += 1";
 "-PS(qS1PS_PK, qS1PS_SK): qS1[qS1PS_SK] += -1";
 "+S(qPS1S_SK, qPS1S_SN): qPS1[qPS1S_SK, qPS1S_SN] += 1";
 "-S(qPS1S_SK, qPS1S_SN): qPS1[qPS1S_SK, qPS1S_SN] += -1"]
);;

(* profits by customer *)
let q = 
   (make_term(RVal(AggSum(RVal(Var ("PR", TInt)),
                          RA_MultiNatJoin [relO; relLI]))))
in
test_query "select CK, sum(PR) from O,LI where O.OK = LI.OK group by CK"
(Compiler.compile Calculus.ModeExtractFromCond
                 tpch_sch (q, (map_term "q" [("CK", TInt)])) cg [])
(
["+O(qO_OK, qO_CK, qO_DA, qO_XCH): q[qO_CK] += qO1[qO_OK]";
 "-O(qO_OK, qO_CK, qO_DA, qO_XCH): q[qO_CK] += (qO1[qO_OK]*-1)";
 "+LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): foreach CK do q[CK] += (qLI_PR*qLI1[qLI_OK, CK])";
 "-LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): foreach CK do q[CK] += (qLI_PR*qLI1[qLI_OK, CK]*-1)";
 "+O(qLI1O_OK, qLI1O_CK, qLI1O_DA, qLI1O_XCH): qLI1[qLI1O_OK, qLI1O_CK] += 1";
 "-O(qLI1O_OK, qLI1O_CK, qLI1O_DA, qLI1O_XCH): qLI1[qLI1O_OK, qLI1O_CK] += -1";
 "+LI(qO1LI_OK, qO1LI_PK, qO1LI_SK, qO1LI_PR): qO1[qO1LI_OK] += qO1LI_PR";
 "-LI(qO1LI_OK, qO1LI_PK, qO1LI_SK, qO1LI_PR): qO1[qO1LI_OK] += (qO1LI_PR*-1)"]
);;


(* a monstrous cyclic query *)
let q = 
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                          RA_MultiNatJoin [relO; relLI; relPS]))))
in
test_query ("select count() from O,LI,PS "^
            "where O.OK = LI.OK and LI.PK = PS.PK and LI.SK = PS.SK")
(Compiler.compile Calculus.ModeExtractFromCond
                 tpch_sch (q, (map_term "q" [])) cg [])
(
["+PS(qPS_PK, qPS_SK): q[] += qPS1[qPS_PK, qPS_SK]";
 "-PS(qPS_PK, qPS_SK): q[] += (qPS1[qPS_PK, qPS_SK]*-1)";
 "+O(qO_OK, qO_CK, qO_DA, qO_XCH): q[] += qO1[qO_OK]";
 "-O(qO_OK, qO_CK, qO_DA, qO_XCH): q[] += (qO1[qO_OK]*-1)";
 "+LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): q[] += (qLI1[qLI_OK]*qLI2[qLI_PK, qLI_SK])";
 "-LI(qLI_OK, qLI_PK, qLI_SK, qLI_PR): q[] += (qLI1[qLI_OK]*qLI2[qLI_PK, qLI_SK]*-1)";
 "+PS(qLI2PS_PK, qLI2PS_SK): qLI2[qLI2PS_PK, qLI2PS_SK] += 1";
 "-PS(qLI2PS_PK, qLI2PS_SK): qLI2[qLI2PS_PK, qLI2PS_SK] += -1";
 "+O(qLI1O_OK, qLI1O_CK, qLI1O_DA, qLI1O_XCH): qLI1[qLI1O_OK] += 1";
 "-O(qLI1O_OK, qLI1O_CK, qLI1O_DA, qLI1O_XCH): qLI1[qLI1O_OK] += -1";
 "+PS(qO1PS_PK, qO1PS_SK): foreach qO_OK do qO1[qO_OK] += qO1PS1[qO_OK, qO1PS_PK, qO1PS_SK]";
 "-PS(qO1PS_PK, qO1PS_SK): foreach qO_OK do qO1[qO_OK] += (qO1PS1[qO_OK, qO1PS_PK, qO1PS_SK]*-1)";
 "+LI(qO1LI_OK, qO1LI_PK, qO1LI_SK, qO1LI_PR): qO1[qO1LI_OK] += qO1LI1[qO1LI_PK, qO1LI_SK]";
 "-LI(qO1LI_OK, qO1LI_PK, qO1LI_SK, qO1LI_PR): qO1[qO1LI_OK] += (qO1LI1[qO1LI_PK, qO1LI_SK]*-1)";
 "+PS(qO1LI1PS_PK, qO1LI1PS_SK): qO1LI1[qO1LI1PS_PK, qO1LI1PS_SK] += 1";
 "-PS(qO1LI1PS_PK, qO1LI1PS_SK): qO1LI1[qO1LI1PS_PK, qO1LI1PS_SK] += -1";
 "+LI(qO1PS1LI_OK, qO1PS1LI_PK, qO1PS1LI_SK, qO1PS1LI_PR): qO1PS1[qO1PS1LI_OK, qO1PS1LI_PK, qO1PS1LI_SK] += 1";
 "-LI(qO1PS1LI_OK, qO1PS1LI_PK, qO1PS1LI_SK, qO1PS1LI_PR): qO1PS1[qO1PS1LI_OK, qO1PS1LI_PK, qO1PS1LI_SK] += -1";
 "+O(qPS1O_OK, qPS1O_CK, qPS1O_DA, qPS1O_XCH): foreach qPS_PK, qPS_SK do qPS1[qPS_PK, qPS_SK] += qPS1O1[qPS1O_OK, qPS_PK, qPS_SK]";
 "-O(qPS1O_OK, qPS1O_CK, qPS1O_DA, qPS1O_XCH): foreach qPS_PK, qPS_SK do qPS1[qPS_PK, qPS_SK] += (qPS1O1[qPS1O_OK, qPS_PK, qPS_SK]*-1)";
 "+LI(qPS1LI_OK, qPS1LI_PK, qPS1LI_SK, qPS1LI_PR): qPS1[qPS1LI_PK, qPS1LI_SK] += qPS1LI1[qPS1LI_OK]";
 "-LI(qPS1LI_OK, qPS1LI_PK, qPS1LI_SK, qPS1LI_PR): qPS1[qPS1LI_PK, qPS1LI_SK] += (qPS1LI1[qPS1LI_OK]*-1)";
 "+O(qPS1LI1O_OK, qPS1LI1O_CK, qPS1LI1O_DA, qPS1LI1O_XCH): qPS1LI1[qPS1LI1O_OK] += 1";
 "-O(qPS1LI1O_OK, qPS1LI1O_CK, qPS1LI1O_DA, qPS1LI1O_XCH): qPS1LI1[qPS1LI1O_OK] += -1";
 "+LI(qPS1O1LI_OK, qPS1O1LI_PK, qPS1O1LI_SK, qPS1O1LI_PR): qPS1O1[qPS1O1LI_OK, qPS1O1LI_PK, qPS1O1LI_SK] += 1";
 "-LI(qPS1O1LI_OK, qPS1O1LI_PK, qPS1O1LI_SK, qPS1O1LI_PR): qPS1O1[qPS1O1LI_OK, qPS1O1LI_PK, qPS1O1LI_SK] += -1"]
);;


