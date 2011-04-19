open Calculus;;

(* the schema *)
let sch = [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["A"; "D"])]

let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;
let relU = RA_Leaf(Rel("U", ["A"; "C"]));;

let mt = Compiler.mk_external "m" [];;



(* select sum(A*C) from R, S where R.B=S.B *)
Compiler.compile Calculus.ModeExtractFromCond sch mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): m[] += mR1[x_mR_B]";
 "+S(x_mS_B, x_mS_C): m[] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


(* select sum(A*D) from R, S, T where R.B=S.B and S.C=T.C *)
Compiler.compile Calculus.ModeExtractFromCond sch mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS; relT]))))) =
["+R(x_mR_A, x_mR_B): m[] += mR1[x_mR_B]";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*mS2[x_mS_C])";
 "+T(x_mT_C, x_mT_D): m[] += mT1[x_mT_C]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += mR1S1[x_mR1S_C]";
 "+T(x_mR1T_C, x_mR1T_D): foreach x_mR_B do mR1[x_mR_B] += mR1T1[x_mR_B, x_mR1T_C]";
 "+T(x_mR1S1T_C, x_mR1S1T_D): mR1S1[x_mR1S1T_C] += 1";
 "+S(x_mR1T1S_B, x_mR1T1S_C): mR1T1[x_mR1T1S_B, x_mR1T1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1";
 "+T(x_mS2T_C, x_mS2T_D): mS2[x_mS2T_C] += 1";
 "+R(x_mT1R_A, x_mT1R_B): foreach x_mT_C do mT1[x_mT_C] += mT1R1[x_mT1R_B, x_mT_C]";
 "+S(x_mT1S_B, x_mT1S_C): mT1[x_mT1S_C] += mT1S1[x_mT1S_B]";
 "+S(x_mT1R1S_B, x_mT1R1S_C): mT1R1[x_mT1R1S_B, x_mT1R1S_C] += 1";
 "+R(x_mT1S1R_A, x_mT1S1R_B): mT1S1[x_mT1S1R_B] += 1"]
;;


Compiler.compile Calculus.ModeExtractFromCond
          [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["A"; "C"])] mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS; relT; relU])))));;
(* all but one for-loop have have just one var; the one loop with two vars
   is over A, B. Thus, linear time. *)


Compiler.compile Calculus.ModeExtractFromCond
          [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["A"; "D"])]
          mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS; relT;
                                 RA_Leaf(Rel("U", ["A"; "D"]))])))));;
(* several loops with two vars, but always from a common relation --
   linear domain sizes. *)


Compiler.compile Calculus.ModeExtractFromCond
          [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["A"; "E"]);
           ("V", ["E"; "F"]); ("W", ["F"; "D"])]
          mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS; relT;
                                 RA_Leaf(Rel("U", ["A"; "E"]));
                                 RA_Leaf(Rel("V", ["E"; "F"]));
                                 RA_Leaf(Rel("W", ["F"; "D"]))])))));;


Compiler.compile Calculus.ModeExtractFromCond
          [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["D"; "E"]);
           ("V", ["E"; "F"]); ("W", ["F"; "G"])]
          mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS; relT;
                                 RA_Leaf(Rel("U", ["D"; "E"]));
                                 RA_Leaf(Rel("V", ["E"; "F"]));
                                 RA_Leaf(Rel("W", ["F"; "G"]))])))));;



Compiler.compile Calculus.ModeExtractFromCond
          [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["D"; "E"])]
          mt
(make_term(RVal(AggSum(RVal (Const (Int 1)),
                RA_MultiNatJoin([relR; relS; relT;
                                 RA_Leaf(Rel("U", ["D"; "E"]))])))));;



