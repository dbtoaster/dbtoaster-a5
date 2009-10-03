open Algebra;;

(* the schema *)
let sch = [("R", ["A"; "B"]); ("S", ["B"; "C"]);
           ("T", ["C"; "D"]); ("U", ["A"; "D"])]



let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;
let relU = RA_Leaf(Rel("U", ["A"; "C"]));;

let m = make_term(RVal(AggSum(RProd[RVal (Var("A")); RVal (Var("C"))],
                RA_MultiNatJoin([relR; relS]))));;

let m1 = RVal (AggSum (RVal (Var "C"), RA_Leaf (Rel ("S", ["x_mR_B"; "C"]))))
;;

(Compiler.compile_delta_for_rel "R" ["A"; "B"] "m" [] [] m) =
([("R", ["x_mR_A"; "x_mR_B"], [], [], "m",
   make_term(RProd [RVal (Var "x_mR_A"); m1]))],
 [("mR1", ["x_mR_B"], make_term(m1))])
;;


(* select sum(A*C) from R, S where R.B=S.B *)
Compiler.compile sch "m" [] [] m =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A"]
;;


(* select sum(A*C) from R, S where R.B<S.B *)
Compiler.compile sch "m" [] []
(make_term(RVal(AggSum(RProd[RVal (Var("A")); RVal (Var("C"))],
                RA_MultiNatJoin([
RA_Leaf(Rel("R", ["A"; "B1"]));
RA_Leaf(Rel("S", ["B2"; "C"]));
RA_Leaf(AtomicConstraint(Lt, RVal(Var("B1")), RVal(Var("B2"))))
]))))) =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): foreach x_mR_B do mR1[x_mR_B] += (x_mR1S_C*(if x_mR_B<x_mR1S_B then 1 else 0))";
 "+R(x_mS1R_A, x_mS1R_B): foreach x_mS_B do mS1[x_mS_B] += (x_mS1R_A*(if x_mS1R_B<x_mS_B then 1 else 0))"]
;;


(* select sum(A*C) from R, S where R.B=S.B group by A *)
Compiler.compile sch "m" ["A"] [] m =
["+R(x_mR_A, x_mR_B): m[x_mR_A] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): foreach A do m[A] += (mS1[A, x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_A, x_mS1R_B] += x_mS1R_A"]
;;


(* select sum(A*D) from R, S, T where R.B=S.B and S.C=T.C *)
Compiler.compile sch "m" [] []
(make_term(
   RVal(AggSum(RProd[RVal (Var("A")); RVal (Var("D"))],
                RA_MultiNatJoin([relR; relS; relT]))))) =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*mS2[x_mS_C])";
 "+T(x_mT_C, x_mT_D): m[] += (mT1[x_mT_C]*x_mT_D)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += mR1S1[x_mR1S_C]";
 "+T(x_mR1T_C, x_mR1T_D): foreach x_mR_B do mR1[x_mR_B] += (x_mR1T_D*mR1T1[x_mR_B, x_mR1T_C])";
 "+T(x_mR1S1T_C, x_mR1S1T_D): mR1S1[x_mR1S1T_C] += x_mR1S1T_D";
 "+S(x_mR1T1S_B, x_mR1T1S_C): mR1T1[x_mR1T1S_B, x_mR1T1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A";
 "+T(x_mS2T_C, x_mS2T_D): mS2[x_mS2T_C] += x_mS2T_D";
 "+R(x_mT1R_A, x_mT1R_B): foreach x_mT_C do mT1[x_mT_C] += (x_mT1R_A*mT1R1[x_mT1R_B, x_mT_C])";
 "+S(x_mT1S_B, x_mT1S_C): mT1[x_mT1S_C] += mT1S1[x_mT1S_B]";
 "+S(x_mT1R1S_B, x_mT1R1S_C): mT1R1[x_mT1R1S_B, x_mT1R1S_C] += 1";
 "+R(x_mT1S1R_A, x_mT1S1R_B): mT1S1[x_mT1S1R_B] += x_mT1S1R_A"]
;;


(* select sum(A) from R, S, T where R.B=S.B and S.C=T.C group by D *)
Compiler.compile sch "m" ["D"] []
(make_term(RVal(AggSum(RVal(Var("A")), RA_MultiNatJoin([relR; relS; relT]))))) =
["+R(x_mR_A, x_mR_B): foreach D do m[D] += (x_mR_A*mR1[x_mR_B, D])";
 "+S(x_mS_B, x_mS_C): foreach D do m[D] += (mS1[x_mS_B]*mS2[x_mS_C, D])";
 "+T(x_mT_C, x_mT_D): m[x_mT_D] += mT1[x_mT_C]";
 "+S(x_mR1S_B, x_mR1S_C): foreach D do mR1[x_mR1S_B, D] += mR1S1[x_mR1S_C, D]";
 "+T(x_mR1T_C, x_mR1T_D): foreach x_mR_B do mR1[x_mR_B, x_mR1T_D] += mR1T1[x_mR_B, x_mR1T_C]";
 "+T(x_mR1S1T_C, x_mR1S1T_D): mR1S1[x_mR1S1T_C, x_mR1S1T_D] += 1";
 "+S(x_mR1T1S_B, x_mR1T1S_C): mR1T1[x_mR1T1S_B, x_mR1T1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A";
 "+T(x_mS2T_C, x_mS2T_D): mS2[x_mS2T_C, x_mS2T_D] += 1";
 "+R(x_mT1R_A, x_mT1R_B): foreach x_mT_C do mT1[x_mT_C] += (x_mT1R_A*mT1R1[x_mT1R_B, x_mT_C])";
 "+S(x_mT1S_B, x_mT1S_C): mT1[x_mT1S_C] += mT1S1[x_mT1S_B]";
 "+S(x_mT1R1S_B, x_mT1R1S_C): mT1R1[x_mT1R1S_B, x_mT1R1S_C] += 1";
 "+R(x_mT1S1R_A, x_mT1S1R_B): mT1S1[x_mT1S1R_B] += x_mT1S1R_A"]
;;


(* select count( * ) from R group by B *)
Compiler.compile sch "m" ["B"] []
   (make_term( RVal(AggSum(RVal(Const (Int 1)), relR))))
= ["+R(x_mR_A, x_mR_B): m[x_mR_B] += 1"] ;;


(* select count( * ) from R, S where R.B=S.B group by C *)
Compiler.compile sch "m" ["C"] []
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): foreach C do m[C] += mR1[x_mR_B, C]";
 "+S(x_mS_B, x_mS_C): m[x_mS_C] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B, x_mR1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


(* select count( * ) from R, S where R.B = S.B group by B *)
Compiler.compile sch "m" ["B"] []
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): m[x_mR_B] += mR1[x_mR_B]";
 "+S(x_mS_B, x_mS_C): m[x_mS_B] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


(* select count( * ) from R, S where R.B = S.B group by B, C *)
Compiler.compile sch "m" ["B"; "C"] []
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): foreach C do m[x_mR_B, C] += mR1[x_mR_B, C]";
 "+S(x_mS_B, x_mS_C): m[x_mS_B, x_mS_C] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B, x_mR1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


(* self-join *)
(* select count( * ) from R r1, R r2 where r1.B = r2.A *)
Compiler.compile sch "m" [] []
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", ["x"; "y"]));
 RA_Leaf(Rel("R", ["y"; "z"]))
]))))) =
["+R(x_mR_A, x_mR_B): m[] += mR1[x_mR_B]";
 "+R(x_mR_A, x_mR_B): m[] += mR2[x_mR_A]";
 "+R(x_mR_A, x_mR_B): m[] += (if x_mR_A=x_mR_B then 1 else 0)";
 "+R(x_mR1R_A, x_mR1R_B): mR1[x_mR1R_A] += 1";
 "+R(x_mR2R_A, x_mR2R_B): mR2[x_mR2R_B] += 1"]
;;


(* too large to look at:
(* select sum(A) from R, S, U
   where R.B=S.B and R.A=U.A and S.C=U.C
   group by C
*)
Compiler.compile sch "m" ["C"] []
(make_term(
   RVal(AggSum(RVal (Var("A")),
                RA_MultiNatJoin([relR; relS; relU])))))
;;
*)


(* select count( * ) from R, S where R.A=S.A and R.B=S.B *)
Compiler.compile sch "m" [] []
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", ["x"; "y"]));
 RA_Leaf(Rel("S", ["x"; "y"]))
])))))
=
["+R(x_mR_A, x_mR_B): m[] += mR1[x_mR_A, x_mR_B]";
 "+S(x_mS_B, x_mS_C): m[] += mS1[x_mS_B, x_mS_C]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B, x_mR1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_A, x_mS1R_B] += 1"]
;;


(* select count( * ) from R where R.A < 5 and R.B = 'Bla' *)
Compiler.compile sch "q" [] []
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", ["x"; "y"]));
 RA_Leaf(AtomicConstraint(Lt, RVal(Var("x")), RVal(Const(Int 5))));
 RA_Leaf(AtomicConstraint(Eq, RVal(Var("y")), RVal(Const(String "Bla"))));
]))))) =
["+R(x_qR_A, x_qR_B): q[] += ((if x_qR_A<5 then 1 else 0)*(if x_qR_B='Bla' then 1 else 0))"]
;;



(* if(0 < AggSum(1, R(A,B))) then 1 else 0 *)
Compiler.compile [("R", ["A"; "B"])] "m" [] []
(make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)), RA_Leaf(Rel("R", ["A"; "B"]))))))))
))
=
["+R(x_mR_A, x_mR_B): m[] += (if 0<(mR1[]+1) and mR1[]<=0 then 1 else 0)";
 "+R(x_mR_A, x_mR_B): m[] += (-1*(if (mR1[]+1)<=0 and 0<mR1[] then 1 else 0))";
 "+R(x_mR1R_A, x_mR1R_B): mR1[] += 1"]
;;






