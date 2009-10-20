open Algebra;;

(* Name functions used in the unit test for simpler refactoring *)
module C = Compiler.MessageCompiler
let compile_step = Compiler.compile_delta_for_rel
let compile_main a b c d e f = C.compile_readable_messages a b c d e max_int f

(* the schema *)
let rSch = ("R", [("A", TInt); ("B", TInt)])
let sSch = ("S", [("B", TInt); ("C", TInt)])
let tSch = ("T", [("C", TInt); ("D", TInt)])
let uSch = ("U", [("A", TInt); ("D", TInt)])
let sch = [rSch; sSch; tSch; uSch]



let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;
let relU = RA_Leaf(Rel("U", [("A", TInt); ("D", TInt)]));;

let m = make_term(
    RVal(AggSum(RProd[RVal (Var("A", TInt)); RVal (Var("C", TInt))],
                RA_MultiNatJoin([relR; relS]))));;

let m1 = RVal (AggSum (RVal (Var("C", TInt)),
    RA_Leaf (Rel ("S", [("x_mR_B", TInt); ("C", TInt)]))))
;;

let pos = false
let neg = true;;

let test_step =
(compile_step "R" [("A", TInt); ("B", TInt)] "m" [] [] pos m) =
([("R", [("x_mR_A", TInt); ("x_mR_B", TInt)], [], [], "m",
   make_term(RProd [RVal (Var("x_mR_A", TInt)); m1]))],
 [("mR1", [("x_mR_B", TInt)], make_term(m1))])
;;


let sum_ac_rs_eq =
(* select sum(A*C) from R, S where R.B=S.B *)
compile_main sch "m" [] [] pos m =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A"]
;;


let sum_ac_rs_lt =
(* select sum(A*C) from R, S where R.B<S.B *)
compile_main sch "m" [] [] pos
(make_term(RVal(AggSum(RProd[RVal (Var("A", TInt)); RVal (Var("C", TInt))],
                RA_MultiNatJoin([
RA_Leaf(Rel("R", [("A", TInt); ("B1", TInt)]));
RA_Leaf(Rel("S", [("B2", TInt); ("C", TInt)]));
RA_Leaf(AtomicConstraint(Lt, RVal(Var("B1", TInt)), RVal(Var("B2", TInt))))
]))))) =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): foreach x_mR_B do mR1[x_mR_B] += (x_mR1S_C*(if x_mR_B<x_mR1S_B then 1 else 0))";
 "+R(x_mS1R_A, x_mS1R_B): foreach x_mS_B do mS1[x_mS_B] += (x_mS1R_A*(if x_mS1R_B<x_mS_B then 1 else 0))"]
;;


let sum_ac_rs_eq_gb_a =
(* select sum(A*C) from R, S where R.B=S.B group by A *)
compile_main sch "m" [("A", TInt)] [] pos m =
["+R(x_mR_A, x_mR_B): m[x_mR_A] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): foreach A do m[A] += (mS1[A, x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_A, x_mS1R_B] += x_mS1R_A"]
;;


let sum_ad_rst_eq =
(* select sum(A*D) from R, S, T where R.B=S.B and S.C=T.C *)
compile_main sch "m" [] [] pos
(make_term(
   RVal(AggSum(RProd[RVal (Var("A", TInt)); RVal (Var("D", TInt))],
                RA_MultiNatJoin([relR; relS; relT]))))) =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*mS2[x_mS_C])";
 "+T(x_mT_C, x_mT_D): m[] += (mT1[x_mT_C]*x_mT_D)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += mS2[x_mR1S_C]";
 "+T(x_mR1T_C, x_mR1T_D): foreach x_mR_B do mR1[x_mR_B] += (x_mR1T_D*mR1T1[x_mR_B, x_mR1T_C])";
 "+S(x_mR1T1S_B, x_mR1T1S_C): mR1T1[x_mR1T1S_B, x_mR1T1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A";
 "+T(x_mS2T_C, x_mS2T_D): mS2[x_mS2T_C] += x_mS2T_D";
 "+R(x_mT1R_A, x_mT1R_B): foreach x_mT_C do mT1[x_mT_C] += (x_mT1R_A*mR1T1[x_mT1R_B, x_mT_C])";
 "+S(x_mT1S_B, x_mT1S_C): mT1[x_mT1S_C] += mS1[x_mT1S_B]"]

(* Version with duplicate maps...
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
*)
;;


let sum_a_rst_eq_gb_d =
(* select sum(A) from R, S, T where R.B=S.B and S.C=T.C group by D *)
compile_main sch "m" [("D", TInt)] [] pos
(make_term(RVal(AggSum(RVal(Var("A", TInt)),
    RA_MultiNatJoin([relR; relS; relT]))))) =
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


let sum_1_r_gb_b =
(* select count( * ) from R group by B *)
compile_main sch "m" [("B", TInt)] [] pos
   (make_term( RVal(AggSum(RVal(Const (Int 1)), relR))))
= ["+R(x_mR_A, x_mR_B): m[x_mR_B] += 1"] ;;


let sum_1_rs_eq_gb_c =
(* select count( * ) from R, S where R.B=S.B group by C *)
compile_main sch "m" [("C", TInt)] [] pos
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): foreach C do m[C] += mR1[x_mR_B, C]";
 "+S(x_mS_B, x_mS_C): m[x_mS_C] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B, x_mR1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


let sum_1_rs_eq_gb_b =
(* select count( * ) from R, S where R.B = S.B group by B *)
compile_main sch "m" [("B", TInt)] [] pos
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): m[x_mR_B] += mR1[x_mR_B]";
 "+S(x_mS_B, x_mS_C): m[x_mS_B] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


let sum_1_rs_eq_gb_bc =
(* select count( * ) from R, S where R.B = S.B group by B, C *)
compile_main sch "m" [("B", TInt); ("C", TInt)] [] pos
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) =
["+R(x_mR_A, x_mR_B): foreach C do m[x_mR_B, C] += mR1[x_mR_B, C]";
 "+S(x_mS_B, x_mS_C): m[x_mS_B, x_mS_C] += mS1[x_mS_B]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B, x_mR1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += 1"]
;;


(* self-join *)
let sum_1_rr_eq =
(* select count( * ) from R r1, R r2 where r1.B = r2.A *)
compile_main sch "m" [] [] pos
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
 RA_Leaf(Rel("R", [("y", TInt); ("z", TInt)]))
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
compile_main sch "m" [("C", TInt)] [] pos
(make_term(
   RVal(AggSum(RVal (Var("A", TInt)),
                RA_MultiNatJoin([relR; relS; relU])))))
;;
*)


let sum_1_rs_eq =
(* select count( * ) from R, S where R.A=S.A and R.B=S.B *)
compile_main sch "m" [] [] pos
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
 RA_Leaf(Rel("S", [("x", TInt); ("y", TInt)]))
])))))
=
["+R(x_mR_A, x_mR_B): m[] += mR1[x_mR_A, x_mR_B]";
 "+S(x_mS_B, x_mS_C): m[] += mS1[x_mS_B, x_mS_C]";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B, x_mR1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_A, x_mS1R_B] += 1"]
;;


let sum_1_r_lt_str =
(* select count( * ) from R where R.A < 5 and R.B = 'Bla' *)
compile_main sch "q" [] [] pos
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TString)]));
 RA_Leaf(AtomicConstraint(Lt, RVal(Var("x", TInt)), RVal(Const(Int 5))));
 RA_Leaf(AtomicConstraint(Eq, RVal(Var("y", TString)), RVal(Const(String "Bla"))));
]))))) =
["+R(x_qR_A, x_qR_B): q[] += ((if x_qR_A<5 then 1 else 0)*(if x_qR_B='Bla' then 1 else 0))"]
;;



let cond_sum_1_r =
(* if(0 < AggSum(1, R(A,B))) then 1 else 0 *)
compile_main [("R", [("A", TInt); ("B", TInt)])] "m" [] [] pos
(make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)), RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]))))))))
))
=
["+R(x_mR_A, x_mR_B): m[] += (if 0<(mR1[]+1) and mR1[]<=0 then 1 else 0)";
 "+R(x_mR_A, x_mR_B): m[] += (-1*(if (mR1[]+1)<=0 and 0<mR1[] then 1 else 0))";
 "+R(x_mR1R_A, x_mR1R_B): mR1[] += 1"]
;;






