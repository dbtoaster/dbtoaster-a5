open Calculus;;

let cg = Compiler.generate_unit_test_code;;

(* the schema *)
let sch = [("R", [("A", TInt); ("B", TInt)]); ("S", [("B", TInt); ("C", TInt)]);
           ("T", [("C", TInt); ("D", TInt)]); ("U", [("A", TInt); ("D", TInt)])]

let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;
let relU = RA_Leaf(Rel("U", [("A", TInt); ("C", TInt)]));;

let m = make_term(RVal(
   AggSum(RProd[RVal (Var("A", TInt)); RVal (Var("C", TInt))],
          RA_MultiNatJoin([relR; relS]))));;

let mt = map_term "m" [];;

Compiler.compile_delta_for_rel "R" [("A", TInt); ("B", TInt)] false mt [] [] m =
([(false, "R", [("mR_A", TInt); ("mR_B", TInt)], ([],[]),
   make_term(RProd [RVal (Var ("mR_A", TInt));
                    RVal (External("mR1", [("mR_B", TInt)]))]))],
 [(make_term(RVal
       (AggSum (RVal (Var ("C", TInt)),
                RA_Leaf (Rel ("S", [("mR_B", TInt); ("C", TInt)]))))),
  (map_term "mR1" [("mR_B", TInt)]))])
;;

List.hd (fst (
Compiler.compile_delta_for_rel "S" [("B", TInt); ("C", TInt)] false
   (map_term "mR1" [("mR_B", TInt)]) [] []
   (make_term(RVal
      (AggSum (RVal (Var ("C", TInt)),
               RA_Leaf (Rel ("S", [("mR_B", TInt); ("C", TInt)]))))))))
=
(false, "S", [("mR1S_B", TInt); ("mR1S_C", TInt)],
   ([("mR1S_B", TInt)], []),
   make_term(RVal (Var ("mR1S_C", TInt))));;



(* select sum(A*C) from R, S where R.B=S.B *)
(*
Compiler.compile Calculus.ModeExtractFromCond sch (m,mt) cg []
=
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A"]
;;
*)

(* select sum(A*C) from R, S where R.B<S.B *)
(*
let m = 
(make_term(RVal(AggSum(
   RProd[RVal (Var(("A", TInt))); RVal (Var(("C", TInt)))],
   RA_MultiNatJoin([
      RA_Leaf(Rel("R", [("A", TInt); ("B1", TInt)]));
      RA_Leaf(Rel("S", [("B2", TInt); ("C", TInt)]));
      RA_Leaf(AtomicConstraint(Lt,
         RVal(Var(("B1", TInt))), RVal(Var(("B2", TInt)))))
   ])))))
in
Compiler.compile Calculus.ModeExtractFromCond sch (m,mt) cg [] =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): foreach x_mR_B do mR1[x_mR_B] += "^
    "(x_mR1S_C*(if x_mR_B<x_mR1S_B then 1 else 0))";
 "+R(x_mS1R_A, x_mS1R_B): foreach x_mS_B do mS1[x_mS_B] += "^
    "(x_mS1R_A*(if x_mS1R_B<x_mS_B then 1 else 0))"]
;;
*)

(* select sum(A*C) from R, S where R.B=S.B group by A *)
(*
Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("A", TInt)])) cg [] =
["+R(x_mR_A, x_mR_B): m[x_mR_A] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): foreach A do m[A] += (mS1[A, x_mS_B]*x_mS_C)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += x_mR1S_C";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_A, x_mS1R_B] += x_mS1R_A"]
;;
*)


(* select sum(A*D) from R, S, T where R.B=S.B and S.C=T.C *)
(*
let m =
(make_term(
   RVal(AggSum(RProd[RVal (Var(("A", TInt))); RVal (Var(("D", TInt)))],
                RA_MultiNatJoin([relR; relS; relT]))))) 
in
Compiler.compile Calculus.ModeExtractFromCond sch (m, mt) cg [] =
["+R(x_mR_A, x_mR_B): m[] += (x_mR_A*mR1[x_mR_B])";
 "+S(x_mS_B, x_mS_C): m[] += (mS1[x_mS_B]*mS2[x_mS_C])";
 "+T(x_mT_C, x_mT_D): m[] += (mT1[x_mT_C]*x_mT_D)";
 "+S(x_mR1S_B, x_mR1S_C): mR1[x_mR1S_B] += mR1S1[x_mR1S_C]";
 "+T(x_mR1T_C, x_mR1T_D): foreach x_mR_B do mR1[x_mR_B] += "^
    "(x_mR1T_D*mR1T1[x_mR_B, x_mR1T_C])";
 "+T(x_mR1S1T_C, x_mR1S1T_D): mR1S1[x_mR1S1T_C] += x_mR1S1T_D";
 "+S(x_mR1T1S_B, x_mR1T1S_C): mR1T1[x_mR1T1S_B, x_mR1T1S_C] += 1";
 "+R(x_mS1R_A, x_mS1R_B): mS1[x_mS1R_B] += x_mS1R_A";
 "+T(x_mS2T_C, x_mS2T_D): mS2[x_mS2T_C] += x_mS2T_D";
 "+R(x_mT1R_A, x_mT1R_B): foreach x_mT_C do mT1[x_mT_C] += "^
    "(x_mT1R_A*mT1R1[x_mT1R_B, x_mT_C])";
 "+S(x_mT1S_B, x_mT1S_C): mT1[x_mT1S_C] += mT1S1[x_mT1S_B]";
 "+S(x_mT1R1S_B, x_mT1R1S_C): mT1R1[x_mT1R1S_B, x_mT1R1S_C] += 1";
 "+R(x_mT1S1R_A, x_mT1S1R_B): mT1S1[x_mT1S1R_B] += x_mT1S1R_A"]
;;
*)

(* select sum(A) from R, S, T where R.B=S.B and S.C=T.C group by D *)
let m =  (make_term(RVal(
   AggSum(RVal(Var(("A", TInt))), RA_MultiNatJoin([relR; relS; relT])))))
in
Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("D", TInt)])) cg [] =
["+R(mR_A, mR_B): foreach D do m[D] += (mR_A*mR1[mR_B, D])";
 "-R(mR_A, mR_B): foreach D do m[D] += (mR_A*-1*mR1[mR_B, D])";
 "+S(mS_B, mS_C): foreach D do m[D] += (mS1[mS_B]*mS2[mS_C, D])";
 "-S(mS_B, mS_C): foreach D do m[D] += (mS1[mS_B]*-1*mS2[mS_C, D])";
 "+T(mT_C, mT_D): m[mT_D] += mT1[mT_C]";
 "-T(mT_C, mT_D): m[mT_D] += (mT1[mT_C]*-1)";
 "+R(mT1R_A, mT1R_B): foreach mT_C do mT1[mT_C] += "^
    "(mT1R_A*mT1R1[mT1R_B, mT_C])";
 "-R(mT1R_A, mT1R_B): foreach mT_C do mT1[mT_C] += "^
    "(mT1R_A*-1*mT1R1[mT1R_B, mT_C])";
 "+S(mT1S_B, mT1S_C): mT1[mT1S_C] += mT1S1[mT1S_B]";
 "-S(mT1S_B, mT1S_C): mT1[mT1S_C] += (mT1S1[mT1S_B]*-1)";
 "+R(mT1S1R_A, mT1S1R_B): mT1S1[mT1S1R_B] += mT1S1R_A";
 "-R(mT1S1R_A, mT1S1R_B): mT1S1[mT1S1R_B] += (mT1S1R_A*-1)";
 "+S(mT1R1S_B, mT1R1S_C): mT1R1[mT1R1S_B, mT1R1S_C] += 1";
 "-S(mT1R1S_B, mT1R1S_C): mT1R1[mT1R1S_B, mT1R1S_C] += -1";
 "+T(mS2T_C, mS2T_D): mS2[mS2T_C, mS2T_D] += 1";
 "-T(mS2T_C, mS2T_D): mS2[mS2T_C, mS2T_D] += -1";
 "+R(mS1R_A, mS1R_B): mS1[mS1R_B] += mS1R_A";
 "-R(mS1R_A, mS1R_B): mS1[mS1R_B] += (mS1R_A*-1)";
 "+S(mR1S_B, mR1S_C): foreach D do mR1[mR1S_B, D] += mR1S1[mR1S_C, D]";
 "-S(mR1S_B, mR1S_C): foreach D do mR1[mR1S_B, D] += (-1*mR1S1[mR1S_C, D])";
 "+T(mR1T_C, mR1T_D): foreach mR_B do mR1[mR_B, mR1T_D] += mR1T1[mR_B, mR1T_C]";
 "-T(mR1T_C, mR1T_D): foreach mR_B do mR1[mR_B, mR1T_D] += "^
    "(-1*mR1T1[mR_B, mR1T_C])";
 "+S(mR1T1S_B, mR1T1S_C): mR1T1[mR1T1S_B, mR1T1S_C] += 1";
 "-S(mR1T1S_B, mR1T1S_C): mR1T1[mR1T1S_B, mR1T1S_C] += -1";
 "+T(mR1S1T_C, mR1S1T_D): mR1S1[mR1S1T_C, mR1S1T_D] += 1";
 "-T(mR1S1T_C, mR1S1T_D): mR1S1[mR1S1T_C, mR1S1T_D] += -1"]
;;


(* select count( * ) from R group by B *)
let m = (make_term( RVal(AggSum(RVal(Const (Int 1)), relR)))) in
Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("B", TInt)])) cg [] =
["+R(mR_A, mR_B): m[mR_B] += 1";
 "-R(mR_A, mR_B): m[mR_B] += -1"] ;;


(* select count( * ) from R, S where R.B=S.B group by C *)
let m = 
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS])))))
in
Compiler.compile Calculus.ModeExtractFromCond
                 sch (m, (map_term "m" [("C", TInt)])) cg [] =
["+R(mR_A, mR_B): foreach C do m[C] += mR1[mR_B, C]";
 "-R(mR_A, mR_B): foreach C do m[C] += (-1*mR1[mR_B, C])";
 "+S(mS_B, mS_C): m[mS_C] += mS1[mS_B]";
 "-S(mS_B, mS_C): m[mS_C] += (-1*mS1[mS_B])";
 "+R(mS1R_A, mS1R_B): mS1[mS1R_B] += 1";
 "-R(mS1R_A, mS1R_B): mS1[mS1R_B] += -1";
 "+S(mR1S_B, mR1S_C): mR1[mR1S_B, mR1S_C] += 1";
 "-S(mR1S_B, mR1S_C): mR1[mR1S_B, mR1S_C] += -1"]
;;


(* select count( * ) from R, S where R.B = S.B group by B *)
let m = 
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS])))))
in
Compiler.compile Calculus.ModeExtractFromCond
                 sch (m, (map_term "m" [("B", TInt)])) cg []
=
["+R(mR_A, mR_B): m[mR_B] += mR1[mR_B]";
 "-R(mR_A, mR_B): m[mR_B] += (-1*mR1[mR_B])";
 "+S(mS_B, mS_C): m[mS_B] += mS1[mS_B]";
 "-S(mS_B, mS_C): m[mS_B] += (-1*mS1[mS_B])";
 "+R(mS1R_A, mS1R_B): mS1[mS1R_B] += 1";
 "-R(mS1R_A, mS1R_B): mS1[mS1R_B] += -1";
 "+S(mR1S_B, mR1S_C): mR1[mR1S_B] += 1";
 "-S(mR1S_B, mR1S_C): mR1[mR1S_B] += -1"]
;;


(* select count( * ) from R, S where R.B = S.B group by B, C *)
let m = 
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([relR; relS]))))) 
in
Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "m" [("B", TInt); ("C", TInt)])) cg [] =
["+R(mR_A, mR_B): foreach C do m[mR_B, C] += mR1[mR_B, C]";
 "-R(mR_A, mR_B): foreach C do m[mR_B, C] += (-1*mR1[mR_B, C])";
 "+S(mS_B, mS_C): m[mS_B, mS_C] += mS1[mS_B]";
 "-S(mS_B, mS_C): m[mS_B, mS_C] += (-1*mS1[mS_B])";
 "+R(mS1R_A, mS1R_B): mS1[mS1R_B] += 1";
 "-R(mS1R_A, mS1R_B): mS1[mS1R_B] += -1";
 "+S(mR1S_B, mR1S_C): mR1[mR1S_B, mR1S_C] += 1";
 "-S(mR1S_B, mR1S_C): mR1[mR1S_B, mR1S_C] += -1"]
;;


(* self-join *)
(* select count( * ) from R r1, R r2 where r1.B = r2.A *)
let m = 
   (make_term(RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
 RA_Leaf(Rel("R", [("y", TInt); ("z", TInt)]))
])))))
in Compiler.compile Calculus.ModeExtractFromCond sch (m, mt) cg [] =
["+R(mR_A, mR_B): m[] += mR1[mR_B]";
 "+R(mR_A, mR_B): m[] += mR2[mR_A]";
 "+R(mR_A, mR_B): m[] += (if mR_A=mR_B then 1 else 0)";
 "-R(mR_A, mR_B): m[] += (-1*mR1[mR_B])";
 "-R(mR_A, mR_B): m[] += (-1*mR2[mR_A])";
 "-R(mR_A, mR_B): m[] += (if mR_A=mR_B then 1 else 0)";
 "+R(mR2R_A, mR2R_B): mR2[mR2R_B] += 1";
 "-R(mR2R_A, mR2R_B): mR2[mR2R_B] += -1";
 "+R(mR1R_A, mR1R_B): mR1[mR1R_A] += 1";
 "-R(mR1R_A, mR1R_B): mR1[mR1R_A] += -1"]
;;


(* too large to look at:
(* select sum(A) from R, S, U
   where R.B=S.B and R.A=U.A and S.C=U.C
   group by C
*)
Compiler.compile Calculus.ModeExtractFromCond
                 sch (map_term "m" [("C", TInt)])
(make_term(
   RVal(AggSum(RVal (Var(("A", TInt))),
                RA_MultiNatJoin([relR; relS; relU])))))
;;
*)


(* select count( * ) from R, S where R.A=S.A and R.B=S.B *)
let m = (make_term( RVal(AggSum(RVal(Const (Int 1)),
   RA_MultiNatJoin([
      RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
      RA_Leaf(Rel("S", [("x", TInt); ("y", TInt)]))
   ])))))
in Compiler.compile Calculus.ModeExtractFromCond sch (m, mt) cg [] =
["+R(mR_A, mR_B): m[] += mR1[mR_A, mR_B]";
 "-R(mR_A, mR_B): m[] += (-1*mR1[mR_A, mR_B])";
 "+S(mS_B, mS_C): m[] += mS1[mS_B, mS_C]";
 "-S(mS_B, mS_C): m[] += (-1*mS1[mS_B, mS_C])";
 "+R(mS1R_A, mS1R_B): mS1[mS1R_A, mS1R_B] += 1";
 "-R(mS1R_A, mS1R_B): mS1[mS1R_A, mS1R_B] += -1";
 "+S(mR1S_B, mR1S_C): mR1[mR1S_B, mR1S_C] += 1";
 "-S(mR1S_B, mR1S_C): mR1[mR1S_B, mR1S_C] += -1"]
;;


(* select count( * ) from R where R.A < 5 and R.B = 'Bla' *)
let m = (make_term(RVal(AggSum(RVal(Const (Int 1)),
   RA_MultiNatJoin([
      RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
      RA_Leaf(AtomicConstraint(Lt,
                 RVal(Var(("x", TInt))), RVal(Const(Int 5))));
      RA_Leaf(AtomicConstraint(Eq,
                 RVal(Var(("y", TInt))), RVal(Const(String "Bla"))));
   ])))))
in Compiler.compile Calculus.ModeExtractFromCond
   sch (m, (map_term "q" [])) cg [] =
["+R(qR_A, qR_B): q[] += "^
   "((if qR_A<5 then 1 else 0)*(if qR_B='Bla' then 1 else 0))";
 "-R(qR_A, qR_B): q[] += "^
   "(-1*(if qR_A<5 then 1 else 0)*(if qR_B='Bla' then 1 else 0))"]
;;



(* if(0 < AggSum(1, R(A,B))) then 1 else 0 *)
let m = (make_term(RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)),
                  RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]))))))))
))
in
Compiler.compile Calculus.ModeExtractFromCond
   [("R", [("A", TInt); ("B", TInt)])] (m, mt) cg [] =
["+R(mR_A, mR_B): m[] += (if 0<(mR1[]+1) and mR1[]<=0 then 1 else 0)";
 "+R(mR_A, mR_B): m[] += (-1*(if 0<mR1[] and (mR1[]+1)<=0 then 1 else 0))";
 "-R(mR_A, mR_B): m[] += (if 0<(mR1[]+-1) and mR1[]<=0 then 1 else 0)";
 "-R(mR_A, mR_B): m[] += (-1*(if 0<mR1[] and (mR1[]+-1)<=0 then 1 else 0))";
 "+R(mR1R_A, mR1R_B): mR1[] += 1";
 "-R(mR1R_A, mR1R_B): mR1[] += -1"]
;;

(* old version: SemiRing and old delta -- it's equivalent.

["+R(x_mR_A, x_mR_B): m[] += (if 0<(mR1[]+1) and mR1[]<=0 then 1 else 0)";
 "+R(x_mR_A, x_mR_B): m[] += (-1*(if (mR1[]+1)<=0 and 0<mR1[] then 1 else 0))";
 "+R(x_mR1R_A, x_mR1R_B): mR1[] += 1"]
;;
*)





