open Calculus;;

let make x = make_relcalc x;;
let readable x = readable_relcalc x;;
let vars x = relcalc_vars x;;

let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;
let relU = RA_Leaf(Rel("U", ["A"; "D"]));;


let test01 = make(RA_Leaf False) = relcalc_zero;;
let test02 = polynomial (make (RA_Leaf False)) = make(RA_Leaf False);;
let test03 = monomials  (make (RA_Leaf False)) = [];;



(* (R bowtie S) bowtie T *)
let q_r = RA_MultiNatJoin [RA_MultiNatJoin ([relR; relS]); relT];;
let q = make q_r;;

let test04 = readable q = q_r;;
let test05 = readable (polynomial q) = RA_MultiNatJoin [relR; relS; relT];;

let test06 = make relR =
polynomial (make (RA_MultiNatJoin[RA_MultiNatJoin[RA_MultiNatJoin[relR]]]));;

let test07 =
readable(polynomial(make(
   RA_MultiNatJoin [RA_MultiUnion([relR; relS]); RA_MultiUnion([relT; relU])]
))) =
RA_MultiUnion
 [RA_MultiNatJoin [relR; relT]; RA_MultiNatJoin [relS; relT];
  RA_MultiNatJoin [relR; relU]; RA_MultiNatJoin [relS; relU]]
;;

let test08 = readable (polynomial (relcalc_delta "R" ["a"; "b"] q)) =
  RA_MultiNatJoin
   [RA_Leaf (AtomicConstraint (Eq, RVal(Var("A")), RVal(Var("a"))));
    RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("b"))));
    relS; relT]
;;

let test09 = readable (polynomial (relcalc_delta "S" ["b"; "c"] q)) =
RA_MultiNatJoin
 [relR;
  RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("b"))));
  RA_Leaf (AtomicConstraint (Eq, RVal(Var("C")), RVal(Var("c"))));
  relT]
;;

let test10 =
polynomial(make(
   RA_MultiNatJoin [RA_MultiNatJoin[RA_Leaf (False); relS];
                    RA_MultiNatJoin[relR; relS]])) =
make(RA_Leaf False)
;;

let q12 = make(
RA_MultiNatJoin [
   RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("xR_B"))));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("xS_B"))))]);;


let test12 = extract_substitutions (List.hd (monomials q12)) ["xS_B"] =
([("B", "xS_B"); ("xR_B", "xS_B"); ("xS_B", "xS_B")], relcalc_one);;

let test13 = vars   q12 = ["B"; "xR_B"; "xS_B"];;

let test15 =
extract_substitutions(make(
RA_MultiNatJoin [
   RA_Leaf (AtomicConstraint (Lt, RVal(Var("A")), RVal(Var("x"))));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("y"))));
   RA_Leaf (Rel("S", ["B"; "C"]))])) ["y"]
=
([("B", "y"); ("y", "y")],
make(
 RA_MultiNatJoin
  [RA_Leaf (AtomicConstraint (Lt, RVal(Var("A")), RVal(Var("x"))));
   RA_Leaf (Rel ("S", ["y"; "C"]))]))
;;



let test16 =
extract_substitutions
(make(
     RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, RVal(Var("x")), RVal(Var("AA"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("x")), RVal(Var("BB"))))]))
["AA"; "BB"] =
([("x", "AA"); ("AA", "AA"); ("BB", "AA")],
 make(RA_Leaf (AtomicConstraint (Eq, RVal(Var("AA")), RVal(Var("BB"))))));;



safe_vars (make(RA_MultiUnion[relR; relT])) [] = [];;

safe_vars (make(RA_MultiUnion[relR; relT])) ["A"] = ["A"];;

safe_vars (make(RA_MultiUnion[relR;
   RA_MultiNatJoin[relT; RA_Leaf (AtomicConstraint (Eq, RVal(Var("C")),
                                     RVal(Var("A"))))]])) [] = ["A"];;



