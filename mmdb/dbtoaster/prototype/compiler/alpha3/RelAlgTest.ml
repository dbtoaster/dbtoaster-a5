open Algebra;;

let make x = make_relalg x;;
let readable x = readable_relalg x;;
let vars x = relalg_vars x;;

let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;
let relU = RA_Leaf(Rel("U", [("A", TInt); ("D", TInt)]));;


let test01 = make(RA_Leaf Empty) = relalg_zero;;
let test02 = polynomial (make (RA_Leaf Empty)) = make(RA_Leaf Empty);;
let test03 = monomials  (make (RA_Leaf Empty)) = [];;

let pos = false
let neg = true

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

let test08 = readable
    (polynomial (relalg_delta neg "R" [("a", TInt); ("b", TInt)] q)) =
  RA_MultiNatJoin
   [RA_Leaf
       (AtomicConstraint (Eq, RVal(Var("A", TInt)), RVal(Var("a", TInt))));
    RA_Leaf
        (AtomicConstraint (Eq, RVal(Var("B", TInt)), RVal(Var("b", TInt))));
    relS; relT]
;;

let test09 = readable
    (polynomial (relalg_delta neg "S" [("b", TInt); ("c", TInt)] q)) =
RA_MultiNatJoin
 [relR;
  RA_Leaf (AtomicConstraint (Eq, RVal(Var("B", TInt)), RVal(Var("b", TInt))));
  RA_Leaf (AtomicConstraint (Eq, RVal(Var("C", TInt)), RVal(Var("c", TInt))));
  relT]
;;

let test10 =
polynomial(make(
   RA_MultiNatJoin [RA_MultiNatJoin[RA_Leaf (Empty); relS];
                    RA_MultiNatJoin[relR; relS]])) =
make(RA_Leaf Empty)
;;

let q12 = make(
RA_MultiNatJoin [
   RA_Leaf (AtomicConstraint
       (Eq, RVal(Var("B", TInt)), RVal(Var("xR_B", TInt))));
   RA_Leaf (AtomicConstraint
       (Eq, RVal(Var("B", TInt)), RVal(Var("xS_B", TInt))))]);;


let test12 = 
    extract_substitutions (List.hd (monomials q12)) [("xS_B", TInt)] =
    ([(("B", TInt), ("xS_B", TInt));
    (("xR_B", TInt), ("xS_B", TInt));
    (("xS_B", TInt), ("xS_B", TInt))], relalg_one);;

let test13 = vars q12 = [("B", TInt); ("xR_B", TInt); ("xS_B", TInt)];;

let test15 =
extract_substitutions(make(
RA_MultiNatJoin [
   RA_Leaf (AtomicConstraint
       (Lt, RVal(Var("A", TInt)), RVal(Var("x", TInt))));
   RA_Leaf (AtomicConstraint
       (Eq, RVal(Var("B", TInt)), RVal(Var("y", TInt))));
   RA_Leaf (Rel("S", [("B", TInt); ("C", TInt)]))])) [("y", TInt)]
=
([(("B", TInt), ("y", TInt)); (("y", TInt), ("y", TInt))],
make(
 RA_MultiNatJoin
  [RA_Leaf (AtomicConstraint
      (Lt, RVal(Var("A", TInt)), RVal(Var("x", TInt))));
   RA_Leaf (Rel ("S", [("y", TInt); ("C", TInt)]))]))
;;



let test16 =
extract_substitutions
(make(
     RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint
          (Eq, RVal(Var("x", TInt)), RVal(Var("AA", TInt))));
       RA_Leaf (AtomicConstraint
           (Eq, RVal(Var("x", TInt)), RVal(Var("BB", TInt))))]))
[("AA", TInt); ("BB", TInt)] =
([(("x", TInt), ("AA", TInt));
(("AA", TInt), ("AA", TInt));
(("BB", TInt), ("AA", TInt))],
 make(RA_Leaf (AtomicConstraint
     (Eq, RVal(Var("AA", TInt)), RVal(Var("BB", TInt))))));;



