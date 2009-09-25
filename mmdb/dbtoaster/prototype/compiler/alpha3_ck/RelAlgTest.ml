open RelAlg;;


let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;
let relU = RA_Leaf(Rel("U", ["A"; "D"]));;


let test01 = make(RA_Leaf Empty) = zero;;
let test02 = polynomial (make (RA_Leaf Empty)) = make(RA_Leaf Empty);;
let test03 = monomials  (make (RA_Leaf Empty)) = [];;



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

let test08 = readable (polynomial (delta "R" ["a"; "b"] q)) =
  RA_MultiNatJoin
   [RA_Leaf (AtomicConstraint (Eq, "A", "a"));
    RA_Leaf (AtomicConstraint (Eq, "B", "b"));
    relS; relT]
;;

let test09 = readable (polynomial (delta "S" ["b"; "c"] q)) =
RA_MultiNatJoin
 [relR;
  RA_Leaf (AtomicConstraint (Eq, "B", "b"));
  RA_Leaf (AtomicConstraint (Eq, "C", "c"));
  relT]
;;

let test10 =
polynomial(make(
   RA_MultiNatJoin [RA_MultiNatJoin[RA_Leaf (Empty); relS];
                    RA_MultiNatJoin[relR; relS]])) =
make(RA_Leaf Empty)
;;

let test11 = schema (make(RA_MultiNatJoin[relR; relS])) = ["A"; "B"; "C"] ;;


let q12 = make(
RA_MultiNatJoin [RA_Leaf (AtomicConstraint (Eq, "B", "xR_B"));
       RA_Leaf (AtomicConstraint (Eq, "B", "xS_B"))]);;


let test12 = extract_substitutions (List.hd (monomials q12)) ["xS_B"] =
([("B", "xS_B"); ("xR_B", "xS_B"); ("xS_B", "xS_B")], one);;

let test13 = vars   q12 = ["B"; "xR_B"; "xS_B"];;
let test14 = schema q12 = ["B"; "xR_B"; "xS_B"];;

let test15 =
extract_substitutions(make(
RA_MultiNatJoin [RA_Leaf (AtomicConstraint (Lt, "A", "x"));
                 RA_Leaf (AtomicConstraint (Eq, "B", "y"));
                 RA_Leaf (Rel("S", ["B"; "C"]))])) ["y"]
=
([("B", "y"); ("y", "y")],
make(
 RA_MultiNatJoin
  [RA_Leaf (AtomicConstraint (Lt, "A", "x"));
   RA_Leaf (Rel ("S", ["y"; "C"]))]))
;;



(* FIXME: this throws an exception. but this scenario arises in the
   delta of a self-join. *)
RelAlg.extract_substitutions
(RelAlg.make(
     RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, "x", "AA"));
       RA_Leaf (AtomicConstraint (Eq, "x", "BB"))]))
["AA"; "BB"] =
([("x", "AA"); ("AA", "AA"); ("BB", "AA")],
 RelAlg.make(RA_Leaf (AtomicConstraint (Eq, "AA", "BB"))));;








