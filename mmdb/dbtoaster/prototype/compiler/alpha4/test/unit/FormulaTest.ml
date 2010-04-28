open Calculus
open Util;;

let make x = make_relcalc x;;
let readable x = readable_relcalc x;;
let vars x = relcalc_vars x;;

let test s = Debug.log_unit_test ("Formula "^s);;
let rr_as_string r = relcalc_as_string (make_relcalc r);;
let rl_as_string l = String.concat "\n" (List.map relcalc_as_string l);;
let vl_as_string l = String.concat "," (List.map fst l);;
let bvr_as_string (l,r) = (vl_as_string (List.map fst l))^" "^(relcalc_as_string r);;

let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;
let relU = RA_Leaf(Rel("U", [("A", TInt); ("D", TInt)]));;


test "test01" relcalc_as_string (make(RA_Leaf False)) relcalc_zero;;
test "test02" relcalc_as_string (polynomial (make (RA_Leaf False))) (make(RA_Leaf False));;
(*
test "test03" rl_as_string (monomials  (make (RA_Leaf False))) [];;
*)



(* (R bowtie S) bowtie T *)
let q_r = RA_MultiNatJoin [RA_MultiNatJoin ([relR; relS]); relT];;
let q = make q_r;;

test "test04" rr_as_string (readable q) q_r;;
test "test05" rr_as_string
   (readable (polynomial q)) (RA_MultiNatJoin [relR; relS; relT]);;

test "test06" relcalc_as_string (make relR)
(polynomial (make (RA_MultiNatJoin[RA_MultiNatJoin[RA_MultiNatJoin[relR]]])));;

test "test07" rr_as_string 
(readable(polynomial(make(
   RA_MultiNatJoin [RA_MultiUnion([relR; relS]); RA_MultiUnion([relT; relU])]
))))
(RA_MultiUnion
 [RA_MultiNatJoin [relR; relT]; RA_MultiNatJoin [relS; relT];
  RA_MultiNatJoin [relR; relU]; RA_MultiNatJoin [relS; relU]])
;;

test "test08"  rr_as_string
(readable (polynomial
   (relcalc_delta [] false "R" [("a", TInt); ("b", TInt)] q)))
  (RA_MultiNatJoin
   [RA_Leaf (AtomicConstraint (Eq, RVal(Var(("A", TInt))), RVal(Var(("a", TInt)))));
    RA_Leaf (AtomicConstraint (Eq, RVal(Var(("B", TInt))), RVal(Var(("b", TInt)))));
    relS; relT])
;;

test "test09"  rr_as_string
(readable (polynomial (relcalc_delta [] false "S" [("b", TInt); ("c", TInt)] q)))
(RA_MultiNatJoin
 [relR;
  RA_Leaf (AtomicConstraint (Eq, RVal(Var(("B", TInt))), RVal(Var(("b", TInt)))));
  RA_Leaf (AtomicConstraint (Eq, RVal(Var(("C", TInt))), RVal(Var(("c", TInt)))));
  relT])
;;

test "test10" relcalc_as_string
(polynomial(make(
   RA_MultiNatJoin [RA_MultiNatJoin[RA_Leaf (False); relS];
                    RA_MultiNatJoin[relR; relS]])))
(make(RA_Leaf False))
;;

let q12 = make(
RA_MultiNatJoin [
   RA_Leaf (AtomicConstraint (Eq, RVal(Var(("B", TInt))), RVal(Var(("xR_B", TInt)))));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var(("B", TInt))), RVal(Var(("xS_B", TInt)))))]);;


test "test12" bvr_as_string
(extract_substitutions (List.hd (monomials q12)) [("xS_B", TInt)])
([(("B", TInt), ("xS_B", TInt)); (("xR_B", TInt), ("xS_B", TInt));
  (("xS_B", TInt), ("xS_B", TInt))], relcalc_one);;

test "test13" vl_as_string
   (vars   q12) [("B", TInt); ("xR_B", TInt); ("xS_B", TInt)];;

test "test15" bvr_as_string
(extract_substitutions(make(
RA_MultiNatJoin [
   RA_Leaf (AtomicConstraint (Lt, RVal(Var(("A", TInt))), RVal(Var(("x", TInt)))));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var(("B", TInt))), RVal(Var(("y", TInt)))));
   RA_Leaf (Rel("S", [("B", TInt); ("C", TInt)]))])) [("y", TInt)])
([(("B", TInt), ("y", TInt)); (("y", TInt), ("y", TInt))],
make(
 RA_MultiNatJoin
  [RA_Leaf (AtomicConstraint (Lt, RVal(Var(("A", TInt))), RVal(Var(("x", TInt)))));
   RA_Leaf (Rel ("S", [("y", TInt); ("C", TInt)]))]))
;;



test "test16" bvr_as_string
(extract_substitutions
(make(
     RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, RVal(Var(("x", TInt))), RVal(Var(("AA", TInt)))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var(("x", TInt))), RVal(Var(("BB", TInt)))))]))
[("AA", TInt); ("BB", TInt)])
([(("x", TInt), ("AA", TInt));
  (("AA", TInt), ("AA", TInt)); (("BB", TInt), ("AA", TInt))],
 make(RA_Leaf (AtomicConstraint (Eq,
    RVal(Var(("AA", TInt))), RVal(Var(("BB", TInt)))))));;


test "safe vars 1" vl_as_string
(safe_vars (make(RA_MultiUnion[relR; relT])) []) [];;

test "safe vars 2" vl_as_string
(safe_vars (make(RA_MultiUnion[relR; relT])) [("A", TInt)]) [("A", TInt)];;

test "safe vars 3" vl_as_string
(safe_vars (make(RA_MultiUnion[relR;
   RA_MultiNatJoin[relT; RA_Leaf (AtomicConstraint (Eq, RVal(Var(("C", TInt))),
                                     RVal(Var(("A", TInt)))))]])) [])
[("A", TInt)];;



