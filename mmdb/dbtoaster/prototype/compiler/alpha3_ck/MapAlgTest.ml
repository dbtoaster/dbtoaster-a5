
open Algebra;;

let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;

let relR2 = make_relalg relR;;


(* (R bowtie S) bowtie T *)
let q = RA_MultiNatJoin [relR; relS; relT];;

let q1 = make_term(RVal (AggSum(RVal(Const (Int 1)), relR)));;
let q2 = term_delta "R" ["x"; "y"] q1;;


let test01 = readable_term(term_one)  = RVal (Const (Int 1));;
let test02 = readable_term(term_zero) = RVal (Const (Int 0));;


(* To run these tests, compile without Algebra.mli *)
(*
let mk_sum  l = TermSemiRing.mk_sum l;;
let mk_prod l = TermSemiRing.mk_prod l;;
let mk_val  x = TermSemiRing.mk_val x;;

let test03 = mk_sum [] = term_zero;;
let test04 = mk_sum [mk_sum[]] = term_zero;;
let test05 = mk_prod[] = term_one;;
let test06 = mk_sum [term_one;term_zero] = term_one;;
let test07 = mk_prod[term_one;term_zero] = term_zero;;
let test08 = mk_sum [term_zero;term_one;term_zero] = term_one;;
let test09 = mk_prod[term_one;term_zero;term_one;term_one] = term_zero;;

let test11 = simplify(mk_val (AggSum(term_one, relalg_one))) [] [] =
   [([], term_one)];;
let test12 = simplify(mk_val (AggSum(term_zero, relalg_one))) [] [] =
   [([], term_zero)];;
let test13 = simplify(mk_val (AggSum(term_zero, make_relalg(relR)))) [] [] =
   [([], term_zero)];;
*)


let test10 = term_delta "R" ["x"; "y"] term_one = term_zero;;


roly_poly(
   make_term(
    RVal (AggSum(RSum[RVal (AggSum(RVal(Const (Int 1)), relR));
                      RVal (AggSum(RVal(Const (Int 1)), relR))],
                 RA_MultiUnion [relR; relS; relT]))))
=
make_term(
RSum
 [RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("R", ["A"; "B"]))));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("R", ["A"; "B"]))));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("S", ["B"; "C"]))));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("S", ["B"; "C"]))));
  RProd
   [RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"]))));
    RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("T", ["C"; "D"]))))];
  RProd
   [RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"]))));
    RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("T", ["C"; "D"]))))]]
);;


let test16 = readable_term q2 =
RVal
 (AggSum
   (RVal (Const (Int 1)),
    RA_MultiNatJoin
     [RA_Leaf (AtomicConstraint (Eq, RVal (Var "A"), RVal (Var "x")));
      RA_Leaf (AtomicConstraint (Eq, RVal (Var "B"), RVal (Var "y")))]))
;;

let test17 = simplify q2 [] [] = [([], term_one)];;

let test19 = simplify
    (term_delta "R" ["x"; "y"]
        (make_term (RVal (AggSum(RVal(Const(Int 1)), relS))))) [] []
= [([], term_zero)];;


let test21 =
List.map (fun (x,y) -> (x, readable_term y))
(Algebra.simplify (make_term(RVal(AggSum(RSum[RVal(Var("A")); RVal(Var("C"))],
                    RA_MultiNatJoin [relR; relT])))) [] []) =
  [([],
    RProd
     [RVal (AggSum (RVal (Var "A"), RA_Leaf (Rel ("R", ["A"; "B"]))));
      RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("T", ["C"; "D"]))))]);
   ([],
    RProd
     [RVal (AggSum (RVal (Var "C"), RA_Leaf (Rel ("T", ["C"; "D"]))));
      RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["A"; "B"]))))])]
;;

let test22 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (make_term(RVal(AggSum(RProd[RVal(Var("A")); RVal(Var("C")); RVal(Var("D"))],
   (RA_MultiNatJoin[RA_Leaf(Rel("R", ["A"; "B"; "D"]));
                   RA_Leaf(Rel("S", ["C"; "E"]))]))))) [] [])
=
[([],
  RProd
   [RVal
     (AggSum
       (RProd [RVal (Var "A"); RVal (Var "D")],
        RA_Leaf (Rel ("R", ["A"; "B"; "D"]))));
    RVal (AggSum (RVal (Var "C"), RA_Leaf (Rel ("S", ["C"; "E"]))))])]
;;


let q3 =
make_term(RVal (AggSum (RProd [RVal (Var "A"); RVal (Var "C")],
RA_MultiNatJoin
    [RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, RVal(Var("A")), RVal(Var("x"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("y"))))];
     RA_Leaf (Rel ("S", ["B"; "C"]))]
))
);;


let test23 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 ["x"; "y"] []) =
[([],
  RProd
   [RVal (Var "x");
    RVal (AggSum (RVal (Var "C"), RA_Leaf (Rel ("S", ["y"; "C"]))))])]
;;


let q4 = 
make_term(
RVal (AggSum (RProd [RVal (Var "B"); RVal (Var "C")],
RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("x"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("y"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("C")), RVal(Var("z"))))]
)));;


let test24 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q4 [] []) =
   [([], RProd [RVal (Var "B"); RVal (Var "C")])];;
let test25 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q4 ["y"] []) =
   [([], RProd [RVal (Var "y"); RVal (Var "C")])];;
let test26 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q4 ["w"] []) =
   [([], RProd [RVal (Var "B"); RVal (Var "C")])];;




List.map (fun (x,y) -> (x, readable_term y))
(simplify (term_delta "R" ["A"; "B"]
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", ["x"; "y"]));
 RA_Leaf(Rel("R", ["y"; "z"]))]))))))
[] ["x"]) =
[(["x"], RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["y"; "z"])))));
 (["x"], RVal (AggSum (RVal (Const (Int 1)), RA_Leaf (Rel ("R", ["x"; "y"])))));
 (["x"], RVal (Const (Int 1)))]
;;



let q =
make_term(RVal(AggSum(RVal(Const (Int 1)),
RA_MultiNatJoin [
   RA_Leaf (Rel ("R", ["A"; "B"]));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Const(Int 5))))
])));;


List.map (fun (x,y) -> (x, readable_term y))
(simplify (term_delta "R" ["x"; "y"] q) ["x"; "y"] [])
=
[([],
  RVal
   (AggSum
     (RVal (Const (Int 1)),
      RA_Leaf (AtomicConstraint (Eq, RVal (Var "y"), RVal (Const (Int 5)))))))]
;;







