
open Calculus;;

let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;


(* (R bowtie S) bowtie T *)
let q = RA_MultiNatJoin [relR; relS; relT];;

let q2 = term_delta Compiler.externals_forbidden false "R" ["x"; "y"]
   (make_term(RVal (AggSum(RVal(Const (Int 1)), relR))));;


let test01 = readable_term(term_one)  = RVal (Const (Int 1));;
let test02 = readable_term(term_zero) = RVal (Const (Int 0));;


(* To run these tests, compile without Calculus.mli *)
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

let test11 = simplify(mk_val (AggSum(term_one, relcalc_one))) [] [] =
   [([], term_one)];;
let test12 = simplify(mk_val (AggSum(term_zero, relcalc_one))) [] [] =
   [([], term_zero)];;
let test13 = simplify(mk_val (AggSum(term_zero, make_relcalc(relR)))) [] [] =
   [([], term_zero)];;
*)


let test10 = term_delta Compiler.externals_forbidden false "R" ["x"; "y"] term_one = term_zero;;


let test11 =
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
    (term_delta Compiler.externals_forbidden false "R" ["x"; "y"]
        (make_term (RVal (AggSum(RVal(Const(Int 1)), relS))))) [] []
= [([], term_zero)];;


let test21 =
List.map (fun (x,y) -> (x, readable_term y))
(Calculus.simplify (make_term(RVal(AggSum(RSum[RVal(Var("A")); RVal(Var("C"))],
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



let test23 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify
(make_term(RVal (AggSum (RProd [RVal (Var "A"); RVal (Var "C")],
RA_MultiNatJoin
    [RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, RVal(Var("A")), RVal(Var("x"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("y"))))];
     RA_Leaf (Rel ("S", ["B"; "C"]))]
))))
["x"; "y"] []) =
[([],
  RProd
   [RVal (Var "x");
    RVal (AggSum (RVal (Var "C"), RA_Leaf (Rel ("S", ["y"; "C"]))))])]
;;


let q3 = 
make_term(
RVal (AggSum (RProd [RVal (Var "B"); RVal (Var "C")],
RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("x"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Var("y"))));
       RA_Leaf (AtomicConstraint (Eq, RVal(Var("C")), RVal(Var("z"))))]
)));;


let test24 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 [] []) = [([], RProd [RVal (Var "B"); RVal (Var "C")])];;
let test25 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 ["y"] []) = [([], RProd [RVal (Var "y"); RVal (Var "C")])];;
let test26 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 ["w"] []) = [([], RProd [RVal (Var "B"); RVal (Var "C")])];;



let test27 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (term_delta Compiler.externals_forbidden false "R" ["A"; "B"]
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



let test28 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (term_delta Compiler.externals_forbidden false "R" ["x"; "y"]
(make_term(RVal(AggSum(RVal(Const (Int 1)),
RA_MultiNatJoin [
   RA_Leaf (Rel ("R", ["A"; "B"]));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var("B")), RVal(Const(Int 5))))
]))))
) ["x"; "y"] [])
=
[([],
  RVal
   (AggSum
     (RVal (Const (Int 1)),
      RA_Leaf (AtomicConstraint (Eq, RVal (Var "y"), RVal (Const (Int 5)))))))]
;;




let test29 =
(term_delta Compiler.externals_forbidden false "R" ["x"; "y"] (make_term(
   RVal(AggSum(RVal(Const(Int 1)), RA_Leaf(True)))
))) =
make_term (RVal(Const(Int 0)));;



let test30 =
readable_term (term_delta Compiler.externals_forbidden false "R" ["x"; "y"] (make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)), RVal(Var("z"))))))
))) =
   RVal (Const (Int 0))
;;


(* if (0 < AggSum(1, R(A,B))) then 1 else 0 *)
let test31 =
List.map (fun (x,y) -> (x, term_as_string y))
(simplify (term_delta Compiler.externals_forbidden false "R" ["x"; "y"] (make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)), RA_Leaf(Rel("R", ["A"; "B"]))))))))
))) ["x"; "y"] [])
=
[([], "(if 0<(AggSum(1, R(A, B))+1) and AggSum(1, R(A, B))<=0 then 1 else 0)");
 ([],
  "(-1*(if (AggSum(1, R(A, B))+1)<=0 and 0<AggSum(1, R(A, B)) then 1 else 0))")]
;;
(* correct, but the condition of the second row is unsatisfiable, so the
   row could be removed. *)


let test32 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (make_term (
RVal
 (AggSum
   (RVal (Const (Int 2)),
     RA_Leaf
       (AtomicConstraint (Le,
           RVal
            (AggSum
              (RVal (Var "A"),
                RA_Leaf
                  (AtomicConstraint (Eq, RVal (Var "A"), RVal (Var "x"))))),
            RVal (Const (Int 0))));
      ))
)) ["x"] []) =
[([],
  RProd
   [RVal (Const (Int 2));
    RVal
     (AggSum
       (RVal (Const (Int 1)),
        RA_Leaf (AtomicConstraint (Le, RVal (Var "x"), RVal (Const (Int 0))))))])]
;;


let test33 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (make_term (
RVal (AggSum (RVal (Var "A"),
   RA_Leaf (AtomicConstraint (Eq, RVal (Var "A"), RVal (Var "x")))))
)) ["x"] []) =
[([], RVal (Var "x"))]
;;


(* this throws an exception because the two innermost AtomicConstraints
   produce inconsistent variable mappings for x and y.

   Commented out: now this doesn't throw an exception anymore.
*)
(*
let test34 =
try
ignore
(List.map (fun (x,y) -> (x, readable_term y))
(simplify (make_term (
RVal (AggSum ((RVal (Const (Int 1))),
   RA_MultiNatJoin [
              RA_Leaf (AtomicConstraint (Eq,
                        RVal(AggSum((RVal (Const (Int 1))),
      RA_Leaf (AtomicConstraint (Eq, RVal (Var "x"), RVal (Var "y"))))),
                 RVal (Const (Int 1))));
              RA_Leaf (AtomicConstraint (Eq,
                        RVal(AggSum((RVal (Const (Int 1))),
      RA_Leaf (AtomicConstraint (Eq, RVal (Var "y"), RVal (Var "x"))))),
                 RVal (Const (Int 1))))
                   ])))) [] [])
); false
with _ -> true;;
*)


let test35 =
Calculus.simplify_roly true (make_term
  (RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf
      (AtomicConstraint (Eq, RVal (Const (Int 0)),
        RSum
         [RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var "z"), RVal(Var "y")))));
          RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var "z"), RVal(Var "x")))))
]))))
)) ["x"; "y"] =
([], make_term (RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf (AtomicConstraint (Eq, RVal (Const (Int 0)),
       RSum [RVal (Const (Int 1)); RVal (Const (Int 1))]))))))
;;



let test36 =
Calculus.simplify_roly true (make_term (
  RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf
      (AtomicConstraint (Eq,
         RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var "z"), RVal(Var "y"))))),
         RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var "z"), RVal(Var "x")))))
)))))) ["x"; "y"] =
([], make_term( RVal (AggSum (RVal (Const (Int 1)),
  RA_Leaf (AtomicConstraint (Eq, RVal (Const (Int 1)), RVal (Const (Int 1))))))
))
;;





