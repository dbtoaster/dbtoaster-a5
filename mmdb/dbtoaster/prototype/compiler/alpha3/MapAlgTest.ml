
open Algebra;;

let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;

let relR2 = make_relalg relR;;

let pos = fun x -> x
let neg = fun x -> make_term (RProd[RVal(Const(Int(-1))); (readable_term x)])

(* (R bowtie S) bowtie T *)
let q = RA_MultiNatJoin [relR; relS; relT];;

let q2 = term_delta pos "R" [("x", TInt); ("y", TInt)]
   (make_term(RVal (AggSum(RVal(Const (Int 1)), relR))));;


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


let test10 = term_delta pos "R" [("x", TInt); ("y", TInt)] term_one = term_zero;;


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
     (RVal (AggSum (RVal (Const (Int 1)), relR)), relR));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), relR)), relR));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), relR)), relS));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)), relR)), relS));
  RProd
   [RVal (AggSum (RVal (Const (Int 1)), relR));
    RVal (AggSum (RVal (Const (Int 1)), relT))];
  RProd
   [RVal (AggSum (RVal (Const (Int 1)), relR));
    RVal (AggSum (RVal (Const (Int 1)), relT))]]
);;


let test16 = readable_term q2 =
RVal
 (AggSum
   (RVal (Const (Int 1)),
    RA_MultiNatJoin
     [RA_Leaf (AtomicConstraint
         (Eq, RVal (Var("A", TInt)), RVal (Var("x", TInt))));
      RA_Leaf (AtomicConstraint
          (Eq, RVal (Var("B", TInt)), RVal (Var("y", TInt))))]))
;;

let test17 = simplify q2 [] [] = [([], term_one)];;

let test19 = simplify
    (term_delta pos "R" [("x", TInt); ("y", TInt)]
        (make_term (RVal (AggSum(RVal(Const(Int 1)), relS))))) [] []
= [([], term_zero)];;


let test21 =
List.map (fun (x,y) -> (x, readable_term y))
(Algebra.simplify (make_term(
    RVal(AggSum(RSum[RVal(Var("A", TInt)); RVal(Var("C", TInt))],
                    RA_MultiNatJoin [relR; relT])))) [] []) =
  [([],
    RProd
     [RVal (AggSum (RVal (Var("A", TInt)), relR));
      RVal (AggSum (RVal (Const (Int 1)), relT))]);
   ([],
    RProd
     [RVal (AggSum (RVal (Var("C", TInt)), relT));
      RVal (AggSum (RVal (Const (Int 1)), relR))])]
;;

let test22 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (make_term(
    RVal(AggSum(RProd
        [RVal(Var("A", TInt)); RVal(Var("C", TInt)); RVal(Var("D", TInt))],
   (RA_MultiNatJoin[RA_Leaf(Rel("R", [("A", TInt); ("B", TInt); ("D", TInt)]));
                   RA_Leaf(Rel("S", [("C", TInt); ("E", TInt)]))]))))) [] [])
=
[([],
  RProd
   [RVal
     (AggSum
       (RProd [RVal (Var("A", TInt)); RVal (Var ("D", TInt))],
        RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt); ("D", TInt)]))));
    RVal (AggSum (RVal (Var ("C", TInt)),
      RA_Leaf (Rel ("S", [("C", TInt); ("E", TInt)]))))])]
;;



let test23 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify
(make_term(RVal (AggSum (RProd [RVal (Var("A", TInt)); RVal (Var("C", TInt))],
RA_MultiNatJoin
    [RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint
          (Eq, RVal(Var("A", TInt)), RVal(Var("x", TInt))));
       RA_Leaf (AtomicConstraint
           (Eq, RVal(Var("B", TInt)), RVal(Var("y", TInt))))];
     RA_Leaf (Rel ("S", [("B", TInt); ("C", TInt)]))]
))))
[("x", TInt); ("y", TInt)] []) =
[([],
  RProd
   [RVal (Var("x", TInt));
    RVal (AggSum (RVal (Var("C", TInt)),
      RA_Leaf (Rel ("S", [("y", TInt); ("C", TInt)]))))])]
;;


let q3 = 
make_term(
RVal (AggSum (RProd [RVal (Var("B", TInt)); RVal (Var("C", TInt))],
RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint
          (Eq, RVal(Var("B", TInt)), RVal(Var("x", TInt))));
       RA_Leaf (AtomicConstraint
           (Eq, RVal(Var("B", TInt)), RVal(Var("y", TInt))));
       RA_Leaf (AtomicConstraint
           (Eq, RVal(Var("C", TInt)), RVal(Var("z", TInt))))]
)));;


let test24 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 [] []) =
    [([], RProd [RVal (Var("B", TInt)); RVal (Var("C", TInt))])];;
let test25 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 [("y", TInt)] []) =
    [([], RProd [RVal (Var("y", TInt)); RVal (Var("C", TInt))])];;
let test26 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify q3 [("w", TInt)] []) =
    [([], RProd [RVal (Var("B", TInt)); RVal (Var("C", TInt))])];;



let test27 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (term_delta pos "R" [("A", TInt); ("B", TInt)]
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
 RA_Leaf(Rel("R", [("y", TInt); ("z", TInt)]))]))))))
[] [("x", TInt)]) =
[([("x", TInt)],
    RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf (Rel ("R", [("y", TInt); ("z", TInt)])))));
 ([("x", TInt)],
    RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf (Rel ("R", [("x", TInt); ("y", TInt)])))));
 ([("x", TInt)], RVal (Const (Int 1)))]
;;



let test28 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (term_delta pos "R" [("x", TInt); ("y", TInt)]
(make_term(RVal(AggSum(RVal(Const (Int 1)),
RA_MultiNatJoin [
   RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var("B", TInt)), RVal(Const(Int 5))))
]))))
) [("x", TInt); ("y", TInt)] [])
=
[([],
  RVal
   (AggSum
     (RVal (Const (Int 1)),
      RA_Leaf (AtomicConstraint
          (Eq, RVal (Var("y", TInt)), RVal (Const (Int 5)))))))]
;;




let test29 =
(term_delta pos "R" [("x", TInt); ("y", TInt)] (make_term(
   RVal(AggSum(RVal(Const(Int 1)), RA_Leaf(ConstantNullarySingleton)))
))) =
make_term (RVal(Const(Int 0)));;



let test30 =
readable_term (term_delta pos "R" [("x", TInt); ("y", TInt)] (make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)), RVal(Var("z", TInt))))))
))) =
  RSum
   [RVal
     (AggSum
       (RVal (Const (Int 1)),
        RA_MultiNatJoin
         [RA_Leaf
           (AtomicConstraint (Lt, RVal (Const (Int 0)), RVal (Var("z", TInt))));
          RA_Leaf
           (AtomicConstraint (Le, RVal (Var("z", TInt)),
             RVal (Const (Int 0))))]));
    RVal
     (AggSum
       (RVal (Const (Int (-1))),
        RA_MultiNatJoin
         [RA_Leaf
           (AtomicConstraint
               (Le, RVal (Var("z", TInt)), RVal (Const (Int 0))));
          RA_Leaf
           (AtomicConstraint
               (Lt, RVal (Const (Int 0)), RVal (Var("z", TInt))))]))]
;;
(* correct, but both conditions are unsatisfiable, so the value is 0 *)


(* if (0 < AggSum(1, R(A,B))) then 1 else 0 *)
let test31 =
List.map (fun (x,y) -> (x, term_as_string y []))
(simplify (term_delta pos "R" [("x", TInt); ("y", TInt)] (make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)),
      RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]))))))))
))) [("x", TInt); ("y", TInt)] [])
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
              (RVal (Var("A", TInt)),
                RA_Leaf
                  (AtomicConstraint
                      (Eq, RVal (Var("A", TInt)), RVal (Var("x", TInt)))))),
            RVal (Const (Int 0))));
      ))
)) [("x", TInt)] []) =
[([],
  RProd
   [RVal (Const (Int 2));
    RVal
     (AggSum
       (RVal (Const (Int 1)),
        RA_Leaf (AtomicConstraint
            (Le, RVal (Var("x", TInt)), RVal (Const (Int 0))))))])]
;;


let test33 =
List.map (fun (x,y) -> (x, readable_term y))
(simplify (make_term (
RVal (AggSum (RVal (Var("A", TInt)),
   RA_Leaf (AtomicConstraint
       (Eq, RVal (Var("A", TInt)), RVal (Var("x", TInt))))))
)) [("x", TInt)] []) =
[([], RVal (Var("x", TInt)))]
;;




