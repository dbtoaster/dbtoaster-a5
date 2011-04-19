
open Calculus
open Util;;

let test s = Debug.log_unit_test ("Term "^s);;
let rt_as_string r = term_as_string(make_term r);;
let strvars l = String.concat "," (List.map fst l);;
let s_as_string l = String.concat "\n" (List.map (fun ((x,y),z) ->
   String.concat "; " [(strvars x); (term_as_string y); (strvars z)]) l);;

let sr_as_string l = String.concat "\n" (List.map (fun ((x,y),z) ->
   String.concat "; "
      [(strvars x); (term_as_string (make_term y)); (strvars z)]) l);;
 
let ss_as_string l = String.concat ";" (List.map (fun ((x,y),z) -> y) l)

let relR = RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]));;
let relS = RA_Leaf(Rel("S", [("B", TInt); ("C", TInt)]));;
let relT = RA_Leaf(Rel("T", [("C", TInt); ("D", TInt)]));;


(* (R bowtie S) bowtie T *)
let q = RA_MultiNatJoin [relR; relS; relT];;

let q2 = term_delta [] false "R" [("x", TInt); ("y", TInt)]
   (make_term(RVal (AggSum(RVal(Const (Int 1)), relR))));;


test "test01" rt_as_string
   (readable_term(term_one)) (RVal (Const (Int 1)));;

test "test02" rt_as_string
   (readable_term(term_zero)) (RVal (Const (Int 0)));;


(* To run these tests, compile without Calculus.mli *)
(*
let mk_sum  l = TermSemiRing.mk_sum l;;
let mk_prod l = TermSemiRing.mk_prod l;;
let mk_val  x = TermSemiRing.mk_val x;;

test "test03" (mk_sum [])   term_zero;;
test "test04" (mk_sum [mk_sum[]])   term_zero;;
test "test05" (mk_prod[])   term_one;;
test "test06" (mk_sum [term_one;term_zero])   term_one;;
test "test07" (mk_prod[term_one;term_zero])   term_zero;;
test "test08" (mk_sum [term_zero;term_one;term_zero])   term_one;;
test "test09" (mk_prod[term_one;term_zero;term_one;term_one]) term_zero;;

test "test11" s_as_string
   (simplify(mk_val (AggSum(term_one, relcalc_one))) [] [])  
   [([], term_one)];;
test "test12" s_as_string
   (simplify(mk_val (AggSum(term_zero, relcalc_one))) [] [])  
   [([], term_zero)];;
test "test13" s_as_string
   (simplify(mk_val (AggSum(term_zero, make_relcalc(relR)))) [] [])  
   [([], term_zero)];;
*)


test "test10" term_as_string
   (term_delta [] false "R" [("x", TInt); ("y", TInt)] term_one) term_zero;;


test "test11" term_as_string
(roly_poly(
   make_term(
    RVal (AggSum(RSum[RVal (AggSum(RVal(Const (Int 1)), relR));
                      RVal (AggSum(RVal(Const (Int 1)), relR))],
                 RA_MultiUnion [relR; relS; relT])))))
(make_term(
RSum
 [RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)),
                    RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)])))),
      RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]))));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)),
                    RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)])))),
     RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]))));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)),
                    RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)])))),
     RA_Leaf (Rel ("S", [("B", TInt); ("C", TInt)]))));
  RVal
   (AggSum
     (RVal (AggSum (RVal (Const (Int 1)),
                    RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)])))),
     RA_Leaf (Rel ("S", [("B", TInt); ("C", TInt)]))));
  RProd
   [RVal (AggSum (RVal (Const (Int 1)),
                  RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]))));
    RVal (AggSum (RVal (Const (Int 1)),
                  RA_Leaf (Rel ("T", [("C", TInt); ("D", TInt)]))))];
  RProd
   [RVal (AggSum (RVal (Const (Int 1)),
                  RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]))));
    RVal (AggSum (RVal (Const (Int 1)),
                  RA_Leaf (Rel ("T", [("C", TInt); ("D", TInt)]))))]]
));;


test "test16" rt_as_string (readable_term q2)
(RVal
 (AggSum
   (RVal (Const (Int 1)),
    RA_MultiNatJoin
     [RA_Leaf (AtomicConstraint (Eq,
         RVal (Var ("A", TInt)), RVal (Var ("x", TInt))));
      RA_Leaf (AtomicConstraint (Eq,
         RVal (Var ("B", TInt)), RVal (Var ("y", TInt))))])))
;;

test "test17" s_as_string
   (simplify q2 [] [] []) ([(([], term_one), [])]);;

test "test19" s_as_string (simplify
    (term_delta [] false "R" [("x", TInt); ("y", TInt)]
        (make_term (RVal (AggSum(RVal(Const(Int 1)), relS))))) [] [] []) [];;


test "test21" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(Calculus.simplify
   (make_term(RVal(AggSum(RSum[RVal(Var(("A", TInt))); RVal(Var(("C", TInt)))],
                          RA_MultiNatJoin [relR; relT])))) [] [] []))
  ([(([],
    RProd
     [RVal (AggSum (RVal (Var ("A", TInt)),
                    RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]))));
      RVal (AggSum (RVal (Const (Int 1)),
                    RA_Leaf (Rel ("T", [("C", TInt); ("D", TInt)]))))]),
     []);
   (([],
    RProd
     [RVal (AggSum (RVal (Var ("C", TInt)),
                    RA_Leaf (Rel ("T", [("C", TInt); ("D", TInt)]))));
      RVal (AggSum (RVal (Const (Int 1)),
                    RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]))))]),
     [])])
;;

test "test22" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify (make_term(RVal(
   AggSum(RProd[RVal(Var(("A", TInt)));
                RVal(Var(("C", TInt))); RVal(Var(("D", TInt)))],
         (RA_MultiNatJoin[
            RA_Leaf(Rel("R", [("A", TInt); ("B", TInt); ("D", TInt)]));
            RA_Leaf(Rel("S", [("C", TInt); ("E", TInt)]))]))))) [] [] []))
([(([],
  RProd
   [RVal
     (AggSum
       (RProd [RVal (Var ("A", TInt)); RVal (Var ("D", TInt))],
        RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt); ("D", TInt)]))));
    RVal (AggSum (RVal (Var ("C", TInt)),
                  RA_Leaf (Rel ("S", [("C", TInt); ("E", TInt)]))))]),
   [])])
;;



test "test23" sr_as_string 
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify
(make_term(RVal (AggSum (RProd [RVal (Var ("A", TInt)); RVal (Var ("C", TInt))],
RA_MultiNatJoin
    [RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq,
         RVal(Var(("A", TInt))), RVal(Var(("x", TInt)))));
       RA_Leaf (AtomicConstraint (Eq,
         RVal(Var(("B", TInt))), RVal(Var(("y", TInt)))))];
     RA_Leaf (Rel ("S", [("B", TInt); ("C", TInt)]))]
))))
[("x", TInt); ("y", TInt)] [] []))

([(([],
   RProd
    [RVal (Var ("x", TInt));
     RVal (AggSum (RVal (Var ("C", TInt)),
                   RA_Leaf (Rel ("S", [("y", TInt); ("C", TInt)]))))]),
   [])])
;;


let q3 = 
make_term(
RVal (AggSum (RProd [RVal (Var ("B", TInt)); RVal (Var ("C", TInt))],
RA_MultiNatJoin
   [RA_Leaf (AtomicConstraint (Eq,
      RVal(Var(("B", TInt))), RVal(Var(("x", TInt)))));
    RA_Leaf (AtomicConstraint (Eq,
       RVal(Var(("B", TInt))), RVal(Var(("y", TInt)))));
    RA_Leaf (AtomicConstraint (Eq,
       RVal(Var(("C", TInt))), RVal(Var(("z", TInt)))))]
)));;


test "test24" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify q3 [] [] []))
   ([(([], RProd [RVal (Var ("B", TInt)); RVal (Var ("C", TInt))]), [])]);;

test "test25" sr_as_string 
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify q3 [("y", TInt)] [] []))
   ([(([], RProd [RVal (Var ("y", TInt)); RVal (Var ("C", TInt))]), [])]);;

test "test26" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify q3 [("w", TInt)] [] []))
   ([(([], RProd [RVal (Var ("B", TInt)); RVal (Var ("C", TInt))]), [])]);;



test "test27" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify (term_delta [] false "R" [("A", TInt); ("B", TInt)]
   (make_term(
      RVal(AggSum(RVal(Const (Int 1)),
                   RA_MultiNatJoin([
 RA_Leaf(Rel("R", [("x", TInt); ("y", TInt)]));
 RA_Leaf(Rel("R", [("y", TInt); ("z", TInt)]))]))))))
[] [] [("x", TInt)]))
([(([("x", TInt)], RVal (AggSum (RVal (Const (Int 1)),
      RA_Leaf (Rel ("R", [("y", TInt); ("z", TInt)]))))),[]);
  (([("x", TInt)], RVal (AggSum (RVal (Const (Int 1)),
      RA_Leaf (Rel ("R", [("x", TInt); ("y", TInt)]))))),[]);
  (([("x", TInt)], RVal (Const (Int 1))),[])])
;;



test "test28" sr_as_string 
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify (term_delta [] false "R" [("x", TInt); ("y", TInt)]
(make_term(RVal(AggSum(RVal(Const (Int 1)),
RA_MultiNatJoin [
   RA_Leaf (Rel ("R", [("A", TInt); ("B", TInt)]));
   RA_Leaf (AtomicConstraint (Eq, RVal(Var(("B", TInt))), RVal(Const(Int 5))))
]))))
) [("x", TInt); ("y", TInt)] [] []))
([(([],
   RVal
    (AggSum
      (RVal (Const (Int 1)),
       RA_Leaf (AtomicConstraint (Eq,
         RVal (Var ("y", TInt)), RVal (Const (Int 5))))))),
   [])])
;;




test "test29" term_as_string
(term_delta [] false "R" [("x", TInt); ("y", TInt)] (make_term(
   RVal(AggSum(RVal(Const(Int 1)), RA_Leaf(True)))
)))
(make_term (RVal(Const(Int 0))));;



test "test30" rt_as_string
(readable_term (term_delta [] false "R" [("x", TInt); ("y", TInt)] (make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)), RVal(Var(("z", TInt)))))))
))))
   (RVal (Const (Int 0)))
;;



let x =
relcalc_delta [] false "R" [("x", TInt); ("y", TInt)] (make_relcalc(
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)),
                  RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]))))))))
;;

test "relcalc_as_string" (fun x -> x)
(relcalc_as_string x)
("(0<(AggSum(1, R(A, B))+(if A=x and B=y then 1 else 0)) and "^
    "AggSum(1, R(A, B))<=0 or -(0<AggSum(1, R(A, B)) and "^
    "(AggSum(1, R(A, B))+(if A=x and B=y then 1 else 0))<=0))")
;;





(* if (0 < AggSum(1, R(A,B))) then 1 else 0 *)
test "test31" ss_as_string
(List.map (fun ((x,y),z) -> ((x, term_as_string y),z))
(simplify (term_delta [] false "R" [("x", TInt); ("y", TInt)] (make_term(
RVal(AggSum(RVal(Const(Int 1)),
   RA_Leaf(AtomicConstraint(Lt, RVal(Const (Int 0)),
      RVal(AggSum(RVal(Const(Int 1)),
                  RA_Leaf(Rel("R", [("A", TInt); ("B", TInt)]))))))))
))) [("x", TInt); ("y", TInt)] [] []))
([(([],
  "(if 0<(AggSum(1, R(A, B))+1) and AggSum(1, R(A, B))<=0 then 1 else 0)"),
   []);
 (([],
  "((if 0<AggSum(1, R(A, B)) and (AggSum(1, R(A, B))+1)<=0 then 1 else 0)*-1)"),
   [])])
;;

(* This is what the compiler produced before we moved to the Ring and the
   new way of computing deltas: (It's equivalent!)

[([], "(if 0<(AggSum(1, R(A, B))+1) and AggSum(1, R(A, B))<=0 then 1 else 0)");
 ([],
  "(-1*(if (AggSum(1, R(A, B))+1)<=0 and 0<AggSum(1, R(A, B)) then 1 else 0))")]
;;
*)
(* correct, but the condition of the second row is unsatisfiable, so the
   row could be removed. *)


test "test32" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify (make_term (
RVal
 (AggSum
   (RVal (Const (Int 2)),
     RA_Leaf
       (AtomicConstraint (Le,
           RVal
            (AggSum
              (RVal (Var ("A", TInt)),
                RA_Leaf
                  (AtomicConstraint (Eq,
                     RVal (Var ("A", TInt)), RVal (Var ("x", TInt)))))),
            RVal (Const (Int 0))));
      ))
)) [("x", TInt)] [] []))
([(([],
  RProd
   [RVal (Const (Int 2));
    RVal
     (AggSum
       (RVal (Const (Int 1)),
        RA_Leaf (AtomicConstraint (Le,
           RVal (Var ("x", TInt)), RVal (Const (Int 0))))))]),
   [])])
;;


test "test33" sr_as_string
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify (make_term (
RVal (AggSum (RVal (Var ("A", TInt)),
   RA_Leaf (AtomicConstraint (Eq, RVal (Var ("A", TInt)), RVal (Var ("x", TInt))))))
)) [("x", TInt)] [] []))
([(([], RVal (Var ("x", TInt))), [])])
;;


(* this throws an exception because the two innermost AtomicConstraints
   produce inconsistent variable mappings for x and y.

   Commented out: now this doesn't throw an exception anymore.
*)
(*
test "test34" string_of_bool
(try
ignore
(List.map (fun ((x,y),z) -> ((x, readable_term y),z))
(simplify (make_term (
RVal (AggSum ((RVal (Const (Int 1))),
   RA_MultiNatJoin [
              RA_Leaf (AtomicConstraint (Eq,
                        RVal(AggSum((RVal (Const (Int 1))),
      RA_Leaf (AtomicConstraint (Eq, RVal (Var ("x", TInt)), RVal (Var ("y", TInt)))))),
                 RVal (Const (Int 1))));
              RA_Leaf (AtomicConstraint (Eq,
                        RVal(AggSum((RVal (Const (Int 1))),
      RA_Leaf (AtomicConstraint (Eq, RVal (Var ("y", TInt)), RVal (Var ("x", TInt)))))),
                 RVal (Const (Int 1))))
                   ])))) [] [] [])
); false
with _ -> true);;
*)


test "test35" (fun (x,y) -> term_as_string y)
(Calculus.simplify_roly true (make_term
  (RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf
      (AtomicConstraint (Eq, RVal (Const (Int 0)),
        RSum
         [RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var ("z", TInt)), RVal(Var ("y", TInt))))));
          RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var ("z", TInt)), RVal(Var ("x", TInt))))))
]))))
)) [("x", TInt); ("y", TInt)] [])
([], make_term (RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf (AtomicConstraint (Eq, RVal (Const (Int 0)),
       RSum [RVal (Const (Int 1)); RVal (Const (Int 1))]))))))
;;



test "test36" (fun (x,y) -> term_as_string y)
(Calculus.simplify_roly true (make_term (
  RVal (AggSum (RVal (Const (Int 1)),
    RA_Leaf
      (AtomicConstraint (Eq,
         RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var ("z", TInt)), RVal(Var ("y", TInt)))))),
         RVal
           (AggSum (RVal (Const (Int 1)),
               RA_Leaf(AtomicConstraint(Eq, RVal(Var ("z", TInt)), RVal(Var ("x", TInt))))))
)))))) [("x", TInt); ("y", TInt)] [])
([], make_term( RVal (AggSum (RVal (Const (Int 1)),
  RA_Leaf (AtomicConstraint (Eq, RVal (Const (Int 1)), RVal (Const (Int 1))))))
))
;;





