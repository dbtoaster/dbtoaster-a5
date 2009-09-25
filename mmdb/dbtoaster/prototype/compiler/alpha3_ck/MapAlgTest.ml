(* To run these tests, compile without MapAlg.mli *)

open RelAlg;;
open MapAlg;;
open MA_BASE;;
open MASemiRing;;


let relR = RA_Leaf(Rel("R", ["A"; "B"]));;
let relS = RA_Leaf(Rel("S", ["B"; "C"]));;
let relT = RA_Leaf(Rel("T", ["C"; "D"]));;

let relR2 = RelAlg.make relR;;


(* (R bowtie S) bowtie T *)
let q = RA_MultiNatJoin [relR; relS; relT];;

let q1 = MapAlg.make(RVal (RAggSum(RVal(RConst 1), relR)));;
let q2 = MapAlg.delta "R" ["x"; "y"] q1;;


let test01 = readable(one)  = RVal (RConst 1);;
let test02 = readable(zero) = RVal (RConst 0);;
let test03 = mk_sum[] = zero;;
let test04 = mk_sum[mk_sum[]] = zero;;
let test05 = mk_prod[] = one;;
let test06 = mk_sum [one;zero] = one;;
let test07 = mk_prod[one;zero] = zero;;
let test08 = mk_sum[zero;one;zero] = one;;
let test09 = mk_prod[one;zero;one;one] = zero;;

let test10 = MapAlg.delta "R" ["x"; "y"] one = zero;;

MapAlg.roly_poly(
   Val (AggSum(mk_sum[Val (AggSum(one, relR2));
                      Val (AggSum(one, relR2))],
               RelAlg.make(RA_MultiUnion [relR; relS; relT]))))
=
make(
RSum
 [RVal
   (RAggSum
     (RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("R", ["A"; "B"]))));
  RVal
   (RAggSum
     (RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("R", ["A"; "B"]))));
  RVal
   (RAggSum
     (RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("S", ["B"; "C"]))));
  RVal
   (RAggSum
     (RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("R", ["A"; "B"])))),
     RA_Leaf (Rel ("S", ["B"; "C"]))));
  RProd
   [RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("R", ["A"; "B"]))));
    RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("T", ["C"; "D"]))))];
  RProd
   [RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("R", ["A"; "B"]))));
    RVal (RAggSum (RVal (RConst 1), RA_Leaf (Rel ("T", ["C"; "D"]))))]]
);;


let test11 = MapAlg.simplify(Val (AggSum(one, RelAlg.one))) [] [] =
   [([], one)];;
let test12 = MapAlg.simplify(Val (AggSum(zero, RelAlg.one))) [] [] =
   [([], zero)];;
let test13 = MapAlg.simplify(Val (AggSum(zero, RelAlg.make(relR)))) [] [] =
   [([], zero)];;

let test16 = q2 =
  mk_sum
   [Val (AggSum (zero, RelAlg.make(relR)));
    Val (AggSum (one,
     RelAlg.make(RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, "A", "x"));
       RA_Leaf (AtomicConstraint (Eq, "B", "y"))])));
    Val (AggSum (zero,
     RelAlg.make(RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, "A", "x"));
       RA_Leaf (AtomicConstraint (Eq, "B", "y"))])))]
;;

let test17 = MapAlg.simplify q2 [] [] = [([], one)];;

let test19 = MapAlg.simplify
    (MapAlg.delta "R" ["x"; "y"] (Val (AggSum(one, RelAlg.make(relS))))) [] []
= [([], zero)];;


let test20 =
polynomial(
   Prod[Sum[Val(Const(1)); Prod[Sum[]]];
        Prod[Sum[Val(Const(2)); Sum[Sum[]]]];
        Prod[Sum[Val(Const(3)); Val(Const(4))];
             Prod[Prod[Sum[Val(Const(5)); Prod[Prod[]]]]]]]
) =
Sum
 [Prod [Val (Const 2); Val (Const 3); Val (Const 5)];
  Prod [Val (Const 2); Val (Const 4); Val (Const 5)];
  Prod [Val (Const 2); Val (Const 3)]; Prod [Val (Const 2); Val (Const 4)]]
;;

let test21 =
MapAlg.simplify(Val(AggSum(Sum[Val(Var("A")); Val(Var("C"))],
  RelAlg.make(RA_MultiNatJoin [relR; relT])))) [] [] =
[([], Sum
 [Prod
   [Val (AggSum (Val (Var "A"), RelAlg.make(relR)));
    Val (AggSum (Val (Const 1), RelAlg.make(relT)))];
  Prod
   [Val (AggSum (Val (Var "C"), RelAlg.make(relT)));
    Val (AggSum (Val (Const 1), RelAlg.make(relR)))]]
)];;

let test22 =
MapAlg.simplify (Val(AggSum(Prod[Val(Var("A")); Val(Var("C")); Val(Var("D"))],
   RelAlg.make(RA_MultiNatJoin[RA_Leaf(Rel("R", ["A"; "B"; "D"]));
                   RA_Leaf(Rel("S", ["C"; "E"]))])))) [] []
=
[([], Prod
 [Val (AggSum (Prod [Val (Var "A"); Val (Var "D")],
              RelAlg.make(RA_Leaf (Rel ("R", ["A"; "B"; "D"])))));
  Val (AggSum (Val (Var "C"),
              RelAlg.make(RA_Leaf (Rel ("S", ["C"; "E"])))))]
)];;



let q3 =
Val (AggSum (Prod [Val (Var "A"); Val (Var "C")],
RelAlg.make(
RA_MultiNatJoin
    [RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, "A", "x"));
       RA_Leaf (AtomicConstraint (Eq, "B", "y"))];
     RA_Leaf (Rel ("S", ["B"; "C"]))]
))
);;


let test23 =
MapAlg.simplify q3 ["x"; "y"] [] =
[([], Prod
 [Val (Var "x");
  Val (AggSum (Val (Var "C"),
    RelAlg.make(RA_Leaf (Rel ("S", ["y"; "C"])))))]
)];;

let q4 = 
Val (AggSum (Prod [Val (Var "B"); Val (Var "C")],
RelAlg.make(
RA_MultiNatJoin
      [RA_Leaf (AtomicConstraint (Eq, "B", "x"));
       RA_Leaf (AtomicConstraint (Eq, "B", "y"));
       RA_Leaf (AtomicConstraint (Eq, "C", "z"))]
)));;


let test24 = MapAlg.simplify q4 [] [] =
   [([], Prod [Val (Var "B"); Val (Var "C")])];;
let test25 = MapAlg.simplify q4 ["y"] [] =
   [([], Prod [Val (Var "y"); Val (Var "C")])];;
let test26 = MapAlg.simplify q4 ["w"] [] =
   [([], Prod [Val (Var "B"); Val (Var "C")])];;





