(* Unit tests for Calculus -> M3 Compilation *)

open Util
open Calculus
open CalcToM3
open Compiler
;;

(****************************************************************************)
(* Query Definitions *)

(* Select Sum(A * D) From R(A,B) Join S(B,C) Join T(C,D) *)
let rst = make_term (
  RVal(AggSum(
    RProd[RVal(Var("a",TInt));RVal(Var("d",TInt))],
    RA_MultiNatJoin[
      RA_Leaf(Rel("r", [  "a",TInt;"r_b",TInt]));
      RA_Leaf(Rel("s", ["s_b",TInt;"s_c",TInt]));
      RA_Leaf(Rel("t", ["t_c",TInt;"d"  ,TInt]));
      RA_Leaf(AtomicConstraint(Eq,RVal(Var("r_b",TInt)),RVal(Var("s_b",TInt))));
      RA_Leaf(AtomicConstraint(Eq,RVal(Var("s_c",TInt)),RVal(Var("t_c",TInt))))
    ]
  ))
);;

(* VWAP *)
let make_bids id = 
  let (p,v) = ("B"^id^"_PRICE", TInt),
              ("B"^id^"_VOLUME", TDouble) in
  (RA_Leaf(Rel("BIDS", [p;v])),RVal(Var(p)),RVal(Var(v)));;

let (b1,b1p,b1v) = make_bids "1"
let (b2,b2p,b2v) = make_bids "2"
let (b3,b3p,b3v) = make_bids "3"

let vwap = make_term (RVal(AggSum(
    RProd[b1p;b1v],
    RA_MultiNatJoin[
      b1;
      RA_Leaf(AtomicConstraint(Lt,
        RVal(AggSum(b2v, (RA_MultiNatJoin[
          b2;
          RA_Leaf(AtomicConstraint(Lt,
            b1p,b2p
          ))
        ]))),
        RVal(AggSum(b3v, b3))
      ))
    ]
  )));;

(****************************************************************************)
(* Make sure that the compiler is handling input variables properly *)

let (bigsum_vars, bsrw_theta, bsrw_term) = 
  bigsum_rewriting ModeOpenDomain (roly_poly vwap) [] ("REWRITE__")

let (deltas,todos) = 
  compile_delta_for_rel 
    (* reln = *)              "BIDS"
    (* relsch = *)            ["QBIDS_PRICE",TInt;"QBIDS_VOLUME",TDouble]
    (* delete = *)            false
    (* map_term = *)          (map_term "Q" [])
    (* external_bound_vars *) bigsum_vars
    (* externals_mapping *)   bsrw_theta
    (* term = *)              bsrw_term;;

Debug.log_unit_test "Bigsum Rewriting" (fun x->x)
  (term_as_string bsrw_term)
  ("(if REWRITE__2[B1_PRICE]<REWRITE__3[] then REWRITE__1[B1_PRICE] else 0)")
;;

Debug.log_unit_test "VWAP Compilation" (string_of_list "\n")
  (List.map (fun (pm,rel,invars,params,rhs) -> term_as_string rhs) deltas)
  ([
    "((if B1_PRICE=QBIDS_QBIDS_PRICE then B1_PRICE else 0)*QBIDS_QBIDS_VOLUME*(if REWRITE__2[B1_PRICE]<REWRITE__3[] then 1 else 0))";
    "(REWRITE__1[B1_PRICE]*(if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) then 1 else 0)*(if REWRITE__3[]<=REWRITE__2[B1_PRICE] then 1 else 0))";
    "(REWRITE__1[B1_PRICE]*-1*(if REWRITE__2[B1_PRICE]<REWRITE__3[] then 1 else 0)*(if (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then 1 else 0))";
    "((if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) then ((if B1_PRICE=QBIDS_QBIDS_PRICE then B1_PRICE else 0)*QBIDS_QBIDS_VOLUME) else 0)*(if REWRITE__3[]<=REWRITE__2[B1_PRICE] then 1 else 0))";
    "((if (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then ((if B1_PRICE=QBIDS_QBIDS_PRICE then B1_PRICE else 0)*QBIDS_QBIDS_VOLUME) else 0)*-1*(if REWRITE__2[B1_PRICE]<REWRITE__3[] then 1 else 0))"
  ])
;;