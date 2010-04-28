(* Unit tests for Compiler / Compilation components of Calculus  *)

open Util
open Calculus
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

let vwap_delta_1 = 
  (* (if REWRITE__2[B1_PRICE]<REWRITE__3[] 
      then (if B1_PRICE=QBIDS_PRICE and B1_VOLUME=QBIDS_VOLUME 
            then (B1_PRICE*B1_VOLUME) 
            else 0) 
      else 0) 
  *)
  make_term (
    RVal(AggSum(
      RVal(AggSum(
        RProd[
          RVal(Var("B1_PRICE",TInt));
          RVal(Var("B1_VOLUME",TDouble))
        ],
        RA_MultiNatJoin[
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("B1_PRICE",TInt)),
            RVal(Var("QBIDS_PRICE",TInt))
          ));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("B1_VOLUME",TDouble)),
            RVal(Var("QBIDS_VOLUME",TDouble))
          ))
        ]
      )),
      RA_Leaf(AtomicConstraint(Lt,
        RVal(External(
          "REWRITE__2", ["B1_PRICE",TInt])),
        RVal(External("REWRITE__3", []))
      ))
    ))
  );;

(****************************************************************************)
(* Tracking down the bigsum factorization glitch *)

Debug.log_unit_test "make_term Sanity Check" (fun x->x)
  (term_as_string vwap_delta_1)
  "(if REWRITE__2[B1_PRICE]<REWRITE__3[] then (if B1_PRICE=QBIDS_PRICE and B1_VOLUME=QBIDS_VOLUME then (B1_PRICE*B1_VOLUME) else 0) else 0)";;


let vwap_delta_1_t, vwap_delta_1_r =
  match readable_term vwap_delta_1 with
    | RVal(AggSum(t,r)) ->
      ((make_term t),(make_relcalc r))
    | _ -> failwith "BUG!";;

Debug.log_unit_test "term_vars Sanity Check" (list_to_string fst)
  (term_vars vwap_delta_1_t)
  ["B1_PRICE",TInt; 
   "B1_VOLUME",TDouble; 
   "QBIDS_PRICE",TInt; 
   "QBIDS_VOLUME",TDouble];;

Debug.log_unit_test "relcalc_vars Sanity Check" (list_to_string fst)
  (relcalc_vars vwap_delta_1_r)
  ["B1_PRICE",TInt];;

let vwap_delta_1_factors = 
  MixedHyperGraph.connected_components
    term_vars relcalc_vars
    (Util.MixedHyperGraph.make
      [vwap_delta_1_t]
      [vwap_delta_1_r]
    );;

Debug.log_unit_test "Hypergraph Factorization" 
  (string_of_list0 "\n---\n" (fun (t,r) -> 
    (string_of_list0 "\n" term_as_string t)^" && "^
    (string_of_list0 "\n" relcalc_as_string r)))
  (List.map MixedHyperGraph.extract_atoms vwap_delta_1_factors)
  [([vwap_delta_1_t],[vwap_delta_1_r])]


let vwap_delta_1_factorized =
  factorize_aggsum_mm vwap_delta_1_t vwap_delta_1_r;;

Debug.log_unit_test "Aggsum Factorization" term_as_string
  vwap_delta_1_factorized
  vwap_delta_1

(****************************************************************************)
(* Simplify correctness on bigsum vars *)

let (vwap_delta_1_inner_subs,vwap_delta_1_inner_simplified) =
  (simplify_roly true vwap_delta_1_t
                          ["QBIDS_PRICE",TInt;
                           "QBIDS_VOLUME",TDouble]
                          ["B1_PRICE",TInt;
                           "B1_VOLUME",TDouble]);;

Debug.log_unit_test "Bigsum Simplification" term_as_string
  vwap_delta_1_inner_simplified
  (make_term
    (RProd[
      RVal(Var("QBIDS_PRICE",TInt));
      RVal(Var("QBIDS_VOLUME",TDouble))
    ])
  );;

Debug.log_unit_test "Bigsum Term Substitutions" 
  (list_to_string (fun ((x,_),(y,_))->x^"->"^y))
  vwap_delta_1_inner_subs
  [("B1_PRICE",TInt),("QBIDS_PRICE",TInt);
   ("QBIDS_PRICE",TInt),("QBIDS_PRICE",TInt);
   ("B1_VOLUME",TDouble),("QBIDS_VOLUME",TDouble);
   ("QBIDS_VOLUME",TDouble),("QBIDS_VOLUME",TDouble)]

(****************************************************************************)
(* Make sure that the compiler is handling input variables properly *)

let (bigsum_vars, bsrw_theta, bsrw_term) = 
  bigsum_rewriting ModeOpenDomain (roly_poly vwap) [] ("REWRITE__");;

Debug.log_unit_test "Bigsum Rewriting" (fun x->x)
  (term_as_string bsrw_term)
  ("(if REWRITE__2[B1_PRICE]<REWRITE__3[] then REWRITE__1[B1_PRICE] else 0)")
;;


(*Debug.log_unit_test "Delta Computation"
  term_as_string
  (term_delta bsrw_theta false "BIDS" 
                       ["QBIDS_PRICE",TInt;"QBIDS_VOLUME",TDouble]
                       bsrw_term)
  term_zero*)

let (deltas,todos) = 
  compile_delta_for_rel 
    (* reln = *)              "BIDS"
    (* relsch = *)            ["QBIDS_PRICE",TInt;"QBIDS_VOLUME",TDouble]
    (* delete = *)            false
    (* map_term = *)          (map_term "Q" [])
    (* bigsum_vars = *)       bigsum_vars
    (* externals_mapping = *) bsrw_theta
    (* term = *)              bsrw_term;;

Debug.log_unit_test "Bigsum Compilation" (string_of_list "\n")
  (List.map (fun (pm,rel,invars,params,rhs) -> term_as_string rhs) deltas)
  ([
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] then QBIDS_QBIDS_PRICE else 0)*QBIDS_QBIDS_VOLUME)";
    "(if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) and REWRITE__3[]<=REWRITE__2[B1_PRICE] then REWRITE__1[B1_PRICE] else 0)";
    "((if REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then REWRITE__1[B1_PRICE] else 0)*-1)";
    "(if (REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) and REWRITE__3[]<=REWRITE__2[QBIDS_QBIDS_PRICE] then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)";
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] and (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1)"
  ])
;;


let (neg_deltas,neg_todos) = 
  compile_delta_for_rel 
    (* reln = *)              "BIDS"
    (* relsch = *)            ["QBIDS_PRICE",TInt;"QBIDS_VOLUME",TDouble]
    (* delete = *)            true
    (* map_term = *)          (map_term "Q" [])
    (* bigsum_vars = *)       bigsum_vars
    (* externals_mapping = *) bsrw_theta
    (* term = *)              bsrw_term;;

Debug.log_unit_test "Bigsum Delete Compilation" (string_of_list "\n")
  (List.map (fun (pm,rel,invars,params,rhs) -> term_as_string rhs) deltas)
  ([
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] then QBIDS_QBIDS_PRICE else 0)*QBIDS_QBIDS_VOLUME)";
    "(if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) and REWRITE__3[]<=REWRITE__2[B1_PRICE] then REWRITE__1[B1_PRICE] else 0)";
    "((if REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then REWRITE__1[B1_PRICE] else 0)*-1)";
    "(if (REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) and REWRITE__3[]<=REWRITE__2[QBIDS_QBIDS_PRICE] then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)";
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] and (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1)"
  ])
;;

