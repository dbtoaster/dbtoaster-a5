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

let vwap_delta_1 = 
  (* (if REWRITE__2[B1_PRICE]<REWRITE__3[] 
      then (if B1_PRICE=QBIDS_PRICE and B1_VOLUME=QBIDS_VOLUME 
            then (B1_PRICE*B1_VOLUME) 
            else 0) 
      else 0) 
  *)
  Calculus.make_term (
    Calculus.RVal(Calculus.AggSum(
      Calculus.RVal(Calculus.AggSum(
        Calculus.RProd[
          Calculus.RVal(Calculus.Var("B1_PRICE",TInt));
          Calculus.RVal(Calculus.Var("B1_VOLUME",TDouble))
        ],
        Calculus.RA_MultiNatJoin[
          Calculus.RA_Leaf(Calculus.AtomicConstraint(Calculus.Eq,
            Calculus.RVal(Calculus.Var("B1_PRICE",Calculus.TInt)),
            Calculus.RVal(Calculus.Var("QBIDS_PRICE",Calculus.TInt))
          ));
          Calculus.RA_Leaf(Calculus.AtomicConstraint(Calculus.Eq,
            Calculus.RVal(Calculus.Var("B1_VOLUME",Calculus.TDouble)),
            Calculus.RVal(Calculus.Var("QBIDS_VOLUME",Calculus.TDouble))
          ))
        ]
      )),
      Calculus.RA_Leaf(Calculus.AtomicConstraint(Calculus.Lt,
        Calculus.RVal(Calculus.External(
          "REWRITE__2", ["B1_PRICE",Calculus.TInt])),
        Calculus.RVal(Calculus.External("REWRITE__3", []))
      ))
    ))
  );;

(****************************************************************************)
(* Tracking down the bigsum factorization glitch *)

Debug.log_unit_test "make_term Sanity Check" (fun x->x)
  (Calculus.term_as_string vwap_delta_1)
  "(if REWRITE__2[B1_PRICE]<REWRITE__3[] then (if B1_PRICE=QBIDS_PRICE and B1_VOLUME=QBIDS_VOLUME then (B1_PRICE*B1_VOLUME) else 0) else 0)";;


let vwap_delta_1_t, vwap_delta_1_r =
  match Calculus.readable_term vwap_delta_1 with
    | Calculus.RVal(Calculus.AggSum(t,r)) ->
      ((Calculus.make_term t),(Calculus.make_relcalc r))
    | _ -> failwith "BUG!";;

Debug.log_unit_test "term_vars Sanity Check" (list_to_string fst)
  (Calculus.term_vars vwap_delta_1_t)
  ["B1_PRICE",TInt; 
   "B1_VOLUME",TDouble; 
   "QBIDS_PRICE",TInt; 
   "QBIDS_VOLUME",TDouble];;

Debug.log_unit_test "relcalc_vars Sanity Check" (list_to_string fst)
  (Calculus.relcalc_vars vwap_delta_1_r)
  ["B1_PRICE",TInt];;

let vwap_delta_1_factors = 
  MixedHyperGraph.connected_components
    Calculus.term_vars Calculus.relcalc_vars
    (Util.MixedHyperGraph.make
      [vwap_delta_1_t]
      [vwap_delta_1_r]
    );;

Debug.log_unit_test "Hypergraph Factorization" 
  (string_of_list0 "\n---\n" (fun (t,r) -> 
    (string_of_list0 "\n" Calculus.term_as_string t)^" && "^
    (string_of_list0 "\n" Calculus.relcalc_as_string r)))
  (List.map MixedHyperGraph.extract_atoms vwap_delta_1_factors)
  [([vwap_delta_1_t],[vwap_delta_1_r])]


let vwap_delta_1_factorized =
  Calculus.factorize_aggsum_mm vwap_delta_1_t vwap_delta_1_r;;

Debug.log_unit_test "Aggsum Factorization" Calculus.term_as_string
  vwap_delta_1_factorized
  vwap_delta_1

(****************************************************************************)
(* Simplify correctness on bigsum vars *)

let (vwap_delta_1_inner_subs,vwap_delta_1_inner_simplified) =
  (Calculus.simplify_roly true vwap_delta_1_t
                          ["QBIDS_PRICE",Calculus.TInt;
                           "QBIDS_VOLUME",Calculus.TDouble]
                          ["B1_PRICE",Calculus.TInt;
                           "B1_VOLUME",Calculus.TDouble]);;

Debug.log_unit_test "simplify_roly Simplified Term" Calculus.term_as_string
  vwap_delta_1_inner_simplified
  (Calculus.make_term
    (Calculus.RProd[
      Calculus.RVal(Calculus.Var("QBIDS_PRICE",Calculus.TInt));
      Calculus.RVal(Calculus.Var("QBIDS_VOLUME",Calculus.TDouble))
    ])
  );;

Debug.log_unit_test "simplify_roly Term Substitution" 
  (list_to_string (fun ((x,_),(y,_))->x^"->"^y))
  vwap_delta_1_inner_subs
  [("B1_PRICE",Calculus.TInt),("QBIDS_PRICE",Calculus.TInt);
   ("QBIDS_PRICE",Calculus.TInt),("QBIDS_PRICE",Calculus.TInt);
   ("B1_VOLUME",Calculus.TDouble),("QBIDS_VOLUME",Calculus.TDouble);
   ("QBIDS_VOLUME",Calculus.TDouble),("QBIDS_VOLUME",Calculus.TDouble)]

(****************************************************************************)
(* Make sure that the compiler is handling input variables properly *)

let (bigsum_vars, bsrw_theta, bsrw_term) = 
  bigsum_rewriting ModeOpenDomain (roly_poly vwap) [] ("REWRITE__");;

Debug.log_unit_test "Bigsum Rewriting" (fun x->x)
  (term_as_string bsrw_term)
  ("(if REWRITE__2[B1_PRICE]<REWRITE__3[] then REWRITE__1[B1_PRICE] else 0)")
;;

let (deltas,todos) = 
  compile_delta_for_rel 
    (* reln = *)              "BIDS"
    (* relsch = *)            ["QBIDS_PRICE",TInt;"QBIDS_VOLUME",TDouble]
    (* delete = *)            false
    (* map_term = *)          (map_term "Q" [])
    (* bigsum_vars = *)       bigsum_vars
    (* externals_mapping = *) bsrw_theta
    (* term = *)              bsrw_term;;

Debug.log_unit_test "Unified Compilation" (string_of_list "\n")
  (List.map (fun (pm,rel,invars,params,rhs) -> term_as_string rhs) deltas)
  ([
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] then QBIDS_QBIDS_PRICE else 0)*QBIDS_QBIDS_VOLUME)";
    "(if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) and REWRITE__3[]<=REWRITE__2[B1_PRICE] then REWRITE__1[B1_PRICE] else 0)";
    "((if REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then REWRITE__1[B1_PRICE] else 0)*-1)";
    "(if (REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+QBIDS_QBIDS_VOLUME) and REWRITE__3[]<=REWRITE__2[QBIDS_QBIDS_PRICE] then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)";
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] and (REWRITE__3[]+QBIDS_QBIDS_VOLUME)<=(REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1)"
  ])
;;

