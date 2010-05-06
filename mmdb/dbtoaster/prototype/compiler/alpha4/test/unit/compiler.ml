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


(****************************************************************************)
(* Handling deletions *)

let delete_delta = 
  term_delta bsrw_theta true "BIDS" 
             ["QBIDS_PRICE",TInt;"QBIDS_VOLUME",TDouble]
             bsrw_term;;

Debug.log_unit_test "Bigsum Delete Delta" (fun x -> x)
  (term_as_string delete_delta)
  "((if REWRITE__2[B1_PRICE]<REWRITE__3[] then (if -(B1_PRICE=QBIDS_PRICE and B1_VOLUME=QBIDS_VOLUME) then (B1_PRICE*B1_VOLUME) else 0) else 0)+(if ((REWRITE__2[B1_PRICE]+(if -(B2_PRICE=QBIDS_PRICE and B2_VOLUME=QBIDS_VOLUME) and B1_PRICE<B2_PRICE then B2_VOLUME else 0))<(REWRITE__3[]+(if -(B3_PRICE=QBIDS_PRICE and B3_VOLUME=QBIDS_VOLUME) then B3_VOLUME else 0)) and REWRITE__3[]<=REWRITE__2[B1_PRICE] or -(REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+(if -(B3_PRICE=QBIDS_PRICE and B3_VOLUME=QBIDS_VOLUME) then B3_VOLUME else 0))<=(REWRITE__2[B1_PRICE]+(if -(B2_PRICE=QBIDS_PRICE and B2_VOLUME=QBIDS_VOLUME) and B1_PRICE<B2_PRICE then B2_VOLUME else 0)))) then REWRITE__1[B1_PRICE] else 0)+(if ((REWRITE__2[B1_PRICE]+(if -(B2_PRICE=QBIDS_PRICE and B2_VOLUME=QBIDS_VOLUME) and B1_PRICE<B2_PRICE then B2_VOLUME else 0))<(REWRITE__3[]+(if -(B3_PRICE=QBIDS_PRICE and B3_VOLUME=QBIDS_VOLUME) then B3_VOLUME else 0)) and REWRITE__3[]<=REWRITE__2[B1_PRICE] or -(REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+(if -(B3_PRICE=QBIDS_PRICE and B3_VOLUME=QBIDS_VOLUME) then B3_VOLUME else 0))<=(REWRITE__2[B1_PRICE]+(if -(B2_PRICE=QBIDS_PRICE and B2_VOLUME=QBIDS_VOLUME) and B1_PRICE<B2_PRICE then B2_VOLUME else 0)))) then (if -(B1_PRICE=QBIDS_PRICE and B1_VOLUME=QBIDS_VOLUME) then (B1_PRICE*B1_VOLUME) else 0) else 0))";;

let vwap_delta_1_del = 
  (*(if REWRITE__2[B1_PRICE]<REWRITE__3[] 
     then (if -(B1_PRICE=QBIDS_PRICE) and -(B1_VOLUME=QBIDS_VOLUME) 
           then (B1_PRICE*B1_VOLUME) else 0) else 0) *)
  make_term (
    RVal(AggSum(
      RVal(AggSum(
        RProd[
          RVal(Var("B1_PRICE",TInt));
          RVal(Var("B1_VOLUME",TDouble))
        ],
        RA_Neg(RA_MultiNatJoin[
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("B1_PRICE",TInt)),
            RVal(Var("QBIDS_PRICE",TInt))
          ));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("B1_VOLUME",TDouble)),
            RVal(Var("QBIDS_VOLUME",TDouble))
          ))
        ])
      )),
      RA_Leaf(AtomicConstraint(Lt,
        RVal(External("REWRITE__2",["B1_PRICE",TInt])),
        RVal(External("REWRITE__3",[]))
      ))
    ))
  );;

Debug.log_unit_test "Bigsum Roly Delete(small)" term_as_string
  (roly_poly vwap_delta_1_del)
  (make_term (
    RProd[
      RVal(AggSum(
        RVal(AggSum(
          RVal(Var("B1_PRICE",TInt)),
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("B1_PRICE",TInt)),
            RVal(Var("QBIDS_PRICE",TInt))
          ))
        )),
        RA_Leaf(AtomicConstraint(Lt,
          RVal(External("REWRITE__2",["B1_PRICE",TInt])),
          RVal(External("REWRITE__3",[]))
        ))
      ));
      RVal(AggSum(
        RVal(Var("B1_VOLUME",TDouble)),
        RA_Leaf(AtomicConstraint(Eq,
          RVal(Var("B1_VOLUME",TDouble)),
          RVal(Var("QBIDS_VOLUME",TDouble))
        ))
      ));
      RVal(Const(Int(-1)))
    ]
  ));;
  
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
  (List.map (fun (pm,rel,invars,params,rhs) -> term_as_string rhs) neg_deltas)
  ([
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] then QBIDS_QBIDS_PRICE else 0)*QBIDS_QBIDS_VOLUME*-1)";
    "(if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)*-1))<(REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1)) and REWRITE__3[]<=REWRITE__2[B1_PRICE] then REWRITE__1[B1_PRICE] else 0)";
    "((if REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1))<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)*-1)) then REWRITE__1[B1_PRICE] else 0)*-1)";
    "((if (REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)*-1))<(REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1)) and REWRITE__3[]<=REWRITE__2[QBIDS_QBIDS_PRICE] then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1)";
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] and (REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1))<=(REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)*-1)) then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1*-1)"
  ])
  (*
  ([
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] then QBIDS_QBIDS_PRICE else 0)*QBIDS_QBIDS_VOLUME*-1)";
    "(if (REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*-1*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1)) and REWRITE__3[]<=REWRITE__2[B1_PRICE] then REWRITE__1[B1_PRICE] else 0)";
    "((if REWRITE__2[B1_PRICE]<REWRITE__3[] and (REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1))<=(REWRITE__2[B1_PRICE]+(QBIDS_QBIDS_VOLUME*-1*(if B1_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then REWRITE__1[B1_PRICE] else 0)*-1)";
    "((if (REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*-1*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0)))<(REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1)) and REWRITE__3[]<=REWRITE__2[QBIDS_QBIDS_PRICE] then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1)";
    "((if REWRITE__2[QBIDS_QBIDS_PRICE]<REWRITE__3[] and (REWRITE__3[]+(QBIDS_QBIDS_VOLUME*-1))<=(REWRITE__2[QBIDS_QBIDS_PRICE]+(QBIDS_QBIDS_VOLUME*-1*(if QBIDS_QBIDS_PRICE<QBIDS_QBIDS_PRICE then 1 else 0))) then (QBIDS_QBIDS_PRICE*QBIDS_QBIDS_VOLUME) else 0)*-1*-1)"
  ])*)
;;

(****************************************************************************)
(* Tracking down wierdness in RST generation *)

let (rst_bigsum_vars, rst_bsrw_theta, rst_bsrw_term) = 
  bigsum_rewriting ModeOpenDomain (roly_poly rst) [] ("REWRITE__");;

Debug.log_unit_test "Unnecessary Bigsum Rewriting" term_as_string
  rst_bsrw_term
  (roly_poly rst)
;;
Debug.log_unit_test "Unnecessary Bigsum Vars" (list_to_string fst)
  rst_bigsum_vars
  []
;;

let rst_true_delta_r = 
  (make_term (
    RVal(AggSum(
      RProd[RVal(Var("a",TInt));RVal(Var("d",TInt))],
      RA_MultiNatJoin[
        RA_Neg(RA_MultiNatJoin[
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("a",TInt)),RVal(Var("Qr_a",TInt))));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("r_b",TInt)),RVal(Var("Qr_r_b",TInt))));
        ]);
        RA_Leaf(AtomicConstraint(Eq,
          RVal(Var("r_b",TInt)),RVal(Var("s_b",TInt))));
        RA_Leaf(Rel("s",["s_b",TInt;"s_c",TInt]));
        RA_Leaf(AtomicConstraint(Eq,
          RVal(Var("s_c",TInt)),RVal(Var("t_c",TInt))));
        RA_Leaf(Rel("t",["t_c",TInt;"d",TInt]))
      ]
    ))
  ));;

Debug.log_unit_test "RST Delete R term_delta" term_as_string
  (term_delta rst_bsrw_theta true "r" ["Qr_a",TInt;"Qr_r_b",TInt] rst_bsrw_term)
  rst_true_delta_r
;;
  
let rst_true_delta_r_simplified =
  (simplify rst_true_delta_r 
            ["Qr_a",TInt;"Qr_r_b",TInt] [] 
            ["a",TInt;"r_b",TInt]);;

Debug.log_unit_test "Simplify RST Delete Delta" 
  (string_of_list0 "\n" term_as_string)
  (List.map (fun ((_,x),_) ->  x) rst_true_delta_r_simplified)
  [make_term (
    RProd[
      RVal(Var("Qr_a",TInt));
      RVal(AggSum(
        RVal(Var("d",TInt)),
        RA_MultiNatJoin[
          RA_Leaf(Rel("t",["s_c",TInt;"d",TInt]));
          RA_Leaf(Rel("s",["Qr_r_b",TInt;"s_c",TInt]))
        ]
      ));
      RVal(Const(Int(-1)))
    ]
  )];;

Debug.log_unit_test "RST Extract Aggregates" 
  (string_of_list0 "\n" term_as_string)
  (List.map fst 
    (snd (extract_named_aggregates 
          "FOO_"
          ["Qr_a",TInt;"Qr_r_b",TInt]
          (List.map fst rst_true_delta_r_simplified))))
  [make_term (
    RVal(AggSum(RVal(Var("d",TInt)),
                RA_MultiNatJoin[RA_Leaf(Rel("t",["s_c",TInt;"d",TInt]));
                                RA_Leaf(Rel("s",["Qr_r_b",TInt;"s_c",TInt]))
                               ])))];;
    

let (rst_deltas,rst_todos) = 
  compile_delta_for_rel 
    (* reln = *)              "r"
    (* relsch = *)            ["a",TInt;"r_b",TInt]
    (* delete = *)            true
    (* map_term = *)          (map_term "Q" [])
    (* bigsum_vars = *)       rst_bigsum_vars
    (* externals_mapping = *) rst_bsrw_theta
    (* term = *)              rst_bsrw_term;;

Debug.log_unit_test "RST Todos" (string_of_list0 "\n" (fun (defn, term) ->
    (term_as_string term)^" := "^(term_as_string defn)))
  rst_todos
  [(
    make_term (RVal(AggSum(RVal(Var("d",TInt)), RA_MultiNatJoin[
      RA_Leaf(Rel("t",["s_c",TInt;"d",TInt]));
      RA_Leaf(Rel("s",["Qr_r_b",TInt;"s_c",TInt]))
    ]))),
    map_term "Qr1" ["Qr_r_b",TInt]
  )];;

Debug.log_unit_test "RST Deltas" (string_of_list0 "\n" term_as_string)
  (List.map (fun (_,_,_,_,x) -> x) rst_deltas)
  [make_term (
    RProd[RVal(Var("Qr_a",TInt)); RVal(External("Qr1",["Qr_r_b",TInt]));
          RVal(Const(Int(-1)))]
  )]
;;

(****************************************************************************)
(* Compiling Ladder *)

let ladder_1 = 
  make_term (
    RVal(AggSum(
      RVal(Calculus.Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R", ["T1__A",TInt; "T1__B",TInt]));
        RA_Leaf(Rel("R", ["V0__A",TInt; "V0__B",TInt]));
        RA_Leaf(Rel("R", ["V1__A",TInt; "V1__B",TInt]));
        RA_Leaf(Rel("R", ["B1__A",TInt; "B1__B",TInt]));
        RA_Leaf(AtomicConstraint(Calculus.Eq, 
                                 RVal(Calculus.Var("T1__A",TInt)),
                                 RVal(Calculus.Var("V0__A",TInt))));
        RA_Leaf(AtomicConstraint(Calculus.Eq, 
                                 RVal(Calculus.Var("B1__A",TInt)),
                                 RVal(Calculus.Var("V0__B",TInt))));
        RA_Leaf(AtomicConstraint(Calculus.Eq, 
                                 RVal(Calculus.Var("T1__B",TInt)),
                                 RVal(Calculus.Var("V1__A",TInt))));
        RA_Leaf(AtomicConstraint(Calculus.Eq, 
                                 RVal(Calculus.Var("B1__B",TInt)),
                                 RVal(Calculus.Var("V1__B",TInt))))
      ]
    ))
  );;

let string_of_term_mapping ((d,t):(term_t*term_t)) = 
  (term_as_string t)^" := "^(term_as_string d) 

let (ladder_1_compiled, ladder_1_todos) =
  compile_delta_for_rel 
      "R" 
      ["A",TInt;"B",TInt]
      false
      (map_term "Q" [])
      []
      []
      ladder_1;;

Debug.log_unit_test_list "Quad-Self-Join Stage 1 Term" term_as_string
  (List.map (fun (_,_,_,_,x) -> x) ladder_1_compiled)
  [
    make_term (RVal(External("QR1",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR2",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR3",["QR_B",TInt])));
    make_term (RVal(External("QR4",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR5",["QR_A",TInt;"QR_B",TInt])));
    make_term (RProd[RVal(External("QR6",["QR_A",TInt]));
                     RVal(External("QR7",["QR_B",TInt]))]);
    make_term (RVal(External("QR8",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR9",["QR_A",TInt;"QR_B",TInt])));
    make_term (RProd[RVal(External("QR6",["QR_A",TInt]));
                     RVal(External("QR7",["QR_B",TInt]))]);
    make_term (RVal(External("QR10",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR8",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR11",["QR_A",TInt])));
    make_term (RVal(External("QR8",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(External("QR8",["QR_A",TInt;"QR_B",TInt])));
    make_term (RVal(AggSum(RVal(Const(Int(1))),
                           RA_Leaf(AtomicConstraint(Eq,
                             RVal(Var("QR_A",TInt)),
                             RVal(Var("QR_B",TInt)))))))
  ];;

Debug.log_unit_test_list "Quad-Self-Join Stage 1 Todos" string_of_term_mapping 
  ladder_1_todos
  [
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_A", TInt;"B1__A",TInt]));
        RA_Leaf(Rel("R",["B1__A",TInt;"B1__B",TInt]));
        RA_Leaf(Rel("R",["QR_B", TInt;"B1__B",TInt]))
      ]))),
    map_term "QR1"  ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_A", TInt;"T1__B",TInt]));
        RA_Leaf(Rel("R",["T1__B",TInt;"B1__B",TInt]));
        RA_Leaf(Rel("R",["QR_B", TInt;"B1__B",TInt]))
      ]))),
    map_term "QR2"  ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_B",TInt;"B1__B",TInt]));
        RA_Leaf(Rel("R",["QR_B",TInt;"B1__B",TInt]))
      ]))),
    map_term "QR3"  ["QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["T1__A",TInt;"QR_A",TInt]));
        RA_Leaf(Rel("R",["T1__A",TInt;"B1__A",TInt]));
        RA_Leaf(Rel("R",["B1__A",TInt;"QR_B",TInt]))
      ]))),
    map_term "QR4"  ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_A",TInt;"B1__A",TInt]));
        RA_Leaf(Rel("R",["B1__A",TInt;"QR_A",TInt]));
        RA_Leaf(AtomicConstraint(Eq,RVal(Var("QR_A",TInt)),RVal(Var("QR_B",TInt))))
      ]))),
    map_term "QR5"  ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_Leaf(Rel("R",["QR_A", TInt;"QR_A",TInt]));
    ))),
    map_term "QR6"  ["QR_A",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
        RA_Leaf(Rel("R",["QR_B", TInt;"QR_B",TInt]));
      ))),
    map_term "QR7"  ["QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_A", TInt;"QR_A",TInt]));
        RA_Leaf(AtomicConstraint(Eq,RVal(Var("QR_A",TInt)),RVal(Var("QR_B",TInt))))
      ]))),
    map_term "QR8"  ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["T1__A",TInt;"T1__B",TInt]));
        RA_Leaf(Rel("R",["T1__A",TInt;"QR_A",TInt]));
        RA_Leaf(Rel("R",["T1__B",TInt;"QR_B",TInt]))
      ]))),
    map_term "QR9"  ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_A",TInt;"T1__B",TInt]));
        RA_Leaf(Rel("R",["T1__B",TInt;"QR_A",TInt]));
        RA_Leaf(AtomicConstraint(Eq,RVal(Var("QR_A",TInt)),RVal(Var("QR_B",TInt))))
      ]))),
    map_term "QR10" ["QR_A",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["T1__A",TInt;"QR_A",TInt]));
        RA_Leaf(Rel("R",["T1__A",TInt;"QR_A",TInt]));
      ]))),
    map_term "QR11" ["QR_A",TInt]
  ];;

let (ladder_2, ladder_2_term) = List.hd ladder_1_todos;;


let tc_test_term = 
  make_term (
    RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(AtomicConstraint(Eq, RVal(Var("QR_A",TInt)),
                                     RVal(Var("QR1R_A",TInt))));
        RA_Leaf(AtomicConstraint(Eq, RVal(Var("B1__A",TInt)),
                                     RVal(Var("QR1R_A",TInt))));
        RA_Leaf(AtomicConstraint(Eq, RVal(Var("B1__A",TInt)),
                                     RVal(Var("QR1R_B",TInt))));
        RA_Leaf(AtomicConstraint(Eq, RVal(Var("B1__B",TInt)),
                                     RVal(Var("QR1R_B",TInt))));
        RA_Leaf(Rel("R",["QR_B",TInt;"B1__B",TInt]))
      ]
    ))
  );;

Debug.log_unit_test_list "Simplify Transitive Closure" term_as_string
  (List.map (fun ((_,x),_) -> x)
    (Calculus.simplify tc_test_term
      ["QR1R_A",TInt;"QR1R_B",TInt] [] ["QR_A",TInt;"QR_B",TInt])
  )
  [
    make_term (
      RVal(AggSum(RVal(Const(Int(1))),
        RA_MultiNatJoin[
          RA_Leaf(Rel("R",["QR_B",TInt;"QR1R_A",TInt]));
          RA_Leaf(AtomicConstraint(Eq,RVal(Var("QR1R_A",TInt)),
                                      RVal(Var("QR1R_B",TInt))))
        ]
      ))
    )
  ];;

let (ladder_2_compiled, ladder_2_todos) = 
  compile_delta_for_rel 
      "R" 
      ["A",TInt;"B",TInt]
      false
      ladder_2_term
      []
      []
      ladder_2;;

Debug.log_unit_test_list "Quad-Self-Join Stage 2 Term" term_as_string
  (List.map (fun (_,_,_,_,x) -> x) ladder_2_compiled)
  [
    make_term (RVal(External("QR1R1",["QR1R_B",TInt;"QR_B",TInt])));
    make_term (RProd[RVal(External("QR1R2",["QR_A",TInt;"QR1R_A",TInt]));
                     RVal(External("QR1R3",["QR_B",TInt;"QR1R_B",TInt]))]);
    make_term (RVal(External("QR1R4",
                                ["QR_B",TInt;"QR1R_A",TInt;"QR1R_B",TInt])));
    make_term (RVal(External("QR1R5",["QR_A",TInt;"QR1R_B",TInt])));
    make_term (RVal(External("QR1R6",["QR1R_B",TInt])));
    make_term (RVal(External("QR1R2",["QR_A",TInt;"QR1R_A",TInt])));
    make_term (RVal(AggSum(RVal(Const(Int(1))),
                           RA_Leaf(AtomicConstraint(Eq,
                             RVal(Var("QR1R_A",TInt)),
                             RVal(Var("QR1R_B",TInt)))))))
  ];;
  
Debug.log_unit_test_list "Quad-Self-Join Stage 2 Todos" string_of_term_mapping 
  ladder_2_todos
  [
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR1R_B",TInt;"B1__B",TInt]));
        RA_Leaf(Rel("R",["QR_B", TInt;"B1__B",TInt]))
      ]))),
    map_term "QR1R1" ["QR1R_B",TInt;"QR_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_Leaf(Rel("R",["QR_A",TInt;"QR1R_A",TInt]))
    ))),
    map_term "QR1R2" ["QR_A",TInt;"QR1R_A",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_Leaf(Rel("R",["QR_B",TInt;"QR1R_B",TInt]))
    ))),
    map_term "QR1R3" ["QR_B",TInt;"QR1R_B",TInt]
    ;
    (* This one's a little funky, but yeah... it needs to be the way it is *)
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_B",TInt;"QR1R_A",TInt]));
        RA_Leaf(AtomicConstraint(Eq,RVal(Var("QR1R_A",TInt)),
                                    RVal(Var("QR1R_B",TInt))))
      ]))),
    map_term "QR1R4" ["QR_B",TInt;"QR1R_A",TInt;"QR1R_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_MultiNatJoin[
        RA_Leaf(Rel("R",["QR_A",TInt;"B1__A",TInt]));
        RA_Leaf(Rel("R",["B1__A",TInt;"QR1R_B",TInt]));
      ]))),
    map_term "QR1R5" ["QR_A",TInt;"QR1R_B",TInt]
    ;
    make_term (RVal(AggSum(RVal(Const(Int(1))),
      RA_Leaf(Rel("R",["QR1R_B",TInt;"QR1R_B",TInt]))
    ))),
    map_term "QR1R6" ["QR1R_B",TInt]
  ]