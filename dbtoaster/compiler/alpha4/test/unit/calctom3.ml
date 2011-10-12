(* Unit tests for CalcToM3 and parts of Compiler that generate M3 *)

open Util
open Calculus
open M3
open CalcToM3
open Compiler
;;

let mk_vwap_delta_2 price_var volume_var : M3.stmt_t =
  (
    ("QUERY_1_1",[],[],(mk_c 0.,())),
    M3.Stmt_Update,
    ((mk_if 
      (mk_lt
        (mk_ma false ("QUERY_1_1__2",[price_var],[],(
          (mk_if 
            (mk_lt (mk_v price_var) (mk_v "B2__PRICE"))
            ((mk_ma false ("QUERY_1_1__2_init",[],[price_var],(mk_c 0.,()))))
          )
        ,())))
        (mk_prod (mk_c 0.25) (mk_ma false ("QUERY_1_1__3",[],[],(mk_c 0.,()))))
      )
      (mk_v volume_var)
    ),()),
    ()
  );;


Debug.log_unit_test "M3Common.rename_vars Sanity Check" 
  (M3Common.pretty_print_stmt)
  (M3Common.rename_vars 
    ["QUERY_1_1BIDS_B1__PRICE";"QUERY_1_1BIDS_B1__VOLUME"]
    ["QUERY_1_1__1BIDS_B1__PRICE";"QUERY_1_1__1BIDS_B1__VOLUME"]
    (mk_vwap_delta_2 "QUERY_1_1BIDS_B1__PRICE" "QUERY_1_1BIDS_B1__VOLUME"))
  (mk_vwap_delta_2 "QUERY_1_1__1BIDS_B1__PRICE" "QUERY_1_1__1BIDS_B1__VOLUME")
    
let calc_cmp_terms_1 = 
  make_term (
    RVal(AggSum(
      RVal(Calculus.Const(Int(1))),
      RA_Leaf(Rel("E", 
          ["E2__X",TInt;"E2__Y",TInt;"QUERY_1_1E1E_E1__PLAYER",TInt]))
    ))
  );; 
let calc_cmp_terms_2 = 
  make_term (
    RVal(AggSum(
      RVal(Calculus.Const(Int(1))),
      RA_Leaf(Rel("E", 
          ["E2__X",TInt;"E2__Y",TInt;"E2__PLAYER",TInt]))
    ))
  );;

let var_translation = 
try
  let var_translation = equate_terms calc_cmp_terms_1 calc_cmp_terms_2 in
    match (StringMap.fold (fun l r a -> 
      Some((match a with Some(a) -> a^"; " | None -> "")^l^"->"^r) 
    ) var_translation None) with
      | None -> ""
      | Some(a) -> "["^a^"]"
with TermsNotEquivalent(a) -> ("Comparison Error: "^a);;

Debug.log_unit_test "Calculus.equate_terms Variable Diff" (fun x->x)
  var_translation
  "[E2__X->E2__X; E2__Y->E2__Y; QUERY_1_1E1E_E1__PLAYER->E2__PLAYER]";;