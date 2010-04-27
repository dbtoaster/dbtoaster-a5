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
    ((mk_if 
      (mk_lt
        (mk_ma ("QUERY_1_1__2",[price_var],[],(
          (mk_if 
            (mk_lt (mk_v price_var) (mk_v "B2__PRICE"))
            ((mk_ma ("QUERY_1_1__2_init",[],[price_var],(mk_c 0.,()))))
          )
        ,())))
        (mk_prod (mk_c 0.25) (mk_ma ("QUERY_1_1__3",[],[],(mk_c 0.,()))))
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
    
