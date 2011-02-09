open Util;;
open M3;;

open UpdateApply;;
open EnumerateMessages;;

(* Setting partition info. 
   Divide mod for all tests *)
let q1 = ("q1", [12; 20])
let q2 = ("q2", [10]);;
let q3 = ("q3", [4]);;
let q4 = ("q4", [2]);;
let test_map_sizes = [q1; q2; q3; q4];;

Partitions.map_sizes_pointers := test_map_sizes;;

(* get_index_array *)
let out_var = "a" in
  let max_trigger_args = [|("b", 2)|] in
    let index = EnumerateMessages.get_index_array out_var max_trigger_args in
      Debug.log_unit_test "Enumerate variable not found" string_of_int 
      (index)
      (EnumerateMessages.invalid)      
;;


let out_var = "a" in
  let max_trigger_args = [|("b", 2); ("a", 3)|] in
    let index = EnumerateMessages.get_index_array out_var max_trigger_args in
      Debug.log_unit_test "Enumerate variable found on index 1" string_of_int 
      (index)
      (1)      
;;

(* process one out_var *)
let max_trigger_args = [||];;
let arr = ref max_trigger_args;;

let mapn = "q3" in
  let out_vars = ["a"] in
  let out_var = "a" in
  let trigger_args = ["b"; "a"] in
  arr := EnumerateMessages.enumerate_out_var mapn out_vars trigger_args (!arr) out_var
;;

 Debug.log_unit_test "First insert a" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 4)|])      
;;

let mapn = "q4" in
  let out_vars = ["a"] in
  let out_var = "a" in
  let trigger_args = ["a"; "b"] in
  arr := EnumerateMessages.enumerate_out_var mapn out_vars trigger_args (!arr) out_var
;;

  Debug.log_unit_test "Second insert a" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 4)|])      
;;

(* process ma *)
let ma = ("q2", [], ["a"], (mk_c 0.0, ())) in
  let trigger_args = ["a"; "b"] in
  arr := enumerate_map_access trigger_args (!arr) ma
;;

Debug.log_unit_test "Third insert a in ma" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 10)|])
;; 

let ma = ("q1", [], ["b"; "a"], (mk_c 0.0, ())) in
  let trigger_args = ["a"; "b"] in
  arr := enumerate_map_access trigger_args (!arr) ma
;;

(* a is in arr before b is added *)
Debug.log_unit_test "Insert of two args in ma" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 20); ("b", 12)|])
;;      

(* process statement *)

(* at this point arrays is empty again *)
let max_trigger_args = [||];;
let arr = ref max_trigger_args;;

let stmt0 =
  (("q3", [], ["a"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["b"], (mk_c 0.0, ()))) 
              (mk_ma ("q4", [], ["b"], (mk_c 0.0, ())))), 
    ()),
  ()) in
  let trigger_args = ["a"; "b"] in
  arr := enumerate_stmt trigger_args (!arr) stmt0
;;

Debug.log_unit_test "Test simple statement" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 4); ("b", 2)|])
;;      

(* print_combinations *)
let trigger_args = ["a"; "b"] in
  let combinations = set_max_trigger_args trigger_args (!arr) in
    Debug.log_unit_test "Print combinations" Combinations.string_of_int_combinations 
      (combinations)
      ([
        [0; 0]; [0; 1];
        [1; 0]; [1; 1];
        [2; 0]; [2; 1];
        [3; 0]; [3; 1];
      ])
;;

(* how to call running enumerated
let stmt1 =
  (("q3", [], ["a"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["b"], (mk_c 0.0, ()))) 
              (mk_ma ("q4", [], ["b"], (mk_c 0.0, ())))), 
    ()),
  ()) in
  let trig0 = (Insert, "R", ["a"; "b"], [stmt1]) in
  let trigger_args = ["a"; "b"] in
  let combinations = set_max_trigger_args trigger_args (!arr) in
  let messages = run_enumerated trig0 combinations in
  print_string (messages)
;;
*)


(* at this point arrays is empty again *)
let max_trigger_args = [||];;
let arr = ref max_trigger_args;;

let stmt1 =
  (("q2", [], ["a"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q3", [], ["b"], (mk_c 0.0, ()))) 
              (mk_ma ("q1", [], ["b"; "a"], (mk_c 0.0, ())))), 
    ()),
  ()) in
  let trigger_args = ["a"; "b"] in
  arr := enumerate_stmt trigger_args (!arr) stmt1
;;

Debug.log_unit_test "Test two out_vars statement" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 20); ("b", 12)|])
;;     

(* process trigger *)
(* at this point arrays is empty again *)
let max_trigger_args = [||];;
let arr = ref max_trigger_args;;

let stmt0 =
  (("q3", [], ["a"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["b"], (mk_c 0.0, ()))) 
              (mk_ma ("q4", [], ["b"], (mk_c 0.0, ())))), 
    ()),
  ()) in
  let trig0 = (Insert, "R", ["a"; "b"], [stmt0]) in
  arr := enumerate_trigger (!arr) trig0
;;

Debug.log_unit_test "Test trigger" EnumerateMessages.string_of_max_trigs
      (!arr)
      ([|("a", 4); ("b", 2)|])
;;     


(* at this point new result_array is used, initialized as empty *)
let stmt0 =
  (("q3", [], ["a"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["b"], (mk_c 0.0, ()))) 
              (mk_ma ("q4", [], ["b"], (mk_c 0.0, ())))), 
    ()),
  ()) in
  let trig0 = (Insert, "R", ["a"; "b"], [stmt0]) in
  let trigger_args = UpdateApply.extract_trig_args trig0 in
  let result_array = enumerate_trigger [||] trig0 in
  let combinations = set_max_trigger_args trigger_args result_array in
  let messages = run_enumerated trig0 combinations in
  print_string (messages)
;;


