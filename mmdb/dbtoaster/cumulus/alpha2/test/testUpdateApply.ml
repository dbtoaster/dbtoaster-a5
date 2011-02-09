open Util;;
open M3;;

open StmtToMapAccessList;;
open Partitions;;
open Combinations;;
open UpdateApply;;

(* Setting partition info. 
   Divide mod for all tests *)
let q3 = ("q3", [4]);;
let q4 = ("q4", [8; 32]);;
let q5 = ("q5", [16; 4]);;
let q6 = ("q6", []);;
let q7 = ("q7", [4; 2; 8; 3]);;
let test_map_sizes = [q3; q4; q5; q6; q7];;

Partitions.map_sizes_pointers := test_map_sizes;;


(* Initialization of program. *)
let stmt0 =
  (("q5", [], ["a"; "c"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q3", [], ["a"], (mk_c 0.0, ()))) 
              (mk_ma ("q3", [], ["c"], (mk_c 0.0, ())))), 
    ()),
  ())
;;

let stmt1 =
  (("q3", [], ["a"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["b"], (mk_c 0.0, ()))) 
              (mk_ma ("q5", [], ["b"], (mk_c 0.0, ())))), 
    ()),
  ())
;;

let stmt2 =
  (("q3", [], ["c"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["a"], (mk_c 0.0, ()))) 
              (mk_ma ("q3", [], ["c"], (mk_c 0.0, ())))), 
    ()),
  ())
;;

let stmt3 =
  (("q5", [], ["a"; "b"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["a"], (mk_c 0.0, ()))) 
              (mk_ma ("q3", [], ["c"], (mk_c 0.0, ())))), 
    ()),
  ())
;;


let stmt4 =
  (("q6", [], [], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["a"], (mk_c 0.0, ()))) 
              (mk_ma ("q3", [], ["c"], (mk_c 0.0, ())))), 
    ()),
  ())
;;

let stmt5 =
  (("q7", [], ["a"; "c"; "b"; "d"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["a"], (mk_c 0.0, ()))) 
              (mk_ma ("q3", [], ["c"], (mk_c 0.0, ())))), 
    ()),
  ())
;;


let trig0 = (Insert, "R", ["a"; "b"], [stmt0]);;
let trig1 = (Delete, "R", ["a"; "b"], [stmt1]);;

let prog0 : prog_t =
  (
    [("q3", [], [VT_Int]); ("q4", [], [VT_Int]); ("q5", [], [VT_Int])],
    [trig0; trig1]
  )
;;

(* Actual testings *)
let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt0 in
  let left_combinations = UpdateApply.process_ma update_var_values left_ma in
  Debug.log_unit_test "UpdateApply (a = 3) (loop c has 4 partitions)" 
    (generate_message "") 
    (left_combinations)
    (
      "q5", [ 
              [("a", 3); ("c", 0)];
              [("a", 3); ("c", 1)];
              [("a", 3); ("c", 2)];
              [("a", 3); ("c", 3)]
            ]
    )
;;

let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt2 in
  let left_combinations = UpdateApply.process_ma update_var_values left_ma in
  Debug.log_unit_test "UpdateApply (loop c has 4 partitions)"
    (generate_message "") 
    (left_combinations)
    (
      "q3", [ 
              [("c", 0)];
              [("c", 1)];
              [("c", 2)];
              [("c", 3)]
            ]
    )
;;

let update_var_values = extract_update_var_values trig0 [3; 2] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt3 in
  let left_combinations = UpdateApply.process_ma update_var_values left_ma in
  Debug.log_unit_test "UpdateApply q5 (a=3) (b=2)"
    (generate_message "") 
    (left_combinations)
    (
      "q5", [ 
              [("a", 3); ("b", 2)]
            ]
    )
;;


let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt4 in
  let left_combinations = UpdateApply.process_ma update_var_values left_ma in
  Debug.log_unit_test "UpdateApply q6 single partition"
    (generate_message "") 
    (left_combinations)
    (
      "q6", [ 
              []
            ]
    )
;;
    
let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt5 in
  let left_combinations = UpdateApply.process_ma update_var_values left_ma in
  Debug.log_unit_test "UpdateApply q7 (a=3) (c=0 1) (b=4) (d=0 1 2)"
    (generate_message "") 
    (left_combinations)
    (
      "q7", [ 
              [("a", 3); ("c", 0); ("b", 4); ("d", 0)];
              [("a", 3); ("c", 0); ("b", 4); ("d", 1)];
              [("a", 3); ("c", 0); ("b", 4); ("d", 2)];
              [("a", 3); ("c", 1); ("b", 4); ("d", 0)];
              [("a", 3); ("c", 1); ("b", 4); ("d", 1)];
              [("a", 3); ("c", 1); ("b", 4); ("d", 2)]
            ]
    )
;;


(* Printings 
print_string(stmt_to_string stmt0);;*)
print_string(update prog0 M3.Insert "R" [3; 4]);;

