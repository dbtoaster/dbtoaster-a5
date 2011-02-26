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
  let left_possibility = UpdateApply.process_ma update_var_values left_ma in
  let left_combinations = mapn_comb_of_poss left_possibility in
  Debug.log_unit_test "UpdateApply (a = 3) (loop c has 4 partitions)" 
    (string_of_map_comb_list) 
    (left_combinations)
    ( 	    
            [ 
              ("q5", [("a", 3); ("c", 0)]);
              ("q5", [("a", 3); ("c", 1)]);
              ("q5", [("a", 3); ("c", 2)]);
              ("q5", [("a", 3); ("c", 3)])
            ]
    )
;;

let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt2 in
  let left_possibility = UpdateApply.process_ma update_var_values left_ma in
  let left_combinations = mapn_comb_of_poss left_possibility in
  Debug.log_unit_test "UpdateApply (loop c has 4 partitions)"
    (string_of_map_comb_list) 
    (left_combinations)
    (
            [ 
              ("q3", [("c", 0)]);
              ("q3", [("c", 1)]);
              ("q3", [("c", 2)]);
              ("q3", [("c", 3)])
            ]
    )
;;


let update_var_values = extract_update_var_values trig0 [3; 2] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt3 in
  let left_possibility = UpdateApply.process_ma update_var_values left_ma in
  let left_combinations = mapn_comb_of_poss left_possibility in
  Debug.log_unit_test "UpdateApply q5 (a=3) (b=2)"
    (string_of_map_comb_list)
    (left_combinations)
    (
            [ 
              ("q5", [("a", 3); ("b", 2)])
            ]
    )
;;


let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt4 in
  let left_possibility = UpdateApply.process_ma update_var_values left_ma in
  let left_combinations = mapn_comb_of_poss left_possibility in
  Debug.log_unit_test "UpdateApply q6 single partition"
    (string_of_map_comb_list)
    (left_combinations)
    (
            [ 
              ("q6", [])
            ]
    )
;;
    
let update_var_values = extract_update_var_values trig0 [3; 4] in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt5 in
  let left_possibility = UpdateApply.process_ma update_var_values left_ma in
  let left_combinations = mapn_comb_of_poss left_possibility in
  Debug.log_unit_test "UpdateApply q7 (a=3) (c=0 1) (b=4) (d=0 1 2)"
    (string_of_map_comb_list)
    (left_combinations)
    (
            [ 
              ("q7", [("a", 3); ("c", 0); ("b", 4); ("d", 0)]);
              ("q7", [("a", 3); ("c", 0); ("b", 4); ("d", 1)]);
              ("q7", [("a", 3); ("c", 0); ("b", 4); ("d", 2)]);
              ("q7", [("a", 3); ("c", 1); ("b", 4); ("d", 0)]);
              ("q7", [("a", 3); ("c", 1); ("b", 4); ("d", 1)]);
              ("q7", [("a", 3); ("c", 1); ("b", 4); ("d", 2)])
            ]
    )
;;

let mp = ("q1", [SINGLE_VAR("a", 8); LOOP_VAR("b", 4); SINGLE_VAR("c", 3); LOOP_VAR("d", 2)]) in
  let converted = UpdateApply.mapn_comb_of_poss mp in
  Debug.log_unit_test "Conversion of mapn_possibility to mapn_combination list"
    (UpdateApply.string_of_map_comb_list)
    (converted)
    (
            [ 
              ("q1", [("a", 8); ("b", 0); ("c", 3); ("d", 0)]);
              ("q1", [("a", 8); ("b", 0); ("c", 3); ("d", 1)]);
              ("q1", [("a", 8); ("b", 1); ("c", 3); ("d", 0)]);
              ("q1", [("a", 8); ("b", 1); ("c", 3); ("d", 1)]);
              ("q1", [("a", 8); ("b", 2); ("c", 3); ("d", 0)]);
              ("q1", [("a", 8); ("b", 2); ("c", 3); ("d", 1)]);
              ("q1", [("a", 8); ("b", 3); ("c", 3); ("d", 0)]);
              ("q1", [("a", 8); ("b", 3); ("c", 3); ("d", 1)])
            ]
    )
;;


let right_out_vars = [("a", 44); ("b", 22)] in
  let left_out_vars = [("c", 1); ("b", 2); ("a", 4); ("d", 2)] in
  let lp_list = UpdateApply.create_loop_params right_out_vars left_out_vars in
  Debug.log_unit_test "Creation of loop_params structure"
    (UpdateApply.string_of_lp_list)
    (lp_list)
    (
            [ 
               ("a", 4, 44) ; ("b", 2, 22)
            ]
    )
;;

let lp_list = [("a", 4, 8); ("a", 4, 8); ("b", 8, 4); ("b", 8, 4); ("c", 8, 8)] in
  let extracted = [("a", 7); ("a", 2); ("b", 0); ("b", 3); ("c", 6)] in
  let computed = UpdateApply.lhs_compute_loops lp_list extracted in
  Debug.log_unit_test "Lhs concrete computation"
    (UpdateApply.string_of_var_set_list)
    (computed)
    (
            [ 
               ("a" , [3]); ("a" , [2]);
	       ("b" , [0; 4]); ("b", [3; 7]);
               ("c" , [6])
            ]
    )
;;

let right_poss = ("qr", [
                           SINGLE_VAR("a", 1);
	                   SINGLE_VAR("b", 2)
                        ]
  ) in
  let left_poss = ("ql", [
                           SINGLE_VAR("a", 1);
	                   SINGLE_VAR("b", 2);
                           SINGLE_VAR("c", 3) 
                         ]
  ) in
  let expected = [
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 1); ("b", 2)]),
                     ("ql", [("a", 1); ("b", 2); ("c", 3)])
                   )
                 ] in
  let computed = UpdateApply.generate_push_message right_poss left_poss in
   Debug.log_unit_test "Push messages no loops"
    (UpdateApply.string_of_msg_list)
    (computed)
    (expected)
;;


let right_poss = ("qr", [
                           LOOP_VAR("a", 1);
	                   LOOP_VAR("b", 2);
                           SINGLE_VAR("c", 3) 
                        ]
  ) in
  let left_poss = ("ql", [
                           LOOP_VAR("a", 1);
	                   LOOP_VAR("b", 2);
                           SINGLE_VAR("c", 3); 
                           SINGLE_VAR("d", 4);
                         ]
  ) in
  let expected = [
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 0); ("b", 0); ("c", 3)]),
                     ("ql", [("a", 0); ("b", 0); ("c", 3); ("d", 4)])
                   ); 

		   (UpdateApply.PUSH, 
                     ("qr", [("a", 0); ("b", 1); ("c", 3)]),
                     ("ql", [("a", 0); ("b", 1); ("c", 3); ("d", 4)])
                   )
                 ] in
  let computed = UpdateApply.generate_push_message right_poss left_poss in
   Debug.log_unit_test "Push messages 2 loops same length"
    (UpdateApply.string_of_msg_list)
    (computed)
    (expected)
;;
   

let right_poss = ("qr", [
                           SINGLE_VAR("b", 3); 
                        ]
  ) in
  let left_poss = ("ql", [
                            LOOP_VAR("a", 4);
			    SINGLE_VAR("b", 3); 
                            LOOP_VAR("c", 2)
                         ]
  ) in
  let expected = [
		   (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 0); ("b", 3); ("c", 0)])
                   ); 

		   (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 0); ("b", 3); ("c", 1)])
                   );

                   (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 1); ("b", 3); ("c", 0)])
                   );

	           (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 1); ("b", 3); ("c", 1)])
                   );

	           (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 2); ("b", 3); ("c", 0)])
                   );

                   (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 2); ("b", 3); ("c", 1)])
                   );

                  (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 3); ("b", 3); ("c", 0)])
                   );

                  (UpdateApply.PUSH, 
                     ("qr", [("b", 3)]),
                     ("ql", [("a", 3); ("b", 3); ("c", 1)])
                   );

                 ] in
  let computed = UpdateApply.generate_push_message right_poss left_poss in
   Debug.log_unit_test "Push messages 2 loop 1 concrete"
    (UpdateApply.string_of_msg_list)
    (computed)
    (expected)
;;


let right_poss = ("qr", [
                           SINGLE_VAR("a", 8); 
                           LOOP_VAR("b", 2); 
                           SINGLE_VAR("c", 3); 
                           LOOP_VAR("d", 4);
                        ]
  ) in
  let left_poss = ("ql", [
                            SINGLE_VAR("a", 8);
                            LOOP_VAR("b", 4);
			    SINGLE_VAR("c", 3); 
                            LOOP_VAR("d", 2);
                         ]
  ) in
  let expected = [
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 0)]),
                     ("ql", [("a", 8); ("b", 0); ("c", 3); ("d", 0)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 0)]),
                     ("ql", [("a", 8); ("b", 2); ("c", 3); ("d", 0)])
                   ); 
		  (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 1)]),
                     ("ql", [("a", 8); ("b", 0); ("c", 3); ("d", 1)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 1)]),
                     ("ql", [("a", 8); ("b", 2); ("c", 3); ("d", 1)])
                   ); 
		
                   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 2)]),
                     ("ql", [("a", 8); ("b", 0); ("c", 3); ("d", 0)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 2)]),
                     ("ql", [("a", 8); ("b", 2); ("c", 3); ("d", 0)])
                   ); 
                   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 3)]),
                     ("ql", [("a", 8); ("b", 0); ("c", 3); ("d", 1)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 0); ("c", 3); ("d", 3)]),
                     ("ql", [("a", 8); ("b", 2); ("c", 3); ("d", 1)])
                   ); 

		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 0)]),
                     ("ql", [("a", 8); ("b", 1); ("c", 3); ("d", 0)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 0)]),
                     ("ql", [("a", 8); ("b", 3); ("c", 3); ("d", 0)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 1)]),
                     ("ql", [("a", 8); ("b", 1); ("c", 3); ("d", 1)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 1)]),
                     ("ql", [("a", 8); ("b", 3); ("c", 3); ("d", 1)])
                   );

                   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 2)]),
                     ("ql", [("a", 8); ("b", 1); ("c", 3); ("d", 0)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 2)]),
                     ("ql", [("a", 8); ("b", 3); ("c", 3); ("d", 0)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 3)]),
                     ("ql", [("a", 8); ("b", 1); ("c", 3); ("d", 1)])
                   ); 
		   (UpdateApply.PUSH, 
                     ("qr", [("a", 8); ("b", 1); ("c", 3); ("d", 3)]),
                     ("ql", [("a", 8); ("b", 3); ("c", 3); ("d", 1)])
                   );
                 ] in
  let computed = UpdateApply.generate_push_message right_poss left_poss in
   Debug.log_unit_test "Push messages 2 lhs loops"
    (UpdateApply.string_of_msg_list)
    (computed)
    (expected)
;;


(* Printings 
print_string(stmt_to_string stmt0);;*)
print_string(update prog0 M3.Insert "R" [3; 4]);;
