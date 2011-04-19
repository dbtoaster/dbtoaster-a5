open Util;;
open Combinations;;

(* This example do not work due to string methods, 
   but Combinations works correctly for this example
let initial = [[]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination empty" Combinations.string_of_string_combinations 
      (combinations)
        ([
          [] 
        ])
;;
*)

let initial = [["F1"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 1+0" Combinations.string_of_string_combinations 
      (combinations)
        ([
          ["F1"]
        ])
;;


let initial = [["F1"; "F2"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 2+0" Combinations.string_of_string_combinations 
      (combinations)
        ([
          ["F1"]; ["F2"] 
        ])
;;


(* This example in my program will never happen. 
   Logically, it should return empty set. *)
let initial = [["F1"]; []] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 1+empty" Combinations.string_of_string_combinations 
      (combinations)
        ([
          ["F1"] 
        ])
;;

let initial = [["F1"]; ["E1"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 1+1" Combinations.string_of_string_combinations 
      (combinations)
        ([
          ["F1"; "E1"]; 
        ])
;;

let initial = [["F1"]; ["E1"; "E2"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 1+2" Combinations.string_of_string_combinations 
      (combinations)
        ([
          ["F1"; "E1"]; ["F1"; "E2"]; 
        ])
;;

let initial = [["F1"; "F2"]; ["E1"; "E2"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 2-2" Combinations.string_of_string_combinations 
      (combinations)
      ([
        ["F1"; "E1"]; ["F1"; "E2"]; 
        ["F2"; "E1"]; ["F2"; "E2"]
      ])
;;

let initial = [["FIR1"; "FIR2"]; ["M1"; "M2"]; ["SEC1"; "SEC2"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 3-2" Combinations.string_of_string_combinations 
      (combinations)
      ([
        ["FIR1"; "M1"; "SEC1"]; ["FIR1"; "M1"; "SEC2"];
        ["FIR1"; "M2"; "SEC1"]; ["FIR1"; "M2"; "SEC2"];
        ["FIR2"; "M1"; "SEC1"]; ["FIR2"; "M1"; "SEC2"];
        ["FIR2"; "M2"; "SEC1"]; ["FIR2"; "M2"; "SEC2"];
      ])
;;

let initial = [["F1"; "F2"; "F3"]; ["E1"; "E2"; "E3"]] in
  let combinations = Combinations.combine_parts initial in
    Debug.log_unit_test "Combination 2-3" Combinations.string_of_string_combinations 
      (combinations)
      ([
        ["F1"; "E1"]; ["F1"; "E2"]; ["F1"; "E3"];
        ["F2"; "E1"]; ["F2"; "E2"]; ["F2"; "E3"];
        ["F3"; "E1"]; ["F3"; "E2"]; ["F3"; "E3"];
      ])
;;

