open Util;;
open M3;;
open Partitions;;

(* Setting partition info. 
   Divide mod for all tests *)
let q3 = ("q3", [4]);;
let q4 = ("q4", [8; 32]);;
let q5 = ("q5", [16; 3]);;
let test_map_sizes = [q3; q4; q5];;
Partitions.map_sizes_pointers := test_map_sizes;;



let mapn = "q4" in
  let dims = Partitions.get_num_of_dimensions mapn in
    Debug.log_unit_test "Partitions dim" string_of_int 
      (dims)
      (2)
;;

let mapn = "q4" in
  let size = Partitions.get_dimension_size mapn 0 in
    Debug.log_unit_test "Partitions size" string_of_int 
      (size)
      (8)
;;

let mapn = "q4" in
  let partition = Partitions.get_partition mapn 0 31 in
    Debug.log_unit_test "Partitions get_partition" string_of_int 
      (partition)
      (7)
;;
