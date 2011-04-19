open Util;;
open M3;;

open StmtToMapAccessList;;

(* This ml file contains maps in the system and their partitions.
   Paritioning could be multidimensional, i.e. q[]["a","b"] *)


(* DEFINITIONS to arrive from partition input file
   (name_of_map, [num_of_partitions_per_dimension0, 
		  num_of_partitions_per_dimension1, ...]) *) 
type map_dimension_sizes = (string * int list);;
let q3 = ("q3", [4]);;
let q4 = ("q4", [8; 32]);;
let q5 = ("q5", [16; 5]);;
let map_sizes_list = [q3; q4; q5];;

(* Initial value is present,
   but could be changed anytime from outside world. *)
let map_sizes_pointers = ref map_sizes_list;;


(* METHODS *)

(* Extract partitioning info for mapn from particular MapAccess 
   i.e. for ("q3", [4]) it returns [4]				*)
let get_map_sizes_from_ma
  (mapn : string) : map_dimension_sizes =
  try
    List.find (fun x -> fst (x) = mapn) (!map_sizes_pointers)
  with Not_found -> 
    failwith (" No entry in Partitions file for MapAccess named " ^ mapn ^ ".")
;;

(* Returns number of dimensions for MapAccess, 
   i.e. for q[]["a"; "b"] it returns 2 *)
let get_num_of_dimensions 
  (mapn : string) : int =
  let map_sizes = get_map_sizes_from_ma mapn in 
  let sizes = snd map_sizes in
  List.length sizes
;;

(* Returns x-th dimension size of map_dimension_sizes
   with name extracted from MapAccess *)
let get_dimension_size 
  (mapn : string) 
  (x : int) : int =
  let map_sizes = get_map_sizes_from_ma mapn in  
(* returns map_dimension_sizes 
   i.e. (q3, [10, 20]), which means q3 has 10x20 partitions *)
  let sizes = snd map_sizes in
  try
    List.nth sizes x
(* returns for dimension 1 in above example value 20 *)
  with 
  |  Failure _ -> 
      failwith ("Requested nonexisting higher dimension " ^ 
      string_of_int(x) ^ " for map " ^ mapn ^ ".")
  |  Invalid_argument _ -> 
       failwith ("Negative dimension not allowed " ^ mapn ^ ".")
;;


(* Specifies function which translates index to a partition
   1-dimensional
   point to some of Divide methods, for now always divide_mod 
   Having two arguments of type int, and return type of int   *)
type function_partition = int -> int -> int;;

(* Divides consecutive block of indexes among consecutive DW Node *)
(* eg indexes 0 1 2 3 will be divided among two DW Nodes as (0,2) and (1,3) *)
let divide_mod 
  (num_of_partitions : int) (index : int) : int =
  index mod num_of_partitions
;;

(* Divides consecutive block of indexes to the same DW Node *)
(* eg indexes 0 1 2 3 will be divided among two DW Nodes as (0,1) and (2,3) *)
let divide_consecutive 
  (size_of_partition : int) (index : int) : int =
  index / size_of_partition
;;

(* This will also come from partition input file *)
(* Function could be specified as strings in file 
      and matched with appropriate representation
   For now there is only divide_consecutive and divide_moduo functions,
	but additional ones could be specified with the same signature *)
let get_function_partition 
  (mapn : string) : function_partition=
  divide_mod
;;


(* This is external entrance point.
   It returns partition number for particular dimension and index.
   I.e. q[][10; 20] for dimension 1 and index 12 return 12%20=12.
   This method will not be called for loops.		
*)
let get_partition 
  (mapn : string) 
  (dimension : int) 
  (index : int) : int =
  let func_part = get_function_partition mapn in
(* always return divide_mod function *)
  let size = get_dimension_size mapn dimension in
(* size is actual partition size for particular MapAccess and dimension *)
  func_part size index
;;

