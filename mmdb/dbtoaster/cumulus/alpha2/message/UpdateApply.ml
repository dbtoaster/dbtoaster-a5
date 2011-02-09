open Util;;
open M3;;

open StmtToMapAccessList;;
open Partitions;;
open Combinations;;
			 (* mapn * var_name * partition  *)
                         (* i.e 
                           q5    a=3  b=2 
 				 a=2  b=2
                         *)
type mapn_combinations = string * ((string * int) list list);;

(*************************** GENERAL PURPOSE ***************************)
(* generate numbers from 0 to max_index-1 *)
let generate_indices (max_index : int) : int list =
  if max_index < 0 then
    failwith ("Indexes could be generated only for non-negative numbers.")
  else
    let array_indexes = Array.init max_index (fun x -> x) in
    Array.to_list array_indexes
;;

let both_empty 
  (vars : var_t list)
  (values : int list) : bool =
  List.length vars = 0 &&
  List.length values = 0
;;

(*************************** MAP ACCESS ***************************)

(* Extract map access's output variables
   That is from q["a"]["b", "c"] return ["b", "c"] *)
let extract_out_vars 
  ((mapn, inv, outv, init) : ('c,'a) generic_mapacc_t) : var_t list=
  if List.length inv!=0 then 
    failwith("Input variables not yet supported in " ^ mapn)
  else 
    outv
;;


(* Generate String from output of the process_left_ma method *)
let generate_message
  (introduce_text : string)
  ((mapn, var_value_dbl_list) : mapn_combinations) : string = 
  let generate_pair 
    ((out_var, partition) : (var_t * int)) : string =
    "["^ out_var ^ "=" ^ (string_of_int(partition)) ^ "]" in
  let generate_combination
    (combination : (var_t * int) list) : string =
    String.concat "" (List.map generate_pair combination) in
  introduce_text ^ mapn ^
  String.concat ("\n" ^ mapn) (List.map generate_combination var_value_dbl_list) 
;;


(* From MapAccess and possible combinations 
   creates mapn_combinations structure *)
let generate_mapn_combinations
  (ma : ('c,'a) generic_mapacc_t) 
  (combinations : int list list) : mapn_combinations = 
  let mapn = StmtToMapAccessList.extract_map_name ma in
  let out_vars = extract_out_vars ma in
  let generate_combination 
    (combination: int list) : (var_t * int) list =
    try
      List.combine out_vars combination	 
    with Invalid_argument _ -> 
      failwith ("Unexpected: Combination size and output variables size do not match.")
  in
  if List.length out_vars = 0 then
    (mapn, [[]])
  else
    (mapn, (List.map generate_combination combinations))
;;


(* Distinguish between loop variables 
	(variables showing in Output Variable list of MapAccess and NOT in trigger arguments list)
   and regular variables 
	(variables showing both in Output Variable list of MapAccess and in trigger arguments list) 
   It is the same for the left and for the right hand side. *)
let process_ma 
  (update_var_values : (var_t * int) list)
  (ma : ('c,'a) generic_mapacc_t) : mapn_combinations =
  let mapn = StmtToMapAccessList.extract_map_name ma in
  let out_vars = extract_out_vars ma in
  let out_vars_length = List.length out_vars in
  let dimensions = generate_indices out_vars_length in
  let out_vars_indexed = List.combine out_vars dimensions in
(* indexes_by_dimensions for (concrete, loop) yields ([2], [0, 1, 2, 3, 4, ... max])*)
  let partition_by_dimension 
    ((out_var, dimension) : (var_t * int)) : int list =
    if List.mem_assoc out_var update_var_values then
(* Output variable from map access is available in trigger argument list *)
      let update_var_value = List.assoc out_var update_var_values in
      let partition = Partitions.get_partition mapn dimension update_var_value in
      ([partition])
    else
(* Output variable from map access is NOT available in trigger argument list 
      We will send generate partition for each index in the map_access            *)
      let size = Partitions.get_dimension_size mapn dimension in
      let partitions = generate_indices size in
      (partitions)
  in
  let partitions_by_dimensions = 
(* Comparing size of output variables list in program (dimensions)
   against number of dimensions in Partitions input file *)
    if out_vars_length = Partitions.get_num_of_dimensions mapn then
      List.map partition_by_dimension out_vars_indexed
    else
      failwith ("Number of dimensions for " ^ mapn ^ 
      " do not match in Partitions and M3 program.") 
  in
  let combinations = Combinations.combine_parts partitions_by_dimensions in
  generate_mapn_combinations ma combinations
;;


(* This method run processing for each right MapAccess and concatenates resulting strings *)
let process_right_ma_list 
  (update_var_values : (var_t * int) list )
  (right_ma_list : ('c,'a) generic_mapacc_t list) : mapn_combinations list =
  List.map
    (fun ma -> process_ma update_var_values ma) right_ma_list 
;;


(*************************** STATEMENT ***************************)
let extract_stmts_from_trigger 
  ((_, _, _, stmt_list) : ('c, 'a, 'sm) generic_trig_t) 
  : (('c, 'a, 'sm) generic_stmt_t list) =
  stmt_list
;;

(* Extract the left hand side map and the right-hand side maps from a statement 
   in order to produce FETCH and PUT messages *)
let stmt_to_messages 
  (statement : ('c, 'a, 'sm) generic_stmt_t) 
  (update_var_values : (var_t * int) list ) : string =
  let left_ma, right_ma_list = StmtToMapAccessList.maps_of_stmt statement in
  let left_intro = "PUT message is sent to the following partitions: \n" in
  let left_mapn_info = process_ma update_var_values left_ma in
  let left_message = generate_message left_intro left_mapn_info in
  let right_intro = "FETCH message is sent to the following partitions: \n" in
  let right_mapn_info_list = 
    process_right_ma_list update_var_values right_ma_list in
  let right_message_list = 
    List.map (generate_message right_intro) right_mapn_info_list in
  let right_message = String.concat "\n" right_message_list in

 "\nLEFT SIDE: \n" ^ left_message ^ "\n" ^
  "\nRIGHT SIDE: \n" ^ right_message ^ "\n"
;;

(*************************** TRIGGER ***************************)
let extract_trigs_from_program 
  (prog : ('c,'a,'sm) generic_prog_t) : ('c,'a,'sm) generic_trig_t list =
  snd (prog)
;;

let extract_trig_args ((_, _, var_list, _) : ('c, 'a, 'sm) generic_trig_t) : var_t list =
  var_list
;;

let extract_trig_insdel 
  ((pm, _, _, _) : ('c, 'a, 'sm) generic_trig_t) : M3.pm_t =
  pm
;;

let extract_trig_name 
  ((_, relname, _, _) : ('c, 'a, 'sm) generic_trig_t) : M3.rel_id_t =
  relname
;;

(* Extract statements from a trigger
   in order to produce FETCH and PUT messages*)
let trigger_to_messages 
  (trigger : ('c, 'a, 'sm) generic_trig_t) 
  (update_var_values :  (var_t * int) list ) : string =
  let stmt_list=extract_stmts_from_trigger trigger in
  let list_of_messages=List.map 
    (fun stmt -> stmt_to_messages stmt update_var_values) 
    stmt_list in
  String.concat "\n" list_of_messages
;;


(* Update method will consist of List with elements, i.e. : [3; 4] 
     which will correspond to the trigger arguments, i.e. : ["a", "b"].
   For each map access we have to apply exchange trigger argument to concrete value from Update method, if one exist.
   Otherwise we may end up having slices constructs.
*)
let extract_update_var_values 
  (trig : ('c, 'a, 'sm) generic_trig_t) 
  (update_args: int list) : (var_t * int) list =  
  let trigger_args = extract_trig_args trig in
(* Both of them has to be zero in order not to raise exception*)
  if both_empty trigger_args update_args then
    ([])
  else
    try 
      List.combine trigger_args update_args
    with Invalid_argument _ -> 
      failwith("Length of trigger arguments and Update arguments do not match!")
;;

(* update_var_values is a tuple consisting of 
     1) trigger variable names as specified in M3 program
     2) update_args which comes from particular execution of a trigger *)
let update_trigger 
  (trig : ('c, 'a, 'sm) generic_trig_t) 
  (update_args: int list) : string = 
  let update_var_values = extract_update_var_values trig update_args in
  trigger_to_messages trig update_var_values
;;


(*************************** PROGRAM ***************************)
(* This is main method run on M3.prog_t as argument *)
let update 
  (prog : ('c,'a,'sm) generic_prog_t) 
  (insdel : pm_t)
  (relname : rel_id_t) 
  (update_args : int list) : string = 
  try	
    let triggers = extract_trigs_from_program prog in
    let find_trig trig =
      extract_trig_name trig = relname 
      && extract_trig_insdel trig = insdel in
    let trigger = List.find find_trig triggers in
    update_trigger trigger update_args
  with Not_found -> 
    "There is no trigger with name " ^ relname ^
    " and type " ^ 
    match insdel with
      | M3.Insert -> "Insert"
      | M3.Delete -> "Delete"
    ^ "."
;;
