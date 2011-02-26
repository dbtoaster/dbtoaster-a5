open Util;;
open M3;;

open StmtToMapAccessList;;
open Partitions;;
open Combinations;;

                (* i.e. a=3 *) 
type var_part = string * int;;
                        (* mapn * var_name * partition  *)
                         (* i.e 
                           q5    a=3  b=2 
                         *)
type mapn_combination = string * (var_part list);;

      (* i.e. a = (2, 4) mean lhs use 2 as maximum loop index, while rhs use 4 *)
type loop_params = string * int * int;;

               (* name, [value1; value2; value3] 
                  values are computed from lhs and rhs mapn_possibility 
                    and refers to lhs *)
type var_set = string * (int list);;

(* string is variable name
   if variable is LOOP_VAR than int is upper bound of indices,
     otherwise it is its concrete value *)
type var_poss = 
  | SINGLE_VAR of string * int
  | LOOP_VAR of string * int
;;

(* var_poss list keeps order of our_var from MapAccess *)
type mapn_possibility = string * (var_poss list)

type msg_type = 
  | FETCH 
  | PUT 
  | PUSH
;;

(* Msg_type, i.e. FETCH, Source i.e. q[a=3, b=2], Destination i.e. q1[a=2]  *)
type message = msg_type * mapn_combination * mapn_combination;;

(* This string has special meaning 
   if found in mapn in source or destination *)
let switch_name = "SWITCH";;

(*************************** GENERAL PURPOSE ***************************)
(* generate numbers from start_index to end_index-1 
   if start_index and end_index are equal, return empty list 
   Working only for positive steps
   Method rev_add is tail-recursive, 
     and List.rev has to invert elements of the list
  *)
let generate_indices
  ?(start_index = 0) 
  ?(step = 1) 
  (end_index : int) : int list =
  let rec rev_add result_lst current_val =
    match (current_val >= end_index) with
      | false -> rev_add (current_val :: result_lst) (current_val + step)
      | true -> result_lst
  in
  if (end_index - start_index < 0) then
    failwith ("Generate indices for end_index < start_index is impossible.")
  else
    List.rev (rev_add [] start_index)
;;

let both_empty 
  (vars : var_t list)
  (values : int list) : bool =
  List.length vars = 0 &&
  List.length values = 0
;;

(* Combine appropriate out_vars names and values 
   combinations are from Combinations.combine_parts *)
let generate_mapn_combinations
  (mapn : string)
  (out_vars : var_t list)
  (combinations : int list list) : mapn_combination list =
  let generate_combination 
    (combination: int list) : var_part list =
    try
      List.combine out_vars combination	 
    with Invalid_argument _ -> 
      failwith ("Unexpected: Combination size and output variables size do not match.")
    in
  let append_mapn
    (mapn : string)
    (vp_list : var_part list) : mapn_combination =
    (mapn, vp_list)  
  in
  if List.length out_vars = 0 then
    [(mapn, [])]
  else
(* mapn is put on the begginging of each combination [var = value; var = value] *)
    List.map (append_mapn mapn) (List.map generate_combination combinations)
;;

(* Extract out variables from var_poss list of mapn_possibility 
   Original order from MapAccess is preserved *)
let vars_from_poss_list
  (var_poss_list : var_poss list) : var_t list = 
  let extract_name
    (vp : var_poss) : var_t =
    match vp with
      | SINGLE_VAR (name, _) -> name
      | LOOP_VAR (name, _) -> name
  in
  List.map extract_name var_poss_list
;;
    

(* from mapn_possibility to mapn_combination list convertor *)
let mapn_comb_of_poss
  ((mapn, var_poss_list) : mapn_possibility) 
    : mapn_combination list =
  let out_vars = vars_from_poss_list var_poss_list in
  let expand_var (vp : var_poss) : int list =
    match vp with
      | SINGLE_VAR (name, value) ->
          [value]
      | LOOP_VAR (name, value) ->
          generate_indices  value
  in
  let partitions = List.map expand_var var_poss_list in
  let combinations = Combinations.combine_parts partitions in
  generate_mapn_combinations mapn out_vars combinations
;;
  

(*************************** STRING ***************************)
let string_of_msg_type
  (msg_t: msg_type) : string =
  match msg_t with 
    | FETCH -> "Fetch message "
    | PUT -> "Put message "
    | PUSH -> "Push message "
;; 

let string_of_map_comb
  ((mapn, combination) : mapn_combination) : string =
  let generate_pair 
    ((out_var, partition) : (var_t * int)) : string =
    "["^ out_var ^ "=" ^ (string_of_int(partition)) ^ "]" in  
  mapn ^ (String.concat "" (List.map generate_pair combination))
;;

let string_of_map_comb_list
  (mapn_combs : mapn_combination list) : string =
  Util.string_of_list0 "\n" string_of_map_comb mapn_combs
;;

let string_of_map_poss
  (mapn_poss : mapn_possibility) : string =
  string_of_map_comb_list (mapn_comb_of_poss mapn_poss)
;;

let string_of_map_poss_list
  (mapn_poss : mapn_possibility list) : string =
  Util.string_of_list0 "\n" string_of_map_poss mapn_poss
;;

(* Generate String from output of the (source, destination) pair *)
let string_of_message
  ((msg_t, src, dst) : message) : string = 
  string_of_msg_type msg_t ^ 
  "from " ^ string_of_map_comb src ^
  " to " ^ string_of_map_comb dst
;;

let string_of_msg_list
  (msg_list : message list) : string =
  (Util.string_of_list0 "\n" string_of_message msg_list) ^ "\n"
;;

let string_of_loop_params
  ((name, lmax, rmax) : loop_params) : string =
  "[" ^ name ^ ", " ^ 
  string_of_int lmax ^ ", " ^ 
  string_of_int rmax ^ "]"
;;

let string_of_lp_list
  (lp_list : loop_params list) : string =
  (Util.string_of_list0 "\n" string_of_loop_params lp_list) ^ "n"
;;

let string_of_var_set
  ((name, values) : var_set) : string =
  name ^ "[" ^
  (Util.string_of_list0 ", " string_of_int values) ^
  "]"
;;

let string_of_var_set_list
  (var_set_list : var_set list) : string =
  (Util.string_of_list0 "\n" string_of_var_set var_set_list) ^ "n"
;;

(*************************** MAP ACCESS ***************************)

(* Extract map access's output variables
                q[input variables][output variables]    
   That is from q["a"]["b", "c"] return ["b", "c"] *)
let extract_out_vars 
  ((mapn, inv, outv, init) : ('c,'a) generic_mapacc_t) : var_t list=
  if List.length inv!=0 then
    failwith("Input variables not yet supported in " ^ mapn)
  else 
    outv
;;

(* Return list of messages from multiple mapn_combination 
   from a single MapAccess.
   Works for both FETCH and PUT messages. *)
let generate_switch_message
  (msg_t : msg_type)
  (mapn_combs : mapn_combination list) : message list =
  let create_msg
    (mapn_comb : mapn_combination) : message = 
    (msg_t, (switch_name, []), (mapn_comb)) in
  List.map create_msg mapn_combs
;;

(* Create list of loop vars structuted as list:
   [(out_var1, max_value1); (out_var2, max_value2);...]
   Result_list is expanded only in case of LOOP_VAR.
   add_if_loop is tail-recursive, so at the end result has to be reversed. *)
let filter_loops
  (var_poss_list : var_poss list) : var_part list =
  let rec add_if_loop result_list remain_list =
    match remain_list with
      | [] -> result_list
      | hd :: tail -> 
          match hd with
            | LOOP_VAR (name, value) ->
                add_if_loop ((name, value) :: result_list) tail
            | SINGLE_VAR (name, value) ->
                add_if_loop result_list tail
  in
  List.rev (add_if_loop [] var_poss_list)
;;

(* Find element (in_name, value) from var_part_list and return its value *)
let find_val 
  (in_name : string)
  (var_part_list : var_part list) : int =
  let elem_comb = 
    try
      List.find (fun (name, value) -> name = in_name) var_part_list
    with Not_found -> failwith ("Unexpected error : element not found in the collection.")
  in
  (snd(elem_comb))
;;


(* Puts maximum from lhs and rhs into single structure 
   (name, left_max, right_max).
   out_var_loops has same structure as var_part list : (string, int).
   out_var_loops are from filter_loops method. 
   Output of this method contains all loop variables from the right-hand side,
     but not necessarily all from the left-hand side MapAccess.
   Tail-recursive with reversal of the result.
  *)
let create_loop_params
  (right_out_var_loops : var_part list) 
  (left_out_var_loops : var_part list) : loop_params list =
  let add_loop
    ((rname, rvalue) : var_part) : loop_params =
    (rname,
    (find_val rname left_out_var_loops), 
    rvalue) in
  let rev_result = List.rev_map add_loop right_out_var_loops in
  List.rev rev_result
;;

(* From the right-hand side single mapn_combination
   extract values of loop output variables.
   Result is in form of [(name1, value1); (name2, value2);...]
   All names in result is from loop_params_list.
   Tail-recursive with reversal of the result. *)
let extract_loop_var
  ((mapn, var_part_list) : mapn_combination)
  (loop_params_list : loop_params list) : var_part list =
  let add_value
    ((name, lvalue, rvalue) : loop_params) : var_part =
    (name, (find_val name var_part_list)) in
  let rev_result = List.rev_map add_value loop_params_list in
  List.rev rev_result
;;

(* Using loop_params in form of list of (name, lhs max, rhs max) 
   and extracted concrete values of loop out vars from rhs, 
   compute possibilities for all lhs loop out vars 
     which exists in the rhs as well.
   loop_params_list and extracted_vars has to be the same length. 
   value from extracted vars must not be bigger
     than appropriate field in loop_params_list.
  *)
let lhs_compute_loops
  (loop_params_list : loop_params list)
  (extracted_vars : var_part list) : var_set list =
  let generate_values
    ((lp_name, lmax, rmax) : loop_params)
    ((var, value) : var_part) : var_set =
    if (value >= rmax) then 
      failwith ("Unexpected: The concrete value from the rhs higher
        than its maximum allowed.")
    else
    if (rmax < lmax) then
      (lp_name, (generate_indices ~start_index:value ~step:rmax lmax))
    else if (rmax > lmax) then
      (lp_name, [(value mod lmax)])
    else
      (lp_name, [value])
  in 
  try
    List.map2 generate_values loop_params_list extracted_vars    
  with Invalid_argument _ -> failwith ("Unexpected : Lists are not equal lenght.")
;;

(* Creates mapn_combination list
   -if out_var is loop
     - if out_var name is in computed its possibilities are adopted - (int list)
     - otherwise loop is present only on the lhs, full range has to be produces
   -else
     - single value is adopted.
   mapn_possibility is lhs,
   Possible multiple computed_loop_concrete 
     per each rhs possible rhs out_var combination. 
*)
let generate_push_combination
  ((mapn, var_poss_list) : mapn_possibility)
  (computed_loop_concrete : var_set list) : mapn_combination list =
  let out_vars = vars_from_poss_list var_poss_list in
  let isComputed
    (name : string) : bool =
    List.exists (fun x -> fst(x) = name) computed_loop_concrete in
  let computed_list 
    (name : string) = 
    let loop_concrete = 
      List.find (fun x -> fst(x) = name) computed_loop_concrete in
    (snd(loop_concrete)) in
  let generate_partitions
    (vp : var_poss) : int list =
    match vp with
      | SINGLE_VAR (name, value) -> [value]
      | LOOP_VAR (name, value) -> 
          match isComputed name with
            | false ->
                generate_indices value
            | true ->
                computed_list name
  in 
  let partitions = List.map generate_partitions var_poss_list in
  let combinations = Combinations.combine_parts partitions in
  generate_mapn_combinations mapn out_vars combinations
;;

(* Called for each pair lhs-rhs in a statement. *)
let generate_push_message 
  (right_mapn_poss : mapn_possibility) 
  (left_mapn_poss : mapn_possibility) : message list =
  let right_mapn_comb_list =
    mapn_comb_of_poss right_mapn_poss in
  let right_var_poss_list = 
    (snd(right_mapn_poss)) in
  let left_var_poss_list = 
    (snd(left_mapn_poss)) in
  let right_out_var_loops = 
    filter_loops right_var_poss_list in
  let left_out_var_loops = 
    filter_loops left_var_poss_list in
  let loop_parameters = 
    create_loop_params right_out_var_loops left_out_var_loops in
  let create_msg
    (src : mapn_combination)
    (dst : mapn_combination) : message = 
    (PUSH, (src), (dst)) in  
  let one_right_comb
    (right_mapn_comb : mapn_combination) : message list =
    let extracted_vars = extract_loop_var right_mapn_comb loop_parameters in
    let computed_for_lhs = lhs_compute_loops loop_parameters extracted_vars in
    let left_mapn_comb_list = 
      generate_push_combination left_mapn_poss computed_for_lhs in    
    List.map (create_msg right_mapn_comb) left_mapn_comb_list
  in 
  let messages_list = List.map one_right_comb right_mapn_comb_list in
  List.concat messages_list
;;


(* From MapAccess and possible combinations 
   creates mapn_combinations structure *)
let generate_ma_combinations
  (ma : ('c,'a) generic_mapacc_t) 
  (combinations : int list list) : mapn_combination list = 
  let mapn = StmtToMapAccessList.extract_map_name ma in
  let out_vars = extract_out_vars ma in
  generate_mapn_combinations mapn out_vars combinations
;;


(* Distinguish between loop variables 
(variables showing in Output Variable list of MapAccess and NOT in trigger arguments list)
   and regular variables 
(variables showing both in Output Variable list of MapAccess and in trigger arguments list) 
   It is the same for the left and for the right hand side. *)
let process_ma 
  (update_var_values : (var_t * int) list)
  (ma : ('c,'a) generic_mapacc_t) : mapn_possibility =
  let mapn = StmtToMapAccessList.extract_map_name ma in
  let out_vars = extract_out_vars ma in
  let out_vars_length = List.length out_vars in
  let dimensions = generate_indices out_vars_length in
  let out_vars_indexed = List.combine out_vars dimensions in
(* indexes_by_dimensions for (concrete, loop) yields ([2], [0, 1, 2, 3, 4, ... max])*)
  let partition_by_dimension 
    ((out_var, dimension) : (var_t * int)) : var_poss =
    if List.mem_assoc out_var update_var_values then
(* Output variable from map access is available in trigger argument list *)
      let update_var_value = List.assoc out_var update_var_values in
      let partition = Partitions.get_partition mapn dimension update_var_value in
      (SINGLE_VAR (out_var, partition))
    else
(* Output variable from map access is NOT available in trigger argument list 
      We will send generate partition for each index in the map_access            *)
      let size = Partitions.get_dimension_size mapn dimension in
      (LOOP_VAR (out_var, size))
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
  (mapn, partitions_by_dimensions)
;;


(* This method run processing for each right MapAccess and concatenates resulting strings *)
let process_right_ma_list 
  (update_var_values : (var_t * int) list )
  (right_ma_list : ('c,'a) generic_mapacc_t list) : mapn_possibility list =
  List.map
    (fun ma -> process_ma update_var_values ma) right_ma_list 
;;


(*************************** STATEMENT ***************************)
let extract_stmts_from_trigger 
  ((_, _, _, stmt_list) : ('c, 'a, 'sm) generic_trig_t) 
  : (('c, 'a, 'sm) generic_stmt_t list) =
  stmt_list
;;
 
(* Collects all message lists into single list of messages. 
   Tail recursive, but lists has to be inverted first *)
let collect_messages 
  (fetches : message list)
  (puts_list : message list list)
  (pushes_list : message list list) : message list = 
  let puts_list_inverted = List.rev puts_list in
  let pp_list = List.rev_append puts_list_inverted pushes_list in
  let pp = List.concat pp_list in
  let fetches_inverted = List.rev fetches in
  List.rev_append fetches_inverted pp
;;

(* Extract the left hand side map and the right-hand side maps from a statement 
   in order to produce FETCH, PUT and PUSH messages.
   Return type stmt and any higher level of M3 program is a message list.
   *)
let stmt_to_messages 
  (statement : ('c, 'a, 'sm) generic_stmt_t) 
  (update_var_values : (var_t * int) list ) : message list =
  let left_ma, right_ma_list = StmtToMapAccessList.maps_of_stmt statement in
  let left_mapn_poss = process_ma update_var_values left_ma in
  let left_mapn_comb = mapn_comb_of_poss left_mapn_poss in
  let fetches = generate_switch_message FETCH left_mapn_comb in
  let right_mapn_poss_list = 
    process_right_ma_list update_var_values right_ma_list in
  let right_mapn_comb_list = List.map mapn_comb_of_poss right_mapn_poss_list in
  let puts_list = List.map (generate_switch_message PUT) right_mapn_comb_list in
  let pushes_list = List.map 
    (fun right_mapn_poss -> generate_push_message right_mapn_poss left_mapn_poss) 
    right_mapn_poss_list
  in
    collect_messages fetches puts_list pushes_list
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
   in order to produce FETCH, PUT and PUSH messages *)
let trigger_to_messages 
  (trigger : ('c, 'a, 'sm) generic_trig_t) 
  (update_var_values : (var_t * int) list) : message list =
  let stmt_list = extract_stmts_from_trigger trigger in  
  (* Since in trigger cold potentially be a lot of statements:
  Tail recursive version of:
  List.fold_left 
    (fun msgs stmt -> 
       msgs @ (stmt_to_messages stmt update_var_values))
    ) [] stmt_list)
     is below:
  *)
  List.rev(List.fold_left 
    (fun msgs stmt -> 
       List.rev_append (stmt_to_messages stmt update_var_values) (msgs)
    ) [] stmt_list)
;;


(* Update method will consist of List with elements, i.e. : [3; 4] 
     which will correspond to the trigger arguments, i.e. : ["a", "b"].
   For each map access we have to apply exchange trigger argument
     to concrete value from Update method, if one exist.
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
  (update_args: int list) : message list = 
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
    let msgs = update_trigger trigger update_args in
    string_of_msg_list msgs
  with Not_found -> 
    "There is no trigger with name " ^ relname ^
    " and type " ^ 
    match insdel with
      | M3.Insert -> "Insert"
      | M3.Delete -> "Delete"
    ^ "."
;;
