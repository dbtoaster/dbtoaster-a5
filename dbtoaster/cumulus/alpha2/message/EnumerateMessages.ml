open Util;;
open M3;;

open StmtToMapAccessList;;
open UpdateApply;;

(*********************** STRING ***********************)
let string_of_max_trigs
  (max_trigger_args : (var_t * int) array) : string =
  let tuple_to_string ((variable, value) : (var_t * int)) : string =
    "Max of variable " ^ variable ^ 
    " is " ^ string_of_int value in
  let tuple_arr = Array.map tuple_to_string max_trigger_args in
  let tuple_list= Array.to_list tuple_arr in
  String.concat "\n" tuple_list
;;


(*********************** UTILITY ***********************)
(* If element do not exist in max_trigger_args array *)
let invalid = -1;;

(* return index of out_var in max_trigger_array *)
(* max_trigger_args has to be array in order to change them in place *)
(* returns index or invalid if element is not found *)
let get_index_array
  (out_var : string)
  (max_trigger_args : (var_t * int) array) : int =
  let len = Array.length max_trigger_args in
  let index = ref invalid in
  for i = 0 to len - 1 do
    if ( fst (max_trigger_args.(i)) = out_var) then
      index := i
  done;
  (!index)
;;

(* return index of out_var in out_vars list *)
let rec get_index_list
  (out_var : var_t)
  (out_vars : var_t list) : int =
  match out_vars with
    | [] -> 0
    | hd :: tail -> 
        if out_var = hd then
          0
        else  
          1 + get_index_list out_var tail
;;

let rec is_power_2
  ?(pow : int = 1)
  (number : int) : bool = 
   if number <= 0 then
     false
   else if pow = number then
     true
   else if pow > number then
     false
   else
     is_power_2 ~pow:(2*pow) number
;;

(* Returns Lowest Common Multiple of first and second
   It will be greater of them if they are powers of 2 *)
let lcm 
  (first : int)
  (second : int) : int =
  let is_power_first = is_power_2 first in
  let is_power_second = is_power_2 second in
  if (is_power_first && is_power_second) then
    if first > second then
      first
    else 
      second
  else
    failwith ("Partition sizes are not powers of 2!")
;;

(*********************** ENUMERATING ***********************)

(* max_trigger_args will contain only those out variables from trigger
   which already shown in some previous Map Access / out_var 
  
   update_var_values consists of tuples 
     (trigger variable: string, update value from current execution: int)

   If variable exist in the trigger variables, 
     we will check maximum in partition info file.

   out_var element is added to max_trigger_array and new array is returned
   added means element is put in array 
     only if it didn't existed there or existed but with smaller value
   
   out_vars is sent in order to get dimension of out_var in out_vars
     not possible to do automatically in List.fold_left method

   TODO: rewrite it using Lists, since arrays have limitations of 4MB in size.
*)
let enumerate_out_var
  (mapn : string)
  (out_vars : var_t list)
  (trigger_args : var_t list)
  (max_trigger_args : (var_t * int) array)
  (out_var : string) : (var_t * int) array =
  let dimension = get_index_list out_var out_vars in
  if List.exists 
    (fun trigger_arg -> trigger_arg = out_var) trigger_args then
      let value = Partitions.get_dimension_size mapn dimension in
      let index_array = get_index_array out_var max_trigger_args in
      let current_val = ref 0 in
      let arr = ref [||] in
      if index_array = invalid then begin
        arr := Array.append max_trigger_args [|(out_var, value)|];
        (!arr)
      end else begin
        current_val := (snd(max_trigger_args.(index_array)));
        if (!current_val < value) then
          max_trigger_args.(index_array) <- (out_var, (lcm value !current_val));
        (max_trigger_args)
      end
   else
     (max_trigger_args)
;;

(* Enumerate all maximums in map_access.
   This method return array since max_trigger_args is array. *)
let enumerate_map_access
  (trigger_args : var_t list)
  (max_trigger_args : (var_t * int) array)
  ((mapn, inv, outv, init) : ('c,'a) generic_mapacc_t) : (var_t * int) array =
  List.fold_left (enumerate_out_var mapn outv trigger_args) max_trigger_args outv
;;

(* Enumerate all maximums in statement. *)
let enumerate_stmt
  (trigger_args : var_t list)
  (max_trigger_args : (var_t * int) array)
  (statement : ('c, 'a, 'sm) generic_stmt_t) : (var_t * int) array =
  let left_ma, right_ma_list = StmtToMapAccessList.maps_of_stmt statement in
  let ma_list = left_ma :: right_ma_list in
  List.fold_left (enumerate_map_access trigger_args) max_trigger_args ma_list
;;

(* Enumerate all maximums in trigger. *)
let enumerate_trigger
  (max_trigger_args : (var_t * int) array)
  (trigger : ('c, 'a, 'sm) generic_trig_t) : (var_t * int) array =
  let trigger_args = UpdateApply.extract_trig_args trigger in
  let stmt_list = UpdateApply.extract_stmts_from_trigger trigger in
  List.fold_left (enumerate_stmt trigger_args) max_trigger_args stmt_list
;;


(* max_trigger_args has to be filled before call to this method *)
let possible_trig_args
  (trigger_args : var_t list) 
  (max_trigger_args : (var_t * int) array) : int list list =
  let generate_partitions (variable : var_t) : int list =
    let index = get_index_array variable max_trigger_args in
    if index = invalid then
      (* could be any value, since this variable is not used in any map access *) 
      [0]
    else
      let size = snd (max_trigger_args.(index)) in
      let partitions = UpdateApply.generate_indices size in
      (partitions) in        
  let partitions_by_trigger_args = List.map generate_partitions trigger_args in
  let combinations = Combinations.combine_parts partitions_by_trigger_args in
  (combinations)
;;  


let run_enumerated
  (trig : ('c, 'a, 'sm) generic_trig_t) 
  (update_args_list: int list list) : string = 
  let args_to_string (args : int list) : string =
    let str_list = List.map ( fun x -> string_of_int x) args in
    "[" ^ (String.concat ", " str_list) ^ "]" in
  let args_text_list = 
    List.map args_to_string update_args_list in
  let run_list = List.map (update_trigger trig) update_args_list in
  let text_list = List.map2 
    (fun argument result -> "\nFor tuple " ^ argument ^ " messages are:\n" ^ string_of_msg_list result) 
    args_text_list run_list in
  String.concat "" text_list
;;
