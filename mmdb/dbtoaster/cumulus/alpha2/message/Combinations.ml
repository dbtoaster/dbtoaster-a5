open Util;;

(* Make all combinations of elements from sublists
   combine_parts [["F1"; "F2"] ["E1"; "E2"]] 
	returns [["F1"; "E1"]; ["F1"; "E2"]; ["F2"; "E1"]; ["F2"; "E2"]]	 
  Types are not defined in order to work with all types of data,
    i.e. int as well
*)

let rec combine_parts lst =
  match lst with
  | [] -> []
  | head :: tail -> append_list_to_possibilities head (combine_parts tail)
(* Append list elements to possibilities
   combine 
	lst1 - ["F1"; "F2"] 
	possibilities - [["E1"]; ["E2"]] 
	returns [["F1"; "E1"]; ["F1"; "E2"]; ["F2"; "E1"]; ["F2"; "E2"]]	 
*)
and append_list_to_possibilities lst1 possibilities =
	(* return type list of lists *)
  match possibilities with
(* No rev_map is used since we want InOrder result in combine_parts *)
  | [] -> (List.map (fun x -> [x]) lst1)
  | _ -> (match lst1 with 
	  | [] -> []
(* Tail recursive yields better performance:
 rev_append is used, since we reversed once in append_elt *)
          | head :: tail -> 
              List.rev_append (append_elt head possibilities)
                (append_list_to_possibilities tail possibilities)
         )
(* For each element in possibilities add elt on the beginning  
   append "B" [["E1"]; ["E2"]]
	returns [ ["B"; "E2"]; ["B"; "E1"]]	
	list is reversed since we use rev_map*)
and append_elt elt possibilities =
  List.rev_map (fun x -> (elt :: x)) possibilities
;;

let string_of_int_combinations (combinations : int list list) : string =
  let string_of_combination combination = 
    Util.list_to_string string_of_int combination in
  Util.list_to_string string_of_combination combinations
;;

let string_of_string_combinations (combinations : string list list) : string =
  let string_of_combination combination = 
    Util.list_to_string (fun x -> x) combination in
  Util.list_to_string string_of_combination combinations
;;
