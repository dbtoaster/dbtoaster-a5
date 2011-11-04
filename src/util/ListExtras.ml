(* 
   Additional functionality for manipulating and working with lists
*)

let scan (f:('a list -> 'a -> 'a list -> 'b)) (l:('a list)): ('b list) = 
   let rec iterate prev curr_next =
      match curr_next with 
         | []         -> []
         | curr::next -> (f prev curr next) :: (iterate (prev@[curr]) next)
   in iterate [] l

let reduce_assoc (l:('a * 'b) list): (('a * ('b list)) list) =
   List.fold_right (fun (a,b) ret ->
      if List.mem_assoc a ret
      then (a, b :: (List.assoc a ret)) :: (List.remove_assoc a ret)
      else (a, [b]) :: ret
   ) l []

let flatten_list_pair (l:('a list * 'b list) list): ('a list * 'b list) =
   let (a, b) = List.split l in
      (List.flatten a, List.flatten b)

let outer_join_assoc (a:(('c * 'a) list)) (b:(('c * 'b) list)):
                     ('c * ('a option * 'b option)) list =
   let (outer_b, join) = 
      List.fold_left (fun (b, join) (c, a_v) ->
         (  List.remove_assoc c b,
            join @ 
               [  c, ((Some(a_v)),
                  (if List.mem_assoc c b then Some(List.assoc c b) 
                                         else None))
               ])
      ) (b, []) a
   in
      join @ (List.map (fun (c, b_v) -> (c, ((None), (Some(b_v))))) outer_b)

let string_of_list ?(sep = "; ") (string_of_elem: ('a -> string)) (l: 'a list) =
   String.concat sep (List.map string_of_elem l)

let ocaml_of_list (string_of_elem: 'a -> string) (l: 'a list) = 
   "[" ^ (string_of_list string_of_elem l) ^ "]"