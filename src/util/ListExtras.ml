(**
   Additional functionality for manipulating and working with lists
*)

(** Scan through elements of a list.  Similar to List.iter, but the elements
    both before and after the element currently being processed are passed to 
    the lambda function.
    
    e.g., On the list [1;2;3;4], the lambda function would be invoked as...
    f [] 1 [2;3;4]
    f [1] 2 [3;4]
    f [1;2] 3 [4]
    f [1;2;3] 4 [] *)
let scan (f:('a list -> 'a -> 'a list -> unit)) (l:('a list)): unit = 
   let rec iterate prev curr_next =
      match curr_next with 
         | []         -> ()
         | curr::next -> (f prev curr next); (iterate (prev@[curr]) next)
   in iterate [] l
   
(** Map the elements of a list in the same manner as scan.  Like 
    ListExtras.scan, but based on List.map instead of List.iter. *)
let scan_map (f:('a list -> 'a -> 'a list -> 'b)) (l:('a list)): ('b list) = 
   let rec iterate prev curr_next =
      match curr_next with 
         | []         -> []
         | curr::next -> (f prev curr next) :: (iterate (prev@[curr]) next)
   in iterate [] l

(** Fold the elements of a list in the sam manner as scan.  Like ListExtras.scan
    but based on List.fold_left instead of List.iter *)
let scan_fold (f:('b -> 'a list -> 'a -> 'a list -> 'b)) (init:'b) 
              (l:('a list)): 'b = 
   let rec iterate curr_val prev curr_next =
      match curr_next with 
         | []         -> curr_val
         | curr::next -> (iterate (f curr_val prev curr next) 
                                  (prev@[curr]) next
                         )
   in iterate init [] l

(** A standard reduce (i.e., group-by) implementation on lists.

    Given a list of 2-tuples {b (a,b)}, produce a list of 2-tuples 
    {b (a,b list)} such that the returned list has only one element for every 
    distinct value of {b a}, and every value of {b b} appears paired with the 
    same value of {b a} that it appeared with in the input list.*)
let reduce_assoc (l:('a * 'b) list): (('a * ('b list)) list) =
   List.fold_right (fun (a,b) ret ->
      if List.mem_assoc a ret
      then (a, b :: (List.assoc a ret)) :: (List.remove_assoc a ret)
      else (a, [b]) :: ret
   ) l []

(** Flatten on a list of 2-tuples of lists.  Shorthand for List.split, and 
    flattening each list individually *)
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

let index_of (a:'a) (l:'a list): int =
   let idx, fnd = (List.fold_left (fun (i,r) e -> 
      if r then (i,r) else if e = a then (i,true) else (i+1,false)
   ) (0,false) l) in
      if fnd then idx else raise Not_found

let sublist (start:int) (cnt:int) (l:'a list):'a list =
   snd (List.fold_left (fun (i, r) e ->
      if i <= 0 then
         if (List.length r < cnt) or (cnt < 0) then (i, r @ [e])
                                               else (i, r)
      else (i-1,r)
   ) (start,[]) l)

let split_at_pivot (a:'a) (l:'a list): 'a list * 'a list =
   let idx = index_of a l in
      (sublist 0 (idx) l, sublist (idx+1) (-1) l)

let assoc_fold (fn: 'a -> 'b -> 'c -> 'c) (init:'c) (l:('a * 'b) list): 'c =
   List.fold_left (fun c (a, b) -> fn a b c) init l

let max (l:'a list):'a = 
   if l = [] then raise Not_found
   else List.fold_left Pervasives.max (List.hd l) (List.tl l)

let min (l:'a list):'a = 
   if l = [] then raise Not_found
   else List.fold_left Pervasives.min (List.hd l) (List.tl l)

let sum l = List.fold_left (+) 0 l