(**
   Additional functionality for manipulating and working with lists, meant to 
   supplement the core functionality in Ocaml's List module.
*)

(** 
   Scan through elements of a list.  Similar to List.iter, but the elements
   both before and after the element currently being processed are passed to 
   the lambda function.
    
   e.g., On the list {[ [1;2;3;4] ]} the lambda function would be invoked as...
   
   {[ f [] 1 [2;3;4] ]}
   
   {[ f [1] 2 [3;4] ]}
   
   {[ f [1;2] 3 [4] ]}
   
   {[ f [1;2;3] 4 [] ]}
    
   @param f    The scan function.  On any given invocation of f, l may be 
               reconstructed by concatenating f's three inputs in order.
   @param l    The list to scan over
*)
let scan (f:('a list -> 'a -> 'a list -> unit)) (l:('a list)): unit = 
   let rec iterate prev curr_next =
      match curr_next with 
         | []         -> ()
         | curr::next -> (f prev curr next); (iterate (prev@[curr]) next)
   in iterate [] l
   
(** 
   Map the elements of a list in the same manner as scan.  Like 
   ListExtras.scan, but based on List.map instead of List.iter. 
   
   @param f    The map function.  On any given invocation of f, l may be 
               reconstructed by concatenating f's three inputs in order.
   @param l    The list to map over
   @return     The re-mapped list
*)
let scan_map (f:('a list -> 'a -> 'a list -> 'b)) (l:('a list)): ('b list) = 
   let rec iterate prev curr_next =
      match curr_next with 
         | []         -> []
         | curr::next -> (f prev curr next) :: (iterate (prev@[curr]) next)
   in iterate [] l

(** 
   Map the elements of a list in the same manner as scan.  Like 
   ListExtras.scan, but based on List.map instead of List.iter. 
   Note that the PREV list contains mapped elements.
   
   @param f    The map function.  On any given invocation of f, l may be 
               reconstructed by concatenating f's three inputs in order.
   @param l    The list to map over
   @return     The re-mapped list
*)
let scan_map2 (f:('b list -> 'a -> 'a list -> 'b)) (l:('a list)): ('b list) = 
   let rec iterate prev curr_next =
      match curr_next with 
         | []         -> []
         | curr::next -> 
            let mapped_curr = (f prev curr next) in
              mapped_curr :: (iterate (prev@[mapped_curr]) next)
   in iterate [] l

(** 
   Fold the elements of a list in the same manner as scan. Like ListExtras.scan
   but based on List.fold_left instead of List.iter  
   
   @param f    The fold function.  On any given invocation of f, l may be 
               reconstructed by concatenating f's first three inputs in order.  
               The fourth parameter is the most recent fold value.
   @param init The initial fold value
   @param l    The list to fold over
   @return     The final fold value
*)
let scan_fold (f:('b -> 'a list -> 'a -> 'a list -> 'b)) (init:'b) 
              (l:('a list)): 'b = 
   let rec iterate curr_val prev curr_next =
      match curr_next with 
         | []         -> curr_val
         | curr::next -> (iterate (f curr_val prev curr next) 
                                  (prev@[curr]) next
                         )
   in iterate init [] l

(** 
   A standard reduce (i.e., group-by) implementation on lists.

   Given a list of 2-tuples {b (a,b)}, produce a list of 2-tuples 
   {[ (a,b list) ]} such that the returned list has only one element for every 
   distinct value of [a], and every value of [b] appears paired with the 
   same value of [a] that it appeared with in the input list.
   
   @param l  The list of [(a,b)] elements to reduce
   @return   Tuples in [l] grouped by [a]
*)
let reduce_assoc (l:('a * 'b) list): (('a * ('b list)) list) =
   List.fold_right (fun (a,b) ret ->
      if List.mem_assoc a ret
      then (a, b :: (List.assoc a ret)) :: (List.remove_assoc a ret)
      else (a, [b]) :: ret
   ) l []

(** 
   Flatten on a list of 2-tuples of lists.  Shorthand for List.split, and 
   flattening each list individually 
   
   @param l  A list of list pairs
   @return   A pair of lists
*)
let flatten_list_pair (l:('a list * 'b list) list): ('a list * 'b list) =
   let (a, b) = List.split l in
      (List.flatten a, List.flatten b)

(**
   Compute the full associative outer join of two lists of 2-tuples using the 
   first tuple element as a join column.
   
   @param a  A list of 2-tuples [(c,a)]
   @param b  A list of 2-tuples [(c,b)]
   @return   A list of tuples [(c,(a option,b option))] where c comes from the 
             union of the first tuple elements of [a] and [b], and the 
             remaining fields are populated by the result of {[List.assoc a c]}
             and {[List.assoc b c]}.
*)
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

(**
   Translate a list into a string.
   
   @param sep            (optional) Inter-element separator string
   @param string_of_elem Stringifier for individual list elements
   @param l              The list to stringify
   @return               The stringified form of the list
*)
let string_of_list ?(sep = "; ") (string_of_elem: ('a -> string)) (l: 'a list) =
   String.concat sep (List.map string_of_elem l)

(**
   Translate a list into an ocaml-style string list
   
   @param string_of_elem Stringifier for individual list elements
   @param l              The list to stringify
   @return               The stringified form of the list
*)
let ocaml_of_list (string_of_elem: 'a -> string) (l: 'a list) = 
   "[" ^ (string_of_list string_of_elem l) ^ "]"

(**
   Find and return the index of the indicated element (testing for equality 
   using =).  The returned index is the parameter that would be passed to 
   List.nth to get the indicated element.
   
   @param a   The element to look for
   @param l   The list to look for the element in
   @return    The index of the element in the list 
*)
let index_of ?(cmp_fn:('a -> 'a -> bool) = (fun x y -> x = y))
             (a:'a) (l:'a list): int =
   let idx, fnd = (List.fold_left (fun (i,r) e -> 
      if r then (i,r) else if cmp_fn e a then (i,true) else (i+1,false)
   ) (0,false) l) in
      if fnd then idx else raise Not_found

(**
   Return the sublist of the passed list starting at the indicated index and 
   containing the indicated number of elements.
   
   @param start  The index of the first element to be included in the sublist
   @param cnt    The number of consecutive elements to return in the sublist
   @param l      The list to compute a sublist of
   @return       The sublist of [l] of size [cnt] starting at index [start].
*)
let sublist (start:int) (cnt:int) (l:'a list):'a list =
   snd (List.fold_left (fun (i, r) e ->
      if i <= 0 then
         if (List.length r < cnt) || (cnt < 0) then (i, r @ [e])
                                               else (i, r)
      else (i-1,r)
   ) (start,[]) l)

(**
   Partition a list, splitting at the position of a given pivot element.  
   This does not attempt to compare elements to the pivot element, but rather 
   returns splits based on position in the input list.
   
   @param a  The pivot element
   @param l  The input list
   @return   The 2-tuple consisting of all elements that occur (positionally) in
             [l] before the first occurrence of [a], and all the elements that 
             occur after.
*)
let split_at_pivot ?(cmp_fn:('a -> 'a -> bool) = (fun x y -> x = y))
                   (a:'a) (l:'a list): 'a list * 'a list =
   let idx = index_of ~cmp_fn:cmp_fn a l in
      (sublist 0 (idx) l, sublist (idx+1) (-1) l)

(**
   Partition a list, splitting at the given position.
   
   @param a  The split position
   @param l  The input list
   @return   The 3-tuple consisting of all elements that occur (positionally) in
             [l] before the split position, the element at the split position, 
             and all the elements that occur after.
*)
let split_at_position (pos:int) (l:'a list): 'a list * 'a list * 'a list =
   (sublist 0 (pos) l, sublist (pos) 1 l, sublist (pos+1) (-1) l)

(**
   Partition a list, splitting at the last position.
   
   @param l  The input list
   @return   The 3-tuple consisting of all elements that occur (positionally) in
             [l] before the split position, the element at the split position, 
             and all the elements that occur after.
*)
let split_at_last (l:'a list): 'a list * 'a list=
   let (x, y, _) = split_at_position ((List.length l)-1) l in
      (x, y)
   

(**
   Helper function for performing folds over associative lists.  Like 
   [List.fold_left], but the function takes 3 parameters rather than 2.
   
   @param fn    The fold function (with 3 parameters, rather than 2)
   @param init  The initial fold value
   @param l     The list to fold over
   @return      The final fold value
*)
let assoc_fold (fn: 'a -> 'b -> 'c -> 'c) (init:'c) (l:('a * 'b) list): 'c =
   List.fold_left (fun c (a, b) -> fn a b c) init l

(**
   Compute the maximal element of a list (according to [Pervasives.max]).
   
   @param l   The list
   @return    The maximal element of [l]
*)
let max (l:'a list):'a = 
   if l = [] then raise Not_found
   else List.fold_left Pervasives.max (List.hd l) (List.tl l)

(**
   Compute the minimal element of a list (according to [Pervasives.min]).
   
   @param l   The list
   @return    The minimal element of [l]
*)
let min (l:'a list):'a = 
   if l = [] then raise Not_found
   else List.fold_left Pervasives.min (List.hd l) (List.tl l)

(**
   Compute the sum of the elements of a list of integers.
   
   @param l   A list of integers
   @return    The maximal element of [l]
*)
let sum l = List.fold_left (+) 0 l

(**
   Convert a 2-element list into a 2-tuple
   
   @param l   A 2 element list
   @return    A 2-tuple containing the list
*)
let list_to_pair (l:'a list): 'a * 'a = 
   (List.hd l, List.hd (List.tl l)) 

(**
   Creates all k-tuples of elements of the list src.
   
   Example:
   
   {[ k_tuples 3 [1;2] =
            [[1; 1; 1]; [2; 1; 1]; [1; 2; 1]; [2; 2; 1]; [1; 1; 2];
             [2; 1; 2]; [1; 2; 2]; [2; 2; 2]]
   ]}
   
   @param k    The size of each returned list
   @param src  The set of possible elements in each returned list
   @return     A list of the returned lists
*)
let rec k_tuples k (src: 'a list) : 'a list list =
   if (k <= 0) then [[]]
   else List.flatten (List.map (fun t -> List.map (fun x -> x::t) src)
                               (k_tuples (k-1) src))

(** 
   Computes all subsets of r that have exactly k elements.
   does not produce duplicate sets if r does not have duplicates.
   sorting -- e.g. (List.sort Pervasives.compare l) --
   improves readability of the result. 
   @param k  The size of each returned list set
   @param r  The superset of all of the returned subsets 
*)
let subsets_of_size k r =
   let rec add_k_elements_to_from k l r =
      (* select_gt: selects those elements of r that are greater than x *)
      let select_gt x r =
         let gt y = y > x in
         List.filter (gt) r in
      let f x =
          (add_k_elements_to_from (k-1) (l@[x]) (select_gt x r)) in
      if k <= 0 then [l]
      else           List.flatten (List.map f r) in
   add_k_elements_to_from k [] r

(** Distributes a list of lists.

   e.g., 
   
   distribute [[[1;2]; [2;3]]; [[3;4]; [4;5]]; [[5;6]]];;
   
   results in

   [[[1; 2]; [3; 4]; [5; 6]]; [[2; 3]; [3; 4]; [5; 6]];
    [[1; 2]; [4; 5]; [5; 6]]; [[2; 3]; [4; 5]; [5; 6]]]
    
   @param l  A list of lists
   @return   The n-way cross product of all n elements in l
*)
let rec distribute (l: 'a list list) =
   if l = [] then [[]]
   else
      let f tail =
         let g x = [x] @ tail in
         List.map (g) (List.hd l)
      in
      List.flatten (List.map f (distribute (List.tl l)));;

(** Permutes a list of elements.

   e.g., 
   
   permute [1;2;3];
   
   results in

   [[1;2;3]; [1;3;2]; [2;1;3]; [2;3;1]; [3;1;2]; [3;2;1]]
    
   @param l  A list of lists
   @return   The permutation of all n elements in l
*)
let rec permute (l: 'a list) : 'a list list =
   if l = [] then [[]] else
   let rec insert elem sublist = match sublist with
      | [] -> [[elem]]
      | hd::tl -> (elem::sublist) :: List.map (fun x -> hd::x) (insert elem tl)
   in
      List.flatten (List.map (insert (List.hd l)) (permute (List.tl l)));


exception CycleFound

(** 
   Perform topological sort on a given graph starting from a given node.
    
   Example: 
        
   {[ toposort_from_node [ (1, [2;3]); (2, [4;5]); (4, [6]) ] [] 1 = 
                         [ 1; 3; 2; 5; 4; 6]
    ]}
        
   @param graph      A graph representing a partial order between elements
   @param visited    A list of already visited nodes (useful in multiple
                     invocations, see [toposort])
   @param start_node The root node of the graph
   @return           A list of topologically sorted elements 
*)
let toposort_from_node graph visited start_node =
   let rec explore path node visited =
      if List.mem node path then raise CycleFound 
      else if List.mem node visited then visited 
      else                    
         let new_path = node::path in
         let child_nodes = try List.assoc node graph with Not_found -> [] in
         let visited = List.fold_right (explore new_path) child_nodes visited in
            node :: visited
   in 
      explore [] start_node visited

(** 
   Perform topological sort on a given graph which may contain 
   multiple root elements.
   Example: 
        
   {[ toposort [ (0, [2; 3]); (1, [2]) ] = 
               [ 0; 3; 1; 2]
    ]}
   {[ toposort [ (1, [2]); (5, [6; 7]); (3, [2]); (6, [3; 7]); (8, [7]); 
                 (4, [3; 1]) ] =
               [ 5; 6; 8; 7; 4; 3; 1; 2]
    ]}
        
   @param graph      A graph representing a partial order between elements
   @return           A list of topologically sorted elements 
*)
let toposort graph =
   List.fold_right (fun (node, _) visited -> 
      toposort_from_node graph visited node) graph [] 
                                                            
(** 
   Compute transitive closure of a given set of edges. 
   Example:
        
   {[ transitive_closure [ (1,2); (2,3); (3,4) ] = 
               [(1, 2); (2, 3); (3, 4); (1, 3); (2, 4); (1, 4)]
    ]}
        
   @param edges      A list of edges
   @return           A transitive closure of the given set of edges 
*)
let transitive_closure (edges: ('k * 'k) list): ('k * 'k) list =
   let rec set_transitive_closure (s: ('k * 'k) list): ('k * 'k) list =
      let expand_closure (ss: ('k * 'k) list): ('k * 'k) list =
         ListAsSet.uniq (ListAsSet.union ss
            (List.flatten (List.map (fun (x1, y1) ->
                  List.flatten (List.map (fun (x2, y2) ->
                     if y1 = x2 then [(x1, y2)] else []
                  ) ss)
               ) ss 
         )))
      in
      let t = expand_closure(s) in
      if ListAsSet.seteq t s then t else set_transitive_closure(t)
   in
      set_transitive_closure (ListAsSet.uniq edges)
