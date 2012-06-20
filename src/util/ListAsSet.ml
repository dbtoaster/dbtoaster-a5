(**
   Utility functionality for using Lists as lightweight sets.  Item equality is
   tested for using deep equality: = and <>.
*)

(** 
   Computes the difference between two sets
   @param l1 The first list set
   @param l2 The second list set
   @return   The difference l1 - l2
*)
let diff l1 l2 =
   let eq x y = x = y in
   let f x = if List.exists (eq x) l2 then [] else [x] in
   List.flatten (List.map f l1)

(** 
   Determines if one list is a subset of the other.
   @param l1 The first list set
   @param l2 The second list set
   @return   true if l1 is a subset of l2
*)
let subset l1 l2 = ((diff l1 l2) = [])

(** 
   Compare two sets for equality
   @param l1 The first list set
   @param l2 The second list set
   @return   true if l1 and l2 describe the same set
*)
let seteq l1 l2 = ((subset l1 l2) && (subset l2 l1))

(** 
   Compute the intersection of two sets
   @param l1 The first list set
   @param l2 The second list set
   @return   The set intersection of l1 and l2 
*)
let inter l1 l2 = diff l1 (diff l1 l2)

(** 
   Compute the union of two sets
   @param l1 The first list set
   @param l2 The second list set
   @return   The union of l1 and l2
*)
let union l1 l2 = l1 @ (diff l2 l1)

(** 
   Compute the union of multiple sets
   @param l  A list of list sets
   @return   The union of all sets in l
 *)
let multiunion l = List.fold_left union [] l

(** 
   Compute the intersection of multiple sets
   @param l  A list of list sets
   @return   The intersection of all lists in l
*)
let multiinter l = List.fold_left inter (List.hd l) (List.tl l)

(** 
   Eliminate all duplicates in a list
   @param l  A list
   @return   The set of all distinct elements in l
*)
let no_duplicates l = multiunion (List.map (fun x -> [x]) l)

(** 
   Eliminate all duplicates in a list (identical to no_duplicates)
   @param l  A list
   @return   The set of all distinct elements in l
*)
let uniq = no_duplicates

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
