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
let eq_fn = fun x y -> x = y

let diff ?(eq=eq_fn) l1 l2 =
   let f x = if List.exists (eq x) l2 then [] else [x] in
   List.flatten (List.map f l1)

(** 
   Determines if one list is a subset of the other.
   @param l1 The first list set
   @param l2 The second list set
   @return   true if l1 is a subset of l2
*)
let subset ?(eq=eq_fn) l1 l2 = ((diff ~eq:eq l1 l2) = [])

(** 
   Determines if one list is a proper subset of the other.
   @param l1 The first list set
   @param l2 The second list set
   @return   true if l1 is a proper subset of l2
*)
let proper_subset ?(eq=eq_fn) l1 l2 = 
   ((subset ~eq:eq l1 l2) && not (subset ~eq:eq l2 l1))

(** 
   Compare two sets for equality
   @param l1 The first list set
   @param l2 The second list set
   @return   true if l1 and l2 describe the same set
*)
let seteq ?(eq=eq_fn) l1 l2 = ((subset ~eq:eq l1 l2) && (subset ~eq:eq l2 l1))

(** 
   Compute the intersection of two sets
   @param l1 The first list set
   @param l2 The second list set
   @return   The set intersection of l1 and l2 
*)
let inter ?(eq=eq_fn) l1 l2 = diff ~eq:eq l1 (diff ~eq:eq l1 l2)

(** 
   Compute the union of two sets
   @param l1 The first list set
   @param l2 The second list set
   @return   The union of l1 and l2
*)
let union ?(eq=eq_fn) l1 l2 = l1 @ (diff ~eq:eq l2 l1)

(** 
   Compute the union of multiple sets
   @param l  A list of list sets
   @return   The union of all sets in l
 *)
let multiunion ?(eq=eq_fn) l = List.fold_left (union ~eq:eq) [] l

(** 
   Compute the intersection of multiple sets
   @param l  A list of list sets
   @return   The intersection of all lists in l
*)
let multiinter ?(eq=eq_fn) l = 
   List.fold_left (inter ~eq:eq) (List.hd l) (List.tl l)

(** 
   Eliminate all duplicates in a list
   @param l  A list
   @return   The set of all distinct elements in l
*)
let no_duplicates ?(eq=eq_fn) l = multiunion ~eq:eq (List.map (fun x -> [x]) l)

(** 
   Eliminate all duplicates in a list (identical to no_duplicates)
   @param l  A list
   @return   The set of all distinct elements in l
*)
let uniq ?(eq=eq_fn) = no_duplicates ~eq:eq

(** 
   Check if a list has no duplicates. 
   @param l  A list
   @return   true if all elements in l are distinct
*)
let has_no_duplicates ?(eq=eq_fn) l = (no_duplicates ~eq:eq l) = l
