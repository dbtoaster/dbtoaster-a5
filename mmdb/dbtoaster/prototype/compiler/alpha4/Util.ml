module StringMap = Map.Make(String)
module StringSet = Set.Make(String)

(* Set operations on lists. We do not enforce that the input lists
   have no duplicates. Goal: I want to see the lists for easier debugging,
   otherwise I would instantiate Set (which would be abstract). *)
module ListAsSet =
struct
   (* computes the difference l1 - l2 *)
   let diff l1 l2 =
      let eq x y = x = y in
      let f x = if List.exists (eq x) l2 then [] else [x] in
      List.flatten(List.map f l1)

   let subset l1 l2 = ((diff l1 l2) = [])     (* is l1 a subset of l2 ? *)
   let inter l1 l2 = diff l1 (diff l1 l2)               (* intersection *)
   let union l1 l2 = l1 @ (diff l2 l1)
(* alternative impl
   let union l1 l2 = l1@(List.filter (fun k -> (not (List.mem k l1))) l2)
*)

   (* eliminates duplicates if the element lists of l are duplicate free. *)
   let multiunion l = List.fold_left union [] l

   let multiinter l = List.fold_left inter (List.hd l) (List.tl l)

   (* not the most intelligent of implementations. quadratic time *)
   let no_duplicates l = multiunion (List.map (fun x -> [x]) l)

   let member a l = (subset [a] l)         (* is "a" a member of set l? *)

   (* computes all subsets of r that have exactly k elements.
      does not produce duplicate sets if r does not have duplicates.
      sorting -- e.g. (List.sort Pervasives.compare l) --
      improves readability of the result. *)
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


   (* distributes a list of lists, e.g.

      distribute [[[1;2]; [2;3]]; [[3;4]; [4;5]]; [[5;6]]];;

      results in

      [[[1; 2]; [3; 4]; [5; 6]]; [[2; 3]; [3; 4]; [5; 6]];
       [[1; 2]; [4; 5]; [5; 6]]; [[2; 3]; [4; 5]; [5; 6]]]
   *)
   let rec distribute (l: 'a list list) =
      if l = [] then [[]]
      else
         let f tail =
            let g x = [x] @ tail in
            List.map (g) (List.hd l)
         in
         List.flatten (List.map f (distribute (List.tl l)));;
    
end;;

module MapAsSet =
struct
  let union (merge:(string -> 'a -> 'a -> 'a)) 
            (map_a:'a StringMap.t)
            (map_b:'a StringMap.t): 'a StringMap.t =
    StringMap.fold (fun k v accum -> 
      if StringMap.mem k accum then
        StringMap.add k (merge k (StringMap.find k accum) v) accum
      else 
        StringMap.add k v accum
    ) map_a map_b

  let left_priority k vl vr = vl
  let right_priority k vl vr = vr
  
  let union_right a b: 'a StringMap.t = 
    (union right_priority a b)
  
  let singleton k v = StringMap.add k v StringMap.empty
  
end;;


(*
ListAsSet.subsets_of_size 3 [2;1;4;3] =
   [[1; 2; 3]; [1; 2; 4]; [1; 3; 4]; [2; 3; 4]];;

ListAsSet.subsets_of_size 5 [1;2;3;4] = [];;
*)



module HyperGraph =
struct
(* compute the connected components of the hypergraph where the nodes
   in guard_set may be ignored.
   factorizes a MultiNatJoin of leaves given in list form.

   The top-level result list thus conceptually is a MultiProduct.
*)
let rec connected_components (get_nodes: 'edge_t -> 'node_t list)
                             (hypergraph: 'edge_t list): ('edge_t list list)
   =
   let rec complete_component c g =
      if (g = []) then c
      else if (c = []) then c
      else
         let relevant_set = (List.flatten (List.map get_nodes c)) in
         let neighbor e = ((ListAsSet.inter relevant_set (get_nodes e)) != [])
         in
         let newset = (List.filter neighbor g) in
         c @ (complete_component newset (ListAsSet.diff g newset)) in
   if hypergraph = [] then []
   else
      let c = complete_component [List.hd hypergraph] (List.tl hypergraph) in
      [c] @ (connected_components get_nodes (ListAsSet.diff hypergraph c))
end



module MixedHyperGraph =
struct
   type ('a, 'b) edge_t = AEdge of 'a | BEdge of 'b
   type ('a, 'b) hypergraph_t = (('a, 'b) edge_t) list

   let make (alist: 'a list) (blist: 'b list): ('a, 'b) hypergraph_t =
        (List.map (fun x -> AEdge x) alist)
      @ (List.map (fun x -> BEdge x) blist)

   let connected_components (a_get_nodes: 'a -> 'node_t)
                            (b_get_nodes: 'b -> 'node_t)
                            (hypergraph: ('a, 'b) hypergraph_t):
                            ((('a, 'b) hypergraph_t) list) =
      let get_nodes hyperedge =
         match hyperedge with AEdge(a) -> a_get_nodes a
                            | BEdge(b) -> b_get_nodes b
      in
      HyperGraph.connected_components get_nodes hypergraph

   let extract_atoms (component: ('a, 'b) hypergraph_t):
                                 (('a list) * ('b list)) =
      let extract_atom x = match x with AEdge(a) -> ([a], [])
                                      | BEdge(b) -> ([], [b])
      in
      let (a_ll, b_ll) = (List.split (List.map extract_atom component))
      in
      (List.flatten a_ll, List.flatten b_ll)
end



let string_of_list0 (sep: string) (elem_to_string: 'a -> string)
                    (l: 'a list): string =
   if (l = []) then ""
   else List.fold_left (fun x y -> x^sep^(elem_to_string y))
                       (elem_to_string (List.hd l))
                       (List.tl l)

(* (string_of_list ", " ["a"; "b"; "c"] = "a, b, c". *)
let string_of_list (sep: string) (l: string list): string =
   string_of_list0 sep (fun x->x) l

let list_to_string (elem_to_string: 'a -> string) (l: 'a list) : string =
   "["^(string_of_list0 "; " elem_to_string l)^" ]"



module Function =
struct
   type ('a, 'b) table_fn_t = ('a * 'b) list

   exception NonFunctionalMappingException

   (* for partial functions with a default value to be returned if x is
      not in the domain of theta. *)
   let apply (theta: ('a, 'b) table_fn_t)
             (default: 'b) (x: 'a): 'b =
      let g (y, z) = if(x = y) then [z] else []
      in
      let x2 = List.flatten (List.map g theta)
      in
      if (List.length x2) = 0 then default
      else if (List.length x2) = 1 then (List.hd x2)
      else raise NonFunctionalMappingException

   let apply_strict (theta: ('a, 'b) table_fn_t) (x: 'a): 'b =
      let g (y, z) = if(x = y) then [z] else []
      in
      let x2 = List.flatten (List.map g theta)
      in
      if (List.length x2) = 1 then (List.hd x2)
      else raise NonFunctionalMappingException


   let dom (theta: ('a, 'b) table_fn_t): 'a list = fst (List.split theta)
   let img (theta: ('a, 'b) table_fn_t): 'a list = snd (List.split theta)

   let functional (theta: ('a, 'b) table_fn_t): bool =
      let d = dom theta in
      ((ListAsSet.no_duplicates d) = d)

   let identities (theta: ('a, 'a) table_fn_t): ('a list) =
      let f (x,y) = if (x=y) then [x] else [] in
      List.flatten (List.map f theta)

   let intransitive (theta: ('a, 'a) table_fn_t): bool =
      ((ListAsSet.diff (ListAsSet.inter (dom theta) (img theta))
                       (identities theta)) = [])

   (* only for functions from strings to strings *)
   let string_of_string_fn theta: string =
      "{" ^(string_of_list ", " (List.map (fun (x,y) -> x^"->"^y) theta))^"}"
end



module Vars =
struct
   (* the input is a list of pairs of equated variables.
      The result is a list of lists of variables and in general contains
      duplicates
   *)
   let equivalence_classes (eqs: ('v * 'v) list) =
      List.map ListAsSet.multiunion
         (HyperGraph.connected_components (fun x -> x)
             (List.map (fun (x,y) -> [x;y]) eqs))

   let closure (equalities: ('v * 'v) list) (vars: 'v list): ('v list) =
      ListAsSet.union vars
         (List.flatten (List.filter (fun x -> (ListAsSet.inter x vars) != [])
                                    (equivalence_classes equalities)))

   type 'v mapping_t = ('v * 'v) list

   (* given a list of variables to be unified (i.e., a single variable from
      this list is to be chosen by which all others will be replaced),
       a variable that occurs in list parameter_vars, and
      output a mapping from the variables to the chosen variable. *)
   let unifier0 (vars_to_unify: 'v list)   (* an equivalence class *)
               (parameter_vars: 'v list):  (* the unifier must be the identity
                                              on these variables *)
               ('v mapping_t * (('v * 'v) list)) =
      let bv = ListAsSet.inter vars_to_unify parameter_vars
      in
      let incons =
         (
         if (List.length bv > 1) then
            List.map (fun x -> ((List.hd bv), x)) (List.tl bv)
            (* This means we are not allowed to unify; we must
               keep a constraint around checking that the elements
               of bv are the same. *)
         else []
         )
      in 
      let v = if (bv = []) then (List.hd vars_to_unify) else (List.hd bv)
      in
      (List.flatten (List.map (fun x -> [(x,v)]) vars_to_unify), incons)

   (* returns a most general unifier of the variables occuring in the
      equations, and respecting those unless they are inconsistent
      with a given set of required identities. Returns the unifier and
      a fully descriptive set of inconsistent equations. For example,

      (unifier [("x", "y"); ("y", "z")] ["z"]) =
      ([("x", "z"); ("y", "z")], [])

      (unifier [("x", "y"); ("y", "z")] ["y"; "z"]) =
      ([("x", "y"); ("y", "y"); ("z", "y")], [("y", "z")])

      Note the ("z", "y") tuple. This is inconsistent with the requirement
      that the mapping is the identity on z. However, in those cases
      where the inconsistent equations hold (here, when the variable y and
      z have the same value), the mapping is ok. This is the way to use
      inconsistent equations; they are additional requirements to be
      checked.
   *)
   let unifier (equations: ('v * 'v) list) (identities: 'v list):
               ('v mapping_t * (('v * 'v) list)) =
      let (x, y) = (List.split
                      (List.map (fun comp -> (unifier0 comp identities))
                                (equivalence_classes equations)))
      in
      (List.flatten x, List.flatten y)

   (* mapping is a partial function. returns mapping(x) if mapping is defined
      on x, otherwise x. *)
   let apply_mapping (mapping: 'v mapping_t) (x: 'v): 'v =
      Function.apply mapping x x
end


(*
let b = [("A", "x"); ("B", "y"); ("A", "z"); ("C", "u"); ("C", "x");
         ("B", "v")];;

Vars.equivalence_classes b =
   [["A"; "x"; "A"; "z"; "C"; "x"; "C"; "u"]; ["B"; "y"; "B"; "v"]];;

Vars.closure [("x", "y"); ("u", "v"); ("v", "y")] ["x"] =
   ["x"; "y"; "v"; "u"];;
*)



(* turn a list into a list of pairs of values and their positions in the list.
   Examle:
   add_positions ["a"; "b"; "c"] 1 = [("a", 1); ("b", 2); ("c", 3)];;
*)
let rec add_positions (l: 'a list) i =
   if (l = []) then []
   else ((List.hd l), i) :: (add_positions (List.tl l) (i+1))

(* add_names "foo" ["a"; "b"] = [("foo1", "a"), ("foo2", "b")] *)
let add_names (name_prefix: string) (l: 'a list): ((string * 'a) list) =
   let add_name (x, i) = (name_prefix^(string_of_int i), x)
   in
   List.map add_name (add_positions l 1)


(* creates all k-tuples of elements of the list src.
   Example: k_tuples 3 [1;2] =
            [[1; 1; 1]; [2; 1; 1]; [1; 2; 1]; [2; 2; 1]; [1; 1; 2];
             [2; 1; 2]; [1; 2; 2]; [2; 2; 2]]
*)
let rec k_tuples k (src: 'a list) : 'a list list =
   if (k <= 0) then [[]]
   else List.flatten (List.map (fun t -> List.map (fun x -> x::t) src)
                               (k_tuples (k-1) src))


