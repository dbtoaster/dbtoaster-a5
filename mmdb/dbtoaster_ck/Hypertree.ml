
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
      let union l1 l2 = l1 @ l2
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



ListAsSet.subsets_of_size 3 [2;1;4;3];;
(* result: [[1; 2; 3]; [1; 2; 4]; [1; 3; 4]; [2; 3; 4]] *)

ListAsSet.subsets_of_size 5 [1;2;3;4];;    (* result: [] *)







(* returns (f i) for the smallest i <= k for which (f i) != [], if such an
   i exists; else returns [].
*)
let run_upto f k =
   let rec g i =
      let result = f i in
      if ((i < k) && (result = [])) then g (i+1) else result in
   g 1;;


(* iterate through list l; try f on each element; break on first element x
   for which (f x) != [].
*)
let rec sat_once f l =
   if l = [] then []
   else
      let result = f (List.hd l) in
      if (result != []) then [((List.hd l), result)]
      else sat_once f (List.tl l);;


type 'node hyperedge = 'node list;;
type 'node hypergraph = 'node hyperedge list;;
type 'node hypertree = Node of 'node hyperedge list * 'node hypertree list;;



(* compute the connected components of the hypergraph where the nodes
   in guard_set may be ignored.
*)
let rec connected_components hypergraph guard_set =
   let rec complete_component c g =
      if (g = []) then c
      else if (c = []) then c
      else
         let relevant_set = (ListAsSet.diff (List.flatten c) guard_set) in
         let neighbor e = ((ListAsSet.inter relevant_set e) != []) in
         let newset = (List.filter neighbor g) in
         c @ (complete_component newset (ListAsSet.diff g newset)) in
   if hypergraph = [] then []
   else
      let c = complete_component [List.hd hypergraph] (List.tl hypergraph) in
      [c] @ (connected_components (ListAsSet.diff hypergraph c) guard_set);;


connected_components [[1;2;3]; [2;4]; [4;5]; [5;3]; [5;2;3]; [7;8]; [8;9]]
   [3;4;2];;




let satisfies_connectedness hypergraph cut parent_guardset =
   (ListAsSet.subset (ListAsSet.inter (List.flatten hypergraph)
                                      parent_guardset)
                     (List.flatten cut));;


let decompose hgraph cut parent_guardset =
   let rest = (ListAsSet.diff hgraph cut) in
   if (satisfies_connectedness rest cut parent_guardset) then
      (connected_components rest (List.flatten cut))
   else [];;


(* if only all of hgraph is a valid cut, we do not return it even
   if its cardinality does not exceed cutsize
*)
let find_cut_of_size_and_decompose cutsize hgraph parent_guardset =
   let g earset_size =
      let has_small_ear s = (List.length s) <= earset_size in
      let f cut =
         let v = decompose hgraph cut parent_guardset in
         if (List.filter (has_small_ear) v) != [] then v
         else [] in
      sat_once (f) (ListAsSet.subsets_of_size cutsize hgraph) in
   run_upto g cutsize;;

(* prefer small branches; this is not a strategy
   for creating balanced trees. *)




let find_cut_and_decompose max_cutsize hgraph parent_guardset =
   if hgraph = [] then []
   else
      let f cutsize =
         find_cut_of_size_and_decompose cutsize hgraph parent_guardset in
      let result = run_upto f max_cutsize in
      if result != [] then result
      else [(hgraph, [])];; (* cannot decompose *)


let rec compute_hypertree_decomposition hypergraph dependencies =
   let f h g = compute_hypertree_decomposition g (List.flatten h) in
   let mknode (h, t) = Node(h, (List.map (f h) t)) in
   mknode (List.hd (find_cut_and_decompose 20 hypergraph dependencies));;
   

compute_hypertree_decomposition [[2; 4]; [4; 5]; [5; 3]; [2; 3; 5]; [1;2;3]]
  [];;


(* from Gottlob, Leone, Scarcello slides *)
compute_hypertree_decomposition
    [[1; 2; 3; 4; 6]; [1; 8; 9; 5; 7]; [4; 5; 10]; [2; 10];
     [8; 10]; [6; 7; 11]; [3; 11]; [9; 11]; [12; 2; 8; 3; 9]; [13; 3; 6];
     [14; 3; 6]] [];;


compute_hypertree_decomposition
[[1; 2; 3; 4; 6]; [1; 8; 9; 5; 7]; [4; 5; 10]; [2; 10];
[8; 10]; [6; 7; 11]; [3; 11]; [9; 11]; [13; 3; 6];
  [14; 3; 6]] [12; 2; 8; 3; 9];;


compute_hypertree_decomposition
[[1; 2; 3; 4; 6]; [1; 8; 9; 5; 7]; [4; 5; 10]; [2; 10];
[8; 10]; [6; 7; 11]; [3; 11]; [9; 11]; [12; 2; 8; 3; 9]] [];;


