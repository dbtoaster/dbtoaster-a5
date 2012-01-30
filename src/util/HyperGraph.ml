(* compute the connected components of the hypergraph where the nodes
   in guard_set may be ignored.
   factorizes a MultiNatJoin of leaves given in list form.

   The top-level result list thus conceptually is a MultiProduct.
   
   connected_unique_components treats the edge list as a set; Each edge appears 
   exactly once in the returned component list.  
   connected_components treats the edge list as a bag.
   
   Edges in any individual returned edge list are not guaranteed to be in the
   same order that they were first received in.  HOWEVER, two edges will never
   be reordered (with respect to their initial order) unless:
      The two edges are part of separate, disconnected components in the 
      subgraph consisting of all edges to the left of (and including) the 
      rightmost of the two edges in the input edge list.
   
   This guarantee is used by DBToaster.CalculusDecomposition
*)
let rec connected_unique_components (get_nodes: 'edge_t -> 'node_t list)
                                    (hypergraph: 'edge_t list): 
                                       ('edge_t list list) =
   let rec complete_component c g =
      if (g = []) then c
      else if (c = []) then c
      else
         let relevant_set = (List.flatten (List.map get_nodes c)) in
         let neighbor e = ((ListAsSet.inter relevant_set (get_nodes e)) !=[])
         in
         let newset = (List.filter neighbor g) in
         c @ (complete_component newset (ListAsSet.diff g newset)) in
   if hypergraph = [] then []
   else
      let c = complete_component [List.hd hypergraph] (List.tl hypergraph) in
      [c] @ (connected_unique_components get_nodes
                                         (ListAsSet.diff hypergraph c))

let connected_components (get_nodes: 'edge_t -> 'node_t list)
                         (hypergraph: 'edge_t list): 
                            ('edge_t list list) =
   List.map (fun factor -> List.map snd factor)
      (connected_unique_components
         (fun (_,x) -> get_nodes x)
         (snd (List.fold_right 
            (fun x (i,old) -> (i+1, (i, x)::old)) hypergraph (0,[])))
      )