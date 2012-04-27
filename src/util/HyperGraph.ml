(** 
   Tools for working with HyperGraphs, and in particular identifying connected
   components

 compute the connected components of the hypergraph where the nodes
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

(**
   Compute the {b set} of connected edges in the provided hypergraph
   @param get_nodes  A function for obtaining the nodes that each edge is 
                     connected to
   @param hypergraph The list of edges in the hypergraph
   @return           A list of list {b sets}, describing each edge in the
                     hypergraph, with duplicates eliminated.
*)
let rec connected_unique_components (get_nodes: 'edge_t -> 'node_t list)
                                    (hypergraph: 'edge_t list): 
                                       ('edge_t list list) =
   if hypergraph = [] then []
   else
   (* Given a horizon of (connected) nodes and a set of edges, find the full set
      of nodes connected to at least one node in the horizon *)
   let rec expand_node_horizon horizon graph_edge_nodes = 
      let horizon_extensions =
         List.filter (fun edge_nodes -> 
                        (ListAsSet.inter horizon edge_nodes) <> [])
                     graph_edge_nodes
      in
         (* Base case, if we're not expanding the horizon then we're done *)
         if horizon_extensions = [] then horizon
         (* Otherwise, recur with a horizon containing all extensions, and a
            graph with those nodes deleted *)
         else expand_node_horizon (ListAsSet.multiunion ([horizon]@
                                                         horizon_extensions))
                                  (ListAsSet.diff graph_edge_nodes
                                                  horizon_extensions)
   in
   let first_component_nodes = 
      expand_node_horizon (get_nodes (List.hd hypergraph))
                          (List.map get_nodes (List.tl hypergraph))
   in
   let (first_component,rest) =
      (List.partition (fun edge -> 
         (ListAsSet.inter first_component_nodes (get_nodes edge)) <> [])
         (List.tl hypergraph))
   in
      (  ((List.hd hypergraph)::first_component)
         ::(connected_unique_components get_nodes rest))
;;
(**
   Compute the {b bag} of connected edges in teh provided hypergraph
   @param get_nodes  A function for obtaining the nodes that each edge is 
                     connected to
   @param hypergraph The list of edges in the hypergraph
   @return           A list of list {b bags}, describing each edge in the
                     hypergraph, without duplicates eliminated.
*)
let connected_components (get_nodes: 'edge_t -> 'node_t list)
                         (hypergraph: 'edge_t list): 
                            ('edge_t list list) =
   List.map (fun factor -> List.map snd factor)
      (connected_unique_components
         (fun (_,x) -> get_nodes x)
         (snd (List.fold_right 
            (fun x (i,old) -> (i+1, (i, x)::old)) hypergraph (0,[])))
      )