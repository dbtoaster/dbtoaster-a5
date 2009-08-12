open Algebra

exception AnalysisException of string

type dependency_identifier = [
| `Variable of code_variable
| `MapAccess of map_key
| `Map of map_identifier
| `Domain of relation_identifier ]

let string_of_dependency_identifier d_id =
    match d_id with
        | `Variable cv -> cv
        | `MapAccess mk -> string_of_map_key mk
        | `Map n -> n
        | `Domain n -> n

let unique l =
    List.fold_left
        (fun acc x -> if List.mem x acc then acc else acc@[x]) [] l

(* arith_code_expression -> vars or maps used *)
let rec get_arith_dependencies ac_expr =
    let binary_fn l r =
        (get_arith_dependencies l)@(get_arith_dependencies r)
    in
        match ac_expr with
            | `CTerm (`Variable v) -> [`Variable v]
            | `CTerm (`MapAccess mk) -> [`MapAccess mk]
            | `CTerm _ -> []
            | `Sum (l,r) | `Minus (l,r) | `Product (l,r)
            | `Min (l,r) | `Max (l,r)
                -> binary_fn l r

let rec get_term_usage bc_expr =
    let binary_fn l r =
        let lterms = get_arith_dependencies l in
            lterms@
                (List.filter
                    (fun x -> not(List.mem x lterms))
                    (get_arith_dependencies r))
    in
        match bc_expr with
            | `BCTerm bct_expr ->
                  begin
                      match bct_expr with
                          | `True | `False -> []
                          | `EQ (l,r) | `NE (l,r)
                          | `LT (l,r) | `LE (l,r)
                          | `GT (l,r) | `GE (l,r)
                                -> binary_fn l r
                  end
                      
            | `Not b -> get_term_usage b
            | `And (l,r) | `Or (l,r) -> (get_term_usage l)@(get_term_usage r)


(* code_expression -> child_deps *)
let rec get_dependencies c_expr =
    match c_expr with
        | `Declare _ -> []
        | `Assign (v, a) ->
              let ch_used = get_arith_dependencies a in
                  unique (List.map (fun u -> (`Variable v,u)) ch_used)

        | `AssignMap (mk, a)
        | `EraseMap (mk, a) ->
              let ch_used = get_arith_dependencies a in
                  unique (List.map (fun u -> (`MapAccess mk,u)) ch_used)

        | `InsertTuple (ds, cvl)
        | `DeleteTuple (ds, cvl) ->
              begin
                  match ds with
                      | `Map (mid,_,_) ->
                            unique(List.map (fun v -> (`Map mid, `Variable v)) cvl)

                      | `Set (n, _) | `Multiset (n,_) ->
                            unique(List.map (fun v -> (`Domain n, `Variable v)) cvl)
              end

        | `Eval a -> []

        | `IfNoElse (b,c) ->
              let ch_deps = get_dependencies c in
              let pred_used = get_term_usage b in
              let new_deps =
                  List.filter
                      (fun d -> not(List.mem d ch_deps))
                      (List.fold_left
                          (fun acc (t,s) -> acc@(List.map (fun ns -> (t,ns)) pred_used))
                          [] ch_deps)
              in
                  ch_deps@new_deps

        | `IfElse (b,l,r) ->
              let lch_deps = get_dependencies l in
              let rch_deps = get_dependencies r in
              let ch_deps = lch_deps@(List.filter (fun d -> not(List.mem d lch_deps)) rch_deps) in
              let pred_used = get_term_usage b in
              let new_deps =
                  List.filter
                      (fun d -> not(List.mem d ch_deps))
                      (List.fold_left
                          (fun acc (t,s) -> acc@(List.map (fun ns -> (t,ns)) pred_used))
                          [] ch_deps)
              in
                  ch_deps@new_deps

        | `ForEach (ds,c) ->
              begin
                  let ch_deps = get_dependencies c in
                  let ds_deps =
                      match ds with
                          | `Map (id,f,_) ->
                                List.map (fun (fid,_) -> (`Variable fid, `Map id)) f 

                          | `Set (id,f) | `Multiset (id,f) ->
                                List.map (fun (fid,_) -> (`Variable fid, `Domain id)) f 
                  in
                      unique(ds_deps@ch_deps)
              end

        | `Block cl -> unique(List.flatten (List.map get_dependencies cl))

        | `Return _ -> []

        | `Handler (_,_,_,cl) -> unique(List.flatten (List.map get_dependencies cl))

        | `Profile (_,_,c) -> get_dependencies c


let build_dependency_graph dependencies =
    let add_node n nodes = if List.mem n nodes then nodes else nodes@[n] in
    let add_parent_node d nodes =
        match d with
            | `Variable v -> add_node v nodes
            | `MapAccess (mid, f) ->
                  add_node (string_of_map_key (mid,f)) (add_node mid nodes)
            | `Map id -> add_node id nodes
            | `Domain id -> add_node id nodes
    in
    let add_child_node d nodes =
        match d with
            | `Variable v -> add_node v nodes
            | `MapAccess (mid, f) -> nodes
            | `Map id -> add_node id nodes
            | `Domain id -> add_node id nodes
    in
    let get_node d nodes =
        let check n =
            if List.mem n nodes then n
            else raise (AnalysisException ("No node found: "^n))
        in
        match d with
            | `Variable v -> check v
            | `MapAccess (mid, f) -> check (string_of_map_key (mid, f))
            | `Map id -> check id
            | `Domain id -> check id
    in
    let add_edge src dest edges =
        if List.mem (src,dest) edges then edges else edges@[(src, dest)]
    in
        List.fold_left
            (fun (node_acc, edge_acc) (t,s) ->
                let new_node_acc = add_child_node s (add_parent_node t node_acc) in
                let gn d = get_node d new_node_acc in
                let new_edge_acc =
                    match (t,s) with
                        | (`MapAccess (mid,f), _) ->
                              add_edge (gn t) (gn s)
                                  (List.fold_left
                                      (fun acc v -> add_edge (gn t) (gn (`Variable v)) acc)
                                      (add_edge (gn (`Map mid)) (gn t) edge_acc) f)

                        | _ -> add_edge (gn t) (gn s) edge_acc
                in
                    (new_node_acc, new_edge_acc))
            ([], []) dependencies
