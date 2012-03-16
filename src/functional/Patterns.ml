(* copied from alpha4 to make the K3 modules compile in alpha 5
TODO add pattern functionality to alpha5 M3 *)

  type var_t = string

	type pattern =
       In of (var_t list * int list)
     | Out of (var_t list * int list)
  
  type pattern_map = (string * pattern list) list
  
  let index l x =
     let pos = fst (List.fold_left (fun (run, cur) y ->
        if run >= 0 then (run, cur) else ((if x=y then cur else run), cur+1))
        (-1, 0) l)
     in if pos = -1 then raise Not_found else pos
  
  let equal_pat a b = match a,b with
    | In(u,v), In(x,y) | Out(u,v), Out(x,y) -> v = y
    | _,_ -> false

  let make_in_pattern dimensions accesses =
     In(accesses, List.map (index dimensions) accesses)
  
  let make_out_pattern dimensions accesses =
     Out(accesses, List.map (index dimensions) accesses)
  
  let get_pattern = function | In(x,y) | Out(x,y) -> y
  
  let get_pattern_vars = function | In(x,y) | Out(x,y) -> x
  
  let empty_pattern_map() = []
  
  let get_filtered_patterns filter_f pm mapn =
     let map_patterns = if List.mem_assoc mapn pm
                        then List.assoc mapn pm else []
     in List.map get_pattern (List.filter filter_f map_patterns)
  
  let get_in_patterns (pm:pattern_map) mapn = get_filtered_patterns
     (function | In _ -> true | _ -> false) pm mapn
  
  let get_out_patterns (pm:pattern_map) mapn = get_filtered_patterns
     (function | Out _ -> true | _ -> false) pm mapn
  
  let get_out_pattern_by_vars (pm:pattern_map) mapn vars = 
    List.hd (get_filtered_patterns
     (function Out(x,y) -> x = vars | _ -> false) pm mapn)
  
  let add_pattern pm (mapn,pat) =
     let existing = if List.mem_assoc mapn pm then List.assoc mapn pm else [] in
     let new_pats = pat::(List.filter (fun x -> not(equal_pat x pat)) existing) in
        (mapn, new_pats)::(List.remove_assoc mapn pm)
  
  let merge_pattern_maps p1 p2 =
     let aux pm (mapn, pats) =
        if List.mem_assoc mapn pm then
           List.fold_left (fun acc p -> add_pattern acc (mapn, p)) pm pats
        else (mapn, pats)::pm
     in List.fold_left aux p1 p2
  
  let singleton_pattern_map (mapn,pat) = [(mapn, [pat])]
  
  let string_of_pattern p =
    let f x y = List.map (fun (a,b) -> a^":"^b)
      (List.combine (List.map string_of_int y) x)
    in match p with
    | In(x,y) -> "in{"^(String.concat "," (f x y))^"}"
    | Out(x,y) -> "out{"^(String.concat "," (f x y))^"}"
      
  let patterns_to_string pm =
     let patlist_to_string pl = List.fold_left (fun acc pat ->
        let pat_str = String.concat "," (
           match pat with | In(x,y) | Out(x,y) ->
              List.map (fun (a,b) -> a^":"^b)
                 (List.combine (List.map string_of_int y) x))
        in
        acc^(if acc = "" then acc else " / ")^pat_str) "" pl
     in
     List.fold_left (fun acc (mapn, pats) ->
        acc^"\n"^mapn^": "^(patlist_to_string pats)) "" pm
