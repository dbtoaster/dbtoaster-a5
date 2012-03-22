(******************* Patterns *******************)

module Patterns =
struct
  type pattern =
       In of (Types.var_t list * int list)
     | Out of (Types.var_t list * int list)
  
  type pattern_map = (string * pattern list) list
  
  let index l x =
     let pos = fst (List.fold_left 
											(fun (run, cur) y ->
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
    let f x y = List.map (fun (a,b) -> a^":"^(Types.string_of_var b))
      (List.combine (List.map string_of_int y) x)
    in match p with
    | In(x,y) -> "in{"^(String.concat "," (f x y))^"}"
    | Out(x,y) -> "out{"^(String.concat "," (f x y))^"}"
      
  let patterns_to_string pm =
     let patlist_to_string pl = List.fold_left (fun acc pat ->
        let pat_str = String.concat "," (
           match pat with | In(x,y) | Out(x,y) ->
              List.map (fun (a,b) -> a^":"^(Types.string_of_var b))
                 (List.combine (List.map string_of_int y) x))
        in
        acc^(if acc = "" then acc else " / ")^pat_str) "" pl
     in
     List.fold_left (fun acc (mapn, pats) ->
        acc^"\n"^mapn^": "^(patlist_to_string pats)) "" pm
end


open Patterns



let extract_patterns (triggers : M3.trigger_t list) : pattern_map =
		let create_pattern_map_from_access mapn theta_vars inv outv =
				let bound_vars vl = ListAsSet.inter vl theta_vars in 
				let valid_pattern vl = List.length (bound_vars vl) < List.length vl in
				List.fold_left
						(fun acc (pattern_f, vl) -> 
	            		add_pattern acc (mapn, pattern_f vl (bound_vars vl))) 
						(empty_pattern_map())
						(List.filter (fun (_,vl) -> valid_pattern vl)
												 [make_in_pattern, inv;make_out_pattern, outv])
		in
					
	  let rec extract_from_calc (theta_vars: Types.var_t list) (calc: Calculus.expr_t) : pattern_map = 
				let sum_pattern_fn _ sum_pats : pattern_map = 
						List.fold_left merge_pattern_maps (empty_pattern_map()) sum_pats 
				in
				let prod_pattern_fn _ prod_pats : pattern_map = 
						List.fold_left merge_pattern_maps (empty_pattern_map()) prod_pats 
				in
				let neg_pattern_fn _ neg_pat : pattern_map = neg_pat
				in
				let leaf_pattern_fn (tvars,_) lf_calc : pattern_map = 
						
						begin match lf_calc with
							| Calculus.Value _ 
							| Calculus.Cmp   _ -> empty_pattern_map()
							| Calculus.AggSum( gb_vars, agg_calc ) -> extract_from_calc tvars agg_calc
							| Calculus.Lift( v, lift_calc ) -> 				extract_from_calc tvars lift_calc
							| Calculus.Rel( reln, relv, _ ) -> 
									create_pattern_map_from_access reln tvars [] relv
							| Calculus.External( mapn, inv, outv, _, None ) -> 
									create_pattern_map_from_access mapn tvars inv outv
							| Calculus.External( mapn, inv, outv, _, Some(init_calc) ) ->
		           		let init_tvars = ListAsSet.union tvars inv  in           
		           		merge_pattern_maps 
											(extract_from_calc init_tvars init_calc)
											(create_pattern_map_from_access mapn tvars inv outv )								
						end
				in
				Calculus.fold ~scope:theta_vars	
											sum_pattern_fn prod_pattern_fn 
											neg_pattern_fn leaf_pattern_fn calc
	
		in  

   	let extract_from_stmt theta_vars (stmt : Plan.stmt_t) : pattern_map =      
				let (lmapn, linv, loutv, ret_type, init_calc_opt) = 
							Plan.expand_ds_name stmt.Plan.target_map 
				in
				let incr_calc = stmt.Plan.update_expr in
				
	      let theta_w_loopinv = ListAsSet.union theta_vars      linv  in
	      let theta_w_lhs     = ListAsSet.union theta_w_loopinv loutv in
	      
	      let init_patterns = begin match init_calc_opt with
					| Some(init_calc) -> extract_from_calc theta_w_lhs init_calc 
					| None            -> empty_pattern_map()
					end
				in
	      let incr_patterns = extract_from_calc theta_w_loopinv incr_calc in
	      let stmt_patterns = create_pattern_map_from_access lmapn theta_vars linv loutv 		in
	      List.fold_left merge_pattern_maps 
											 stmt_patterns
	        						 ([init_patterns; incr_patterns])
	  in

	  let extract_from_trigger (trig : M3.trigger_t) : pattern_map =
				let trig_args = Schema.event_vars trig.M3.event	in
				List.fold_left merge_pattern_maps 
											 (empty_pattern_map()) 
											 (List.map (extract_from_stmt trig_args) !(trig.M3.statements))   
		in
   	List.fold_left merge_pattern_maps 
									(empty_pattern_map()) 
									(List.map extract_from_trigger triggers)
	
	

	