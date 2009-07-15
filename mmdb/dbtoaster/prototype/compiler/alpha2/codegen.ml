open Algebra

let rec create_code_expression expr =
    match expr with
	| `ETerm (x) ->
	      begin
		  `CTerm(
		      match x with
			  | `Int y -> `Int y
			  | `Float y -> `Float y
			  | `String y -> `String y
			  | `Long y -> `Long y
			  | `Variable y -> `Variable y
			  | `Attribute y ->
				begin
				    match y with |`Qualified(_,f) | `Unqualified f -> `Variable(f)
				end) 
	      end
	| `UnaryMinus (e) -> raise InvalidExpression
	| `Sum (l,r) -> `Sum(create_code_expression l, create_code_expression r)
	| `Product (l,r)  -> `Product(create_code_expression l, create_code_expression r)
	| `Minus (l,r) -> `Minus(create_code_expression l, create_code_expression r)
	| `Divide (l,r) -> raise InvalidExpression
	| `Function (fid, args) -> raise InvalidExpression

let rec create_code_predicate b_expr =
    match b_expr with
	| `BTerm(x) ->
	      begin
		  `BCTerm(
		      match x with
			  | `MEQ(_) -> `EQ(`CTerm(`Variable("dummy")), `CTerm(`Int 0))
			  | `MLT(_) -> `LT(`CTerm(`Variable("dummy")), `CTerm(`Int 0))

			  (*
			    | `MEQ(_)
			    | `MLT(_) -> raise InvalidExpression
			  *)
				
			  | `LT(l,r) -> `LT(create_code_expression l, create_code_expression r)
			  | `LE(l,r) -> `LE(create_code_expression l, create_code_expression r)
			  | `GT(l,r) -> `GT(create_code_expression l, create_code_expression r)
			  | `GE(l,r) -> `GE(create_code_expression l, create_code_expression r)
			  | `EQ(l,r) -> `EQ(create_code_expression l, create_code_expression r)
			  | `NE(l,r) -> `NE(create_code_expression l, create_code_expression r)
			  | `True -> `True
			  | `False -> `False)
	      end
	| `And(l,r) -> `And(create_code_predicate l, create_code_predicate r)
	| `Or(l,r) -> `Or(create_code_predicate l, create_code_predicate r)
	| `Not(e) -> `Not(create_code_predicate e)

let rec substitute_arith_code_vars assignments ac_expr =
    let sa = substitute_arith_code_vars assignments in
    let get_var_matches v = 
	List.filter
	    (fun a -> match a with
		| `Assign(y,_) -> v = y
		| _ -> raise InvalidExpression)
	    assignments
    in
    let substitute_cv_list vl = 
	List.map
	    (fun v ->
		let v_matches = get_var_matches v in
		    begin match v_matches with
			| [] -> v
			| [`Assign(_,`CTerm(`Variable(z)))] -> z
			| _ -> raise DuplicateException
		    end)
	    vl
    in
	match ac_expr with
	    | `CTerm(`Variable(x)) ->
		  let x_matches = get_var_matches x in
		      begin match x_matches with
			  | [] -> ac_expr
			  | [`Assign(_,z)] -> z
			  | _ -> raise DuplicateException
		      end

	    | `CTerm(`MapAccess(mid,kf)) -> `CTerm(`MapAccess(mid, substitute_cv_list kf))
	    | `CTerm(`MapContains(mid, kf)) -> `CTerm(`MapContains(mid, substitute_cv_list kf))

	    | `CTerm(_) -> ac_expr

	    | `Sum (l,r) -> `Sum(sa l, sa r) 
	    | `Minus (l,r) -> `Minus(sa l, sa r) 
	    | `Product (l,r) -> `Product(sa l, sa r) 
	    | `Min (l,r) -> `Min(sa l, sa r)
	    | `Max (l,r) -> `Max(sa l, sa r)



let rec substitute_bool_code_vars assignments bc_expr =
    let sa = substitute_arith_code_vars assignments in
    let sb = substitute_bool_code_vars assignments in
	match bc_expr with
	    | `BCTerm (x) -> `BCTerm(
		  begin match x with
		      | `True -> `True | `False -> `False
		      | `LT (l,r) -> `LT(sa l, sa r)
		      | `LE (l,r) -> `LE(sa l, sa r)
		      | `GT (l,r) -> `GT(sa l, sa r)
		      | `GE (l,r) -> `GE(sa l, sa r)
		      | `EQ (l,r) -> `EQ(sa l, sa r)
		      | `NE (l,r) -> `NE(sa l, sa r)
		  end)

	    | `Not (e) -> `Not(sb e)
	    | `And (l,r) -> `And(sb l, sb r)
	    | `Or (l,r) -> `Or (sb l, sb r)

let rec substitute_code_vars assignments c_expr =
    let sa = substitute_arith_code_vars assignments in
    let sb = substitute_bool_code_vars assignments in
    let sc = substitute_code_vars assignments in
    let get_var_matches v = 
	List.filter
	    (fun a -> match a with
		| `Assign(y,_) -> v = y
		| _ -> raise InvalidExpression)
	    assignments
    in
    let substitute_cv_list vl = 
	List.map
	    (fun v ->
		let v_matches = get_var_matches v in
		    begin match v_matches with
			| [] -> v
			| [`Assign(_,`CTerm(`Variable(z)))] -> z
			| _ -> raise DuplicateException
		    end)
	    vl
    in
	match c_expr with
	    | `Declare d ->
		  begin
		      match d with
			  | `Variable (n,f) ->
				let n_is_asgn =
				    List.exists
					(fun a -> match a with
					    | `Assign(x,_) -> x = n
					    | _ -> raise InvalidExpression)
					assignments
				in
				    if n_is_asgn then
                                        begin
                                            print_endline ("Attempted to substitute assigned variable "^n);
                                            print_endline (indented_string_of_code_expression c_expr);
                                            raise (CodegenException
                                                ("Attempted to substitute assigned variable "^n))
                                        end
				    else `Declare(d)

			  | _ -> `Declare(d)
		  end

	    | `Assign(x, c) ->
		  if not(List.mem c_expr assignments) then
		      `Assign(x, sa c)
		  else c_expr
		      
	    | `AssignMap((mid, kf), c) ->
		  `AssignMap((mid, substitute_cv_list kf), sa c)

	    | `EraseMap((mid, kf), c) ->
		  `EraseMap((mid, substitute_cv_list kf), sa c)

            | `InsertTuple(ds, cvl) -> `InsertTuple(ds, substitute_cv_list cvl)
            | `DeleteTuple(ds, cvl) -> `DeleteTuple(ds, substitute_cv_list cvl)

	    | `IfNoElse(p, c) -> `IfNoElse(sb p, sc c)
	    | `IfElse(p, l, r) -> `IfElse(sb p, sc l, sc r)
	    | `ForEach(ds, c) -> `ForEach(ds, sc c)
	    | `Eval x -> `Eval (sa x)
	    | `Block cl -> `Block(List.map sc cl)
	    | `Return x -> `Return (sa x)
	    | `Handler(n, args, rt, cl) -> `Handler(n, args, rt, List.map sc cl)

let rec merge_block_code cl acc =
    match cl with
	| [] -> acc
	| (`Block a)::((`Block b)::t) -> merge_block_code ((`Block(a@b))::t) acc
	| h::t -> merge_block_code t (acc@[h])

let rec simplify_code c_expr =

      print_endline "Simplify code:";
      print_endline (indented_string_of_code_expression c_expr);
      print_endline (String.make 50 '-');

      let r =

    match c_expr with
	| `IfNoElse (p,c) -> `IfNoElse(p, simplify_code c)
	| `IfElse (p,l,r) -> `IfElse(p, simplify_code l, simplify_code r)
	| `ForEach(ds, c) -> `ForEach(ds, simplify_code c)

	(* flatten blocks *)
        | `Block ([x; `Block(y)]) when not(is_block x) ->
              simplify_code (`Block([x]@y))
	| `Block ([`Block(x)]) -> `Block (List.map simplify_code x)
	| `Block ([x]) -> simplify_code x

	| `Block(x) ->
	      (* reorder locally scoped vars to beginning of block
               *  Note: assumes unique variables 
               *)
	      let (decls, code) =
		  List.partition
		      (fun y -> match y with
			  | `Declare(z) -> true | _ -> false) x
	      in
	      let simplified_non_decls = List.map simplify_code code in
              let collapsed_non_decls =
                  List.fold_left
                      (fun acc ce -> match ce with
                          | `Block((h::t) as y) ->
                                begin match h with | `Declare _ -> acc@[ce] | _ -> acc@y end
                          | _ -> acc@[ce])
                      [] simplified_non_decls
              in
	      let non_decls = collapsed_non_decls in

	      (* substitute redundant vars *)
	      let (new_decls, substituted_code) =
		  let assigned_by_vars =
		      List.filter
			  (fun c ->
			      match c with
				  | `Assign(_, `CTerm(`Variable(_))) -> true
				  | _ -> false)
			  non_decls
		  in
		  let decls_assigned_by_vars =
		      List.filter
			  (fun d -> match d with
			      | `Declare(`Variable(v1,_)) ->
				    List.exists
					(fun a -> 
					    match a with
						| `Assign(v2, _) -> v2 = v1
						| _ -> raise InvalidExpression)
					assigned_by_vars
			      | _ -> false) decls
		  in
		  let filtered_decls =
		      List.filter (fun c -> not (List.mem c decls_assigned_by_vars)) decls
		  in
		  let filtered_code =
                      (* substitute LHS vars assigned by RHS vars in code, and
                         remove assignments to corresponding LHS vars *)
		      List.filter
			  (fun c -> not (List.mem c assigned_by_vars))
			  (List.map
			      (substitute_code_vars assigned_by_vars)
			      non_decls)
		  in
		      (filtered_decls, filtered_code)
	      in
	      let reordered_code = new_decls@substituted_code in
	      let merged_code = merge_block_code reordered_code [] in
                  begin match merged_code with
		      | [] -> raise InvalidExpression
		      | [x] -> simplify_code x
		      | h::t ->
                            if merged_code = x then
                                `Block(merged_code)
                            else
                                simplify_code (`Block(merged_code))
		  end

	| `Handler(n, args, rt, c) ->
	      `Handler(n, args, rt, merge_block_code (List.map simplify_code c) [])

	| _ -> c_expr

      in
      print_endline "Result:";
      print_endline (indented_string_of_code_expression r);
      print_endline (String.make 50 '-');
      r



let gc_assign_state_var code decl var =
    let rv = get_return_val code in
    let new_decl =
        let nvd = `Variable(var, "int") in
            if List.mem nvd decl then decl else nvd::decl
    in
        match rv with
            | `Eval x ->
                  let new_code =
                      if rv = code then `Assign(var, x)
                      else
                          (replace_return_val code (`Assign (var, x)))
                  in
		      (new_code, new_decl)
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_assign_state_var: invalid return val")


let gc_incr_state_var code decl var oplus diff =
    let oper op = function x -> match op with 
	| `Plus -> `Sum (`CTerm(`Variable(var)), x) 
	| `Minus -> `Minus(`CTerm(`Variable(var)), x)
	| `Min -> `Min (`CTerm(`Variable(var)), x)
	| `Max -> `Max (`CTerm(`Variable(var)), x)
	| `Decrmin _ -> raise InvalidExpression
    in
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      match (rv = code, diff) with
                          | (true, true) -> `Block([`Assign(var, oper oplus x); rv])
                          | (true, false) -> `Assign(var, oper oplus x)
                          | (false, true) ->
                                `Block([replace_return_val code (`Assign (var, oper oplus x)); rv])
                          | (false, false) ->
                                replace_return_val code (`Assign (var, oper oplus x))
                  in
		      (new_code, (`Variable(var, "int"))::decl)
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_incr_state_var: invalid return val")


let gc_assign_state_map code decl map_key map_decl =
    let new_decl = if List.mem map_decl decl then decl else map_decl::decl in
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      if rv = code then `AssignMap(map_key, x)
                      else
                          replace_return_val code (`AssignMap (map_key, x))
                  in
		      (new_code, new_decl)
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_assign_state_map: invalid return val")


let gc_incr_state_map code decl map_key map_decl oplus diff =
    let map_access_code = `CTerm(`MapAccess(map_key)) in
    let new_decl = if List.mem map_decl decl then decl else map_decl::decl in
    let oper op = function x -> match op with 
	| `Plus -> `AssignMap(map_key, `Sum (map_access_code, x))
	| `Minus -> `AssignMap(map_key, `Minus(map_access_code, x))
	| `Min -> `AssignMap(map_key, `Min (map_access_code, x))
	| `Max -> `AssignMap(map_key, `Max (map_access_code, x))
	| `Decrmin _ -> raise InvalidExpression
    in		
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      match (rv = code, diff) with
                          | (true, true) -> `Block([(oper oplus x); rv])
                          | (true, false) -> (oper oplus x)
                          | (false, true) ->
                                `Block([replace_return_val code (oper oplus x); rv])
                          | (false, false) ->
                                replace_return_val code (oper oplus x)
                  in
		      (new_code, new_decl)
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_incr_state_map: invalid return val")

(* TODO: handle deletions.
 * Note: only deletions on selects, nested aggregates need IncrPlan *)
(*
let gc_incr_state_dom code decl dom_ds dom_decl oplus diff =
    let new_decl = if List.mem dom_decl decl then decl else dom_decl::decl in
    let oper op = function x -> match op with
        | `Union -> `InsertTuple(dom_ds, x)
        | `Diff -> `DeleteTuple(dom_ds, x)
    in
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      match (rv = code, diff) with
                          | (true, true) ->  `Block([oper oplus x; rv])
                          | (false, true) -> `Block([replace_return_val code (oper oplus x); rv])

                          | (_, false) ->
                                print_endline ("gc_incr_state_dom: IncrPlan unsupported.");
                                raise InvalidExpression
                  in
                      (new_code, new_decl)
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_incr_state_map: invalid return val")
*)

let gc_insert_event m_expr decl event =
    let br = get_base_relations m_expr in
    let event_vars = match event with 
        | `Insert(_, f) | `Delete(_, f) -> List.map (fun (id, ty) -> id) f
    in
    let event_rel = get_bound_relation event in
    let event_rel_decl =
        (* Check event relation is in the recomputed expression,
           and is already declared *)
        let check_base_relations =
            List.exists
                (fun y -> match y with
                    | `Relation(n,_) when n = event_rel -> true
                    | _ -> false) br
        in
            if check_base_relations then
                List.filter
                    (fun y -> match y with
                        | `Relation(n,_) when n = event_rel -> true
                        | _ -> false) decl
            else []
    in
        match event_rel_decl with
            | [] -> None
            | er_decl::t ->
                  let insert_code =
                      `InsertTuple(datastructure_of_declaration er_decl, event_vars)
                  in
                      Some(insert_code)


(* returns whether any bindings are used by m_expr and the bound variables *)
let is_binding_used m_expr bindings =
    (* returns vars as `Unqualified(var_name) *)
    let m_ubav = get_unbound_attributes_from_map_expression m_expr true in
    let bindings_used =
        List.filter
            (fun x -> match x with
                | `BindMapExpr(var_id, _) ->
                      List.mem (`Unqualified(var_id)) m_ubav
                | _ -> false)
            bindings
    in
    let binding_used_rv =
        List.map
            (fun x -> match x with
                | `BindMapExpr(v, bc) -> v
                | _ -> raise InvalidExpression)
            bindings_used
    in
        match binding_used_rv with
            | [] -> (false, [])
            | x -> (true, x)

let generate_code handler bindings event =
    (* map_expression -> declaration list ->  code_expression * declaration list *)
    let rec gc_aux e decl bind_info : code_expression * (declaration list) =
        let gc_binary_expr l r decl bind_info merge_rv_fn =
	    let (l_code, l_decl) = gc_aux l decl bind_info in
	    let (r_code, r_decl) = gc_aux r l_decl bind_info in
	        begin
		    match (l_code, r_code) with
		        | (`Eval x, `Eval y) ->  (merge_rv_fn x y, r_decl)
			      
		        | (`Block x, `Eval y) | (`Eval y, `Block x) ->
                              merge_with_block (`Block x) (merge_rv_fn y) r_decl
                                  
		        | (`Block x, `Block y) ->
			      merge_blocks l_code r_code merge_rv_fn r_decl
				  
		        | _ ->
			      print_endline ("gc_binary_expr: "^(string_of_code_expression l_code));
			      print_endline ("gc_binary_expr: "^(string_of_code_expression r_code));
			      raise InvalidExpression
	        end
        in
	match e with
	    | `METerm(x) ->
		  begin
		      (`Eval(`CTerm(
			  match x with 
			      | `Attribute y -> `Variable(field_of_attribute_identifier y) 
			      | `Int y -> `Int y
			      | `Float y -> `Float y 
			      | `Long y -> `Long y
			      | `String y -> `String y
			      | `Variable y -> `Variable y)), decl)
		  end

	    | `Sum(l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Sum(a,b)))

	    | `Minus(l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Minus(a,b)))

	    | `Product(l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Product(a,b)))

	    | `Min (l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Min(a,b)))

            | `Max (l,r) ->
                  gc_binary_expr l r decl bind_info (fun a b -> `Eval(`Max(a,b)))

	    | `MapAggregate(`Sum, f, q) ->
		  (* TODO: variable type? *)
		  let run_sum_var = gen_var_sym() in
		  let (f_code, f_decl) = gc_aux f decl bind_info in
		      begin
			  match f_code with
			      | `Eval (x) ->
				    let (agg_block, agg_decl) =
					gc_plan_aux q  
					    (`Assign(run_sum_var,
					    `Sum (`CTerm(`Variable(run_sum_var)), x)))
					    f_decl bind_info 
				    in
					(`Block(
					    [`Declare(`Variable(run_sum_var, "int"));
					    agg_block;
					    `Eval(`CTerm(`Variable(run_sum_var)))]), agg_decl)

			      | _ -> raise InvalidExpression
		      end

	    | `MapAggregate(`Min, f, q) ->
		  let run_min_var = gen_var_sym() in
		  let (f_code, f_decl) = gc_aux f decl bind_info in
		      begin
			  match f_code with
			      | `Eval (x) ->
				    let (agg_block, agg_decl) =
					gc_plan_aux q
					    (`Assign(run_min_var,
					    `Min (`CTerm(`Variable(run_min_var)), x)))
					    f_decl bind_info 
				    in
					(`Block(
					    [`Declare(`Variable(run_min_var, "int"));
					    agg_block;
					    `Eval(`CTerm(`Variable(run_min_var)))]), agg_decl)

			      | _ -> raise InvalidExpression
		      end

	    | (`Incr (sid, op, re, e) as oe) | (`IncrDiff(sid, op, re, e) as oe) ->
                  let diff = match oe with `Incr _ -> false | _ -> true in
		  let (e_code, e_decl) = gc_aux e decl bind_info in
		  let (e_is_bound, rc_l) = 
		      match bind_info with
                          | (true, bb) -> is_binding_used e bb
			  | _ -> (false, [])
		  in
                      print_endline ("is_bound "^(string_of_map_expression oe)^" "^
                          (string_of_bool e_is_bound));

                      (* test for rule 67-70 *)
		      if e_is_bound then
                          (* TODO: support multiple bindings for rules 67-70 *)
                          (* Note bound variables should already be declared and assigned.
                           * This currently happens in compile_target *)
                          let rc = List.hd rc_l in
 		          let mid = gen_map_sym sid in
		          let c = gen_var_sym() in
		          let map_decl = `Map(mid, [(c, "int")], "int") in 
		          let map_key = (mid, [rc]) in
                          let update_map_key = (mid, [c]) in
		              match event with 
			          | `Insert (_,_) ->
                                        print_endline ("new map: "^mid^" rc: "^rc^" c: "^c);
                                        print_endline ("e: \n"^(indented_string_of_map_expression e 0));
                                        let (update_code, update_decl) =
                                            (* substitute binding var for map local var *)
                                            let substituted_code = 
                                                substitute_code_vars ([`Assign(rc, `CTerm(`Variable(c)))]) e_code
                                            in
                                                gc_incr_state_map substituted_code e_decl update_map_key map_decl op diff
                                        in
                                        let (insert_code, new_decl) =
                                            (* use recomputation code rather than delta code *)
                                            let (re_code, re_decl) = gc_aux re update_decl (false, []) in
                                                gc_assign_state_map re_code re_decl map_key map_decl
                                        in
				        let update_and_init_binding_code = 
			     	            `Block ([ 
					        `ForEach (map_decl, update_code);
				 	        `IfNoElse (
                                                    `BCTerm (
                                                        `EQ(`CTerm(`MapContains(map_key)), `CTerm(`MapIterator(`End(mid))))), 
                                                    insert_code);
					        `Eval(`CTerm(`MapAccess(map_key))) ])
				        in
                                            (update_and_init_binding_code, new_decl)

				  (* TODO: garbage collection, min *)	
			          | `Delete _ -> 
                                        let (delete_code, delete_decl) =
                                            gc_incr_state_map e_code decl map_key map_decl op diff
                                        in
                                        let (update_code, new_decl) =
                                            gc_incr_state_map e_code delete_decl update_map_key map_decl op diff
                                        in
				        let delete_and_update_binding_code = 
				            `Block ([
                                                delete_code;
					        (* TODO: if S_f[rc] = f(R=0)[rc]) delete S_f[rc] *)
	 				        `ForEach (map_decl, update_code);
					        `Eval(`CTerm(`MapAccess(map_key)))
				            ])
				        in
                                            (delete_and_update_binding_code, new_decl)

		      else 
		          let e_uba =
		              match event with
		                  | `Insert(_,vars) | `Delete(_,vars) ->
			                List.filter
			                    (fun uaid -> not
			                        (List.exists
				                    (fun (id,_) -> (field_of_attribute_identifier uaid) = id)
				                    vars))
			                    (get_unbound_attributes_from_map_expression e true)
		          in
		              begin match e_uba with
			          | [] ->
				        let incr_var = gen_var_sym() in
                                            print_endline ("Declaring "^incr_var^" for "^(string_of_map_expression e));
				            gc_incr_state_var e_code e_decl incr_var op diff

			          | _ ->
				        let incr_mid = gen_map_sym sid in
				        let incr_map =
				            let matching_maps =
					        List.filter
					            (fun x -> match x with
						        | `Map(mid, _,_) -> mid = incr_mid
						        | _ -> false) e_decl
				            in
					        match matching_maps with
					            | [] ->
                                                          let fields =
                                                              List.map (fun x -> (field_of_attribute_identifier x,"int")) e_uba
                                                          in
						              `Map(incr_mid, fields, "int")
					            | [m] -> m
					            | _ -> raise (RewriteException "Multiple matching maps.")
				        in
				        let map_key =
				            let mf = List.map field_of_attribute_identifier e_uba in
					        (incr_mid, mf)
				        in
                                            print_endline ("Referencing "^(string_of_map_key map_key)^" for "^
                                                (string_of_map_expression oe));
				            gc_incr_state_map e_code e_decl map_key incr_map op diff
		              end

	    | (`Init (sid, e) as oe) ->
		  let (e_code, e_decl) = gc_aux e decl bind_info in

                  (* Note: no need to filter handler args, since e should be a recomputation,
                   * i.e. a map_expression where deltas have not been applied *)
		  let e_uba = get_unbound_attributes_from_map_expression e true in
		      begin match e_uba with
			  | [] ->
				let init_var = gen_var_sym() in
                                    print_endline ("Declaring "^init_var^" for "^(string_of_map_expression oe));
				    gc_assign_state_var e_code e_decl init_var

			  | _ ->
				let init_mid = gen_map_sym sid in
                                let init_fields = 
                                    List.map
                                        (fun x -> (field_of_attribute_identifier x,"int")) e_uba
                                in
				let init_map =
				    let matching_maps =
					List.filter
					    (fun x -> match x with
						| `Map(mid, _,_) -> mid = init_mid
						| _ -> false) e_decl
				    in
					match matching_maps with
					    | [] -> `Map(init_mid, init_fields, "int")
					    | [m] -> m
					    | _ -> raise (RewriteException "Multiple matching maps.")
				in
				let map_key = (init_mid, (let (x,_) = List.split init_fields in x)) in
                                    print_endline "Assigning state map for init";
                                    print_endline ("expr: "^(string_of_map_expression oe));
                                    print_endline ("code: "^(indented_string_of_code_expression e_code));
                                    print_endline ("Referencing "^(string_of_map_key map_key)^" for "^
                                        (string_of_map_expression oe));
				    gc_assign_state_map e_code e_decl map_key init_map
		      end

	    | _ -> 
		  print_endline("gc_aux: "^(string_of_map_expression e));
		  raise InvalidExpression

    (* plan -> code_expression list -> code_expression * declaration list *)
    and gc_plan_aux q iter_code decl bind_info : code_expression * (declaration list) =
	match q with
	    | `Relation (n,f) ->
		  let (r_decl, new_decl) =
                      (* Use name based matching, since columns are unique *)
                      let existing_decl =
                          List.filter
                              (fun d -> match d with
                                  | `Relation (n2,f2) -> n = n2 | _ -> false)
                              decl
                      in
                          match existing_decl with
                              | [] -> let y = `Relation(n,f) in (y, decl@[y])
                              | [x] -> (x, decl)
                              | _ -> raise DuplicateException
		  in
		      (`ForEach(datastructure_of_declaration r_decl, iter_code), new_decl)

            | `Domain (sid, attrs) ->
                  let (domain_decl, new_decl) =
                      let dom_fields =
                          List.map (fun x -> (field_of_attribute_identifier x, "int")) attrs
                      in
                      let existing_decl =
                          List.filter
                              (fun ds -> match ds with
                                  | `Domain (_, f) -> f = dom_fields | _ -> false)
                              decl
                      in
                          match existing_decl with
                              | [] ->
		                    let dom_id = gen_dom_sym (sid) in
                                    let dom_decl = `Domain(dom_id, dom_fields) in 
                                        (dom_decl, decl@[dom_decl])
                              | [x] -> (x, decl)
                              | _ -> raise DuplicateException
                  in
                      (`ForEach(datastructure_of_declaration domain_decl, iter_code), new_decl)

            | `TrueRelation -> (iter_code, decl)

	    | `Project (a, `TrueRelation) ->
                  let local_decl =
                      List.concat (List.map
                          (fun (aid,expr) ->
                              let new_var = field_of_attribute_identifier aid in
                                  [`Declare(`Variable(new_var, "int"));
                                  `Assign(new_var, create_code_expression expr)])
                          a)
                  in
                      begin
                          match iter_code with
                              | `Block y -> (`Block(local_decl@y), decl)
                              | _ -> (`Block(local_decl@[iter_code]), decl)
                      end


	    | `Select (pred, cq) ->
		  begin
		      match pred with 
			  | `BTerm(`MEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) ->
				(* TODO: pred var types *)
				let (pred_var_code, new_decl) = gc_aux m_expr decl bind_info in
				let (pred_cterm, assign_var_code, pred_decl) =
                                    let pred_var_rv = get_return_val pred_var_code in
                                    let code_wo_rv =
                                        if pred_var_code = pred_var_rv then None
                                        else Some(remove_return_val pred_var_code)
                                    in
                                        match pred_var_rv with
                                            | `Eval(`CTerm(`Variable(x))) ->
						  (* (`Variable(x), code_wo_rv, new_decl) *)
                                                  let error = "Found independent nested map expression"^
                                                      "(should have been lifted.)"
                                                  in
                                                      print_endline ("gc_plan_aux: "^error^":\n"^
                                                          (indented_string_of_code_expression pred_var_code));
                                                      raise (CodegenException error);


                                            | `Eval(`CTerm(`MapAccess(mf))) ->
                                                  (`MapAccess(mf), code_wo_rv, new_decl)

                                            | `Eval(x) ->
						  let pv = gen_var_sym() in 
                                                  let av_code = replace_return_val pred_var_code (`Assign(pv,x)) in
                                                      print_endline ("Declaring var "^pv^" in "^(string_of_plan q));
                                                      print_endline ("pv_code:\n"^
                                                          (indented_string_of_code_expression pred_var_code));
                                                      print_endline ("x_code: "^(string_of_arith_code_expression x));
						      (`Variable(pv), Some(av_code), (`Variable(pv, "int"))::new_decl)

                                            | _ ->
                                                  print_endline ("Invalid return val:\n"^
                                                      (indented_string_of_code_expression pred_var_code));
                                                  raise InvalidExpression
				in
				let pred_test_code =
				    begin
					match pred with
					    | `BTerm(`MEQ(m_expr)) ->
						  `BCTerm(`EQ(`CTerm(pred_cterm), `CTerm(`Int 0)))
					    | `BTerm(`MLT(m_expr)) ->
						  `BCTerm(`LT(`CTerm(pred_cterm), `CTerm(`Int 0)))
					    | _ -> raise InvalidExpression
				    end
				in

				let new_iter_code =
				    match (m_expr, pred_cterm) with
                                         (* Note: only inserts have `Init expressions.
                                            Generate insert into base relations before recomputing as necessary *)
                                        | (`Init (_,e), `MapAccess(mf)) ->
					      let (mid, _) = mf in
					      let map_contains_code = `CTerm(`MapContains(mf)) in
                                              let insert_code = gc_insert_event e pred_decl event in
                                              let init_code =
						  `IfNoElse(
						      `BCTerm(`EQ(map_contains_code, `CTerm(`MapIterator(`End(mid))))),
                                                      match assign_var_code with
                                                          | None -> `IfNoElse(pred_test_code, iter_code)
                                                          | Some av -> 
						                `Block([av; `IfNoElse(pred_test_code, iter_code)]))
                                              in
                                                  begin
                                                      match insert_code with
                                                          | None -> init_code
                                                          | Some(ic) -> `Block([ic; init_code])
                                                  end

                                        | (`Init (_,e), `Variable _) ->
                                              let insert_code = gc_insert_event e pred_decl event in
                                                  begin
                                                      match (insert_code, assign_var_code) with
                                                          | (None, None) -> `IfNoElse(pred_test_code, iter_code)
                                                          | (Some ins, None) ->
                                                                `Block([ins; `IfNoElse(pred_test_code, iter_code)])
                                                          | (None, Some av) ->
                                                                `Block([av; `IfNoElse(pred_test_code, iter_code)])
                                                          | (Some ins, Some av) -> 
                                                                `Block([ins; av; `IfNoElse(pred_test_code, iter_code)])
                                                  end

                                        | (`Incr _, _) | _ ->
                                              match assign_var_code with
                                                  | None -> `IfNoElse(pred_test_code, iter_code)
                                                  | Some av -> `Block([av; `IfNoElse(pred_test_code, iter_code)])
				in
				    gc_plan_aux cq new_iter_code pred_decl bind_info 

			  | _ ->
				let new_iter_code =
                                    `IfNoElse(create_code_predicate pred, iter_code)
				in
				    gc_plan_aux cq new_iter_code decl bind_info 
		  end
		      
	    | `Union ch ->
		  let (ch_code, ch_decl) = 
		      List.split (List.map (fun c -> gc_plan_aux c iter_code decl bind_info ) ch)
		  in
		      (`Block(ch_code), List.flatten ch_decl)
			  
	    | `Cross(l,r) ->
		  let (inner_code, inner_decl) = gc_plan_aux r iter_code decl bind_info in
		      gc_plan_aux l inner_code inner_decl bind_info 
	                  
	    (* to handle rule 39, 41 *)
	    | `IncrPlan(sid, op, nq, d) | `IncrDiffPlan(sid, op, nq, d) ->
                  let print_incrp_debug q_code incrp_dom =
		      print_endline ("rule 39,41: incrp_dom "^(string_of_code_expression (`Declare incrp_dom)));
                      print_endline ("rule 39,41: iter_code "^(string_of_plan nq));
                      print_endline ("rule 39,41: q_code "^(indented_string_of_code_expression q_code));

                  in
                  (* let diff = match q with `IncrPlan _ -> false | _ -> true in *)
		  let dom_id = gen_dom_sym (sid) in
                  let dom_fields = List.map (fun x -> (field_of_attribute_identifier x, "int")) d in
		  let dom_decl = `Domain(dom_id, dom_fields) in
                  let dom_ds = datastructure_of_declaration dom_decl in
                  let (new_iter_code, new_decl) =
                      let incr_code = match op with
                          | `Union -> `InsertTuple(dom_ds, List.map field_of_attribute_identifier d)
                          | `Diff -> `DeleteTuple(dom_ds, List.map field_of_attribute_identifier d)
                      in
                          (`Block([incr_code; iter_code]),
                           if List.mem dom_decl decl then decl else dom_decl::decl)
                  in
		  let (q_code, q_decl) = gc_plan_aux nq new_iter_code new_decl bind_info in 
                      print_incrp_debug q_code dom_decl;
                      (*
                        print_endline ("Referencing "^dom_id^" for "^(string_of_plan q));
		        gc_incr_state_dom q_code q_decl dom_var dom_relation op diff
                      *)
                      (q_code, q_decl)
                              
	    | _ ->
		  print_endline ("gc_aux_plan: "^(string_of_plan q));
		  raise InvalidExpression
                      (*    and gc_aux_min f q decl bind_info : code_expression * (declaration list) =
  	                    let run_min_var = gen_var_sym() in
	                    let (f_code, f_decl) = gc_aux f decl bind_info in
	                    begin
		            match f_code with
		            | `Eval (x) ->
			    let (agg_block, agg_decl) =
			    gc_plan_aux (`IncrPlan (q))
			    (`Assign(run_min_var,
			    `Min (`CTerm(`Variable(run_min_var)), x)))	
			    f_decl bind_info
			    in
			    (`Block(
			    [`Declare(`Variable(run_min_var, "int"));
			    agg_block;
			    `Eval(`CTerm(`Variable(run_min_var)))]), agg_decl)
		            | _ -> raise InvalidExpression
	                    end *)
    in
	
    let handler_fields = 
        match event with
            | `Insert (_, fields) | `Delete (_, fields) ->
	          print_endline (
	              "Handler("^(string_of_schema fields)^"):\n"^
	                  (indented_string_of_map_expression handler 0));
	          fields
    in

    (* TODO: declared variable types *)
    let (binding_bodies, binding_decls) =
        List.fold_left
            (fun (code_acc, decl_acc) b ->
                match b with
                    | `BindExpr (v, expr) ->
                          (code_acc@[`Assign(v, create_code_expression expr)],
                          decl_acc@[`Variable(v, "int")])

                    | `BindBoolExpr (v, b_expr) -> 
                          (* TODO *)
                          print_endline "Boolean expression bindings currently unsupported.";
                          raise InvalidExpression

                    | `BindMapExpr (v, m_expr) ->
                          let (binding_code,d) = gc_aux m_expr decl_acc (false, []) in
                          let rv = get_return_val binding_code in
                              match rv with 
                                  | `Eval x ->
                                        (code_acc@[(replace_return_val binding_code (`Assign(v, x)))],
                                        d@[`Variable(v, "int")])
                                  | _ ->
                                        print_endline ("Invalid return val: "^(string_of_code_expression rv));
                                        raise InvalidExpression)
            ([], []) bindings
    in

    let (handler_body, handler_decl) = gc_aux handler binding_decls (true, bindings) in

    (* Note: binding declarations are included in declaration_code *)
    let declaration_code = List.map (fun x -> `Declare(x)) handler_decl in

    let (handler_body_with_rv, result_type) = 
	match get_return_val handler_body with
	    | `Eval x ->
                  (replace_return_val handler_body (`Return x),
                      ctype_of_arith_code_expression x)
	    | _ ->
                  print_endline ("Invalid return val:\n"^
                      (indented_string_of_code_expression handler_body));
                  raise InvalidExpression
    in
    let (global_decls, handler_code) =
	List.partition
	    (fun x -> match x with
		| `Declare(`Map _) | `Declare(`Relation _) | `Declare(`Domain _) -> true
		| _ -> false)
	    (declaration_code@binding_bodies@[handler_body_with_rv])
    in
	(global_decls,
	    simplify_code
	        (`Handler(handler_name_of_event event,
	            handler_fields, result_type, handler_code)))

(* parent class to handle file_stream *)
let file_stream_body = 
    "struct file_stream\n"^
	"{\n"^
	"    public:\n"^
	"    string delim;\n"^
	"    int num_fields;\n"^
	"    ifstream *input_file;\n"^
	"\n"^
	"    file_stream(string file_name, string d, int n): num_fields(n), delim(d) {\n"^
 	"        input_file = new ifstream(file_name.c_str());\n"^
	"        if(!(input_file->good()))\n"^
	"            cerr << \"Failed to open file \" << file_name << endl;\n"^
	"    }\n"^
	"\n"^
 	"    tuple<bool, tokeniz> read_inputs(int line_num)\n"^
	"    {\n"^
	"        char buf[256];\n"^
	"        char_separator<char> sep(delim.c_str());\n"^
	"        input_file->getline(buf, sizeof(buf));\n"^
	"        string line = buf;\n"^
	"        tokeniz tokens(line, sep);\n"^
	"\n"^
	"        if(*buf == '\\0')\n"^
	"            return make_tuple(false, tokens);\n"^
	"\n"^
	"        if (is_valid_num_tokens(tokens)) {\n"^
	"            cerr<< \"Failed to parse record at line \" << line_num << endl;\n"^
	"            return make_tuple(false, tokens);\n"^
	"        }\n"^
	"        return make_tuple(true, tokens);\n"^
	"    }\n"^
	"\n"^
	"    inline bool is_valid_num_tokens(tokeniz tok)\n"^
	"    {\n"^
	"        int i = 0;\n"^
	"        for (tokeniz::iterator beg = tok.begin(); beg!=tok.end(); ++beg, i ++)\n"^
	"            ;\n"^
	"        return i == num_fields;\n"^
	"    }\n"^
	"\n"^
	"    void init_stream()\n"^
	"    {\n"^
	"        int line = 0;\n"^
	"        while(input_file -> good())\n"^
	"        {\n"^
	"            tuple<bool, tokeniz> valid_input = read_inputs(line ++);\n"^
	"            if(!get<0> (valid_input))\n"^
	"                break;\n"^
	"            if(!insert_tuple(get<1>(valid_input)))\n"^
	"                cerr<< \"Failed to parse record at line \" << line << endl;\n"^
	"        }\n"^
	"    }\n"^
	"\n"^
	"    virtual bool insert_tuple(tokeniz t) =0;\n"^
	"};\n"^
	"\n"

let make_file_streams `Relation(id, fields) =
    let typename = 
	let ftype = ctype_of_datastructure_fields fields in
	    if (List.length fields) = 1 then ftype else "tuple<"^ftype^">" in
    let classname = "stream_"^id in
    let struct_f_1 = 
	"struct "^classname^" : public file_stream\n"^
	    "{\n"^
	    "    "^classname^"(string filename, string delim, int n)\n"^
	    "        : file_stream(filename, delim, n) { }\n"^
	    "\n"^
	    "    bool insert_tuple(tokeniz tok)\n"^
	    "    {\n"^
	    "        "^typename^" r;\n"^
	    "        int i = 0;\n"^
	    "        for (tokeniz::iterator beg = tok.begin(); beg!=tok.end(); ++beg) {\n"
    in
    let switch_body fields n = 
	let (m, str) = 
	    List.fold_left 
		(fun (n,s) (_,t) ->
		    (n+1, 
		    (* TODO : need to handle string, long, float too *)
		    (if t = "int" then s^
			"            case "^(string_of_int n)^":\n"^ 
			"                get<"^(string_of_int n)^">(r) = atoi((*beg).c_str());\n"^
			"                break;\n" else s))
		) 
		(0,"") fields
	in 	
	    (m, "            switch(i) {\n"^str^"            }\n")
    in let (num_fields, s_body) = switch_body fields 0 
    in let struct_f_2 = 
	    "\n"^
	        "            i ++;\n"^
	        "        }\n"^
	        "\n"^
	        "        "^id^".push_back(r);\n"^
	        "        return true;\n"^
	        "    }\n"^
	        "\n"^
	        "};\n"^
	        "\n"
    in let whole_body = struct_f_1 ^ s_body ^ struct_f_2
    in let caller = 
	    let rec arguments acc n =
	        if n = num_fields then acc
	        else if n = 0 then arguments (acc^"get<"^string_of_int n^"> (*front)") (n+1)
	        else arguments (acc^", get<"^string_of_int n^"> (*front)") (n+1)
	    in
	        function x -> 
	            "    {\n"^
		        "        list <"^typename^" >::iterator front = "^id^".begin();\n"^
		        "        list <"^typename^" >::iterator end = "^id^".end();\n"^
		        "        for ( ; front != end; ++front) {\n"^
		        "            "^x^"( "^(arguments "" 0) ^");\n"^
		        "        }\n"^
		        "    }\n" 
    in (classname, whole_body, caller)

let config_handler global_decls = 
    let c_handler_1 = 
	"list<file_stream *> initialize(ifstream *config)\n"^
	    "{\n"^
	    "    char buf[256];\n"^
	    "    string line;\n"^
	    "    char_separator<char> sep(\" \");\n"^
	    "    int i;\n"^
   	    "    list <file_stream *> files;\n"^
	    "    file_stream * f_stream;\n"^
	    "\n"^
	    "    while(config->good()) {\n"^
	    "        string param[4];\n"^
	    "        config->getline(buf, sizeof(buf));\n"^
	    "        line = buf;\n"^
	    "        tokeniz tok(line, sep);\n"^
	    "        i = 0;\n"^
	    "\n"^
	    "        for (tokeniz::iterator beg = tok.begin() ; beg != tok.end(); ++beg, i ++) {\n"^
	    "            param[i] = *beg;\n"^
	    "        }\n"^ 
	    "\n" in
    let c_handler_2 =
	"\n"^
	    "        f_stream->init_stream();\n"^
	    "        files.push_back(f_stream);\n"^
	    "    }\n"^
	    "    return files;\n"^
	    "}\n\n" in
    let if_stmts = 
	let param = "param[1], param[2], atoi(param[3].c_str())" in
	    List.fold_left
	        (fun acc x -> match x with
		        `Declare d ->
		            begin
			        match d with
			                `Relation(i,f) -> if acc = "" then 
				            acc^"        if(param[0] == \""^i^"\")\n"^
				                "            f_stream = new stream_"^i^"("^param^");\n"
				        else 
				            acc^"        else if(param[0] == \""^i^"\")\n"^
				                "            f_stream = new stream_"^i^"("^param^");\n"
			            | _ -> acc
		            end
		    | _ -> raise InvalidExpression ) "" global_decls 
    in c_handler_1 ^ if_stmts ^ c_handler_2

