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

let rec create_code_predicate b_expr base_rels events decl =
    match b_expr with
	| `BTerm(x) ->
              let dummy_pair = (`CTerm(`Variable("dummy")), `CTerm(`Int 0)) in
              let binary l r fn = fn (create_code_expression l) (create_code_expression r)
	      in
	      begin
		  `BCTerm(
		      match x with
			  | `True -> `True
			  | `False -> `False

			  | `EQ(l,r) -> binary l r (fun x y -> `EQ(x,y)) 
			  | `NE(l,r) -> binary l r (fun x y -> `NE(x,y))
			  | `LT(l,r) -> binary l r (fun x y -> `LT(x,y))
			  | `LE(l,r) -> binary l r (fun x y -> `LE(x,y))
			  | `GT(l,r) -> binary l r (fun x y -> `GT(x,y))
			  | `GE(l,r) -> binary l r (fun x y -> `GE(x,y))

			  | `MEQ(_) -> `EQ(dummy_pair)
			  | `MNEQ(_) -> `NE(dummy_pair)
			  | `MLT(_) -> `LT(dummy_pair)
			  | `MLE(_) -> `LE(dummy_pair)
			  | `MGT(_) -> `GT(dummy_pair)
			  | `MGE(_) -> `GE(dummy_pair))
	      end
	| `And(l,r) -> `And(create_code_predicate l base_rels events decl, create_code_predicate r base_rels events decl)
	| `Or(l,r) -> `Or(create_code_predicate l base_rels events decl, create_code_predicate r base_rels events decl)
	| `Not(e) -> `Not(create_code_predicate e base_rels events decl)

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
            | `Profile(st, p,c) -> `Profile(st, p, sc c)

let rec merge_block_code cl acc =
    match cl with
	| [] -> acc
	| (`Block a)::((`Block b)::t) -> merge_block_code ((`Block(a@b))::t) acc
	| h::t -> merge_block_code t (acc@[h])

let rec simplify_code c_expr =

(*
    print_endline "Simplify code:";
    print_endline (indented_string_of_code_expression c_expr);
    print_endline (String.make 50 '-');

    let r =
*)

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

        | `Profile(st,p,c) -> `Profile(st, p, simplify_code c)

	| _ -> c_expr

(*
      in
      print_endline "Result:";
      print_endline (indented_string_of_code_expression r);
      print_endline (String.make 50 '-');
      r
*)


let gc_assign_state_var code var =
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      if rv = code then `Assign(var, x)
                      else
                          (replace_return_val code (`Assign (var, x)))
                  in
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_assign_state_var: invalid return val")


let gc_incr_state_var code var oplus diff =
    let oper op = function x -> match op with 
	| `Plus -> `Sum (`CTerm(`Variable(var)), x) 
	| `Minus -> `Minus (`CTerm(`Variable(var)), x)
	| `Min -> `Min (`CTerm(`Variable(var)), x)
	| `Max -> `Max (`CTerm(`Variable(var)), x)
	| `Decrmin _ | `Decrmax _
              -> raise InvalidExpression
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
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_incr_state_var: invalid return val")


let gc_assign_state_map code map_key =
    let rv = get_return_val code in
        match rv with
            | `Eval x ->
                  let new_code =
                      if rv = code then `AssignMap(map_key, x)
                      else
                          replace_return_val code (`AssignMap (map_key, x))
                  in
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_assign_state_map: invalid return val")


let gc_incr_state_map code map_key oplus diff =
    let map_access_code = `CTerm(`MapAccess(map_key)) in
    let oper op = function x -> match op with 
	| `Plus -> `AssignMap(map_key, `Sum (map_access_code, x))
	| `Minus -> `AssignMap(map_key, `Minus(map_access_code, x))
	| `Min -> `AssignMap(map_key, `Min (map_access_code, x))
	| `Max -> `AssignMap(map_key, `Max (map_access_code, x))
	| `Decrmin _ | `Decrmax _ ->
              raise InvalidExpression
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
		      new_code
            | _ ->
                  print_endline ("Invalid return val: "^(indented_string_of_code_expression rv));
                  raise (RewriteException "get_incr_state_map: invalid return val")


(* Helper for dealing with `Init *)
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
        match (event, event_rel_decl) with
            | (`Insert _, []) -> None
            | (`Insert _, er_decl::t) ->
                  let insert_code =
                      `InsertTuple(datastructure_of_declaration er_decl, event_vars)
                  in
                      Some(insert_code)
            | (`Delete _, _) ->
                  print_endline ("gc_insert_event: called on "^(string_of_delta event)^
                      " with map expr:\n"^(indented_string_of_map_expression m_expr 0));
                  raise (CodegenException ("gc_insert_event: called on "^(string_of_delta event)))

let gc_declare_state_for_map_expression m_expr decl unbound_attrs_vars state_id base_rels events =
    print_endline (string_of_map_expression m_expr);
    let new_type = type_inf_mexpr m_expr base_rels events decl in

    match unbound_attrs_vars with
	| [] ->
	      let state_var = state_id in
              let r_decl = `Variable(state_var, new_type) in
                  print_endline ("Declaring var state "^state_var^" for "^
                      (string_of_map_expression m_expr));
		  (r_decl, decl@[r_decl])

	| e_uba ->
	      let state_mid = state_id in
	      let (state_map, existing) =
		  let existing_maps =
		      List.filter
			  (fun x -> match x with
			      | `Map(mid, _,_) -> mid = state_mid
			      | _ -> false) decl
		  in
		      match existing_maps with
			  | [] ->
                                let fields = List.map
                                    (fun x -> (field_of_attribute_identifier x,
                                    type_inf_mexpr (`METerm (`Attribute(x))) base_rels events decl )) e_uba
                                in
				    (`Map(state_mid, fields, new_type), false)
			  | [m] -> (m, true)
			  | _ -> raise (RewriteException "Multiple matching maps.")
	      in
                  print_endline ("Declaring map state "^state_mid^" for "^
                      (string_of_map_expression m_expr));
                  if existing then (state_map, decl) else (state_map, decl@[state_map])


let gc_declare_handler_state_for_map_expression m_expr decl unbound_attrs_vars base_rels events =
    let global_decfn =
        fun x ->
            gc_declare_state_for_map_expression
                m_expr decl unbound_attrs_vars x base_rels events
    in
    match m_expr with
	| `Incr (sid,_,_,_,_) | `IncrDiff(sid,_,_,_,_) | `Init(sid,_,_) ->
              begin
                  match unbound_attrs_vars with
                      | [] -> global_decfn  (gen_var_sym())
                      | _ -> global_decfn (gen_map_sym sid)
              end

        | _ ->
              print_endline ("Invalid map expr for declaration:\n"^
                  (indented_string_of_map_expression m_expr 0));
              raise InvalidExpression


(* TODO: support sliced access for partially bound keys in M-D maps
 * -- requires extension to `ForEach to support partial iteration *)
let gc_foreach_map e e_code e_decl e_vars e_uba_fields mk op diff recursion_decls =
    let (mid, mf) = mk in
    let mf_len = List.length mf in
    let bound_f = List.filter (fun f -> List.mem f e_vars) mf in
    let incr_code = gc_incr_state_map e_code mk op diff in
        begin
            print_endline ("mf: "^(string_of_code_var_list mf)^
                " uba: "^(string_of_code_var_list e_uba_fields));
            print_endline ("Accessing "^(string_of_map_key mk)^
                " in expression "^(string_of_map_expression e));
            print_endline ("bound_f: "^(string_of_code_var_list bound_f));
            print_endline ("e_vars: "^(string_of_code_var_list e_vars));
            print_endline ("test: "^(string_of_int (List.length bound_f))^" "^(string_of_int mf_len));
            print_endline ("incr_code: "^(indented_string_of_code_expression incr_code));
            if (List.length bound_f) = mf_len then
                incr_code
            else
                begin
                    let decl_matches =
                        List.filter
                            (fun d -> (identifier_of_declaration d) = mid)
                            (List.map (function | `Declare d -> d) recursion_decls)
                    in
                    let ds = match decl_matches with
                        | [x] -> datastructure_of_declaration x
                        | [] ->
                              print_endline ("Could not find declaration for "^mid);
                              raise InvalidExpression
                        | _ -> raise DuplicateException
                    in
                        `ForEach(ds, incr_code)
                end
        end


let gc_state_accessor_code par_decl bindings
        e e_code e_decl e_vars e_uba_fields op diff recursion_decls
=
    match par_decl with
        | `Variable(v,_) ->
                  if (List.mem v e_uba_fields) then
                      begin
                          print_endline ("Could not find declaration for "^v);
                          raise InvalidExpression
                      end
                  else
                      (gc_incr_state_var e_code v op diff, e_decl)

        | `Map(n,f,_) ->
              (* Building map accessors from bindings *)
              let mk =
                  let access_fields =
                      List.map (fun (id, _) ->
                          let bound_f =
                              List.filter (fun (a,_) ->
                                  (field_of_attribute_identifier a) = id) bindings
                          in
                              match bound_f with
                                  | [] -> id
                                  | [(a, e_opt)] ->
                                        begin
                                            match e_opt with
                                                | Some(`ETerm(`Variable(b))) -> b
                                                | Some(e) ->
                                                      print_endline ("Invalid map access field: "^(string_of_expression e));
                                                      raise (CodegenException
                                                          "Map access fields must be variables!")
                                                | None -> id
                                        end
                                  | _ -> raise DuplicateException)
                          f
                  in
                      (n, access_fields)
              in
                  (gc_foreach_map e e_code e_decl e_vars e_uba_fields mk op diff recursion_decls, e_decl)

let gc_declare_and_incr_state incr_e e_code e_decl e_uba op diff base_rels events =
    let (state_decl, new_decl) =
        gc_declare_handler_state_for_map_expression incr_e e_decl e_uba base_rels events 
    in
	begin match e_uba with
	    | [] ->
                  let state_var = match state_decl with
                      | `Variable(id, _) -> id | _ -> raise InvalidExpression
                  in
                      (gc_incr_state_var e_code state_var op diff, new_decl)

	    | _ ->
		  let map_key =
                      let mid = match state_decl with
                          | `Map(id,_,_) -> id | _ -> raise InvalidExpression
                      in
		      let mf = List.map field_of_attribute_identifier e_uba in
			  (mid, mf)
		  in
                      print_endline ("Referencing "^(string_of_map_key map_key)^" for "^
                          (string_of_map_expression incr_e));
		      (gc_incr_state_map e_code map_key op diff, new_decl)
	end

let gc_declare_and_assign_state init_e e_code e_decl e_uba base_rels events=
    let print_debug orig_expr code map_key =
        print_endline "Assigning state map for init";
        print_endline ("expr: "^(string_of_map_expression orig_expr));
        print_endline ("code: "^(indented_string_of_code_expression code));
        print_endline ("Referencing "^(string_of_map_key map_key)^" for "^
            (string_of_map_expression orig_expr))
    in

    let (state_decl, new_decl) =
        gc_declare_handler_state_for_map_expression init_e e_decl e_uba base_rels events
    in
	begin match e_uba with
	    | [] -> 
                  let state_var = match state_decl with
                      | `Variable(id, _) -> id | _ -> raise InvalidExpression
                  in
                      (gc_assign_state_var e_code state_var, new_decl)

	    | _ ->
		  let map_key =
                      let mid = match state_decl with
                          | `Map(id,_,_) -> id | _ -> raise InvalidExpression
                      in
		      let mf = List.map field_of_attribute_identifier e_uba in
			  (mid, mf)
                  in
                      print_debug init_e e_code map_key;
		      (gc_assign_state_map e_code map_key, new_decl)
	end


(* map_expression -> binding list -> delta -> boolean
   -> (var id * code terminal) list -> (var id list * code terminal) list
   -> code_expression * declaration list *)
let generate_code handler bindings event body_only map_var_accessors state_p_decls recursion_decls base_rels=
    print_endline ("Generating code for: "^(string_of_map_expression handler));

    (* map_expression -> declaration list -> bool * binding list
         -> code_expression * declaration list *)
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
			      | `Variable y ->
                                    if (List.mem_assoc y map_var_accessors) then
                                        List.assoc y map_var_accessors
                                    else
                                        `Variable y)),
                      decl)
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

	    | `MapAggregate(fn, f, q) ->
		  let running_var = gen_var_sym() in
		  let (f_code, f_decl) = gc_aux f decl bind_info in
		  let new_type = type_inf_mexpr f base_rels [event] decl in
		  print_endline ("!!map aggregate in gc_aux "^string_of_map_expression f ^" "^new_type);
                  let incr_code x =
                      match fn with
                          | `Sum -> `Sum (`CTerm(`Variable(running_var)), x)
                          | `Min -> `Min (`CTerm(`Variable(running_var)), x)
                          | `Max -> `Max (`CTerm(`Variable(running_var)), x)
                  in
		      begin
			  match f_code with
			      | `Eval (x) ->
				    let (agg_block, agg_decl) =
					gc_plan_aux q  
					    (`Assign(running_var, incr_code x))
					    f_decl bind_info 
				    in
					(`Block(
					    [`Declare(`Variable(running_var, new_type));
					    agg_block;
					    `Eval(`CTerm(`Variable(running_var)))]), agg_decl)

			      | _ -> raise InvalidExpression
		      end

	    | (`Incr (sid, op, re, bd, e) as oe) | (`IncrDiff(sid, op, re, bd, e) as oe) ->
                  let diff = match oe with `Incr _ -> false | _ -> true in
		  let (e_code, e_decl) = gc_aux e decl bind_info in
		  let (e_is_bound, rc_l) = 
		      match bind_info with
                          | (true, bb) -> is_binding_used e bb
			  | _ -> (false, [])
		  in
                      print_endline ("is_bound "^
                          (string_of_map_expression oe)^" "^(string_of_bool e_is_bound));

                      (* test for rule 67-70 *)
		      if e_is_bound then
                          (* TODO: support multiple bindings for rules 67-70 *)
                          (* Note bound variables should already be declared and assigned.
                           * This currently happens in compile_target *)
                          let (rc, rc_m) = List.hd rc_l in
			  let rc_type = type_inf_mexpr rc_m base_rels [event] decl in
 		          let mid = gen_map_sym sid in
		  	  let mid_type = type_inf_mexpr e base_rels [event] decl in	
		          let c = gen_var_sym() in
		          let map_decl = `Map(mid, [(c, rc_type)], mid_type) in 
		          let map_key = (mid, [rc]) in
                          let update_map_key = (mid, [c]) in
                          let new_decl =
                              if List.mem map_decl e_decl then e_decl else e_decl@[map_decl]
                          in
		              match event with 
			          | `Insert (_,_) ->
                                        print_endline ("new map: "^mid^" rc: "^rc^" c: "^c);
                                        print_endline ("e: \n"^(indented_string_of_map_expression e 0));
                                        let update_code =
                                            (* substitute binding var for map local var *)
                                            let substituted_code = 
                                                substitute_code_vars ([`Assign(rc, `CTerm(`Variable(c)))]) e_code
                                            in
                                                gc_incr_state_map substituted_code update_map_key op diff
                                        in
                                        let (insert_code, insert_decl) =
                                            (* use recomputation code rather than delta code *)
                                            let (re_code, re_decl) = gc_aux re new_decl (false, []) in
                                                (gc_assign_state_map re_code map_key, re_decl)
                                        in
				        let (update_and_init_binding_code, insert_wprof_decl) = 
                                            let prof_loc = generate_profile_id event in
			     	                (`Profile("cpu", prof_loc, 
                                                `Block ([
					            `ForEach (map_decl, update_code);
				 	            `IfNoElse (
                                                        `BCTerm (
                                                            `EQ(`CTerm(`MapContains(map_key)),
                                                            `CTerm(`MapIterator(`End(mid))))),
                                                        insert_code);
					            `Eval(`CTerm(`MapAccess(map_key)))])),
                                                (`ProfileLocation prof_loc)::insert_decl)
				        in
                                            (update_and_init_binding_code, insert_wprof_decl)

				  (* TODO: garbage collection, min *)	
			          | `Delete _ -> 
                                        let delete_code = gc_incr_state_map e_code map_key op diff in
                                        let update_code = gc_incr_state_map e_code update_map_key op diff in
				        let (delete_and_update_binding_code, delete_wprof_decl) = 
                                            let prof_loc = generate_profile_id event in
                                                (`Profile("cpu", prof_loc,
				                `Block ([
                                                    delete_code;
					            (* TODO: if S_f[rc] = f(R=0)[rc]) delete S_f[rc] *)
	 				            `ForEach (map_decl, update_code);
					            `Eval(`CTerm(`MapAccess(map_key)))])),
                                                (`ProfileLocation prof_loc)::new_decl)
				        in
                                            (delete_and_update_binding_code, delete_wprof_decl)

		      else
                          let (e_vars, _) = List.split(match event with
                              | `Insert(_,vars) | `Delete(_,vars) -> vars)
                          in
		          let e_uba = List.filter
			      (fun uaid -> not(List.mem (field_of_attribute_identifier uaid) e_vars))
			      (get_unbound_attributes_from_map_expression e true)
		          in
                          let e_uba_fields = List.map field_of_attribute_identifier e_uba in

                          (*
                          if List.mem_assoc oe termp_accessors then
                              gc_termp_accessor_code oe termp_accessors
                                  e e_code e_decl e_vars e_uba_fields op diff recursion_decls
                          else
                              begin
                                  let sorted_uba =
                                      List.sort compare
                                          (List.map field_of_attribute_identifier e_uba)
                                  in
                                      if (List.mem_assoc sorted_uba varp_accessors) then
                                          gc_varp_accessor_code sorted_uba varp_accessors
                                              e e_code e_decl e_vars e_uba_fields op diff recursion_decls
                                      else
                                          gc_declare_and_incr_state oe e_code e_decl e_uba op diff
                              end
                          *)

                              if List.mem_assoc sid state_p_decls then
                                  let par_decl = List.assoc sid state_p_decls in
                                      gc_state_accessor_code par_decl bd
                                          e e_code e_decl e_vars e_uba_fields op diff recursion_decls
                              else
                                  gc_declare_and_incr_state oe e_code e_decl e_uba op diff base_rels [event]

            (* TODO: generate map key from bindings *)
	    | (`Init (sid, bd, e) as oe) ->

		  let (e_code, e_decl) = gc_aux e decl bind_info in

                  (* Note: no need to filter handler args, since e should be a recomputation,
                   * i.e. a map_expression where deltas have not been applied *)
		  let e_uba = get_unbound_attributes_from_map_expression e true in
                      gc_declare_and_assign_state oe e_code e_decl e_uba base_rels [event]

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
                              (* Note: local renaming of fields *)
                              | [`Relation(n2,f2)] -> (`Relation(n2,f), decl)
                              | _ -> raise DuplicateException
		  in
		      (`ForEach(datastructure_of_declaration r_decl, iter_code), new_decl)

            | `Domain (sid, attrs) ->
                  let (domain_decl, new_decl) =
                      let dom_fields =
                          List.map
                              (fun x ->
                                  (field_of_attribute_identifier x,
                                  type_inf_mexpr (`METerm (`Attribute(x))) base_rels [event] decl))
                              attrs
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
			      let new_type = type_inf_mexpr (`METerm (`Attribute(aid))) base_rels [event] decl
                              in
                                  [`Declare(`Variable(new_var, new_type));
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
			  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
			  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                ->
				(* TODO: pred var types *)
				let (pred_var_code, new_decl) = gc_aux m_expr decl bind_info in
				let pred_var_type = type_inf_mexpr m_expr base_rels [event] decl in
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
                                                      print_endline ("Declaring pred var "^pv^" in "^(string_of_plan q));
                                                      print_endline ("pv_code:\n"^
                                                          (indented_string_of_code_expression pred_var_code));
                                                      print_endline ("x_code: "^(string_of_arith_code_expression x));
						      (`Variable(pv), Some(av_code), (`Variable(pv, pred_var_type))::new_decl)

                                            | _ ->
                                                  print_endline ("Invalid return val:\n"^
                                                      (indented_string_of_code_expression pred_var_code));
                                                  raise InvalidExpression
				in
				let pred_test_code =
                                    let pz_pair = (`CTerm(pred_cterm), `CTerm(`Int 0)) in
                                    let np =
					match pred with
					    | `BTerm(`MEQ _) -> `EQ(pz_pair)
					    | `BTerm(`MNEQ _) -> `NE(pz_pair)
					    | `BTerm(`MLT _) -> `LT(pz_pair)
					    | `BTerm(`MLE _) -> `LE(pz_pair)
					    | `BTerm(`MGT _) -> `GT(pz_pair)
					    | `BTerm(`MGE _) -> `GE(pz_pair)
					    | _ -> raise InvalidExpression
                                    in
                                        `BCTerm(np)
				in

				let new_iter_code =
				    match (m_expr, pred_cterm) with
                                         (* Note: only inserts have `Init expressions.
                                            Generate insert into base relations before recomputing as necessary *)
                                        | (`Init (_,_,e), `MapAccess(mf)) ->
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

                                        | (`Init (_,_,e), `Variable _) ->
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
                                let (profiled_new_iter_code, pred_wprof_decl) =
                                    let prof_loc = generate_profile_id event in
                                        (`Profile("cpu", prof_loc, new_iter_code),
                                        (`ProfileLocation prof_loc)::pred_decl)
                                in
				    gc_plan_aux cq profiled_new_iter_code pred_wprof_decl bind_info 

			  | _ ->
				let new_iter_code =
                                    `IfNoElse(create_code_predicate pred base_rels [event] decl, iter_code)
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
            (* TODO: handle unbound attributes in bindings bd *)
	    | `IncrPlan(sid, op, d, bd, nq) | `IncrDiffPlan(sid, op, d, bd, nq) ->
                  let print_incrp_debug q_code incrp_dom =
		      print_endline ("rule 39,41: incrp_dom "^(string_of_code_expression (`Declare incrp_dom)));
                      print_endline ("rule 39,41: iter_code "^(string_of_plan nq));
                      print_endline ("rule 39,41: q_code "^(indented_string_of_code_expression q_code));

                  in
		  let dom_id = gen_dom_sym (sid) in
                  let dom_field_names = List.map field_of_attribute_identifier d in
                  let dom_fields =
                      List.map
                          (fun x ->
                              (field_of_attribute_identifier x, 
		              type_inf_mexpr (`METerm (`Attribute(x))) base_rels [event] decl)) d
                  in
		  let dom_decl = `Domain(dom_id, dom_fields) in
                  let dom_ds = datastructure_of_declaration dom_decl in
                  let (new_iter_code, new_decl) =
                      let incr_code = match op with
                          | `Union -> `InsertTuple(dom_ds, dom_field_names)
                          | `Diff -> `DeleteTuple(dom_ds, dom_field_names)
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
		  print_endline ("gc_plan_aux: "^(string_of_plan q));
		  raise InvalidExpression
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
    print_endline (String.make 50 '-');
    print_endline "Generating binding bodies.";

    let (binding_bodies, binding_decls) =
        List.fold_left
            (fun (code_acc, decl_acc) b ->
                match b with
                    | `BindExpr (v, expr) ->
                          (code_acc@[`Assign(v, create_code_expression expr)],
                          decl_acc@[`Variable(v, type_inf_expr expr base_rels [event] decl_acc)])

                    | `BindMapExpr (v, m_expr) ->
                          let (binding_code,d) = gc_aux m_expr decl_acc (false, []) in
			  let binding_type = type_inf_mexpr m_expr base_rels [event] decl_acc in
                          let rv = get_return_val binding_code in
                              match rv with 
                                  | `Eval x ->
                                        (code_acc@[(replace_return_val binding_code (`Assign(v, x)))],
                                        d@[`Variable(v, binding_type)])
                                  | _ ->
                                        print_endline ("Invalid return val: "^(string_of_code_expression rv));
                                        raise InvalidExpression)
            ([], []) bindings
    in
    
    print_endline (String.make 50 '-');
    print_endline "Generating handler bodies.";

    let (handler_body, handler_decl) = gc_aux handler binding_decls (true, bindings) in


    (* Note: binding declarations are included in declaration_code *)
    let declaration_code = List.map (fun x -> `Declare(x)) handler_decl in
    let separate_global_declarations code =
	List.partition
	    (fun x -> match x with
		| `Declare(`Map _) | `Declare(`Relation _)
                | `Declare(`Domain _) | `Declare(`ProfileLocation _)
                      -> true
		| _ -> false)
            code
    in
        if body_only then
                let (global_decls, handler_code) =
                    separate_global_declarations
	                (declaration_code@binding_bodies@[handler_body])
                in
	            (global_decls, simplify_code (`Block(handler_code)))
        else
            begin
                let (handler_body_with_rv, result_type) = 
	            match get_return_val handler_body with
	                | `Eval x ->
                              (replace_return_val handler_body (`Return x),
                              type_inf_arith_expr x (declaration_code@binding_bodies@[handler_body]))
	                | _ ->
                              print_endline ("Invalid return val:\n"^
                                  (indented_string_of_code_expression handler_body));
                              raise InvalidExpression
                in
                let (global_decls, handler_code) =
                    separate_global_declarations
	                (declaration_code@binding_bodies@[handler_body_with_rv])
                in
	            (global_decls,
	            simplify_code
	                (`Handler(handler_name_of_event event,
	                handler_fields, result_type, handler_code)))
            end



(* map expression list ->
   (int * variable identifier) list -> (state_identifier * int) list ->
   plan list -> delta list ->
   (declaration list) * (variable_identifier * code_term) list * (state_identifier * declaration) list *)
let generate_map_declarations maps map_vars state_parents base_rels events =

    List.iter (fun me ->
        print_endline ("gmd: "^
            " hash:"^(string_of_int (dbt_hash me))^
            " map: "^(string_of_map_expression me)))
        maps;

    List.iter (fun (hv, var) ->
        print_endline ("gmd var: "^var^" hash: "^(string_of_int hv)))
        map_vars;

    List.iter (fun (sid, par, _) ->
        print_endline ("gmd sid: "^sid^" hash: "^(string_of_int par)))
        state_parents;

    print_endline ("# maps: "^(string_of_int (List.length maps))^
        ", #vars: "^(string_of_int (List.length map_vars)));

    let get_map_id m_expr me_uba_and_vars =
        let string_of_attrs attrs =
            List.fold_left (fun acc f -> acc^f)
                "" (List.map field_of_attribute_identifier attrs)
        in
        let (agg_prefix, agg_attrs) = match m_expr with
            | `MapAggregate(fn, f, q) ->
                  let f_uba = get_unbound_attributes_from_map_expression f false in
                      (begin match fn with | `Sum -> "s" | `Min -> "mn" | `Max -> "mx" end,
                      f_uba)
            | _ -> raise InvalidExpression
        in
            agg_prefix^"_"^
                (let r = string_of_attrs agg_attrs in
                    if (String.length r) = 0 then "1" else r)^"_"^
                (string_of_attrs me_uba_and_vars)
    in
    let maps_and_hvs = List.combine maps (List.map dbt_hash maps) in
    let (map_decls_and_cterms, var_cterms) =
        List.fold_left
            (fun (decl_acc, accessor_acc) (me, me_hv) ->
                let me_uba_w_vars = get_unbound_attributes_from_map_expression me true in
                let me_id = get_map_id me me_uba_w_vars in
                let (new_decl, _) =
                    gc_declare_state_for_map_expression me [] me_uba_w_vars me_id base_rels events
                in
                (* Handle mulitple uses of this map *)
                let vars_using_map =
                    let vum = List.map (fun (_,v) -> v)
                        (List.filter (fun (hv, _) -> hv = me_hv) map_vars)
                    in
                        print_endline ("Vars using map "^(string_of_int me_hv)^": "^
                            (List.fold_left (fun acc var ->
                                (if (String.length acc) = 0 then "" else acc^", ")^var) "" vum));
                        vum
                in

                (* Associate map key with each var using map *)
                let decl_map_key =
                    match new_decl with
                        | `Map(id,f,_) -> (id, let (r,_) = List.split f in r)
                        | _ -> raise (CodegenException ("Invalid map declaration:"^
                              (identifier_of_declaration new_decl)))
                in
                let new_accessors = List.map
                    (fun v -> (v, `MapAccess(decl_map_key))) vars_using_map
                in
                    (decl_acc@[(me_hv, (new_decl, `MapAccess(decl_map_key)))],
                        accessor_acc@new_accessors))
            ([], []) maps_and_hvs
    in
    let (new_stp_decls, stp_decls) = List.fold_left
        (fun (decl_acc, stp_acc) (sid, par, m) ->
            let is_map = List.mem_assoc par map_decls_and_cterms in
            let is_decl = if is_map then false else (List.mem_assoc par decl_acc) in
                match (is_map, is_decl) with
                    | (true, false) | (false, true) -> 
                          let decl =
                              if is_map then
                                  let (r, _) = List.assoc par map_decls_and_cterms in r
                              else
                                  List.assoc par decl_acc
                          in
                              (decl_acc, stp_acc@[(sid, decl)])

                    | (false, false) ->
                          let new_decl_var = gen_var_sym() in
			  let new_type = type_inf_mexpr_map m base_rels events maps map_vars in
                          let new_decl = `Variable(new_decl_var, new_type) in
                              print_endline ("Using newly declared var: "^new_decl_var^
                                  " for hash: "^(string_of_int par));
                              (decl_acc@[(par, new_decl)], stp_acc@[(sid, new_decl)])

                    | _ -> raise (CodegenException "Invalid parent: multiple declarations found."))

        ([],[]) state_parents
    in
    let all_decls =
        (List.map (fun (_, d) -> d) new_stp_decls)@
        (List.map (fun (_,(d,_)) -> d) map_decls_and_cterms)
    in
        (all_decls, var_cterms, stp_decls)


(*
 * Code generation constants
 *)

let generate_includes out_chan =
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let includes =
        ["// DBToaster includes.";
        "#include <cmath>";
        "#include <cstdio>";
        "#include <cstdlib>\n";
        "#include <iostream>";
        "#include <map>";
        "#include <list>";
        "#include <set>\n";
        "#include <tr1/tuple>";
        "#include <tr1/unordered_set>\n";
        "using namespace std;";
        "using namespace tr1;\n\n"]
    in
        output_string out_chan (list_code includes)


(************************************
 * Standalone engine+debugger generation
 ************************************)

(* TODO: move id generation to algebra.ml *)

(* unit -> stream name * stream id *)
let stream_ids = Hashtbl.create 10
let get_stream_id_name stream_name = "stream"^stream_name^"Id"

let generate_stream_id stream_name =
    let (id_name, id) =
        (get_stream_id_name stream_name, Hashtbl.length stream_ids)
    in
        Hashtbl.add stream_ids id_name id;
        (id_name, id)

let fun_obj_id_counter = ref 0
let generate_function_object_id handler_name =
    let r = !fun_obj_id_counter in
        incr fun_obj_id_counter;
        "fo_"^handler_name^"_"^(string_of_int r)

let generate_stream_engine_includes out_chan =
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let includes =
        ["\n\n// Stream engine includes.";
        "#include \"streamengine.h\"";
        "#include \"profiler.h\"";
        "#include \"datasets/adaptors.h\"";
        "#include \"boost/bind.hpp\"\n\n";
        "using namespace DBToaster::Profiler;\n"; ]
    in
        output_string out_chan (list_code includes)
    

(* Generates init() and main() methods for standalone stream engine for compiled queries.
 * source metadata: (stream type * tuple type * stream name * handler * event) list
 * source metadata -> unit
*)
let generate_stream_engine_init out_chan streams_handlers_and_events =
    let indent s = ("    "^s) in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let get_handler_metadata handler adaptor_type adaptor_bindings =
        match handler with
            | `Handler(fid, args, ret_type, _) ->
                  let input_metadata =
                      let ifa =
                          (fun input_instance ->
                              List.fold_left
                                  (fun acc (id, ty) ->
                                      if (List.mem_assoc id adaptor_bindings) then
                                          let in_field = List.assoc id adaptor_bindings in
                                              (if (String.length acc) = 0 then "" else acc^",")^
                                                  (input_instance^"."^in_field)
                                      else raise (CodegenException
                                          ("Could not find input arg binding for "^id)))
                                  "" args)
                      in
                          (adaptor_type^"::Result", ifa)
                  in
                      (fid, input_metadata, ctype_of_type_identifier ret_type)

            | _ -> raise (CodegenException
                  ("Invalid handler: "^(string_of_code_expression handler)))
    in
    let (init_decls, init_body) =
        List.fold_left
            (fun (decls_acc, init_body_acc) (stream_type_info, stream_name, handler, event) ->

                (* Type assumptions: 
                   -- adaptor constructor: adaptor()
                *)
                let (source_type, source_args, tuple_type, adaptor_type, adaptor_bindings, _) = stream_type_info in

                (* add streams to multiplexer
                   -- instantiate input stream sources.
                   -- instantiate adaptors.
                   -- initialize (i.e. buffer data for) input streams
                   -- generate stream id, register with multiplexer via
                   add_stream(stream id, input stream)
                *)

                (* declare_stream: <stream type> <stream inst>; static int <stream id name> = <stream id val>;
                 * register_stream: sources.addStream< <tuple_type> >(&<stream inst>, <stream id name>);
                 * stream_id: <stream id name>
                 *)

                let (declare_stream, register_stream, stream_id) =
                    let new_decl = source_type^" "^stream_name^"("^source_args^");\n" in
                        if List.mem new_decl decls_acc then
                            ([], [], get_stream_id_name stream_name)
                        else
                            let (id_name, id) = generate_stream_id stream_name in
                            let adaptor_name = stream_name^"_adaptor" in
                            let new_adaptor =
                                "boost::shared_ptr<"^adaptor_type^"> "^
                                    adaptor_name^"(new "^adaptor_type^"());"
                            in
                            let new_id = ("static int "^id_name^" = "^(string_of_int id)^";\n") in
                            let stream_pointer = "&"^stream_name in
                            let adaptor_deref = "*"^adaptor_name in
                            let reg =
                                "sources.addStream<"^tuple_type^">("^stream_pointer^", "^adaptor_deref^", "^id_name^");"
                            in
                                ([ new_decl; new_adaptor; new_id ], [reg], id_name)
                in

                let (handler_name, handler_input_metadata, handler_ret_type) =
                    get_handler_metadata handler adaptor_type adaptor_bindings
                in
                let (handler_input_type_name, handler_input_args) =
                    handler_input_metadata
                in
                let handler_dml_type = match event with
                    | `Insert _ -> "DBToaster::StandaloneEngine::insertTuple"
                    | `Delete _ -> "DBToaster::StandaloneEngine::deleteTuple"
                in

                (* add handlers to dispatcher
                   -- create function object with operator()(boost::any data)
                   -- cast from boost::any to expected struct, and invoke handler with struct fields.
                   -- register handler with dispatcher for a given stream, via
                   add_handler(stream id, dml type, function object)
                *)

                (* TODO: allocation model for inputs? *)
                (* declare_handler_fun_obj:
                   struct <fun obj type name>
                   { <handler ret type> operator() { cast; invoke; } }
                 * fun_obj_type: <fun obj type name> *)
                let (declare_handler_fun_obj, fun_obj_type) = 
                    let input_instance = "input" in
                    let fo_type = handler_name^"_fun_obj" in
                        ([("struct "^fo_type^" { ");
                          (indent (handler_ret_type^" operator()(boost::any data) { "));
                          (indent (indent
                              (handler_input_type_name^" "^input_instance^" = ")));
                          (indent (indent (indent ("boost::any_cast<"^handler_input_type_name^">(data); "))));
                          (indent (indent
                              (handler_name^"("^
                                  (handler_input_args input_instance)^");")));
                          (indent "}"); "};\n"],
                        fo_type)
                in

                (* declare_handler_fun_obj_inst: <fo_type> <fo instance name>; *)
                let (declare_handler_fun_obj_inst, handler_fun_obj_inst) =
                    let h_inst = generate_function_object_id handler_name in
                    let decl_code = [ handler_name^"_fun_obj "^h_inst^";\n" ] in
                        (decl_code, h_inst)
                in
                (* register_handler:
                   router.addHandler(<stream id name>, handler dml type, <fo instance name>) *)
                let register_handler =
                    "router.addHandler("^stream_id^","^handler_dml_type^","^handler_fun_obj_inst^");"
                in
                    (decls_acc@declare_stream@
                        declare_handler_fun_obj@declare_handler_fun_obj_inst,
                    init_body_acc@register_stream@[register_handler]))
            ([], []) streams_handlers_and_events
    in
    (* void init(multiplexer, sources) { register stream, handler;} *)
    let init_defn =
        let init_code =
            ["\n\nvoid init(DBToaster::StandaloneEngine::Multiplexer& sources,";
             (indent "DBToaster::StandaloneEngine::Dispatcher& router)"); "{";]@
                (List.map indent init_body)@[ "}\n\n"; ]
        in
            list_code init_code
    in
        output_string out_chan (list_code init_decls);
        output_string out_chan init_defn


(* TODO: generate standalone profiler running an event loop in its own thread *)
let generate_stream_engine_main out_chan =
    (* main() { declare multiplexer, dispatcher; loop over multiplexer, dispatching; }  *)
    let indent s = ("    "^s) in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let block l = ["{"]@(List.map indent l)@["}"] in
    let main_defn =
        let main_code =
            ["int main(int argc, char** argv)"]@
                (block
                    (["DBToaster::StandaloneEngine::Multiplexer sources(12345, 20);";
                    "DBToaster::StandaloneEngine::Dispatcher router;";
                    "PROFILER_ENGINE_IMPLEMENTATION;\n";
                    "init(sources, router);";
                    "while ( sources.streamHasInputs() ) {";
                    (indent "DBToaster::StandaloneEngine::DBToasterTuple t = sources.nextInput();");
                    (indent "router.dispatch(t);");
                    "}"]))
        in
            list_code main_code
    in
        output_string out_chan main_defn

(* Standalone stepper/debugger *)

(*
 * Thrift helpers
 *)

(* Note these two functions are asymmetric *)
let thrift_type_of_base_type t =
    match t with
        | "int" -> "i32"
        | "float" -> "double"
        | "long" -> "i64"
        | "string" -> "string"
        | _ -> raise (CodegenException ("Unsupported base type "^t))

(* TODO: extend to support map, list, set *)
let ctype_of_thrift_type t =
    match t with
        | "i32" -> "int32_t"
        | "double" -> "double"
        | "i64" -> "long long"
        | "string" -> "string"
        | "bool" -> "bool"
        | "byte" | "i16" | "binary"
        | _ ->
              raise (CodegenException ("Unsupported Thrift type: "^t))

let thrift_type_of_datastructure d =
    match d with
        | `Variable(n,typ) -> thrift_type_of_base_type typ
	| `Map (n,f,r) ->
	      let key_type = 
                  match f with
                      | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                      | [(x,y)] -> thrift_type_of_base_type y
                      | _ -> n^"_key"
	      in
		  "map<"^key_type^","^(thrift_type_of_base_type (ctype_of_type_identifier r))^">"

        | (`Set(n, f) as ds) 
	| (`Multiset (n,f) as ds) ->
	      let el_type = 
		  if (List.length f) = 1
                  then thrift_type_of_base_type (let (_,ty) = List.hd f in ty)
                  else n^"_elem"
	      in
              let ds_type =
                  match ds with | `Set _ -> "set" | `Multiset _ -> "list"
              in
		  ds_type^"<"^el_type^">"


let ctype_of_thrift_datastructure d =
    match d with
        | `Variable(n,typ) -> thrift_type_of_base_type typ
	| `Map (n,f,r) ->
	      let key_type = 
                  match f with
                      | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                      | [(x,y)] -> ctype_of_thrift_type (thrift_type_of_base_type y)
                      | _ -> n^"_key"
	      in
              let rt_thrift_type =
                  thrift_type_of_base_type (ctype_of_type_identifier r)
              in
		  "map<"^key_type^","^(ctype_of_thrift_type rt_thrift_type)^">"

        | (`Set(n, f) as ds) 
	| (`Multiset (n,f) as ds) ->
	      let el_type = 
		  if (List.length f) = 1
                  then ctype_of_thrift_type (thrift_type_of_base_type (let (_,ty) = List.hd f in ty))
                  else n^"_elem"
	      in
              let ds_type =
                  match ds with | `Set _ -> "set" | `Multiset _ -> "vector"
              in
		  ds_type^"<"^el_type^">"

let thrift_element_type_of_datastructure d =
    match d with
        | `Variable(n,typ) ->
              raise (CodegenException
                  ("Invalid datastructure for elements: "^n))

	| `Map (n,f,r) ->
	      let key_type = 
                  match f with
                      | [] -> raise (CodegenException "Invalid datastructure type, no fields found.")
                      | [(x,y)] -> thrift_type_of_base_type y
                      | _ -> n^"_key"
	      in
		  "pair<"^key_type^","^(ctype_of_type_identifier r)^">"

        | `Set(n, f)
	| `Multiset (n,f) ->
	      let el_type = 
		  if (List.length f) = 1
                  then thrift_type_of_base_type (let (_,ty) = List.hd f in ty)
                  else n^"_elem"
	      in
                  el_type


let thrift_inner_declarations_of_datastructure d =
    let indent s = ("    "^s) in
    let field_declarations f =
        let (_, r) =
            List.fold_left (fun (counter, acc) (id,ty) ->
                let field_decl =
                    ((string_of_int counter)^": "^
                        (thrift_type_of_base_type (ctype_of_type_identifier ty))^
                        " "^id^",")
                in
                    (counter+1, acc@[field_decl]))
                (1, []) f
        in
            r
    in
        match d with
            | `Variable(n,typ) -> raise (CodegenException
                  ("Datastructure has no inner declarations: "^n))

	    | (`Map (n,f,_) as ds)
            | (`Set(n, f) as ds)
	    | (`Multiset (n,f) as ds) ->
                  if (List.length f) = 1 then
                      ("", [])
                  else
	              let field_decls = field_declarations f in
                      let decl_name = match ds with
                          | `Map _ -> n^"_key" | `Set _ | `Multiset _ -> n^"_elem"
                      in
                      let struct_decl =
                          ["struct "^decl_name^" {"]@(List.map indent field_decls)@["}\n"]
                      in
		          (decl_name, struct_decl)


let copy_element_for_thrift d dest src =
    let copy_tuple f dest src = 
        List.fold_left (fun (counter,acc) (id, ty) ->
            (counter+1, acc@[dest^"."^id^" = get<"^(string_of_int counter)^">("^src^");"]))
            (0, []) f
    in
        match d with
            | `Variable(n,typ) -> raise (CodegenException "Invalid element copy.")
	    | `Map (n,f,r) ->
                  (* Copy from pair< tuple<>, <ret type> > -> pair< struct, <ret type> > *)
                  let (key_dest_decl, key_dest) = ([n^"_key key;"], "key") in
                  let (_, copy_tuple_defn) = copy_tuple f key_dest (src^".first") in
                  let ret_src = src^".second" in
                      key_dest_decl@
                      copy_tuple_defn@
                      [dest^" = make_pair("^key_dest^","^ret_src^");"]

            | `Set(n, f)
	    | `Multiset (n,f) ->
                  (* Copy from tuple<> -> struct *)
                  let (_,r) = copy_tuple f dest src in r


let thrift_inserter_of_datastructure d =
    let indent s = ("    "^s) in
        match d with
	    | `Map (n,f,_)
            | `Set(n, f)
	    | `Multiset (n,f) ->
                  let inserter_body =
                      if (List.length f) = 1 then
                          [(indent "dest.insert(dest.begin(), src);")]
                      else
                          let elem_type = thrift_element_type_of_datastructure d in
                          let copy_fields dest_var src_var =
                              copy_element_for_thrift d dest_var src_var
                          in
                              (List.map indent
                                  ([elem_type^" r;"]@
                                      (copy_fields "r" "src")@
                                      ["dest.insert(dest.begin(), r);"]))
                  in
                  let inserter_name = "insert_thrift_"^n in
                  let inserter_defn =
                      let ttype = ctype_of_thrift_datastructure d in
                      let elem_ctype = element_ctype_of_datastructure d in
                          [("inline void "^inserter_name^"("^
                              ttype^"& dest, "^elem_ctype^"& src)"); "{" ]@
                              inserter_body@
                              ["}\n"]
                  in
                      (inserter_defn, inserter_name)


(* Assumes each tuple type has a matching thrift definition named:
   Thrift<tuple type>, e.g. ThriftVwapTuple *)
(* TODO: define and use structs as complex map keys rather than tuples,
   or convert tuples to structs here in Thrift *)

let generate_stream_debugger_includes out_chan =
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let includes =
        ["// Thrift includes";
        "#include \"Debugger.h\"";
        "#include <protocol/TBinaryProtocol.h>";
        "#include <server/TSimpleServer.h>";
        "#include <transport/TServerSocket.h>";
        "#include <transport/TBufferTransports.h>\n";
        "using namespace apache::thrift;";
        "using namespace apache::thrift::protocol;";
        "using namespace apache::thrift::transport;";
        "using namespace apache::thrift::server;\n";
        "using boost::shared_ptr;\n\n"]
    in
        output_string out_chan (list_code includes)


let generate_stream_debugger_class
        decl_out_chan impl_out_chan
        query_id global_decls streams_handlers_and_events
    =

    (* Generate stream engine preamble: stream definitions and dispatcher function objects,
     * multiplexer, dispatcher 
     * Note: this initializes stream dispatching identifiers which are need for the
     * Thrift protocol spec file, hence the invocation prior to protocol generation. *)
    generate_stream_engine_init impl_out_chan streams_handlers_and_events;

    (* Generate Thrift protocol spec file
       -- void step(tuple)
       -- void stepn(int n)
       -- state/map accessors *)
    let indent s = ("    "^s) in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let block l = ["{"]@(List.map indent l)@["}"] in
    let strip_namespace type_name =
        let ns_and_type = Str.split (Str.regexp "::") type_name in
            List.nth ns_and_type (List.length ns_and_type - 1)
    in
    let streams = List.fold_left
        (fun acc (stream_type_info, stream_name, handler, event) ->
            if (List.mem_assoc stream_name acc) then acc
            else acc@[(stream_name, stream_type_info)])
        [] streams_handlers_and_events
    in
    let stream_id_decls = 
        Hashtbl.fold
            (fun stream_id_name stream_id acc ->
                acc@["const i32 "^stream_id_name^" = "^(string_of_int stream_id)])
            stream_ids []
    in
    let (tuple_decls, steps_decls, stepns_decls) =
        List.fold_left
            (fun (td_acc, step_acc, stepn_acc)
                (stream_name,
                    (source_type, _, tuple_type,
                    adaptor_type, adaptor_bindings,
                    thrift_tuple_namespace))
                ->
                let (thrift_tuple_decl, thrift_tuple_type) =
                    let normalized_tuple_type = (strip_namespace tuple_type) in
                    let new_type = "Thrift"^normalized_tuple_type in
                        if (List.mem_assoc new_type td_acc) then
                            ([], new_type)
                        else
                            let tt_prefix =
                                if (String.length thrift_tuple_namespace) > 0
                                then (thrift_tuple_namespace^".") else ""
                            in
                            let new_decl = list_code
                                ([ "struct "^new_type^" {"]@
                                    (List.map indent
                                        (["1: DmlType type,";
                                        "2: DBToasterStreamId id,";
                                        "3: "^tt_prefix^normalized_tuple_type^" data"]))@
                                    [ "}\n"])
                            in
                                ([(new_type, new_decl)], new_type)
                in
                let step_stream = "void step_"^(stream_name)^"(1:"^thrift_tuple_type^" input)," in
                let stepn_stream = "void stepn_"^(stream_name)^"(1:i32 n)," in
                    (td_acc@thrift_tuple_decl, step_acc@[step_stream], stepn_acc@[stepn_stream]))
        ([], [], []) streams
    in
    let (key_decls, accessors_decls) =
        List.fold_left (fun (key_acc, accessor_acc) d ->
            let (key_decl, accessor_decl) =
                match d with
                    | `Declare x ->
                          begin
                              match x with
		                  | `Variable(n, typ) -> 
                                        let ttype = thrift_type_of_base_type typ in
                                            ([], ttype^" get_"^n^"(),")

                                  | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                        let ds = datastructure_of_declaration y in
                                        let n = identifier_of_datastructure ds in
                                        let ttype = thrift_type_of_datastructure ds in
                                        let (inner_decl_name, inner_decl) =
                                            thrift_inner_declarations_of_datastructure ds
                                        in
                                        let acs_decl = ttype^" get_"^n^"()," in
                                            if (List.length inner_decl) > 0 then
                                                ([(inner_decl_name, list_code inner_decl)], acs_decl)
                                            else
                                                ([], acs_decl)

                                  | `ProfileLocation _ -> ([], "")
                          end
                    | _ -> raise (CodegenException
                          ("Invalid declaration: "^(indented_string_of_code_expression d)))
            in
                if key_decl = [] && accessor_decl = "" then
                    (key_acc, accessor_acc)
                else
                    (key_acc@key_decl, accessor_acc@[accessor_decl]))
            ([],[]) global_decls
    in
    let service_namespace =
        "DBToaster.Debugger"^(if query_id = "" then "" else ("."^query_id))
    in
    let cpp_service_namespace =
        "DBToaster::Debugger"^(if query_id = "" then "" else ("::"^query_id))
    in
    let service_decl =
        ["include \"datasets.thrift\"\n";
        "include \"profiler.thrift\"\n";
        ("namespace cpp "^service_namespace);
        ("namespace java "^(service_namespace)^"\n");
        "typedef i32 DBToasterStreamId";
        "enum DmlType { insertTuple = 0, deleteTuple = 1 }\n"]@
        stream_id_decls@
        (let (_,r) = List.split tuple_decls in r)@
        (let (_,r) = List.split key_decls in r)@
        ["service Debugger extends profiler.Profiler {"]@
        (List.map indent steps_decls)@
        (List.map indent stepns_decls)@
        (List.map indent accessors_decls)@
        ["}"]
    in

    (* Generate Thrift service implementation
       -- instantiate + initialize multiplexer, dispatcher
       -- void step(tuple) { dispatch(tuple); }
       -- void stepn(int n) { for i=0:n-1 { dispatch(multiplexer->nextInput()); } }
       -- state/map accessors
       -- Copy Thrift service main from skeleton
    *)
    let class_name = "DebuggerHandler" in
    let (steps_defns, stepns_defns) = 
        List.fold_left
            (fun (step_acc, stepn_acc)
                (stream_name,
                    (source_type, _, tuple_type,
                    adaptor_type, adaptor_bindings, _))
                ->
                let normalized_tuple_type = (strip_namespace tuple_type) in
                let thrift_tuple_type = "Thrift"^normalized_tuple_type in
                 (* Create a DBToasterTuple from argument, and invoke dispatch *)
                let step_stream_body = 
                    ["void step_"^stream_name^"(const "^thrift_tuple_type^"& input)"]@
                        (block
                            (["DBToaster::StandaloneEngine::DBToasterTuple dbtInput;";
                            "dbtInput.id = input.id;";
                            "dbtInput.type = static_cast<DBToaster::StandaloneEngine::DmlType>(input.type);";
                            "dbtInput.data = boost::any(input.data);";
                            "router.dispatch(dbtInput);" ]))
                in
                (* Read n tuples from the stream *)
                let stepn_stream_body =
                    ["void stepn_"^stream_name^"(const int32_t n)"]@
                        (block
                            (["for (int32_t i = 0; i < n; ++i)"]@
                                (block 
                                    (["if ( !sources.streamHasInputs() ) break;";
                                    "DBToaster::StandaloneEngine::DBToasterTuple t "^
                                        "= sources.nextInput();";
                                    "router.dispatch(t);"]))))
                in
                    (step_acc@step_stream_body, stepn_acc@stepn_stream_body))
            ([], []) streams
    in
    let accessor_defns =
        List.fold_left (fun acc d ->
            let new_defn =
                match d with
                    | `Declare x ->
                          begin
                              match x with
		                  | `Variable(n, typ) -> 
                                        (* Return declared variable *)
                                        let ttype = thrift_type_of_base_type typ in
                                        let dtype = ctype_of_thrift_type ttype in
                                            [dtype^" get_"^n^"()"; "{" ]@
                                                (List.map indent
                                                    ([dtype^" r = static_cast<"^dtype^">("^n^");";
                                                    "return r;"]))@
                                                [ "}\n" ]

                                  | (`Relation _ as y) | (`Map _ as y) | (`Domain _ as y) ->
                                        let ds = datastructure_of_declaration y in
                                        let n = identifier_of_datastructure ds in
                                        let ttype = ctype_of_thrift_datastructure ds in
                                        let (inserter_defn, inserter_name) = thrift_inserter_of_datastructure ds in
                                        let inserter_mem_fn =
                                            "boost::bind(&"^class_name^"::"^inserter_name^", this, _return, _1)"
                                        in
                                        let (begin_it, end_it, begin_decl, end_decl) =
		                            range_iterator_declarations_of_datastructure ds
                                        in
                                            (* Copy datastructure into _return *)
                                            inserter_defn@
                                            ["void get_"^n^"("^ttype^"& _return)"; "{"]@
                                                (List.map indent
                                                    ([(begin_decl^" = "^(identifier_of_datastructure ds)^".begin();");
                                                    (end_decl^" = "^(identifier_of_datastructure ds)^".end();");
                                                    "for_each("^begin_it^", "^end_it^",";
                                                    (indent inserter_mem_fn)^");"]))@
                                                ["}\n"]
                                  | `ProfileLocation _ -> []
                          end
                    | _ -> raise (CodegenException
                          ("Invalid declaration: "^(indented_string_of_code_expression d)))
            in
                acc@new_defn)
            [] global_decls
    in
    let impl_code =
        let internals =
            (* Local datastructures and constructor *)
            List.map indent
                (["DBToaster::StandaloneEngine::Multiplexer& sources;";
                "DBToaster::StandaloneEngine::Dispatcher& router;\n"])
        in
        let constructor =
            List.map indent
                (["public:";
                class_name^"(";
                (indent (indent "DBToaster::StandaloneEngine::Multiplexer& s,"));
                (indent (indent "DBToaster::StandaloneEngine::Dispatcher& r)"));
                (indent (": sources(s), router(r)"));
                "{"; (indent "PROFILER_INITIALIZATION"); "}\n"])
        in
        ["using namespace "^cpp_service_namespace^";\n";
        ("class "^class_name^" : virtual public DebuggerIf"); "{"]@
            internals@
            constructor@
            (List.map indent
                (steps_defns@["\n"]@
                stepns_defns@["\n"]@
                accessor_defns@
                ["PROFILER_SERVICE_METHOD_IMPLEMENTATION"]))@
            ["};\n\n"]
    in
        output_string decl_out_chan (list_code service_decl);
        output_string impl_out_chan (list_code impl_code);
        class_name


let generate_stream_debugger_main impl_out_chan stream_debugger_class =
    let indent s = ("    "^s) in
    let list_code l =
        List.fold_left
            (fun acc c ->
                (if (String.length acc) = 0 then "" else acc^"\n")^c)
            "" l
    in
    let block l = ["{"]@(List.map indent l)@["}"] in
    let main_defn =
        ["int main(int argc, char **argv) "]@
            (block 
                (["DBToaster::StandaloneEngine::Multiplexer sources(12345, 20);";
                "DBToaster::StandaloneEngine::Dispatcher router;";
                "init(sources, router);\n";
                "int port = (70457>>3);";
                "shared_ptr<"^stream_debugger_class^"> handler(new "^stream_debugger_class^"(sources, router));";
                "shared_ptr<TProcessor> processor(new DebuggerProcessor(handler));";
                "shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));";
                "shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());";
                "shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());";
                "TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);";
                "server.serve();";
                "return 0;";]))
    in
        output_string impl_out_chan (list_code main_defn)


(*
 * File I/O generation
 *)
let generate_fileio_includes out_chan =
    output_string out_chan "#include <fstream>\n";
    output_string out_chan "#include <boost/tokenizer.hpp>\n\n";
    output_string out_chan "using namespace boost;\n\n";
    output_string out_chan "typedef tokenizer <char_separator<char> > tokeniz;\n\n"


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
	        "        "^id^".insert(r);\n"^
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



(***************************
 * Stale code
 ***************************)

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

(*
let gc_termp_accessor_code incr_e termp_accessors e e_code e_decl e_vars e_uba_fields op diff recursion_decls =
    let par_ct = List.assoc incr_e termp_accessors in
        match par_ct with
            | `Variable(v) ->
                  if (List.mem v e_uba_fields) then
                      begin
                          print_endline ("Could not find declaration for "^v);
                          raise InvalidExpression
                      end
                  else (gc_incr_state_var e_code v op diff, e_decl)

            | `MapAccess((mid, mf) as mk) ->
                  (gc_foreach_map e e_code e_decl e_vars e_uba_fields mk op diff recursion_decls, e_decl)

let gc_varp_accessor_code sorted_uba varp_accessors e e_code e_decl e_vars e_uba_fields op diff recursion_decls =
    let par_access = List.assoc sorted_uba varp_accessors in
        match par_access with
            | `Variable(v) ->
                  if (List.mem v e_uba_fields) then
                      begin
                          print_endline ("Could not find declaration for "^v);
                          raise InvalidExpression
                      end
                  else
                      (gc_incr_state_var e_code v op diff, e_decl)

            | `MapAccess((mid, mf) as mk) ->
                  print_endline ("Var group: "^(string_of_code_var_list e_uba_fields));
                  (gc_foreach_map e e_code e_decl e_vars e_uba_fields mk op diff recursion_decls, e_decl)
*)

(* TODO: update sig *)
(* map expression list -> (int * variable identifier) list ->
   (declaration list) *  (var * code terminal) list * (var list * code terminal) list*)
(*
let generate_map_declarations maps map_vars state_parents vars_parents term_parents =

    List.iter (fun me ->
        print_endline ("gmd: "^
            " hash:"^(string_of_int (dbt_hash me))^
            " map: "^(string_of_map_expression me)))
        maps;

    List.iter (fun (hv, var) ->
        print_endline ("gmd var: "^var^" hash: "^(string_of_int hv)))
        map_vars;

    List.iter (fun (sid, par) ->
        print_endline ("gmd sid: "^sid^" hash: "^(string_of_int par)))
        state_parents;

    List.iter (fun (vg, (par, unif)) ->
        print_endline ("gmd vg: "^(string_of_code_var_list vg)^
            " par: "^(string_of_int par)))
        vars_parents;

    List.iter (fun (te, (par, unif)) ->
        print_endline ("gmd tp: "^(string_of_map_expression te)^
            " par: "^(string_of_int par)))
        term_parents;

    print_endline ("# maps: "^(string_of_int (List.length maps))^
        ", #vars: "^(string_of_int (List.length map_vars)));

    let get_map_id m_expr me_uba_and_vars =
        let string_of_attrs attrs =
            List.fold_left (fun acc f -> acc^f)
                "" (List.map field_of_attribute_identifier attrs)
        in
        let (agg_prefix, agg_attrs) = match m_expr with
            | `MapAggregate(fn, f, q) ->
                  let f_uba = get_unbound_attributes_from_map_expression f false in
                      (begin match fn with | `Sum -> "s" | `Min -> "mn" | `Max -> "mx" end,
                      f_uba)
            | _ -> raise InvalidExpression
        in
            agg_prefix^"_"^
                (let r = string_of_attrs agg_attrs in
                    if (String.length r) = 0 then "1" else r)^"_"^
                (string_of_attrs me_uba_and_vars)
    in
    let maps_and_hvs = List.combine maps (List.map dbt_hash maps) in
    let (map_decls_and_cterms, var_cterms) =
        List.fold_left
            (fun (decl_acc, accessor_acc) (me, me_hv) ->
                let me_uba_w_vars = get_unbound_attributes_from_map_expression me true in
                let me_id = get_map_id me me_uba_w_vars in
                let (new_decl, _) =
                    gc_declare_state_for_map_expression me [] me_uba_w_vars me_id
                in
                ** Handle mulitple uses of this map **
                let vars_using_map =
                    let vum = List.map (fun (_,v) -> v)
                        (List.filter (fun (hv, _) -> hv = me_hv) map_vars)
                    in
                        print_endline ("Vars using map "^(string_of_int me_hv)^": "^
                            (List.fold_left (fun acc var ->
                                (if (String.length acc) = 0 then "" else acc^", ")^var) "" vum));
                        vum
                in

                ** Associate map key with each var using map **
                let decl_map_key =
                    match new_decl with
                        | `Map(id,f,_) -> (id, let (r,_) = List.split f in r)
                        | _ -> raise (CodegenException ("Invalid map declaration:"^
                              (identifier_of_declaration new_decl)))
                in
                let new_accessors = List.map
                    (fun v -> (v, `MapAccess(decl_map_key))) vars_using_map
                in
                    (decl_acc@[(me_hv, (new_decl, `MapAccess(decl_map_key)))],
                        accessor_acc@new_accessors))
            ([], []) maps_and_hvs
    in
    ** (int * declaration) list * code terminal list **
    let (vp_hvs_and_decls, vp_cterms) =
        List.fold_left
            (fun (hv_decl_acc, vpk_acc) (vg, (phv, unif)) ->
                if not(List.mem_assoc phv map_decls_and_cterms) then
                    begin
                        print_endline ("Failed to find map key for: "^(string_of_int phv));
                        let (decl_var, new_hv_decl_acc) =
                            if (List.mem_assoc phv hv_decl_acc) then
                                begin
                                    let phv_decl = List.assoc phv hv_decl_acc in
                                    let phv_var = match phv_decl with
                                        | `Variable(v,_) -> v | _ -> raise InvalidExpression
                                    in
                                        print_endline ("Using var: "^phv_var^
                                            " for hash: "^(string_of_int phv));
                                    (phv_var, hv_decl_acc)
                                end
                            else
                                begin
                                    let new_decl_var = gen_var_sym() in
                                        print_endline ("Using newly declared var: "^new_decl_var^
                                            " for hash: "^(string_of_int phv));
                                        (new_decl_var,
                                            hv_decl_acc@[(phv, `Variable(new_decl_var, "int"))])
                                end
                        in
                            (new_hv_decl_acc, vpk_acc@[(vg, `Variable(decl_var))])
                    end
                else 
                    let (_,ct) = List.assoc phv map_decls_and_cterms in
                        match ct with
                            | `MapAccess((mid, mf)) ->
                                  let new_mf = List.map
                                      (fun f ->
                                          let f_matches = List.filter
                                              (fun (aid, e) -> f = (field_of_attribute_identifier aid)) unif
                                          in
                                              match f_matches with
                                                  | [] -> f
                                                  | [(_, `ETerm(`Variable(v)))] -> v
                                                  | _ -> raise DuplicateException)
                                      mf
                                  in
                                  let new_ct = `MapAccess((mid, new_mf)) in
                                      print_endline ("Using map key: "^
                                          (string_of_code_expr_terminal new_ct)^
                                          " for hash: "^(string_of_int phv));
                                      (hv_decl_acc, vpk_acc@[(vg, new_ct)])
                            | _ -> raise (CodegenException "Invalid cterm"))
            ([], []) vars_parents
    in
    let (tp_decls, tp_cterms) =
        List.fold_left (fun (tp_decl_acc, tp_acc) (te, (par, unif)) ->
            if not(List.mem_assoc par map_decls_and_cterms) then
                if not(List.mem_assoc par vp_hvs_and_decls) then
                    begin
                        ** Generate new variable for query result **
                        let new_decl_var = gen_var_sym () in
                            print_endline ("Using newly declared query var: "^new_decl_var^
                                " for hash: "^(string_of_int par));
                            (tp_decl_acc@([`Variable(new_decl_var, "int")]),
                                (tp_acc@[(te, `Variable(new_decl_var))]))
                    end
                else
                    let var_decl = List.assoc par vp_hvs_and_decls in
                    let var_cterm = match var_decl with | `Variable(n,_) -> `Variable(n) in
                        (tp_decl_acc, tp_acc@[(te, var_cterm)])
            else
                let (_,ct) = List.assoc par map_decls_and_cterms in
                    match ct with
                        | `MapAccess((mid, mf)) ->
                              let new_mf = List.map
                                  (fun f ->
                                      let f_matches = List.filter
                                          (fun (aid, e) -> f = (field_of_attribute_identifier aid)) unif
                                      in
                                          match f_matches with
                                              | [] -> f
                                              | [(_, `ETerm(`Variable(v)))] -> v
                                              | _ -> raise DuplicateException)
                                  mf
                              in
                              let new_ct = `MapAccess((mid, new_mf)) in
                                  print_endline ("Using map key: "^
                                      (string_of_code_expr_terminal new_ct)^
                                      " for hash: "^(string_of_int par));
                                  (tp_decl_acc, tp_acc@[(te, new_ct)])
                        | _ -> raise (CodegenException "Invalid cterm"))
            ([], []) term_parents
    in
    let (new_stp_decls, stp_decls) = List.fold_left
        (fun (decl_acc, stp_acc) (sid, par) ->
            let is_map = List.mem_assoc par map_decls_and_cterms in
            let is_decl = if is_map then false else (List.mem_assoc par decl_acc) in
                match (is_map, is_decl) with
                    | (true, false) | (false, true) -> 
                          let decl =
                              if is_map then
                                  let (r, _) = List.assoc par map_decls_and_cterms in r
                              else
                                  List.assoc par decl_acc
                          in
                              (decl_acc, stp_acc@[(sid, decl)])

                    | (false, false) ->
                          let new_decl_var = gen_var_sym() in
                          let new_decl = `Variable(new_decl_var, "int") in
                              print_endline ("Using newly declared var: "^new_decl_var^
                                  " for hash: "^(string_of_int par));
                              (decl_acc@[(par, new_decl)], stp_acc@[(sid, new_decl)])

                    | _ -> raise (CodegenException "Invalid parent: multiple declarations found."))

        ([],[]) state_parents
    in
    let all_decls =
        tp_decls@
        (List.map (fun (_, d) -> d) vp_hvs_and_decls)@
        (List.map (fun (_, d) -> d) new_stp_decls)@
        (List.map (fun (_,(d,_)) -> d) map_decls_and_cterms)
    in
        (all_decls, var_cterms, stp_decls, vp_cterms, tp_cterms)
*)
