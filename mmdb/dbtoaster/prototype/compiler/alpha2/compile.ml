open Algebra

exception DuplicateException
exception RewriteException of string

(* TODO:
 * -- apply_delta_plan_rules: handle conjunctive/disjunctive map expression
 * comparisons for both selections and joins
 * -- more robust attribute naming, qualification, attribute comparison, aliasing  
 *)

let get_bound_relation =
    function
	| `Insert (r, _) -> r
	| `Delete (r, _) -> r

(* TODO: handle duplicate attributes, from multiple uses of a relation *)
let rec get_bound_attributes q =
    match q with
	| `Relation (n,f) ->
	      List.map (fun (id,typ) -> `Qualified(n,id)) f

	| `Select(pred, cq) -> get_bound_attributes cq
	| `Project(attrs, cq) -> List.map (fun (a,e) -> a) attrs

	| `Union ch -> get_bound_attributes (List.hd ch)
	| `Cross (l,r) | `NaturalJoin (l,r) | `Join (_,l,r) ->
	      (List.map
		   (fun x -> match x with | `Qualified _ -> x | `Unqualified y -> `Qualified("left", y))
		   (get_bound_attributes l))@
		  (List.map
		       (fun x -> match x with | `Qualified _ -> x | `Unqualified y -> `Qualified("right", y))
		       (get_bound_attributes r))

	| `TrueRelation -> []		  
	| `FalseRelation -> []
	| `DeltaPlan (_, cq) | `NewPlan(cq) | `IncrPlan(_,_, cq) | `IncrDiffPlan(_,_, cq) -> (get_bound_attributes cq)


let compare_attributes a1 a2 =
    match (a1, a2) with
	| (`Qualified (n1, f1), `Qualified (n2, f2)) -> n1 = n2 && f1 = f2
	| (`Qualified (n1, f1), `Unqualified f2) -> f1 = f2
	| (`Unqualified f1, `Qualified (n2, f2)) -> f1 = f2
	| (`Unqualified f1, `Unqualified f2) -> f1 = f2

(* event -> (attribute_id * expression) list *)
let create_bindings event name fields =
  match event with
  | `Insert(_, vars) | `Delete(_, vars) ->
      List.map2
	(fun (vid, vtyp) (fid, ftyp) -> (`Unqualified fid, `ETerm(`Variable(vid))) )
	vars fields 


(* replaces attributes in map_expression with project's bound variables
 * Note: this function uses the following scoping rule:
 * -- nested map expressions are first scoped by their associated query
 *    prior to any attributes in parent queries *)
(* map_expression -> (attribute_id * expression) list -> map_expression *)
let remove_proj_attrs pa new_attrs =
    List.filter
	(fun (aid, bvar) ->
	     List.length (List.filter (compare_attributes aid) new_attrs) = 0)
	pa 

let rec ab_expr_aux expr (pa : (attribute_identifier * expression) list) =
    match expr with
	| `ETerm (`Attribute(x)) ->
	      let matched_attrs = 
		  List.filter
		      (fun (aid, bvar) ->
			   let r = compare_attributes x aid in
			       print_endline ("Comparing "^
						  (string_of_attribute_identifier aid)^" "^
						  (string_of_attribute_identifier x)^": "^
						  (string_of_bool r));
			       r)
		      pa
	      in
		  begin
		      match matched_attrs with
			  | [] -> expr
			  | [(_, `ETerm(`Variable(y)))] -> `ETerm(`Variable(y))
			  | [(_, _)] -> expr
			  | _ ->
				begin
				    List.iter
					(fun (aid, bvar) ->
					     print_endline (string_of_attribute_identifier aid))
					matched_attrs;
				    raise DuplicateException
				end
		  end
	| `ETerm (_) -> expr
	| `UnaryMinus (e) -> ab_expr_aux e pa
	| `Sum (l,r) -> `Sum(ab_expr_aux l pa, ab_expr_aux r pa)
	| `Product (l,r) -> `Product(ab_expr_aux l pa, ab_expr_aux r pa)
	| `Minus (l,r) -> `Minus(ab_expr_aux l pa, ab_expr_aux r pa)
	| `Divide (l,r) -> `Divide(ab_expr_aux l pa, ab_expr_aux r pa)
	| `Function(fid, args) ->
	      `Function(fid, List.map (fun a -> ab_expr_aux a pa) args)

let rec ab_map_expr_aux m_expr pa =
    match m_expr with
	| `METerm (`Attribute(x)) ->
	      let matched_attrs = 
		  List.filter
		      (fun (aid, bvar) ->
			   let r = compare_attributes x aid in
			       print_endline ("Comparing "^
						  (string_of_attribute_identifier aid)^" "^
						  (string_of_attribute_identifier x)^": "^
						  (string_of_bool r));
			       r)
		      pa
	      in
		  begin
		      match matched_attrs with
			  | [] -> m_expr
			  | [(_, `ETerm(`Variable(y)))] -> `METerm(`Variable(y))
			  | [(_, _)] -> m_expr
			  | _ ->
				begin
				    List.iter
					(fun (aid, bvar) ->
					     print_endline (string_of_attribute_identifier aid))
					matched_attrs;
				    raise DuplicateException
				end
		  end

	| `METerm (`Int _) | `METerm (`Float _) | `METerm (`String _)
	| `METerm (`Long _) | `METerm (`Variable _) ->  m_expr

	| `Delta (b,e) ->  `Delta (b, ab_map_expr_aux e pa)
	| `New(e) -> `New (ab_map_expr_aux e pa)
	| `Incr(sid, o, e) -> `Incr (sid, o, ab_map_expr_aux e pa)
	| `IncrDiff(sid, o, e) -> `IncrDiff (sid, o, ab_map_expr_aux e pa)
	| `Init(sid, e) -> `Init (sid, ab_map_expr_aux e pa)
	| `Insert(sid, m, e) -> `Insert (sid, 
	      (match ab_map_expr_aux (`METerm m) pa with
		  `METerm(x) -> x
		  | _ -> raise InvalidExpression
	      ), ab_map_expr_aux e pa)
	| `Update(sid, o, m, e) -> `Update (sid, o, 
	      (match ab_map_expr_aux (`METerm m) pa with
		  `METerm(x) -> x
		  | _ -> raise InvalidExpression
	      ), ab_map_expr_aux e pa)
	| `Delete(sid, m) -> `Delete(sid, 
	      (match ab_map_expr_aux (`METerm m) pa with
		  `METerm(x) -> x
		  | _ -> raise InvalidExpression
	      ))

	| `Sum (l,r) -> `Sum(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Minus (l,r) -> `Minus(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Product (l,r) -> `Product(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Min (l,r) -> `Min(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Max (l,r) -> `Max(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `IfThenElse (b,l,r) -> `IfThenElse(
	      (match ab_pred_aux (`BTerm (b)) pa with
 		  `BTerm(x) -> x
		  | _ -> raise InvalidExpression )
	      , ab_map_expr_aux l pa, ab_map_expr_aux r pa)

	| `MapAggregate (fn,f,q) ->
	      let new_pa = remove_proj_attrs pa (get_bound_attributes q)
	      in
		  `MapAggregate(fn, ab_map_expr_aux f new_pa, ab_plan_aux q pa)

and ab_pred_aux pred pa =
    match pred with
	| `BTerm(`False) | `BTerm(`True) -> pred
	| `BTerm(`LT(l,r)) -> `BTerm(`LT(ab_expr_aux l pa, ab_expr_aux r pa)) 
	| `BTerm(`LE(l,r)) -> `BTerm(`LE(ab_expr_aux l pa, ab_expr_aux r pa))
	| `BTerm(`GT(l,r)) -> `BTerm(`GT(ab_expr_aux l pa, ab_expr_aux r pa))
	| `BTerm(`GE(l,r)) -> `BTerm(`GE(ab_expr_aux l pa, ab_expr_aux r pa))
	| `BTerm(`EQ(l,r)) -> `BTerm(`EQ(ab_expr_aux l pa, ab_expr_aux r pa))
	| `BTerm(`NE(l,r)) -> `BTerm(`NE(ab_expr_aux l pa, ab_expr_aux r pa))
	| `BTerm(`MEQ(m_expr)) -> `BTerm(`MEQ(ab_map_expr_aux m_expr pa))
	| `BTerm(`MLT(m_expr)) -> `BTerm(`MLT(ab_map_expr_aux m_expr pa))
	| `And (l,r) -> `And(ab_pred_aux l pa, ab_pred_aux r pa)
	| `Or (l,r) -> `Or(ab_pred_aux l pa, ab_pred_aux r pa)
	| `Not (e) -> `Not(ab_pred_aux e pa)

and ab_plan_aux q pa =
    match q with
	| `Relation (_,_) -> q
	| `Select(pred, cq) ->
	      let new_pa = remove_proj_attrs pa (get_bound_attributes cq) in
		  `Select(ab_pred_aux pred new_pa, ab_plan_aux cq pa)

	| `Project (attrs, cq) -> ab_plan_aux cq pa
	| `Union ch -> `Union (List.map (fun c -> ab_plan_aux c pa) ch)
	| `Cross (l,r) -> `Cross (ab_plan_aux l pa, ab_plan_aux r pa)
	| `NaturalJoin (l,r) ->
	      `NaturalJoin (ab_plan_aux l pa, ab_plan_aux r pa)
	| `Join (p, l, r) ->
	      let new_pa =
		  remove_proj_attrs
		      (remove_proj_attrs pa (get_bound_attributes l))
		      (get_bound_attributes r)
	      in
		  `Join(ab_pred_aux p new_pa, ab_plan_aux l pa, ab_plan_aux r pa) 

	| `TrueRelation -> `TrueRelation
	| `FalseRelation -> `FalseRelation
	| `DeltaPlan (b,e) -> `DeltaPlan (b, ab_plan_aux e pa)
	| `NewPlan(e) -> `NewPlan (ab_plan_aux e pa)
	| `IncrPlan(sid, p, e) -> `IncrPlan (sid, p, ab_plan_aux e pa)
	| `IncrDiffPlan(sid, p, e) -> `IncrDiffPlan (sid, p, ab_plan_aux e pa)

let add_map_expression_bindings m_expr proj_attrs =
    ab_map_expr_aux m_expr proj_attrs 

let add_predicate_bindings pred proj_attrs =
    ab_pred_aux pred proj_attrs


(* var_id -> bindings -> expression *)
let get_expr_definition var bindings =
    match var with
	| `Variable(sym) ->
	      let matches =
		  List.filter
		      (fun x -> match x with
			   | `BindExpr (n,d) -> n = sym
			   | `BindBoolExpr (_,_) -> false
			   | `BindMapExpr (_,_) -> false)
		      bindings
	      in
		  begin
		      match matches with
			  | [] -> raise Not_found
			  | [`BindExpr(n,d)] -> d
			  | (`BindExpr(n,d))::t -> d (* raise exception for duplicate bindings? *)
			  | _ -> raise Not_found 
		  end
	| _ -> raise Not_found

(* Note removes duplicate bindings *)
let remove_expr_binding var bindings =
    match var with
	| `Variable(sym) ->
	      List.filter
		  (fun x -> match x with
		       | `BindExpr (n,d) -> n <> sym
		       | `BindBoolExpr (_,_) -> true
		       | `BindMapExpr (_,_) -> true)
		  bindings
	| _ -> raise Not_found


let resolve_unbound_attributes attrs bindings =
    List.filter
	(fun x ->
	     let fq_match = List.filter (fun y -> x = y) bindings in 
		 match x with
		     | `Qualified (r,n) -> (List.length fq_match) = 0  
		     | `Unqualified n ->
			   let uq_match = List.filter (compare_attributes x) bindings in
			   let left_match =
			       List.filter (fun y -> (`Qualified("left", n)) = y) bindings in
			   let right_match =
			       List.filter (fun y -> (`Qualified("right", n)) = y) bindings in
			       match ((List.length left_match), (List.length right_match)) with
				   | (0, 0) -> (List.length fq_match) = 0 && (List.length uq_match) = 0
				   | (x, y) when x > 0 && y > 0 -> raise InvalidExpression
				   | (_, _) -> false)
	attrs

let rec get_unbound_attributes_from_expression expr include_vars =
    match expr with
	| `ETerm (`Attribute x) -> [x]
	| `ETerm (`Int _) | `ETerm (`Float _)
	| `ETerm (`String _) | `ETerm (`Long _) -> []
	| `ETerm (`Variable x) -> if include_vars then [`Unqualified x]  else []

	| `UnaryMinus e ->
	      get_unbound_attributes_from_expression e include_vars
		  
	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      (get_unbound_attributes_from_expression l include_vars)@
		  (get_unbound_attributes_from_expression r include_vars)

	| `Function(fid, args) ->
	      List.flatten
		  (List.map
		       (fun a ->
			    get_unbound_attributes_from_expression a include_vars)
		       args)

let rec get_unbound_attributes_from_predicate b_expr include_vars =
    match b_expr with
	| `BTerm(`True) | `BTerm(`False) -> []

	| `BTerm(`LT(l,r)) | `BTerm(`LE(l,r)) | `BTerm(`GT(l,r))
	| `BTerm(`GE(l,r)) | `BTerm(`EQ(l,r)) | `BTerm(`NE(l,r)) ->
	      (get_unbound_attributes_from_expression l include_vars)@
		  (get_unbound_attributes_from_expression r include_vars)

	| `BTerm(`MEQ(m_expr)) | `BTerm(`MLT(m_expr)) ->
	      (get_unbound_attributes_from_map_expression m_expr include_vars)

	| `And(l,r) | `Or(l,r) ->
	      (get_unbound_attributes_from_predicate l include_vars)@
		  (get_unbound_attributes_from_predicate r include_vars)

	| `Not(e) -> (get_unbound_attributes_from_predicate e include_vars)

and get_unbound_attributes_from_map_expression m_expr include_vars =
    match m_expr with
	| `METerm (`Attribute x) -> [x]
	| `METerm (`Int _) | `METerm (`Float _) | `METerm (`String _)
	| `METerm (`Long _) -> []
	| `METerm (`Variable x) -> if include_vars then [`Unqualified x] else []

	| `Delta (b,e) -> get_unbound_attributes_from_map_expression e include_vars
	      
	| `Sum (l,r) | `Minus (l,r) | `Product (l,r) | `Min(l,r) | `Max(l,r) ->
	      (get_unbound_attributes_from_map_expression l include_vars)@
		  (get_unbound_attributes_from_map_expression r include_vars)

	| `MapAggregate (fn,f,q) ->
	      let f_uba = get_unbound_attributes_from_map_expression f include_vars in
	      let q_ba = get_bound_attributes q in
		  (resolve_unbound_attributes f_uba q_ba)@
		      (get_unbound_attributes_from_plan q include_vars)

	| `New(e) | `Incr(_,_,e) | `IncrDiff(_,_,e) | `Init(_,e) ->
	      get_unbound_attributes_from_map_expression e include_vars

	| `Insert(_,m,e) | `Update(_,_,m,e) ->
	      (get_unbound_attributes_from_map_expression (`METerm (m)) include_vars)@
	          (get_unbound_attributes_from_map_expression e include_vars)

	| `Delete(_,m) ->
	      get_unbound_attributes_from_map_expression (`METerm (m)) include_vars

	| `IfThenElse(b,l,r) ->
	      (get_unbound_attributes_from_predicate (`BTerm (b)) include_vars)@ 
	          (get_unbound_attributes_from_map_expression l include_vars)@
		  (get_unbound_attributes_from_map_expression r include_vars)

and get_unbound_attributes_from_plan q include_vars =
    match q with
	| `Relation _ | `TrueRelation | `FalseRelation -> []

	| `Select(pred, cq) ->
	      let pred_uba = get_unbound_attributes_from_predicate pred include_vars in
	      let cq_ba = get_bound_attributes cq in
		  (resolve_unbound_attributes pred_uba cq_ba)@
		      (get_unbound_attributes_from_plan cq include_vars)

	| `Project(attrs, cq) ->
	      let attrs_uba = 
		  List.flatten
		      (List.map
			   (fun (a,e) -> get_unbound_attributes_from_expression e include_vars)
			   attrs)
	      in
	      let cq_ba = get_bound_attributes cq in
		  (resolve_unbound_attributes attrs_uba cq_ba)@
		      (get_unbound_attributes_from_plan cq include_vars)

	| `Union ch ->
	      List.flatten
		  (List.map
		       (fun c -> get_unbound_attributes_from_plan c include_vars) ch)

	| `Cross (l, r) ->
	      (get_unbound_attributes_from_plan l include_vars)@
		  (get_unbound_attributes_from_plan r include_vars)

	| `NaturalJoin (l, r) ->
	      (get_unbound_attributes_from_plan l include_vars)@
		  (get_unbound_attributes_from_plan r include_vars)

	| `Join (p, l, r) ->
	      let pred_uba = get_unbound_attributes_from_predicate p include_vars in
	      let c_ba = (get_bound_attributes l)@(get_bound_attributes r) in
		  (resolve_unbound_attributes pred_uba c_ba)@
		      (get_unbound_attributes_from_plan l include_vars)@
		      (get_unbound_attributes_from_plan r include_vars)

	| `DeltaPlan(_,e) | `NewPlan(e) | `IncrPlan(_,_,e) | `IncrDiffPlan(_,_,e) ->
	      get_unbound_attributes_from_plan e include_vars

let get_unbound_map_attributes_from_plan f q include_vars =
    let f_uba = get_unbound_attributes_from_map_expression f include_vars in
    let q_ba = get_bound_attributes q in
	(resolve_unbound_attributes f_uba q_ba)@
	    (get_unbound_attributes_from_plan q include_vars)


(* expression -> bool 
 * returns whether the expression is constant,
 * i.e. uses only constants and variables, not attributes *)
let rec is_constant_expr expr =
    match expr with
	| `ETerm (`Attribute _) -> false
	| `ETerm (`Int _) | `ETerm (`Float _)
	| `ETerm (`String _) | `ETerm (`Long _) | `ETerm (`Variable _) -> true
	| `UnaryMinus e -> is_constant_expr e
	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      is_constant_expr(l) && is_constant_expr(r)

	(* TODO: && is_deterministic(fid) *)
	| `Function(fid, args) -> List.for_all is_constant_expr args

let rec is_static_constant_expr expr =
    match expr with
	| `ETerm (`Attribute _) | `ETerm (`Variable _) -> false
	| `ETerm (`Int _) | `ETerm (`Float _)
	| `ETerm (`String _) | `ETerm (`Long _) -> true
	| `UnaryMinus e -> is_static_constant_expr e
	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      is_static_constant_expr(l) && is_static_constant_expr(r)

	(* TODO: && is_deterministic(fid) *)
	| `Function(fid, args) -> List.for_all is_static_constant_expr args

(* expression -> bindings list -> bool *)
let is_bound_expr expr bindings =
    try
	match expr with
	    | `ETerm(`Variable v) ->
		  let _ = get_expr_definition (`Variable v) bindings in true
	    | _ -> false
    with Not_found -> false

(* map_expression -> plan -> bool
 * returns whether a map aggregate expression is independent from its query *)
let is_independent m_expr q =
    let q_ba = get_bound_attributes q in
    let me_a = get_unbound_attributes_from_map_expression m_expr false in
    let me_al = List.length me_a in
    let unresolved_l = List.length (resolve_unbound_attributes me_a q_ba) in
	me_al > 0 && (me_al = unresolved_l)   


(*
 * 
 * Delta simplification
 * 
 *)
let rec push_delta_plan mep =
    match mep with
	| `DeltaPlan(ev, `Relation(name, fields)) ->
	      if name = (get_bound_relation ev) then
		`Project(create_bindings ev name fields, `TrueRelation)
	      else
		`FalseRelation

	| `DeltaPlan(ev, `Select(pred, c)) ->
	      `Select(pred, push_delta_plan(`DeltaPlan(ev, c)))

	(* Note: simplify surrounding map expression *)
	|  `DeltaPlan(ev, `Project(projs, c)) -> `Project(projs, c)

	| `DeltaPlan(ev, `Union(c)) ->
	      `Union
		  (List.map
		       (fun ch -> push_delta_plan(`DeltaPlan(ev,ch))) c)

	| `DeltaPlan(ev, `Cross(l, r)) ->
	      let delta_l = push_delta_plan(`DeltaPlan(ev,l)) in
	      let delta_r = push_delta_plan(`DeltaPlan(ev,r)) in
		  `Union(
		      [`Cross(delta_l, r); `Cross(l, delta_r); `Cross(delta_l, delta_r)])

	(* TODO:
	   | `DeltaPlan(ev, `NaturalJoin(l,r)) -> ()
	   | `DeltaPlan(ev, `Join(p,l,r)) -> ()
	*)

	| _ ->
	      print_endline (string_of_plan mep);
	      raise (RewriteException "Invalid Delta")

let rec push_delta m_expr =
    match m_expr with
	| `Delta (ev, `METerm _) -> `METerm (`Int 0)

	| `Delta (ev, `Sum (l, r)) ->
	      `Sum(push_delta(`Delta(ev,l)), push_delta(`Delta(ev,r)))
		  
	| `Delta (ev, `Minus (l, r)) ->
	      `Minus(push_delta(`Delta(ev,l)), push_delta(`Delta(ev,r)))
		  
	| `Delta (ev, `Product (l, r)) ->
	      `Sum(`Product(l, push_delta(`Delta(ev,r))),
		   `Sum(`Product(push_delta(`Delta(ev,l)), r),
			`Product(push_delta(`Delta(ev,l)),
				 push_delta(`Delta(ev,r)))))

	| `Delta (ev, `MapAggregate (`Sum, f, q)) ->
	      let delta_f = push_delta(`Delta(ev,f)) in
	      let delta_plan = push_delta_plan(`DeltaPlan(ev,q)) in
	   	  `Sum(`MapAggregate(`Sum, delta_f, q),
	          	`Sum(`MapAggregate(`Sum, f, delta_plan),
			     `MapAggregate(`Sum, delta_f, delta_plan)))
	| `Delta (ev, `MapAggregate (`Min, f, q)) ->
	      let br = get_base_relations m_expr in
	      let rel = get_bound_relation ev in
	      let l = List.filter (
			fun x -> match x with `Relation (n,_) -> n = rel | _ -> raise InvalidExpression ) br in
	      if List.length l = 0 then `METerm (`Int 0)
	      else 
	          let delta_plan = push_delta_plan(`DeltaPlan(ev,q)) in
		  begin 
		    match ev with 
			| `Insert _ -> `MapAggregate(`Min, f, `IncrPlan (gen_state_sym(), `Union, delta_plan))
		   	| `Delete _ -> `MapAggregate(`Min, f, `IncrDiffPlan (gen_state_sym(), `Diff, delta_plan)) (* check later *)
		  end

	| _ -> m_expr


(* rcs: recompute mode *)
let rec apply_delta_rules m_expr event rcs =
    let adr_new x = 
	match x with 
	    | `New (`METerm y) -> `METerm y
		  
	    | `New (`Sum (l, r)) ->
		  `Sum(apply_delta_rules l event (Some New),
		       apply_delta_rules r event (Some New))
		      
	    | `New (`Minus (l, r)) ->
		  `Minus(apply_delta_rules l event (Some New),
		       apply_delta_rules r event (Some New))
		      
	    | `New (`Product (l, r)) ->
		  `Product(apply_delta_rules l event (Some New),
			   apply_delta_rules r event (Some New))
		      
	    | `New (`Min (l,r)) ->
		  `Min(apply_delta_rules l event (Some New),
		       apply_delta_rules r event (Some New))

	    | `New (`Max (l,r)) ->
		  `Max(apply_delta_rules l event (Some New),
		       apply_delta_rules r event (Some New))

	    | `New (`MapAggregate (fn, f, q)) ->
		  `MapAggregate(fn,
				apply_delta_rules f event (Some New),
				apply_delta_plan_rules q event (Some New))
		      
	    | _ -> raise (RewriteException "Invalid recomputation expression.")
    in
	match m_expr with
	    | `Incr(sid, op, m_expr) -> `Incr(sid, op, push_delta (`Delta (event, m_expr)))
	    | `Init(sid, m_expr) -> `Init(sid, adr_new (`New (m_expr)))
	    | `New(_) as e -> adr_new e
	    | _ -> 
		  begin
		      match rcs with
			  | Some New -> adr_new (`New (m_expr))
			  | Some Incr -> push_delta (`Delta (event, m_expr))
			  | None -> raise (RewriteException "Invalid recomputation state.")
		  end


and apply_delta_plan_rules q event rcs =
    let adpr_new x = 
	match x with
	    | `NewPlan(`Relation r) -> `Relation r
		  
	    | `NewPlan(`Select (p, cq)) ->
		  begin
		      match p with
			  | `BTerm(`MEQ(m_expr)) ->
				`Select(`BTerm(`MEQ(apply_delta_rules m_expr event (Some New))),
					apply_delta_plan_rules cq event (Some New))
				    
			  | `BTerm(`MLT(m_expr)) ->
				`Select(`BTerm(`MLT(apply_delta_rules m_expr event (Some New))),
					apply_delta_plan_rules cq event (Some New))
				    
			  (* TODO: handle conjunctive/disjunctive map expression comparisons *)
			  | _ -> `Select(p, apply_delta_plan_rules cq event (Some New))
		  end
		      
	    | `NewPlan(`Project (attrs, cq)) ->
		  `Project(attrs, apply_delta_plan_rules cq event (Some New))
		      
	    | `NewPlan(`Union children) ->
		  `Union (List.map (fun c -> apply_delta_plan_rules c event (Some New)) children)
		      
	    | `NewPlan(`Cross (l,r)) ->
		  `Cross(apply_delta_plan_rules l event (Some New),
			 apply_delta_plan_rules r event (Some New))
		      
	    | `NewPlan(`NaturalJoin (l,r)) ->
		  `NaturalJoin (apply_delta_plan_rules l event (Some New),
				apply_delta_plan_rules r event (Some New))
		      
	    | `NewPlan(`Join (p,l,r)) ->
		  begin
		      match p with
			  | `BTerm(`MEQ(m_expr)) ->
				`Join(`BTerm(`MEQ(apply_delta_rules m_expr event (Some New))),
				      apply_delta_plan_rules l event (Some New),
				      apply_delta_plan_rules r event (Some New))
				    
			  | `BTerm(`MLT(m_expr)) ->
				`Join(`BTerm(`MLT(apply_delta_rules m_expr event (Some New))),
				      apply_delta_plan_rules l event (Some New),
				      apply_delta_plan_rules r event (Some New))
				    
			  | _ ->
				`Join(p, apply_delta_plan_rules l event (Some New),
				      apply_delta_plan_rules r event (Some New))
		  end
		      
	    | _ -> raise (RewriteException "Invalid recomputation plan.")
    in
	match q with
	    | `IncrPlan(sid, op, qq) -> `IncrPlan(sid, op, push_delta_plan (`DeltaPlan (event, qq)))
	    | `NewPlan(_) as p -> adpr_new p
	    | _ ->
		  begin
		      match rcs with
			  | Some New -> adpr_new (`NewPlan (q))
			  | Some Incr -> push_delta_plan q
			  | _ -> raise (RewriteException "Invalid recomputation state.")
		  end


(*
 *
 * Constant simplification
 *
 *) 

let is_zero = function
    | `METerm (`Int 0) -> true
    | `METerm (`Long x) when x = Int64.zero -> true
    | `METerm (`Float 0.0) -> true
    | _ -> false

let is_one = function
    | `METerm (`Int 1) -> true
    | `METerm (`Long x) when x = Int64.one -> true
    | `METerm (`Float 1.0) -> true
    | _ -> false

let bool_constant b = if b then `True else `False

let simplify_comparison l r f vareq varneq =
    match (l,r) with
	| (`ETerm(`Int x), `ETerm(`Int y)) -> Some(bool_constant(f x y))
	| (`ETerm(`Float x), `ETerm(`Float y)) ->
	      Some(bool_constant(f (compare x y) 0))
		  
	| (`ETerm(`Long x), `ETerm(`Long y)) ->
	      Some(bool_constant(f (Int64.compare x y) 0))

	| (`ETerm(`String x), `ETerm(`String y)) ->
	      Some(bool_constant(f (compare x y) 0))

	| (`ETerm(`Variable x), `ETerm(`Variable y)) ->
	      if vareq then Some(bool_constant(x = y))
	      else if varneq then Some(bool_constant (x <> y))
	      else None

	| _ -> None

let get_comparison_function b_term =
    match b_term with
	| `BTerm(`LT(_,_)) -> (<)
	| `BTerm(`LE(_,_)) -> (<=)
	| `BTerm(`GT(_,_)) -> (>)
	| `BTerm(`GE(_,_)) -> (>=)
	| `BTerm(`EQ(_,_)) -> (=)
	| `BTerm(`NE(_,_)) -> (<>)
	| `BTerm(`MEQ(_)) -> (=)
	| `BTerm(`MLT(_)) -> (<)
	| _ -> raise InvalidExpression

let rec simplify_expr_constants e =
    match e with
	| `ETerm _ as x -> x
	| `UnaryMinus(e) ->
	      let se = simplify_expr_constants e in
		  begin
		      match se with
			  | `ETerm (`Int x) -> `ETerm (`Int (-x))
			  | `ETerm (`Float x) -> `ETerm (`Float (-.x))
			  | `ETerm (`Long x) -> `ETerm (`Long (Int64.neg x))
			  | _ -> `UnaryMinus(se)
		  end

	| `Sum(l,r) | `Product(l,r) | `Minus(l,r) | `Divide(l,r) ->
	      begin
		  let le = simplify_expr_constants l in
		  let re = simplify_expr_constants r in
		      match (le,re) with
			  | (`ETerm(`Int y), `ETerm(`Int z)) ->
				begin
				    match e with
					| `Sum (_,_) -> `ETerm(`Int (y+z))
					| `Product (_,_) -> `ETerm(`Int (y*z))
					| `Minus (_,_) -> `ETerm(`Int (y-z))
					| `Divide (_,_) -> `ETerm(`Int (y/z))
					| _ -> raise InvalidExpression
				end
				    
			  | (`ETerm(`Float y), `ETerm(`Float z)) ->
				begin
				    match e with
					| `Sum (_,_) -> `ETerm(`Float (y+.z))
					| `Product (_,_) -> `ETerm(`Float (y*.z))
					| `Minus (_,_) -> `ETerm(`Float (y-.z))
					| `Divide (_,_) -> `ETerm(`Float (y/.z))
					| _ -> raise InvalidExpression
				end
				    
			  | (`ETerm(`Long y), `ETerm(`Long z)) ->
				begin
				    match e with
					| `Sum (_,_) -> `ETerm(`Long (Int64.add y z))
					| `Product (_,_) -> `ETerm(`Long (Int64.mul y z))
					| `Minus (_,_) -> `ETerm(`Long (Int64.sub y z))
					| `Divide (_,_) -> `ETerm(`Long (Int64.div y z))
					| _ -> raise InvalidExpression
				end

			  | _ ->
				begin
				    match e with
					| `Sum (_,_) -> `Sum(le, re)
					| `Product (_,_) -> `Product(le, re)
					| `Minus (_,_) -> `Minus(le, re)
					| `Divide (_,_) -> `Divide(le, re)
					| _ -> raise InvalidExpression
				end
	      end

	| `Function(fid, args) ->
	      `Function(fid, List.map simplify_expr_constants args)

let rec simplify_predicate_constants b_expr =
    match b_expr with
	| `BTerm(`True) -> `BTerm(`True) 
	| `BTerm(`False) -> `BTerm(`False)
	      
	| `BTerm(`LT(l,r))
	| `BTerm(`LE(l,r))
	| `BTerm(`GT(l,r))
	| `BTerm(`GE(l,r))
	| `BTerm(`EQ(l,r))
	| `BTerm(`NE(l,r)) ->
	      let le = simplify_expr_constants l in
	      let re = simplify_expr_constants r in
		  if is_constant_expr(le) && is_constant_expr(re) then
		      let cmp_fun = get_comparison_function b_expr in
		      let vareq = match b_expr with | `BTerm(`EQ(_,_)) -> true | _ -> false in
		      let varneq = match b_expr with | `BTerm(`NE(_,_)) -> true | _ -> false in 
			  match (simplify_comparison le re cmp_fun vareq varneq) with
			      | Some(x) -> `BTerm(x)
			      | None ->
				    begin
					match b_expr with
					    | `BTerm(`LT(_,_)) -> `BTerm(`LT(le,re))
					    | `BTerm(`LE(_,_)) -> `BTerm(`LE(le,re))
					    | `BTerm(`GT(_,_)) -> `BTerm(`GT(le,re))
					    | `BTerm(`GE(_,_)) -> `BTerm(`GE(le,re))
					    | `BTerm(`EQ(_,_)) -> `BTerm(`EQ(le,re))
					    | `BTerm(`NE(_,_)) -> `BTerm(`NE(le,re))
					    | _ -> raise InvalidExpression
				    end
		  else
		      begin
			  match b_expr with
			      | `BTerm(`LT(_,_)) -> `BTerm(`LT(le,re))
			      | `BTerm(`LE(_,_)) -> `BTerm(`LE(le,re))
			      | `BTerm(`GT(_,_)) -> `BTerm(`GT(le,re))
			      | `BTerm(`GE(_,_)) -> `BTerm(`GE(le,re))
			      | `BTerm(`EQ(_,_)) -> `BTerm(`EQ(le,re))
			      | `BTerm(`NE(_,_)) -> `BTerm(`NE(le,re))
			      | _ -> raise InvalidExpression
		      end

	| `BTerm(`MEQ(m_expr)) | `BTerm(`MLT(m_expr)) ->
	      let sm = simplify_map_expr_constants m_expr in
		  begin
		      match sm with
			      (* TODO: does it make sense to compare strings to 0? *)
			  | `METerm(`Int y) ->
				`BTerm(bool_constant ((get_comparison_function b_expr) y 0))
				    
			  | `METerm(`Float y) ->
				`BTerm(bool_constant ((get_comparison_function b_expr) y 0.0))

			  | `METerm(`Long y) ->
				`BTerm(bool_constant
					   ((get_comparison_function b_expr) (Int64.compare y Int64.zero) 0))
				    
			  | _ ->
				begin
				    match b_expr with
					| `BTerm(`MEQ(_)) -> `BTerm(`MEQ(sm))
					| `BTerm(`MLT(_)) -> `BTerm(`MLT(sm))
					| _ -> raise InvalidExpression
				end
		  end

	| `And (l,r) ->
	      let le = simplify_predicate_constants l in
	      let re = simplify_predicate_constants r in
		  begin
		      match (le, re) with
			  | (`BTerm(`False), _) | (_, `BTerm(`False)) -> `BTerm(`False)
			  | (`BTerm(`True), `BTerm(`True)) -> `BTerm(`True)
			  | (_, _) -> `And(le, re)
		  end

	| `Or (l,r) ->
	      let le = simplify_predicate_constants l in
	      let re = simplify_predicate_constants r in
		  begin
		      match (le, re) with
			  | (`BTerm(`True), _) | (_, `BTerm(`True)) -> `BTerm(`True)
			  | (`BTerm(`False), `BTerm(`False)) -> `BTerm(`False)
			  | (_, _) -> `Or(le, re)
		  end

	| `Not(e) ->
	      let se = simplify_predicate_constants e in
		  begin
		      match se with
			  | `BTerm(`True) -> `BTerm(`False)
			  | `BTerm(`False) -> `BTerm(`True)
			  | _ -> `Not(se)
		  end

and simplify_map_expr_constants m_expr =
    match m_expr with
	| `METerm _ -> m_expr

	| `Sum (l, r) ->
	      begin
		  let sl = simplify_map_expr_constants l in
		  let sr = simplify_map_expr_constants r in
		      match (is_zero(sl), is_zero(sr)) with
			  | (true, true) -> `METerm(`Int 0)
			  | (true, false) -> sr
			  | (false, true) -> sl
			  | (false, false) -> `Sum(sl, sr)
	      end
		  
	| `Minus (l, r) ->
	      begin
		  let sl = simplify_map_expr_constants l in
		  let sr = simplify_map_expr_constants r in
		      match (is_zero(sl), is_zero(sr)) with
			  | (true, true) -> `METerm(`Int 0)
			  | (true, false) -> `Minus(`METerm(`Int 0), sr)
			  | (false, true) -> sl
			  | (false, false) -> `Minus(sl, sr)
	      end
		  
	| `Product (l, r) ->
	      begin
		  let sl = simplify_map_expr_constants l in
		  let sr = simplify_map_expr_constants r in
		      match (is_zero(sl), is_zero(sr), is_one(sl), is_one(sr)) with
			  | (true, _, _, _) -> `METerm(`Int 0)
			  | (_, true, _, _) -> `METerm(`Int 0)
			  | (false, false, true, true) -> `METerm(`Int 1)
			  | (false, false, true, false) -> sr
			  | (false, false, false, true) -> sl
			  | (false, false, false, false) -> `Product(sl, sr)
	      end

	| `Min (l, r) ->
	      let sl = simplify_map_expr_constants l in
	      let sr = simplify_map_expr_constants r in
		  `Min(sl, sr)
		      
	| `Max (l, r) ->
	      let sl = simplify_map_expr_constants l in
	      let sr = simplify_map_expr_constants r in
		  `Max(sl, sr)
		      
	(* TODO: static evaluation *)
	(*
	  begin
	  match (is_static_constant_expr(sl), is_static_constant_expr(sr)) with
	  | (true, true) -> eval(`IfThenElse(`BCTerm(`LT(sl, sr)), sl, sr))
	  | _ -> `Min(sl, sr)
	  end
	*)

	| `MapAggregate(aggfn,f,q) ->
	      begin
		  let sf = simplify_map_expr_constants f in
		  let sq = simplify_plan_constants q in
		      match (is_zero(sf), sq) with
		      |  (_, `TrueRelation) -> f
		      | (true, _)  | (_, `FalseRelation) -> `METerm(`Int 0)
		      | _ -> `MapAggregate(aggfn, sf, sq)
	      end

	| `Delta (b,e) -> `Delta(b, simplify_map_expr_constants e)
	| `New(e) -> `New(simplify_map_expr_constants e)
	| `Incr(sid, o, e) -> `Incr(sid, o, simplify_map_expr_constants e)
	| `IncrDiff(sid, o, e) -> `IncrDiff(sid, o, simplify_map_expr_constants e)
	| `Init (sid, e) -> `Init(sid, simplify_map_expr_constants e)
	| `Insert(sid, m, e) -> `Insert(sid, m, simplify_map_expr_constants e)
	| `Update(sid, o, m, e) -> `Update(sid, o, m, simplify_map_expr_constants e)
	| `Delete(sid, m) -> m_expr
	| `IfThenElse (b, l, r) ->
	      let sb = match simplify_predicate_constants (`BTerm b) with
		          | `BTerm(x) -> x
			  | _ -> raise InvalidExpression
	      in
	      let sl = simplify_map_expr_constants l in
	      let sr = simplify_map_expr_constants r in
		  `IfThenElse(sb, sl, sr)
	      

and simplify_plan_constants q =
    match q with
	| `Relation (n,f) as r -> r
		      
	| `Select(pred, cq) ->
	      let sp = simplify_predicate_constants pred in
	      let cqq = simplify_plan_constants cq in
		  begin
		      match (sp, cqq) with
		      | (_, `FalseRelation) | (`BTerm(`False), _) -> `FalseRelation
		      | (`BTerm(`True), _) -> cqq
		      | (_,_) -> `Select(sp, cqq)
		  end

	| `Project(attrs, cq) ->
	      let cqq = simplify_plan_constants cq in
	      if cqq = `FalseRelation then cqq else `Project(attrs, cqq)

	| `Union ch ->
	      (* Unions are typechecked outside this simplification. *)
	      let chq = List.map simplify_plan_constants ch in
	      if (List.exists (fun c -> c = `TrueRelation) chq) then `TrueRelation
	      else if (List.for_all (fun c -> c = `FalseRelation) chq) then `FalseRelation
	      else
		begin
		  let non_empty_chq = List.filter (fun x -> x <> `FalseRelation) chq in
		  match non_empty_chq with
		  | [x] -> x
		  | _ -> `Union non_empty_chq
		end
		      
	| `Cross (l,r) ->
	      let lq = simplify_plan_constants l in
	      let rq = simplify_plan_constants r in
	      begin
		match (lq, rq) with
		| (`TrueRelation, x) | (x, `TrueRelation) -> x
		| (`FalseRelation, _) | (_, `FalseRelation) -> `FalseRelation
		| _ -> `Cross (lq, rq)
	      end
		      
	| `NaturalJoin (l,r) ->
	      let lq = simplify_plan_constants l in
	      let rq = simplify_plan_constants r in
	      begin
		match (lq, rq) with
		| (`TrueRelation, x) | (x, `TrueRelation) -> x
		| (`FalseRelation, _) | (_, `FalseRelation) -> `FalseRelation
		| _ ->  `NaturalJoin (lq, rq)
	      end

	| `Join (p, l, r) ->
	      let sp = simplify_predicate_constants p in
	      let lq = simplify_plan_constants l in
	      let rq = simplify_plan_constants r in
	      begin
		match (sp, lq, rq) with
		| (_, `TrueRelation, x) | (_, x, `TrueRelation) -> `Select(sp, x)
		| (_, `FalseRelation, _) | (_, _, `FalseRelation)
		| (`BTerm(`False), _, _) -> `FalseRelation
		| (`BTerm(`True), _, _) -> `Cross(lq, rq)
		| _ -> `Join(sp, lq, rq)
	      end

	| `TrueRelation -> `TrueRelation

	| `FalseRelation -> `FalseRelation
	      
	| `DeltaPlan(b, cq) ->
	      let cqq = simplify_plan_constants cq in
	      if cq = `FalseRelation then `FalseRelation else `DeltaPlan(b, cqq)

	| `NewPlan (cq) ->
	      let cqq = simplify_plan_constants cq in
	      if cq = `FalseRelation then `FalseRelation else `NewPlan(cqq)

	| `IncrPlan (sid, op, cq) ->
	      let cqq = simplify_plan_constants cq in
	      if cq = `FalseRelation then `FalseRelation else `IncrPlan(sid,op,cqq)

	| `IncrDiffPlan (sid, op, cq) ->
	      let cqq = simplify_plan_constants cq in
	      if cq = `FalseRelation then `FalseRelation else `IncrDiffPlan(sid,op,cqq)



(*
 *
 * Map expression rewriting
 *
 *)

(* bottom up map expression rewriting *)
(* map expression -> accessor_element list -> map_expression *)
let rec simplify m_expr rewrites =
    print_endline ("Expr "^(indented_string_of_map_expression m_expr 0));
    print_endline
	("Pending rewrites("^(string_of_int (List.length rewrites))^"):\n    "^
	     (List.fold_left
		  (fun acc ae ->
		       (if (String.length acc) = 0 then "" else acc^"\n    ")^
			   (string_of_accessor_element ae)) 
		  "" rewrites));
    print_endline (String.make 50 '-');
    match rewrites with
	| [] -> m_expr
	| (`MapExpression n)::t when n = m_expr -> m_expr
	| n::t ->
	      begin
		  print_endline ("t len: "^(string_of_int (List.length t)));
		  print_endline ("Parent arg "^(string_of_accessor_element n));

		  let x = parent m_expr n in
		      match x with
			  | None -> simplify m_expr t
			  | Some np ->
				begin
				    print_endline ("Found parent "^(string_of_accessor_element np));
				    print_endline (String.make 50 '-');

				    match np with
				    | `Plan(`Select (pred, `Project(projs, q))) ->
					let new_np =
					  `Plan(`Project(projs,
							 `Select(add_predicate_bindings pred projs, q)))
					in
					let new_m_expr = splice m_expr np new_np in
					let new_rewrites = new_np::t in
					print_endline ("NR len: "^(string_of_int (List.length new_rewrites)));
					simplify new_m_expr new_rewrites

					| `Plan(`Select (pred, `Union ch)) ->
					      let new_np =
						  `Plan(`Union (List.map (fun c -> `Select(pred, c)) ch))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = (List.map (fun c -> `Plan c) ch)@t in
						  simplify new_m_expr new_rewrites

					(* Assume attribute names in left and right branches are unique *)
					| `Plan(`Cross(x,`Project(a,q)))
					| `Plan(`Cross(`Project(a,q),x)) ->
					      let new_np = `Plan(`Project(a, `Cross(x,q))) in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = new_np::t in
						  simplify new_m_expr new_rewrites

					| `MapExpression(
					      `MapAggregate(fn, f, `Project(projs, `TrueRelation))) ->
					      let new_np = `MapExpression(add_map_expression_bindings f projs) in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = new_np::t in
						  simplify new_m_expr new_rewrites

					| `MapExpression(`MapAggregate (fn, f, `Project(projs, q))) ->
					    let new_np = `MapExpression(
					      `MapAggregate (fn,
							     add_map_expression_bindings f projs, q))
					    in
					    let new_m_expr = splice m_expr np new_np in
					    let new_rewrites = (`Plan(q))::t in
					        simplify new_m_expr new_rewrites
					      
					| `MapExpression(`MapAggregate (`Sum, f, `Union(c))) ->
					      let new_np = `MapExpression(
						  List.fold_left
						      (fun acc ch -> `Sum(`MapAggregate(`Sum, f, ch), acc))
						      (`MapAggregate (`Sum, f, (List.hd c))) (List.tl c))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@(List.map (fun x -> `Plan x) c) in
						  simplify new_m_expr new_rewrites 

					| `MapExpression(`MapAggregate (`Min, f, `Union(c))) ->
					      let new_np = `MapExpression(
						  List.fold_left
						      (fun acc ch -> `Min(`MapAggregate(`Min, f, ch), acc))
						      (`MapAggregate (`Min, f, (List.hd c))) (List.tl c))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@(List.map (fun x -> `Plan x) c) in
						  simplify new_m_expr new_rewrites

					(* Note: bindings are added to 'f' while processing any parent projection *)
					| `MapExpression(`MapAggregate (fn, f, `TrueRelation)) ->
					      let new_np = `MapExpression f in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@[(`MapExpression f)] in
						  simplify new_m_expr new_rewrites

					| `MapExpression(`MapAggregate (`Sum, `Sum(l,r), q)) ->
					      let new_np =
						  `MapExpression(`Sum(
								     `MapAggregate(`Sum, l, q), `MapAggregate(`Sum, r, q))) in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@[new_np] in
						  simplify new_m_expr new_rewrites 
						      
					| `MapExpression(`MapAggregate (fn, `Product(l,r), `Cross(ql, qr))) ->
					      let unbound_l = get_unbound_map_attributes_from_plan l ql false in
					      let unbound_r = get_unbound_map_attributes_from_plan r qr false in
						  begin
						      match (unbound_l, unbound_r) with
							  | ([], []) -> 
								let new_np = `MapExpression(
								    `Product(`MapAggregate(fn, l, ql), `MapAggregate(fn, r, qr)))
								in
								let new_m_expr = splice m_expr np new_np in
								let new_rewrites = t@[new_np] in
								    simplify new_m_expr new_rewrites
									
							  | _ -> simplify m_expr (t@[np])
						  end
						      
					| `MapExpression(`MapAggregate (fn, f, q)) ->
					      begin
						  if is_independent f q then
						      let new_np =
							  match fn with
							      | `Sum -> `MapExpression(
								    `Product(f, `MapAggregate(fn, `METerm (`Int 1), q)))
							      | `Min -> `MapExpression(f)
							      | `Max -> raise InvalidExpression (* TODO *)
						      in
						      let new_m_expr = splice m_expr np new_np in
						      let new_rewrites = t@[new_np] in
							  simplify new_m_expr new_rewrites
						  else
						      simplify m_expr (t@[np])
					      end

					| _ -> simplify m_expr (t@[np])
				end 
	      end


(*
 *
 * Constant lifting and variable binding.
 *
 *)

(* pull up constant bindings *)
let bind_expr_pair (le, lb) (re, rb) :
	bool * expression * (binding list) * expression * (binding list)
	=
    let (lc, leb) = (is_constant_expr le, (is_bound_expr le lb)) in
    let (rc, reb) = (is_constant_expr re, (is_bound_expr re rb)) in
	match (lc, leb, rc, reb) with
	    | (true, true, true, true) ->
		  let lv = match le with
		      | `ETerm(`Variable x) -> `Variable x | _ -> raise InvalidExpression
		  in
		  let rv = match re with
		      | `ETerm(`Variable x) -> `Variable x | _ -> raise InvalidExpression
		  in
		  let (l_def, r_def) =
		      (get_expr_definition lv lb, get_expr_definition rv rb)
		  in
		      (true, l_def, (remove_expr_binding lv lb),
		       r_def, (remove_expr_binding rv rb))
			  
	    | (true, false, true, false) ->
		  let (l_sym, r_sym) = (gen_var_sym(), gen_var_sym()) in
		      (true,
		       `ETerm(`Variable(l_sym)), lb@[`BindExpr(l_sym, le)],
		       `ETerm(`Variable(r_sym)), rb@[`BindExpr(r_sym, re)])
			  
	    | (true, true, true, false) ->
		  let lv = match le with
		      | `ETerm(`Variable x) -> `Variable x | _ -> raise InvalidExpression
		  in
		  let l_def = get_expr_definition lv lb in
		  let r_sym = gen_var_sym() in
		      (true, l_def, (remove_expr_binding lv lb),
		       `ETerm(`Variable(r_sym)), rb@[`BindExpr(r_sym, re)])
			  
	    | (true, false, true, true) ->
		  let rv = match re with
		      | `ETerm(`Variable x) -> `Variable x | _ -> raise InvalidExpression
		  in
		  let l_sym = gen_var_sym() in
		  let r_def = get_expr_definition rv rb in
		      (true, `ETerm(`Variable(l_sym)), lb@[`BindExpr(l_sym, le)],
		       r_def, (remove_expr_binding rv rb))
			  
	    | (true, false, _, _) ->
		  let l_sym = gen_var_sym() in
		      (false, `ETerm(`Variable(l_sym)), lb@[`BindExpr(l_sym, le)], re, rb)
			  
	    | (_, _, true, false) ->
		  let r_sym = gen_var_sym() in
		      (false, le, lb, `ETerm(`Variable(r_sym)), rb@[`BindExpr(r_sym, re)])
			  
	    | _ -> (false, le, lb, re, rb)

(* expression -> expression * bindings list *)
let rec extract_expr_bindings expr : expression * (binding list) =
    match expr with
	| `ETerm (t) -> (expr, [])
	| `UnaryMinus (e) -> extract_expr_bindings e
	| `Sum (l, r) | `Product (l, r) | `Minus (l, r) | `Divide (l, r) ->
	      let (le, lb) = extract_expr_bindings l in
	      let (re, rb) = extract_expr_bindings r in
	      let (bound, nl, nlb, nr, nrb) = bind_expr_pair (le, lb) (re, rb)
 	      in
		  begin
		      match (bound, expr) with
			  | (true, `Sum (_,_)) ->
				let new_var = gen_var_sym() in
				    (`ETerm(`Variable(new_var)),
				     nlb@nrb@[`BindExpr(new_var, `Sum (nl, nr))])

			  | (true, `Product (_,_)) ->
				let new_var = gen_var_sym() in
				    (`ETerm(`Variable(new_var)),
				     nlb@nrb@[`BindExpr(new_var, `Product (nl, nr))])

			  | (true, `Minus (_,_)) -> 
				let new_var = gen_var_sym() in
				    (`ETerm(`Variable(new_var)),
				     nlb@nrb@[`BindExpr(new_var, `Minus (nl, nr))])

			  | (true, `Divide (_,_)) -> 
				let new_var = gen_var_sym() in
				    (`ETerm(`Variable(new_var)),
				     nlb@nrb@[`BindExpr(new_var, `Divide (nl, nr))])

			  | (false, `Sum _) -> (`Sum (nl, nr), nlb@nrb)
			  | (false, `Product _) -> (`Product (nl, nr), nlb@nrb)
			  | (false, `Minus _) -> (`Minus (nl, nr), nlb@nrb)
			  | (false, `Divide _) -> (`Divide (nl, nr), nlb@nrb)

			  | _ -> raise InvalidExpression
		  end

	| `Function (fid, args) ->
	      let (argse, argsb) = List.split (List.map extract_expr_bindings args) in
		  (`Function (fid, argse), List.flatten argsb) 

(* bool_expression -> bool_expression * bindings list *)
let rec extract_bool_expr_bindings b_expr : boolean_expression * (binding list) =
    match b_expr with
	    (* | `BoolVariable _ -> (b_expr, []) *)
	| `BTerm(`True) | `BTerm(`False) -> (b_expr, [])

	| `BTerm(`LT (l, r)) | `BTerm(`LE (l, r))
	| `BTerm(`GT (l, r)) | `BTerm(`GE (l, r))
	| `BTerm(`EQ (l, r)) | `BTerm(`NE (l, r)) ->
	      let (le, lb) = extract_expr_bindings l in
	      let (re, rb) = extract_expr_bindings r in
		  (*
		    let (bind, nl, nlb, nr, nrb) = bind_expr_pair (le, lb) (re, rb) in
		    begin
		    if bind then
		    (let new_var = gen_var_sym() in
		    match b_expr with
		    | `LT _ ->
		    (`BoolVariable(new_var), lb@rb@[`Bind(new_var, `LT(nl, nr))])
		    | `LE _ -> (`LE(le, re), lb@rb)
		    (`BoolVariable(new_var), lb@rb@[`Bind(new_var, `LE(nl, nr))])
		    | `GT _ -> (`GT(le, re), lb@rb)
		    (`BoolVariable(new_var), lb@rb@[`Bind(new_var, `GT(nl, nr))])
		    | `GE _ -> (`GE(le, re), lb@rb)
		    (`BoolVariable(new_var), lb@rb@[`Bind(new_var, `GE(nl, nr))])
		    | `EQ _ -> (`EQ(le, re), lb@rb)
		    (`BoolVariable(new_var), lb@rb@[`Bind(new_var, `EQ(nl, nr))])
		    | `NE _ -> (`NE(le, re), lb@rb)
		    (`BoolVariable(new_var), lb@rb@[`Bind(new_var, `NE(nl, nr))]))
		    else
		    (match b_expr with 
		    | `LT _ -> (`LT(le, re), lb@rb)
		    | `LE _ -> (`LE(le, re), lb@rb)
		    | `GT _ -> (`GT(le, re), lb@rb)
		    | `GE _ -> (`GE(le, re), lb@rb)
		    | `EQ _ -> (`EQ(le, re), lb@rb)
		    | `NE _ -> (`NE(le, re), lb@rb))
		    end
		  *)
		  begin
		      match b_expr with 
			  | `BTerm(`LT(_,_)) -> (`BTerm(`LT(le, re)), lb@rb)
			  | `BTerm(`LE(_,_)) -> (`BTerm(`LE(le, re)), lb@rb)
			  | `BTerm(`GT(_,_)) -> (`BTerm(`GT(le, re)), lb@rb)
			  | `BTerm(`GE(_,_)) -> (`BTerm(`GE(le, re)), lb@rb)
			  | `BTerm(`EQ(_,_)) -> (`BTerm(`EQ(le, re)), lb@rb)
			  | `BTerm(`NE(_,_)) -> (`BTerm(`NE(le, re)), lb@rb)
			  | _ -> raise InvalidExpression
		  end

	| `BTerm(`MEQ(m_expr)) | `BTerm(`MLT(m_expr)) -> 
	      let (mee, meb) = extract_map_expr_bindings m_expr in
		  begin
		      match b_expr with
			  | `BTerm(`MEQ(_)) -> (`BTerm(`MEQ(mee)), meb)
			  | `BTerm(`MLT(_)) -> (`BTerm(`MLT(mee)), meb)
			  | _ -> raise InvalidExpression
		  end
		      
	| `And (l,r) | `Or (l,r) ->
	      let (le, lb) = extract_bool_expr_bindings l in
	      let (re, rb) = extract_bool_expr_bindings r in
		  begin match b_expr with
		      | `And _ ->  (`And (le, re), lb@rb)
		      | `Or _ -> (`Or (le, re), lb@rb)
		      | _ -> raise InvalidExpression
		  end

	| `Not (be) -> let (bee, beb) = extract_bool_expr_bindings be in (`Not(bee), beb)


(* map_expression -> map_expression * (bindings list) *)
and extract_map_expr_bindings m_expr : map_expression * (binding list) =
    match m_expr with
	| `METerm x -> (m_expr, [])
	| `Delta(_,_) -> (m_expr, [])
	| `Sum(l,r) ->
	      let (le, lb) = extract_map_expr_bindings l in
	      let (re, rb) = extract_map_expr_bindings r in
		  (`Sum(le, re), lb@rb)

	| `Minus(l,r) ->
	      let (le, lb) = extract_map_expr_bindings l in
	      let (re, rb) = extract_map_expr_bindings r in
		  (`Minus(le, re), lb@rb)

	| `Product(l,r) ->
	      let (le, lb) = extract_map_expr_bindings l in
	      let (re, rb) = extract_map_expr_bindings r in
		  (`Product(le, re), lb@rb)

	| `Min (l,r) ->
	      let (le, lb) = extract_map_expr_bindings l in
	      let (re, rb) = extract_map_expr_bindings r in
		  (`Min(le, re), lb@rb)

	| `Max (l,r) ->
	      let (le, lb) = extract_map_expr_bindings l in
	      let (re, rb) = extract_map_expr_bindings r in
		  (`Max(le, re), lb@rb)

	| `MapAggregate(fn, f,q) ->
	      let (fe, fb) = extract_map_expr_bindings f in
	      let (qq, qb) = extract_plan_bindings q in
		  if (is_independent fe qq) then
		      let new_var = gen_var_sym() in
			  begin
			      match fn with
				  | `Sum ->
					(`Product(
					     `METerm(`Variable(new_var)),
					     `MapAggregate(fn, `METerm(`Int(1)), qq)),
					 (fb@qb@[`BindMapExpr(new_var, fe)]))
				  | `Min -> (f, fb)
				  | `Max -> raise InvalidExpression
			  end
		  else (`MapAggregate(fn, fe, qq), fb@qb)
		      
	| _ ->
	      raise (RewriteException "Invalid map expression for extract_map_expr_bindings")

(* plan -> plan * bindings list *)
and extract_plan_bindings q : plan * (binding list) =
    match q with
	| `Relation (name, fields) -> (q, [])

	| `Select (pred, cq) ->
	      let (prede, predb) = extract_bool_expr_bindings pred in
	      let (cqq, cqb) = extract_plan_bindings cq in
	      let unbound_attrs = (get_unbound_attributes_from_predicate prede false) in
		  if (List.length unbound_attrs) = 1 then
		      let uba = List.hd unbound_attrs in
		      let predem = is_monotonic prede uba in

			  print_endline ("Checking monotonicity for "^
					     (string_of_attribute_identifier uba)^":" ^(string_of_bool predem));

			  let (new_q, new_b) =
			      begin
				  match (predem, prede) with
				      | (true, `BTerm(`MEQ(_))) ->
					    let new_var = gen_var_sym() in
						(`Select(
						     `BTerm(`EQ(`ETerm(`Attribute(uba)),
								`ETerm(`Variable(new_var)))), cqq),
						 predb@cqb@[`BindMapExpr(new_var,
									 `MapAggregate(`Min,
										       `METerm(`Attribute(uba)), `Select(prede, cqq)))])

				      | (true, `BTerm(`MLT(_))) ->
					    let new_var = gen_var_sym() in
						(`Select(
						     `BTerm(`LT(`ETerm(`Attribute(uba)),
								`ETerm(`Variable(new_var)))), cqq),
						 predb@cqb@[`BindMapExpr(new_var,
									 `MapAggregate(`Min,
										       `METerm(`Attribute(uba)), `Select(prede, cqq)))])

				      | (_, _) -> (`Select(prede, cqq), predb@cqb)
			      end
			  in
			      (new_q, new_b)
		  else
		      (* TODO: handle multivariate monotonicity *)
		      (`Select(prede, cqq), predb@cqb)

	| `Project (attrs, cq) ->
	      let (attrse, attrsb) =
		  List.split
		      (List.map
			   (fun (a,e) ->
				let (ee, eb) = extract_expr_bindings e in ((a,ee), eb))
			   attrs)
	      in
	      let (cqq, cqb) = extract_plan_bindings cq in
		  (`Project (attrse, cqq), (List.flatten attrsb)@cqb)

	| `Union children ->
	      let (new_ch, ch_b) =
		  List.split (List.map extract_plan_bindings children)
	      in
		  (`Union new_ch, List.flatten ch_b)

	| `Cross (l, r) ->
	      let (lq, lb) = extract_plan_bindings l in
	      let (rq, rb) = extract_plan_bindings r in
		  (`Cross (lq, rq), lb@rb)

	| `NaturalJoin (l, r) -> 
	      let (lq, lb) = extract_plan_bindings l in
	      let (rq, rb) = extract_plan_bindings r in
		  (`NaturalJoin (lq, rq), lb@rb)

	| `Join (pred, l, r) ->
	      (* TODO: think about monotonicity in join predicates.
	       * -- this may need multivariate monotonicity *)
	      let (prede, predb) = extract_bool_expr_bindings pred in
	      let (lq, lb) = extract_plan_bindings l in
	      let (rq, rb) = extract_plan_bindings r in
		  (`Join (prede, lq, rq), predb@lb@rb)

	| `TrueRelation | `FalseRelation | `DeltaPlan (_,_) -> (q, []) 

	| `NewPlan(cq) | `IncrPlan(_,_,cq) | `IncrDiffPlan(_,_,cq) -> 
	      let (cqq, cqb) = extract_plan_bindings cq in (q, cqb)

(*
 *
 * Incrementality analysis
 *
 *)

let rec compute_initial_value m_expr =
    match m_expr with
	| `METerm x -> `METerm x
	      
	| `Sum(l,r) ->
	      `Sum(compute_initial_value l, compute_initial_value r)
		  
	| `Minus(l,r) ->
	      `Minus(compute_initial_value l, compute_initial_value r)
		  
	| `Product(l,r) ->
	      `Product(compute_initial_value l, compute_initial_value r)

	| `Min(l,r) ->
	      `Min(compute_initial_value l, compute_initial_value r)

	| `Max(l,r) ->
	      `Max(compute_initial_value l, compute_initial_value r)

	| `MapAggregate(fn, f, q) -> `METerm (`Int 0)
	      
	| _ -> raise (RewriteException "Invalid intial value expression.") 

let rec apply_recompute_rules m_expr delta : map_expression =
    match m_expr with
	| `METerm _ -> `Incr(gen_state_sym(), (match delta with `Insert _ -> `Plus | `Delete _ -> `Minus), m_expr)
	| `Sum (l,r) ->
	      let new_l = apply_recompute_rules l delta in
	      let new_r = apply_recompute_rules r delta in
		  begin
		      match (new_l, new_r) with
			  | (`Incr (sx,ox,x), `Incr (sy,oy,y)) -> 
				if ox = oy then `Incr(sx, ox, `Sum(x, y))
			   	else `New(`Sum (new_l, new_r))
			  | (`New x, `New y) -> `New(`Sum(x, y))
			  | (`Incr x, `New y) -> `New(`Sum(new_l, y))
			  | (`New x, `Incr y) -> `New(`Sum(x, new_r))
			  | _ -> raise InvalidExpression
		  end

	| `Minus (l,r) ->
	      let new_l = apply_recompute_rules l delta in
	      let new_r = apply_recompute_rules r delta in
		  begin
		      match (new_l, new_r) with
			  | (`Incr (sx,ox,x), `Incr (sy,oy,y)) -> 
				if ox = oy then `Incr(sx, ox, `Minus(x, y))
			   	else `New(`Minus (new_l, new_r))
			  | (`New x, `New y) -> `New(`Minus(x, y))
			  | (`Incr x, `New y) -> `New(`Minus(new_l, y))
			  | (`New x, `Incr y) -> `New(`Minus(x, new_r))
			  | _ -> raise InvalidExpression
		  end

	| `Product (l,r) ->
	      let new_l = apply_recompute_rules l delta in
	      let new_r = apply_recompute_rules r delta in
		  begin
		      match (new_l, new_r) with
			  | (`Incr (sx,ox,x), `Incr (sy,oy,y)) -> 
				if ox = oy then `Incr(sx, ox, `Product(x, y))
			   	else `New(`Product (new_l, new_r))
			  | (`New x, `New y) -> `New(`Product(x, y))
			  | (`Incr x, `New y) -> `New(`Product(new_l, y))
			  | (`New x, `Incr y) -> `New(`Product(x, new_r))
			  | _ -> raise InvalidExpression
		  end

	| `Min (l,r) ->
	      let new_l = apply_recompute_rules l delta in
	      let new_r = apply_recompute_rules r delta in
		  begin
		      match (new_l, new_r) with
			  | (`Incr x, `Incr y) -> `Min(new_l, new_r)
			  | (`New x, `New y) -> `New(`Min(x, y))
			  | (`Incr x, `New y) -> `New(`Min(new_l, y))
			  | (`New x, `Incr y) -> `New(`Min(x, new_r))
			  | _ -> raise InvalidExpression
		  end

	| `Max (l,r) ->
	      let new_l = apply_recompute_rules l delta in
	      let new_r = apply_recompute_rules r delta in
		  begin
		      match (new_l, new_r) with
			  | (`Incr x, `Incr y) -> `Max(new_l, new_r)
			  | (`New x, `New y) -> `New(`Max(x, y))
			  | (`Incr x, `New y) -> `New(`Max(new_l, y))
			  | (`New x, `Incr y) -> `New(`Max(x, new_r))
			  | _ -> raise InvalidExpression
		  end

	| `MapAggregate (fn, f, q) ->
	      let new_f = apply_recompute_rules f delta in
	      let new_q = apply_recompute_plan_rules q delta in
		  begin
		      print_endline ("CP2:"^(match fn with | `Sum -> "sum" | `Min -> "min" | `Max -> "max"));
		      print_endline ("CP2a: "^(string_of_map_expression new_f));
		      print_endline ("CP2b: "^(string_of_plan new_q));
		      match (fn, new_f, new_q) with
			  | (`Sum, `Incr (sx,o,x), `IncrPlan (_,_,y)) -> `Incr (sx, o, `MapAggregate(fn, x, y)) (* TODO : check this out *)
			  (*| (`Min, `Incr x, `IncrPlan y) -> `New (`MapAggregate(fn, new_f, new_q)) *)
			  | (`Min, `Incr (sx,o,x), `IncrPlan (_,_,y)) -> 
				begin
				match delta with 
				    | `Insert _ -> `Incr (sx, `Min, `MapAggregate(fn, x, y))
				    | `Delete _ -> `Incr (sx, `Decrmin (x), `MapAggregate(fn, x, y))
				end
			  | (_, `New x, `NewPlan y) -> `New (`MapAggregate(fn, x, y))
			  | (_, `Incr x, `NewPlan y) -> `New (`MapAggregate(fn, new_f, y))
			  | (_, `New x, `IncrPlan y) -> `New (`MapAggregate(fn, x, new_q))
			  | _ -> raise InvalidExpression
		  end

	| `Delta _  | `New _ | `Incr _ | `IncrDiff _ | `Init _ | `Insert _ | `Update _ | `Delete _ | `IfThenElse _ ->
	      raise (RewriteException 
			 "Invalid map expression argument for apply_recompute_rules.")

and apply_recompute_plan_rules q delta : plan =
    match q with
	| `Relation _ -> `IncrPlan (gen_state_sym(), (match delta with `Insert _ -> `Union | `Delete _ -> `Diff), q)
	| `Select (pred, cq) ->
	      begin
		  match pred with 
		      | `BTerm(`MEQ(m_expr))
		      | `BTerm(`MLT(m_expr)) ->
			    let mer = apply_recompute_rules m_expr delta in
			    let cqr = apply_recompute_plan_rules cq delta in
			    let mer_new =
				match mer with
				    | `Incr x ->
					  begin
					      match pred with
						  | `BTerm(`MEQ(_)) -> `BTerm(`MEQ(mer))
						  | `BTerm(`MLT(_)) -> `BTerm(`MLT(mer))
						  | _ -> raise InvalidExpression
					  end

				    | `New x ->
					  begin
					      match pred with
						  | `BTerm(`MEQ(_)) -> `BTerm(`MEQ(x))
						  | `BTerm(`MLT(_)) -> `BTerm(`MLT(x))
						  | _ -> raise InvalidExpression
					  end
				    | _ -> raise InvalidExpression
			    in
			    let cqr_new =
				match cqr with
				    | `IncrPlan x -> cqr | `NewPlan x -> x
				    | _ -> raise InvalidExpression
			    in
				`NewPlan(`Select(mer_new, cqr_new))

		      | _ ->
			    print_endline ("CP1: "^(string_of_plan q));
			    let cqr = apply_recompute_plan_rules cq delta in
				begin
				    match cqr with
					| `NewPlan cqq -> `NewPlan(`Select (pred, cqq))
					| `IncrPlan (sid, op, cqq) -> `IncrPlan(sid, op, `Select (pred, cqq))
					| _ -> raise InvalidExpression
				end
	      end

	| `Project (attrs, cq) ->
	      let cqr = apply_recompute_plan_rules cq delta in
		  begin
		      match cqr with
			  | `NewPlan x -> `NewPlan (`Project(attrs, x)) 
			  | `IncrPlan x -> `NewPlan (`Project(attrs, cqr))
			  | _ -> raise InvalidExpression
		  end

	| `Union children ->
	      let rc = List.map (fun x -> (apply_recompute_plan_rules x delta)) children in
	      let all_incr = List.for_all
		  (fun x -> match x with | `IncrPlan _ -> true | _ -> false) rc
	      in
		  begin
		      if all_incr then
		  	  let t = List.hd rc in
		  	  let (sid, op) = match t with `IncrPlan(sid,o,_) -> (sid, o) | _ -> raise InvalidExpression in 
	      		  let all_oplus = 
		  	      List.for_all
		      		  (fun x -> match x with | `IncrPlan(_,o,_) -> o = op | _ -> false) rc
			  in 
			  begin 
			      if all_oplus then 
			          `IncrPlan(sid, op, `Union
					(List.map
					     (fun x -> match x with
						  | `IncrPlan (_,_,c) -> c | _ -> raise InvalidExpression) rc))
			      else 
			  	  `NewPlan(`Union
				        (List.map
					    (fun x -> match x with
						 | `NewPlan c -> c
						 | `IncrPlan c -> x
						 | _ -> raise InvalidExpression) rc))
				
			  end
		      else
			  `NewPlan(`Union
				       (List.map
					    (fun x -> match x with
						 | `NewPlan c -> c
						 | `IncrPlan c -> x
						 | _ -> raise InvalidExpression) rc))
		  end

	| `Cross (l, r) ->
	      let lr = apply_recompute_plan_rules l delta in
	      let rr = apply_recompute_plan_rules r delta in
		  begin
		      match (lr, rr) with
			  | (`IncrPlan (sx,ox,x), `IncrPlan (_,oy,y)) -> 
				if ox = oy then `IncrPlan(sx, ox, `Cross (x,y))
				else `NewPlan(`Cross(lr, rr))
			  | (`NewPlan x, `NewPlan y) -> `NewPlan(`Cross(x,y))
			  | (`IncrPlan x, `NewPlan y) -> `NewPlan(`Cross(lr, y))
			  | (`NewPlan x, `IncrPlan y) -> `NewPlan(`Cross(x, rr)) 
			  | _ -> raise InvalidExpression
		  end

	| `NaturalJoin (l, r) ->
	      let lr = apply_recompute_plan_rules l delta in
	      let rr = apply_recompute_plan_rules r delta in
		  begin
		      match (lr, rr) with
			  | (`IncrPlan (sx,ox,x), `IncrPlan (_,oy,y)) ->
				if ox = oy then `IncrPlan(sx, ox, `NaturalJoin (x,y))
				else `NewPlan(`NaturalJoin(lr, rr))
			  | (`NewPlan x, `NewPlan y) -> `NewPlan(`NaturalJoin (x,y))
			  | (`IncrPlan x, `NewPlan y) -> `NewPlan(`NaturalJoin(lr, y))
			  | (`NewPlan x, `IncrPlan y) -> `NewPlan(`NaturalJoin(x, rr)) 
			  | (_,_) -> raise InvalidExpression
		  end
		      
		      
	| `Join (pred, l, r) ->
	      begin
		  match pred with
		      | `BTerm(`MEQ(m_expr)) | `BTerm(`MLT(m_expr)) ->
			    let mer =
				match apply_recompute_rules m_expr delta with
				    | `New(x) -> x | `Incr(x) as y-> y
				    | _ -> raise InvalidExpression
			    in
			    let lr =
				match apply_recompute_plan_rules l delta with
				    | `NewPlan(x) -> x | `IncrPlan(x) as y-> y
				    | _ -> raise InvalidExpression
			    in
			    let rr =
				match apply_recompute_plan_rules r delta with
				    | `NewPlan(x) -> x | `IncrPlan(x) as y-> y
				    | _ -> raise InvalidExpression
			    in
				begin
				    match pred with
					| `BTerm(`MEQ(m_expr)) -> `NewPlan(`Join(`BTerm(`MEQ(mer)), lr, rr))
					| `BTerm(`MLT(m_expr)) -> `NewPlan(`Join(`BTerm(`MLT(mer)), lr, rr))
					| _ -> raise InvalidExpression
				end

		      | _ ->
			    let lr = apply_recompute_plan_rules l delta in
			    let rr = apply_recompute_plan_rules r delta in
				begin
				    match (lr, rr) with
					| (`IncrPlan (sx,ox,x), `IncrPlan (_,oy,y)) ->
						if ox = oy then `IncrPlan(sx, ox, `Join (pred, x,y))
						else `NewPlan(`Join(pred, lr, rr))
					| (`NewPlan x, `NewPlan y) -> `NewPlan(`Join(pred, x, y))
					| (`IncrPlan x, `NewPlan y) -> `NewPlan(`Join(pred, lr, y))
					| (`NewPlan x, `IncrPlan y) -> `NewPlan(`Join(pred, x, rr)) 
					| (_,_) -> raise InvalidExpression
				end
	      end

	| `TrueRelation | `FalseRelation | `DeltaPlan _ | `NewPlan _ | `IncrPlan _ | `IncrDiffPlan _->
	      raise (RewriteException "Invalid plan for apply_recompute_plan_rules.")


let rec compute_new_map_expression (m_expr : map_expression) rcs =
    let cnmp_new (x : map_expression) =
	match x with
	    | `METerm y -> `METerm y 
		  
	    | `Sum(l,r) ->
		  `Sum(compute_new_map_expression l (Some New),
		       compute_new_map_expression r (Some New))
		      
	    | `Minus(l,r) ->
		  `Minus(compute_new_map_expression l (Some New),
		       compute_new_map_expression r (Some New))
		      
	    | `Product(l,r) ->
		  `Product(compute_new_map_expression l (Some New),
			   compute_new_map_expression r (Some New))
		      
	    | `Min(l,r) ->
		  `Min(compute_new_map_expression l (Some New),
		       compute_new_map_expression r (Some New))
		      
	    | `MapAggregate(fn,f,q) ->
		  `MapAggregate(fn, compute_new_map_expression f (Some New),
				compute_new_plan q (Some New))
	    | _ -> raise InvalidExpression
    in
    let cnmp_incr (x : map_expression) state_sym op =
	match x with
	    | `METerm e -> compute_initial_value x
	    | `Sum(l,r) ->
		  `Sum(
		      `Sum(compute_initial_value l, compute_initial_value r),
		      `Incr(state_sym, op, x))

	    | `Minus(l,r) ->
		  `Sum(
		      `Minus(compute_initial_value l, compute_initial_value r),
		      `Incr(state_sym, op, x))

	    | `Product(l,r) ->
		  let il = compute_initial_value l in
		  let ir = compute_initial_value r in
		      begin
			  match (il, ir) with
			      | (`METerm (`Int 0), `METerm (`Int 0)) -> `Incr(state_sym, op, x)
			      | (`METerm (`Int 0), a) -> `Incr(state_sym, op, x)
			      | (a, `METerm (`Int 0)) -> `Incr(state_sym, op, x)
			      | (a, b) -> `Product(`Product(a, b), `Incr(state_sym, op, x)) 
		      end
			  
	    | `Min (l,r) -> `Min(l,r)
		  
	    | `MapAggregate (`Sum, f, q) ->
		  `Sum(compute_initial_value x, `Incr(state_sym, op, x))
		      
	    | `MapAggregate (`Min, f, q) -> `Incr(state_sym, op, x)
		  

	    | _ -> raise InvalidExpression
    in
	match m_expr with
	    | `New(e) -> `New(cnmp_new e)
	    | `Incr(sid, o, e) -> cnmp_incr e sid o
	    | _ -> 
		  begin
		      match rcs with
			  | Some New -> cnmp_new m_expr
			  | Some Incr | _ ->
				raise (RewriteException "Invalid nested recomputed expression.")
		  end

and compute_new_plan (q : plan) rcs =
    let cnp_new (x : plan) =
	match x with
	    | `Relation (n,f) -> x
	    | `Select (pred, cq) ->
		  let new_pred =
		      match pred with
			  | `BTerm(`MEQ(m_expr)) ->
				`BTerm(`MEQ(compute_new_map_expression m_expr (Some New)))
				    
			  | `BTerm(`MLT(m_expr)) ->
				`BTerm(`MLT(compute_new_map_expression m_expr (Some New)))
				    
			  | _ -> pred
		  in
		  let new_cq = compute_new_plan cq (Some New) in
		      begin
			  match (pred, new_cq) with (*TODO check later *)
			      | (`BTerm(`MEQ(`Incr(sid, op, m_incr))), `IncrPlan(sid2, op2, cqq)) ->
				    `Union
					[`Select(`BTerm(`MEQ(`Init(sid, m_incr))), new_cq);
					 `Select(new_pred, cqq)]
					
			      | (`BTerm(`MLT(`Incr(sid, op, m_incr))), `IncrPlan(sid2, op2, cqq)) ->
				    `Union
					[`Select(`BTerm(`MLT(`Init(sid, m_incr))), new_cq);
					 `Select(new_pred, cqq)]
					
			      | _ ->  `Select(new_pred, new_cq)
		      end
			  
	    | `Project (a, cq) -> `Project(a, compute_new_plan cq (Some New))
	    | `Union ch -> `Union (List.map (fun c -> compute_new_plan c (Some New)) ch)
	    | `Cross (l,r) ->
		  `Cross(compute_new_plan l (Some New), compute_new_plan r (Some New))
	    | `NaturalJoin (l,r) ->
		  `NaturalJoin(compute_new_plan l (Some New), compute_new_plan r (Some New))
	    | `Join (pred,l,r) ->
		  begin
		      match pred with
			      (* TODO: recompute branches via unions as in select above *)
			  | `BTerm(`MEQ(m_expr)) ->
				`Join(`BTerm(`MEQ(compute_new_map_expression m_expr (Some New))),
				      compute_new_plan l (Some New), compute_new_plan r (Some New))

			  | `BTerm(`MLT(m_expr)) ->
				`Join(`BTerm(`MLT(compute_new_map_expression m_expr (Some New))),
				      compute_new_plan l (Some New), compute_new_plan r (Some New))
				    
			  | _ ->
				`Join(pred,
				      compute_new_plan l (Some New), compute_new_plan r (Some New))
		  end
	    | _ -> raise InvalidExpression
    in
	match q with
	    | `NewPlan(cq) -> cnp_new cq
	    | `IncrPlan(sid, op, cq) -> `IncrPlan(sid, op, cq)
	    | _ ->
		  begin
		      match rcs with
			  | Some New -> cnp_new q
			  | Some Incr | None ->
				raise (RewriteException "Invalid nested recomputed expression.")
		  end
		      
let compile_target m_expr delta =
    let compile_aux e = 
	let bh_code =
	    compute_new_map_expression (apply_recompute_rules e delta) (Some New)
	in
	let dh_code =
	    simplify_map_expr_constants
		(apply_delta_rules bh_code delta (Some New))
	in
	print_endline ("bh_code: "^(string_of_map_expression bh_code));
	(*print_endline ("bh_code 2:\n"^(indented_string_of_map_expression bh_code 0));*)
	(*print_endline ("delta code: \n"^
	   (indented_string_of_map_expression
	   (apply_delta_rules bh_code delta (Some New)) 0));*)
	print_endline ("dh_code: "^(indented_string_of_map_expression dh_code 0));
	print_endline ("# br: "^(string_of_int (List.length (get_bound_relations dh_code))));
	let br = List.map (fun x -> `Plan x) (get_bound_relations dh_code) in
	    simplify dh_code br
    in
    let (mee, meb) = extract_map_expr_bindings m_expr in
    let bound_exprs =
	List.map
	    (fun b ->
		 match b with
		     | `BindMapExpr(v,e) -> `BindMapExpr(v, compile_aux e)
		     | _ -> b)
	    meb
    in
	(compile_aux mee, bound_exprs)

let generate_all_events m_expr = 
    let gbr = get_base_relations m_expr 
    in
    List.fold_left 
        (fun acc x ->
	    match x with 
	    | `Relation (n,f) ->
		(* [`Insert (n);`Delete (n)] @ acc *)
		print_endline ("insert "^n);
		(`Insert (n,f)):: acc
	    | _ -> acc
	) [] gbr 

let rec extract_incremental m_expr =
    match m_expr with
	| `Delete _ 
	| `METerm _ -> []
	| `Sum (l, r)
	| `Minus (l, r)
	| `Product (l, r)
	| `Max (l, r) 
	| `Min (l, r) 
	| `IfThenElse (_, l, r) -> (extract_incremental l) @ (extract_incremental r)
	| `MapAggregate (_, e, _)
	| `Delta (_, e)
	| `Insert (_,_,e)
	| `Update (_,_,_,e)
	| `Init (_, e) -> extract_incremental e
	| `New (e)
	| `Incr (_, _, e) -> [e]
	| `IncrDiff (_, _, e) -> [e]

let rec extract_incremental_binding binding =
    List.concat (
        List.fold_left
 	    (fun acc x -> 
	        match x with
		    | `BindMapExpr (v, m) -> (extract_incremental m)::acc
		    | _ -> acc
	    ) [] binding
    )

let set_difference l1 l2 = List.filter (fun el -> not(List.mem el l2)) l1

let concat (handler, bindings) =
    let bound_map_exprs =
        List.map
            (fun b -> match b with | `BindMapExpr(v,e) -> e | _ -> raise InvalidExpression)
                (List.filter
                    (fun b -> match b with | `BindMapExpr(v,e) -> true | _ -> false)
                        bindings)
    in handler :: bound_map_exprs

let print_handler_bindings (handler, bindings) = 
    let newlist = concat (handler, bindings)
    in
    print_endline "handler:";
    print_endline ("  "^string_of_map_expression (List.hd newlist));
    print_endline "bindings:";
    List.iter
        (fun h -> print_endline ("  "^(string_of_map_expression h)^"\n\n\n"))
            (List.tl newlist)

let compile_target_all m_expr = 
    let event_list = generate_all_events m_expr in
    let rec cta_aux e_list m_expr_list =
	List.concat ( List.map 
	    (fun event -> 
		let new_events = set_difference e_list [event] in 
		let result_list = 
		    List.map 
			(fun x -> compile_target x event) m_expr_list
		in 
		let new_expr = 
		    List.concat 
		        (List.map
			    (fun (handler, binding) -> 
				(extract_incremental handler) @ extract_incremental_binding binding 
			    )
			result_list)
		in 
		let children = 
		    match new_events with
		  	[] -> []
			| _ -> cta_aux new_events new_expr
		in (event, result_list)::children
	    ) e_list 
	)
    in 
    let result = cta_aux event_list [m_expr] in
    List.fold_left 
        (fun res event ->
	    let t = List.filter(fun (e,_) -> e = event) result in
	    (event, List.concat(List.map (fun (e,l2) -> l2) t)) :: res
	) [] event_list

(*
 *
 * Code generation
 *
 *)

(* TODO:*)
(* -- natural_join_predicate *)

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
	| `Sum (l,r) ->
	      `Sum(create_code_expression l, create_code_expression r)
		  
	| `Product (l,r)  ->
	      `Product(create_code_expression l, create_code_expression r)
		  
	| `Minus (l,r) -> raise InvalidExpression
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
	    | `Assign(x, c) ->
		  if not(List.mem c_expr assignments) then
		      `Assign(x, sa c)
		  else c_expr
		      
	    | `AssignMap((mid, kf), c) ->
		  `AssignMap((mid, substitute_cv_list kf), sa c)

	    | `EraseMap((mid, kf), c) ->
		  `EraseMap((mid, substitute_cv_list kf), 
			begin
			    match sa (`CTerm c) with `CTerm x -> x | _ -> raise InvalidExpression
			end)

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
				    if n_is_asgn then raise InvalidExpression
				    else `Declare(d)

			  | _ -> `Declare(d)
		  end

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
	| `Handler(n, args, rt, c) ->
	      `Handler(n, args, rt, merge_block_code (List.map simplify_code c) [])

	(* flatten singleton blocks *)
	| `Block ([`Block(x)]) -> `Block (List.map simplify_code x)
	| `Block ([x]) -> simplify_code x

	| `Block(x) ->
	      (* reorder locally scoped vars to beginning of block *)
	      let (decls, code) =
		  List.partition
		      (fun y -> match y with
			   | `Declare(z) -> true | _ -> false) x
	      in
	      let simplified_non_decls = List.map simplify_code code in
		  
	      (* substitute redundant vars *)
	      let (new_decls, substituted_code) =
		  let var_assignments =
		      List.filter
			  (fun c ->
			       match c with
				   | `Assign(_, `CTerm(`Variable(_))) -> true
				   | _ -> false)
			  simplified_non_decls
		  in
		  let var_declarations =
		      List.filter
			  (fun d -> match d with
			       | `Declare(`Variable(v1,_)) ->
				     List.exists
					 (fun a -> 
					      match a with
						  | `Assign(v2, _) -> v2 = v1
						  | _ -> raise InvalidExpression)
					 var_assignments
			       | _ -> false) decls
		  in
		  let filtered_decls =
		      List.filter
			  (fun c -> not (List.mem c var_declarations)) decls
		  in
		  let filtered_code =
		      List.filter
			  (fun c -> not (List.mem c var_assignments))
			  (List.map
			       (substitute_code_vars var_assignments)
			       simplified_non_decls)
		  in
		      (filtered_decls, filtered_code)
	      in
	      let reordered_code = new_decls@substituted_code in
	      let merged_code = merge_block_code reordered_code [] in
		  begin match merged_code with
		      | [] -> raise InvalidExpression
		      | [x] -> x
		      | h::t -> `Block(merged_code)
		  end

	| _ -> c_expr
	      (*
		in
		print_endline "Result:";
		print_endline (indented_string_of_code_expression r);
		print_endline (String.make 50 '-');
		r
	      *)


let gc_assign_state_var code decl var =
    begin match code with
	| `Eval x ->
	      let new_code =
		  `Block([
			     `Assign(var,
				     `Sum(`CTerm(`Variable(var)), x));
			     `Eval (`CTerm(`Variable(var)))])
	      in
		  (new_code, (`Variable(var, "int"))::decl)

	| `Block xl ->
	      let last_x = get_block_last code in
		  begin
		      match last_x with
			  | `Eval y -> 
				let new_code =
				    append_to_block
					(replace_block_last code
					     (`Assign(var,
						      `Sum(`CTerm(`Variable(var)), y))))
					(`Eval(`CTerm(`Variable(var))))
				in
				    (new_code, (`Variable(var, "int"))::decl)

			  | _ -> raise InvalidExpression
		  end

	| _ -> raise InvalidExpression
    end

let gc_assign_state_map code decl map_key map_decl =
    let map_access_code = `CTerm(`MapAccess(map_key)) in
    let new_decl = if List.mem map_decl decl then decl else map_decl::decl in
    let (assign_code, assign_decl) = 
	match code with
	    | `Eval x ->
		  let new_code =
		      `Block([
				 `AssignMap(map_key, `Sum(map_access_code, x));
				 `Eval(map_access_code)])
		  in
		      (new_code, new_decl)
			  
	    | `Block xl ->
		  let last_x = get_block_last code in
		      begin
			  match last_x with
			      | `Eval y -> 
				    let new_code =
					append_to_block
					    (replace_block_last code
						 (`AssignMap(map_key, `Sum(map_access_code, y))))
					    (`Eval(map_access_code))
				    in
					(new_code, new_decl)
					    
			      | _ -> raise InvalidExpression
		      end
			  
	    | _ -> raise InvalidExpression
    in
	(assign_code, assign_decl)

let is_bound expr binding =
    let rec ib_aux ex v =
	match ex with 
	    `MapAggregate (_, _, p) -> ib_aux_plan p v
	    | _ -> (false, "")
    and ib_aux_plan plan v =
	match plan with
	    `Select (be, p) -> ib_aux_boolean be v
	    | _ -> (false, "")
    and ib_aux_boolean be v =
	match be with
	    `BTerm bt -> ib_aux_bt bt v
	    | `Not b -> ib_aux_boolean b v
	    | `And (b1, b2) | `Or (b1, b2) ->
		let (ltf, lv) = (ib_aux_boolean b1 v) in
		let (rtf, rv) = (ib_aux_boolean b2 v) in
		    if ltf = true then (ltf, lv)
		    else if rtf = true then (rtf, rv)
		    else (false, "") (*TODO both are true *)
    and ib_aux_bt bt v = 
	match bt with
	    `MEQ (_) | `MLT (_) | `True | `False -> (false, "")
	    | `LT (e1, e2) | `LE (e1, e2) | `GT(e1, e2) 
	    | `GE (e1, e2) | `EQ (e1, e2) | `NE(e1, e2) -> begin
		match e2 with 
		    `ETerm (`Variable (x)) -> if x = v then (true, v) else (false, "")
	 	    | _ -> (false, "")
	        end
    in 
    let l = List.map (fun x -> match x with `BindMapExpr(v,_) -> (ib_aux expr v) 
					    | _ -> raise InvalidExpression) binding
    in let l2 = List.filter ( fun (t, _) -> t ) l
    in if List.length l2= 0 then (false, "")
	else List.hd l2 (* TODO multiple bindings *)

let generate_code handler bindings event =
    (* map_expression -> declaration list ->  code_expression * declaration list *)
    let rec gc_aux e decl bind_info  : code_expression * (declaration list) =
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
		  let (l_code, l_decl) = gc_aux l decl bind_info in
		  let (r_code, r_decl) = gc_aux r l_decl bind_info in
		      begin
			  match (l_code, r_code) with
			      | (`Eval x, `Eval y) ->  (`Eval(`Sum(x, y)), r_decl)
				    
			      | (`Block x, `Eval y) | (`Eval y, `Block x) ->
				    let new_block =
					merge_with_block (`Block x) (fun a -> `Eval(`Sum(a,y)))
				    in
					(new_block, r_decl)

			      | (`Block x, `Block y) ->
				    let new_block =
					merge_blocks l_code r_code (fun a b -> `Eval(`Sum(a,b)))
				    in
					(new_block, r_decl)
					    
			      | _ ->
				    print_endline ("suml: "^(string_of_code_expression l_code));
				    print_endline ("sumr: "^(string_of_code_expression r_code));
				    raise InvalidExpression
		      end

	    | `Minus(l,r) ->
		  let (l_code, l_decl) = gc_aux l decl bind_info in
		  let (r_code, r_decl) = gc_aux r l_decl bind_info in
		      begin
			  match (l_code, r_code) with
			      | (`Eval x, `Eval y) ->  (`Eval(`Minus(x, y)), r_decl)
				    
			      | (`Block x, `Eval y) | (`Eval y, `Block x) ->
				    let new_block =
					merge_with_block (`Block x) (fun a -> `Eval(`Minus(a,y)))
				    in
					(new_block, r_decl)

			      | (`Block x, `Block y) ->
				    let new_block =
					merge_blocks l_code r_code (fun a b -> `Eval(`Minus(a,b)))
				    in
					(new_block, r_decl)
					    
			      | _ ->
				    print_endline ("minusl: "^(string_of_code_expression l_code));
				    print_endline ("minusr: "^(string_of_code_expression r_code));
				    raise InvalidExpression
		      end

	    | `Product(l,r) ->
		  let (l_code, l_decl) = gc_aux l decl bind_info in
		  let (r_code, r_decl) = gc_aux r l_decl bind_info in
		      begin
			  match (l_code, r_code) with
			      | (`Eval x, `Eval y) ->  (`Eval(`Product(x, y)), r_decl)

			      | (`Block x, `Eval y) | (`Eval y, `Block x) ->
				    let new_block =
					merge_with_block (`Block x) (fun a -> `Eval(`Product(a,y)))
				    in
					(new_block, r_decl)

			      | (`Block x, `Block y) ->
				    let new_block =
					merge_blocks l_code r_code (fun a b -> `Eval(`Product(a,b)))
				    in
					(new_block, r_decl)
					    
			      | _ -> 
				    print_endline ("prodl: "^(string_of_code_expression l_code));
				    print_endline ("prodr: "^(string_of_code_expression r_code));
				    raise InvalidExpression
		      end

	    | `Min (l,r) ->
		  let (l_code, l_decl) = gc_aux l decl bind_info in
		  let (r_code, r_decl) = gc_aux r l_decl bind_info in
		      begin
			  match (l_code, r_code) with
			      | (`Eval x, `Eval y) ->  (`Eval(`Min(x, y)), r_decl)

			      | (`Block x, `Eval y) | (`Eval y, `Block x) ->
				    let new_block =
					merge_with_block (`Block x) (fun a -> `Eval(`Min(a,y)))
				    in
					(new_block, r_decl)

			      | (`Block x, `Block y) ->
				    let new_block =
					merge_blocks l_code r_code (fun a b -> `Eval(`Min(a,b)))
				    in
					(new_block, r_decl)
					    
			      | _ ->
				    print_endline ("minl:"^(string_of_code_expression l_code));
				    print_endline ("minr:"^(string_of_code_expression r_code));
				    raise InvalidExpression
		      end

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

	    | `Incr (sid, op, e) ->
		  (* TODO: incr var types *)
(*		  begin
		  (* if aggregate is Min then handle it first *) 
		  match e with `MapAggregate (`Min, f, q) ->
			let (e_code, e_decl) = gc_aux_min f q decl bind_info in
			raise InvalidExpression
		  | _ -> 
*)
		  let (e_code, e_decl) = gc_aux e decl bind_info in
		  let (e_is_bound, rc) = 
			match bind_info with (true, _, bb) -> is_bound e bb
			| _-> ( false, "")
		  in
		  if e_is_bound = true then
 		     let mid = gen_map_sym sid in
		     let map_key = (mid, [rc]) in
		     let temp = gen_var_sym() in
		     let c = gen_var_sym() in
		     let map_decl = `Map(mid, [(c, "int")], "int") in 
			 let last_code = get_block_last e_code in
			 let last_var = match last_code with `Eval(x) -> x | _-> raise InvalidExpression in
		         match event with 
			 | `Insert (_,_) ->
				 let insert_st = 
			     	 `Block ( [ 
					`Declare (`Variable(temp, "int"));
					`Assign (temp, `CTerm (`Variable (rc)));

					`ForEach ( map_decl, 
						`Block ([ 
							`Assign (rc, `CTerm (`Variable (c)));
							append_to_block (remove_block_last e_code) 
							(`AssignMap (map_key, `Sum(`CTerm(`MapAccess(map_key)), last_var)));
						])
					);
					(* something weired happens when simplify_code is called *)
					(*`Assign (rc, `CTerm (`Variable (temp)));*)
				 	`IfNoElse ( `BCTerm ( 
							`EQ ( 
								`CTerm ( 
									`MapContains(map_key)
								), 
					                        `CTerm (
									`MapIterator(`End(mid))
								)
							)
						  ) , 
						        append_to_block (remove_block_last e_code)
						        (`AssignMap ( map_key, last_var ))
						  );
					`Eval(`CTerm(`MapAccess(map_key)))
				 ])
				 in (insert_st, map_decl::e_decl)
				(* TODO: garbage collection, min *)	
			     | `Delete _ -> 
				 let delete_st = 
				 `Block ([
					`Declare (`Variable(temp, "int"));
					`Assign (temp, `CTerm (`Variable (rc)));
					append_to_block (remove_block_last e_code)
					(`AssignMap (map_key, `Sum(`CTerm (`MapAccess(map_key)),
						`Product (`CTerm (`Int (-1)), last_var))));
					(* TODO: if S_f[rc] = f(R=0)[rc]) delete S_f[rc] *)
	 				`ForEach (map_decl,
						`Block ([
							`Assign (rc, `CTerm (`Variable (c)));
							append_to_block (remove_block_last e_code)
							(`AssignMap (map_key, `Sum(`CTerm(`MapAccess(map_key)), 
									`Product(`CTerm (`Int (-1)), last_var))));
						])
					);
					`Eval(`CTerm(`MapAccess(map_key)))
				])
				in (delete_st, map_decl::e_decl)

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
				    gc_assign_state_var e_code e_decl incr_var

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
						  `Map(incr_mid,
						       List.map
							   (fun x -> (field_of_attribute_identifier x,"int")) e_uba,
						       "int")
					    | [m] -> m
					    | _ -> raise (RewriteException "Multiple matching maps.")
				in
				let map_key =
				    let mf = List.map field_of_attribute_identifier e_uba in
					(incr_mid, mf)
				in
				    gc_assign_state_map e_code e_decl map_key incr_map
		      end
(*		  end *)

	    | `Init (sid, e) ->				
		  let (e_code, e_decl) = gc_aux e decl bind_info in
		  let e_uba = get_unbound_attributes_from_map_expression e true in
		      begin match e_uba with
			  | [] ->
				let incr_var = gen_var_sym() in
				    gc_assign_state_var e_code e_decl incr_var

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
						  `Map(incr_mid,
						       List.map
							   (fun x -> (field_of_attribute_identifier x,"int")) e_uba,
						       "int")
					    | [m] -> m
					    | _ -> raise (RewriteException "Multiple matching maps.")
				in
				let map_key =
				    let mf = List.map field_of_attribute_identifier e_uba in
					(incr_mid, mf)
				in
				    gc_assign_state_map e_code e_decl map_key incr_map
		      end

	    | _ -> 
		  print_endline("gc_aux: "^(string_of_map_expression e));
		  raise InvalidExpression

    (* plan -> code_expression list -> code_expression * declaration list *)
    and gc_plan_aux q iter_code decl bind_info : code_expression * (declaration list) =
	match q with
	    | `Relation (n,f) ->
		  let new_decl =
		      if List.mem (`Relation (n,f)) decl then decl
		      else decl@[`Relation(n,f)]
		  in
		      (`ForEach(`List(n,f), iter_code), new_decl)

	    | `TrueRelation -> (iter_code, decl)

	    | `Select (pred, cq) ->
		  begin
		      match pred with 
			  | `BTerm(`MEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) ->
				(* TODO: pred var types *)
				let (pred_var_code, new_decl) = gc_aux m_expr decl bind_info in
				let (pred_cterm, assign_var_code, pred_decl) =
				    begin match pred_var_code with
					| `Block xl ->
					      let last_code = get_block_last pred_var_code in
						  begin match last_code with
						      | `Eval(`CTerm(`Variable(x))) ->
							    (`Variable(x), (remove_block_last pred_var_code), new_decl)
						      | `Eval(`CTerm(`MapAccess(mf))) ->
							    (`MapAccess(mf), (remove_block_last pred_var_code), new_decl)
						      | `Eval (x) ->
							    let pv = gen_var_sym() in 
								(`Variable(pv), 
								 (append_to_block
								      (remove_block_last pred_var_code)
								      (`Assign(pv, x))),
								 ((`Variable(pv, "int"))::new_decl))
						      | _ -> raise InvalidExpression
						  end
					| `Eval(`CTerm(`Variable(x))) ->
					      (`Variable(x), (remove_block_last pred_var_code), new_decl)
					| `Eval(`CTerm(`MapAccess(mf))) ->
					      (`MapAccess(mf), (remove_block_last pred_var_code), new_decl)
					| `Eval x ->
					      let pv = gen_var_sym() in
						  (`Variable(pv), `Assign(pv, x),
						   ((`Variable(pv, "int"))::new_decl))
					| _ -> raise InvalidExpression
				    end
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
					| (`Init x, `MapAccess(mf)) ->
					      let (mid, _) = mf in
					      let map_contains_code = `CTerm(`MapContains(mf)) in
						  `IfNoElse(
						      `BCTerm(`EQ(map_contains_code, `CTerm(`MapIterator(`End(mid))))),
						      `Block(
							  [assign_var_code;
							   `IfNoElse(pred_test_code, iter_code)]))
					| (`Init _, `Variable(_)) | (`Incr _, _) | _ ->
					      `Block(
						  [assign_var_code; `IfNoElse(pred_test_code, iter_code)])
				in
				    gc_plan_aux cq new_iter_code pred_decl bind_info 

			  | _ ->
				let new_iter_code =
				    `IfNoElse(create_code_predicate pred, iter_code)
				in
				    gc_plan_aux cq new_iter_code decl bind_info 
		  end
		      
	    (* 
	      | `Project (a, cq) ->
	      let new_iter_code = (gc_project a)@iter_code in
	      gc_plan_aux new_iter_code decl
	    *) 
		      
	    | `Union ch ->
		  let (ch_code, ch_decl) = 
		      List.split (List.map (fun c -> gc_plan_aux c iter_code decl bind_info ) ch)
		  in
		      (`Block(ch_code), List.flatten ch_decl)
			  
	    | `Cross(l,r) ->
		  let (inner_code, inner_decl) = gc_plan_aux r iter_code decl bind_info in
		      gc_plan_aux l inner_code inner_decl bind_info 
	
	    (* to handle rule 39, 41 *)
	    | `IncrPlan(sid, op, q) ->
		  let e_uba = 
		    match event with
		    | `Insert(_, vars) | `Delete(_, vars) ->
			List.filter 
			  (fun uaid -> not
			      (List.exists
				  (fun (id,_) -> (field_of_attribute_identifier uaid) = id)
				  vars))
			  (get_unbound_attributes_from_plan q true)
		  in
		  let (q_code, q_decl) =
			gc_plan_aux q iter_code decl bind_info
		  in 
		  let incrp_mid = gen_map_sym (gen_state_sym()) in
		  let incrp_map = `Map(incrp_mid, 
					List.map
					    (fun x -> (field_of_attribute_identifier x, "int")) e_uba,
					"int") in
		  print_endline (string_of_code_expression (`Declare incrp_map));
		  raise InvalidExpression

		  
	    (*
	      | `NaturalJoin (l,r) ->
	      ** TODO: create natural join predicate **
	      let nj_pred = natural_join_predicate l r in
	      let nj_iter_code = [`IfNoElse(nj_pred, iter_code)] in
	      let (inner_code, inner_decl) = gc_plan_aux r nj_iter_code decl in
	      gc_plan_aux l inner_code inner_decl

	      | `Join(p,l,r) ->
	      ** TODO: what if p is a map_expr? **
	      let (inner_code, inner_decl) = gc_plan_aux l [`IfNoElse(p, iter_code)] in
	      gc_plan_aux r inner_code inner_decl
	    *)

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
	  "Handler("^
	  (List.fold_left
	     (fun acc (id, typ) ->
	       (if (String.length acc = 0) then "" else acc^",")^id) "" fields)^
	  "):\n"^
	  (indented_string_of_map_expression handler 0));
	  fields
    in

    (* TODO: declared variable types *)
    let bound_map_exprs =
	List.filter
	    (fun b -> match b with | `BindMapExpr(v,e) -> true | _ -> false)
	    bindings
    in
    let binding_handlers_and_decls =
	List.map
	    (fun b ->
		 match b with
		     | `BindMapExpr (v,e) ->
			   let (bh,d) = gc_aux e [] (false, [], []) in
			       begin
				   match bh with
				       | `Eval x -> (`Assign(v, x), (`Variable(v, "int"))::d)
				       | `Block xl ->
					     begin
						 match (get_block_last bh) with
						     | `Eval y ->
							   ((append_to_block
								 (remove_block_last bh)
								 (`Assign(v, y))),
							    (`Variable(v, "int"))::d)
						     | _ -> raise InvalidExpression
					     end
				       | _ -> raise InvalidExpression
			       end
		     | _ -> raise InvalidExpression)
	    bound_map_exprs
    in
	
    let binding_code =
	List.flatten
	    (List.map
		 (fun (bh,d) ->
		      (List.map (fun x -> `Declare(x)) d)@[bh])
		 binding_handlers_and_decls)
    in
    let (handler_main, handler_decl) = gc_aux handler [] (true, binding_code, bindings) in
    let declarations = List.map (fun x -> `Declare(x)) handler_decl in
    let (result_code, result_type) = 
	match get_last_code_expr handler_main with
	    | `Eval x -> (`Return x, ctype_of_arith_code_expression x)
	    | _ -> raise InvalidExpression
    in
    let new_handler_main = remove_block_last handler_main in
(*    let new_handler_main =
	List.fold_left
	    (fun acc c ->
		 match c with
		     | `Variable _ -> acc
		     | `Relation(id,f) -> acc 
		     | `Map(mid, kf, rt) -> `ForEach (`Map(mid, kf, rt), acc)) 
	    (remove_block_last handler_main) handler_decl 
    in *)
    let (global_decls, handler_local_code) =
	List.partition
	    (fun x -> match x with
		 | `Declare(`Map _) | `Declare(`Relation _) -> true
		 | _ -> false)
(*	    (declarations@[new_handler_main]@binding_code@[result_code]) *)
	    (declarations@binding_code@[new_handler_main]@[result_code])
    in
	(global_decls, 
	 simplify_code
	     (`Handler(handler_name_of_event event,
		       handler_fields, result_type, handler_local_code)))

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

let compile_code m_expr event output_file_name =
    begin
	let (handler, bindings) = compile_target m_expr event in
	let (global_decls, handler_code) = generate_code handler bindings event in
	let (class_bodies, class_names, callers) =
	    List.fold_left 
		(fun (b, l, c) x -> match x with
			 `Declare d -> 
			     begin
				 match d with
					 `Relation(i,f)-> let (n, b2, c2) = make_file_streams `Relation(i,f)
					 in (b^b2, n::l, c2::c)
				     | _ -> (b, l, c)
			     end
		     | _ -> raise InvalidExpression ) ("", [], []) global_decls in

	let out_chan = open_out output_file_name in
	    (* Preamble *)
	    output_string out_chan "#include <iostream>\n";
	    output_string out_chan "#include <fstream>\n";
	    output_string out_chan "#include <cmath>\n";
	    output_string out_chan "#include <cstdio>\n";
	    output_string out_chan "#include <cstdlib>\n";
	    output_string out_chan "#include <map>\n";
	    output_string out_chan "#include <list>\n\n";
	    output_string out_chan "#include <tr1/tuple>\n";
	    output_string out_chan "#include <boost/tokenizer.hpp>\n";
	    output_string out_chan "using namespace std;\n";
	    output_string out_chan "using namespace tr1;\n";
	    output_string out_chan "using namespace boost;\n\n";
	    output_string out_chan "typedef tokenizer <char_separator<char> > tokeniz;\n\n";
	    
	    (* Global declarations *)
	    List.iter
		(fun x -> output_string out_chan
		     ((indented_string_of_code_expression x)^"\n")) global_decls;
	    output_string out_chan "\n";

	    (* file stream *)
	    output_string out_chan file_stream_body;
	    output_string out_chan class_bodies;

	    (* config handler *)
	    output_string out_chan (config_handler global_decls);
	    
	    (* Handlers *)
	    output_string out_chan
		((indented_string_of_code_expression handler_code)^"\n");
	    
	    let caller = 
		List.fold_left 
		    (fun acc x ->
			match handler_code with
	 	            | `Handler(name, _, _, _) -> acc ^ (x name)
			    | _ -> raise InvalidExpression
		    ) "" callers

	    (* Dummy main *)
(*	    let (dummy_call, dummy_args) =
		match handler_code with
		    | `Handler(name, args, _, _) -> (name,
						     List.fold_left
							 (fun acc (id, typ) ->
							      (if (String.length acc) = 0 then "" else acc^", ")^
								  (match typ with 
								       | "int" | "long" -> "0"
								       | "float" | "double" -> "0.0"
								       | "string" -> "\"a\""))
							 "" args)
		    | _ -> raise InvalidExpression
*)
	    in
	    let instantiate =
		let config_filename = "config" in
	 	    "    ifstream *config;\n"^
		    "    string fcon;\n"^
		    "\n"^
		    "    if(argc == 1) fcon = \""^config_filename^"\";\n"^
		    "    else fcon = argv[1];\n"^
		    "\n"^
		    "    config = new ifstream(fcon.c_str());\n"^
		    "    if(!config->good()) {\n"^
		    "        cerr << \"Failed to read config file\" << fcon << endl;\n"^
		    "        exit(1);\n"^
		    "    }\n"^
		    "\n"^
		    "    initialize(config);\n" 
	    in
	    let dummy_main =
		"\n"^
		    "int main(int argc, char* argv[])\n{\n"^
		    instantiate^
		    caller^
(*		    "    "^dummy_call^"("^dummy_args^");\n"^ *)
		    "    return 0;\n"^
		    "}\n"
	    in
		output_string out_chan dummy_main
    end
