open Algebra
open Codegen

(* TODO:
 * -- apply_delta_plan_rules: handle conjunctive/disjunctive map expression
 * comparisons for both selections and joins
 * -- more robust attribute naming, qualification, attribute comparison, aliasing  
 *)


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

        (* TODO: maps may be multidimensional, but as seen below, we should bind lookups *)
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
        (*
	| `NaturalJoin (l,r) ->
	      `NaturalJoin (ab_plan_aux l pa, ab_plan_aux r pa)
	| `Join (p, l, r) ->
	      let new_pa =
		  remove_proj_attrs
		      (remove_proj_attrs pa (get_bound_attributes l))
		      (get_bound_attributes r)
	      in
		  `Join(ab_pred_aux p new_pa, ab_plan_aux l pa, ab_plan_aux r pa) 
        *)
	| `TrueRelation -> `TrueRelation
	| `FalseRelation -> `FalseRelation
	| `DeltaPlan (b,e) -> `DeltaPlan (b, ab_plan_aux e pa)
	| `NewPlan(e) -> `NewPlan (ab_plan_aux e pa)
	| `IncrPlan(sid, p, e) -> `IncrPlan (sid, p, ab_plan_aux e pa)
	| `IncrDiffPlan(sid, p, e) -> `IncrDiffPlan (sid, p, ab_plan_aux e pa)

let add_map_expression_bindings m_expr proj_attrs =
    ab_map_expr_aux m_expr proj_attrs 

let add_plan_bindings plan proj_attrs =
    ab_plan_aux plan proj_attrs

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
	(me_al = 0) || (me_al > 0 && (me_al = unresolved_l))


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
	      let l =
                  List.filter
                      (fun x -> match x with
                          | `Relation (n,_) -> n = rel
                          | _ -> raise InvalidExpression )
                      br
              in
	          if List.length l = 0 then `METerm (`Int 0)
	          else 
	              let delta_plan = push_delta_plan(`DeltaPlan(ev,q)) in
		          begin 
		              match ev with 
			          | `Insert _ ->
                                        `MapAggregate(`Min, f, `IncrPlan (gen_state_sym(), `Union, delta_plan))
		   	          | `Delete _ ->
                                        (* check later *)
                                        `MapAggregate(`Min, f, `IncrDiffPlan (gen_state_sym(), `Diff, delta_plan))
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
		      
            (*
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
            *)
		      
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
	      if vareq then (if (x = y) then Some(`True) else None)
	      else if varneq then (if (x = y) then Some(`False) else None)
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

	(* TODO: static evaluation *)
	(*
	  begin
	  match (is_static_constant_expr(sl), is_static_constant_expr(sr)) with
	  | (true, true) -> eval(`IfThenElse(`BCTerm(`LT(sl, sr)), sl, sr))
	  | _ -> `Min(sl, sr)
	  end
	*)
		      
	| `Max (l, r) ->
	      let sl = simplify_map_expr_constants l in
	      let sr = simplify_map_expr_constants r in
		  `Max(sl, sr)
		      
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
                      
        (*
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
        *)

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

let distribute_conjunctive_predicate pred l r =
    let lba = get_bound_attributes l in
    let rba = get_bound_attributes r in
    let merge l r = match (l, r) with
        | (Some x, Some y) -> Some(`And(x,y))
        | (None, _) -> r | (_, None) -> l
    in
    let rec dcp_aux p =
        match p with 
            | `BTerm _ ->
                  let p_uba = get_unbound_attributes_from_predicate p false in
                  let num_uba = List.length p_uba in
                  let p_resolve_l = resolve_unbound_attributes p_uba lba in
                  let p_resolve_r = resolve_unbound_attributes p_uba rba in
                      begin
                          match (List.length p_resolve_l, List.length p_resolve_r) with
                              | (x, 0) when x = num_uba -> `Split(None, Some p)
                              | (0, x) when x = num_uba -> `Split(Some p, None)
                              | _ -> `NoSplit
                      end
            | `And (lp,rp) ->
                  let ld = dcp_aux lp in
                  let rd = dcp_aux rp in
                      begin
                          match (ld, rd) with
                              | (`NoSplit, _) | (_, `NoSplit) -> `NoSplit
                              | (`Split(ldl, ldr),`Split(rdl, rdr)) -> `Split(merge ldl rdl, merge ldr rdr)
                      end

            | _ -> `NoSplit
    in
        match (dcp_aux pred) with
            | `NoSplit -> (None, None)
            | `Split(x, y) -> (x, y)

(* TODO: think about unification for disjunctive predicates*)
let unify_predicate pred q =
    let qba = get_bound_attributes q in
    let rec unify_binop l r op_fn =
        let (lp, lu) = unify_aux l in
        let (rp, ru) = unify_aux r in
        let new_p = match (lp, rp) with
            | (None, _) -> rp | (_, None) -> lp
            | (Some x, Some y) -> Some(op_fn x y)
        in
            (new_p, lu@ru)
    and unify_aux p =
        match p with
            | `BTerm(`EQ(`ETerm(`Variable(x)), `ETerm(`Attribute(aid))))
            | `BTerm(`EQ(`ETerm(`Attribute(aid)), `ETerm(`Variable(x)))) ->
                  begin
                      match (resolve_unbound_attributes [aid] qba) with
                          | [] -> (Some p, [])
                          | [y] -> (None, [(aid, `ETerm(`Variable(x)))])
                          | _ -> raise (RewriteException "Unification failed.")
                  end
            | `BTerm _ -> (Some p, [])

            | `And(l,r) -> unify_binop l r (fun x y -> `And(x,y))
            | `Or(l,r) -> unify_binop l r (fun x y -> `Or(x,y))
            | `Not(b) -> unify_aux b
    in
        match (unify_aux pred) with
            | (None, u) -> 
                  (* All attributes are unbound, replace them as variables
                     for the predicate, for use in domain iteration, but use the
                     unified values elsewhere
                  let new_bindings =
                      List.map
                          (fun (aid, expr) ->
                              match aid with
                                  | `Qualified(_,f) | `Unqualified(f) ->
                                        (aid, `ETerm(`Variable(f))))
                          u
                  in
                  *)
                      (pred, u)

            | (Some(p), u) -> (p, u)

(* bottom up map expression rewriting *)
(* map expression -> accessor_element list -> map_expression *)
let rec simplify m_expr rewrites =
    (* Debugging helpers *)
    let print_call () =
        print_endline (String.make 50 '>');
        print_endline ("Expr:\n"^(indented_string_of_map_expression m_expr 0));
        print_endline
            ("Pending rewrites("^(string_of_int (List.length rewrites))^"):\n    "^
                (List.fold_left
                    (fun acc ae ->
                        (if (String.length acc) = 0 then "" else acc^"\n    ")^
                            (string_of_accessor_element ae)) 
                    "" rewrites));
        print_endline (String.make 50 '>')
    in
    let print_rewrite_step n t =
	print_endline ("Remainder rewrites: "^(string_of_int (List.length t)));
	print_endline ("Rewrite node: "^(string_of_accessor_element n))
    in
    let print_parent np =
	print_endline ("Found parent "^(string_of_accessor_element np));
	print_endline (String.make 50 '-');
    in
    let print_rule_description ruleid =
        let desc =
            match ruleid with
                | `PullProjectSelectTuple -> "pull tuple binding above selection"
                | `PullProjectSelect -> "pull project above selection"
                | `PullUnionSelect -> "pull union above selection"
                | `PullProjectCross -> "pull project above cross"
                | `PushSelectCross -> "push select below cross, w/ distribute, unify"
                | `PushSelect -> "push select down, w/ unify"
                | `BindAggTuple -> "binding aggregate args from tuple"
                | `BindAggQuery -> "binding aggregate args from query"
                | `DistributeSumAggUnion -> "distributing sum aggregate over union"
                | `DistributeMinAggUnion -> "distributing min aggregate over union"
                | `TupleAgg -> "simplifying tuple aggregate"
                | `DistributeProdAggCross -> "distributing product agg over cross"
                | `DistributeSumAgg -> "distributing sum agg over query"
                | `DistributeProdAgg -> "distributing product agg over query"
                | `DistributeIndepAgg -> "distributing independent agg"
                | `NoRule -> "ascending, no valid rule"
        in
            print_endline ("Applying rule: "^desc)
    in
    (* Code body *)
    print_call();
    match rewrites with
        | [] -> m_expr
	| (`MapExpression n)::t when n = m_expr -> m_expr
	| n::t ->
              begin
                  print_rewrite_step n t;

		  let x = parent m_expr n in
		      match x with
			  | None -> simplify m_expr t
			  | Some np ->
				begin
                                    print_parent np;

				    match np with
				        | `Plan(`Select (pred, `Project(projs, `TrueRelation))) ->
                                              print_rule_description `PullProjectSelectTuple;
					      let new_np = `Plan(
                                                  `Project(projs,
                                                  `Select(add_predicate_bindings pred projs, `TrueRelation)))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@[new_np] in
					          simplify new_m_expr new_rewrites

				        | `Plan(`Select (pred, `Project(projs, q))) ->
                                              print_rule_description `PullProjectSelect;
					      let new_np = `Plan(
                                                  `Project(projs,
                                                  `Select(add_predicate_bindings pred projs, q)))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = `Plan(q)::t in
					          simplify new_m_expr new_rewrites

					| `Plan(`Select (pred, `Union ch)) ->
                                              print_rule_description `PullUnionSelect;
					      let new_np =
						  `Plan(`Union
                                                      (List.map (fun c -> `Select(pred, c)) ch))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = (List.map (fun c -> `Plan c) ch)@t in
						  simplify new_m_expr new_rewrites

					| `Plan(`Cross(x,`Project(a,q)))
					| `Plan(`Cross(`Project(a,q),x)) ->
                                              print_rule_description `PullProjectCross;
					      let new_np = `Plan(`Project(a, `Cross(x,q))) in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = new_np::t in
						  simplify new_m_expr new_rewrites

                                        | `Plan(`Select(pred, (`Cross(l,r) as q))) ->
                                              print_rule_description `PushSelectCross;
                                              let (new_pred, unifications) = unify_predicate pred q in
                                              let dpred = distribute_conjunctive_predicate new_pred l r in
                                              let (new_dpred_np, dpred_rw) =
                                                  match dpred with
                                                      | (Some lpred, Some rpred) ->
                                                            (Some(`Cross(`Select(lpred, l), `Select(rpred,r))), [(`Plan l); (`Plan r)])
                                                      | (Some lpred, None) -> (Some(`Cross(`Select(lpred, l), r)), [(`Plan l)])
                                                      | (None, Some rpred) -> (Some(`Cross(l, `Select(rpred, r))), [(`Plan r)])
                                                      | (None, None) -> (None, [])
                                              in
                                              let (new_unified_np, rw_before) = 
                                                  match (new_dpred_np, unifications) with
                                                      | (None, []) -> (new_dpred_np, [])
                                                      | (Some x, []) -> (new_dpred_np, dpred_rw)
                                                      | (None, y) ->
                                                            let new_q = `Project(y, add_plan_bindings q y) in
                                                                (Some(`Select (new_pred, new_q)), [(`Plan new_q)])
                                                      | (Some x, y) -> (Some(`Project(y, add_plan_bindings x y)), dpred_rw)
                                              in
                                                  begin
                                                      match new_unified_np with
                                                          | None -> simplify m_expr (t@[np])
                                                          | Some new_np ->
                                                                let new_m_expr = splice m_expr np (`Plan new_np) in
                                                                let new_rewrites = rw_before@t in
                                                                    simplify new_m_expr new_rewrites
                                                  end

                                        | `Plan(`Select (pred, q)) ->
                                              print_rule_description `PushSelect;
                                              let (new_pred, unifications) = unify_predicate pred q in
                                                  begin
                                                      match unifications with
                                                          | [] -> simplify m_expr (t@[np])
                                                          | _ ->
                                                                (* Push unbound attr unifications down into query *)
                                                                let new_q = `Project(
                                                                    unifications, add_plan_bindings q unifications)
                                                                in
                                                                let new_np = `Plan(`Select (new_pred, new_q)) in
                                                                let new_m_expr = splice m_expr np new_np in
                                                                let new_rewrites = `Plan(new_q)::t in
                                                                    simplify new_m_expr new_rewrites
                                                  end

					| `MapExpression(
					      `MapAggregate(fn, f, `Project(projs, `TrueRelation))) ->
                                              print_rule_description `BindAggTuple;
					      let new_np = 
                                                  `MapExpression(add_map_expression_bindings f projs)
                                              in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = new_np::t in
						  simplify new_m_expr new_rewrites

					| `MapExpression(`MapAggregate (fn, f, `Project(projs, q))) ->
                                              print_rule_description `BindAggQuery;
					      let new_np = `MapExpression(
					          `MapAggregate (fn,
						  add_map_expression_bindings f projs, q))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = (`Plan(q))::t in
					          simplify new_m_expr new_rewrites
					              
					| `MapExpression(`MapAggregate (`Sum, f, `Union(c))) ->
                                              print_rule_description `DistributeSumAggUnion;
					      let new_np = `MapExpression(
						  List.fold_left
						      (fun acc ch -> `Sum(`MapAggregate(`Sum, f, ch), acc))
						      (`MapAggregate (`Sum, f, (List.hd c))) (List.tl c))
					      in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@(List.map (fun x -> `Plan x) c) in
						  simplify new_m_expr new_rewrites 

					| `MapExpression(`MapAggregate (`Min, f, `Union(c))) ->
                                              print_rule_description `DistributeMinAggUnion;
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
                                              print_rule_description `TupleAgg;
					      let new_np = `MapExpression f in
					      let new_m_expr = splice m_expr np new_np in
					      let new_rewrites = t@[(`MapExpression f)] in
						  simplify new_m_expr new_rewrites

					| `MapExpression(`MapAggregate (fn, `Product(l,r), `Cross(ql, qr))) ->
                                              print_rule_description `DistributeProdAggCross;
                                              let distribute dl dr dql dqr =
                                                  let new_np = `MapExpression(
						      `Product(`MapAggregate(fn, dl, dql), `MapAggregate(fn, dr, dqr)))
						  in
						  let new_m_expr = splice m_expr np new_np in
						  let new_rewrites = t@[new_np] in
						      simplify new_m_expr new_rewrites
                                              in
					      let unbound_lql = get_unbound_map_attributes_from_plan l ql false in
					      let unbound_rqr = get_unbound_map_attributes_from_plan r qr false in
					      let unbound_lqr = get_unbound_map_attributes_from_plan l qr false in
					      let unbound_rql = get_unbound_map_attributes_from_plan r ql false in
                                                  print_endline ("Matching agg_{a*b}(cross): "^
                                                      (string_of_int (List.length unbound_lql))^", "^
                                                      (string_of_int (List.length unbound_rqr))^", "^
                                                      (string_of_int (List.length unbound_lqr))^", "^
                                                      (string_of_int (List.length unbound_rql)));
                                                  print_endline ("Unbound lql: "^(string_of_attribute_identifier_list unbound_lql));
                                                  print_endline ("Unbound rqr: "^(string_of_attribute_identifier_list unbound_rqr));
                                                  print_endline ("Unbound lqr: "^(string_of_attribute_identifier_list unbound_lqr));
                                                  print_endline ("Unbound rql: "^(string_of_attribute_identifier_list unbound_rql));
						  begin
						      match (unbound_lql, unbound_rqr, unbound_lqr, unbound_rql) with
							  | ([], [], _, _) -> distribute l r ql qr
                                                          | (_, _, [], []) -> distribute l r qr ql
							  | _ -> simplify m_expr (t@[np])
						  end

                                        | `MapExpression(`MapAggregate (fn, (`Sum(fl, fr) as f), q)) ->
                                              print_rule_description `DistributeSumAgg;
                                              let extract_f old_f new_f =
                                                  match fn with
                                                      | `Sum -> let e = `MapAggregate(fn, new_f, q) in (`Product(old_f, e), [e])
                                                      | `Min | `Max -> (old_f, [])
                                              in
                                              let fl_indep = is_independent fl q in
                                              let fr_indep = is_independent fr q in
                                                  begin
                                                      let new_np_and_rewrite = 
                                                          match (fl_indep, fr_indep) with
                                                              | (true, true) ->
                                                                    let (e,erw) = extract_f f (`METerm (`Int 1)) in Some(e, erw)
                                                              | (true, false) ->
                                                                    let (e,erw) = extract_f fl (`METerm (`Int 1)) in
                                                                    let e2 = `MapAggregate(fn, fr, q) in
                                                                        Some(`Sum(e, e2), erw@[e2])
                                                              | (false, true) ->
                                                                    let (e,erw) = extract_f fr (`METerm (`Int 1)) in
                                                                    let e2 = `MapAggregate(fn, fl, q) in
                                                                        Some(`Sum(e2, e), [e2]@erw)
                                                              | _ ->
                                                                    begin
                                                                        match fn with
                                                                            | `Sum ->
                                                                                  let e = `MapAggregate(`Sum, fl, q) in
                                                                                  let e2 = `MapAggregate(`Sum, fr, q) in
                                                                                      Some(`Sum(e,e2), [e; e2])
                                                                            | _ -> None
                                                                    end
                                                      in
                                                          match new_np_and_rewrite with
                                                              | Some(x, new_rw) -> 
                                                                    let new_np = `MapExpression(x) in
						                    let new_m_expr = splice m_expr np new_np in
						                    let new_rewrites = t@(List.map (fun x -> `MapExpression(x)) new_rw) in
							                simplify new_m_expr new_rewrites
                                                              | None -> simplify m_expr (t@[np])
                                                  end

                                        | `MapExpression(`MapAggregate(fn, (`Product(fl, fr) as f), q)) ->
                                              print_rule_description `DistributeProdAgg;
                                              let extract_sum_prod_f e_expr r_expr l_expr =
                                                  let e = `MapAggregate(`Sum, r_expr, q) in
                                                      Some((if l_expr then `Product(e_expr, e) else `Product(e, e_expr)), [e])
                                              in
                                              let extract_minmax_prod_f e_expr r_expr l_expr =
                                                  let et = `MapAggregate(fn, r_expr, q) in
                                                  let ee =
                                                      let rev_fn = match fn with
                                                          | `Min -> `Max | `Max -> `Min | _ -> raise InvalidExpression
                                                      in
                                                          `MapAggregate(rev_fn, r_expr, q)
                                                  in 
                                                  (* TODO: change this to `MGE *)
                                                  let r = `IfThenElse(`MLT(e_expr),
                                                      (if l_expr then `Product(e_expr, et) else `Product(et, e_expr)),
                                                      (if l_expr then `Product(e_expr, ee) else `Product(ee, e_expr)))
                                                  in
                                                      Some(r, [et; ee])
                                              in
                                              let fl_indep = is_independent fl q in
                                              let fr_indep = is_independent fr q in
                                                  begin
                                                      print_endline ("Matched agg_{a*b}("^
                                                          (string_of_bool fl_indep)^","^(string_of_bool fr_indep)^
                                                          "): "^(string_of_accessor_element np));

                                                      let new_np_and_rewrite =
                                                          match (fn, fl_indep, fr_indep) with
                                                              | (`Sum, true, true) ->
                                                                    let e = `Product(f, `MapAggregate(`Sum, `METerm(`Int 1), q)) in
                                                                        Some(e, [e])
                                                              | (_, true, true) -> Some(f, [f])
                                                              | (`Sum, true, false) -> extract_sum_prod_f fl fr true
                                                              | (`Sum, false, true) -> extract_sum_prod_f fr fl false
                                                              | (_, true, false) -> extract_minmax_prod_f fl fr true
                                                              | (_, false, true) -> extract_minmax_prod_f fr fl false
                                                              | _ -> None
                                                      in
                                                          match new_np_and_rewrite with
                                                              | Some(x, new_rw) ->
                                                                    let new_np = `MapExpression(x) in
						                    let new_m_expr = splice m_expr np new_np in
						                    let new_rewrites =
                                                                        t@(List.map (fun x -> `MapExpression(x)) new_rw)
                                                                    in
							                simplify new_m_expr new_rewrites
                                                              | None -> simplify m_expr (t@[np])
                                                  end

                                        (* Independence rule for map_expressions other than `Sum, `Product *)
					| `MapExpression(`MapAggregate (fn, f, q)) ->
					      if is_independent f q then
                                                  begin
                                                      print_rule_description `DistributeIndepAgg;
						      let new_np =
							  match fn with
							      | `Sum -> `MapExpression(
								    `Product(f, `MapAggregate(fn, `METerm (`Int 1), q)))
							      | `Min | `Max -> `MapExpression(f)
						      in
						      let new_m_expr = splice m_expr np new_np in
						      let new_rewrites = t@[new_np] in
							  simplify new_m_expr new_rewrites
                                                  end
                                              else
                                                  begin
                                                      print_rule_description `NoRule;
						      simplify m_expr (t@[np])
					          end

					| _ ->
                                              print_rule_description `NoRule;
                                              simplify m_expr (t@[np])
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
                  (*
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
				  | `Max -> (f, fb)
			  end
		  else
                  *)
                  (`MapAggregate(fn, fe, qq), fb@qb)
		      
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

        (*
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
        *)

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
                      (*
		      print_endline ("CP2:"^(match fn with | `Sum -> "sum" | `Min -> "min" | `Max -> "max"));
		      print_endline ("CP2a: "^(string_of_map_expression new_f));
		      print_endline ("CP2b: "^(string_of_plan new_q));
                      *)
		      match (fn, new_f, new_q) with
                          (* TODO : check this out *)
			  | (`Sum, `Incr (sx,o,x), `IncrPlan (_,_,y)) -> `Incr (sx, o, `MapAggregate(fn, x, y))
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
                            (* print_endline ("CP1: "^(string_of_plan q)); *)
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

        (*
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
        *)

	| `TrueRelation | `FalseRelation | `DeltaPlan _ | `NewPlan _ | `IncrPlan _ | `IncrDiffPlan _->
              begin
                  print_endline (indented_string_of_plan q 0);
	          raise (RewriteException "Invalid plan for apply_recompute_plan_rules.")
              end


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
			  match (pred, new_cq) with
                              (*TODO check later *)
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

            (*
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
            *)

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
		      
let compile_target m_expr event =
    (* Debugging helpers *)
    let print_incr_and_delta_pass expr frontier_expr delta_expr =
        print_endline ("input: "^(string_of_map_expression expr));
	print_endline ("frontier_expr: "^(string_of_map_expression frontier_expr));
	print_endline ("delta_expr:\n"^(indented_string_of_map_expression delta_expr 0));
	print_endline ("# br: "^(string_of_int (List.length (get_bound_relations delta_expr))))
    in
    let print_simplify_pass compiled_expr simplified_expr =
        print_endline ("cc_expr: "^(string_of_map_expression compiled_expr));
        print_endline ("sc_expr: "^(string_of_map_expression simplified_expr));
        print_endline (String.make 50 '>')
    in
    (* Code body *)
    let compile_aux e = 
	let frontier_expr =
	    compute_new_map_expression (apply_recompute_rules e event) (Some New)
	in
	let delta_expr =
	    simplify_map_expr_constants
		(apply_delta_rules frontier_expr event (Some New))
	in
            print_incr_and_delta_pass e frontier_expr delta_expr;
	    let br = List.map (fun x -> `Plan x) (get_bound_relations delta_expr) in
            let compiled_expr = simplify delta_expr br in
	    let sc_expr = simplify_map_expr_constants compiled_expr in
                print_simplify_pass compiled_expr sc_expr;
                sc_expr
    in
    let (dependent_expr, binding_exprs) = extract_map_expr_bindings m_expr in
    let compiled_binding_exprs =
	List.map
	    (fun b ->
	        match b with
	            | `BindMapExpr(v,e) ->
                          print_endline ("Compiling binding: "^v);
                          `BindMapExpr(v, compile_aux e)
	            | _ -> b)
	    binding_exprs
    in
	(compile_aux dependent_expr, compiled_binding_exprs)



(*
 *
 * Recursive compilation
 *
 *)

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

let rec extract_incremental_query m_expr =
    let rec eiq_find_plan incr_m_expr =
        match incr_m_expr with
            | `METerm _ | `Insert _ | `Delete _ | `Update _ -> []
	    | `Sum (l, r) | `Minus (l, r) | `Product (l, r)
	    | `Max (l, r) | `Min (l, r) 
	    | `IfThenElse (_, l, r) ->
                  (eiq_find_plan l) @ (eiq_find_plan r)
            | `MapAggregate _ -> [incr_m_expr]
            | `Incr _ | `IncrDiff _ | `New _ | `Init _ | `Delta _ -> raise InvalidExpression
    in
    let r_list =  
        match m_expr with
            | `Incr(sid,_,e) | `IncrDiff(sid,_,e) ->
                  (* TODO: create map accessor *)
                  eiq_find_plan e
	    | `Sum (l, r) | `Minus (l, r) | `Product (l, r)
	    | `Max (l, r) | `Min (l, r) 
	    | `IfThenElse (_, l, r) ->
                  (extract_incremental_query l) @ (extract_incremental_query r)
            | `MapAggregate(_,f,q) -> extract_incremental_query f
            | `New (e) -> extract_incremental_query e
	    | `METerm _
            | `Init _ 
            | `Insert _ | `Delete _ | `Update _ -> []
            | `Delta _ -> raise InvalidExpression
    in
        List.map 
            (fun r ->
                let r_uba = get_unbound_attributes_from_map_expression r true in
                let r_with_uba =
                    map_map_expr
                        (fun e -> match e with
                            | `ETerm (`Variable(v)) ->
                                  if List.mem (`Unqualified v) r_uba
                                  then `ETerm(`Attribute(`Unqualified(v))) else e
                            | _ -> e)
                        (fun b -> b) (fun m -> m) (fun p -> p) r
                in
                    print_endline ("r with uba: "^(string_of_map_expression r_with_uba));
                    r_with_uba)
            r_list
            

let extract_incremental_query_from_bindings bindings =
    List.concat (
        List.fold_left
 	    (fun acc x -> 
	        match x with
		    | `BindMapExpr (v, m) -> (extract_incremental_query m)::acc
		    | _ -> acc)
            [] bindings)


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
    (* Debugging helpers *)
    let print_event_path evt evt_path =
        print_endline (String.make 50 '-');
        print_endline ("Compiling for event path:"^
            (List.fold_left
                (fun acc e ->
                    (if (String.length acc) = 0 then "" else acc^", ")^
                        (string_of_delta e))
                "" (evt::evt_path)))
    in
    let print_input m_expr_list = 
        print_endline "cta_aux input:";
        print_endline
            (List.fold_left
                (fun acc e ->
                    (if (String.length acc) = 0 then "input: " else acc^"\ninput: ")^
                        (string_of_map_expression e))
                "" m_expr_list)
    in
    let print_new_map_expressions event event_path compiled_exprs map_exprs =
        print_endline (String.make 50 '-');
        print_endline ("New maps: ("^
            (string_of_int (List.length compiled_exprs))^" compiled results, "^
            (string_of_int (List.length map_exprs))^" map expressions)");
        print_event_path event event_path;
        List.iter (fun x -> print_endline ("map: "^(string_of_map_expression x))) map_exprs
    in
    (* Code body *)
    let event_list = generate_all_events m_expr in
    let rec cta_aux e_list m_expr_list e_path =
        print_input m_expr_list;
	List.concat ( List.map
	    (fun event -> 
		let new_events = set_difference e_list [event] in 
		let result_list = 
		    List.map 
			(fun x -> 
                            print_event_path event e_path;
                            compile_target x event) m_expr_list
		in 
		let map_exprs = 
		    List.concat 
		        (List.map
			    (fun (handler, binding) -> 
				(extract_incremental_query handler) @
                                    (extract_incremental_query_from_bindings binding))
			    result_list)
		in
                print_new_map_expressions event e_path result_list map_exprs;
		let children = 
		    match new_events with
		  	| [] -> []
			| _ -> cta_aux new_events map_exprs (event::e_path)
		in (event, result_list)::children)
            e_list)
    in
    let group_by_event compiled_exprs event_list =
        List.fold_left 
            (fun res event ->
	        let t = List.filter(fun (e,_) -> e = event) compiled_exprs in
	            (event, List.concat(List.map (fun (e,l2) -> l2) t)) :: res)
            [] event_list
    in
    let result = cta_aux event_list [m_expr] [] in
        group_by_event result event_list



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
