open Algebra

(* TODO:
 * -- apply_delta_plan_rules: handle conjunctive/disjunctive map expression
 * comparisons for both selections and joins
 *)


(*
 *
 * Binding helpers
 * 
 *)

(* event -> (attribute_id * expression) list *)
let create_bindings event name fields =
    match event with
        | `Insert(_, vars) | `Delete(_, vars) ->
              List.map2
	          (fun (vid, vtyp) (fid, ftyp) ->
                      (`Unqualified fid, `ETerm(`Variable(vid))) )
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

let rec ab_expr_aux expr pa =
    let print_comparison_debug aid x r =
	print_endline ("Comparing "^
	    (string_of_attribute_identifier aid)^" "^
	    (string_of_attribute_identifier x)^": "^
	    (string_of_bool r))
    in
    match expr with
	| `ETerm (`Attribute(x)) ->
	      let matched_attrs = 
		  List.filter
		      (fun (aid, bvar) ->
			  let r = compare_attributes x aid in
                              print_comparison_debug aid x r;
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
                                    print_endline ("ab_expr_aux: found duplicate substitutions: "^
				        (List.fold_left
					    (fun acc (aid, bvar) ->
					        (if (String.length acc) = 0 then "" else acc^",")^
                                                    (string_of_attribute_identifier aid))
					    "" matched_attrs));
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
    let print_comparison_debug aid x r =
	print_endline ("Comparing "^
	    (string_of_attribute_identifier aid)^" "^
	    (string_of_attribute_identifier x)^": "^
	    (string_of_bool r))
    in
    let recur_meterm mt =
	match ab_map_expr_aux (`METerm mt) pa with
	    | `METerm(x) -> x
	    | _ -> raise InvalidExpression
    in
    match m_expr with
	| `METerm (`Attribute(x)) ->
	      let matched_attrs = 
		  List.filter
		      (fun (aid, bvar) ->
			  let r = compare_attributes x aid in
                              print_comparison_debug aid x r;
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
                                    print_endline ("ab_expr_aux: found duplicate substitutions: "^
				        (List.fold_left
					    (fun acc (aid, bvar) ->
					        (if (String.length acc) = 0 then "" else acc^",")^
                                                    (string_of_attribute_identifier aid))
					    "" matched_attrs));
				    raise DuplicateException
				end
		  end

	| `METerm (`Int _) | `METerm (`Float _) | `METerm (`String _)
	| `METerm (`Long _) | `METerm (`Variable _) ->  m_expr

	| `Sum (l,r) -> `Sum(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Minus (l,r) -> `Minus(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Product (l,r) -> `Product(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Min (l,r) -> `Min(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `Max (l,r) -> `Max(ab_map_expr_aux l pa, ab_map_expr_aux r pa)
	| `IfThenElse (b,l,r) -> `IfThenElse(
	      (match ab_pred_aux (`BTerm (b)) pa with
 		  | `BTerm(x) -> x
		  | _ -> raise InvalidExpression )
	          , ab_map_expr_aux l pa, ab_map_expr_aux r pa)

	| `MapAggregate (fn,f,q) ->
	      let new_pa = remove_proj_attrs pa (get_bound_attributes q)
	      in
		  `MapAggregate(fn, ab_map_expr_aux f new_pa, ab_plan_aux q pa)

	| `Delta (b,e) ->  `Delta (b, ab_map_expr_aux e pa)
	| `New(e) -> `New (ab_map_expr_aux e pa)
	| `MaintainMap(sid, iop, bd, e) -> `MaintainMap (sid, iop, bd, ab_map_expr_aux e pa)
	| `Incr(sid, o, re, bd, e) -> `Incr (sid, o, re, bd, ab_map_expr_aux e pa)
	| `IncrDiff(sid, o, re, bd, e) -> `IncrDiff (sid, o, re, bd, ab_map_expr_aux e pa)

        (* TODO: maps may be multidimensional, but as seen below, we should bind lookups *)
	| `Insert(sid, m, e) -> `Insert (sid, recur_meterm m, ab_map_expr_aux e pa)
	| `Update(sid, o, m, e) -> `Update (sid, o, recur_meterm m, ab_map_expr_aux e pa)
	| `Delete(sid, m) -> `Delete(sid, recur_meterm m)


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
	| `BTerm(`MNEQ(m_expr)) -> `BTerm(`MNEQ(ab_map_expr_aux m_expr pa))
	| `BTerm(`MLT(m_expr)) -> `BTerm(`MLT(ab_map_expr_aux m_expr pa))
	| `BTerm(`MLE(m_expr)) -> `BTerm(`MLE(ab_map_expr_aux m_expr pa))
	| `BTerm(`MGT(m_expr)) -> `BTerm(`MGT(ab_map_expr_aux m_expr pa))
	| `BTerm(`MGE(m_expr)) -> `BTerm(`MGE(ab_map_expr_aux m_expr pa))
	| `And (l,r) -> `And(ab_pred_aux l pa, ab_pred_aux r pa)
	| `Or (l,r) -> `Or(ab_pred_aux l pa, ab_pred_aux r pa)
	| `Not (e) -> `Not(ab_pred_aux e pa)

and ab_plan_aux q pa =
    match q with
	| `TrueRelation | `FalseRelation | `Relation _ | `Domain _ -> q
	| `Select(pred, cq) ->
	      let new_pa = remove_proj_attrs pa (get_bound_attributes cq) in
		  `Select(ab_pred_aux pred new_pa, ab_plan_aux cq pa)

	| `Project (attrs, cq) -> ab_plan_aux cq pa
	| `Union ch -> `Union (List.map (fun c -> ab_plan_aux c pa) ch)
	| `Cross (l,r) -> `Cross (ab_plan_aux l pa, ab_plan_aux r pa)
	| `DeltaPlan (b,e) -> `DeltaPlan (b, ab_plan_aux e pa)
	| `NewPlan(e) -> `NewPlan (ab_plan_aux e pa)
	| `IncrPlan(sid, p, d, bd, e) -> `IncrPlan (sid, p, d, bd, ab_plan_aux e pa)
	| `IncrDiffPlan(sid, p, d, bd, e) -> `IncrDiffPlan (sid, p, d, bd, ab_plan_aux e pa)

let add_map_expression_bindings m_expr proj_attrs =
    ab_map_expr_aux m_expr proj_attrs 

let add_plan_bindings plan proj_attrs =
    ab_plan_aux plan proj_attrs

let add_predicate_bindings pred proj_attrs =
    ab_pred_aux pred proj_attrs



(*************************
 * Compilation passes
 *************************)

(*
 * Constant lifting and variable binding (including monotonic variables).
 * -- run as first pass during compilation
 *)

(* pull up constant bindings *)
let bind_expr_pair (le, lb) (re, rb) =
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
		  let (l_sym, r_sym) = (gen_binding_sym (`Expression le), gen_binding_sym (`Expression re)) in
		      (true,
		      `ETerm(`Variable(l_sym)), lb@[`BindExpr(l_sym, le)],
		      `ETerm(`Variable(r_sym)), rb@[`BindExpr(r_sym, re)])
			  
	    | (true, true, true, false) ->
		  let lv = match le with
		      | `ETerm(`Variable x) -> `Variable x | _ -> raise InvalidExpression
		  in
		  let l_def = get_expr_definition lv lb in
		  let r_sym = gen_binding_sym (`Expression re) in
		      (true, l_def, (remove_expr_binding lv lb),
		      `ETerm(`Variable(r_sym)), rb@[`BindExpr(r_sym, re)])
			  
	    | (true, false, true, true) ->
		  let rv = match re with
		      | `ETerm(`Variable x) -> `Variable x | _ -> raise InvalidExpression
		  in
		  let l_sym = gen_binding_sym (`Expression le) in
		  let r_def = get_expr_definition rv rb in
		      (true, `ETerm(`Variable(l_sym)), lb@[`BindExpr(l_sym, le)],
		      r_def, (remove_expr_binding rv rb))
			  
	    | (true, false, _, _) ->
		  let l_sym = gen_binding_sym (`Expression le) in
		      (false, `ETerm(`Variable(l_sym)), lb@[`BindExpr(l_sym, le)], re, rb)
			  
	    | (_, _, true, false) ->
		  let r_sym = gen_binding_sym (`Expression re) in
		      (false, le, lb, `ETerm(`Variable(r_sym)), rb@[`BindExpr(r_sym, re)])
			  
	    | _ -> (false, le, lb, re, rb)

(* expression -> expression * bindings list *)
let rec extract_expr_bindings expr =
    match expr with
	| `ETerm (t) -> (expr, [])
	| `UnaryMinus (e) -> extract_expr_bindings e
	| `Sum (l, r) | `Product (l, r) | `Minus (l, r) | `Divide (l, r) ->
	      let (le, lb) = extract_expr_bindings l in
	      let (re, rb) = extract_expr_bindings r in
	      let (bound, nl, nlb, nr, nrb) = bind_expr_pair (le, lb) (re, rb) in
              let gen_binary_binding fn =
                  let new_be = fn nl nr in
		  let new_var = gen_binding_sym (`Expression new_be) in
		      (`ETerm(`Variable(new_var)),
		      nlb@nrb@[`BindExpr(new_var, new_be)])
              in
		  begin
		      match (bound, expr) with
			  | (true, `Sum (_,_)) -> gen_binary_binding (fun x y -> `Sum (x,y))
			  | (true, `Product (_,_)) -> gen_binary_binding (fun x y -> `Product (x,y))
			  | (true, `Minus (_,_)) -> gen_binary_binding (fun x y -> `Minus (x,y))
			  | (true, `Divide (_,_)) -> gen_binary_binding (fun x y -> `Divide (x,y))

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
let rec extract_bool_expr_bindings b_expr =
    let ee_bt le re lb rb =
        let b = lb@rb in
        let e =
            match b_expr with
	        | `BTerm(`LT _) -> `LT(le, re)
	        | `BTerm(`LE _) -> `LE(le, re)
	        | `BTerm(`GT _) -> `GT(le, re)
	        | `BTerm(`GE _) -> `GE(le, re)
	        | `BTerm(`EQ _) -> `EQ(le, re)
	        | `BTerm(`NE _) -> `NE(le, re)
                | _ -> raise InvalidExpression
        in
            (`BTerm(e), b)
    in
    let ene_bt mee meb =
        let e =
            match b_expr with
		| `BTerm(`MEQ _) -> `MEQ(mee)
		| `BTerm(`MNEQ _) -> `MNEQ(mee)
		| `BTerm(`MLT _) -> `MLT(mee)
		| `BTerm(`MLE _) -> `MLE(mee)
		| `BTerm(`MGT _) -> `MGT(mee)
		| `BTerm(`MGE _) -> `MGE(mee)
		| _ -> raise InvalidExpression
        in
            (`BTerm(e), meb)
    in
    match b_expr with
	| `BTerm(`True) | `BTerm(`False) -> (b_expr, [])

	| `BTerm(`LT (l, r)) | `BTerm(`LE (l, r))
	| `BTerm(`GT (l, r)) | `BTerm(`GE (l, r))
	| `BTerm(`EQ (l, r)) | `BTerm(`NE (l, r)) ->
	      let (le, lb) = extract_expr_bindings l in
	      let (re, rb) = extract_expr_bindings r in
                  ee_bt le re lb rb

	| `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
	| `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
	| `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
              -> 
	      let (mee, meb) = extract_map_expr_bindings m_expr in
                  ene_bt mee meb
		      
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
and extract_map_expr_bindings m_expr =
    let extract_binary l r fn =
	let (le, lb) = extract_map_expr_bindings l in
	let (re, rb) = extract_map_expr_bindings r in
            (fn le re, lb@rb)
    in
    match m_expr with
	| `METerm x -> (m_expr, [])

	| `Sum(l,r) -> extract_binary l r (fun x y -> `Sum(x,y))
	| `Minus(l,r) -> extract_binary l r (fun x y -> `Minus(x,y))
	| `Product(l,r) -> extract_binary l r (fun x y -> `Product(x,y))
	| `Min (l,r) -> extract_binary l r (fun x y -> `Min(x,y))
	| `Max (l,r) -> extract_binary l r (fun x y -> `Max(x,y))

	| `MapAggregate(fn, f,q) ->
	      let (fe, fb) = extract_map_expr_bindings f in
	      let (qq, qb) = extract_plan_bindings q in
                  (`MapAggregate(fn, fe, qq), fb@qb)

        | `IfThenElse(bt, l, r) ->
              let (bte, btb) =
                  let f x y =
                      let (mee, meb) = extract_map_expr_bindings y in
                      let rbt =
                          match x with
                              | `MEQ _ -> `MEQ(mee) | `MNEQ _ -> `MNEQ(mee)
                              | `MLT _ -> `MLT(mee) | `MLE _ -> `MLE(mee)
                              | `MGT _ -> `MGT(mee) | `MGE _ -> `MGE(mee)
                              | _ -> raise InvalidExpression
                      in
                          (rbt, meb)
                  in
                  match bt with
                      | `MEQ(me) | `MNEQ(me)
                      | `MLT(me) | `MLE(me)
                      | `MGT(me) | `MGE(me)
                            -> f bt me                               
                      | _ -> (bt, [])
              in
              let (le, lb) = extract_map_expr_bindings l in
              let (re, rb) = extract_map_expr_bindings r in
                  (`IfThenElse(bte, le, re), (btb@lb@rb))


        | `Delta _ | `New _
        | `Incr _ | `IncrDiff _ | `MaintainMap _
        | `Insert _ | `Update _ | `Delete _
              ->
              print_endline ("Invalid map expression for extract_map_expr_bindings:\n"^
                  (indented_string_of_map_expression m_expr 0));
	      raise (RewriteException "Invalid map expression for extract_map_expr_bindings")

(* plan -> plan * bindings list *)
and extract_plan_bindings q =
    let extract_nested_map_expression mbterm mon uba cqq predb cqb =
        let gen_nested_binding agg_fn bterm_fn =
            let new_bme = 
                `MapAggregate(agg_fn, `METerm(`Attribute(uba)), `Select(mbterm, cqq))
            in
            let new_var = gen_binding_sym (`MapExpression new_bme) in
            let new_cmp_pair = (`ETerm(`Attribute(uba)), `ETerm(`Variable(new_var))) in
            let nbt = bterm_fn new_cmp_pair in
                (`Select(nbt,cqq), predb@cqb@[`BindMapExpr(new_var, new_bme)])
        in
            print_endline ("Extracting nested map expr from: "^(string_of_bool_expression mbterm)^
                " inc mon: "^(string_of_bool (mon = Inc)));
            match (mon, mbterm) with
                | (Inc, `BTerm(`MEQ _)) | (Inc, `BTerm(`MNEQ _))
                | (Dec, `BTerm(`MEQ _)) | (Dec, `BTerm(`MNEQ _)) ->
                      raise (RewriteException "Unable to handle nested equality/inequality map expressions.")

                | (Inc, `BTerm(`MLT _)) | (Inc, `BTerm(`MLE _)) ->
                      gen_nested_binding `Max (fun cp -> `BTerm(`LE(cp)))

                | (Inc, `BTerm(`MGT _)) | (Inc, `BTerm(`MGE _)) ->
                      gen_nested_binding `Min (fun cp -> `BTerm(`GE(cp)))

                | (Dec, `BTerm(`MLT _)) | (Dec, `BTerm(`MLE _)) ->
                      gen_nested_binding `Min (fun cp -> `BTerm(`GE(cp)))

                | (Dec, `BTerm(`MGT _)) | (Dec, `BTerm(`MGE _)) ->
                      gen_nested_binding `Max (fun cp -> `BTerm(`LE(cp)))

                | (_, _) -> (`Select(mbterm, cqq), predb@cqb)
    in
    match q with
	| `Relation _ -> (q, [])

	| `Select (pred, cq) ->
	      let (prede, predb) = extract_bool_expr_bindings pred in
	      let (cqq, cqb) = extract_plan_bindings cq in
	      let unbound_attrs = (get_unbound_attributes_from_predicate prede false) in
		  if (List.length unbound_attrs) = 1 then
		      let uba = List.hd unbound_attrs in
		      let predem = get_monotonicity prede uba in

			  print_endline ("Checking monotonicity for "^
			      (string_of_attribute_identifier uba)^":" ^
                              (string_of_bool (predem = Inc || predem = Dec)));

                          let (new_q, new_b) =
                              match prede with
                                  | `BTerm(`MEQ _) | `BTerm(`MNEQ _)
                                  | `BTerm(`MLT _) | `BTerm(`MLE _)
                                  | `BTerm(`MGT _) | `BTerm(`MGE _) ->
                                        extract_nested_map_expression prede predem uba cqq predb cqb
                                  | _ -> (`Select(prede, cqq), predb@cqb)
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

        (* Binding extraction should occur before incrementality analysis,
           compilation and domain management*)
        | `TrueRelation | `FalseRelation
        | `Domain _
	| `DeltaPlan _ | `NewPlan _
        | `IncrPlan _ | `IncrDiffPlan _ ->
              print_endline ("extract_plan_binding: applied to\n"^
                  (indented_string_of_plan q 0));
              raise InvalidExpression


(*
 * Constant simplification
 * -- first invoked during incrementality analysis to simplify
 *    delta f and check if zero
 * -- invoked after apply_delta_rules and simplify, thus must deal with all
 *    map expression terms
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
	| `BTerm(`MNEQ(_)) -> (<>)
	| `BTerm(`MLT(_)) -> (<)
	| `BTerm(`MLE(_)) -> (<=)
	| `BTerm(`MGT(_)) -> (>)
	| `BTerm(`MGE(_)) -> (>=)
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
	      
	| `BTerm(`EQ(l,r)) | `BTerm(`NE(l,r))
        | `BTerm(`LT(l,r)) | `BTerm(`LE(l,r))
	| `BTerm(`GT(l,r)) | `BTerm(`GE(l,r))
              ->
              let subs_bterm l r =
		  `BTerm(
                      match b_expr with
		          | `BTerm(`LT _) -> `LT(l,r)
		          | `BTerm(`LE _) -> `LE(l,r)
		          | `BTerm(`GT _) -> `GT(l,r)
		          | `BTerm(`GE _) -> `GE(l,r)
		          | `BTerm(`EQ _) -> `EQ(l,r)
		          | `BTerm(`NE _) -> `NE(l,r)
		          | _ -> raise InvalidExpression)
              in
	      let le = simplify_expr_constants l in
	      let re = simplify_expr_constants r in
		  if is_constant_expr(le) && is_constant_expr(re) then
		      let cmp_fun = get_comparison_function b_expr in
		      let vareq = match b_expr with | `BTerm(`EQ(_,_)) -> true | _ -> false in
		      let varneq = match b_expr with | `BTerm(`NE(_,_)) -> true | _ -> false in 
			  match (simplify_comparison le re cmp_fun vareq varneq) with
			      | Some(x) -> `BTerm(x)
			      | None -> subs_bterm le re
		  else
                      subs_bterm le re

	| `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
	| `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
	| `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr)) ->
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
					| `BTerm(`MNEQ(_)) -> `BTerm(`MNEQ(sm))
					| `BTerm(`MLT(_)) -> `BTerm(`MLT(sm))
					| `BTerm(`MLE(_)) -> `BTerm(`MLE(sm))
					| `BTerm(`MGT(_)) -> `BTerm(`MGT(sm))
					| `BTerm(`MGE(_)) -> `BTerm(`MGE(sm))
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
	| `MaintainMap (sid, iop, bd, e) -> `MaintainMap(sid, iop, bd, simplify_map_expr_constants e)
	| `Incr(sid, o, re, bd, e) -> `Incr(sid, o, re, bd, simplify_map_expr_constants e)
        | `IncrDiff(sid, o, re, bd, e) -> `IncrDiff(sid, o, re, bd, simplify_map_expr_constants e)

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
        | `TrueRelation -> `TrueRelation
        | `FalseRelation -> `FalseRelation
        | `Relation _ as r -> r
        | `Domain _ as r -> r
              
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

        | `DeltaPlan(b, cq) ->
              let cqq = simplify_plan_constants cq in
                  if cq = `FalseRelation then `FalseRelation else `DeltaPlan(b, cqq)

        | `NewPlan (cq) ->
              let cqq = simplify_plan_constants cq in
                  if cq = `FalseRelation then `FalseRelation else `NewPlan(cqq)

        | `IncrPlan (sid, op, d, bd, cq) ->
              let cqq = simplify_plan_constants cq in
                  if cq = `FalseRelation then `FalseRelation else `IncrPlan(sid,op,d,bd,cqq)

        | `IncrDiffPlan (sid, op, d, bd, cq) ->
              let cqq = simplify_plan_constants cq in
                  if cq = `FalseRelation then `FalseRelation else `IncrDiffPlan(sid,op,d,bd,cqq)


(*
 * Delta simplification
 * -- delta pushdown used during incrementality analysis to test delta f = 0
 *)

let rec push_delta m_expr : map_expression =
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

	| `Delta (ev, `MapAggregate (`Min as fn, f, `IncrPlan(sq, pop, d, bd, q)))
	| `Delta (ev, `MapAggregate (`Max as fn, f, `IncrPlan(sq, pop, d, bd, q)))
            ->
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
                      let delta_plan = push_delta_plan(`DeltaPlan(ev, q)) in
                          begin
                              (* Validate delta/op combination in addition to rewrite to IncrDiffPlan *)
                              match (ev, pop) with
                                  | (`Insert _, `Union) ->
                                        `MapAggregate(fn, f, `IncrDiffPlan (sq, `Union, d, bd, delta_plan))
                                  | (`Delete _, `Diff) ->
                                        `MapAggregate(fn, f, `IncrDiffPlan (sq, `Diff, d, bd, delta_plan))
                                  | (_,_) ->
                                        print_endline ("Invalid delta/op combination: "^
                                            (string_of_delta ev)^", "^(string_of_poplus pop));
                                        raise (RewriteException "Invalid delta/op combination.");
                          end
	| _ -> m_expr

and push_delta_plan mep =
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

	| _ ->
	      print_endline ("push_delta_plan: invalid delta:\n"^
                  (indented_string_of_plan mep 0));
	      raise (RewriteException "Invalid Delta")


(*
 * Incrementality analysis
 * -- invoked after extracting bindings, and before applying delta rules
 *)

let rec analyse_map_expr_incrementality m_expr delta =
    (* Duplicate eliminating list concatentation *)
    let merge_bindings lb rb =
        List.fold_left
            (fun acc x -> if List.mem x acc then acc else acc@[x]) lb rb
    in
    let analyse_binary l r fn =
	let new_l = analyse_map_expr_incrementality l delta in
	let new_r = analyse_map_expr_incrementality r delta in
	    begin
		match (new_l, new_r) with
		    | (`Incr (sx,ox,rex,bdx,x), `Incr (sy,oy,rey,bdy,y)) -> 
			  if ox = oy then
                              let new_me = fn x y in
                              let new_bd = merge_bindings bdx bdy in 
                                  `Incr(merge_state_sym (`MapExpression (x,bdx)) (`MapExpression (y,bdy))
                                          (`MapExpression (new_me, new_bd)) sx,
                                      ox, fn rex rey, new_bd, new_me)
			  else `New(fn new_l new_r)

		    | (`New x, `New y) -> `New(fn x y)
		    | (`Incr _, `New y) -> `New(fn new_l y)
		    | (`New x, `Incr _) -> `New(fn x new_r)
		    | _ -> raise InvalidExpression
	    end
    in
    let analyse_cmp_binary l r fn =
	let new_l = analyse_map_expr_incrementality l delta in
	let new_r = analyse_map_expr_incrementality r delta in
	    begin
		match (new_l, new_r) with
		    | (`Incr _, `Incr _) -> fn new_l new_r
		    | (`New x, `New y) -> `New(fn x y)
		    | (`Incr _, `New y) -> `New(fn new_l y)
		    | (`New x, `Incr _) -> `New(fn x new_r)
		    | _ -> raise InvalidExpression
	    end
    in
    match m_expr with
	| `METerm x ->
              begin
                  let oplus = match delta with `Insert _ -> `Plus | `Delete _ -> `Minus in
                  let new_bd =
                      match x with
                          | `Attribute(a) -> [(a, None)]
                          | _ -> []
                  in
                      `Incr(gen_state_sym (`MapExpression (m_expr, new_bd)), oplus, m_expr, new_bd, m_expr)
              end

	| `Sum (l,r) -> analyse_binary l r (fun x y -> `Sum(x, y))
	| `Minus (l,r) -> analyse_binary l r (fun x y -> `Minus(x, y))
	| `Product (l,r) -> analyse_binary l r (fun x y -> `Product(x, y))

	| `Min (l,r) -> analyse_cmp_binary l r (fun x y -> `Min(x, y))
        | `Max (l,r) -> analyse_cmp_binary l r (fun x y -> `Max(x, y))

        (* TODO: handle nested aggregates
         * -- sum aggregates with nested min/max in f
         * -- min/max aggregates with non-zero delta f
         *)
	| `MapAggregate (fn, f, q) ->
              let print_attrs_used me_attrs_used = 
                  print_endline
                      (List.fold_left
                          (fun acc a -> (if (String.length acc = 0) then "" else acc^", ")^
                              (string_of_attribute_identifier a))
                          "" me_attrs_used)
              in
              let me_attrs_used = get_attributes_used_in_map_expression m_expr in

              let merge_query_bindings fbd qbd q =
                  let unbound_fbd =
                      resolve_unbound_attributes
                          (attributes_of_bindings fbd) (get_bound_attributes q)
                  in
                      merge_bindings
                          (List.filter (fun (a,_) -> List.mem a unbound_fbd) fbd) qbd
              in

	      let new_f = analyse_map_expr_incrementality f delta in
	      let new_q = analyse_plan_incrementality q delta me_attrs_used in
		  begin
                      print_endline ("attrs used in:\n"^(indented_string_of_map_expression m_expr 0));
                      print_attrs_used me_attrs_used;

		      match (fn, new_f, new_q) with
			  | (`Sum, `Incr (sx,ox,rx,bdx,x), `IncrPlan (sy,oy,dy,bdy,y)) ->

                                (* Note: we assume there is no aggregate in y, since we can't pull
                                 * IncrPlan above a Select(MEQ|MLT) *)

                                (* Validate map expr and plan ops *)
                                validate_incr_ops ox oy;
                                
                                if (find_cmp_aggregate x) then
                                    `New (`MapAggregate(fn, new_f, new_q))
                                else
                                    let new_bd = merge_query_bindings bdx bdy y in
                                    let new_me = `MapAggregate(fn, x, y) in
                                        `Incr (merge_state_sym (`MapExpression (x,bdx)) (`Plan (y,dy))
                                                (`MapExpression (new_me,new_bd)) sx,
                                            ox, m_expr, new_bd, new_me)

			  | (`Min, `Incr (sx,ox,rx,bdx,x), `IncrPlan (sy,oy,dy,bdy,y))
			  | (`Max, `Incr (sx,ox,rx,bdx,x), `IncrPlan (sy,oy,dy,bdy,y))
                              -> 
                                validate_incr_ops ox oy;

                                let new_bdx = merge_query_bindings bdx bdy y in
                                let fn_q = `IncrDiffPlan(sy,oy,dy,bdy,y) in
                                let test_delta_f =
                                    simplify_map_expr_constants (push_delta (`Delta (delta, x)))
                                in
                                    if test_delta_f = (`METerm (`Int 0)) then
                                        let oplus = match delta with
                                            | `Insert _ ->
                                                  (match fn with
                                                      | `Min -> `Min | `Max -> `Max
                                                      | _ -> raise InvalidExpression)
                                            | `Delete _ ->
                                                  (match fn with
                                                      | `Min -> `Decrmin(x, sy) | `Max -> `Decrmax(x, sy)
                                                      | _ -> raise InvalidExpression)
                                        in
                                        let new_me = `MapAggregate(fn, x, fn_q) in
                                            `Incr(merge_state_sym (`MapExpression (x,bdx)) (`Plan (y,dy))
                                                    (`MapExpression (new_me, new_bdx)) sx, 
                                                oplus, m_expr, new_bdx, new_me)
                                    else
                                        `New (`MapAggregate(fn, new_f, new_q))

			  | (_, `New x, `NewPlan y) -> `New (`MapAggregate(fn, x, y))
			  | (_, `Incr _, `NewPlan y) -> `New (`MapAggregate(fn, new_f, y))
			  | (_, `New x, `IncrPlan _) -> `New (`MapAggregate(fn, x, new_q))
			  | _ -> raise InvalidExpression
		  end

        (* TODO: should we support `IfThenElse in user input (i.e. before any compilation)? *)
        | `IfThenElse _
	| `Delta _  | `New _ | `MaintainMap _
        | `Incr _ | `IncrDiff _
        | `Insert _ | `Update _ | `Delete _ ->
	      raise (RewriteException 
		  "Invalid map expression argument for analyse_map_expr_incrementality.")

and analyse_plan_incrementality q delta q_attrs_used =
    (* Duplicate eliminating list concatentation *)
    let merge_bindings lb rb =
        List.fold_left
            (fun acc x -> if List.mem x acc then acc else acc@[x]) lb rb
    in
    let get_domain plan =
        let intersect_attributes attrs existing_attrs =
            List.filter (fun a -> List.exists (compare_attributes a) existing_attrs) attrs
        in
            intersect_attributes q_attrs_used (get_flat_schema plan)
    in
    (*
    let print_domain plan = 
        let domain = get_domain plan in
            print_endline ("recompute plan\n:"^(indented_string_of_plan q 0));
            print_endline ("domain of plan:\n"^(indented_string_of_plan plan 0));
            print_endline
                (List.fold_left
                    (fun acc a -> (if (String.length acc = 0) then "" else acc^", ")^
                        (string_of_attribute_identifier a))
                    "" domain)
    in
    *)
    match q with
	| `Relation _ ->
              let incr_op = match delta with `Insert _ -> `Union | `Delete _ -> `Diff in
              let new_dom = get_domain q in
                  `IncrPlan (gen_state_sym (`Plan (q, new_dom)), incr_op, new_dom, [], q)

	| `Select (pred, cq) ->
              let subs_mbterm mbterm me =
                  match mbterm with
		      | `BTerm(`MEQ(_)) -> `BTerm(`MEQ(me))
		      | `BTerm(`MNEQ(_)) -> `BTerm(`MNEQ(me))
		      | `BTerm(`MLT(_)) -> `BTerm(`MLT(me))
		      | `BTerm(`MLE(_)) -> `BTerm(`MLE(me))
		      | `BTerm(`MGT(_)) -> `BTerm(`MGT(me))
		      | `BTerm(`MGE(_)) -> `BTerm(`MGE(me))
                      | _ -> raise InvalidExpression
              in
	      begin
		  match pred with 
		      | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
		      | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
		      | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr)) ->
			    let mer = analyse_map_expr_incrementality m_expr delta in
			    let cqr = analyse_plan_incrementality cq delta q_attrs_used in
			    let mer_new =
				match mer with
				    | `Incr x -> subs_mbterm pred mer
				    | `New x -> subs_mbterm pred x
				    | _ -> raise InvalidExpression
			    in
			    let cqr_new =
				match cqr with
				    | `IncrPlan (_,_,_,_,x) -> cqr
                                    | `NewPlan x -> x
				    | _ -> raise InvalidExpression
			    in
				`NewPlan(`Select(mer_new, cqr_new))

		      | _ ->
			    let cqr = analyse_plan_incrementality cq delta q_attrs_used in
				begin
				    match cqr with
					| `NewPlan cqq -> `NewPlan(`Select (pred, cqq))
					| `IncrPlan (sid, op, d, bd, cqq) ->
                                              let pred_uba =
                                                  resolve_unbound_attributes
                                                      (get_unbound_attributes_from_predicate pred false)
                                                      (get_bound_attributes cqq)
                                              in
                                              let new_bd = merge_bindings
                                                  (List.map (fun x -> (x, None)) pred_uba) bd
                                              in
                                              let new_p = `Select (pred, cqq) in
                                              let new_dom = get_domain cqq in
                                                  `IncrPlan(update_state_sym (`Plan (cqq, d)) (`Plan (new_p, new_dom)) sid, 
                                                      op, new_dom, new_bd, new_p)
					| _ -> raise InvalidExpression
				end
	      end

	| `Project (attrs, cq) ->
	      let cqr = analyse_plan_incrementality cq delta q_attrs_used in
		  begin
		      match cqr with
			  | `NewPlan x -> `NewPlan (`Project(attrs, x)) 
			  | `IncrPlan _ -> `NewPlan (`Project(attrs, cqr))
			  | _ -> raise InvalidExpression
		  end

	| `Union children ->
              let lift_child_newplans l =
                  List.map
                      (fun x -> match x with
                          | `NewPlan c -> c | `IncrPlan _ -> x
                          | _ -> raise InvalidExpression)
                      l
              in
              let lift_child_incrplans l =
                  List.map
		      (fun x -> match x with
			  | `IncrPlan (_,_,_,bd,c) -> (bd, c)
                          | _ -> raise InvalidExpression)
                      l
              in
	      let rc = List.map (fun x ->
                  (analyse_plan_incrementality x delta q_attrs_used)) children
              in
	      let (sid, op, all_incr) =
                  let (sid, op) =
                      match (List.hd rc) with
                          | `IncrPlan(s,o,_,_,_) -> (s,o)
                          | _ -> raise InvalidExpression
                  in
                      (sid, op,
                      List.for_all
		          (fun x -> match x with
                              | `IncrPlan (s,o,_,_,_) when o = op -> true | _ -> false)
                          rc)
	      in
		  begin
		      if all_incr then
                          let (bds, cqs) = List.split (lift_child_incrplans rc) in
                          let new_bd = List.fold_left merge_bindings [] bds in
                          let new_p = `Union cqs in
                              List.iter 
                                  (fun x -> match x with 
                                      |`IncrPlan(_,_,d,_,p) -> remove_state_sym (`Plan (p, d))
                                      | _ -> raise InvalidExpression)
                              rc;
                              let new_dom = get_domain q in
			          `IncrPlan(gen_state_sym (`Plan (new_p, new_dom)),
                                      op, new_dom, new_bd, new_p)
		      else 
			  `NewPlan(`Union (lift_child_newplans rc))
		  end

	| `Cross (l, r) ->
	      let lr = analyse_plan_incrementality l delta q_attrs_used in
	      let rr = analyse_plan_incrementality r delta q_attrs_used in
		  begin
		      match (lr, rr) with
			  | (`IncrPlan (sx,ox,dx,bdx,x), `IncrPlan (sy,oy,dy,bdy,y)) -> 
                                begin
				    if ox = oy then
                                        let new_p = `Cross(x,y) in
                                        let new_dom = get_domain q in
                                            `IncrPlan(
                                                merge_state_sym (`Plan (x,dx))
                                                    (`Plan (y,dy)) (`Plan (new_p, new_dom)) sx, 
                                                ox, new_dom, merge_bindings bdx bdy, new_p)
				    else
                                        `NewPlan(`Cross(lr, rr))
                                end

			  | (`NewPlan x, `NewPlan y) -> `NewPlan(`Cross(x,y))
			  | (`IncrPlan _, `NewPlan y) -> `NewPlan(`Cross(lr, y))
			  | (`NewPlan x, `IncrPlan _) -> `NewPlan(`Cross(x, rr)) 
			  | _ -> raise InvalidExpression
		  end

	| `TrueRelation | `FalseRelation
        | `Domain _
        | `DeltaPlan _ | `NewPlan _
        | `IncrPlan _ | `IncrDiffPlan _->
              begin
                  print_endline (indented_string_of_plan q 0);
	          raise (RewriteException "Invalid plan for analyse_plan_incrementality.")
              end

(*
 * Expression initialization
 * -- invoked after incrementality analysis
 *)

let rec compute_initial_value m_expr =
    let binary l r fn =
        fn (compute_initial_value l) (compute_initial_value r)
    in
    match m_expr with
	| `METerm x -> `METerm x
	      
	| `Sum(l,r) -> binary l r (fun x y -> `Sum(x,y))
	| `Minus(l,r) -> binary l r (fun x y -> `Minus(x,y))
	| `Product(l,r) -> binary l r (fun x y -> `Product(x,y))
	| `Min(l,r) -> binary l r (fun x y -> `Min(x,y))
        | `Max(l,r) -> binary l r (fun x y -> `Max(x,y))

	| `MapAggregate (fn, f, q) ->
              begin
                  match fn with
                      | `Sum -> `METerm (`Int 0)
                      | `Min -> `METerm (`Int max_int)
                      | `Max -> `METerm (`Int min_int)
              end
	      
	| _ -> raise (RewriteException "Invalid intial value expression.") 

let rec initialize_map_expression m_expr rcs =
    let intialize_new x =
        let binary l r fn =
            fn (initialize_map_expression l (Some New))
		(initialize_map_expression r (Some New))
        in
	match x with
	    | `METerm y -> `METerm y 
		  
	    | `Sum(l,r) -> binary l r (fun x y -> `Sum(x,y))
	    | `Minus(l,r) -> binary l r (fun x y -> `Minus(x,y))
	    | `Product(l,r) -> binary l r (fun x y -> `Product(x,y))
	    | `Min(l,r) -> binary l r (fun x y -> `Min(x,y))
            | `Max(l,r) -> binary l r (fun x y -> `Max(x,y))

	    | `MapAggregate(fn,f,q) ->
		  `MapAggregate(fn, initialize_map_expression f (Some New),
		      initialize_plan q (Some New))

	    | _ -> raise InvalidExpression
    in
    let intialize_incr x state_sym op recompute bd =
	match x with
	    | `METerm e -> compute_initial_value x
	    | `Sum(l,r) ->
		  `Sum(
		      `Sum(compute_initial_value l, compute_initial_value r),
		      `Incr(state_sym, op, recompute, bd, x))

	    | `Minus(l,r) ->
		  `Sum(
		      `Minus(compute_initial_value l, compute_initial_value r),
		      `Incr(state_sym, op, recompute, bd, x))

	    | `Product(l,r) ->
		  let il = compute_initial_value l in
		  let ir = compute_initial_value r in
		      begin
			  match (il, ir) with
			      | (`METerm (`Int 0), `METerm (`Int 0)) -> `Incr(state_sym, op, recompute, bd, x)
			      | (`METerm (`Int 0), a) -> `Incr(state_sym, op, recompute, bd, x)
			      | (a, `METerm (`Int 0)) -> `Incr(state_sym, op, recompute, bd, x)
			      | (a, b) -> `Product(`Product(a, b), `Incr(state_sym, op, recompute, bd, x)) 
		      end

            (* TODO: think about this *)
	    | `Min (l,r) -> `Min(l,r)
            | `Max (l,r) -> `Max(l,r)
		  
	    | `MapAggregate (`Sum, f, q) ->
		  `Sum(compute_initial_value x, `Incr(state_sym, op, recompute, bd, x))

            (* TODO: think about this *)
	    | `MapAggregate (`Min, f, q) -> `Incr(state_sym, op, recompute, bd, x)
	    | `MapAggregate (`Max, f, q) -> `Incr(state_sym, op, recompute, bd, x)

	    | _ -> raise InvalidExpression
    in
	match m_expr with
            (* `MaintainMap should not exist until after map domain maintenance pass *)
            | `MaintainMap _ -> raise InvalidExpression
	    | `New(e) -> `New(intialize_new e)
	    | `Incr(sid, o, re, bd, e) -> intialize_incr e sid o re bd
	    | _ -> 
		  begin
		      match rcs with
			  | Some New -> intialize_new m_expr
			  | Some Incr | _ ->
				raise (RewriteException "Invalid nested recomputed expression.")
		  end

and initialize_plan q rcs =
    let initialize_new x =
        let init_nme mbterm me =
            let init = initialize_map_expression me (Some New) in
            let r =
                match mbterm with
                    | `BTerm(`MEQ _) -> `MEQ(init)
                    | `BTerm(`MNEQ _) -> `MNEQ(init)
                    | `BTerm(`MLT _) -> `MLT(init)
                    | `BTerm(`MLE _) -> `MLE(init)
                    | `BTerm(`MGT _) -> `MGT(init)
                    | `BTerm(`MGE _) -> `MGE(init)
                    | _ -> raise InvalidExpression
            in
                `BTerm(r)
        in
	match x with
	    | `Relation (n,f) -> x
	    | `Select (pred, cq) ->
		  let new_pred =
		      match pred with
			  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
			  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                -> init_nme pred m_expr
			  | _ -> pred
		  in
		  let new_cq = initialize_plan cq (Some New) in
                      `Select(new_pred, new_cq)
			  
	    | `Project (a, cq) -> `Project(a, initialize_plan cq (Some New))
	    | `Union ch -> `Union (List.map (fun c -> initialize_plan c (Some New)) ch)
	    | `Cross (l,r) ->
		  `Cross(initialize_plan l (Some New), initialize_plan r (Some New))

	    | _ -> raise InvalidExpression
    in
	match q with
	    | `NewPlan(cq) -> initialize_new cq
	    | `IncrPlan _ -> q
	    | _ ->
		  begin
		      match rcs with
			  | Some New -> initialize_new q
			  | Some Incr | None ->
				raise (RewriteException "Invalid nested recomputed expression.")
		  end


(*
 * Delta rule application
 * -- invoked after binding extraction, incrementality analysis and increment
 *    initialization
 *)

(* rcs: recompute mode *)
let rec apply_delta_rules m_expr event rcs =
    let print_debug e =
        print_endline ("apply_delta_rules: "^
            "invalid recomputation expr:\n"^
            (indented_string_of_map_expression e 0))
    in
    let adr_new x = 
        let adr_binary l r fn =
            fn (apply_delta_rules l event (Some New))
                (apply_delta_rules r event (Some New))
        in
	match x with 
	    | `New (`METerm y) -> `METerm y
		  
	    | `New (`Sum (l, r)) -> adr_binary l r (fun x y -> `Sum(x,y))
	    | `New (`Minus (l, r)) -> adr_binary l r (fun x y -> `Minus(x,y))
	    | `New (`Product (l, r)) -> adr_binary l r (fun x y -> `Product(x,y))
	    | `New (`Min (l,r)) -> adr_binary l r (fun x y -> `Min(x,y))
	    | `New (`Max (l,r)) -> adr_binary l r (fun x y -> `Max(x,y))

	    | `New (`MapAggregate (fn, f, q)) ->
		  `MapAggregate(fn,
		  apply_delta_rules f event (Some New),
		  apply_delta_plan_rules q event (Some New))
		      
	    | _ -> 
                  print_debug x;
                  raise (RewriteException "Invalid recomputation expression.")
    in
	match m_expr with
            (* `MaintainMap should only be created when maintaining map domains *)
	    | `MaintainMap _ -> raise InvalidExpression

	    | `Incr(sid, op, re, bd, m_expr) ->
                  `Incr(sid, op, re, bd, push_delta (`Delta (event, m_expr)))
	    | `New(_) as e -> adr_new e

	    | _ -> 
		  begin
		      match rcs with
			  | Some New -> adr_new (`New (m_expr))
			  | Some Incr -> push_delta (`Delta (event, m_expr))
			  | None ->
                                print_debug m_expr;
                                raise (RewriteException "Invalid recomputation state.")
		  end


and apply_delta_plan_rules q event rcs =
    let print_debug p =
        print_endline ("apply_delta_plan_rules: "^
            "invalid recomputation plan:\n"^(indented_string_of_plan p 0))
    in
    let adpr_new x = 
        let adpr_mbterm mbterm cq =
            let adr_me me = apply_delta_rules me event (Some New) in
            let new_mbterm = 
                match mbterm with
                    | `BTerm(`MEQ(m_expr)) -> `MEQ(adr_me m_expr)
                    | `BTerm(`MNEQ(m_expr)) -> `MNEQ(adr_me m_expr)
                    | `BTerm(`MLT(m_expr)) -> `MLT(adr_me m_expr)
                    | `BTerm(`MLE(m_expr)) -> `MLE(adr_me m_expr)
                    | `BTerm(`MGT(m_expr)) -> `MGT(adr_me m_expr)
                    | `BTerm(`MGE(m_expr)) -> `MGE(adr_me m_expr)
                    | _ -> raise InvalidExpression
            in
                `Select(`BTerm(new_mbterm), apply_delta_plan_rules cq event (Some New))
        in
	match x with
	    | `NewPlan(`Relation r) -> `Relation r
		  
	    | `NewPlan(`Select (p, cq)) ->
		  begin
		      match p with
			  | `BTerm(`MEQ(m_expr)) | `BTerm(`MNEQ(m_expr))
			  | `BTerm(`MLT(m_expr)) | `BTerm(`MLE(m_expr))
			  | `BTerm(`MGT(m_expr)) | `BTerm(`MGE(m_expr))
                                -> adpr_mbterm p cq
			  | _ -> `Select(p, apply_delta_plan_rules cq event (Some New))
		  end
		      
	    | `NewPlan(`Project (attrs, cq)) ->
		  `Project(attrs, apply_delta_plan_rules cq event (Some New))
		      
	    | `NewPlan(`Union children) ->
		  `Union (List.map (fun c -> apply_delta_plan_rules c event (Some New)) children)
		      
	    | `NewPlan(`Cross (l,r)) ->
		  `Cross(apply_delta_plan_rules l event (Some New),
		  apply_delta_plan_rules r event (Some New))
		      		      
	    | _ ->
                  print_debug x;
                  raise (RewriteException "Invalid recomputation plan.")
    in
	match q with
	    | `IncrPlan(sid, op, d, bd, qq) ->
                  `IncrPlan(sid, op, d, bd, push_delta_plan (`DeltaPlan (event, qq)))

	    | `NewPlan(_) as p -> adpr_new p
	    | _ ->
		  begin
		      match rcs with
			  | Some New -> adpr_new (`NewPlan (q))
			  | Some Incr -> push_delta_plan q
			  | _ -> 
                                print_debug q;
                                raise (RewriteException "Invalid recomputation state.")
		  end

(*
 * Map expression simplification
 * -- invoked after binding extraction, incrementality analysis, and delta pushdown.
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
                              | (`Split(ldl, ldr),`Split(rdl, rdr)) ->
                                    `Split(merge ldl rdl, merge ldr rdr)
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
        unify_aux pred
        (*
        match (unify_aux pred) with
            | (None, u) ->
                  * Note if all conjuncts are removed we have an invalid predicate,
                   * thus we simply return the input, and let the caller handle the
                   * rewrite *
                  (pred, u)

            | (Some(p), u) -> (p, u)
        *)

(* TODO: remove unifications return val *)
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
                | `UnifyAttributes -> "unify attributes in select"
                | `BindAggTuple -> "binding aggregate args from tuple"
                | `BindAggQuery -> "binding aggregate args from query"
                | `DistributeAggUnion -> "distributing aggregate over union"
                | `TupleAgg -> "simplifying tuple aggregate"
                | `DistributeProdAggCross -> "distributing product agg over cross"
                | `DistributeSumAgg -> "distributing sum agg over query"
                | `DistributeProdAgg -> "distributing product agg over query"
                | `DistributeIndepAgg -> "distributing independent agg"
                | `NoRule -> "ascending, no valid rule"
        in
            print_endline ("Applying rule: "^desc)
    in
    (* Helper functions *)
    let bind_nearest_incr m_expr node bindings =
        let add_to_bindings bd =
            List.map (fun (a, e_opt) ->
                let a_matches = List.filter (fun (a2,e) -> compare_attributes a a2) bindings in
                    match a_matches with
                        | [] -> (a, None)
                        | [(a2, e)] -> (a, Some(e))
                        | _ -> raise DuplicateException)
                bd
        in
        let anc = ancestors m_expr node in
        let incr_matches =
            List.filter (fun a ->
                match a with
                    | `MapExpression (`Incr _)
                    | `MapExpression (`IncrDiff _)
                    | `Plan(`IncrPlan _)
                    | `Plan(`IncrDiffPlan _) -> true
                    | _ -> false)
                anc
        in
        let r =
            match incr_matches with
                | [] -> m_expr
                | h::t ->
                      begin
                          match h with
                              | `MapExpression (`Incr(sid,op,re,bd,e)) ->
                                    let new_bd = add_to_bindings bd in
                                        splice m_expr h (`MapExpression (`Incr(sid,op,re,new_bd,e)))

                              | `MapExpression (`IncrDiff(sid,op,re,bd,e)) ->
                                    let new_bd = add_to_bindings bd in
                                        splice m_expr h (`MapExpression (`IncrDiff(sid,op,re,new_bd,e)))

                              | `Plan (`IncrPlan(sid,op,d,bd,p)) ->
                                    let new_bd = add_to_bindings bd in
                                        splice m_expr h (`Plan (`IncrPlan(sid,op,d,new_bd,p)))

                              | `Plan (`IncrDiffPlan(sid,op,d,bd,p)) ->
                                    let new_bd = add_to_bindings bd in
                                        splice m_expr h (`Plan (`IncrDiffPlan(sid,op,d,new_bd,p)))

                              | _ -> raise InvalidExpression
                      end
        in
            print_endline ("bound nearest_incr:\n"^(indented_string_of_map_expression r 0));
            r
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

				        | `Plan(`Select (pred, `Project(projs, q))) ->
                                              print_rule_description `PullProjectSelect;
                                              let bound_pred = add_predicate_bindings pred projs in
                                              let (new_pred, new_unifications) = unify_predicate bound_pred q in
                                              let (new_np, new_rewrites) =
                                                  let npj = projs@new_unifications in
                                                      match (new_pred, q) with
                                                          | (None, `TrueRelation) ->
                                                                let x = `Plan(`Project(npj, q)) in
                                                                    (x, x::t)

                                                          | (Some p, `TrueRelation) ->
                                                                let nch = `Select(p, q) in
                                                                    (`Plan(`Project(npj, nch)), `Plan(nch)::t)

                                                          | (None, _) ->
                                                                (`Plan(`Project(npj, q)), `Plan(q)::t)

                                                          | (Some p, _) ->
                                                                let nch = `Select(p, q) in
                                                                    (`Plan(`Project(npj, nch)), `Plan(q)::t)
                                              in
					      let new_m_expr = splice m_expr np new_np in
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
                                              let (new_pred, new_unifications) = unify_predicate pred q in
                                                  begin
                                                      match new_pred with
                                                          (* Try to distribute predicate and push down *)
                                                          | Some(p) ->
                                                                let dpred = distribute_conjunctive_predicate p l r in
                                                                let (new_dpred_np, dpred_rw) =
                                                                    match dpred with
                                                                        | (Some lpred, Some rpred) ->
                                                                              (Some(`Cross(`Select(lpred, l), `Select(rpred,r))),
                                                                              [(`Plan l); (`Plan r)])

                                                                        | (Some lpred, None) ->
                                                                              (Some(`Cross(`Select(lpred, l), r)), [(`Plan l)])

                                                                        | (None, Some rpred) ->
                                                                              (Some(`Cross(l, `Select(rpred, r))), [(`Plan r)])

                                                                        | (None, None) -> (None, [])
                                                                in
                                                                let (new_unified_np, rw_before) = 
                                                                    match (new_dpred_np, new_unifications) with
                                                                        | (None, []) -> (new_dpred_np, [])
                                                                        | (Some x, []) -> (new_dpred_np, dpred_rw)
                                                                        | (None, y) ->
                                                                              let new_q = `Project(y, add_plan_bindings q y) in
                                                                                  (Some(`Select (p, new_q)), [(`Plan new_q)])
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

                                                          (* Completely prune selection from plan *)
                                                          | None ->
                                                                begin
                                                                    match new_unifications with
                                                                        | [] -> raise InvalidExpression
                                                                        | y ->
                                                                              let new_np = `Plan(`Project(y, add_plan_bindings q y)) in
                                                                              let new_m_expr = splice m_expr np new_np in
                                                                              let new_rewrites = new_np::t in
                                                                                  simplify new_m_expr new_rewrites
                                                                end
                                                  end

                                        (*
                                        | `Plan(`Select(pred, (`TrueRelation as q))) ->
                                              print_rule_description `UnifyAttributes;
                                              let (new_pred, new_unifications) = unify_predicate pred q in
                                                  begin
                                                      match new_unifications with
                                                          | [] -> simplify m_expr (t@[np])
                                                          | y ->
                                                                let new_np = `Plan(`Project(y, `TrueRelation)) in
                                                                let new_m_expr = splice m_expr np new_np in
                                                                let new_rewrites = new_np::t in
                                                                    simplify new_m_expr new_rewrites
                                                  end
                                        *)

                                        | `Plan(`Select (pred, q)) ->
                                              print_rule_description `UnifyAttributes;
                                              let (new_pred, new_unifications) = unify_predicate pred q in
                                                  begin
                                                      match new_pred with
                                                          | Some(p) ->
                                                                begin
                                                                    match new_unifications with
                                                                        | [] -> simplify m_expr (t@[np])
                                                                        | y ->
                                                                              (* Push unbound attr unifications down into query *)
                                                                              let new_q = `Project(y, add_plan_bindings q y) in
                                                                              let new_np = `Plan(`Select (p, new_q)) in
                                                                              let new_m_expr = splice m_expr np new_np in
                                                                              let new_rewrites = `Plan(new_q)::t in
                                                                                  simplify new_m_expr new_rewrites
                                                                end

                                                          (* Remove selection *)
                                                          | None ->
                                                                begin
                                                                    match new_unifications with
                                                                        | [] -> raise InvalidExpression
                                                                        | y ->
                                                                              let new_np = `Plan(`Project(y, add_plan_bindings q y)) in
                                                                              let new_m_expr = splice m_expr np new_np in
                                                                              let new_rewrites = new_np::t in
                                                                                  simplify new_m_expr new_rewrites
                                                                end
                                                  end

					| `MapExpression(
					      `MapAggregate(fn, f, `Project(projs, `TrueRelation))) ->
                                              print_rule_description `BindAggTuple;
					      let new_np = 
                                                  `MapExpression(add_map_expression_bindings f projs)
                                              in
					      let new_m_expr =
                                                  bind_nearest_incr (splice m_expr np new_np) new_np projs
                                              in
					      let new_rewrites = new_np::t in
						  simplify new_m_expr new_rewrites

					| `MapExpression(`MapAggregate (fn, f, `Project(projs, q))) ->
                                              print_rule_description `BindAggQuery;
					      let new_np = `MapExpression(
					          `MapAggregate (fn,
						  add_map_expression_bindings f projs, q))
					      in
					      let new_m_expr =
                                                  bind_nearest_incr (splice m_expr np new_np) new_np projs
                                              in
					      let new_rewrites = (`Plan(q))::t in
					          simplify new_m_expr new_rewrites
					              
					| `MapExpression(`MapAggregate (fn, f, `Union(c))) ->
                                              print_rule_description `DistributeAggUnion;
					      let new_np = `MapExpression(
						  List.fold_left
						      (fun acc ch ->
                                                          match fn with
                                                              | `Sum -> `Sum(acc, `MapAggregate(fn, f, ch))
                                                              | `Min -> `Min(acc, `MapAggregate(fn, f, ch))
                                                              | `Max -> `Max(acc, `MapAggregate(fn, f, ch)))
						      (`MapAggregate (fn, f, (List.hd c))) (List.tl c))
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

                                        (* rule 52 *)
					| `MapExpression(`MapAggregate (`Sum, `Product(l,r), `Cross(ql, qr))) ->
                                              print_rule_description `DistributeProdAggCross;
                                              let distribute dl dr dql dqr =
                                                  let new_np = `MapExpression(
						      `Product(`MapAggregate(`Sum, dl, dql), `MapAggregate(`Sum, dr, dqr)))
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

                                        (* rule 48a, 50, 54a, 55 *)
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

                                        (* rule 48b, 49, 54b, 56 *)
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
                                                  let r = `IfThenElse(`MGE(e_expr),
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
                                        (* rule 48c, 54c *)
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
 * Map domain maintenance
 *
 *)

let rec maintain_map_expression_domains m_expr =
    let recur e = maintain_map_expression_domains e in
    let maintain_binary l r fn = fn (recur l) (recur r) in
    let maintain_nme mbterm ce =
        let rce = recur ce in
            match mbterm with
                | `MEQ _ -> `MEQ(rce)
                | `MNEQ _ -> `MNEQ(rce)
                | `MLT _ -> `MLT(rce)
                | `MLE _ -> `MLE(rce)
                | `MGT _ -> `MGT(rce)
                | `MGE _ -> `MGE(rce)
                | _ -> raise InvalidExpression
    in
        match m_expr with
            | `METerm _ -> m_expr
            | `Sum (l,r) -> maintain_binary l r (fun x y -> `Sum(x,y))
            | `Minus (l,r) -> maintain_binary l r (fun x y -> `Minus(x,y))
            | `Product (l,r) -> maintain_binary l r (fun x y -> `Product(x,y))
            | `Min (l,r) -> maintain_binary l r (fun x y -> `Min(x,y))
            | `Max (l,r) -> maintain_binary l r (fun x y -> `Max(x,y))

            (* TODO: handle nested aggregates *)
            | `MapAggregate (fn, f, q) -> 
                  `MapAggregate(fn, recur f, maintain_plan_domains q)

            | `IfThenElse (cond, l, r) ->
                  begin
                      let new_cond = 
                          match cond with
                              | `MEQ(ce) | `MNEQ(ce)
                              | `MLT(ce) | `MLE(ce)
                              | `MGT(ce) | `MGE(ce)
                                    -> maintain_nme cond ce
                              | _ -> cond
                      in
                          `IfThenElse(new_cond, recur l, recur r)
                  end

            | `Delta _ | `New _ -> raise InvalidExpression

            (* Should be handled as part of map aggregates *)
            | `MaintainMap _
            | `Incr _ | `IncrDiff _ ->
                  print_endline ("maintain_map_expression_domains: found unhandled expr:\n"^
                      (indented_string_of_map_expression m_expr 0));
                  raise InvalidExpression

            | `Insert _ | `Update _ | `Delete _ -> m_expr


and maintain_plan_domains plan =
    (* Helper functions *)
    let recur q = maintain_plan_domains q in
    let is_immediately_incremental m_expr =
        match m_expr with
            | `Incr _ | `IncrDiff _ -> true
            | _ -> false
    in
    let is_incremental_plan plan =
        match plan with
            | `IncrPlan _ | `IncrDiffPlan _ -> true
            | _ -> false
    in
    let rec is_incremental m_expr =
        match m_expr with
            | `Incr _ | `IncrDiff _ -> true
            | `Sum(l,r) | `Minus(l,r) | `Product(l,r)
            | `Min(l,r) | `Max(l,r) -> (is_incremental l) || (is_incremental r)
            | _ -> false
    in
    (* Body *)
    match plan with
        | `TrueRelation | `FalseRelation | `Relation _ -> plan

        | `Select (pred, cq) ->
              let nme_pred mbterm p =
                  let r = 
                      match mbterm with
                          | `BTerm(`MEQ _) -> `MEQ(p)
                          | `BTerm(`MNEQ _) -> `MNEQ(p)
                          | `BTerm(`MLT _) -> `MLT(p) 
                          | `BTerm(`MLE _) -> `MLE(p) 
                          | `BTerm(`MGT _) -> `MGT(p) 
                          | `BTerm(`MGE _) -> `MGE(p) 
                          | _ -> raise InvalidExpression
                  in
                      `BTerm(r)
              in
              let nme_pred_pair mbterm u_me id_me =
                  let (l,r) =
                      match mbterm with
                          | `BTerm(`MEQ _) -> (`MEQ(u_me), `MEQ(id_me))
                          | `BTerm(`MNEQ _) -> (`MNEQ(u_me), `MNEQ(id_me))
                          | `BTerm(`MLT _) -> (`MLT(u_me), `MLT(id_me))
                          | `BTerm(`MLE _) -> (`MLE(u_me), `MLE(id_me))
                          | `BTerm(`MGT _) -> (`MGT(u_me), `MGT(id_me))
                          | `BTerm(`MGE _) -> (`MGE(u_me), `MGE(id_me))
                          | _ -> raise InvalidExpression
                  in
                      (`BTerm(l), `BTerm(r))
              in
              begin
                  match (pred, cq) with

                      | (`BTerm(`MEQ(x)), y) | (`BTerm(`MNEQ(x)), y)
                      | (`BTerm(`MLT(x)), y) | (`BTerm(`MLE(x)), y)
                      | (`BTerm(`MGT(x)), y) | (`BTerm(`MGE(x)), y)
                            when (is_immediately_incremental x) && (is_incremental_plan y)
                                ->
                            let insert_event =
                                match (x, y) with
                                    | (`Incr (_,e_op,_,_,_), `IncrPlan(_,p_op,_,_,_)) ->
                                          is_insert_incr e_op p_op
                                    | _ ->
                                          raise (ValidationException
                                              "Invalid plan types for domain maintenance.")
                            in

                            let ((update_me, update_cq), (ins_or_del_me, ins_or_del_cq)) =
                                match (x,y) with
                                    | (`Incr (side, opp, re, bde, e), `IncrPlan (sidq, opq, d, bdq, cqq)) ->
                                          let u = (x, `Domain(sidq, d)) in
                                          let id =
                                              (`MaintainMap(side,(if insert_event then `Init d else `Final d), bde,re),
                                              (`IncrDiffPlan(sidq, opq, d, bdq, cqq)))
                                          in
                                              (u, id)

                                    | (`IncrDiff _, _) | (_, `IncrDiffPlan _) | _
                                              -> raise InvalidExpression
                            in

                            let (update_pred, ins_or_del_pred) =
                                nme_pred_pair pred update_me ins_or_del_me
                            in

                                if insert_event then
                                    `Union
                                        [`Select(update_pred, update_cq);
                                         `Select(ins_or_del_pred, ins_or_del_cq)]
                                else
                                    `Union
                                        [`Select(ins_or_del_pred, ins_or_del_cq);
                                         `Select(update_pred, update_cq)]
                        

                      (* TODO: what if m_expr contains a mixture of `New and `Incr aggregates?
                       * This will currently throw an exception in maintain_map_expression_domains *)
                      | (`BTerm(`MEQ(x)), y) | (`BTerm(`MNEQ(x)), y)
                      | (`BTerm(`MLT(x)), y) | (`BTerm(`MLE(x)), y)
                      | (`BTerm(`MGT(x)), y) | (`BTerm(`MGE(x)), y)
                            when (is_incremental x) && (is_incremental_plan y)
                                ->
                            begin
                                print_endline ("maintain_plan_domains: "^
                                    "complex nested aggregate conditions not yet supported.");
                                raise InvalidExpression
                            end


                      | (`BTerm(`MEQ(m_expr)), _) | (`BTerm(`MNEQ(m_expr)), _)
                      | (`BTerm(`MLT(m_expr)), _) | (`BTerm(`MLE(m_expr)), _)
                      | (`BTerm(`MGT(m_expr)), _) | (`BTerm(`MGE(m_expr)), _)
                            ->
                            let new_me = maintain_map_expression_domains m_expr in
                            let new_pred = nme_pred pred new_me in
                                `Select(new_pred, recur cq)


                      | (_, _) -> `Select(pred, recur cq)
              end

        | `Project (attrs, cq) -> `Project(attrs, recur cq)
        | `Union ch -> `Union (List.map recur ch)
        | `Cross (l,r) -> `Cross(recur l, recur r)

        | `DeltaPlan _ | `NewPlan _ -> raise InvalidExpression

        (* Should be handled as part of select *)
        | `IncrPlan _ | `IncrDiffPlan _ -> raise InvalidExpression

        (* Domains are produced by this pass. *)
        | `Domain _ -> raise InvalidExpression


(*
 *
 * Query compilation
 *
 *)		      
let compile_target m_expr delta =
    (* Debugging helpers *)
    let print_incr_and_delta_pass expr frontier_expr delta_expr =
        print_endline ("input: "^(string_of_map_expression expr));
	print_endline ("frontier_expr: "^(string_of_map_expression frontier_expr));
	print_endline ("delta_expr:\n"^(indented_string_of_map_expression delta_expr 0));
	print_endline ("# br: "^(string_of_int (List.length (get_bound_relations delta_expr))))
    in
    let print_domain_pass domain_expr =
        print_endline ("Domain maintaining expr:\n"^
            (indented_string_of_map_expression domain_expr 0))
    in
    let print_simplify_pass compiled_expr simplified_expr =
        print_endline ("cc_expr: "^(string_of_map_expression compiled_expr));
        print_endline ("sc_expr: "^(string_of_map_expression simplified_expr));
        print_endline (String.make 50 '>')
    in
    (* Code body *)
    let compile_aux e = 
	let frontier_expr = analyse_map_expr_incrementality e delta in
        let frontier_w_init_expr =
            initialize_map_expression frontier_expr (Some New)
        in
        let delta_expr = apply_delta_rules frontier_w_init_expr delta (Some New) in
	let simplified_delta_expr = simplify_map_expr_constants delta_expr in

        print_incr_and_delta_pass e frontier_w_init_expr simplified_delta_expr;

        let domain_expr =
            match simplified_delta_expr with
                | `Incr _ | `IncrDiff _ -> simplified_delta_expr
                | _ -> maintain_map_expression_domains simplified_delta_expr
        in

        print_domain_pass domain_expr;

	let br = List.map (fun x -> `Plan x) (get_bound_relations domain_expr) in
        let simplified_domain_expr = simplify domain_expr br in
	let sc_expr = simplify_map_expr_constants simplified_domain_expr in
        let compilation_stages =
            [frontier_expr; frontier_w_init_expr; delta_expr; simplified_delta_expr;
            domain_expr; simplified_domain_expr; sc_expr]
        in
            print_simplify_pass simplified_domain_expr sc_expr;
            (sc_expr, compilation_stages)
    in

    let (dependent_expr, binding_exprs) = extract_map_expr_bindings m_expr in

    print_endline ("Expression after extraction:\n"^
        (indented_string_of_map_expression dependent_expr 0));

    (* binding list * (var * map_expression list) list *)
    let (compiled_binding_exprs, binding_stages) =
        List.split
	    (List.map
	        (fun b ->
	            match b with
	                | `BindMapExpr(v,e) ->
                              print_endline ("Compiling binding: "^v);
                              let (binding_r, binding_cs) = compile_aux e in
                                  (`BindMapExpr(v, binding_r), (v, binding_cs))
	                | _ -> (b, ("", [])))
	        binding_exprs)
    in
    let (compiled_dep_expr, dep_stages) = compile_aux dependent_expr in
	((compiled_dep_expr, compiled_binding_exprs),
        (dep_stages, (List.filter (fun (_,x) -> not(x = [])) binding_stages)))


(*
 *
 * Recursive compilation
 *
 *)

let compare_events e1 e2 =
    match (e1,e2) with
        | (`Insert(n1,_), `Insert(n2,_)) -> n1 = n2
        | (`Delete(n1,_), `Delete(n2,_)) -> n1 = n2
        | _ -> false

let generate_all_events m_expr = 
    let gbr = get_base_relations m_expr in
        List.fold_left 
            (fun acc x ->
	        match x with 
	            | `Relation (n,f) ->
                          let new_events = [`Insert (n,f);`Delete (n,f)] in
                              acc@(List.filter
                                  (fun e -> not(List.exists (compare_events e) acc))
                                  new_events)
	            | _ -> acc)
            [] gbr 

let attributes_of_variables me =
    let me_uba = get_unbound_attributes_from_map_expression me true in
    let me_with_uba =
        map_map_expr
            (fun e -> match e with
                | `ETerm (`Variable(v)) ->
                      if List.mem (`Unqualified v) me_uba
                      then `ETerm(`Attribute(`Unqualified(v))) else e
                | _ -> e)
            (fun b -> b) (fun m -> m) (fun p -> p) me
    in
        print_endline ("attrs_of_vars me: "^(string_of_map_expression me));
        print_endline ("attrs_of_vars me with uba: "^
            (string_of_map_expression me_with_uba)); 
        me_with_uba

(* Returns:
 * new map exprs w/ vars replacing extracted map exprs
 * a list of extracted map exprs
 * a list of hashcodes of the maps extracted, the parent hashcode,
 *   and vars representing extracted maps
 *)
(* map_expression ->
     map_expression * map_expression list * (int * variable_identifier) list
*)
let extract_incremental_query m_expr event_path =
    (* orig me -> incr me
         -> spliced me * incr rel me list * var list *)
    let rec eiq_find_plan orig_m_expr incr_m_expr =
        let recur_binary l r =
            let (ln, lm, lv) = eiq_find_plan orig_m_expr l in
            let (rn, rm, rv) = eiq_find_plan ln r in
                (rn, lm@rm, lv@rv)
        in
        match incr_m_expr with
            | `METerm _ | `Insert _ | `Delete _ | `Update _ -> (orig_m_expr, [], [])
	    | `Sum (l, r) | `Minus (l, r) | `Product (l, r)
	    | `Min (l, r) | `Max (l, r) 
	    | `IfThenElse (_, l, r) -> recur_binary l r

            | `MapAggregate _ ->
                  let br = get_base_relations incr_m_expr in
                  let remaining_deltas =
                      List.filter
                          (fun x -> match x with
                              | `Relation(n,_) ->
                                    not(List.exists
                                        (fun evt ->
                                            match evt with
                                                | `Insert(n2, _) | `Delete(n2, _) -> n2 = n)
                                        event_path)
                              | _ -> raise InvalidExpression)
                          br
                  in
                      if (List.length remaining_deltas = 0) then
                          (orig_m_expr, [], [])
                      else
                          let map_var = gen_var_sym() in
                          let new_m_expr =
                              splice orig_m_expr (`MapExpression incr_m_expr)
                                  (`MapExpression (`METerm(`Variable map_var)))
                          in
                              (new_m_expr, [incr_m_expr], [map_var])

            | `Delta _ | `New _ | `MaintainMap _
            | `Incr _ | `IncrDiff _ -> raise InvalidExpression
    in

    (* map expr -> map expr -> map expr * map expr list * var list *)
    let rec eiq_aux orig_expr m_expr =
        match m_expr with
            (* TODO: should we do something to track the bindings here? *)
            | `Incr(sid,_,_,_,e) | `IncrDiff(sid,_,_,_,e) ->
                  eiq_find_plan orig_expr e

	    | `Sum (l, r) | `Minus (l, r) | `Product (l, r)
	    | `Min (l, r) | `Max (l, r)
	    | `IfThenElse (_, l, r) ->
                  let (l_rw, l_cip, l_v) = eiq_aux orig_expr l in
                  let (r_rw, r_cip, r_v) = eiq_aux l_rw r in
                      (r_rw, l_cip@r_cip, l_v@r_v)

            | `MapAggregate(_,f,q) -> eiq_aux orig_expr f
            | `New (e) -> eiq_aux orig_expr e
            | `MaintainMap (_,_,_,e) -> eiq_aux orig_expr e
	    | `METerm _
            | `Insert _ | `Delete _ | `Update _ -> (orig_expr, [], [])
            | `Delta _ -> raise InvalidExpression
    in
    let (new_m_expr, incr_m_exprs, map_vars) = eiq_aux m_expr m_expr in

    (* Change any unbound variables to unbound attributes.
     * This is to change variables bound by the compilation step
     * just performed to map dimensions *)
    (* Track id of expr kept as a map *)

    let incr_maps = List.map attributes_of_variables incr_m_exprs in
    let map_ids = List.map dbt_hash incr_maps in
    let (x,y,z) = 
        (new_m_expr, incr_maps, List.combine map_ids map_vars)
    in
        List.iter (fun (id, var) ->
            print_endline ("Extracted map "^var^" hash: "^(string_of_int id))) z;
        (x,y,z)


(* map vars = (int * var)
 * binding list -> int -> binding list * map expr list * map vars list *)
let extract_incremental_query_from_bindings bindings parent_id event_path =
    List.fold_left
 	(fun (nme_acc, cip_acc, mv_acc) x -> 
	    match x with
		| `BindMapExpr (v, m) ->
                      let (nme, cip_l, mv_l) = extract_incremental_query m event_path in

                      print_endline ("Extracting binding incr from:"^(string_of_map_expression m));
                      print_endline ("New binding incr from:"^(string_of_map_expression nme));

                      (nme_acc@[`BindMapExpr(v, nme)], cip_acc@cip_l, mv_acc@mv_l)

		| _ -> (nme_acc@[x], cip_acc, mv_acc))
        ([], [], []) bindings


(* map expression list -> delta list ->
 *      map expr list * (int * var) list * (state_id * int) list
 *      * (event list * (handler stages * (var * binding stages) list) list) list
 *      * (event * (handler * binding list)) list
 * 
 * map exprs to compile -> delta list ->
 *      global maps created * map vars * state parents
 *      * compile trace * event handlers
 *)
let compile_target_all m_expr_l event_list =

    (* Debugging helpers *)
    let print_event_path evt_path =
        print_endline (String.make 50 '-');
        print_endline ("Compiling for event path:"^
            (List.fold_left
                (fun acc e ->
                    (if (String.length acc) = 0 then "" else acc^", ")^
                        (string_of_delta e))
                "" evt_path))
    in
    let print_input e_list me_list existing_maps = 
        print_endline ("cta_aux events:"^(String.concat "," (List.map string_of_delta e_list)));
        print_endline
            (List.fold_left
                (fun acc e ->
                    (if (String.length acc) = 0 then "" else acc^"\n")^
                        "input: "^(string_of_map_expression e))
                "" me_list);
        print_endline
            (List.fold_left
                (fun acc e ->
                    (if (String.length acc) = 0 then "existing: " else acc^"\nexisting: ")^
                        (string_of_map_expression e))
                "" existing_maps)
    in
    let print_new_map_expressions event_path hbs me_list  =
        print_endline (String.make 50 '-');
        print_endline ("New maps: ("^
            (string_of_int (List.length hbs))^" compiled results, "^
            (string_of_int (List.length me_list))^" map expressions)");
        print_event_path event_path;
        List.iter
            (fun e -> print_endline ("map: "^(string_of_map_expression e)))
            me_list
    in

    (* Code body *)

    (* TODO: update sig for state parents *)
    (* Signature:
     * map var = (int * var)
     * state parent = (state_identifier * int)
     *
     * event list -> (map expr * int) list -> event list -> map expr list
     * -> map var list -> state parent list
     * -> map expr list * map var list * state parent list *
     *        (int * event * (handler * binding list)) list
     *
     * Semantically:
     * event level -> map exprs to compile
     * -> event path -> running maps
     * -> running map vars -> running state parents
     * -> maps created * map vars * state parents * handlers by level, event *)

    let rec cta_aux e_list me_list e_path maps map_vars state_parents ep_stages =
        print_input e_list me_list maps;
        List.fold_left
	    (fun (maps_acc, mv_acc, stp_acc, eps_acc, eh_acc) event -> 
                let event_rel = get_bound_relation event in
                let current_event_path = e_path@[event] in
		let new_events =
                    List.filter (fun e -> not((get_bound_relation e) = event_rel)) e_list
                in 
                let me_event_used_list =
                    let event_updates_some_base_relation br =
                        List.exists (fun r -> match r with
                            | `Relation (n,_) -> n = event_rel | _ -> false) br
                    in
                        List.filter
                            (fun me ->
                                let br = get_base_relations me in
                                    event_updates_some_base_relation br)
                            me_list
                in

                (* ((handler * binding) * parent id) list *
                 *      (handler stages * binding stages list) list) *)
		let (hbid_l, stages_l) = 
		    List.fold_left
                        (fun (hb_acc, stages_acc) me -> 
                            print_event_path current_event_path;
                            let (me_compiled, me_stages) = compile_target me event in
                                (hb_acc@[(me_compiled, dbt_hash me)], stages_acc@[me_stages]))
                        ([], []) me_event_used_list
		in

                (* map vars list = (int * var) list
                 * state parents list = (state_id * int) list
                 *
                 * ((new map expr, new binding) list, recursive map expr list,
                 *      * map vars list, state parent list) *)

                let (new_hb_list, nearest_incr_me, next_map_vars, next_state_parents) =
                    List.fold_left
			(fun (nhb_acc, rme_acc, mv_acc, sp_acc) ((handler, binding), parent_id) ->
                            (* handler, me_l, me_var_l *)
                            let (new_h, h_rme_l, h_var_l) =
                                extract_incremental_query handler current_event_path
                            in

                            print_endline ("Extracting handler incr from:"^
                                (string_of_map_expression handler));

                            print_endline ("New handler incr: "^(string_of_map_expression new_h));

                            (* Note no state parents for bindings, since bindings and
                             * handlers run in sequence, and thus bindings cannot update parent *)
                            let sp_h_l = 
                                match new_h with
                                    | `Incr(sid,_,_,_,_) | `IncrDiff(sid,_,_,_,_) ->
                                          [(sid, parent_id, handler)]
                                    | _ -> []
                            in

                            (* binding_l, me_l, me_var_l *)
                            let (new_b, b_rme_l, b_var_l) =
                                extract_incremental_query_from_bindings
                                    binding parent_id current_event_path
                            in
                                (* Flatten locally across handler and bindings *)
                                (* ((new map expr, new binding) list, recursive map expr list,
                                 *      * map vars list, state parent list) *)
                                (nhb_acc@[(new_h, new_b)], rme_acc@(h_rme_l@b_rme_l),
                                mv_acc@(h_var_l@b_var_l), sp_acc@(sp_h_l)))

			([], [], [], []) hbid_l
                in
                (* TODO: rethink if we need maps accumulated to include vars
                 * for map name generation *)
                let next_incr_me =
                    List.filter
                        (fun me ->
                            let r = not (List.mem
                                (attributes_of_variables me)
                                (List.map attributes_of_variables maps_acc))
                            in
                                if not r then
                                    print_endline ("Found existing map:\n"^
                                        (indented_string_of_map_expression me 0));
                                r)
		        nearest_incr_me
                in

                    print_new_map_expressions current_event_path new_hb_list next_incr_me;
		    let (child_maps, child_vars, child_stp,  child_eps, children) = 
		        match new_events with
		  	    | [] ->
                                  (maps_acc@next_incr_me,
                                  mv_acc@next_map_vars, stp_acc@next_state_parents,
                                  (eps_acc@[(current_event_path, stages_l)]), [])
			    | _ ->
                                  cta_aux new_events next_incr_me
                                      current_event_path (maps_acc@next_incr_me)
                                      (mv_acc@next_map_vars) (stp_acc@next_state_parents)
                                      (eps_acc@[(current_event_path, stages_l)])
		    in
                        (child_maps, child_vars, child_stp, child_eps,
                        eh_acc@((List.length e_path, event, new_hb_list)::children)))

            (maps, map_vars, state_parents, ep_stages, []) e_list
    in
    let group_by_event compiled_exprs event_list =
        List.fold_left 
            (fun res event ->
	        let event_exprs = List.filter (fun (_,e,_) -> e = event) compiled_exprs in
                let sorted_by_level =
                    List.sort (fun (lv1, _, _) (lv2, _, _) -> compare lv1 lv2) event_exprs
                in
	            res@[(event, List.flatten (List.map (fun (_,_,exprs) -> exprs) sorted_by_level))])
            [] event_list
    in
    let (maps, map_vars) = 
        List.fold_left 
            (fun (map_acc,mv_acc) x -> 
                if List.length (get_unbound_attributes_from_map_expression x true) != 0 then 
                    let map_var = gen_var_sym() in 
                        (x::map_acc, (dbt_hash x, map_var)::mv_acc)
                else (map_acc, mv_acc))
            ([],[]) m_expr_l
    in 
    let (r_maps, r_map_vars, r_st_parents, r_ep_stages, result) =
        cta_aux event_list m_expr_l [] maps map_vars [] [] 
    in
        (r_maps, r_map_vars, r_st_parents, r_ep_stages, group_by_event result event_list)
