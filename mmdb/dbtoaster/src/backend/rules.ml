(*
  Rewrite rules for DBToaster

  Author: Yanif Ahmad
	Date: 3/5/2009
*)

open Plan

exception RuleException of string

(* Plan helpers
 -- these should be moved to the plan module
*)

let rename_attributes attrs id =
	List.map
		(fun a -> match a with
			| `Qualified(r,b) -> `Qualified (id,b) | r -> r)
		attrs

let rec get_output_attributes p acc =
	match p with
		| `Operator (`Select _, schildren) ->
			List.fold_left
				(fun acc (id, cp) ->
					rename_attributes (get_output_attributes cp acc) id)
				acc schildren

		| `Operator (`Project(pparams), _) ->
			List.fold_left (fun acc (attr, _) -> acc@[attr]) 
				[] pparams.pattributes

		| `Operator (`Join(jparams), _) ->
			List.fold_left (fun acc (attr, _) -> acc@[attr]) 
				[] jparams.jattributes

		| `Operator (`Cross(cparams), _) -> 
			List.fold_left (fun acc (attr, _) -> acc@[attr]) 
				[] cparams.cattributes

		| `Operator (`Aggregate(aparams), _) -> acc@[aparams.out]

		| `Operator (`Topk(tparams), _) ->
			raise (PlanException ("Topk is unsupported for now."))
			
		| `TupleRelation r ->
			List.map (fun (id, ty) -> `Unqualified id) r.schema
			
		| `Relation r ->
			List.map (fun (id, ty) -> `Unqualified id) r.schema

let get_attributes_from_expression acc expr =
	fold_map_expr 
		(fun acc expr ->
				match expr with
					| `ETerm (`Attribute a) -> (a::acc, expr)
					| _ -> (acc, expr))
		acc expr

let get_attributes_from_bool_expression b_expr acc =
	fold_map_bool_expr
		(fun acc b_expr ->
			match b_expr with
				| `BTerm(`LT (l,r)) | `BTerm(`LE (l,r))
				| `BTerm(`GT (l,r)) | `BTerm(`GE (l,r))
				| `BTerm(`EQ (l,r)) | `BTerm(`NE (l,r)) ->
					let (acc2, _) = get_attributes_from_expression acc l in
					let (acc3, _) = get_attributes_from_expression acc2 r in
						(acc3, b_expr)
				| _ -> (acc, b_expr))
		acc b_expr

let get_attributes_from_aggregate_expression agg_expr acc =
	match agg_expr with
		| `Sum e  | `Count e 
		| `Min e | `Max e 
		| `Average e  | `Median e  | `StdDev e -> 
				let (attrs, _) = get_attributes_from_expression acc e in attrs

		| `Function (id, args) ->
			List.fold_left
				(fun acc e ->
					let (attrs, _) = get_attributes_from_expression acc e in attrs)
				acc args

let get_variables_from_expression acc expr =
	fold_map_expr
		(fun acc expr ->
			match expr with
				| `ETerm (`Variable a) -> (a::acc, expr)
				| _ -> (acc, expr))
		acc expr

let get_variables_from_bool_expression b_expr acc =
	fold_map_bool_expr
		(fun acc b_expr ->
			match b_expr with
				| `BTerm(`LT (l,r)) | `BTerm(`LE (l,r))
				| `BTerm(`GT (l,r)) | `BTerm(`GE (l,r))
				| `BTerm(`EQ (l,r)) | `BTerm(`NE (l,r)) ->
					let (acc2, _) = get_variables_from_expression acc l in
					let (acc3, _) = get_variables_from_expression acc2 r in
						(acc3, b_expr)
				| _ -> (acc, b_expr))
		acc b_expr

let get_unknowns_from_expression acc expr =
	fold_map_expr
		(fun acc expr ->
			match expr with
				| `ETerm (`Attribute a) -> ((`Attribute a)::acc, expr)
				| `ETerm (`Variable a) -> ((`Variable a)::acc, expr)
				| _ -> (acc, expr))
		acc expr

let get_unknowns_from_bool_expression b_expr acc =
	fold_map_bool_expr
		(fun acc b_expr ->
			match b_expr with
				| `BTerm(`LT (l,r)) | `BTerm(`LE (l,r))
				| `BTerm(`GT (l,r)) | `BTerm(`GE (l,r))
				| `BTerm(`EQ (l,r)) | `BTerm(`NE (l,r)) ->
					let (acc2, _) = get_unknowns_from_expression acc l in
					let (acc3, _) = get_unknowns_from_expression acc2 r in
						(acc3, b_expr)
				| _ -> (acc, b_expr))
		acc b_expr

let rename_expression acc expr old_rid new_rid =
	fold_map_expr
		(fun acc expr ->
			match expr with
				| `ETerm (`Attribute (`Qualified (r, a))) ->
					if r = old_rid then
						((`Qualified (new_rid, a)::acc,
							`ETerm (`Attribute (`Qualified (new_rid, a)))))
					else
						(acc, expr)
				| _ -> (acc, expr))
		acc expr

let rename_bool_expression b_expr acc old_rid new_rid =
	let dispatch_rename_expr acc e = rename_expression acc e old_rid new_rid in
	let dispatch_expr e1 e2 acc =
		let (acc2, ne1) = fold_map_expr dispatch_rename_expr acc e1 in
		let (acc3, ne2) = fold_map_expr dispatch_rename_expr acc2 e2 in
			(acc3, (ne1, ne2))
	in
	fold_map_bool_expr
		(fun acc b_expr ->
			match b_expr with
				| `BTerm b ->
						begin
							match b with
								| `LT(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`LT (e2)))
								| `LE(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`LE (e2)))
								| `GT(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`GT (e2)))
								| `GE(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`GE (e2)))
								| `EQ(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`EQ (e2)))
								| `NE(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`NE (e2)))
								| _ -> (acc, b_expr)
						end
				| _ -> (acc, b_expr))
		acc b_expr 

let rename_aggregate_expression agg_expr acc old_rid new_rid =
	let get_expr (_, expr) = expr in
	match agg_expr with
		| `Sum e -> `Sum (get_expr (rename_expression [] e old_rid new_rid))
		| `Count e -> `Count (get_expr (rename_expression [] e old_rid new_rid))
		| `Min e -> `Min (get_expr (rename_expression [] e old_rid new_rid))
		| `Max e -> `Max (get_expr (rename_expression [] e old_rid new_rid))
		| `Average e -> `Average (get_expr (rename_expression [] e old_rid new_rid))
		| `Median e -> `Median (get_expr (rename_expression [] e old_rid new_rid))
		| `StdDev e -> `StdDev (get_expr (rename_expression [] e old_rid new_rid))
		| `Function (id, args) ->
			`Function(id,
				List.map
					(fun e -> (get_expr (rename_expression [] e old_rid new_rid))) args)

let bind_expression acc expr bound_vars =
	fold_map_expr
		(fun acc expr ->
			match expr with
				| `ETerm (`Attribute(`Qualified (_, attr))) as e ->
						if (List.exists (fun id -> id = attr) bound_vars)
						then (attr::acc, `ETerm(`Variable(attr)))
						else (acc, e)
				| `ETerm (`Attribute(`Unqualified(attr))) as e ->
						if (List.exists (fun id -> id = attr) bound_vars)
						then (attr::acc, `ETerm(`Variable(attr)))
						else (acc, e)
				| e -> (acc, e))
		acc expr

let bind_expression_tuplerel acc expr tuple_rel =
	fold_map_expr
		(fun acc expr ->
			match expr with
				| `ETerm (`Attribute(`Qualified (rel, attr))) as e ->
						if (rel = tuple_rel.name &&
							(List.exists (fun (id, ty) -> id = attr) tuple_rel.schema) )
						then
							(attr::acc, `ETerm(`Variable(attr)))
						else (acc, e)

				| `ETerm (`Attribute(`Unqualified(attr))) as e ->
						if (List.exists (fun (id, ty) -> id = attr) tuple_rel.schema)
						then
							(attr::acc, `ETerm(`Variable(attr)))
						else (acc, e)
	
				| e -> (acc, e))
		acc expr

let bind_bool_expression_aux dispatch_bind_expr b_expr acc  =
	let dispatch_expr e1 e2 acc =
		let (acc2, ne1) = fold_map_expr dispatch_bind_expr acc e1 in
		let (acc3, ne2) = fold_map_expr dispatch_bind_expr acc2 e2 in
			(acc3, (ne1, ne2))
	in
		fold_map_bool_expr
			(fun acc b_expr ->
				match b_expr with
					| `BTerm b ->
						begin
							match b with
								| `LT(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`LT (e2)))
								| `LE(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`LE (e2)))
								| `GT(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`GT (e2)))
								| `GE(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`GE (e2)))
								| `EQ(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`EQ (e2)))
								| `NE(l,r) -> let (acc2, e2) = dispatch_expr l r acc in (acc2, `BTerm(`NE (e2)))
								| _ -> (acc, b_expr)
						end
					| _ -> (acc, b_expr))
		 acc b_expr 

let bind_bool_expression b_expr acc bound_vars =
	let dispatch_bind_expr acc e = bind_expression acc e bound_vars in
		bind_bool_expression_aux dispatch_bind_expr b_expr acc

let bind_bool_expression_tuplerel b_expr acc tuple_rel =
	let dispatch_bind_expr acc e = bind_expression_tuplerel acc e tuple_rel in
		bind_bool_expression_aux dispatch_bind_expr b_expr acc

let bind_aggregate_expression agg_params bound_vars =
	let get_expr (attrs_used, expr) = expr in
	match agg_params.aggregate with
		| `Sum e -> `Sum (get_expr (bind_expression [] e bound_vars))
		| `Count e -> `Count (get_expr (bind_expression [] e bound_vars))
		| `Min e -> `Min (get_expr (bind_expression [] e bound_vars))
		| `Max e -> `Max (get_expr (bind_expression [] e bound_vars))
		| `Average e -> `Average (get_expr (bind_expression [] e bound_vars))
		| `Median e -> `Median (get_expr (bind_expression [] e bound_vars))
		| `StdDev e -> `StdDev (get_expr (bind_expression [] e bound_vars))
		| `Function (id, args) ->
			`Function(id,
				List.map
					(fun e -> (get_expr (bind_expression [] e bound_vars))) args)


(* Rule matching 

	Notes: all rules have the following signature
    plan -> maps list -> (plan, maps, plan list * plan list) option
*)

(* Rule helpers *)

(* symbol generation *)
let id_counter = ref 0

let gen_rel_id () =
	let new_sym = !id_counter in
		incr id_counter;
		"rel"^(string_of_int new_sym)
	
let trim_relation attrs_used rel_plan =
	match rel_plan with
		| `TupleRelation r ->
			let new_fields =
				List.filter (fun (id, ty) -> not(List.mem id attrs_used)) r.schema
			in
				`TupleRelation({name=r.name; schema=new_fields})
		| _ ->
			raise
				(RuleException
					("Attempted to trim something other than a tuple"))

let merge_child_bindings child_maps = 
	List.fold_left
		(fun acc cm ->
			match cm with 
				| [] -> acc 
				| [(_,bindings)] -> acc@bindings
				| _ ->
					raise 
						(RuleException
							("Found multiple bindings, "^
							"nested aggregates are unsupported for now.")))
		[] child_maps

(* TODO: optimize this by keeping an attribute dependency graph
		data structure using ocamlgraph *)
let rec get_defining_base_relations node attrs acc =
	(* Remove any output relation qualification *)
	let unqualified_attrs =
		List.map
			(fun a -> match a with
				| `Qualified (_,b) -> `Unqualified b
				| `Unqualified b -> `Unqualified b )
			attrs
	in
	let chase_attrs project_result children =
		(* Restrict to attributes used downstream *)
		let attr_exprs =
			List.filter
				(fun (id, e) -> List.mem id unqualified_attrs) project_result
		in
			print_endline ("Unqualified:\n"^
				(List.fold_left
					(fun acc id -> acc^","^(string_of_attribute_identifier id))
					"" unqualified_attrs));

			print_endline ("Projections:\n"^
				(List.fold_left
					(fun acc (id, e) -> acc^","^(string_of_attribute_identifier id))
					"" project_result));

			print_endline ("Attr expr:\n"^
				(List.fold_left
					(fun acc (id, e) -> acc^","^(string_of_attribute_identifier id))
					"" attr_exprs));

			List.fold_left
				(fun acc (id, e) ->
					let (e_attrs, _) = get_attributes_from_expression [] e in
					let c_attrs =
						List.map
							(fun (cid, c) -> (cid, c, get_output_attributes c []))
							children in
					let c_attrs_used =
						List.map
							(fun (cid, c, attrs) ->
								(cid, c,
									(List.filter
										(fun a -> match a with 
											| `Qualified (rid,_) -> rid = cid
											| `Unqualified b -> List.mem a attrs)
										e_attrs)))
							c_attrs in
					let c_used =
						List.filter
							(fun (cid, c, attrs_used) ->
								match attrs_used with [] -> false | _ -> true)
							c_attrs_used
					in
						print_endline ("Found "^(string_of_int (List.length c_used))^" used children");
						List.fold_left
							(fun acc (cid, c, c_attrs) ->
									get_defining_base_relations
										c (rename_attributes c_attrs cid) acc)
							acc c_used)
				acc attr_exprs
	in
	print_endline ("Visiting "^(string_of_plan_node node));
	match node with
		| `Relation r
		| `TupleRelation r ->
			if List.mem_assoc node acc then
				let running_attrs = List.assoc node acc in
				let uniq_attrs =
					List.fold_left
						(fun acc2 el ->
							match el with
								| `Qualified (_,b) | `Unqualified b ->
									if (List.mem b acc2) then acc2 else b::acc2)
						running_attrs unqualified_attrs
				in
					(node, uniq_attrs)::(List.remove_assoc node acc)
			else
				(node,
					(List.map
						(fun a -> match a with | `Unqualified b -> b)
						unqualified_attrs))::acc

		| `Operator (`Select _, children) ->
			List.fold_left
				(fun acc (cid, c) ->
					get_defining_base_relations c (rename_attributes attrs cid) acc)
				acc children

		| `Operator (`Project(pparams), children) ->
			chase_attrs pparams.pattributes children

		| `Operator (`Join(jparams), children) ->
			chase_attrs jparams.jattributes children

		| `Operator (`Cross(cparams), children) ->
			chase_attrs cparams.cattributes children

		| `Operator (`Aggregate(aparams), children) ->
			if List.mem aparams.out attrs then
				let e_attrs =
					get_attributes_from_aggregate_expression aparams.aggregate []
				in
					List.fold_left
						(fun acc (cid, c) ->
							get_defining_base_relations c
								(rename_attributes e_attrs cid) acc)
						acc children
			else
				acc

		| `Operator (`Topk(_), children) ->
			raise (RuleException ("Topk not supported yet."))


(* Rule 1 *)

let rule_join_cross join_params (in1_id, in1_p) (in2_id, in2_p) child_maps =
	(* For both branches
	  -- find attributes in conjuncts coming from other branch
	  -- find all base relations contributing to these attributes
	  -- check if all base relations are tuple relations
		-- for the branch containing a tuple rel
		  (assume only one branch has this, needs extension when
			 same relation appears multiple times in subquery):
			++ bind predicate attributes as necessary
			++ trim tuple relation
			++ apply selection with bound attributes to other branch
	*)

	let (pred_attrs, _) =
		get_attributes_from_bool_expression join_params.jpredicate [] in

	print_endline ("Pred attrs:\n"^
		(List.fold_left
			(fun acc id -> acc^","^(string_of_attribute_identifier id))
			"" pred_attrs));

	let check_all_tuple_rel p p_id =
		let out_attrs = get_output_attributes p [] in
		let attrs_used = 
			List.filter
				(fun attr ->
					List.exists
						(fun pa -> match (attr, pa) with
							| (`Qualified (_, oa), `Qualified (id,b))
							| (`Unqualified oa, `Qualified (id,b))
								-> id = p_id && oa = b
							| (`Unqualified oa, `Unqualified b)
							| (`Qualified (_, oa), `Unqualified b) -> oa = b)
						pred_attrs)
				out_attrs in

		print_endline ("Out attrs:\n"^
			(List.fold_left
				(fun acc id -> acc^","^(string_of_attribute_identifier id))
				"" out_attrs));
	
		print_endline ("Attrs used:\n"^
			(List.fold_left
				(fun acc id -> acc^","^(string_of_attribute_identifier id))
				"" attrs_used));

		(* (rel * attr list) list *)
		let defining_rels = get_defining_base_relations p attrs_used [] in

		print_endline ("# Defn rels: "^
			(string_of_int (List.length defining_rels)));
		List.iter		
			(fun (r,_) -> print_endline ("Check TR: "^(string_of_plan_node r)))
			defining_rels;

		let all_tuple_rel =
			List.for_all
				(fun (r, _) ->
					match r with
						| `TupleRelation _ ->
							print_endline ("Found TR: "^(string_of_plan_node r));
							true
						| _ -> false)
				defining_rels
		in
			begin
				if all_tuple_rel then
					print_endline ("Found All TR: "^(string_of_plan_node p))
				else ()
			end;
			(all_tuple_rel, out_attrs, attrs_used, defining_rels)
	in

	let rewrite_predicate r_left attrs_used =
		let bound_vars =
			List.map
				(fun a -> match a with 
					| `Qualified (_,a) | `Unqualified a -> a)
				attrs_used
		in
		let intermediate_rel =
			{name=if r_left then in1_id else in2_id;
			schema=
				(List.map (fun a -> (a, "dummy_type")) bound_vars)}
		in
			(* TODO: filter conjuncts in join predicate referring only
			   to other input *) 
			(*bind_bool_expression join_params.jpredicate [] bound_vars*)
			bind_bool_expression_tuplerel
				join_params.jpredicate [] intermediate_rel
	in
	let rewrite_tuple_relations defining_rels =
		List.map
			(fun (rel, attrs) -> (rel, trim_relation attrs rel))
			defining_rels
	in
	let replace_tuple_relations p old_and_new_trs =
		(*
		List.fold_left
			(fun acc (old_r, new_r) -> splice acc old_r new_r)
			p old_and_new_trs
		*)
		List.fold_left
			(fun (newp, acc) (old_r, new_r) ->
				let (newp2, acc2) = replace newp old_r new_r in
					(newp2, acc@acc2))
			(p, []) old_and_new_trs
	in
	let rewrite_plan r_left attrs_used defining_rels =
		let new_id = gen_rel_id() in
		let (rewrite_bindings, new_pred) = rewrite_predicate r_left attrs_used in
		let merged_bindings = merge_child_bindings child_maps in
		let new_bindings = (merged_bindings@rewrite_bindings) in
		let new_trs = rewrite_tuple_relations defining_rels in
		let new_sp =
			`Operator (`Select({spredicate=new_pred}),
				if r_left then [(in2_id, in2_p)] else [(in1_id, in1_p)]) in
		(* let new_rp = *)
		let (new_rp, new_targets) =
			if r_left then replace_tuple_relations in1_p new_trs
			else replace_tuple_relations in2_p new_trs
		in
		let new_p_children =
			if r_left then
				[(in1_id, new_rp); (new_id, new_sp)]
			else
				[(new_id, new_sp); (in2_id, new_rp)] in
		let new_attributes =
			List.map
				(fun (id, e) ->
					let (_, new_e) =
						rename_expression [] e (if r_left then in2_id else in1_id) new_id
					in
						(id, new_e))
				join_params.jattributes in
		let new_p =
			`Operator (
				`Cross({cattributes=new_attributes}), new_p_children)
		in
		let pushdown_target = if r_left then [in2_p] else [in1_p] in
			Some(new_p, [(new_p, new_bindings)], (new_targets@pushdown_target, [new_sp]))
	in

	let (left_all_tuple_rel, left_out_attrs, left_attrs_used, left_defns) =
		check_all_tuple_rel in1_p in1_id in

	let (right_all_tuple_rel, right_out_attrs, right_attrs_used, right_defns) =
		check_all_tuple_rel in2_p in2_id in

	match (left_all_tuple_rel, right_all_tuple_rel) with
		| (true, true) ->
			raise (RuleException
				("Multiple tuple relations are unsupported for now."))
 
		| (true, false) ->
			rewrite_plan true left_attrs_used left_defns 

		| (false, true) ->
			rewrite_plan false right_attrs_used right_defns 

		| _ -> None


(*
let old_rule_join_cross
	join_params (in1_id, in1_p) (in2_id, in2_p) child_maps
	=
	let new_metadata r rp extra_attrs =
			let (r_attrs_used, new_pred) =
				bind_bool_expression_tuplerel join_params.jpredicate [] r in
			let new_params = {spredicate=new_pred} in
			let new_cparams = {cattributes=join_params.jattributes} in
			let new_id = gen_rel_id() in
			let new_rp = trim_relation (r_attrs_used@extra_attrs) rp in
				(*
				print_endline ("Old rp: "); print_plan rp;
				print_endline ("New rp: "); print_plan new_rp;
				*)
				(new_params, new_id, new_cparams, new_rp, r_attrs_used)
	in
	let new_plan r_left r rp extra_attrs trim_fn =
		let (new_params, new_id, new_cparams, new_rp, r_attrs_used) =
			new_metadata r rp extra_attrs in
		let merged_bindings = merge_child_bindings child_maps in
		let new_bindings = merged_bindings@r_attrs_used in
		let new_sp_children =
			if r_left then [(in2_id, in2_p)] else [(in1_id, in1_p)] in
		let new_sp =
			`Operator (`Select(new_params), new_sp_children) in
		let new_p_children =
			if r_left then [(trim_fn new_rp); (new_id, new_sp)]
			else [(new_id, new_sp); (trim_fn new_rp)] in
		let new_p =
			`Operator (`Cross(new_cparams), new_p_children)
		in
		let pushdown_target = if r_left then [in2_p] else [in1_p] in
			Some(new_p, [(new_p, new_bindings)], (pushdown_target, [new_sp]))
	in
	match (in1_p, in2_p) with
		| (`TupleRelation r, _) ->
			new_plan true r in1_p [] (fun new_rp -> (in1_id, new_rp))

		| (_, `TupleRelation r) ->
			new_plan false r in2_p [] (fun new_rp -> (in2_id, new_rp)) 

		| (`Operator (`Select(sel_params), [(rid, `TupleRelation r)]), _) ->
			let (unknowns, _) =
				get_unknowns_from_bool_expression sel_params.spredicate []
			in
				new_plan true r (`TupleRelation r)
					(List.map
						(fun a -> match a with
							| `Attribute(`Qualified (_,b))
							| `Attribute(`Unqualified b) | `Variable b -> b)
						unknowns)
					(fun new_rp ->
						(in1_id, `Operator (`Select(sel_params), [(rid, new_rp)])))

		| (_, `Operator (`Select(sel_params), [(rid, `TupleRelation r)])) ->
			let (unknowns, _) =
				get_unknowns_from_bool_expression sel_params.spredicate []
			in
				new_plan false r (`TupleRelation r)
					(List.map
						(fun a -> match a with
							| `Attribute(`Qualified (_,b))
							| `Attribute(`Unqualified b) | `Variable b -> b)
						unknowns)
					(fun new_rp ->
						(in2_id, `Operator (`Select(sel_params), [(rid, new_rp)])))

		| _ -> None
*)


(* Rule 2 *)

let filter_projected_attributes proj_result rel =
	List.fold_left 
		(fun acc (out, expr) ->
			let (attrs_used,_) = get_attributes_from_expression [] expr in
				List.fold_left
					(fun acc attr ->
						match attr with
							| `Qualified (r, a) ->
								if r = rel.name then attr::acc else acc
							| `Unqualified a ->
								if List.exists (fun (id, ty) -> id = a) rel.schema
								then attr::acc
								else acc)
					acc attrs_used)
		[] proj_result

let rule_project_cross project_params cross_children cross_maps =
	match cross_children with
		| [(trid, `TupleRelation r); (oid, other_child)] 
		| [(oid, other_child); (trid, `TupleRelation r)]

		| [(_, `Operator(`Select(_), [(trid, `TupleRelation r)]));
				(oid, other_child)]
 
		| [(oid, other_child);
				(_, `Operator(`Select(_), [(trid, `TupleRelation r)]))] ->

			let project_attrs_used =
				List.map
					(fun attr_id -> match attr_id with
						| `Qualified (_,a) | `Unqualified a -> a)
				(filter_projected_attributes project_params.pattributes r)
			in
			let r_used =
				List.exists
					(fun (id, ty) -> List.mem id project_attrs_used)
					r.schema
			in
				if r_used then None
				else 
					(* TODO: make this more robust *)
					(* TODO: think whether we need to distinguish from which branch
					  we're deleting bindings *)
					let new_bindings = 
						let merged_bindings = merge_child_bindings cross_maps in
							List.filter
								(fun v -> List.for_all (fun (id, ty) -> id != v) r.schema)
								merged_bindings
					in
						Some (other_child, [(other_child, new_bindings)], ([], [other_child]))

		| _ -> None


(* Rule 3 *)

let build_binding_predicate tuple_rel cross_params child_maps =
	let cross_attrs_used =
		filter_projected_attributes cross_params.cattributes tuple_rel
	in
	let new_bindings =
		List.fold_left
			(fun acc ca -> match ca with
				| `Qualified (_,a) | `Unqualified a -> a::acc)
			(merge_child_bindings child_maps) cross_attrs_used
	in
		match cross_attrs_used with
			| [] -> (`BTerm(`True), new_bindings)
			| ca::t ->
				let ca_var = `Variable(
					match ca with | `Qualified (_,a) | `Unqualified a -> a)
				in
				let new_pred =
					List.fold_left
						(fun acc attr ->
							let attr_var = `Variable(
								match attr with | `Qualified (_,a) | `Unqualified a -> a)
							in
								`And (acc, (`BTerm (`EQ(
									`ETerm(`Attribute(attr)), `ETerm(attr_var)))))
						)
						(`BTerm (`EQ(
							`ETerm(`Attribute(ca)), `ETerm(ca_var))))
						t
				in
					(new_pred, new_bindings)


let rule_bind_cross agg_params p_id p params children child_maps =
	match children with
		| [(trid, `TupleRelation r); (oid, other)]
		| [(oid, other); (trid, `TupleRelation r)] ->
			let new_sym = gen_rel_id() in
			let (new_predicate, new_bindings) =
				build_binding_predicate r params child_maps
			in
			let new_sp =
				`Operator (`Select ({spredicate=new_predicate}), [(p_id, p)])
			in
			let new_agg_params =
				{aggregate=
					(rename_aggregate_expression agg_params.aggregate [] p_id new_sym);
					out=agg_params.out} in
			let new_p =
				`Operator (`Aggregate(new_agg_params), [(new_sym, new_sp)])
			in 
				Some (new_p, [(new_p, new_bindings)], ([], [new_sp]))
		| [] -> raise (PlanException ("No children found for cross"))
		| [r] -> raise (PlanException ("Cross operator has a single input"))
		| _ -> None


(* Rule 4 *)

let rule_bind_agg_args agg_params sel_params (sel_id, sel_children) child_maps =
	(* Find all aggregate args that are variables in sel_params *)
	let agg_attrs_used =
		match agg_params.aggregate with
			| `Sum e | `Count e | `Min e | `Max e | `Average e
			| `Median e | `StdDev e ->
				let (attrs_used, _) = get_attributes_from_expression [] e in
					attrs_used
			| `Function (id, args) ->
				List.fold_left
					(fun acc e ->
						let (attrs_used, _) = get_attributes_from_expression [] e in
							acc@attrs_used)
					[] args
	in

	(* Find attrs that are not bound inside selection predicate *)
	(* Note we assume type checking has been done, so unqualified
	   attrs are assumed to be unique *)
	let (bound_vars , _) =
		get_variables_from_bool_expression sel_params.spredicate [] in
	let projection_attrs =
		List.filter
			(fun attr ->
				match attr with | `Qualified(_,a) | `Unqualified a ->
					not(List.mem a bound_vars))
			agg_attrs_used
	in

	if projection_attrs = agg_attrs_used then
		None
	else
		begin
			(* Create new plan *)
			let new_id = gen_rel_id() in
			let child_id = match sel_children with
				| [(c, _)] -> c
				| _ -> raise
					(PlanException ("Invalid children for selection "^sel_id))
			in
			let new_agg_expr =
				rename_aggregate_expression 
					(bind_aggregate_expression agg_params bound_vars)
					[] sel_id new_id in
			let new_proj_result = 
				List.map
					(fun attr ->  
						match attr with
							| `Qualified (_,a) ->
								(`Unqualified a, `ETerm(`Attribute(`Qualified(child_id, a))))
							| `Unqualified a ->
								(`Unqualified a, `ETerm(`Attribute(`Unqualified a))))
					projection_attrs
			in
			let new_p =
				`Operator (`Aggregate ({aggregate=new_agg_expr; out=agg_params.out}),
					[(new_id, `Operator(
						`Project({pattributes=new_proj_result}), sel_children))])
			in
					
			(* Compute new bindings *)
			let new_bindings = merge_child_bindings child_maps in
			let (_, next_rewrite) = List.hd sel_children in
				Some (new_p, [(new_p, new_bindings)], ([], [next_rewrite]))
		end


(* Rule 5 *)

let rule_agg_cross_project
	agg_params cross_id cross_params cross_children child_maps 
	=
	(* check agg attributes are taken from non-tuple relation *)
	let rule_aux tuple_rel =
		(* get agg attributes *)
		let agg_attrs_used = 
			match agg_params.aggregate with
			| `Sum e | `Count e | `Min e | `Max e | `Average e
			| `Median e | `StdDev e ->
				let (attrs_used, _) = get_attributes_from_expression [] e in
					attrs_used
			| `Function (id, args) ->
				List.fold_left
					(fun acc e ->
						let (attrs_used, _) = get_attributes_from_expression [] e in
							acc@attrs_used)
					[] args
		in

		(* get tuple relation attributes *)
		let tr_attrs = List.map (fun (id, ty) -> id) tuple_rel.schema in

		(* check for empty intersection between agg and tuple rel attrs *)
		let agg_only_attrs =
			List.filter
				(fun attr -> match attr with 
					| `Qualified (_,a) | `Unqualified a -> not(List.mem a tr_attrs))
				agg_attrs_used
		in

		(* build project result *)
		if (List.length agg_only_attrs) < (List.length agg_attrs_used) then
			None
		else
			let new_id = gen_rel_id() in
			let proj_result =
				List.map (fun a -> (a, `ETerm(`Attribute a))) agg_only_attrs in
			let new_agg_expr =
				rename_aggregate_expression agg_params.aggregate [] cross_id new_id
			in
				Some (new_id, proj_result, new_agg_expr)
	in
	
	match cross_children with
		| [(other_cid, other); (rid, `TupleRelation r)]
		| [(rid, `TupleRelation r); (other_cid, other)]

		| [(other_cid, other);
				(_, `Operator(`Select(_), [(rid, `TupleRelation r)]))]

		| [(_, `Operator(`Select(_), [(rid, `TupleRelation r)]));
				(other_cid, other)] ->

			let new_bindings = merge_child_bindings child_maps in 
			let apply_rule = rule_aux r in
				begin
					match apply_rule with
						| None -> None
						| Some(new_id, proj_result, new_agg_expr) ->
							let new_agg_params =
								{aggregate=new_agg_expr;out=agg_params.out} in
							let cross_sp =
								`Operator(`Cross(cross_params), cross_children) in
							let new_p =
								`Operator (`Aggregate(new_agg_params),
									[(new_id, `Operator(`Project({pattributes=proj_result}),
										[(cross_id, cross_sp)]))])
							in
								Some(new_p, [(new_p, new_bindings)], ([cross_sp], []))
					end

		| _ -> None 


(* Rule 6 *)

(*
let rule_separate_sum_cross agg_params cross_params cross_children child_maps =
	let contained attrs1 attrs2 attr2_id = 
		List.for_all
			(fun el1 -> match el1 with
				| `Qualified (rid,_) -> rid = attr2_id
				| `Unqualified a -> List.mem a attrs2)
			attrs1
	in
	(* Check if aggregate expression is product, and get attributes in each branch *)
	match agg_params.aggregate with
		| `Sum (`Product (l,r)) ->
			begin
				(* Check if both sets of branch attributes come wholly from a cross child. *)
				let l_attrs = get_attributes_from_expression [] l in
				let r_attrs = get_attributes_from_expression [] r in

				let (in1_out_attrs, in2_out_attrs, (in1_id, in1_p), (in2_id, in2_p)) =
					match cross_children with
						| [(in1_id, in1_p); (in2_id, in2_p)] ->
							(get_output_attributes in1_p [],
								get_output_attributes in2_p [],
								(in1_id, in1_p), (in2_id, in2_p)) 
						| _ -> raise (RuleException "Invalid cross operator.")
				in
				let (l_contained_1, l_contained_2) =
					if (contained l_attrs in1_out_attrs) then (true, false)
					else (false, contained l_attrs in2_out_attrs)
				in 
				let (r_contained_1, r_contained_2) =
					if l_contained_1 || l_contained_2 then
						if l_contained_1 then (false, contained r_attrs in2_out_attrs)
						else (contained r_attrs in1_out_attrs, false)
					else
						(false, false)
				in

				(* Create the new map expression *)
				let rewrite_plan (lid, l_child) (rid,r_child) =
					let l_agg_params =
						{out=new_attr_id();aggregate=`Sum(l)} in
					let l_plan =
						`Operator (`Aggregate(l_agg_params), [(lid, lchild)])
					in
					let r_agg_params =
						{out=new_attr_id();aggregate=`Sum(r)} in
					let r_plan =
						`Operator (`Aggregate(r_agg_params), [(rid, rchild)])
					in
					let new_p = `Product (`Plan l_plan, `Plan r_plan) in
						Some(new_p, [], [])
				in
					match (l_contained_1, r_contained_2, l_contained_2, r_contained_1) with
						| (true, true, false, false) ->
							rewrite_plan (in1_id, in1_p) (in2_id, in2_p)
						| (false, false, true, true) ->
							rewrite_plan (in2_id, in2_p) (in1_id, in1_p)
						| _ -> None 
			end

		| _ -> None
*)

(* Additional rules *)

(* Selection pushdown *)

let rule_predicate_pushdown sel_params jc_id sel_child child_maps =
	let get_num_common_attrs out_attrs unknowns =
		List.fold_left
			(fun acc unk ->
				match unk with
					| `Attribute a ->
						if (List.mem a out_attrs) then acc+1 else acc 
					| `Variable v ->
						if ((List.mem (`Unqualified v) out_attrs) ||
									(List.mem (`Qualified (jc_id, v)) out_attrs))
						then acc+1 else acc)
			0 unknowns
	in
	let aux jc_children = 
		match jc_children with
		| [(in1_id, in1_p); (in2_id, in2_p)] ->
			let in1_out_attrs =
				rename_attributes (get_output_attributes in1_p []) jc_id in
			let in2_out_attrs =
				rename_attributes (get_output_attributes in2_p []) jc_id in
			let (attrs_used, _) =
				get_unknowns_from_bool_expression sel_params.spredicate []
			in
			let in1_common = get_num_common_attrs in1_out_attrs attrs_used in
			let in2_common = get_num_common_attrs in2_out_attrs attrs_used in
			let new_bindings = merge_child_bindings child_maps in
				begin
					match (in1_common, in2_common) with
						| (0, 0) ->
							raise (PlanException ("Found unnecessary predicate"))

						| (0, _) ->
							let new_sparams =
								let (_, new_pred) =
									rename_bool_expression sel_params.spredicate [] jc_id in2_id
								in
									{spredicate=new_pred} in
							let new_subp =
								`Operator(`Select(new_sparams), [(in2_id, in2_p)])
							in
								Some ([(in1_id, in1_p); (in2_id, new_subp)],
									new_bindings, ([in2_p], []))

						| (_, 0) ->
							let new_sparams =
								let (_, new_pred) =
									rename_bool_expression sel_params.spredicate [] jc_id in1_id
								in
									{spredicate=new_pred} in
							let new_subp =
								`Operator(`Select(new_sparams), [(in1_id, in1_p)])
							in
								Some ([(in1_id, new_subp); (in1_id, in1_p)],
									new_bindings, ([in1_p], []))

						| _ -> None 
				end

		| _ -> raise (PlanException ("Invalid join inputs for "^jc_id))
	in
	match sel_child with
		| `Operator(`Join(join_params), jchildren) ->
			begin
				match aux jchildren with
					| None -> None
					| Some(new_children, new_bindings, new_targets) ->
						let new_p = `Operator(`Join(join_params), new_children) in
							Some (new_p, [(new_p, new_bindings)], new_targets)
			end

		| `Operator(`Cross(cross_params), cchildren) ->
			begin
				match aux cchildren with
					| None -> None
					| Some(new_children, new_bindings, new_targets) ->
						let new_p = `Operator(`Cross(cross_params), new_children) in
							Some (new_p, [(new_p, new_bindings)], new_targets)
			end


(* Selection eliminations on tuple relations and variable trimming
		for tuple relations *)
let rec get_variable_constraints b_expr acc =
	match b_expr with
		| `BTerm (`EQ (`ETerm(`Attribute(`Qualified (_,a))), `ETerm(`Variable v)))
 		| `BTerm (`EQ (`ETerm(`Variable v), `ETerm(`Attribute(`Qualified (_,a)))))
		| `BTerm (`EQ (`ETerm(`Attribute(`Unqualified a)), `ETerm(`Variable v)))
 		| `BTerm (`EQ (`ETerm(`Variable v), `ETerm(`Attribute(`Unqualified a))))
			->
				(v,a)::acc
				
		| `BTerm _ -> acc

		| `And(l,r) | `Or(l,r) ->
				get_variable_constraints r (get_variable_constraints l acc)
		| `Not(e) -> get_variable_constraints e acc

let rule_select_tuplerel sel_params tuple_rel child_maps =
	let (attrs_used, _) =
		get_attributes_from_bool_expression sel_params.spredicate []
	in
		if (List.length attrs_used) = 0 then
			let new_p = `TupleRelation tuple_rel in 
			Some(new_p, [(new_p, [])], ([],[new_p]))
		else
			begin
				let constraint_attrs =
					List.map
						(fun (v,a) -> a)
						(get_variable_constraints sel_params.spredicate [])
				in
				if List.length constraint_attrs > 0 then
					let new_p = trim_relation constraint_attrs (`TupleRelation tuple_rel) in
						Some(new_p, [(new_p, [])], ([], [new_p]))
				else
					None
			end
			
let rule_cross_tuplerel children child_maps =
	match children with 
		| [(trid, `TupleRelation r); (oid, other)] ->
			let other_bindings = List.nth child_maps 1 in
				if (List.length r.schema) = 0 then
					Some(other, other_bindings, ([], [other]))
				else None
 
		| [(oid, other); (trid, `TupleRelation r)] ->
			let other_bindings = List.nth child_maps 0 in
				if (List.length r.schema) = 0 then
					Some(other, other_bindings, ([], [other]))
				else None
				
		| _ -> None