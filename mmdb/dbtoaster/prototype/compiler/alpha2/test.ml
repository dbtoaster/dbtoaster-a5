open Algebra
open Compile


let tv = 
	`MapAggregate(`Sum,
		      `METerm(`Attribute(`Qualified("B", "V3"))),
		      `Relation("B",[("P3", "int"); ("V3", "int")]))

let sv1_pred =
  `BTerm(`GT(
	 `ETerm(`Attribute(`Qualified("B", "P1"))),
	 `ETerm(`Attribute(`Qualified("B", "P2")))))

let select_sv1 =
  `Select(sv1_pred, `Relation("B",[("P1", "int"); ("V1", "int")]))

let k_sv0 = `Product(`METerm(`Float(0.25)), tv)
let sv1 = `MapAggregate(`Sum,
	`METerm(`Attribute(`Qualified("B","V1"))), select_sv1)

let m_p2 = `Sum(k_sv0, sv1)

let vwap =
  `MapAggregate(`Sum,
		`Product(`METerm(`Attribute(`Qualified("B", "P2"))),
			 `METerm(`Attribute(`Qualified("B", "V2")))),
		`Select(`BTerm(`MLT(m_p2)),
			`Relation("B", [("P2", "int"); ("V2","int")])))

let delta = 
  `Insert ("B", [("p", "int"); ("v", "int")])
;;

(* string_of tests *)
let print_test_type tt =
	print_endline ((String.make 50 '-')^"\n\n"^tt^" tests\n\n"^(String.make 50 '-'));;

print_test_type "string_of";
print_endline (string_of_map_expression vwap);

(* accessor tests *)
print_test_type "accessor (parent)";
let p = parent vwap (`MapExpression tv) in
	print_endline "Parent (total_vol): ";
	print_endline ((string_of_accessor_element_opt p)^"\n");;

let select_sv1_p = parent vwap (`Plan select_sv1) in
	print_endline "Parent (select_sv1): ";
	print_endline ((string_of_accessor_element_opt select_sv1_p)^"\n");;

let vwap_p = parent vwap (`MapExpression vwap) in
	print_endline "Parent (vwap): ";
	print_endline ((string_of_accessor_element_opt vwap_p)^"\n");;



print_test_type "accessor (child)";
let vwap_ch = children (`MapExpression vwap) in
	print_endline "Children (vwap): ";
	List.iter
		(fun c -> print_endline ((string_of_accessor_element c)^"\n"))
		vwap_ch;;

(* Note: nested map expressions in selects don't include the boolean
 * expression when accessing children,
 * e.g. the child is m_expr not `BTerm(`MEQ(m_expr)) *)
let select_sv1_ch = children (`Plan select_sv1) in
	print_endline "Children (select_sv1): ";
	List.iter
		(fun c -> print_endline ((string_of_accessor_element c)^"\n"))
		select_sv1_ch;;



print_test_type "splice";
let new_tv =
	`MapAggregate(`Sum,
		`METerm(`Variable("V4")),
		`Relation("B4",[("P4", "int"); ("V4", "int")]))
in
let new_m_expr = splice vwap (`MapExpression tv) (`MapExpression new_tv)
in
	print_endline "Spliced vwap (1):\n";
	print_endline ((string_of_map_expression new_m_expr)^"\n");;

let new_select_sv1 =
	`Select(
		`BTerm(`MLT(`Product(`METerm(`Variable("P1")), `METerm(`Variable("P2"))))),
		`Relation("B1",[("P1", "int"); ("V1", "int")]))
in
let new_m_expr2 = splice vwap (`Plan select_sv1) (`Plan new_select_sv1)
in
	print_endline "Spliced vwap (2):\n";
	print_endline ((string_of_map_expression new_m_expr2)^"\n");;



(* TODO: support relation aliasing, e.g. for self-joins *)
print_test_type "get_base_relations";
let br = get_base_relations vwap in
	print_endline "vwap base relations:\n";
	List.iter
		(fun r ->
			print_endline
				("Relation "^(match r with
					| `Relation(n,f) -> n | `TupleRelation (n,f) -> n)))
		br;;




print_test_type "monotonicity";
let tv_mon = compute_monotonicity (`Qualified("B2", "P2")) Inc tv in
	print_endline "tv(p2) monotonicity:";
	print_endline ((string_of_monotonicity tv_mon)^"\n");;

let select_sv1_mon =
	compute_plan_monotonicity (`Qualified ("B2", "P2")) Inc select_sv1
in
	print_endline "ssv1(p2):";
	print_endline ((string_of_plan_monotonicity select_sv1_mon)^"\n");;

let k_sv0_mon = compute_monotonicity (`Qualified("B2", "P2")) Inc k_sv0
in
	print_endline "k_sv0(p2) monotonicity:";
	print_endline ((string_of_monotonicity k_sv0_mon)^"\n");;


let sv1_mon = compute_monotonicity (`Qualified("B2", "P2")) Inc sv1
in
	print_endline "sv1(p2) monotonicity:";
	print_endline ((string_of_monotonicity sv1_mon)^"\n");;


let mp2_p2_mon = compute_monotonicity (`Qualified("B2", "P2")) Inc m_p2
in
	print_endline "m(p2) monotonicity:";
	print_endline ((string_of_monotonicity mp2_p2_mon)^"\n");;

let p2_mon = compute_monotonicity (`Qualified("B2", "P2")) Inc vwap
in
	print_endline "vwap(p2) monotonicity:";
	print_endline ((string_of_monotonicity p2_mon)^"\n");;




print_test_type "get_bound_attributes";
let ssv1_ba = get_bound_attributes select_sv1 in
	print_endline "get_bound_attributes(ssv1)";
	List.iter
		(fun aid -> print_endline (string_of_attribute_identifier aid))
		ssv1_ba;;




print_test_type "add_map_expression_bindings";
let proj_attrs =
	[(`Qualified("B2", "P2"), `ETerm(`Variable("P2")));
	 (`Qualified("B2", "V2"), `ETerm(`Variable("V2")))]
in
let bound_m_p2 = add_map_expression_bindings m_p2 proj_attrs in
	print_endline "add_me_bind(m_p2):\n";
	print_endline ((string_of_map_expression bound_m_p2)^"\n");;


print_test_type "add_predicate_bindings";
let proj_attrs2 =
	[(`Qualified("B2", "P2"), `ETerm(`Variable("P2")));
	 (`Qualified("B2", "V2"), `ETerm(`Variable("V2")))]
in
let bound_sv1_pred = add_predicate_bindings sv1_pred proj_attrs2 in
	print_endline "add_pred_bind(sv1_pred):\n";
	print_endline ((string_of_bool_expression bound_sv1_pred)^"\n");




print_test_type "get_unbound_attributes";
let m_p2_uba = get_unbound_attributes_from_map_expression m_p2 false in
	print_endline "get_unbound_attributes(m_p2):\n";
	List.iter (fun uba -> print_endline (string_of_attribute_identifier uba)) m_p2_uba;;

let vwap_uba = get_unbound_attributes_from_map_expression vwap false in
	print_endline "get_unbound_attributes(vwap):\n";
	List.iter (fun uba -> print_endline (string_of_attribute_identifier uba)) vwap_uba;;




print_test_type "push_delta";
let delta_m_p2 = push_delta (`Delta (`Insert("B", [("p", "int"); ("v", "int")]), m_p2)) in
	print_endline "Delta(m_p2):\n";
	print_endline ((string_of_map_expression delta_m_p2)^"\n"); 

	print_test_type "simplify";
	print_endline (string_of_map_expression (simplify_map_expr_constants delta_m_p2));; 




print_test_type "extract_map_expr_bindings";
let (vwap_e, vwap_bindings) = extract_map_expr_bindings vwap in
	print_endline "extract_bindings(vwap):";
	print_endline ((string_of_map_expression vwap_e)^"\n");
	List.iter
		(fun x ->
			print_endline (
				match x with
					| `BindExpr(sym, expr) -> sym^"{"^(string_of_expression expr)^"}"
					| `BindBoolExpr(sym, b_expr) -> sym^"{"^(string_of_bool_expression b_expr)^"}"
					| `BindMapExpr(sym, m_expr) -> sym^"{"^(string_of_map_expression m_expr)^"}"))
		vwap_bindings;

	print_test_type "apply_recompute_rules";
	let code_exprs =
		let bound_map_exprs =
			List.fold_left
				(fun acc b -> match b with | `BindMapExpr (v,d) -> d::acc | _ -> acc)
				[] vwap_bindings
		in 
			List.map 
				(fun x -> apply_recompute_rules x delta) (vwap_e::bound_map_exprs)
	in
		print_endline "apply_recompute_rules(vwap):";
		List.iter
			(fun ce -> print_endline ((string_of_map_expression ce)^"\n"))
			code_exprs;
			
		print_test_type "compute_new_map_expression(vwap):";
		let new_code_exprs =
			List.map (fun c -> compute_new_map_expression c (Some New)) code_exprs
		in
		List.iter
			(fun ce -> print_endline ((string_of_map_expression ce)^"\n"))
			new_code_exprs;

		print_test_type "apply_delta_rules";
		let delta_code_exprs =
			List.map
				(fun ce -> apply_delta_rules ce (`Insert ("B", [("p", "int"); ("v", "int")])) (Some New))
				new_code_exprs
		in
		print_endline "apply_delta_rules(vwap):";
		let handlers = List.map simplify_map_expr_constants delta_code_exprs in 
		List.iter
			(fun h -> print_endline ((string_of_map_expression h)^"\n"))
			handlers;

		print_test_type "simplify";
		let simplified_handlers =
			List.map
				(fun h -> 
					let br = List.map (fun x -> `Plan x) (get_bound_relations h) in
						print_test_type "simplify";
						simplify h br)
				handlers
		in
			List.iter
				(fun h -> print_endline ((string_of_map_expression h)^"\n"))
				simplified_handlers;;

print_test_type "compile_target";

let (handler, bindings) = compile_target vwap (`Insert ("B", [("p", "int"); ("v", "int")])) in
	let bound_map_exprs =
		List.map
			(fun b -> match b with | `BindMapExpr(v,e) -> e | _ -> raise InvalidExpression)
			(List.filter
				(fun b -> match b with | `BindMapExpr(v,e) -> true | _ -> false)
				bindings)
	in
		List.iter
			(fun h -> print_endline ((indented_string_of_map_expression h 0)^"\n"))
			(handler::bound_map_exprs);
			
		print_test_type "generate_code";
		let (global_decls, handler_code) = generate_code handler bindings (`Insert ("B", [("p", "int"); ("v", "int")])) in
			print_endline "generate_code(vwap):";
			List.iter (fun x -> print_endline (indented_string_of_code_expression x)) global_decls;
			print_endline (indented_string_of_code_expression handler_code);;


print_test_type "compile_code";
compile_code vwap (`Insert ("B", [("p", "int"); ("v", "int")])) "vwap.cc";;

print_test_type "treeml_of_map_expression";
let treeml_out_chan = open_out "vwap.tml" in
    output_string treeml_out_chan (treeml_string_of_map_expression vwap);;
