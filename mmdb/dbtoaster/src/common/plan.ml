(*
	DBToaster AST/query plan representation.
	Author: Yanif Ahmad
	Date: 3/4/2009. 
*)

exception PlanException of string

type identifier = string

type column_name = [
	`Qualified of identifier * identifier
	| `Unqualified of identifier ]

type function_name = identifier

type attribute_identifier = column_name
type variable_identifier = identifier
type function_identifier = function_name
type relation_identifier = identifier
type type_identifier = identifier
type field_identifier = identifier

type field = field_identifier * type_identifier 
type relation = { name: relation_identifier; schema: field list }

type eterm = [
	`Int of int
	| `Float of float
	| `String of string
	| `Long of int64
	| `Attribute of attribute_identifier 
	| `Variable of variable_identifier ]

type expression = [
	| `ETerm of eterm
	| `UnaryMinus of expression
	| `Sum of expression * expression
	| `Product of expression * expression
	| `Minus of expression * expression
	| `Divide of expression * expression
	| `Function of function_identifier * (expression list) ]

type bterm = [ `True | `False 
	| `LT of expression * expression
	| `LE of expression * expression
	| `GT of expression * expression
	| `GE of expression * expression
	| `EQ of expression * expression
	| `NE of expression * expression ]

type boolean_expression = [
	| `BTerm of bterm
	| `Not of boolean_expression
	| `And of boolean_expression * boolean_expression
	| `Or of boolean_expression * boolean_expression ]

type aggregate_expression = [
	`Sum of expression
	| `Count of expression
	| `Min of expression
	| `Max of expression
	| `Average of expression
	| `Median of expression
	| `StdDev of expression
	| `Function of function_identifier * (expression list) ]

type project_result = (attribute_identifier * expression) list

type select_parameters = { spredicate: boolean_expression }
type project_parameters = { pattributes: project_result }
type join_parameters = { jpredicate: boolean_expression; jattributes: project_result }
type cross_parameters = { cattributes: project_result }
type aggregate_parameters =
	{ out: attribute_identifier; aggregate: aggregate_expression }
type topk_parameters = { limit: expression }

type operator = [
	`Select of select_parameters
	| `Project of project_parameters
	| `Join of join_parameters
	| `Cross of cross_parameters
	| `Aggregate of aggregate_parameters 
	| `Topk of topk_parameters ]

type plan = [
	| `TupleRelation of relation
	| `Relation of relation
	| `Operator of operator * ((relation_identifier * plan) list) ]

(* TODO: extend to support nested aggregates, which may temporarily
  -- want relational operators around map expressions *)
type map_expression = [
	| `ScalarResult of map_expression
	| `SetResult of map_expression
	| `Sum of map_expression * map_expression
	| `Product of map_expression * map_expression ]


(* Plan transformation helpers *)
let rec map_expr fn expr =
	match expr with 
		| `UnaryMinus e ->
			let e2 = map_expr fn e in (fn (`UnaryMinus e2))

		| `Sum (l,r) ->
			let e2 = (map_expr fn l, map_expr fn r) in
				(fn (`Sum e2))

		| `Product (l,r) ->
			let e2 = (map_expr fn l, map_expr fn r) in
				(fn (`Product e2))

		| `Minus (l,r) -> 
			let e2 = (map_expr fn l, map_expr fn r) in
				(fn (`Minus e2))

		| `Divide (l,r) -> 
			let e2 = (map_expr fn l, map_expr fn r) in
				(fn (`Divide e2))

		| `Function (id, args) ->
			let args2 = List.map (fun e -> map_expr fn e) args in
				(fn (`Function (id, args2)))

		| (`ETerm e) as t -> (fn t)

let rec fold_map_expr fn acc expr =
	match expr with 
		| `UnaryMinus e ->
			let (acc2, e2) = fold_map_expr fn acc e in (fn acc2 (`UnaryMinus e2))

		| `Sum (l,r) ->
			let (acc2, l2) = fold_map_expr fn acc l in
			let (acc3, r2) = fold_map_expr fn acc2 r in
				(fn acc3 (`Sum (l2, r2)))

		| `Product (l,r) ->
			let (acc2, l2) = fold_map_expr fn acc l in
			let (acc3, r2) = fold_map_expr fn acc2 r in
				(fn acc3 (`Product (l2, r2)))

		| `Minus (l,r) -> 
			let (acc2, l2) = fold_map_expr fn acc l in
			let (acc3, r2) = fold_map_expr fn acc2 r in
				(fn acc3 (`Minus (l2, r2)))

		| `Divide (l,r) -> 
			let (acc2, l2) = fold_map_expr fn acc l in
			let (acc3, r2) = fold_map_expr fn acc2 r in
				(fn acc3 (`Divide (l2, r2)))

		| `Function (id, args) ->
			let (acc2, args2) =
				List.fold_left
					(fun (acc, eacc) e ->
						let (acc2, e2) = fold_map_expr fn acc e in (acc2, eacc@[e2]))
					(acc, []) args
			in
				(fn acc2 (`Function (id, args2)))

		| (`ETerm e) as t -> (fn acc t)

let rec map_bool_expr fn (bool_expr : boolean_expression) =
	match bool_expr with
		| `Not e ->
			let e2 = map_bool_expr fn e in (fn (`Not e2))

		| `And (l,r) ->
			let l2 = map_bool_expr fn l in 
			let r2 = map_bool_expr fn r in
				(fn (`And (l2,r2)))

		| `Or (l,r) ->
			let l2 = map_bool_expr fn l in 
			let r2 = map_bool_expr fn r in
				(fn (`Or (l2,r2)))

		| (`BTerm b) as t -> (fn t)  
	
let rec fold_map_bool_expr fn acc bool_expr =
	match bool_expr with
		| `Not e ->
			let (acc2, e2) = fold_map_bool_expr fn acc e in
				(fn acc2 (`Not e2))

		| `And (l,r) ->
			let (acc2,l2) = fold_map_bool_expr fn acc l in
			let (acc3,r2) = fold_map_bool_expr fn acc2 r in
				(fn acc3 (`And (l2,r2)))
		
		| `Or (l,r) ->
			let (acc2,l2) = fold_map_bool_expr fn acc l in
			let (acc3,r2) = fold_map_bool_expr fn acc2 r in
				(fn acc3 (`Or (l2,r2)))

		| (`BTerm b) as t -> (fn acc t)

(* Pretty printing *)
let string_of_attribute_identifier id =
	match id with 
		| `Qualified(r,a) -> r^"."^a
		| `Unqualified a -> a

let string_of_schema schema =
	"{"^
	(List.fold_left
		(fun acc (id, ty) ->
			(if (String.length acc) = 0 then "" else acc^",")^
			id^":"^ty) "" schema) ^"}"

let string_of_expr_terminal eterm =
	match eterm with 
		| `Int i -> string_of_int i
		| `Float f -> string_of_float f
		| `Long l -> Int64.to_string l
		| `String s -> "'"^s^"'"
		| `Attribute (`Qualified(r,a)) -> r^"."^a
		| `Attribute (`Unqualified a) -> a
		| `Variable v -> "Var("^v^")"

let rec string_of_expression expr =
	match expr with
		| `ETerm e -> string_of_expr_terminal e
		| `UnaryMinus e -> "-"^(string_of_expression e)
		| `Sum (l,r) -> (string_of_expression l)^" + "^(string_of_expression r)
		| `Product(l,r) -> (string_of_expression l)^" * "^(string_of_expression r) 
		| `Minus (l,r) -> (string_of_expression l)^" - "^(string_of_expression r)
		| `Divide (l,r) -> (string_of_expression l)^" / "^(string_of_expression r)
		| `Function (id, args) -> id^"("^
			(List.fold_left
				(fun acc e ->
					(if (String.length acc) = 0 then "" else (acc^","))^
					(string_of_expression e))
					"" args)^")"

let rec string_of_bool_expression b_expr =
	let dispatch_expr l r op =
		(string_of_expression l)^op^(string_of_expression r)
	in
	match b_expr with
		| `BTerm b ->
			begin
				match b with
					| `LT (l,r) -> dispatch_expr l r "<"
					| `LE (l,r) -> dispatch_expr l r "<="
					| `GT (l,r) -> dispatch_expr l r ">"
					| `GE (l,r) -> dispatch_expr l r ">="
					| `EQ (l,r) -> dispatch_expr l r "="
					| `NE (l,r) -> dispatch_expr l r "!="
					| `True -> "true"
					| `False -> "false"
			end
		| `Not (e) -> ("not("^(string_of_bool_expression e)^")")
		| `And (l,r) | `Or (l,r) -> 
			("("^(string_of_bool_expression l)^") and ("^
				(string_of_bool_expression r)^")")

let string_of_aggregate_expression a_expr =
	match a_expr with
		| `Sum e -> "sum("^(string_of_expression e)^")"
		| `Count e -> "count("^(string_of_expression e)^")"
		| `Min e ->  "min("^(string_of_expression e)^")"
		| `Max e ->  "max("^(string_of_expression e)^")"
		| `Average e ->  "avg("^(string_of_expression e)^")"
		| `Median e ->  "median("^(string_of_expression e)^")"
		| `StdDev e ->  "stddev("^(string_of_expression e)^")"
		| `Function (id, args) ->
			(id^"("^
			(List.fold_left
			 (fun acc arg ->
					(if (String.length acc) = 0 then "" else acc^",")^
					(string_of_expression arg))
			 "" args)^")")

let string_of_project_result pr =
	List.fold_left
		(fun acc (aid, e) ->
			(if (String.length acc) = 0 then "" else acc^",")^
			(string_of_attribute_identifier aid)^":"^(string_of_expression e))
		"" pr

let string_of_operator op =
	match op with
		| `Select p -> "Select{"^(string_of_bool_expression p.spredicate)^"}"
		| `Project p -> "Project{"^(string_of_project_result p.pattributes)^"}"
		| `Join p ->
				"Join{"^(string_of_bool_expression p.jpredicate)^"}["^
					(string_of_project_result p.jattributes)^"]"
		| `Cross p -> "Cross{"^(string_of_project_result p.cattributes)^"}"
		| `Aggregate p -> "Aggregate{"^(string_of_aggregate_expression p.aggregate)^"}"
		| `Topk p -> "Topk"


let string_of_plan_node node =
	match node with
		| `TupleRelation r -> "TupleRelation ["^r.name^"]"^(string_of_schema r.schema)
		| `Relation r -> "Relation ["^r.name^"]"^(string_of_schema r.schema)
		| `Operator (n, children) ->
				"Operator ("^(string_of_operator n)^")<"^
				(List.fold_left
					(fun acc (id, _) ->
						if (String.length acc) > 0 then acc ^","^id else id)
					"" children)^">"

let print_plan node =
	let rec print_level p l =
		let prefix = (String.make (l*2) '-')^">" in
		let p_str = prefix ^ string_of_plan_node p in
			begin
				print_endline p_str;
				match p with
					| `Operator (n, children) ->
						List.iter (fun (id, c) -> print_level c (l+1)) children
					| _ -> ()
			end 
	in
	 print_level node 0

let print_compile_tree compile_tree =
	print_endline "Can't print yet..."

let print_code code =
	print_endline "Can't print yet..."
	
let print_maps maps =
	print_endline "Cant print yet..."


(* Plan accessors *)

(* plan -> plan_node *)
let get_plan_node plan =
	match plan with 
		| (`TupleRelation r) as pn -> pn
		| (`Relation r) as pn -> pn 
		| `Operator (op, children) -> `Operator(op)

(* plan -> plan -> plan *)
let parent plan node =
	let rec parent_aux p =
		match p with
			| `TupleRelation r -> None
			| `Relation r ->	None
			| (`Operator (n, children)) as pn ->
				if (List.exists (fun (id, c) -> c = node) children) then
					Some pn
				else
					List.fold_left
						(fun acc (_, c) ->
							match acc with
								| None -> parent_aux c 
								| Some r -> Some r)
						None children
	in
		match parent_aux plan with
			| None ->
				print_endline ("Failed to find parent for "^(string_of_plan_node node)^" in plan:");
				print_plan plan;
				raise
					(PlanException
						("Failed to find parent for " ^
							(string_of_plan_node node)))
			| Some p -> p

(* plan -> plan -> plan list *)
let children plan node =
	match node with
		| `TupleRelation r | `Relation r -> []
		| `Operator (_, children) -> List.map (fun (_,c) -> c) children

(* plan- > plan -> plan list *)
let descendants plan node =
	let rec desc_aux c acc =
		match c with
			| `TupleRelation r -> (`TupleRelation r)::acc 
			| `Relation r -> (`Relation r)::acc
			| (`Operator (op, children)) as n ->
				(List.fold_left (fun acc (_,c) -> desc_aux c acc) (acc@[n]) children)
	in
		desc_aux node []

let ancestors plan node =
	let rec ancestor_aux a acc =
		match a with
			| p when p = plan -> plan::acc
			| _ -> let p = parent plan a in ancestor_aux p (p::acc)
	in
		ancestor_aux node []

(* plan -> plan -> plan -> plan *)
let splice plan orig rewrite =
	let rec splice_aux p =
		if p = orig then rewrite
		else
			(match p with
				| `Operator (op, children) ->
					`Operator (op,
						List.map (fun (id, c) -> (id, splice_aux c)) children)
				| r -> r)
	in
		splice_aux plan

let replace plan orig rewrite =
	let rec replace_aux p acc =
		if p = orig then (rewrite, rewrite::acc)
		else
			(match p with
				| `Operator (op, children) ->
					let (cl, acc2) =
						List.fold_left
							(fun (cl_acc, acc3) (id, c) ->
								let (newc, acc4) = replace_aux c acc3 in
									(cl_acc@[(id, newc)], acc4))
								([], acc) children
					in
						(`Operator (op, cl), acc2) 
				| r -> (r, acc))
	in
		replace_aux plan []
		

(* plan -> plan list *)
let get_base_relations plan =
	let rec gbr_aux plan acc = 
		match plan with
			| `TupleRelation r -> (`TupleRelation r)::acc
			| `Relation r -> (`Relation r)::acc
			| `Operator (n, children) ->
				List.fold_left
					(fun acc (id, p) -> gbr_aux p acc) acc children
	in
		gbr_aux plan []

let get_base_relations_by_depths plan =
	let rec gbr_aux p cnt acc =
		match p with
			| `TupleRelation r -> (cnt+1, p)::acc
			| `Relation r -> (cnt+1, p)::acc
			| `Operator (n, children) ->
				List.fold_left
					(fun acc (id, p) -> gbr_aux p (cnt+1) acc) acc children
	in
	let br_depths = gbr_aux plan 0 [] in
		List.map
			(fun (_,br) -> br)
			(List.sort (fun (d1,_) (d2,_) -> d2 - d1) br_depths)

let get_base_relation_names plan =
	List.map
		(fun rp -> match rp with
			| `TupleRelation r | `Relation r -> r.name)
		(get_base_relations plan)

let get_base_relation_names_and_depths plan =
	let rec gbr_aux p cnt acc =
		match p with
			| `TupleRelation r -> (cnt+1, r.name)::acc
			| `Relation r -> (cnt+1, r.name)::acc
			| `Operator (n, children) ->
				List.fold_left
					(fun acc (id, p) -> gbr_aux p (cnt+1) acc) acc children
	in
		gbr_aux plan 0 []