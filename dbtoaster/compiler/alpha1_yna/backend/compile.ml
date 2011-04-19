(*
   SQL to C Compiler

   Author: Yanif Ahmad
   Date: 3/4/2009
	
	 Notes: implements both the rewriter and compiler
	
	 TODO list:
	 -- nested aggregate support
	 -- handle general conjunctive join predicates
	    w/ multiple qualified or unqualified attributes
			used from a single tuple relation  
*)

open Plan
open Rules

exception RewriteException of string 

type target = [`Delta of plan | `Map of plan]
type relation = string

type statement = string
type code = (relation, statement list) Hashtbl.t

type bindings = string list
type plan_map = plan * bindings 
type maps = plan_map list

type target_node = target * relation list
type delta_path = relation list
type compile_tree = [
	`Leaf of target * delta_path * code |
	`Node of target_node * delta_path * code * maps * compile_tree list ]


(* Code generation for base map delta *)
let generate target rel = Hashtbl.create 10


(* Plan map helpers *)
(* plan -> maps -> (plan_node, maps) Hashtbl.t -> (plan_node, maps) Hashtbl.t *)
let track plan maps maps_cache =
	Hashtbl.replace maps_cache (get_plan_node plan) maps;
	maps_cache

(* plan -> (plan_node, maps) Hashtbl.t -> maps *)
let lookup plan maps_cache =
	try
		Hashtbl.find maps_cache (get_plan_node plan)
	with Not_found ->
		begin
			print_endline ("Could not find maps for "^(string_of_plan_node plan));
			raise Not_found
		end

(* plan -> maps list -> maps *)
(* TODO: think more about nested aggregates *)
let apply_operator plan child_maps =
	match plan with
		| `TupleRelation r | `Relation r ->
			raise (RewriteException
				("Invoked apply_operator against a base relation"))
		| (`Operator (op, children)) as pn ->
			begin
				match op with
					| `Select _ | `Project _ 
					| `Aggregate _ | `Topk _ ->
						let op_map = List.hd child_maps in
							begin
								match op_map with
									| [] -> [(pn, [])]
									| (_,bindings)::[] -> [(pn, bindings)]
									(* TODO: nested aggregates may cause multiple bindings
									 * if they aggregate over a cross product *)
									| r -> raise
										(RewriteException
											("Found multiple bindings, "^
												"nested aggregates are unsupported for now.")) 
							end

					| `Join _ | `Cross _ ->
						[(pn,
							(List.fold_left
								(fun acc cm ->
									match cm with
										| [] -> acc
										| (_,bindings)::[] -> acc@bindings
										(* TODO: nested aggregates may cause multiple bindings
									 	 * if they aggregate over a cross product *)
										| r ->
											raise
												(RewriteException
													("Found multiple bindings, "^
														"nested aggregates are unsupported for now.")))
								[] child_maps))]
			end


(* Rule dispatcher *)
let apply_match_rules plan child_maps =
	match plan with
		| `Operator (`Join(p), [subp1; subp2]) ->
			rule_join_cross p subp1 subp2 child_maps

		| `Operator	(`Project(pr_params),
			[(cid, `Operator(`Cross(cparams), cchildren))] ) ->
				rule_project_cross pr_params cchildren child_maps
				(* TODO rename downstream expressions from cid
				    (i.e. cross id) to new child id *) 

		| `Operator (`Cross(cparams), cchildren) ->
			(* Empty tuple relation elimination for cross products *)
			rule_cross_tuplerel cchildren child_maps

		| `Operator (`Aggregate(agg_params),
			[(sid, `Operator(`Select(sel_params), schildren))]) ->
				rule_bind_agg_args agg_params sel_params (sid, schildren) child_maps

		| `Operator (`Aggregate(agg_params),
			[(cid, (`Operator(`Cross(cparams), cchildren) as cross))]) ->
				let r = 
					rule_agg_cross_project agg_params cid cparams cchildren child_maps
				in
				if r = None then
					let r2 =
						rule_bind_cross agg_params cid cross cparams cchildren child_maps
					in
					(*
					if r2 = None then
						rule_separate_sum_cross agg_params cparams cchildren child_maps
					else
					*)
						r2
				else r

		(* Selection pushdown *)
		| `Operator (`Select(sparams),
			[(cid, (`Operator(`Join _, _) as sel_child))])

		| `Operator (`Select(sparams),
			[(cid, (`Operator(`Cross _, _) as sel_child))]) ->

				rule_predicate_pushdown sparams cid sel_child child_maps

		(* Selection elimination on tuple relations *)
		| `Operator (`Select(sparams), [(rid, `TupleRelation r)]) ->
			rule_select_tuplerel sparams r child_maps

		| _ -> None

(* plan -> relation_identifier -> plan *)
let rec substitute_tuple plan rel =
	match plan with
		| `TupleRelation r ->
			raise
				(RewriteException
					("Found existing tuple relation during substitution"))

		| `Relation r ->
			if rel == r.name then `TupleRelation r else `Relation r

		| `Operator (n, children) ->
			`Operator (n,
				List.map
					(fun (id, p) -> (id, substitute_tuple p rel)) children)


(* plan list -> (plan_node, maps) Hashtbl.t *)
let get_maps_cache inputs =
	let r = Hashtbl.create 10 in
		begin
			List.iter (fun i -> Hashtbl.replace r (get_plan_node i) [(i, [])]) inputs;
			r
		end

(* plan -> (plan_node, maps) Hashtbl.t -> maps *)
let nearest_maps plan maps =
	try Hashtbl.find maps (get_plan_node plan)
	with Not_found ->
		print_endline ("Could not find map for "^(string_of_plan_node plan));
		raise Not_found 

(* plan -> (plan_node, maps) Hashtbl.t -> code *)
let generate_statements plan maps = Hashtbl.create 10

(* Bottom-up recursive rewrite:
  plan -> (plan_node, maps) Hashtbl.t -> plan list ->
		plan * (plan_node, maps) Hashtbl.t
*) 
(* TODO: 'plan' argument should be a map expression
  -- match plan with
		| `Plan | `Sum | `Product
	-- for nested aggregates splice plan should be rewritten to
	   enclose map expressions inside a relational plan  
*)
let rec bottom_up_rewrite plan maps_cache plan_inputs =
	print_endline ("Rewrite list:\n"^
		(List.fold_left
			(fun acc n -> (if (String.length acc) = 0 then "" else acc^", ")^
				string_of_plan_node n)
			"" plan_inputs));
	match plan_inputs with
		| [] -> (plan, maps_cache)
		| n::t ->
			if n = plan then
				(plan, maps_cache)
			else
				begin
					let pn = parent plan n in
					let child_maps =
						List.map
							(fun child -> try lookup child maps_cache with Not_found -> [])
							(children plan pn)
					in
					let match_result = apply_match_rules pn child_maps in
						(match match_result with
							| None ->
								let new_maps_cache =
									track pn (apply_operator pn child_maps) maps_cache
								in
								let new_t = if List.mem pn t then t else (t@[pn]) in 
									bottom_up_rewrite plan new_maps_cache new_t
		
							| Some (rewritten_node, bound_maps, (prepend_t, append_t)) ->
								print_endline ("Applied a rule! Result:");
								print_plan rewritten_node;
								let new_t = 
									let desc = descendants plan pn in
									let anc = ancestors plan pn in
										(*
										print_endline ("Ancestors:\n"^
											(List.fold_left
												(fun acc a ->
													(if (String.length acc) = 0
													then "" else acc^", ")^(string_of_plan_node a))
												"" anc));
											*) 
										prepend_t@
										(List.filter
											(fun n -> not((List.mem n desc) or (List.mem n anc))) t)
										@append_t
								in
								let spliced_plan = splice plan pn rewritten_node in
								let new_maps_cache = track rewritten_node bound_maps maps_cache in
									print_endline "New plan after applying rule:";
									print_plan spliced_plan;
								  bottom_up_rewrite spliced_plan new_maps_cache new_t)
				end

(* Query rewriter
 * sig: target * rel -> target list * code * maps
*)
(* TODO:
 	-- bottom_up_rewrite will rewrite a map expression
	-- nearest_maps will strip away expressions until it reaches a set of plans
	-- genereate_statements will apply non-plan operations to reach the same set of plans
	-- rewrite will then return the plans reached as the new targets
*)
let rewrite =
	function 
		| (`Delta p, _) ->
				raise (RewriteException ("Unable to rewrite a delta target"))
		| (`Map p, rel) ->
			print_endline (String.make 50 '-');
			print_endline (String.make 50 ' ');
			print_endline ("Rewriting for base rel "^rel);
			print_plan p;
			print_endline (String.make 50 ' ');
			print_endline (String.make 50 '-');

			let subs_p = substitute_tuple p rel in
			let inputs = get_base_relations_by_depths subs_p in
			let maps_cache = get_maps_cache inputs in
			let (rewrite_p, rewrite_maps) =
				(* TODO: invoke as (`Plan subs_p) *)
				bottom_up_rewrite subs_p maps_cache inputs in
			let maps = nearest_maps rewrite_p rewrite_maps in
			let code = generate_statements rewrite_p maps in
				(List.map (fun (p,b) -> `Map(p)) maps, code, maps)
			

(* let (t,c,m) = rewrite; c *)
let empty_code : code = Hashtbl.create 10

(* let (t,c,m) = rewrite; m list *)
let empty_maps = []

(* target_node list *)
let empty_tr_node_l = []

let set_difference l1 l2 = List.filter (fun el -> not(List.mem el l2)) l1 


(* Compiler via target-wise decomposition
 * Notes: this requires post-processing to gather up all
 *   code segments and maps *)

let decompose_target root_target base_relations =
	let rec decompose_aux tr_node decompose_path code maps =
		match tr_node with
			| (`Map t, r::[]) ->
				let child = `Leaf (`Delta t, decompose_path@[r], (generate t r))
				in
					`Node (tr_node, decompose_path, code, maps, [child])

			| (t, rels) ->
				let children = 
					(List.fold_left
						(fun acc r ->
							let (new_targets, new_code, new_maps) = rewrite (t,r) in
							let new_rels = set_difference rels [r] in
							let tr_children =
								List.map
									(fun nt -> decompose_aux
										(nt, new_rels) (decompose_path@[r]) new_code new_maps)
									new_targets
							in
								(acc@tr_children))
						[] rels)
				in
					`Node (tr_node, decompose_path, code, maps, children)
	in
		decompose_aux
			(root_target, base_relations)
			[] empty_code empty_maps


(* Compiler via level-wise decomposition
   Note: this accumulates maps and code segments inline.
   -- for now we treat maps as a list, rather than a set,
      hence we'll have duplicate maps that can be shared.
*)

let accumulate_targets level_acc new_targets_wr =
	level_acc@new_targets_wr

let accumulate_code code_acc new_code =
	Hashtbl.iter (fun r st -> Hashtbl.add code_acc r st) new_code;
	code_acc

let accumulate_maps maps_acc new_maps = maps_acc@new_maps 

let compile_target_from_inputs target input_relations lt_acc lc_acc lm_acc =
	match input_relations with
		| r :: [] -> (empty_tr_node_l, (generate target r), empty_maps)
		| _ ->
			List.fold_left
				(fun (trnl_acc, code_acc, maps_acc) input ->
					let (new_targets, new_code, new_maps) = rewrite (target, input) in
					let new_relations = set_difference input_relations [input] in
					let new_trnl = List.map (fun nt -> (nt, new_relations)) new_targets in
						((accumulate_targets trnl_acc new_trnl),
							(accumulate_code code_acc new_code),
							(accumulate_maps maps_acc new_maps))) 
				(lt_acc, lc_acc, lm_acc) input_relations


(* let (t,c,m) = rewrite;
 * compile_target_level : (target_node list, c, m) *)
let rec compile_target_level tcm_tuple  =
	match tcm_tuple with
		| ([], code, maps) -> (code, maps)
		| (tr_node_l, code, maps) ->
			let new_tcm_tuple =
				List.fold_left
				(fun (target_wr_acc, code_acc, maps_acc) (target_node, node_relations) ->
					compile_target_from_inputs
						target_node node_relations target_wr_acc code_acc maps_acc)
				(empty_tr_node_l, code, maps) tr_node_l
			in
				compile_target_level new_tcm_tuple

(* Compiler top-level *)
let tree_compiler query_plan =
	let br_depths = get_base_relation_names_and_depths query_plan in
	let br_sorted_by_depth =
		List.sort (fun (d1,_) (d2,_) -> d1-d2) br_depths
	in  		
	let compile_tree =
		decompose_target (`Map(query_plan))
			(List.map (fun (_,br) -> br) br_sorted_by_depth)
	in
		print_compile_tree compile_tree
		
let level_compiler query_plan = 
	let base_relations = get_base_relation_names query_plan in 
	let (code, maps) =
		compile_target_level
			([(`Map(query_plan), base_relations)], empty_code, empty_maps)
	in
		begin
			print_code code;
			print_maps maps
		end

(* Unit tests *)

(* TODO: unit tests for plan helpers
  -- parent
	-- children
	-- descendants
	-- splice *)

(* TODO: unit test for apply_operator *)

(* TODO: unit test for plan transformation helpers
  -- map_bool_expr, map_expr
	-- fold_map_bool_expr, fold_map_expr *)

(* TODO: unit test for rewrite_predicate *)

(* TODO: unit test for rule transformations
  -- rule 1
*)

let unit_test () =
	let query1 =
		`Operator(
			`Aggregate({
				aggregate= `Sum(
					`Product(
						`ETerm(`Attribute(`Unqualified("a"))),
						`ETerm(`Attribute(`Unqualified("d")))));
				out=(`Unqualified "sum")
				}),
			[
				("RST", `Operator(
					`Join({
						jpredicate= `BTerm(`EQ(
							`ETerm(`Attribute(`Qualified("RS", "c"))),
							`ETerm(`Attribute(`Qualified("T", "c")))));
						jattributes= [
							(`Unqualified("a"), `ETerm(`Attribute(`Qualified("RS","a"))));
							(`Unqualified("d"), `ETerm(`Attribute(`Qualified("T","d")))); 
							];
						}),
						[
							("RS", `Operator(
								`Join({
									jpredicate= `BTerm(`EQ(
										`ETerm(`Attribute(`Qualified("R","b"))),
										`ETerm(`Attribute(`Qualified("S","b")))));
									jattributes= [
										(`Unqualified("a"), `ETerm(`Attribute(`Qualified("R","a"))));
										(`Unqualified("c"), `ETerm(`Attribute(`Qualified("S","c"))));
										];
									}),
									[ ("R", `Relation({name="R"; schema=[("a", "int"); ("b", "int")]}));
										("S", `Relation({name="S"; schema=[("b", "int"); ("c", "int")]}))
									]));
							("T", `Relation({name="T"; schema=[("c", "int"); ("d", "int")]}))
						]))
			]) 
	in
		begin
			tree_compiler query1;
			level_compiler query1
		end;;

unit_test();;
		
