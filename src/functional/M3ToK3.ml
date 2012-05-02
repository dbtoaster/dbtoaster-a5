(** Module for translating a M3 program into a K3 program. *)

(**/**)
open Arithmetic
open Calculus

module T = Types
module V = Arithmetic.ValueRing
module C = Calculus.CalcRing
module K = K3



let pair_map pair_fn (a,b) = (pair_fn a, pair_fn b)

let varIdType_to_k3_type   (vars : T.var_t list) = List.map (fun (v,t) -> K.TBase(t)) vars
let varIdType_to_k3_idType (vars : T.var_t list) = List.map (fun (v,t) -> v,K.TBase(t)) vars
let varIdType_to_k3_expr   (vars : T.var_t list) = List.map (fun (v,t) -> K.Var(v,K.TBase(t))) vars
let k3_idType_to_k3_expr   (vars : (K.id_t * K.type_t) list) = List.map (fun (v,t) -> K.Var(v,t)) vars
let k3_idType_to_k3_type   (vars : (K.id_t * K.type_t) list) = List.map snd vars
let k3_expr_to_k3_idType   (var_el : K.expr_t list ) =
	List.map (fun v_e -> 
							begin match v_e with
								| K.Var(v_name,vtype) -> (v_name,vtype)
								| _ -> failwith "K.Var expected."
							end )
					 var_el
					
let k3_expr_to_k3_type   (var_el : K.expr_t list ) =
	List.map (fun v_e -> 
							begin match v_e with
								| K.Var(v_name,vtype) -> vtype
								| _ -> failwith "K.Var expected."
							end )
					 var_el
					
let name_of_kvar kvar = begin match kvar with
		| K.Var(v_name,vtype) -> v_name
		| _ -> failwith "K.Var expected."
	end
let type_of_kvar kvar = begin match kvar with
		| K.Var(v_name,vtype) -> vtype
		| _ -> failwith "K.Var expected."
	end

let m3_map_to_k3_map (m3_map: M3.map_t) : K.map_t = match m3_map with
		| M3.DSView(ds)                    -> 
				let (map_name, input_vars, output_vars, _, _) = Plan.expand_ds_name ds.Plan.ds_name 
				in 
				(map_name, List.map snd input_vars, List.map snd output_vars )
		| M3.DSTable(rel_name, rel_schema,_) -> 
				(rel_name, [], List.map snd rel_schema )

(**/**)

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**/**)
let      zero_int_val = K.Const(T.CInt(0))
let       one_int_val = K.Const(T.CInt(1))
let minus_one_int_val = K.Const(T.CInt(-1))

let      zero_flt_val = K.Const(T.CFloat(0.0))
let       one_flt_val = K.Const(T.CFloat(1.0))
let minus_one_flt_val = K.Const(T.CFloat(-1.0))


let compatible_types t1 t2 = begin match t1, t2 with
	| K.TBase(T.TInt), K.TBase(T.TFloat) -> true
	| K.TBase(T.TFloat), K.TBase(T.TInt) -> true
	| _,_ -> (t1 = t2)
	end

let numerical_type t = begin match t with
	| K.TBase(T.TBool)  -> true
	| K.TBase(T.TInt)   -> true
	| K.TBase(T.TFloat) -> true
	| _ -> false
	end

let init_val_from_type ret_type = begin match ret_type with
	| T.TInt   -> zero_int_val
	| T.TFloat -> zero_flt_val
	| _ -> failwith ("invalid return type ("^(T.string_of_type ret_type)^".")
	end
		
let init_val ret_e = begin match ret_e with
	| K.Var(_,K.TBase(ret_type))   -> init_val_from_type ret_type
	| _ -> failwith "invalid return expression."
	end

let extract_opt opt =  begin match opt with
	| None -> failwith "Trying to extract some from None"
	| Some(s) -> s
	end
(**/**)

(**********************************************************************)
(**/**)
let unique l = List.fold_left (fun acc e -> if List.mem e acc then acc else acc@[e]) [] l	
let keys_from_kvars kvars  = List.map (fun kv -> (name_of_kvar kv,kv)) kvars

let exprs_to_tuple (el: K.expr_t list) = 
	begin match el with
	|  [] -> failwith "invalid empty expressions list"  
	| [e] -> e
	| _   -> K.Tuple(el)
	end

(**/**)

(**[lambda_args v_el]

   Converts a list of K3 variable expressions into K3 lambda arguments.
	@param v_el The list of K3 variable expressions to be converted.
	@return     The K3 lambda argument corresponding to [v_el] *)
let lambda_args v_el =
	begin match v_el with
	| [] -> failwith "invalid empty variables list" 
	| [K.Var(v_name,vtype)] -> K.AVar(v_name,vtype)
	| _ ->
		let v_l = k3_expr_to_k3_idType v_el in 
		let vars = List.map fst v_l in
  		if (unique vars) <> vars then failwith "invalid lambda with duplicate variables"
		else
			K.ATuple(v_l)
	end

(**[lambda v_el body]

   Constructs a lambda expression from the expression [body] with the variables in [v_el].
	@param v_el The list of K3 variables of the [body].
	@param body The body of the lambda expression.
	@return     The K3 lambda expression expecting [v_el] as arguments and computing [body]. *)
let lambda v_el body = K.Lambda( lambda_args v_el, body)

(**[assoc_lambda v1_el v2_el body]

   Constructs an associative lambda expression from the expression [body] with the variables
	 in [v1_el] and [v1_el].
	@param v1_el The first list of K3 variables of the [body].
	@param v2_el The second list of K3 variables of the [body].
	@param body  The body of the associative lambda expression.
	@return      The K3 associative lambda expression expecting [v1_el] and [v2_el] as arguments
					 and computing [body]. *)
let assoc_lambda v1_el v2_el body = 
	K.AssocLambda( lambda_args v1_el, lambda_args v2_el, body )

(**[apply_lambda v_el el body]

   Evaluates the K3 expression [body] by binding the list of expressions in [el]
	to the list of K3 variables in [v_el]. *)
let apply_lambda v_el el body = 
	assert ((List.length v_el) = (List.length el));
	K.Apply(lambda v_el body, (exprs_to_tuple el))

(**[project_fn from_v_el to_v_el]

   Projects the list of variables [from_v_el] to the list of variables [to_v_el].
	When maped over a collection it will project away keys that aren't present 
	in [to_v_el]. *)
let project_fn from_v_el to_v_el = 
	lambda from_v_el (exprs_to_tuple to_v_el)	
							
let accum_counter = ref 0
(**[aggregate_fn v_el multpl_e]

   Constructs an aggregation function.
	@param v_el     The list of K3 variables of the collection being aggregated.
	@param multpl_e Variable expression used for accessing the values associated
						 with each tuple in the aggregated collection. *)												
let aggregate_fn v_el multpl_e =
	let next_accum_var() = incr accum_counter;
   							  "accv_"^(string_of_int !accum_counter) in
	let accv = next_accum_var () in 
	let multpl_t = snd (List.hd (k3_expr_to_k3_idType [multpl_e])) in
	let acce = K.Var( accv, multpl_t ) in
	assoc_lambda (v_el@[multpl_e]) [acce] (K.Add(acce, multpl_e))


(**[external_lambda_args fn t_l]

   Converts a list of K3 types into K3 external lambda arguments.
	@param fn   The name of the external function, used when naming the arguments.
	@param t_l  The list of K3 types to be converted.
	@return     The K3 lambda argument corresponding to [t_l] *)
let external_lambda_args fn t_l =
	begin match t_l with
	| [] -> failwith "invalid empty variables list" 
	| [t] -> K.AVar(fn^"_arg1",t)
	| _ -> 
		let arg_prefix  = fn^"_arg_" in
		let arg_counter = ref 0 in
		let next_arg_var() = incr arg_counter;
		   						 arg_prefix^(string_of_int !arg_counter) in
		K.ATuple(List.map (fun t -> next_arg_var(),t) t_l)
	end

(**[external_lambda fn t_l ftype]

   Constructs an external lambda expression for the external function [fn] with 
	arguments of types corresponding to [t_l] and return type ftype.
	@param fn    The name of the external function.
	@param t_l   The list of K3 types used for typing the arguments.
	@param ftype The return type of the function. *)
let external_lambda fn t_l ftype =
	K.ExternalLambda( fn, external_lambda_args fn t_l, ftype)

(**[apply_external_lambda fn te_l ftype]

   Evaluates the external function [fn] by binding its arguments to the expressions 
	in [te_l]. *)		
let apply_external_lambda fn te_l ftype =
	let (t_l, e_l) = List.split te_l in
	K.Apply( external_lambda fn t_l ftype, (exprs_to_tuple e_l))
						




(**********************************************************************)
(**/**)
(* Utility function for generating map access expression. *)
let map_to_expr mapn ins outs map_type =
	  let (ins_k,outs_k) = pair_map varIdType_to_k3_idType (ins,outs) in
		let map_type_k = K.TBase(map_type) in
		begin match ins, outs with
	  | ([],[]) -> K.SingletonPC(mapn,             map_type_k)
	  | ([], x) ->       K.OutPC(mapn,      outs_k,map_type_k)
	  | ( x,[]) ->        K.InPC(mapn,ins_k,       map_type_k)
	  | ( x, y) ->          K.PC(mapn,ins_k,outs_k,map_type_k)
	  end


(* Utility function for generating temporary maps used in computing sums. *)
let sum_tmp_prefix  = "sum_tmp_"
let sum_tmp_counter = ref 0
let next_sum_tmp_coll outs sum_type_k = 
		incr sum_tmp_counter;
   	let colln = sum_tmp_prefix^(string_of_int !sum_tmp_counter) in
		let outs_k  = varIdType_to_k3_idType outs in
		begin match outs with
	  | [] -> failwith "sum_tmp collection required only if 'outs' is not empty!"
	  |  x -> K.OutPC(colln, outs_k,sum_type_k),
		        (colln, [], List.map snd outs)
	  end
(**/**)		
(**********************************************************************)

(**[map_access_to_expr mapn ins outs map_ret_t theta_vars_el init_expr_opt]

   Generates a K3 expression for accessing a map.
	@param mapn          The name of the map being accessed.
	@param ins           The input variables of the map access.
	@param outs          The output variables of the map access.
	@param map_ret_t     The type of the value associated with each tuple of the map.
	@param theta_vars_el The variables that are in scope when accessing the map.
	@param init_expr_opt The expression that should be used for initialization.
								It is [None] if no expression is provided.
	@return              A tuple consisting of :
								- the schema of the collection resulting from the map access,
								- a variable that can be used for binding against the value 
							     associated with each element of the map,
								- the K3 expression for accessing the map. *)
let map_access_to_expr mapn ins outs map_ret_t theta_vars_el init_expr_opt =
		
		let (ins_k,outs_k)   = pair_map varIdType_to_k3_idType (ins,outs) in
		let (ins_el,outs_el) = pair_map varIdType_to_k3_expr   (ins,outs) in
		let free_vars_el  = ListAsSet.diff outs_el theta_vars_el in
		let bound_vars_el = ListAsSet.diff outs_el free_vars_el in
		
		let (ins_tl,outs_tl) = pair_map varIdType_to_k3_type (ins,outs) in
		(* K3 type of the value associated with each tuple in the map *)
		let map_ret_k = K.TBase(map_ret_t) in
		(* K3 type of the out tier of the map *)
		let map_out_t = if outs_tl = [] then map_ret_k
							  else K.Collection(K.TTuple(outs_tl@[map_ret_k])) in
		
		let map_ret_ve = K.Var("map_ret",map_ret_k) in
		let map_out_ve = K.Var("slice",map_out_t) in
		let iv_e = K.Var("init_val", map_out_t) in
		
		(* Given a collection it slices it according to the bound variables *)
		(* and projects only the free variables *)
		let slice_and_project coll_ve : K.expr_t =
			assert (free_vars_el <> []);
			if free_vars_el = outs_el then coll_ve
			else 
				K.Map( project_fn (outs_el@[map_ret_ve]) (free_vars_el@[map_ret_ve]), 
							 K.Slice(coll_ve, outs_k, keys_from_kvars bound_vars_el))
		in
		
		let expr = begin match ins, outs with
		  | ([],[]) -> K.SingletonPC(mapn, map_ret_k)
		  | ([], y) ->       
				let map_expr = K.OutPC(mapn, outs_k, map_ret_k) in
				if free_vars_el = [] then 
					if init_expr_opt = None then K.Lookup(map_expr,outs_el)
		      	else 
						let _,init_expr = extract_opt init_expr_opt in						
						K.IfThenElse(K.Member(map_expr,outs_el), 
												 K.Lookup(map_expr,outs_el), init_expr)											     		
	      	else
					slice_and_project map_expr
							
		  	| ( x,[]) -> 
				let map_expr = K.InPC(mapn, ins_k, map_ret_k) in
				if init_expr_opt = None then K.Lookup(map_expr,ins_el)
	      	else 
					let _,init_expr = extract_opt init_expr_opt in						
					K.IfThenElse(K.Member(map_expr,ins_el), 
									 K.Lookup(map_expr,ins_el), 
									 apply_lambda [iv_e] [init_expr] 
											(K.Block(
												[K.PCValueUpdate(map_expr, ins_el, outs_el, iv_e);
												iv_e])) )
											
		  	| ( x, y) ->
				let map_expr = K.PC(mapn, ins_k, outs_k, map_ret_k) in
												
				let out_access_expr coll_ve =	
					if free_vars_el = [] then	K.Lookup(coll_ve,outs_el)
		      	else                      slice_and_project coll_ve
				in
				
				if init_expr_opt = None then 
					apply_lambda [map_out_ve] [K.Lookup(map_expr,ins_el)] (out_access_expr map_out_ve)
		      else 
					let init_outs_el,ie = extract_opt init_expr_opt in	
					let init_expr = 
						if init_outs_el = outs_el then ie
						else
							(* Use projection to change the order of output variables from 'init_outs_el' *)
							(* to 'outs_el' *) 
							K.Map( project_fn (init_outs_el@[map_ret_ve]) (outs_el@[map_ret_ve]), ie)
					in
					let init_block = 
						K.Block([K.PCUpdate(map_expr, ins_el, iv_e); out_access_expr iv_e])
					in
					K.IfThenElse( K.Member(map_expr,ins_el), 
						apply_lambda [map_out_ve] [K.Lookup(map_expr,ins_el)] (out_access_expr map_out_ve), 
						apply_lambda       [iv_e]              [init_expr]      init_block)
		      
		end
		in (free_vars_el, map_ret_ve, expr)

		
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

(**[value_to_k3_expr value_calc]

   Converts a value ring expression into a K3 expression.
	@param value_calc The value ring expression being translated.
	@return           The [value_calc] translated into K3 as a tuple containing :
							- the K3 type of the expression,
							- the K3 expression itself. *)
let rec value_to_k3_expr (value_calc : V.expr_t) : K.type_t * K.expr_t =
	let bin_fn op_fn r1 c2 =
		let ret1_t, e1 = r1 in
		let ret2_t, e2 = value_to_k3_expr c2 in
		assert ((numerical_type ret1_t) && (numerical_type ret2_t));
		let ret_t =	if ret1_t = ret2_t  then	  ret1_t
						else                        K.TBase(T.TFloat) 
		in 
		ret_t, (op_fn e1 e2)
	in	
	begin match value_calc with
		| V.Val( AConst(i) )  -> K.TBase(T.type_of_const i), K.Const(i)
		| V.Val( AVar(v,t) )  -> K.TBase(t),                 K.Var(v,K.TBase(t))
		| V.Val( AFn(fn,fargs,ftype) ) -> 
				let ret_t = K.TBase(ftype) in
				let te_l = List.map value_to_k3_expr fargs in 
				ret_t, apply_external_lambda fn te_l ret_t
		| V.Neg( neg_arg ) -> 
				let neg_t, neg_e = value_to_k3_expr neg_arg in
				let neg_cst = begin match neg_t with
					| K.TBase( T.TInt )   -> minus_one_int_val
					| K.TBase( T.TFloat ) -> minus_one_flt_val
					| _ -> failwith "Negation type should be int or float." 
				end in
				neg_t, K.Mult (neg_cst, neg_e)
		| V.Sum( c1::sum_args_tl )	->
				let add_fun e1 e2 = K.Add(e1,e2) in
				List.fold_left (bin_fn add_fun) (value_to_k3_expr c1) sum_args_tl    
		| V.Prod( c1::prod_args_tl )	->	  
				let mult_fun e1 e2 = K.Mult(e1,e2) in
				List.fold_left (bin_fn mult_fun) (value_to_k3_expr c1) prod_args_tl
		| _  -> failwith "Empty V.Sum or V.Prod."
	end



(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

(** Along the translation process we keep two sets of lists encapsulated 
in the meta_t type. 

The elements of the first list contain initialized keys, indicating 
for a specific map under which variables it has been initialized. This is
used as to not generate member tests and initialization expressions for map
accesses that we know have been encountered before, and thus are guaranteed
to be initialized. We only keep initialized keys for input maps or full maps,
 as these are the only maps that get updated when initialization has to be 
performed. For the other maps the initialization expression is returned without 
modifying the map itself. Besides the name of the map, we also keep a list of 
K3 variables representing the input keys for which the map has been initialized. 
When performing initialization on a full map we create an entire output tier at 
once. This is why we only record input keys and no output keys.

The elements of the second list contain the description of temporary maps
needed for performing sum operations. These maps will get added to the set
of maps that need to be created for this K3 program.

If one is not interested in the metadata information one can supply the result
of [empty_meta] as argument to functions that require metadata. 
 *)
type meta_t = ((string * (T.var_t list)) list) * (K.map_t list)
let empty_meta = ([],[])

(**/**)
let meta_append_sum_map meta sum_map =
		(fst meta), (snd meta)@[sum_map]
		
let meta_has_init_keys meta init_keys = 
		List.mem init_keys (fst meta)

let meta_append_init_keys meta init_keys =
		(fst meta)@[init_keys],(snd meta)
(**/**)		
(**********************************************************************)


(**[calc_to_k3_expr meta theta_vars calc]

   Converts a calculus ring expression into a K3 expression.
	@param meta       Metadata associated with the translation process. See 
							[meta_t] definition above.	
	@param theta_vars The variables that are in scope when evaluating the expression.
	@param calc       The calculus expression being translated.
	@return           The [calc] translated into K3 and the updated meta. For the K3 
							result we return a tuple containing :
							- the schema of the K3 expression as a list of K3 variables. This
							can be used for binding against the keys of the collection 
							represented	by the expression. This schema will contain only 
							unbound variables, all bound variables get projected away.
							- a variable that can be used for binding against the value 
							associated with each tuple in the collection represented
							by the expression.
							- the K3 expression itself *)
let rec calc_to_k3_expr meta theta_vars_el calc : 
				((K.expr_t list * K.expr_t * K.expr_t) * meta_t) =
	let rcr        = calc_to_k3_expr meta  theta_vars_el in
	let rcr2 meta2 = calc_to_k3_expr meta2 theta_vars_el in
	
	let (ins,outs) = schema_of_expr calc in
	let ins_el = varIdType_to_k3_expr ins in
	if (ListAsSet.diff ins_el theta_vars_el) != [] then
		(print_endline ("Error: All inputs of "^(string_of_expr calc)^" should be bound!");
		 print_endline ("    ins: "^(ListExtras.string_of_list K.string_of_expr ins_el));
		 print_endline ("    scope: "^(ListExtras.string_of_list K.string_of_expr theta_vars_el)));
	assert ((ListAsSet.diff ins_el theta_vars_el) = []) ;
	
	let bin_fn op_fn c1 c2 =
		let ((ret1_t, e1),(ret2_t, e2)) = pair_map value_to_k3_expr (c1,c2) in
		assert (compatible_types ret1_t ret2_t);
		([], K.Var("v",K.TBase(T.TInt)), (op_fn e1 e2)), meta
	in
  	begin match calc with
		| C.Val( calc_val ) -> begin match calc_val with
			| Value( calc_val_value ) ->
					let ret_t, expr = value_to_k3_expr calc_val_value in
					([], K.Var("v",ret_t), expr), meta
			| Cmp( T.Eq, c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Eq  (e1,e2) ) c1 c2
			| Cmp( T.Lt, c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Lt  (e1,e2) ) c1 c2
	    	| Cmp( T.Lte,c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Leq (e1,e2) ) c1 c2
			| Cmp( T.Gt, c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Leq (e1,e2) ) c2 c1
			| Cmp( T.Gte,c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Lt  (e1,e2) ) c2 c1
			| Cmp( T.Neq,c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Neq (e1,e2) ) c1 c2
			
			| Rel(reln, rel_schema) -> 
					let rel_outs_el, rel_ret_ve, expr = 
							map_access_to_expr reln [] rel_schema T.TInt theta_vars_el None in
				  ((rel_outs_el, rel_ret_ve, expr), meta)
			
			| External(mapn, eins, eouts, ext_type, init_calc_opt) ->
					(* init_expr_opt will contain if required the schema of the intialization expression *)
					(* and the intialization expression itself. *)
					let nm, init_expr_opt = begin match eins, eouts with
						| [],[] -> meta, None
						| [],_  ->  if (Debug.active "M3TOK3-GENERATE-INIT") then 
												meta, Some([],init_val_from_type ext_type)
						           	else meta, None
						| _,_   -> 
							if (Debug.active "M3TOK3-GENERATE-INIT") && 
								 init_calc_opt != None && not (meta_has_init_keys meta (mapn, eins)) then
										let init_calc = extract_opt init_calc_opt in
										let eouts_el = varIdType_to_k3_expr eouts in
										(* Since we want to initialize on entire output tier at once we remove *)
										(*  output variables from the list of variables that are in scope. *)
										let init_theta_vars_el = ListAsSet.diff  theta_vars_el eouts_el in
										let (init_outs_el, init_ret_ve, init_expr), nm_1 = 
												calc_to_k3_expr meta  init_theta_vars_el init_calc in
										assert (ListAsSet.seteq init_outs_el eouts_el);
										assert ((type_of_kvar init_ret_ve) = K.TBase(ext_type));
										(meta_append_init_keys nm_1 (mapn, eins)), 
										Some(init_outs_el,init_expr)
							else meta, None
					end					
					in					  
				  let (map_outs_el, map_ret_ve, expr) = 
						map_access_to_expr mapn eins eouts ext_type theta_vars_el init_expr_opt in
				  ((map_outs_el, map_ret_ve, expr), nm)
					
			| AggSum( agg_vars0, aggsum_calc ) ->
					(* Convert group by variables to K3 variables. *)
					let agg_vars0_el = varIdType_to_k3_expr agg_vars0 in
					
					let (aggsum_outs_el,ret_ve,aggsum_e),nm = rcr aggsum_calc in
					
					(* Make sure that all the group by variables are included in the output*)
					(* schema of the "aggsum" expression. *)
					assert( (ListAsSet.diff agg_vars0_el aggsum_outs_el) = []);
					let agg_vars_el = ListAsSet.inter agg_vars0_el aggsum_outs_el in
										
					let agg_fn = aggregate_fn aggsum_outs_el ret_ve in
					let expr = 
							if aggsum_outs_el = [] then aggsum_e
							else if agg_vars_el = [] then
									K.Aggregate(agg_fn, init_val ret_ve, aggsum_e)
							else
									let gb_fn = project_fn (aggsum_outs_el@[ret_ve]) agg_vars_el in 
									let gb_aggsum_e = 
											K.Slice(aggsum_e, k3_expr_to_k3_idType aggsum_outs_el,[]) in
									K.GroupByAggregate(agg_fn, init_val ret_ve, gb_fn, gb_aggsum_e)
					in
					((agg_vars_el, ret_ve, expr), nm)
									
			| Lift(lift_v, lift_calc) 	      -> 
					let (lift_outs_el,ret_ve,lift_e),nm = rcr lift_calc in
					assert(compatible_types (K.TBase(snd lift_v)) (type_of_kvar ret_ve));
					let lift_ve = K.Var((fst lift_v),type_of_kvar ret_ve) in
					let lift_ret_ve = K.Var("v",K.TBase(T.TInt)) in
					let expr =
							if lift_outs_el = [] then
									K.Singleton(K.Tuple([lift_e;one_int_val]))
							else
									K.Map(lambda (lift_outs_el@[ret_ve]) 
														(K.Tuple(lift_outs_el@[ret_ve;one_int_val])),
												lift_e)
					in
					((lift_outs_el@[lift_ve], lift_ret_ve, expr), nm)
		end
			
    | C.Sum( sum_args )	->
				(* Compute the schema of the resulting sum expression *)
				let outs_el = ListAsSet.diff (varIdType_to_k3_expr outs) theta_vars_el in
				(* Translate all terms of the sum int K3 and make sure their schema *)
				(* corresponds to the sum expression's schema and that their return *)
				(* values have numerical types *)
				let prepare_fn old_meta old_ret_t c = 
					let (e_outs_el,e_ret_ve,e),new_meta = rcr2 old_meta c in
					assert (ListAsSet.seteq e_outs_el outs_el);
					let e_ret_t = type_of_kvar e_ret_ve in
					assert (numerical_type e_ret_t);
					let new_ret_t =	if old_ret_t = e_ret_t  then old_ret_t
													else                         K.TBase(T.TFloat) 
					in
					((e_outs_el,e_ret_ve,e),new_meta,new_ret_t)					
				in
								
				let nm,ret_t,sum_exprs = 
						List.fold_left 
								(fun (old_meta,old_ret_t,old_el) c -> 
										 let result,new_meta,new_ret_t = prepare_fn old_meta old_ret_t c in
										 new_meta,new_ret_t,old_el@[result]) 
								(meta,(K.TBase(T.TInt)),[]) 
								sum_args in
								
				let ret_ve = K.Var("sum",ret_t) in
				let (hd_outs_el,hd_ret_ve,hd_s) = List.hd sum_exprs in
				let sum_exprs_tl = List.tl sum_exprs in
				
				let sum_result, nm2 = if outs_el <> [] then 
					(* For computing sums between collections we use temporary maps. *)
					(* The operation has 3 steps:*)
					(* 1. Reset the temporary collection. *)
					(* 2. Insert into the temporary collection the contents of the collection *)
					(* corresponding to the first term.*)
					(* 3. Update the temporary collection with the contents of the collections *)
					(* corresponding to the rest of the sum terms.*)
					let sum_coll,sum_map = next_sum_tmp_coll outs ret_t in
					
					let reset_update = 
							K.PCElementRemove(sum_coll, [], outs_el) in
					let reset_stmt = 
							K.Iterate( lambda (outs_el@[ret_ve]) reset_update,	sum_coll) in
					
					let head_update = 
							K.PCValueUpdate(sum_coll, [], outs_el, hd_ret_ve) in									
					let head_stmt =
							K.Iterate(lambda (hd_outs_el@[hd_ret_ve])	head_update, hd_s) in
					
					let sum_fn (s_outs_el,s_ret_ve,s) =							
							let sum_update =
								K.PCValueUpdate(sum_coll, [], outs_el, 
									K.IfThenElse(K.Member(sum_coll,outs_el), 
															 K.Add(s_ret_ve,K.Lookup(sum_coll,outs_el)), 
															 s_ret_ve) ) in
							K.Iterate(lambda (s_outs_el@[s_ret_ve])	sum_update,	s) in
					let tail_stmts = List.map sum_fn sum_exprs_tl in
					
					(K.Block( reset_stmt::head_stmt::tail_stmts@[sum_coll] )),
					(meta_append_sum_map nm sum_map)
				else
					let sum_fn sum_e (s_outs_el,s_ret_ve,s)	= K.Add(sum_e,s) in
					(List.fold_left sum_fn hd_s sum_exprs_tl), 
					nm
				in
				((outs_el, ret_ve, sum_result), nm2)				
    
		| C.Prod( prod_args )	->	
				(* Translate all terms of the product int K3 and make sure their return *)
				(* values have numerical types *)
				let prepare_fn (old_meta, old_scope, old_ret_t) c = 
					let (e_outs_el,e_ret_ve,e),new_meta = calc_to_k3_expr old_meta old_scope c in
					let new_scope = ListAsSet.union old_scope e_outs_el in
					let e_ret_t = type_of_kvar e_ret_ve in
					assert (numerical_type e_ret_t);
					let new_ret_t =	if old_ret_t = e_ret_t  then old_ret_t
													else                         K.TBase(T.TFloat)
					in
					((e_outs_el,e_ret_ve,e),(new_meta,new_scope,new_ret_t))
				in
				let (nm,_,ret_t),prod_exprs = 
					List.fold_left (fun (old_extra,old_el) c -> 
															let result,new_extra = prepare_fn old_extra c in
															new_extra,old_el@[result]
													)
													((meta,theta_vars_el,(K.TBase(T.TInt))),[]) 
													prod_args in
				
				let ret_ve = K.Var("prod",ret_t) in
				let prod_expr_hd = List.hd prod_exprs in
				let prod_exprs_tl = List.tl prod_exprs in
				
				let prod_fn (p1_outs_el,_p1_ret_ve,p1) (p2_outs_el,_p2_ret_ve,p2) =
					let p1_ret_ve = K.Var("p1_ret",type_of_kvar _p1_ret_ve) in
					let p2_ret_ve = K.Var("p2_ret",type_of_kvar _p2_ret_ve) in 
					let p_outs_el,p = begin match p1_outs_el,p2_outs_el with
					| [],[] -> [], K.Mult(p1,p2)
					|  _,[] -> p1_outs_el,
										 K.Map( lambda (p1_outs_el@[p1_ret_ve])	
																	 (K.Tuple(p1_outs_el@[K.Mult(p1_ret_ve,p2)])),						
														p1)
					| [], _ -> p2_outs_el,
										 K.Map( lambda (p2_outs_el@[p2_ret_ve])	
																	 (K.Tuple(p2_outs_el@[K.Mult(p2_ret_ve,p1)])),						
														p2)
					|  _, _ -> 
						let union_el = (ListAsSet.union p1_outs_el p2_outs_el) in
						let prod_e = K.Tuple(union_el@[K.Mult(p1_ret_ve,p2_ret_ve)]) in
						let nested = K.Map(lambda (p2_outs_el@[p2_ret_ve]) prod_e, p2) in 
             		union_el, K.Flatten( K.Map(lambda (p1_outs_el@[p1_ret_ve]) nested, p1) )
					end
					in
					(p_outs_el, ret_ve, p)
				in
				
				let prod_result = List.fold_left prod_fn prod_expr_hd prod_exprs_tl in		
				(prod_result, nm)
				
		| C.Neg( neg_arg ) ->
				rcr (C.Prod( [(C.Val(Value(V.Val(AConst(T.CInt(-1))))));neg_arg] ))
  end 


(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)



(**[m3_stmt_to_k3_stmt meta trig_args m3_stmt]

   Converts a M3 statement into a K3 statement.
	@param meta      Metadata associated with the translation process. See [meta_t] definition above.	
	@param m3_stmt   The M3 statement being translated.
	@param trig_args The arguments of the trigger to which this statement belongs to.
	@return          The [m3_stmt] translated into K3 and the updated meta. *)
let m3_stmt_to_k3_stmt (meta: meta_t) trig_args (m3_stmt: Plan.stmt_t) : K.statement_t * meta_t =
	let (mapn, lhs_ins, lhs_outs, map_type, init_calc_opt) = Plan.expand_ds_name m3_stmt.Plan.target_map in
	let {Plan.update_type = update_type; Plan.update_expr = incr_calc} = m3_stmt in 
	
	(* all lhs input variables must be free, they cannot be bound by trigger args *)
	assert ((ListAsSet.inter trig_args  lhs_ins) = []);
	(* K3 collection used for storing the result of the statement.*)
	let lhs_collection  = map_to_expr mapn lhs_ins lhs_outs map_type in
    
	let map_k3_type = K.TBase(map_type) in
	let out_tier_t  = 
		if lhs_outs = [] then map_k3_type 
		else K.Collection(K.TTuple( (varIdType_to_k3_type lhs_outs)@[map_k3_type] )) in
			  
	let (lhs_ins_el,lhs_outs_el) = pair_map varIdType_to_k3_expr (lhs_ins, lhs_outs) in
	let trig_args_el = varIdType_to_k3_expr trig_args in
	let trig_w_ins_el = ListAsSet.union trig_args_el  lhs_ins_el  in
	let trig_w_lhs_el = ListAsSet.union trig_w_ins_el lhs_outs_el in
		
	(* Variable that will get bound to the output tier corresponding to a particular *)
	(* input variables key.*)
	let existing_out_tier = K.Var("existing_out_tier",out_tier_t) in
	(* Determine the current value of the entry in "existing_out_tier" corresponding *)
	(* to a particular output variables key. Used for updating the "existing_out_tier".*)
	(* If this is a replace statement then we'll consider zero as the existing value to *)
	(* be updated. *)
	(* If this is an update statement then we do one of the following: *)
	(* - if there aren't any output variables then "existing_out_tier" is a singleton and *)
	(*   can be used directly in the updating expression. *)
	(* - if we only have output variables then we Lookup for the desired value and we use *)
	(*   zero for initialization, if required. *)
	(* - If we have a full map and initialization is not required then a simple Lookup *)
	(*   suffices. Otherwise we use the initialization expression if the desired value *)
	(*   is not found in "existing_out_tier". The scope of the initialization expression*)
	(*   will also include the lhs output variables and it will return a singleton. *)
	let existing_v, nm0 =
		if update_type = Plan.ReplaceStmt then (init_val_from_type map_type), meta
		else 
		begin match lhs_ins_el,lhs_outs_el with
			|  _,[] ->  existing_out_tier, meta
			| [], y ->  if (Debug.active "M3TOK3-GENERATE-INIT") then 
											K.IfThenElse( K.Member(existing_out_tier, lhs_outs_el),
       														K.Lookup(existing_out_tier, lhs_outs_el),
       														(init_val_from_type map_type) ), meta
					        else 
											K.Lookup(existing_out_tier, lhs_outs_el), meta
			|  x, y ->
				if (Debug.active "M3TOK3-GENERATE-INIT") && init_calc_opt != None then
					let init_calc = extract_opt init_calc_opt in
					let init_result, init_meta = calc_to_k3_expr meta trig_w_lhs_el init_calc in
					let (init_outs_el, init_ret_ve, init_expr) = init_result in
					assert (init_outs_el = []);
					assert (compatible_types (type_of_kvar init_ret_ve) map_k3_type);
					
					K.IfThenElse( K.Member(existing_out_tier, lhs_outs_el),
       								K.Lookup(existing_out_tier, lhs_outs_el),
       								init_expr ), init_meta
				else
					K.Lookup(existing_out_tier, lhs_outs_el), meta
		end
	in
	
	(* Translate the rhs calculus expression into a k3 expression and its *)
	(* corresponding schema. Beside the trigger variables we also have *)
	(* the input variables of the "lhs_collection" in scope as we will *)
	(* update "lhs_collection" while iterating over all lhs input variables. *)
	let incr_result, nm = calc_to_k3_expr nm0 trig_w_ins_el incr_calc in
	let (rhs_outs_el, rhs_ret_ve, incr_expr) = incr_result in
	(* Make sure that the lhs collection and the incr_expr have the same schema. *)
	assert (ListAsSet.seteq (ListAsSet.diff lhs_outs_el trig_args_el) rhs_outs_el);
	assert (compatible_types (type_of_kvar rhs_ret_ve) map_k3_type);
		
	(* Iterate over all the tuples in "incr_expr" collection and update the *)
	(* lhs_collection accordingly. *)
	let coll_update_expr =	
		let single_update_expr = K.PCValueUpdate( lhs_collection, lhs_ins_el, lhs_outs_el, 
																					 		K.Add(existing_v,rhs_ret_ve) ) in
		let inner_loop_body = lambda (rhs_outs_el@[rhs_ret_ve]) single_update_expr in
		if rhs_outs_el = [] then K.Apply(  inner_loop_body,incr_expr)	
		else          					 K.Iterate(inner_loop_body,incr_expr)	
	in
	
	(* In order to implement a statement we iterate over all the values *)
	(* of the input variables of the lhs collection, and for each of them *)
	(* we update the corresponding output tier. *)
	let statement_expr = 
			let outer_loop_body = lambda (lhs_ins_el@[existing_out_tier]) coll_update_expr in
			if lhs_ins_el = [] then K.Apply(  outer_loop_body,lhs_collection)
			else          					K.Iterate(outer_loop_body,lhs_collection)
	in
		(lhs_collection, statement_expr), nm


(**[m3_trig_to_k3_trig meta m3_trig]

   Transforms a M3 trigger into a K3 trigger. 
	@param meta    Metadata associated with the translation process. See [meta_t] definition above.	
	@param m3_trig The M3 trigger being translated.
	@return        The [m3_trig] translated into K3 and the updated meta.
*)
let m3_trig_to_k3_trig (meta: meta_t) (m3_trig: M3.trigger_t) : K.trigger_t * meta_t =
	let trig_args = Schema.event_vars m3_trig.M3.event in
	let k3_trig_stmts, new_meta = 
		List.fold_left 
				(fun (old_stms,om) m3_stmt -> 
							let k3_stmt, nm = m3_stmt_to_k3_stmt om trig_args m3_stmt in 
							(old_stms@[k3_stmt], nm) )
				([],([],snd meta))
				!(m3_trig.M3.statements) 
	in
  (m3_trig.M3.event, k3_trig_stmts), new_meta

(**[m3_to_k3 m3_program]

   Transforms a M3 program into a K3 program. 
	@param  m3_program The M3 program being translated.
	@return The [m3_program] translated into K3.
*)
let m3_to_k3 (m3_program : M3.prog_t) : (K.prog_t) =
	let {M3.maps = m3_prog_schema; M3.triggers = m3_prog_trigs } = m3_program in
	let k3_prog_schema = List.map m3_map_to_k3_map !m3_prog_schema in
	let patterns_map = Patterns.extract_patterns !m3_prog_trigs in
	let k3_prog_trigs, (_,sum_maps) = 
		List.fold_left
				(fun (old_trigs,om) m3_trig -> 
							let k3_trig,nm = m3_trig_to_k3_trig om m3_trig in
							(old_trigs@[k3_trig], nm) )
				([],empty_meta) 
				!m3_prog_trigs 
	
	(* The list of temporary maps required for performing sum operations is *)
	(* appended to the K3 program schema. *)
	in
	( k3_prog_schema@sum_maps, patterns_map, k3_prog_trigs )

