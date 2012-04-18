(** Module for translating a M3 program into a K3 program. *)

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

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

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
	| _ -> failwith "invalid return type."
	end
		
let init_val ret_e = begin match ret_e with
	| K.Var(_,K.TBase(ret_type))   -> init_val_from_type ret_type
	| _ -> failwith "invalid return expression."
	end

let extract_opt opt =  begin match opt with
	| None -> failwith "Trying to extract some from None"
	| Some(s) -> s
	end

(**********************************************************************)
let unique l = List.fold_left (fun acc e -> if List.mem e acc then acc else acc@[e]) [] l	
let keys_from_kvars kvars  = List.map (fun kv -> (name_of_kvar kv,kv)) kvars

let exprs_to_tuple (el: K.expr_t list) = 
		begin match el with
		|  [] -> failwith "invalid empty expressions list"  
		| [e] -> e
		| _   -> K.Tuple(el)
		end

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
		
let lambda v_el body = K.Lambda( lambda_args v_el, body)
let assoc_lambda v1_el v2_el body = 
		K.AssocLambda( lambda_args v1_el, lambda_args v2_el, body )
						
let apply_lambda v_el el body = 
		assert ((List.length v_el) = (List.length el));
		K.Apply(lambda v_el body, (exprs_to_tuple el))
		
let project_fn from_v_el to_v_el = 
		lambda from_v_el (exprs_to_tuple to_v_el)	
							
let accum_prefix  = "accv_"
let accum_counter = ref 0
let next_accum_var() = incr accum_counter;
   										 accum_prefix^(string_of_int !accum_counter)
												
let aggregate_fn v_el multpl_e =
		let accv = next_accum_var () in 
		let multpl_t = snd (List.hd (k3_expr_to_k3_idType [multpl_e])) in
		let acce = K.Var( accv, multpl_t ) in
		assoc_lambda (v_el@[multpl_e]) [acce] (K.Add(acce, multpl_e))
													
(**********************************************************************)

let map_to_expr mapn ins outs map_type =
	  let (ins_k,outs_k) = pair_map varIdType_to_k3_idType (ins,outs) in
		let map_type_k = K.TBase(map_type) in
		begin match ins, outs with
	  | ([],[]) -> K.SingletonPC(mapn,             map_type_k)
	  | ([], x) ->       K.OutPC(mapn,      outs_k,map_type_k)
	  | ( x,[]) ->        K.InPC(mapn,ins_k,       map_type_k)
	  | ( x, y) ->          K.PC(mapn,ins_k,outs_k,map_type_k)
	  end

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
		
(**********************************************************************)


let map_access_to_expr mapn ins outs map_ret_t theta_vars_el init_expr_opt =
		
		let (ins_k,outs_k)   = pair_map varIdType_to_k3_idType (ins,outs) in
		let (ins_el,outs_el) = pair_map varIdType_to_k3_expr   (ins,outs) in
		let free_vars_el  = ListAsSet.diff outs_el theta_vars_el in
		let bound_vars_el = ListAsSet.diff outs_el free_vars_el in
		
		let (ins_tl,outs_tl) = pair_map varIdType_to_k3_type (ins,outs) in
		let map_ret_k = K.TBase(map_ret_t) in
		let map_out_t = if outs_tl = [] then map_ret_k
										else K.Collection(K.TTuple(outs_tl@[map_ret_k])) in
		
		let map_ret_ve = K.Var("map_ret",map_ret_k) in
		let map_out_ve = K.Var("slice",map_out_t) in
		let iv_e = K.Var("init_val", map_out_t) in
					
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
						else K.Map( project_fn (init_outs_el@[map_ret_ve]) (outs_el@[map_ret_ve]), ie)
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
let rec value_to_k3_expr (value_calc : V.expr_t) : K.type_t * K.expr_t =
	let bin_fn op_fn r1 c2 =
		let ret1_t, e1 = r1 in
		let ret2_t, e2 = value_to_k3_expr c2 in
		assert ((numerical_type ret1_t) && (numerical_type ret2_t));
		let ret_t =	if ret1_t = ret2_t  then		ret1_t
								else                         K.TBase(T.TFloat) 
		in 
		ret_t, (op_fn e1 e2)
	in	
	begin match value_calc with
				| V.Val( AConst(i) )  -> K.TBase(T.type_of_const i), K.Const(i)
		    | V.Val( AVar(v,t) )  -> K.TBase(t),                 K.Var(v,K.TBase(t))
		    | V.Val( AFn(fn,fargs,ftype) ) -> failwith "(* TODO: function M3 -> K3 *)"
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

(*                        Initialized_Patterns * Sum_Maps             *)
type meta_t = ((string * (T.var_t list)) list) * (K.map_t list)
let empty_meta = ([],[])

let meta_append_sum_map meta sum_map =
		(fst meta), (snd meta)@[sum_map]
		
let meta_has_init_pattern meta init_pattern = 
		List.mem init_pattern (fst meta)

let meta_append_init_pattern meta init_pattern =
		(fst meta)@[init_pattern],(snd meta)
		
(**********************************************************************)

let rec calc_to_k3_expr meta theta_vars_el calc : 
				((K.expr_t list * K.expr_t * K.expr_t) * meta_t) =
	let rcr        = calc_to_k3_expr meta  theta_vars_el in
	let rcr2 meta2 = calc_to_k3_expr meta2 theta_vars_el in
	
	let (ins,outs) = schema_of_expr calc in
	let ins_el = varIdType_to_k3_expr ins in
	assert ((ListAsSet.diff ins_el theta_vars_el) = []) ;
	
	let bin_fn op_fn c1 c2 =
		let ((ret1_t, e1),(ret2_t, e2)) = pair_map value_to_k3_expr (c1,c2) in
		assert (compatible_types ret1_t ret2_t);
		([], K.Var("v",K.TBase(T.TBool)), (op_fn e1 e2)), meta
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
						let nm, init_expr_opt = begin match eins, eouts with
							| [],[] -> meta, None
							| [],_  ->  if (Debug.active "M3TOK3-GENERATE-INIT") 
													then meta, Some([],init_val_from_type ext_type)
							           	else meta, None
							| _,_   -> 
								if (Debug.active "M3TOK3-GENERATE-INIT") && 
									 init_calc_opt != None && not (meta_has_init_pattern meta (mapn, eins)) then
											let init_calc = extract_opt init_calc_opt in
											let eouts_el = varIdType_to_k3_expr eouts in
											let init_theta_vars_el = ListAsSet.diff  theta_vars_el eouts_el in
											let (init_outs_el, init_ret_ve, init_expr), nm_1 = 
													calc_to_k3_expr meta  init_theta_vars_el init_calc in
											assert (ListAsSet.seteq init_outs_el eouts_el);
											assert ((type_of_kvar init_ret_ve) = K.TBase(ext_type));
											(meta_append_init_pattern nm_1 (mapn, eins)), 
											Some(init_outs_el,init_expr)
								else meta, None
						end					
						in					  
					  let (map_outs_el, map_ret_ve, expr) = 
							map_access_to_expr mapn eins eouts ext_type theta_vars_el init_expr_opt in
					  ((map_outs_el, map_ret_ve, expr), nm)
						
				| AggSum( agg_vars0, aggsum_calc ) ->
						let agg_vars0_el = varIdType_to_k3_expr agg_vars0 in
						
						let (aggsum_outs_el,ret_ve,aggsum_e),nm = rcr aggsum_calc in
						let agg_vars_el = ListAsSet.inter agg_vars0_el aggsum_outs_el in
						
						
						let agg_fn = aggregate_fn aggsum_outs_el ret_ve in
						let expr = 
								if agg_vars_el = [] then
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
						let lift_ve = K.Var("lift_v",type_of_kvar ret_ve) in
						let lift_ret_ve = K.Var("v",K.TBase(T.TInt)) in
						let expr =
								if lift_outs_el = [] then
										K.Singleton(K.Tuple([lift_e;one_flt_val]))
								else
										K.Map(lambda (lift_outs_el@[ret_ve]) 
															(K.Tuple(lift_outs_el@[ret_ve;one_flt_val])),
													lift_e)
						in
						((lift_outs_el@[lift_ve], lift_ret_ve, expr), nm)
			end
			
    | C.Sum( sum_args )	->
				let outs_el = ListAsSet.diff (varIdType_to_k3_expr outs) theta_vars_el in
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
					let sum_coll,sum_map = next_sum_tmp_coll outs ret_t in
					
					let reset_update = 
							K.PCValueUpdate(sum_coll, [], outs_el, init_val ret_ve) in
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





(** Converts a M3 statement into a K3 statement. *)
let collection_stmt (meta: meta_t) trig_args (m3_stmt: Plan.stmt_t) : K.statement_t * meta_t =
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
		
		(* Bound to the output tier corresponding to a particular input variables key.*)
		let existing_out_tier = K.Var("existing_out_tier",out_tier_t) in
		(* Determine the current value of the entry in "existing_out_tier" corresponding *)
		(* to a particular output variables key. *)
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
		
		let incr_result, nm = calc_to_k3_expr nm0 trig_w_ins_el incr_calc in
		let (rhs_outs_el, rhs_ret_ve, incr_expr) = incr_result in
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


(** Transforms a M3 trigger into a K3 trigger. *)
let collection_trig (meta: meta_t) (m3_trig: M3.trigger_t) : K.trigger_t * meta_t =
	let trig_args = Schema.event_vars m3_trig.M3.event in
	let k3_trig_stmts, new_meta = 
		List.fold_left 
				(fun (old_stms,om) m3_stmt -> 
							let k3_stmt, nm = collection_stmt om trig_args m3_stmt in 
							(old_stms@[k3_stmt], nm) )
				([],([],snd meta))
				!(m3_trig.M3.statements) 
	in
  (m3_trig.M3.event, k3_trig_stmts), new_meta

(** Transforms a M3 program into a K3 program. *)
let m3_to_k3 ({M3.maps = m3_prog_schema; 
							 M3.triggers = m3_prog_trigs }:M3.prog_t) : (K.prog_t) =
	let k3_prog_schema = List.map m3_map_to_k3_map !m3_prog_schema in
	let patterns_map = Patterns.extract_patterns !m3_prog_trigs in
	let k3_prog_trigs, (_,sum_maps) = 
		List.fold_left
				(fun (old_trigs,om) m3_trig -> 
							let k3_trig,nm = collection_trig om m3_trig in
							(old_trigs@[k3_trig], nm) )
				([],empty_meta) 
				!m3_prog_trigs 
	
	in
	( k3_prog_schema@sum_maps, patterns_map, k3_prog_trigs )

