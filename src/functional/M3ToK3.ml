open Arithmetic
open Calculus

module T = Types
module V = Arithmetic.ValueRing
module C = Calculus.CalcRing
module K = K3.SR



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
					
let type_of_kvar kvar = begin match kvar with
		| K.Var(v_name,vtype) -> vtype
		| _ -> failwith "K.Var expected."
	end

let m3_map_to_k3_map (m3_map: M3.map_t) : K.map_t = match m3_map with
		| M3.DSView(ds)                    -> 
				let (map_name, input_vars, output_vars, _, _) = Plan.expand_ds_name ds.Plan.ds_name 
				in 
				(map_name, List.map snd input_vars, List.map snd output_vars )
		| M3.DSTable(rel_name, rel_schema,_,_) -> 
				(rel_name, [], List.map snd rel_schema )



let m3_event_to_k3_event m3_event = match m3_event with
			| Schema.InsertEvent( rel_name, rel_schema, _, _ ) -> (K.Insert,rel_name, List.map fst rel_schema)
			| Schema.DeleteEvent( rel_name, rel_schema, _, _ ) -> (K.Delete,rel_name, List.map fst rel_schema)
			| _ -> failwith "Unable to convert 'SystemInitializedEvent' to a k3_event_type_t!"
				

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
	| K.TBase(T.TInt)   -> true
	| K.TBase(T.TFloat) -> true
	| _ -> false
	end

let init_val_from_type ret_type = begin match ret_type with
	| T.TInt   -> one_int_val
	| T.TFloat -> one_flt_val
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
let keys_from_vars vars  = List.map (fun (v,t) -> (v,(K.Var(v,K.TBase(t))))) vars

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
			
(**********************************************************************)


let map_access_to_expr mapn ins outs map_ret_t free_vars init_expr_opt =
		let bound_vars = ListAsSet.diff outs free_vars in
		
		let (ins_k,outs_k)   = pair_map varIdType_to_k3_idType (ins,outs) in
		let (ins_el,outs_el) = pair_map varIdType_to_k3_expr   (ins,outs) in
		let free_vars_el   = varIdType_to_k3_expr free_vars in
		
		let (ins_tl,outs_tl) = pair_map varIdType_to_k3_type (ins,outs) in
		let map_ret_k = K.TBase(map_ret_t) in
		let map_out_t = if outs_tl = [] then map_ret_k
										else K.Collection(K.TTuple(outs_tl@[map_ret_k])) in
		let map_t     = if ins_tl = [] then map_out_t
								    else K.Collection(K.TTuple(ins_tl@[map_out_t])) in
		
		let map_ret_ve = K.Var("map_ret",map_ret_k) in
		let map_out_ve = K.Var("slice",map_out_t) in
		let map_ve     = K.Var("m",map_t) in
		
		let slice_and_project coll_ve : K.expr_t =
			assert (free_vars <> []);
			if free_vars = outs then coll_ve
			else 
				K.Map( project_fn (outs_el@[map_ret_ve]) (free_vars_el@[map_ret_ve]), 
							 K.Slice(coll_ve, outs_k, keys_from_vars bound_vars))
		in
		let single_access_and_init_expr sch_el =	
				if init_expr_opt = None then K.Lookup(map_ve,sch_el)
	      else 
						let init_expr = extract_opt init_expr_opt in						
						let iv_e = K.Var("init_val", map_ret_k) in
						K.IfThenElse(K.Member(map_ve,sch_el), 
												 K.Lookup(map_ve,sch_el), 
												 apply_lambda [iv_e] [init_expr] 
														(K.Block([K.PCValueUpdate(map_ve, ins_el, outs_el, iv_e); 
																		iv_e])))
		in
		let expr = begin match ins, outs with
	  | ([],[]) -> K.SingletonPC(mapn, map_ret_k)
	  | ([], y) ->       
			let map_expr = K.OutPC(mapn, outs_k, map_ret_k) in
			let access_expr =	
				if free_vars = [] then single_access_and_init_expr outs_el      		
	      else                   slice_and_project map_ve
			in 
			apply_lambda [map_ve] [map_expr] access_expr
				
	  | ( x,[]) -> 
			let map_expr = K.InPC(mapn, ins_k, map_ret_k) in
			apply_lambda [map_ve] [map_expr] (single_access_and_init_expr ins_el)
											
  	| ( x, y) ->
			let map_expr = K.PC(mapn, ins_k, outs_k, map_ret_k) in
											
			let out_access_expr coll_ve =	
				if free_vars = [] then	K.Lookup(coll_ve,outs_el)
	      else                    slice_and_project coll_ve
			in
			
			let access_expr = 
				if init_expr_opt = None then 
						apply_lambda [map_out_ve] [K.Lookup(map_ve,ins_el)] (out_access_expr map_out_ve)
	      else 
						let init_expr = extract_opt init_expr_opt in						
						let iv_e = K.Var("init_val", map_out_t) in
						let init_block = K.Block([K.PCUpdate(map_expr, ins_el, iv_e);
																			out_access_expr iv_e])
						in
						K.IfThenElse( K.Member(map_ve,ins_el), 
							apply_lambda [map_out_ve] [K.Lookup(map_ve,ins_el)] (out_access_expr map_out_ve), 
							apply_lambda       [iv_e]              [init_expr]      init_block)
      in 
			apply_lambda [map_ve] [map_expr] access_expr
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
				| V.Sum( c1::c2::sum_args_tl )	->
						let add_fun e1 e2 = K.Add(e1,e2) in
						let r = bin_fn add_fun (value_to_k3_expr c1) c2 in
						List.fold_left (bin_fn add_fun) r sum_args_tl		    
				| V.Prod( c1::c2::prod_args_tl )	->	  
						let mult_fun e1 e2 = K.Mult(e1,e2) in
						let r = bin_fn mult_fun (value_to_k3_expr c1) c2 in
						List.fold_left (bin_fn mult_fun) r prod_args_tl
				| _  -> failwith "Empty or Single operand to V.Sum or Prod."
	end



(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

type meta_t = (string * (T.var_t list)) list

let rec calc_to_k3_expr meta theta_vars calc : (K.expr_t list * K.expr_t * K.expr_t * meta_t) =
	let rcr        = calc_to_k3_expr meta  theta_vars in
	let rcr2 meta2 = calc_to_k3_expr meta2 theta_vars in
	
	let ins,outs = schema_of_expr calc in
	assert ((ListAsSet.diff ins theta_vars) = []) ;
	
	let bin_fn op_fn c1 c2 =
		let ((ret1_t, e1),(ret2_t, e2)) = pair_map value_to_k3_expr (c1,c2) in
		assert (compatible_types ret1_t ret2_t);
		[], K.Var("v",K.TBase(T.TBool)), (op_fn e1 e2), meta
	in
  begin match calc with
		| C.Val( calc_val ) -> begin match calc_val with
				| Value( calc_val_value ) ->
						let ret_t, expr = value_to_k3_expr calc_val_value in
						[], K.Var("v",ret_t), expr, meta
				| Cmp( T.Eq, c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Eq  (e1,e2) ) c1 c2
				| Cmp( T.Lt, c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Lt  (e1,e2) ) c1 c2
		    | Cmp( T.Lte,c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Leq (e1,e2) ) c1 c2
				| Cmp( T.Gt, c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Leq (e1,e2) ) c2 c1
				| Cmp( T.Gte,c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Lt  (e1,e2) ) c2 c1
				| Cmp( T.Neq,c1, c2 )     -> bin_fn ( fun e1 e2 -> K.Neq (e1,e2) ) c1 c2
				
				| AggSum( agg_vars0, aggsum_calc ) ->
						let agg_vars0_el = varIdType_to_k3_expr agg_vars0 in
						
						let aggsum_outs_el,ret_ve,aggsum_e,nm = rcr aggsum_calc in
						let agg_vars_el = ListAsSet.inter agg_vars0_el aggsum_outs_el in
						
						
						let agg_fn = aggregate_fn aggsum_outs_el ret_ve in
						let expr = 
								if agg_vars_el = [] then
										K.Aggregate(agg_fn, init_val ret_ve, aggsum_e)
								else
										let gb_fn = project_fn (aggsum_outs_el@[ret_ve]) agg_vars_el in 
										K.GroupByAggregate(agg_fn, init_val ret_ve, gb_fn, aggsum_e)
						in
						(agg_vars_el, ret_ve, expr, nm)
										
				| Lift(lift_v, lift_calc) 	      -> 
						let lift_outs_el,ret_ve,lift_e,nm = rcr lift_calc in
						let lift_ve = K.Var("lift_v",type_of_kvar ret_ve) in
						let lift_ret_ve = K.Var("v",K.TBase(T.TFloat)) in
						let expr =
								if lift_outs_el = [] then
										K.Singleton(K.Tuple([lift_e;one_flt_val]))
								else
										K.Map(lambda (lift_outs_el@[ret_ve]) 
															(K.Tuple(lift_outs_el@[ret_ve;one_flt_val])),
													lift_e)
						in
						(lift_outs_el@[lift_ve], lift_ret_ve, expr, nm)
						
				| Rel(reln, rel_schema, rel_type) -> 
						let free_vars  = ListAsSet.diff  rel_schema theta_vars in
						let rel_outs_el, rel_ret_ve, expr = 
								map_access_to_expr reln [] rel_schema rel_type free_vars None in
					  (rel_outs_el, rel_ret_ve, expr, meta)
				
				| External(mapn, eins, eouts, ext_type, init_calc_opt) ->
						let free_vars  = ListAsSet.diff  eouts theta_vars in
												
						let nm, init_expr_opt = begin match eins, eouts with
							| [],[] -> meta, None
							| [],_  -> if List.mem (mapn, eouts) meta then meta, None
							           else   meta@[mapn, eouts], Some(init_val_from_type ext_type)
							| _,_   -> 
								if (Debug.active "M3ToK3-GENERATE-INIT") && 
									 init_calc_opt != None && not (List.mem (mapn, eins) meta) then
											let init_calc = extract_opt init_calc_opt in
											let init_theta_vars = ListAsSet.diff  theta_vars eouts in
											let init_outs_el, init_ret_ve, init_expr, nm_1 = 
													calc_to_k3_expr meta  init_theta_vars init_calc in
											assert (init_outs_el = (varIdType_to_k3_expr eouts));
											assert ((type_of_kvar init_ret_ve) = K.TBase(ext_type));
											nm_1@[mapn, eins], Some(init_expr)
								else meta, None
						end					
						in					  
					  let (map_outs_el, map_ret_ve, expr) = 
							map_access_to_expr mapn eins eouts ext_type free_vars init_expr_opt in
					  (map_outs_el, map_ret_ve, expr, nm)
			end
			
    | C.Sum( c1::c2::sum_args_tl )	->
				let outs_el = varIdType_to_k3_expr outs in
				let prepare_fn old_meta c = 
					let e_outs_el,e_ret_ve,e,new_meta = rcr2 old_meta c in
					assert (ListAsSet.seteq e_outs_el outs_el);
					(e_outs_el,e_ret_ve,e,new_meta)
					(*
					if ListAsSet.seteq e_outs outs then e
					else
						assert (ListAsSet.inter outs e_outs) = outs;
						if outs = [] then 
							new_meta, K.Lookup(e, varIdType_to_k3_expr e_outs)
						else
							new_meta, K.Map( project_fn (e_outs@[multpl_v]) (outs@[multpl_v]), e)
					*)
				in
				
				let (e1_outs_el,e1_ret_ve,e1,nm1) = prepare_fn meta c1 in
				let (e2_outs_el,e2_ret_ve,e2,nm2) = prepare_fn nm1  c2 in
				let nm,sum_exprs_tl = 
						List.fold_left (fun (old_meta,old_el) c -> 
																let e_outs_el,e_ret_ve,e,new_meta = prepare_fn old_meta c in
																new_meta,old_el@[e_outs_el,e_ret_ve,e]) 
													 (nm2,[]) 
													 sum_args_tl in
				
				let sum_fn (s1_outs_el,s1_ret_ve,s1) (s2_outs_el,s2_ret_ve,s2) =
					let ret1_t,ret2_t = pair_map type_of_kvar (s1_ret_ve,s2_ret_ve) in
					assert ((numerical_type ret1_t) && (numerical_type ret2_t));
					let ret_t =	if ret1_t = ret2_t  then		ret1_t
											else                         K.TBase(T.TFloat) 
					in
					let ret_ve = K.Var("sum",ret_t) in
					let expr = 
						if outs_el = [] then K.Add(s1,s2)
						else
								let s2_outs_tl = k3_expr_to_k3_type s2_outs_el in
								let sum_t = K.Collection(K.TTuple(s2_outs_tl@[ret2_t])) in
								let s2_ve = K.Var( "s2", sum_t ) in
								let sum_e = 
									K.Apply(lambda [s2_ve]
														(K.IfThenElse( K.Member(s2_ve,s2_outs_el), 
																					K.Add(s1_ret_ve,K.Lookup(s2_ve,s2_outs_el)), 
																					s1_ret_ve)),
													s2) 
								in
								K.Map( lambda (s1_outs_el@[s1_ret_ve]) (K.Tuple(s1_outs_el@[sum_e])) ,						
											 s1)
					in
					(s1_outs_el, ret_ve, expr)
				in
				let r = sum_fn (e1_outs_el,e1_ret_ve,e1) (e2_outs_el,e2_ret_ve,e2) in
				let sum_outs_el, sum_ret_ve, sum_expr = List.fold_left sum_fn r sum_exprs_tl in
				(sum_outs_el, sum_ret_ve, sum_expr, nm)
    
		| C.Prod( c1::c2::prod_args_tl )	->	  
				let (e1_outs_el,e1_ret_ve,e1,nm1) = rcr2 meta c1 in
    		let (e2_outs_el,e2_ret_ve,e2,nm2) = rcr2 nm1  c2 in
				let nm,prod_exprs_tl = List.fold_left 
																	(fun (old_meta,old_el) c -> 
																				let e_outs_el,e_ret_ve,e,new_meta = rcr2 old_meta c in
																				new_meta,old_el@[e_outs_el,e_ret_ve,e])
																	(nm2,[]) 
																	prod_args_tl in
				
				let prod_fn (p1_outs_el,p1_ret_ve,p1) (p2_outs_el,p2_ret_ve,p2) =
					let ret1_t,ret2_t = pair_map type_of_kvar (p1_ret_ve,p2_ret_ve) in
					assert ((numerical_type ret1_t) && (numerical_type ret2_t));
					let ret_t =	if ret1_t = ret2_t  then		ret1_t
											else                         K.TBase(T.TFloat) 
					in
					let ret_ve = K.Var("prod",ret_t) in
					
					let p_outs_el,expr = begin match p1_outs_el,p2_outs_el with
					| [],[] -> [], K.Mult(p1,p2)
					|  _,[] -> p1_outs_el,
										 K.Map( lambda (p1_outs_el@[p1_ret_ve])	(K.Tuple(p1_outs_el@[K.Mult(p1_ret_ve,p2)])) ,						
														p1)
					| [], _ -> p2_outs_el,
										 K.Map( lambda (p2_outs_el@[p2_ret_ve])	(K.Tuple(p2_outs_el@[K.Mult(p2_ret_ve,p1)])) ,						
														p2)
					|  _, _ -> 
						let union_el = (ListAsSet.union p1_outs_el p2_outs_el) in
						let prod_e = K.Tuple(union_el@[K.Mult(p1_ret_ve,p2_ret_ve)]) in
						let nested = K.Map(lambda (p2_outs_el@[p2_ret_ve]) prod_e, p2) in 
             union_el, K.Flatten ( K.Map(lambda (p1_outs_el@[p1_ret_ve]) nested, p1) )
					end
					in
					(p_outs_el, ret_ve, expr)
				in
				
				let r = prod_fn (e1_outs_el,e1_ret_ve,e1) (e2_outs_el,e2_ret_ve,e2) in
				let prod_outs_el, prod_ret_ve, prod_expr = List.fold_left prod_fn r prod_exprs_tl in		
				(prod_outs_el, prod_ret_ve, prod_expr, nm)
				
		| C.Neg( neg_arg ) ->
				rcr (C.Prod( [(C.Val(Value(V.Val(AConst(T.CInt(-1))))));neg_arg] ))
				
		| _ -> failwith "Empty or Single operand to C.Sum or Prod."
  end 


(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)






let collection_stmt trig_args (m3stmt: Plan.stmt_t) : K.statement_t =
		let (mapn, lhs_ins, lhs_outs, map_type, init_calc_opt) = Plan.expand_ds_name m3stmt.Plan.target_map in
		let {Plan.update_type = update_type; Plan.update_expr = incr_calc} = m3stmt in 
		
		let (lhs_ins_el,lhs_outs_el) = pair_map varIdType_to_k3_expr   (lhs_ins, lhs_outs) in
		let (lhs_ins_tl,lhs_outs_tl) = pair_map varIdType_to_k3_type   (lhs_ins, lhs_outs) in
		
		let map_k3_type = K.TBase(map_type) in
    let out_tier_t  = if lhs_outs = [] then map_k3_type 
											else K.Collection(K.TTuple(lhs_outs_tl@[map_k3_type])) in
    let collection  = map_to_expr mapn lhs_ins lhs_outs map_type in
    
		(* all lhs input variables must be free, they cannot be bound by trigger args *)
		assert ((ListAsSet.inter trig_args  lhs_ins) = []);
	  let trig_w_ins = ListAsSet.union trig_args  lhs_ins  in
		let rhs_outs_el,rhs_ret_ve,incr_expr,_ = calc_to_k3_expr [] trig_w_ins incr_calc in
		let trig_args_el = varIdType_to_k3_expr trig_args in
		assert (ListAsSet.seteq (ListAsSet.diff lhs_outs_el trig_args_el) rhs_outs_el);
		assert (compatible_types (type_of_kvar rhs_ret_ve) map_k3_type);
		
    let update_expr existing_slice =
			let existing_v =
				if update_type = Plan.ReplaceStmt then (init_val_from_type map_type)
				else if lhs_outs_el = []          then existing_slice
				else
						if (Debug.active "M3ToK3-GENERATE-INIT") && init_calc_opt != None then
							let init_calc = extract_opt init_calc_opt in
							let trig_w_lhs = ListAsSet.union trig_w_ins lhs_outs in
							let init_outs_el,init_ret_ve,init_expr,_ = calc_to_k3_expr [] trig_w_lhs init_calc in
							assert (init_outs_el = []);
							assert (compatible_types (type_of_kvar init_ret_ve) map_k3_type);
							
							K.IfThenElse( K.Member(existing_slice, lhs_outs_el),
	           								K.Lookup(existing_slice, lhs_outs_el),
	           								init_expr )
						else
							K.Lookup(existing_slice, lhs_outs_el)
			in
			if rhs_outs_el = [] then					
					K.PCValueUpdate( collection, lhs_ins_el, lhs_outs_el, K.Add(existing_v,incr_expr) )
			else
					K.Iterate(lambda (rhs_outs_el@[rhs_ret_ve]) 
													(K.PCValueUpdate( collection, lhs_ins_el, lhs_outs_el, K.Add(existing_v,rhs_ret_ve) )), 
										incr_expr)
		in
		let statement_expr = 
				if lhs_ins_el = [] then (update_expr collection)
				else
					let le = K.Var("existing_slice",out_tier_t) in	
					K.Iterate(lambda (lhs_ins_el@[le]) (update_expr le), 
										collection)
    in
		(collection, statement_expr) 



let collection_trig (m3_trig: M3.trigger_t) : K.trigger_t =
	
	let (k3_event_type, k3_rel_name, k3_trig_args) = m3_event_to_k3_event m3_trig.M3.event 	in
	let trig_args = Schema.event_vars m3_trig.M3.event in
	let k3_trig_stmts = List.map (collection_stmt trig_args) !(m3_trig.M3.statements) 
	in
  (k3_event_type, k3_rel_name, k3_trig_args, k3_trig_stmts)


let m3_to_k3 ({M3.maps = m3_prog_schema; M3.triggers = m3_prog_trigs }:M3.prog_t) : (K.prog_t) =
	 let k3_prog_schema = List.map m3_map_to_k3_map !m3_prog_schema in
   let patterns_map = Patterns.extract_patterns !m3_prog_trigs in
	 let k3_prog_trigs = List.map collection_trig !m3_prog_trigs in
			( k3_prog_schema, patterns_map,	k3_prog_trigs )