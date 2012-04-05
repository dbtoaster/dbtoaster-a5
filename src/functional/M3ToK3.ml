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



let unique l =
  List.fold_left (fun acc e -> if List.mem e acc then acc else acc@[e]) [] l
	
(* Code generator backend uses __v and __y, avoid these. *)
let sym_prefix  = "__t"
let sym_counter = ref 0
let gensym() =  incr sym_counter; 
								sym_prefix^(string_of_int !sym_counter)

let accum_prefix  = "accv_"
let accum_counter = ref 0
let next_accum_var() = incr accum_counter;
   										 accum_prefix^(string_of_int !accum_counter)

let float_t = T.TFloat
let float_k3_t = K.TBase(float_t)
let zero_val = K.Const(T.CFloat(0.0))
let  one_val = K.Const(T.CFloat(1.0))
	
let bind_for_apply_each (trig_args: T.var_t list) (vt_list: K.schema) e = 
  let non_trig_vt_list = 
		List.map (fun (v,t) -> if List.mem_assoc v trig_args then (gensym(),t) 
													 else (v,t)) 
							vt_list 
	in
  let vars = List.map fst non_trig_vt_list in
  if (unique vars) <> vars then
    failwith "invalid lambda with duplicate variables"
  else
  begin match non_trig_vt_list with 
    | [var,vart] -> K.Lambda(K.AVar(var,vart), e)
    | l          -> K.Lambda(K.ATuple(l), e)
  end


let create_proj_fn trig_args from_vars to_vars = 
		bind_for_apply_each 
        trig_args
        (varIdType_to_k3_idType from_vars)
        (K.Tuple(varIdType_to_k3_expr to_vars))		
		
				
let bind_for_aggregate (trig_args: T.var_t list) (vt_list: K.schema) (iv,it) e = 
  let non_trig_vt_list = 
		List.map (fun (v,t) -> if List.mem_assoc v trig_args then (gensym(),t) 
													 else (v,t)) 
							vt_list 
	in
  let vars = List.map fst non_trig_vt_list in
  if (unique vars) <> vars then
    failwith "invalid lambda with duplicate variables"
  else
  begin match non_trig_vt_list with 
    | [var,vart] -> 
			K.AssocLambda(K.AVar(var,vart), K.AVar(iv,it), e )
    | l          -> 
			K.AssocLambda(K.ATuple(l),	K.AVar(iv,it), e )
  end	

let create_agg_fn trig_args vars (multpl_var,multpl_t) =
		let accv = next_accum_var () in 
		bind_for_aggregate 
				trig_args
				(varIdType_to_k3_idType vars@[multpl_var,K.TBase(multpl_t)]) 
				(accv,K.TBase(multpl_t))
     		(K.Add(K.Var(multpl_var, K.TBase(multpl_t)), 
							 K.Var(      accv, K.TBase(multpl_t))))
    
(**********************************************************************)

let map_to_expr mapn ins outs map_type =
  (* TODO: value types *)
	let (ins_k,outs_k) = pair_map varIdType_to_k3_idType (ins,outs) in
	let map_type_k = K.TBase(map_type) in
	begin match ins, outs with
  | ([],[]) -> K.SingletonPC(mapn,             map_type_k)
  | ([], x) ->       K.OutPC(mapn,      outs_k,map_type_k)
  | (x, []) ->        K.InPC(mapn,ins_k,       map_type_k)
  | (x,y)   ->          K.PC(mapn,ins_k,outs_k,map_type_k)
  end

(**********************************************************************)

let map_value_update_expr map_expr ine_l oute_l ve =
  begin match map_expr with
  | K.SingletonPC _ -> K.PCValueUpdate(map_expr, [], [], ve)
  | K.OutPC _ -> K.PCValueUpdate(map_expr, [], oute_l, ve)
  | K.InPC _ -> K.PCValueUpdate(map_expr, ine_l, [], ve)
  | K.PC _ -> K.PCValueUpdate(map_expr, ine_l, oute_l, ve)
  | _ -> failwith "invalid map expression for value update" 
  end

(**********************************************************************)

let map_update_expr map_expr e_l te =
  begin match map_expr with
  | K.OutPC _ -> K.PCUpdate(map_expr, [], te)
  | K.InPC _  -> K.PCUpdate(map_expr, [], te)
  | K.PC _    -> K.PCUpdate(map_expr, e_l, te)
  | _ -> failwith "invalid map expression for update"
  end
							
(**********************************************************************)
	
let lambda_vars (v_name,vtype) = K.AVar(v_name,vtype),K.Var(v_name,vtype)
let keys_from_pattern sch pat  = List.map (fun v -> let t = List.assoc v sch in (v,(K.Var(v,t)))) pat

type k_var_expr

let apply_lambda_block v_e e block = begin match v_e with
		| K.Var(v_name,vtype) -> K.Apply(K.Lambda(K.AVar(v_name,vtype), K.Block(block)), e)
		| _ -> failwith "Invalid argument to apply_lambda_block."
		end

let map_access_to_expr skip_init map_expr init_k3_expr incr_single init_single out_patv =
		begin match map_expr with		
		| K.SingletonPC _ -> map_expr
		
		| K.OutPC (id,outs,map_ret_t) -> 			
			let oute_l = k3_idType_to_k3_expr outs in
			let outt_l = k3_idType_to_k3_type outs in
		
			let map_t = K.Collection(K.TTuple(outt_l@[map_ret_t])) in
			let map_va,map_ve = lambda_vars("slice",map_t) in
			
			let iv_e = if init_single then K.Var("init_val", map_ret_t)
								 else                K.Var("init_val", map_t) in
			
			let access_expr =
	      if incr_single then (
						if (not (Debug.active "NO-DEDUP-IVC")) && skip_init then
		      			K.Lookup(map_ve,oute_l)
		      	else
								let init_block = if init_single then 
										[K.PCValueUpdate(map_expr, [], oute_l, iv_e); iv_e]
								else
										[K.PCUpdate(map_expr, [], iv_e); K.Lookup(iv_e, oute_l)]				
								in			
			        	K.IfThenElse(K.Member(map_ve,oute_l), K.Lookup(map_ve,oute_l), 
														 apply_lambda_block iv_e (K.Const(T.CFloat(0.0))) init_block)
				)
	      else 
						K.Slice(map_ve, outs, keys_from_pattern outs out_patv)
	    in
			K.Apply(K.Lambda(map_va, access_expr), map_expr)
				
		| K.InPC (id,ins,map_ret_t) -> 
			let ine_l = k3_idType_to_k3_expr ins in
			let int_l = k3_idType_to_k3_type ins in
			
			let map_t = K.Collection(K.TTuple(int_l@[map_ret_t])) in
			let map_va,map_ve = lambda_vars("slice",map_t) in
			
			let iv_e = if init_single then K.Var("init_val", map_ret_t)
								 else                K.Var("init_val", map_t) in
			
			if Debug.active "RUNTIME-BIGSUMS" then
					  (* If we're being asked to compute bigsums at runtime, then
					  we don't need to test for membership -- this is always false *)
		      if (not init_single && incr_single)
		      then K.Lookup(init_k3_expr, ine_l)
		      else init_k3_expr
	    else 
					let access_expr =
						if incr_single then (
					      if (not (Debug.active "NO-DEDUP-IVC")) && skip_init then
					      		K.Lookup(map_ve,ine_l)
					      else 
										let init_block = if init_single then
												[K.PCValueUpdate(map_expr, ine_l, [], iv_e); iv_e]
										else
												[K.PCUpdate(map_expr, [], iv_e); K.Lookup(iv_e, ine_l)]	in	
					        	K.IfThenElse(K.Member(map_ve,ine_l), K.Lookup(map_ve,ine_l), 
																 apply_lambda_block iv_e init_k3_expr init_block)
						)
			      else 
								K.Slice(map_ve, ins, keys_from_pattern ins out_patv)
					in 
					K.Apply(K.Lambda(map_va, access_expr), map_expr)
					
		| K.PC (id,ins,outs,map_ret_t) -> 
			let (ine_l,oute_l) = pair_map k3_idType_to_k3_expr (ins,outs) in
			let (int_l,outt_l) = pair_map k3_idType_to_k3_type (ins,outs) in
			
			let map_out_t = K.Collection(K.TTuple(outt_l@[map_ret_t])) in
			let map_out_va,map_out_ve = lambda_vars("slice",map_out_t) in
			
			let map_t = K.Collection(K.TTuple(int_l@[map_out_t])) in
			let map_va,map_ve = lambda_vars("m",map_t) in
			
			let iv_e = if init_single then K.Var("init_val", map_ret_t)
							 	 else                K.Var("init_val", map_out_t) in
							
			if Debug.active "RUNTIME-BIGSUMS" then
				  (* If we're being asked to compute bigsums at runtime, then
				  we don't need to test for membership -- this is always false *)
					if init_single && not incr_single then					K.Singleton(K.Tuple(oute_l@[init_k3_expr]))
					else if not init_single && incr_single then			K.Lookup(init_k3_expr, oute_l)
					else                                            init_k3_expr
			else
				  let init_block =
						begin match init_single, incr_single with								
							| true, true -> 	[K.PCValueUpdate(map_expr, ine_l, oute_l, iv_e); iv_e]
							| true, false ->  [K.PCValueUpdate(map_expr, ine_l, oute_l, iv_e); K.Singleton(K.Tuple(oute_l@[iv_e]))]
							| false, true ->  [K.PCUpdate(map_expr, ine_l, iv_e); K.Lookup(iv_e, oute_l)]
							| false, false -> [K.PCUpdate(map_expr, ine_l, iv_e); iv_e]
						end
					in
				  						
		     	let out_access_expr =
							if incr_single then (
									if (not (Debug.active "NO-DEDUP-IVC")) && skip_init then
					      		K.Lookup(map_out_ve,oute_l)
					      else 
					        	K.IfThenElse(K.Member(map_out_ve,oute_l), K.Lookup(map_out_ve,oute_l), 
																 apply_lambda_block iv_e init_k3_expr init_block)
							)
							else 
									K.Slice(map_out_ve,outs,keys_from_pattern outs out_patv)						    
				  in
					
					let access_expr = 
							K.IfThenElse(K.Member(map_ve,ine_l), 
													 K.Apply(K.Lambda(map_out_va, out_access_expr), K.Lookup(map_ve,ine_l)), 
													 apply_lambda_block iv_e init_k3_expr init_block)
		      in K.Apply(K.Lambda(map_va,access_expr), map_expr)
		
		| _ -> failwith "invalid map for map access"
		end

		
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
let rec value_to_k3_expr value_calc =
	begin match value_calc with
				| V.Val( AConst(i) )  -> K.Const(i)
		    | V.Val( AVar(v,t) )  -> K.Var(v,K.TBase(t))
		    | V.Val( AFn(fn,fargs,ftype) ) -> failwith "(* TODO: function M3 -> K3 *)"
				| V.Neg( neg_arg ) ->    K.Mult (K.Const(T.CFloat(-1.0)), value_to_k3_expr neg_arg)
				| V.Sum( c1::c2::sum_args_tl )	->
						let r = K.Add(value_to_k3_expr c1, value_to_k3_expr c2) in
						List.fold_left (fun acc c -> K.Add(acc, value_to_k3_expr c)) r sum_args_tl		    
				| V.Prod( c1::c2::prod_args_tl )	->	  
						let r = K.Mult(value_to_k3_expr c1, value_to_k3_expr c2) in
						List.fold_left (fun acc c -> K.Mult(acc, value_to_k3_expr c)) r prod_args_tl
				| _  -> failwith "Empty or Single operand to V.Sum or Prod."
	end



(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

let rec calc_to_k3_expr trig_args theta_vars metadata expected_schema map_type calc =
	let rcr = calc_to_k3_expr trig_args metadata expected_schema map_type in
	let rcr2 meta = calc_to_k3_expr trig_args meta expected_schema map_type in
	
	let ins,outs = schema_of_expr calc in
	assert (ListAsSet.diff ins theta_vars) = [] ;
	
	let multpl_v = ("v",map_type) in
	
	
  begin match calc with
		| C.Val( calc_val ) -> begin match calc_val with
				| Value( calc_val_value ) -> [], metadata, (value_to_k3_expr calc_val_value)
				| Cmp( T.Eq, c1, c2 )     -> [], metadata, K.Eq  (value_to_k3_expr c1, value_to_k3_expr c2)
				| Cmp( T.Lt, c1, c2 )     -> [], metadata, K.Lt  (value_to_k3_expr c1, value_to_k3_expr c2)
		    | Cmp( T.Lte,c1, c2 )     -> [], metadata, K.Leq (value_to_k3_expr c1, value_to_k3_expr c2)
				| Cmp( T.Gt, c1, c2 )     -> [], metadata, K.Leq (value_to_k3_expr c1, value_to_k3_expr c2)
				| Cmp( T.Gte,c1, c2 )     -> [], metadata, K.Lt  (value_to_k3_expr c1, value_to_k3_expr c2)
				| Cmp( T.Neq,c1, c2 )     -> [], metadata, K.Neq (value_to_k3_expr c1, value_to_k3_expr c2)
				
				| AggSum( agg_vars, aggsum_calc ) ->
						let aggsum_outs,nm,aggsum_e = rcr aggsum_calc in
						assert (ListAsSet.inter agg_vars aggsum_outs) = agg_vars;
						
						let agg_fn = create_agg_fn trig_args aggsum_outs multpl_v in
						let expr = 
								if agg_vars = [] then
										K.Aggregate(agg_fn, init_val, aggsum_e)
								else
										let gb_fn = create_proj_fn trig_args (aggsum_outs@[multpl_v]) agg_vars in 
										K.GroupByAggregate(agg_fn, init_val, gb_fn, aggsum_e)
						in
						(outs, nm, expr)
										
				| Lift(lift_v, lift_calc) 	      -> 
						let lift_outs,nm,lift_e = rcr aggsum_calc in
						let expr =
								if lift_outs = [] then
										K.Singleton(K.Tuple(lift_e,one_val))
								else
										K.Map(bind_for_apply_each trig_args (lift_outks@[multpl_kv]) 
															K.Tuple(lift_out_el@[multpl_e;one_val]),
													lift_e)
						in
						(outs, nm, expr)
						
				| Rel(reln, rel_schema, rel_type) -> failwith "(* TODO: Relation *)"		
				
				| External(mapn, eins, eouts, ext_type, init_calc_opt) ->
					  let skip = List.mem (mapn, eins, eouts) metadata in
					  let nm   = metadata@[mapn, eins, eouts] in
					  
					  let map_expr = map_to_expr mapn eins eouts ext_type in
					  let     expr = map_access_to_expr skip map_expr init_calc_opt (eouts = []) in
					  (outs, nm, expr)
			end
			
    | C.Sum( c1::c2::sum_args_tl )	->
				let prepare_fn old_meta c = 
					let e_outs,new_meta,e = rcr2 old_meta c in
					if ListAsSet.seteq e_outs outs then e
					else
						assert (ListAsSet.inter outs e_outs) = outs;
						if outs = [] then 
							new_meta, K.Lookup(e, varIdType_to_k3_expr e_outs)
						else
							new_meta, K.Map( create_proj_fn trig_args (e_outs@[multpl_v]) (outs@[multpl_v]), e)
				in
				
				let (nm1,e1) = prepare_fn metadata c1 in
				let (nm2,e2) = prepare_fn nm1 c2 in
				let nm,sum_exprs_tl = List.fold_left 
																	(fun (old_meta,old_el) c -> 
																			let new_meta,e = prepare_fn old_meta c in
																			new_meta,old_el@[e]) 
																	(nm2,[]) 
																	sum_args_tl in
				
				let sum_fn s1 s2 =
					if out_el = [] then K.Add(s1,s2)
					else
						let sum_e = K.IfThenElse( K.Member(s2,out_el), 
																			K.Add(multpl_e,K.Lookup(s2,out_el)), 
																			multpl_e) in
						K.Map( bind_for_apply_each trig_args (outks@[multpl_kv]) 
											K.Tuple(out_el@[sum_e]) ,						
									 s1)
				in
				let r = sum_fn e1 e2 in
				(outs, nm, List.fold_left sum_fn r sum_exprs_tl)
    
		| C.Prod( c1::prod_arg2::prod_args_tl )	->	  
				let (sch1,nm1,e1) = rcr2 metadata c1 in
    		let (sch2,nm2,e2) = rcr2 nm1 c2 in
				let nm,prod_exprs_tl = List.fold_left 
																	(fun (old_meta,old_el) c -> 
																			let sch,new_meta,e = rcr2 old_meta c in
																			new_meta,old_el@[sch,e])
																	(nm2,[]) 
																	prod_args_tl in
				
				let prod_fn (sch_p1,p1) (sch_p2,p2) = begin match sch_p1,sch_p2 with
					| [],[] -> K.Mult(p1,p2)
					|  _,[] -> K.Map( bind_for_apply_each trig_args (sch_p1_ks@[multpl1_kv]) 
																K.Tuple(sch_p1_el@[K.Mult(multpl1_e,p2)]) ,						
														p1)
					| [], _ -> K.Map( bind_for_apply_each trig_args (sch_p2_ks@[multpl2_kv]) 
																K.Tuple(sch_p2_el@[K.Mult(p1,multpl2_e)]) ,						
														p2)
					|  _, _ -> 
						let prod_e = K.Tuple((ListAsSet.union sch_p1_el sch_p2_el)@[K.Mult(multpl1_e,multpl2_e)]) in
						let nested = K.Map(bind_for_apply_each trig_args (sch_p2_ks@[multpl2_kv]) prod_e, p2) in 
             K.Flatten ( K.Map(bind_for_apply_each trig_args (sch_p1_ks@[multpl1_kv]) nested, p1) )
					end
				in
				
				let r = prod_fn (sch1,e1) (sch2,e2) in
				(outs, nm, List.fold_left prod_fn r prod_exprs_tl)		
						
		| C.Neg( neg_arg ) ->
				rcr C.Prod( [(C.Val(Value(V.Val(AConst(T.CFloat(-1.0))))));neg_arg] )
				
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
		
		let (lhs_inks,  lhs_outks)  = pair_map varIdType_to_k3_idType (lhs_ins, lhs_outs) in
		let (lhs_in_el, lhs_out_el) = pair_map varIdType_to_k3_expr   (lhs_ins, lhs_outs) in
		
		let map_k3_type = K.TBase(map_type) in
    let out_tier_t  = if lhs_outs = [] then map_k3_type 
											else K.Collection(K.TTuple(lhs_outks@[map_k3_type])) in
    let collection  = map_to_expr mapn lhs_ins lhs_outs map_k3_type in
    
		(* all lhs input variables must be free, they cannot be bound by trigger args *)
		assert ((ListAsSet.inter trig_args  lhs_ins) = []);
	  let trig_w_ins = ListAsSet.union trig_args  lhs_ins  in
		let rhs_outs,_,incr_expr = calc_to_k3_expr trig_args trig_w_ins [] lhs_outs map_type calc in
		assert (ListAsSet.seteq (ListAsSet.diff lhs_outs trig_args) (ListAsSet.diff rhs_outs trig_args));
		
    let update_expr existing_slice =
			let existing_v =
				if update_type = Plan.ReplaceStmt then init_val
				else if lhs_out_el = []           then existing_slice
				else
						if (Debug.active "M3ToK3-GENERATE-INIT") && init_calc_opt != None then
							let Some(init_calc) = init_calc_opt in
							let trig_w_lhs = ListAsSet.union trig_w_ins lhs_outs in
							let init_outs,_,init_expr = calc_to_k3_expr trig_args trig_w_lhs [] lhs_outs map_type init_calc in
							assert (ListAsSet.diff init_outs trig_w_lhs) = [];
							let sing_init_expr = if ( init_outs = [] ) then init_expr
																	 else                       K.Lookup(init_expr, lhs_out_el) in
							K.IfThenElse( K.Member(existing_slice, lhs_out_el),
	           								K.Lookup(existing_slice, lhs_out_el),
	           								sing_init_expr )
						else
							K.Lookup(existing_slice, lhs_out_el)
			in
			if rhs_outs = [] then					
					map_value_update_expr collection lhs_in_el lhs_out_el K.Add(existing_v,incr_expr)
			else
					let uv = ("update",map_k3_type) in
					let ue = K.Var("update",map_k3_type) in	
					let rhs_outks = varIdType_to_k3_idType rhs_outs in
					K.Iterate(bind_for_apply_each trig_args (rhs_outks@[uv]) 
													map_value_update_expr collection lhs_in_el lhs_out_el K.Add(existing_v,ue), 
										incr_expr)
		in
		let statement_expr = 
				if lhs_ins = [] then (update_expr collection)
				else
					let lv = ("existing_slice",out_tier_t) in
					let le = K.Var("existing_slice",out_tier_t) in	
					K.Iterate(bind_for_apply_each trig_args (lhs_inks@[lv]) (update_expr le), 
										collection)
    in
		(collection, statement_expr) 



let collection_trig (m3_trig: M3.trigger_t) : K.trigger_t =
	
	let (k3_event_type, k3_rel_name, k3_trig_args) = m3_event_to_k3_event m3_trig.M3.event 	in
	let k3_trig_stmts = List.map (collection_stmt (Schema.event_vars m3_trig.M3.event)) !(m3_trig.M3.statements) 
	in
  (k3_event_type, k3_rel_name, k3_trig_args, k3_trig_stmts)


let m3_to_k3 ({M3.maps = m3_prog_schema; M3.triggers = m3_prog_trigs }:M3.prog_t) : (K.prog_t) =
	 let k3_prog_schema = List.map m3_map_to_k3_map !m3_prog_schema in
   let patterns_map = Patterns.extract_patterns !m3_prog_trigs in
	 let k3_prog_trigs = List.map collection_trig !m3_prog_trigs in
			( k3_prog_schema, patterns_map,	k3_prog_trigs )