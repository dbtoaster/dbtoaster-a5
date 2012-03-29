open Calculus
open Arithmetic

module K = K3.SR


let pair_map pair_fn (a,b) = (pair_fn a, pair_fn b)

let varIdType_to_k3_type   (vars : Types.var_t list) = List.map (fun (v,t) -> K.TBase(t)) vars
let varIdType_to_k3_idType (vars : Types.var_t list) = List.map (fun (v,t) -> v,K.TBase(t)) vars
let varIdType_to_k3_expr   (vars : Types.var_t list) = List.map (fun (v,t) -> K.Var(v,K.TBase(t))) vars
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

let float_t = Types.TFloat
let float_k3_t = K.TBase(float_t)
let init_val = K.Const(Types.CFloat(0.0))
			
	
let bind_for_apply_each (trig_args: Types.var_t list) vt_list e = 
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
    | [var,vart] -> K.Lambda(K.AVar(var,K.TBase(vart)), e)
    | l          -> K.Lambda(K.ATuple(varIdType_to_k3_idType l), e)
  end


let create_proj_fn trig_args from_vars to_vars = 
		bind_for_apply_each 
        trig_args
        from_vars
        (K.Tuple(varIdType_to_k3_expr to_vars))		
		
				
let bind_for_aggregate (trig_args: Types.var_t list) vt_list (iv,it) e = 
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
			K.AssocLambda(K.AVar(var,K.TBase(vart)), K.AVar(iv,K.TBase(it)), e )
    | l          -> 
			K.AssocLambda(K.ATuple(varIdType_to_k3_idType l),	K.AVar(iv,K.TBase(it)), e )
  end	

let create_agg_fn trig_args vars (multpl_var,multpl_t) =
		let accv = next_accum_var () in 
		bind_for_aggregate 
				trig_args
				(vars@[multpl_var,multpl_t]) 
				(accv,multpl_t)
     		(K.Add(K.Var(multpl_var, K.TBase(multpl_t)), 
									 K.Var(accv, K.TBase(multpl_t))))
    
(**********************************************************************)

let map_to_expr mapn ins outs map_type =
  (* TODO: value types *)
	let (ins_k,outs_k) = pair_map varIdType_to_k3_idType (ins,outs) in
  begin match ins, outs with
  | ([],[]) -> K.SingletonPC(mapn,map_type)
  | ([], x) -> K.OutPC(mapn,outs_k,map_type)
  | (x, []) -> K.InPC(mapn,ins_k,map_type)
  | (x,y)   -> K.PC(mapn,ins_k,outs_k,map_type)
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
														 apply_lambda_block iv_e (K.Const(Types.CFloat(0.0))) init_block)
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
let rec calc_to_singleton_k3_expr (trig_args: Types.var_t list) metadata (calc:expr_t) =
  let rcr = calc_to_singleton_k3_expr trig_args metadata in
  let bin_op f c1 c2 =
    let nm,c1r = rcr c1 in
    let nm2, c2r = calc_to_singleton_k3_expr trig_args nm c2
    in nm2, f c1r c2r
  in 
  begin match calc with
		| CalcRing.Val( calc_val ) -> begin match calc_val with
				| Value( calc_val_value ) -> begin match calc_val_value with
						| ValueRing.Val( AConst(i) )  -> metadata, K.Const(i)
				    | ValueRing.Val( AVar(v,t) )  -> metadata, K.Var(v,K.TBase(t))
				    | ValueRing.Val( AFn(fn,fargs,ftype) ) -> failwith "(* TODO: function M3 -> K3 *)"
						| ValueRing.Neg( neg_arg ) -> 
								let nm, neg_arg_k = rcr (CalcRing.Val(Value(neg_arg))) in
								nm, K.Mult (K.Const(Types.CFloat(-1.0)),neg_arg_k)
						| ValueRing.Sum( c1::sum_arg2::sum_args_tl )	->
								let c2 = if sum_args_tl = [] then sum_arg2
												 else ValueRing.Sum( sum_arg2::sum_args_tl ) in
								bin_op (fun x y -> K.Add (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))						
				    
						| ValueRing.Prod( c1::prod_arg2::prod_args_tl )	->	  
								let c2 = if prod_args_tl = [] then prod_arg2
										 else ValueRing.Prod( prod_arg2::prod_args_tl ) in
								bin_op (fun x y -> K.Mult (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
						| _  -> failwith "Empty or Single operand to ValueRing.Sum or Prod."
		    end
				| Cmp( Types.Eq, c1, c2 )   -> bin_op (fun x y -> K.Eq  (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Lt, c1, c2 )   -> bin_op (fun x y -> K.Lt  (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
		    | Cmp( Types.Lte,c1, c2 )   -> bin_op (fun x y -> K.Leq (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Gt, c1, c2 )   -> bin_op (fun x y -> K.Leq (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Gte,c1, c2 )   -> bin_op (fun x y -> K.Lt  (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Neq,c1, c2 )   -> bin_op (fun x y -> K.Neq (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				
				| AggSum( [], aggsum_calc )       -> failwith "(* TODO: metadata, (m3rhs_to_k3_expr trig_args [] aggsum_calc) *)"
				| AggSum( agg_vars, aggsum_calc ) -> failwith "(* Aggsum is not singleton *)"		
				| Lift(lift_v, lift_calc) 	      -> failwith "(* TODO: Lift *)"			
		
				| Rel(reln, rel_schema, rel_type) -> failwith "(* Relation is not singleton.*)"		
				| External(mapn, ins, outs, ext_type, None) -> failwith "(* TODO: External with init_calc = None *)"	
		    | External(mapn, ins, outs, ext_type, Some(init_aggecalc)) ->
				  (* Note: no need for lambda construction since all in vars are bound. *)
		      					
		      let init_single = expr_is_singleton ~scope:trig_args init_aggecalc in
		      let init_k3_expr = m3rhs_to_k3_expr trig_args outs init_aggecalc in
					
					let new_metadata = metadata@[mapn, ins, outs] in
					let skip = List.mem (mapn, ins, outs) metadata in
					
					let map_expr = map_to_expr mapn ins outs (K.TBase(ext_type)) in			   
					let r = map_access_to_expr skip map_expr init_k3_expr true init_single []
					in new_metadata, r
			end
    | CalcRing.Sum( c1::sum_arg2::sum_args_tl )	->
				let c2 = if sum_args_tl = [] then sum_arg2
								 else CalcRing.Sum( sum_arg2::sum_args_tl ) in
				bin_op (fun x y -> K.Add (x,y)) c1 c2							
    
		| CalcRing.Prod( c1::prod_arg2::prod_args_tl )	->	  
				let c2 = if prod_args_tl = [] then prod_arg2
						 else CalcRing.Prod( prod_arg2::prod_args_tl ) in
				bin_op (fun x y -> K.Mult (x,y)) c1 c2	            
    
		| CalcRing.Neg( neg_arg ) ->
				let nm, neg_arg_k = rcr neg_arg in
				nm, K.Mult (K.Const(Types.CFloat(-1.0)),neg_arg_k)
		| _ -> failwith "Empry or Single operand to CalcRing.Sum or Prod."
  end

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

and op_to_k3_expr trig_args metadata (expected_sch1,expected_sch2) op c c1 c2 =
  let aux (calc, expected_sch) = ListAsSet.inter (snd (schema_of_expr calc)) expected_sch	
	in	
	let (  outs1,   outs2) = pair_map aux ((c1,expected_sch1),(c2,expected_sch2)) in
	let (c1_sing, c2_sing) = pair_map expr_is_singleton (c1,c2) in
  let (schema,   c_sing) = (snd (schema_of_expr c)), expr_is_singleton c
  in match (c_sing, c1_sing, c2_sing) with
    | (true, false, _) | (true, _, false) | (false, true, true) ->
            failwith "invalid parent singleton"
        
    | (true, _, _) -> 
    		Debug.print "TRACK-K3-SINGLETON" (fun () -> string_of_expr c );
        let (meta,expr) = calc_to_singleton_k3_expr trig_args metadata c in 
				([],meta,expr)
				
    | (false, true, false) | (false, false, true) ->
				let child_sing,child_coll = (if c1_sing then (c1,c2) else (c2,c1)) in
				
				Debug.print "TRACK-K3-SINGLETON" (fun () -> string_of_expr child_sing);
    		let outsch, meta, ce = 
        	calc_to_k3_expr trig_args metadata 
                      (if c1_sing then expected_sch2 else expected_sch1)
                      child_coll in
			  let meta2, inline = 
			     calc_to_singleton_k3_expr trig_args meta child_sing in
					
				let (v,v_t,left,right) = if c1_sing 
						then "v2",float_t,inline,K.Var("v2",float_k3_t)
						else "v1",float_t,K.Var("v1",float_k3_t),inline in
				let (op_sch, op_result) = (op outsch left right) in
				  
				let fn = bind_for_apply_each trig_args (outsch@[v,v_t]) op_result
				in 
						if outsch = [] then (op_sch, meta2, K.Apply(fn,ce))
						else                (op_sch, meta2,  K.Map(fn, ce))
        
    | (false, false, false) ->
      (* Note: there is no difference to the nesting whether this op
       * is a product or a join *)
      let sch1, meta, oute = calc_to_k3_expr trig_args metadata expected_sch1 c1 in
      let sch2, meta2, ine = calc_to_k3_expr trig_args meta     expected_sch2 c2 in
      let ret_schema = ListAsSet.union sch1 sch2 in
			
      let (l,r) = (K.Var("v1",float_k3_t), K.Var("v2",float_k3_t)) in
      let (op_sch, op_result) = (op ret_schema l r) in
			
      let applied_expr = 
         if (sch1 = []) && (sch2 = []) then
            K.Apply(bind_for_apply_each trig_args ["v1",float_t] (
               K.Apply(bind_for_apply_each trig_args ["v2",float_t] (
                  op_result
               ), ine)
            ), oute)
         else if (sch1 = []) || (sch2 = []) then
            let (singleton_v,singleton_e,set_v,set_e) = 
               if sch1 = [] 
               then "v1",oute,sch2@["v2",float_t],ine
               else "v2", ine,sch1@["v1",float_t],oute
            in
               K.Map(bind_for_apply_each trig_args set_v (
                  K.Apply(bind_for_apply_each trig_args [singleton_v,float_t] (
                     op_result
                  ), singleton_e)
               ), set_e)
         else
            let nested = bind_for_apply_each trig_args (sch2@["v2",float_t]) op_result in 
            let inner  = bind_for_apply_each trig_args (sch1@["v1",float_t]) (K.Map(nested, ine)) 
            in K.Flatten(K.Map(inner, oute))
      in op_sch, meta2, applied_expr

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

and calc_to_k3_expr trig_args metadata expected_schema calc =
	let rcr = calc_to_k3_expr trig_args metadata expected_schema in
  let tuple op schema c1 c2 =
    let outsch = if Debug.active "NARROW-SCHEMAS" 
                 then (ListAsSet.inter expected_schema schema)
                 else schema
    in if outsch = []
       then [], (op c1 c2)
       else outsch, K.Tuple((varIdType_to_k3_expr outsch)@[op c1 c2])
  in 
  let bin_op f c1 c2 = (
    op_to_k3_expr trig_args 
               metadata 
               (* Figure out the expected schema for the left and right
                  hand sides of the expressions.  This starts with the
                  schema that the outer expression expects:
                  For both, we have to add any join terms -- variables present
                  on both sides of the expression.
                  Then we also have to take any output variables from the lhs
                  that are expected by the rhs and add them to the lhs expected
                  schema. 
                  Finally, we take out all the trigger arguments
                  *)
               (  let join_schema = ListAsSet.inter (snd (schema_of_expr c1)) 
                                                    (snd (schema_of_expr c2)) in
                  let common_schema = 
                     ListAsSet.diff (ListAsSet.union expected_schema 
                                                     join_schema)
                                    trig_args 
									in
                  let c2_in_schema = (fst (schema_of_expr c2)) in
                     Debug.print "BIN-OP-SCHEMA" (fun () -> 
                        (string_of_expr calc)^
                        (ListExtras.ocaml_of_list (fun (x,t)->x) common_schema)^"; "^
                        (ListExtras.ocaml_of_list (fun (x,t)->x) c2_in_schema)^
                        (string_of_expr c2)
                     );
                     (  ListAsSet.union common_schema c2_in_schema, 
                        common_schema ))
               (tuple f) 
               calc c1 c2
    ) in 
  begin match calc with
		| CalcRing.Val( calc_val ) -> begin match calc_val with
				| Value( calc_val_value ) -> begin match calc_val_value with
						| ValueRing.Val( AConst(i) )  -> [], metadata, K.Const(i)
				    | ValueRing.Val( AVar(v,t) )  -> [], metadata, K.Var(v,K.TBase(t))
				    | ValueRing.Val( AFn(fn,fargs,ftype) ) -> failwith "(* TODO: function M3 -> K3 *)"
						| ValueRing.Neg( neg_arg ) -> 
								let no, nm, neg_arg_k = rcr (CalcRing.Val(Value(neg_arg))) in
								no, nm, K.Mult (K.Const(Types.CFloat(-1.0)),neg_arg_k)
						| ValueRing.Sum( c1::sum_arg2::sum_args_tl )	->
								let c2 = if sum_args_tl = [] then sum_arg2
												 else ValueRing.Sum( sum_arg2::sum_args_tl ) in
								bin_op (fun x y -> K.Add (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))						
				    
						| ValueRing.Prod( c1::prod_arg2::prod_args_tl )	->	  
								let c2 = if prod_args_tl = [] then prod_arg2
										 else ValueRing.Prod( prod_arg2::prod_args_tl ) in
								bin_op (fun x y -> K.Mult (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
						| _  -> failwith "Empty or Single operand to ValueRing.Sum or Prod."
		    end
				| Cmp( Types.Eq, c1, c2 )   -> bin_op (fun x y -> K.Eq  (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Lt, c1, c2 )   -> bin_op (fun x y -> K.Lt  (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
		    | Cmp( Types.Lte,c1, c2 )   -> bin_op (fun x y -> K.Leq (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Gt, c1, c2 )   -> bin_op (fun x y -> K.Leq (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Gte,c1, c2 )   -> bin_op (fun x y -> K.Lt  (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				| Cmp( Types.Neq,c1, c2 )   -> bin_op (fun x y -> K.Neq (x,y)) (CalcRing.Val(Value(c1))) (CalcRing.Val(Value(c2)))
				
				| AggSum( agg_vars, aggsum_calc ) -> failwith "(* TODO: metadata, (m3rhs_to_k3_expr trig_args agg_vars aggsum_calc) *)"
				| Lift(lift_v, lift_calc) 	      -> failwith "(* TODO: Lift *)"			
		
				| Rel(reln, rel_schema, rel_type) -> failwith "(* TODO: Relation *)"		
				| External(mapn, ins, outs, ext_type, None) -> failwith "(* TODO: External with init_calc = None *)"	
		    | External(mapn, ins, outs, ext_type, Some(init_aggecalc)) ->
					  let init_k3_expr = m3rhs_to_k3_expr trig_args outs init_aggecalc in
					  let map_expr     = map_to_expr mapn ins outs (K.TBase(ext_type)) in
					  
						let incr_single = (expr_is_singleton ~scope:trig_args calc)          in
					  let init_single = (expr_is_singleton ~scope:trig_args init_aggecalc) in
						let patv = [] (* M3P.get_extensions (M3P.get_ecalc init_aggecalc) *) in
					  						
						let skip = List.mem (mapn, ins, outs) metadata in
					  let r = map_access_to_expr skip map_expr init_k3_expr incr_single init_single patv in
					  let rets = 
					     if (Debug.active "PROJECTED-MAP-ACCESS")
					     then ListAsSet.inter outs expected_schema
					     else outs in
								(Debug.print "LOG-MAP-PROJECTIONS" (fun () ->
									"Access to : "^mapn^
									"\nDefault Outputs : "^(ListExtras.string_of_list (fun (x,t)->x) outs)^
									"\nExpected Outputs : "^(ListExtras.string_of_list (fun (x,t)->x) expected_schema)^
									"\nResult Outputs : "^(ListExtras.string_of_list (fun (x,t)->x) rets)
								));
					  let new_metadata = metadata@[mapn,ins,outs] in
					  if (ListAsSet.seteq rets outs)
					     then (outs, new_metadata, r)
					     else let projected = (
					        let agg_fn = create_agg_fn trig_args outs ("v",float_t) in
								  let gb_fn  = create_proj_fn trig_args (outs@["v",float_t]) rets in
													
						      if rets = [] 
						      then K.Aggregate(agg_fn, init_val, r)
						      else K.GroupByAggregate(agg_fn, init_val, gb_fn, r)
								 ) in (rets, new_metadata, projected)
			end
    | CalcRing.Sum( c1::sum_arg2::sum_args_tl )	->
				let c2 = if sum_args_tl = [] then sum_arg2
								 else CalcRing.Sum( sum_arg2::sum_args_tl ) in
				bin_op (fun x y -> K.Add (x,y)) c1 c2							
    
		| CalcRing.Prod( c1::prod_arg2::prod_args_tl )	->	  
				let c2 = if prod_args_tl = [] then prod_arg2
						 else CalcRing.Prod( prod_arg2::prod_args_tl ) in
				bin_op (fun x y -> K.Mult (x,y)) c1 c2	            
    
		| CalcRing.Neg( neg_arg ) ->
				let no, nm, neg_arg_k = rcr neg_arg in
				no, nm, K.Mult (K.Const(Types.CFloat(-1.0)),neg_arg_k)
				
		| _ -> failwith "Empty or Single operand to CalcRing.Sum or Prod."
  end 

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
and m3rhs_to_k3_expr trig_args lhs_outs calc : K.expr_t =
   (* invokes calc_to_k3_expr on calc.
    * based on aggregate metadata:
    * -- simply uses the collection
    * -- applies bigsums to collection.
    *    we want to apply structural recursion optimizations in this case.
    * -- projects to lhs vars 
    *)
		let multpl_v = ("v",Types.TFloat) in
		let rhs_outs, _, rhs_expr = (calc_to_k3_expr trig_args [] lhs_outs calc) in
    let agg_fn = create_agg_fn trig_args rhs_outs multpl_v in
		
		let calc_single = expr_is_singleton ~scope:trig_args calc in
		let doesnt_need_aggregate = (ListAsSet.seteq rhs_outs lhs_outs) in
		
    if calc_single || (rhs_outs = []) then rhs_expr
    else if doesnt_need_aggregate then 
		(
      if rhs_outs = lhs_outs then rhs_expr
      else K.Map( create_proj_fn trig_args 
											(rhs_outs@[multpl_v]) 
											(lhs_outs@[multpl_v]),
               			  rhs_expr)
    )
    else if lhs_outs = [] then (* is_full_agg*)
        K.Aggregate(agg_fn, init_val, rhs_expr) 
    else
        (* projection to lhs vars + aggregation *)
        let gb_fn = create_proj_fn trig_args (rhs_outs@[multpl_v]) lhs_outs in 
				K.GroupByAggregate(agg_fn, init_val, gb_fn, rhs_expr)
				


(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)


(* Statement expression construction:
 * -- creates structural representations of the the incr and init value exprs,
 *    composes them in an update expression, merging in the case of delta slices.
 * -- invokes the update expression w/ existing values/slices, binding+looping
 *    persistent collection input vars, and applying updates.
*)
let collection_stmt trig_args (m3stmt: Plan.stmt_t) : K.statement_t =
    (* Helpers 
    let schema vars = List.map (fun v -> (v,TFloat)) vars in
    let vars_expr vars = List.map (fun v -> Var(v,TFloat)) vars in
		*)
    let fn_arg_expr v t = (v,t),K.Var(v,t) in

		(* alpha4:
    let ((mapn, lhs_inv, lhs_outv, init_aggcalc), stmt_type, incr_aggcalc, sm) = m3stmt *) 
		let (mapn, lhs_ins, lhs_outs, map_type, Some(init_aggcalc)) = Plan.expand_ds_name m3stmt.Plan.target_map in
		
		let (lhs_inv,   lhs_outv)  = (List.map         fst lhs_ins, List.map         fst lhs_outs) in
    let (lhs_in_el, lhs_out_el)= (varIdType_to_k3_expr lhs_ins, varIdType_to_k3_expr lhs_outs) in
		let {Plan.update_type = update_type; Plan.update_expr = incr_aggcalc} = m3stmt in 
		
		(* all lhs input variables must be free, they cannot be bound by trigger args *)
		assert (List.length (ListAsSet.inter trig_args  lhs_ins) = 0);
	  let trig_w_ins = ListAsSet.union trig_args  lhs_ins  in
	  let trig_w_lhs = ListAsSet.union trig_w_ins lhs_outs in
		
		let init_single = Calculus.expr_is_singleton ~scope:trig_w_lhs init_aggcalc in 
		let incr_single = Calculus.expr_is_singleton ~scope:trig_w_ins incr_aggcalc in
		
		let (incr_expr,init_expr) = pair_map 
						(fun (calc,single) ->
		            (* Note: we can only use calc_to_singleton_expr if there are
		             * no loop or bigsum vars in calc *) 
		            (if single 
										then snd (calc_to_singleton_k3_expr trig_args       [] calc)
		              	else      m3rhs_to_k3_expr          trig_args lhs_outs calc))
            ((incr_aggcalc,incr_single), (init_aggcalc,init_single))
    in
    
		(* TODO: use typechecker to compute types here *)
    let out_tier_t = Collection(TTuple(List.map snd outs@[TFloat])) in

    let collection = map_to_expr mapn ins outs in
    
    (* Update expression, singleton initializer expression
     * -- update expression: increments the current var/slice by computing
     *    a delta. For slices, merges the delta slice w/ the current.
     * -- NOTE: returns the increment for the delta slice only, not the
     *    entire current slice. Thus each incremented entry must be updated
     *    in the persistent store, and not the slice as a whole. 
     * -- singleton initializer: for singleton deltas, directly computes
     *    the new value by combining (inline) the initial value and delta
     *)
    let (update_expr, sing_init_expr) =
        let zero_init = Const(CFloat(0.0)) in
        let singleton_aux init_e =
            let ca,ce = fn_arg_expr "current_v" TFloat in
            (Lambda(AVar(fst ca, snd ca),Add(ce,incr_expr)), init_e)
        in
        let slice_t = Collection(TTuple((List.map snd outs)@[TFloat])) in
        let slice_aux init_f =
            let ca,ce = fn_arg_expr "current_slice" slice_t in
            let ma,me = fn_arg_expr "dv" TFloat in
            (* merge fn: check if the delta exists in the current slice and
             * increment, otherwise compute an initial value and increment  *)
            let merge_body =
                let build_entry e = Tuple(out_el@[e]) in 
                IfThenElse(Member(ce,out_el),
                    build_entry (Add(Lookup(ce, out_el), me)),
                    build_entry (init_f me))
            in
            let merge_fn = bind_for_apply_each
              trig_args ((args_of_vars lhs_outv)@[ma]) merge_body in
            (* slice update: merge current slice with delta slice *)
            let merge_expr = Map(merge_fn, incr_expr)
            in (Lambda(AVar(fst ca, snd ca),merge_expr), zero_init)
        in
        match (incr_single, init_single) with
        | (true,true) -> singleton_aux (Add(incr_expr,init_expr))

        | (true,false) ->
            (* Note: we don't need to bind lhs_outv for init_expr since
             * incr_expr is a singleton, implying that lhs_outv are all
             * bound vars.
             * -- TODO: how can the init val be a slice then? if there are
             *    no loop out vars, and all in vars are bound, and all bigsums 
             *    are fully aggregated, this case should never occur. 
             *    Check this. *)
            (* Look up slice w/ out vars for init lhs expr *)
            singleton_aux (Add(Lookup(init_expr,out_el), incr_expr))

        | (false,true) -> slice_aux (fun me -> Add(init_expr,me))
        
        | (false,false) ->
            (* All lhs_outv are bound in the map function for the merge,
             * making loop lhs_outv available to the init expr. *)
            slice_aux (fun me -> Add(Lookup(init_expr, out_el), me))
    in
    
    (* Statement expression:
     * -- statement_expr is a side-effecting expression that updates a
     *    persistent collection, thus has type Unit *)

    let loop_in_aux (la,le) loop_fn_body =
        let patv = Util.ListAsSet.inter lhs_inv trig_args in
        let pat_ve = List.map (fun v -> (v,Var(v,TFloat))) patv in
        if (List.length patv) = (List.length lhs_inv)
          then 
            Apply(bind_for_apply_each trig_args [la] loop_fn_body,
                  Lookup(collection, List.map snd pat_ve))
          else
            (*(print_endline ("looping over slices with trig args "^
              (String.concat "," lhs_inv)^" / "^(String.concat "," trig_args));*)
            Iterate(bind_for_apply_each
                      trig_args ((args_of_vars lhs_inv)@[la]) loop_fn_body,
                    Slice(collection, ins, pat_ve))
    in
    let loop_update_aux ins outs delta_slice =
        let ua,ue = fn_arg_expr "updated_v" TFloat in
        let update_body = map_value_update_expr collection ins outs ue in
        let loop_update_fn = bind_for_apply_each
            trig_args ((args_of_vars lhs_outv)@[ua]) update_body
        in Iterate(loop_update_fn, delta_slice)
    in
    let rhs_basic_merge collection_expr =
      match stmt_type with
         | M3.Stmt_Update -> Apply(update_expr, collection_expr)
         | M3.Stmt_Replace -> incr_expr
    in
    let rhs_test_merge collection_expr =
      match stmt_type with
         | M3.Stmt_Update -> 
            IfThenElse(Member(collection_expr, out_el),
                       Apply(update_expr, Lookup(collection_expr, out_el)),
                       sing_init_expr)
         | M3.Stmt_Replace -> incr_expr
    in
    let statement_expr = 
        begin match lhs_inv, lhs_outv, incr_single with
        | ([],[],false) -> failwith "invalid slice update on a singleton map"

        | ([],[],_) ->
            let rhs_expr = rhs_basic_merge collection
            in map_value_update_expr collection [] [] rhs_expr

        | (x,[],false) -> failwith "invalid slice update on a singleton out tier"

        | (x,[],true) ->
            let la,le = fn_arg_expr "existing_v" TFloat in
            let rhs_expr = rhs_basic_merge le
            in loop_in_aux (la,le)
                 (map_value_update_expr collection in_el [] rhs_expr) 

        | ([],x,false) ->
            (* We explicitly loop, updating each incremented value in the rhs slice,
             * since the rhs_expr is only the delta slice and not a full merge with
             * the current slice.
             *)
            let rhs_expr = rhs_basic_merge collection
            in loop_update_aux [] out_el rhs_expr

        | ([],x,true) ->
            let rhs_expr = rhs_test_merge collection
            in map_value_update_expr collection [] out_el rhs_expr

        | (x,y,false) ->
            (* Use a value update loop to persist the delta slice *)
            let la,le = fn_arg_expr "existing_slice" out_tier_t in
            let rhs_expr = rhs_basic_merge le in
            let update_body = loop_update_aux in_el out_el rhs_expr
            in loop_in_aux (la,le) update_body

        | (x,y,true) ->
            let la,le = fn_arg_expr "existing_slice" out_tier_t in
            let rhs_expr = rhs_test_merge le
            in loop_in_aux (la,le)
                (map_value_update_expr collection in_el out_el rhs_expr)
        end
    in
		(collection, statement_expr) 




let collection_trig (m3_trig: M3.trigger_t) : K.trigger_t =
	
	let (k3_event_type, k3_rel_name, k3_trig_args) = m3_event_to_k3_event m3_trig.M3.event 	in
	let k3_trig_stmts = List.map (collection_stmt (Schema.event_vars m3_trig.M3.event)) !(m3_trig.M3.statements) 
	in
  (k3_event_type, k3_rel_name, k3_trig_args, 
   if (Debug.active "RUNTIME-BIGSUMS") then
       	List.filter (fun (c,_) -> match c with 
																	K.PC(_,_,_,_) -> false | K.InPC(_,_,_) -> false | _ -> true) 
										k3_trig_stmts
   else k3_trig_stmts
	)


let m3_to_k3 ({M3.maps = m3_prog_schema; M3.triggers = m3_prog_trigs }:M3.prog_t) : (K.prog_t) =
	 let k3_prog_schema = List.map m3_map_to_k3_map !m3_prog_schema in
   let patterns_map = Patterns.extract_patterns !m3_prog_trigs in
	 let k3_prog_trigs = List.map collection_trig !m3_prog_trigs in
			( k3_prog_schema, patterns_map,	k3_prog_trigs )
			
