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
	
let init_val = K.Const(Types.CFloat(0.0))
			
	
let bind_for_apply_each trig_args vt_list e = 
  let non_trig_vt_list = 
		List.map (fun (v,t) -> if List.mem v trig_args then (gensym(),t) 
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
		
				
let bind_for_aggregate trig_args vt_list (iv,it) e = 
  let non_trig_vt_list = List.map (fun (v,t) ->
    if List.mem v trig_args then (gensym(),t) else (v,t)) vt_list in
  let vars = List.map fst non_trig_vt_list in
  if (unique vars) <> vars then
    failwith "invalid lambda with duplicate variables"
  else
  begin match non_trig_vt_list with 
    | [var,vart] -> K.AssocLambda(K.AVar(var,vart),	K.AVar(iv,it), e )
    | l          -> K.AssocLambda(K.ATuple(l),			K.AVar(iv,it), e )
  end	

let create_agg_fn trig_args vars (multpl_var,multpl_t) =
		let accv = next_accum_var () in 
		bind_for_aggregate 
				trig_args
				(varIdType_to_k3_idType (vars@[multpl_var,multpl_t])) 
				(accv,K.TBase(multpl_t))
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
		      (* TODO: schema+value types *)
		      let (inv,outv) = (List.map fst ins, List.map fst outs) in
					
		      let init_single = expr_is_singleton ~scope:trig_args init_aggecalc in
		      let init_k3_expr = m3rhs_to_k3_expr trig_args outv init_aggecalc in
					
					let new_metadata = metadata@[mapn, inv, outv] in
					let skip = List.mem (mapn, inv, outv) metadata in
					
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
  let aux ecalc expected_sch = 
      (ListAsSet.inter (calc_schema ecalc) expected_sch, 
       M3P.get_singleton ecalc) in
  let (outv1, c1_sing) = aux c1 expected_sch1 in
  let (outv2, c2_sing) = aux c2 expected_sch2 in
  let (schema, c_sing, c_prod) =
    calc_schema c, M3P.get_singleton c, M3P.get_product c
  in match (c_sing, c1_sing, c2_sing) with
    | (true, false, _) | (true, _, false) | (false, true, true) ->
            failwith "invalid parent singleton"
        
    | (true, _, _) -> 
         Debug.print "TRACK-K3-SINGLETON" (fun () ->
            (M3Common.PreparedPrinting.pretty_print_calc c)
         );
         let (meta,expr) = calc_to_singleton_expr trig_args metadata c
         in ([],meta,expr)
    | (false, true, false) | (false, false, true) ->
      (* TODO: types *)
      Debug.print "TRACK-K3-SINGLETON" (fun () ->
         (M3Common.PreparedPrinting.pretty_print_calc
             (if c1_sing then c1 else c2))
      );
      let outsch, meta, ce = 
         calc_to_expr trig_args metadata 
                      (if c1_sing then expected_sch2 else expected_sch1)
                      (if c1_sing then c2 else c1) in
      let meta2, inline = 
         calc_to_singleton_expr 
                      trig_args 
                      meta
                      (if c1_sing then c1 else c2) in
      let (v,v_t,l,r) =
        if c1_sing then "v2",TFloat,inline,Var("v2",TFloat)
        else "v1",TFloat,Var("v1",TFloat),inline in
      let (op_sch, op_result) = (op outsch l r) in
      let fn = bind_for_apply_each
        trig_args ((args_of_vars outsch)@[v,v_t]) op_result
      in 
         if outsch = [] then (op_sch, meta2, Apply(fn,ce))
         else                (op_sch, meta2, Map(fn, ce))
        
    | (false, false, false) ->
      (* TODO: types *)
      (* Note: there is no difference to the nesting whether this op
       * is a product or a join *)
      let sch1, meta, oute = calc_to_expr trig_args metadata expected_sch1 c1 in
      let sch2, meta2, ine = calc_to_expr trig_args meta     expected_sch2 c2 in
      let ret_schema = ListAsSet.union sch1 sch2 in
      let (l,r) = (Var("v1",TFloat), Var("v2",TFloat)) in
      let (op_sch, op_result) = (op ret_schema l r) in
      let applied_expr = 
         if (sch1 = []) && (sch2 = []) then
            Apply(bind_for_apply_each trig_args ["v1",TFloat] (
               Apply(bind_for_apply_each trig_args ["v2",TFloat] (
                  op_result
               ), ine)
            ), oute)
         else if (sch1 = []) || (sch2 = []) then
            let (singleton_v,singleton_e,set_v,set_e) = 
               if sch1 = [] 
               then "v1",oute,(args_of_vars sch2)@["v2",TFloat],ine
               else "v2",ine, (args_of_vars sch1)@["v1",TFloat],oute
            in
               Map(bind_for_apply_each trig_args set_v (
                  Apply(bind_for_apply_each trig_args [singleton_v,TFloat] (
                     op_result
                  ), singleton_e)
               ), set_e)
         else
            let nested = bind_for_apply_each
              trig_args ((args_of_vars sch2)@["v2",TFloat]) op_result in 
            let inner = bind_for_apply_each
              trig_args ((args_of_vars sch1)@["v1",TFloat]) (Map(nested, ine)) 
            in Flatten(Map(inner, oute))
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
       else outsch, 
            K.Tuple((varId_to_k3_expr outsch)@[op c1 c2])
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
                        (list_to_string (fun x->x) common_schema)^"; "^
                        (list_to_string (fun x->x) c2_in_schema)^
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
				  (* Note: no need for lambda construction since all in vars are bound. *)
		      (* TODO: schema+value types *)
		      let (inv,outv) = (List.map fst ins, List.map fst outs) in
					
		      let init_single = expr_is_singleton ~scope:trig_args init_aggecalc in
		      let init_k3_expr = m3rhs_to_k3_expr trig_args outv init_aggecalc in
					
					let new_metadata = metadata@[mapn, inv, outv] in
					let skip = List.mem (mapn, inv, outv) metadata in
					
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
				
		| _ -> failwith "Empty or Single operand to CalcRing.Sum or Prod."

    
    | M3.MapAccess((mapn, inv, outv, init_aggecalc),inline_agg) ->
      (* TODO: schema, value types *)
      let s_l = List.map (List.map (fun v -> (v,TFloat))) [inv; outv] in
      let (ins,outs) = (List.hd s_l, List.hd (List.tl s_l)) in
      let init_expr = m3rhs_to_expr trig_args outv init_aggecalc in
      if inline_agg then
         (outv, metadata, init_expr)
      else
      let map_expr = map_to_expr mapn ins outs in
      let singleton_init_code = 
        (M3P.get_singleton (M3P.get_ecalc init_aggecalc)) ||
        (M3P.get_full_agg (M3P.get_agg_meta init_aggecalc))
      in
      let patv = M3P.get_extensions (M3P.get_ecalc init_aggecalc) in
      let r =
        let skip = List.mem (mapn, inv, outv) metadata
        in map_access_to_expr skip map_expr init_expr
             (M3P.get_singleton calc) singleton_init_code patv in
      let retv = 
         if (Debug.active "PROJECTED-MAP-ACCESS")
         then ListAsSet.inter outv expected_schema
         else outv in
      (Debug.print "LOG-MAP-PROJECTIONS" (fun () ->
         "Access to : "^mapn^
         "\nDefault Outputs : "^(Util.list_to_string (fun x->x) outv)^
         "\nExpected Outputs : "^
            (Util.list_to_string (fun x->x) expected_schema)^
         "\nResult Outputs : "^(Util.list_to_string (fun x->x) retv)
      ));
      let new_metadata = metadata@[mapn,inv,outv] in
      if (ListAsSet.seteq retv outv)
         then (outv, new_metadata, r)
         else let projected = (
            let accv = next_accum_var () in
            let agg_fn = bind_for_aggregate 
               trig_args 
               (args_of_vars (outv@["v"]))
               (accv,TFloat)
               (Add(Var("v", TFloat), Var(accv, TFloat))) in
            let gb_fn = bind_for_apply_each 
               trig_args
               (args_of_vars (outv@["v"]))
               (Tuple(List.map (fun v -> Var(v, TFloat)) retv)) in
            if retv = [] 
            then Aggregate(agg_fn, (Const(M3.CFloat(0.))), r)
            else GroupByAggregate(agg_fn, (Const(M3.CFloat(0.))), gb_fn, r)
         ) in (retv, new_metadata, projected)
    
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
		assert (List.length (ListAsSet.inter trig_args  ins) = 0);
	  let trig_w_ins = ListAsSet.union trig_args  lhs_ins  in
	  let trig_w_lhs = ListAsSet.union trig_w_ins lhs_outs in
		
		let init_single = Calculus.expr_is_singleton ~scope:trig_w_lhs init_aggcalc in 
		let incr_single = Calculus.expr_is_singleton ~scope:trig_w_ins incr_aggcalc in
		
		let (incr_expr,init_expr) = pair_map 
						(fun calc single ->
		            (* Note: we can only use calc_to_singleton_expr if there are
		             * no loop or bigsum vars in calc *) 
		            (if single 
										then snd (calc_to_singleton_expr trig_args       [] calc)
		              	else      m3rhs_to_expr          trig_args lhs_outs calc))
            ((incr_aggcalc,incr_single), (init_aggcalc,init_single))
    in
    (incr_expr,init_expr)




let collection_trig (m3_trig: M3.trigger_t) : K.trigger_t =
	
	let (k3_event_type, k3_rel_name, k3_trig_args) = m3_event_to_k3_event m3_trig.M3.event 	in
	let k3_trig_stmts = List.map (collection_stmt (event_args m3_trig.M3.event)) !m3_trig.M3.statements 
	in
  (k3_event_type, k3_rel_name, k3_trig_args, 
   if (Debug.active "RUNTIME-BIGSUMS") then
       	List.filter (fun (c,_) -> match c with 
																	PC(_,_,_,_) -> false | InPC(_,_,_) -> false | _ -> true) 
										k3_trig_stmts
   else k3_trig_stmts
	)


let m3_to_k3 (( _, m3_prog_schema, m3_prog_trigs, _ ):M3.prog_t) : (K.progr_t) =
	 let k3_prog_schema = List.map m3_map_to_k3_map !m3_prog_schema in
   let patterns_map = Patterns.extract_patterns !m3_prog_trigs in
	 let k3_prog_trigs = List.map collection_trig !m3_prog_trigs in
			( k3_prog_schema, patterns_map,	k3_prog_trigs )
			
