open Util

type m3_var_t      = Types.var_t
type m3_map_t      = M3.map_t
type m3_schema_t   = m3_map_t list
type m3_stmt_t     = Plan.stmt_t
type m3_trigger_t  = M3.trigger_t
type m3_prog_t     = M3.prog_t

(**********************************************************************************)

type k3_var_id_t = string
type k3_map_id_t = string

type k3_var_type_t = VT_String | VT_Int | VT_Float
type k3_var_t      = k3_var_id_t  (*  * k3_var_type_t *)

type k3_map_t      = k3_map_id_t * (k3_var_type_t list) * (k3_var_type_t list)
type k3_schema_t   = k3_map_t list


type k3_stmt_t = k3_expr_t * k3_expr_t

type k3_event_type_t = Insert | Delete
type k3_rel_id_t     = string
type k3_trigger_t    = k3_event_type_t * k3_rel_id_t * k3_var_t list * k3_stmt_t list

type k3_prog_t = k3_schema_t * M3Common.Patterns.pattern_map * k3_trigger_t list

(**********************************************************************************)

let m3_var_to_k3_var ((m3_var_name, m3_var_type) : m3_var_t) : k3_var_t = m3_var_name

let m3_var_to_k3_var_type ((m3_var_name, m3_var_type) : m3_var_t) : k3_var_type_t = 
	match m3_var_type with
		| Types.TInt       -> K3.SR.VT_Int
		| Types.TFloat     -> K3.SR.VT_Float
		| Types.TString(_) -> K3.SR.VT_String
		| _ -> failwith "Unable to convert Types.type_t to k3_var_type_t!"
		

let m3_map_to_k3_map (m3_map: m3_map_t) : k3_map_t = match m3_map with
		| M3.DSView(ds_name, _)                    -> 
				let (map_name, input_vars, output_vars, _, _) = Plan.expand_ds_name ds_name 
				in 
				(map_name, List.map m3_var_to_k3_var_type input_vars,
							     List.map m3_var_to_k3_var_type output_vars )
		| M3.DSTable(rel_name, rel_schema,_,_) -> 
				(rel_name, [], List.map m3_var_to_k3_var_type rel_schema )

let m3_schema_to_k3_schema (m3_schema: m3_schema_t) : k3_schema_t =
		List.map m3_map_to_k3_map m3_schema

let m3_event_to_k3_event m3_event = match m3_event with
			| InsertEvent( rel_name, rel_schema, _, _ ) -> (Insert,rel_name, List.map m3_var_to_k3_var rel_schema)
			| DeleteEvent( rel_name, rel_schema, _, _ ) -> (Delete,rel_name, List.map m3_var_to_k3_var rel_schema)
			| _ -> failwith "Unable to convert 'SystemInitializedEvent' to a k3_event_type_t!"
				
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

(* TODO: types *)
let varId_to_varIdType   vars = List.map (fun v -> v,TFloat)            vars
let varIdType_to_k3_expr vars = List.map (fun v -> K3.SR.Var(v))        vars
let varId_to_k3_expr     vars = List.map (fun v -> K3.SR.Var(v,TFloat)) vars    
		
		
		
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
	
let init_val = K3.SR.Const(CFloat(0.0))
			
	
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
    | [var,vart] -> K3.SR.Lambda(K3.SR.AVar(var,vart), e)
    | l          -> K3.SR.Lambda(K3.SR.ATuple(l), e)
  end


let create_proj_fn trig_args from_vars to_vars = 
		bind_for_apply_each 
        trig_args
        (varId_to_varIdType from_vars)
        (Tuple(varId_to_k3_expr to_vars))		
		
				
let bind_for_aggregate trig_args vt_list (iv,it) e = 
  let non_trig_vt_list = List.map (fun (v,t) ->
    if List.mem v trig_args then (gensym(),t) else (v,t)) vt_list in
  let vars = List.map fst non_trig_vt_list in
  if (unique vars) <> vars then
    failwith "invalid lambda with duplicate variables"
  else
  begin match non_trig_vt_list with 
    | [var,vart] -> K3.SR.AssocLambda(K3.SR.AVar(var,vart),
																			K3.SR.AVar(iv,it), e )
    | l          -> K3.SR.AssocLambda(K3.SR.ATuple(l),
																			K3.SR.AVar(iv,it), e )
  end	

let create_agg_fn trig_args vars multpl_var =
		let accv = next_accum_var () in 
		bind_for_aggregate 
				trig_args 
				(varId_to_varIdType (vars@[multpl_var])) 
				(accv,K3.SR.TFloat)
     		(K3.SR.Add(K3.SR.Var(multpl_var, K3.SR.TFloat), 
									 K3.SR.Var(accv, K3.SR.TFloat)))
    
(**********************************************************************)

let map_to_expr mapn ins outs map_type =
  (* TODO: value types *)
  begin match ins, outs with
  | ([],[]) -> K3.SR.SingletonPC(mapn,map_type)
  | ([], x) -> K3.SR.OutPC(mapn,outs,map_type)
  | (x, []) -> K3.SR.InPC(mapn,ins,map_type)
  | (x,y)   -> K3.SR.PC(mapn,ins,outs,map_type)
  end

(**********************************************************************)

let map_value_update_expr map_expr ine_l oute_l ve =
  begin match map_expr with
  | SingletonPC _ -> PCValueUpdate(map_expr, [], [], ve)
  | OutPC _ -> PCValueUpdate(map_expr, [], oute_l, ve)
  | InPC _ -> PCValueUpdate(map_expr, ine_l, [], ve)
  | PC _ -> PCValueUpdate(map_expr, ine_l, oute_l, ve)
  | _ -> failwith "invalid map expression for value update" 
  end
			
(**********************************************************************)
	
let map_access_to_expr
    skip_init map_expr init_expr singleton init_singleton out_patv
	  =
	  let aux sch t aux_init_expr =
		    (* TODO: use typechecker to compute type *)
		    let ke = varIdType_to_k3_expr sch in 
		    let map_t = Collection(TTuple((List.map snd sch)@[t])) in
		    let map_v,map_var = "slice",K3.SR.Var("slice", map_t) in
		    let access_expr =
		      if (not (Debug.active "NO-DEDUP-IVC")) && skip_init && singleton then
		      		Lookup(map_var,ke)
		      else if singleton then 
		        	IfThenElse(Member(map_var,ke), Lookup(map_var,ke), aux_init_expr) (*ReplaceInit*)
		      else let p_ve = 
									List.map (fun v -> let t = List.assoc v sch in (v,(Var(v,t)))) out_patv in  (*ReplaceInit*)
		        	Slice(map_var,sch,p_ve)
		    in Lambda(AVar(map_v, map_t), access_expr)
	  in
	  begin match map_expr with
	  | SingletonPC(id,t) -> map_expr
	  | OutPC(id,outs,t)  -> Apply(aux outs t, map_expr)	
	  | InPC(id,ins,t)    -> Apply(aux ins t, map_expr)	
	  | PC(id,ins,outs,t) ->
		    let nested_t =
	        (* TODO: use typechecker to compute type *)
	        let ins_t  = List.map snd ins in
	        let outs_t = List.map snd outs
	        in Collection(TTuple(ins_t@[Collection(TTuple(outs_t@[t]))]))
	      in
	      let in_el = varIdType_to_k3_expr ins in
	      let map_v,map_var = "m",Var("m", nested_t) in
	      let out_access_fn = Apply(aux outs t, Lookup(map_var,in_el)) in
	      let access_fn = Lambda(AVar(map_v, nested_t),
	          IfThenElse(Member(map_var,in_el), out_access_fn, init_expr))  (*ReplaceInit*)
	      in Apply(access_fn, map_expr)
	        
	  | _ -> failwith "invalid map for map access"
	  end
		
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

let rec calc_to_singleton_k3_expr trig_args metadata calc =
  let rcr = calc_to_singleton_k3_expr trig_args metadata in
  let bin_op f c1 c2 =
    let nm,c1r = rcr c1 in
    let nm2, c2r = calc_to_singleton_k3_expr trig_args nm c2
    in nm2, f c1r c2r
  in 
  begin match (calc) with
		| Calculus.CalcRing.Val( Calculus.Value(Arithmetic.AConst(i))) -> 
				metadata, K3.SR.Const(i)
    | Calculus.CalcRing.Val( Calculus.Value(Arithmetic.AVar(x)))   -> 
				metadata, K3.SR.Var(x, TFloat) (* TODO: var type *)
    | Calculus.CalcRing.Val( Calculus.Value(Arithmetic.AFn(x)))    -> 
				failwith "(* TODO: function M3 -> K3 *)"
    
		| Calculus.CalcRing.Val( Calculus.CalcRing.AggSum( [], aggsum_calc ) )  ->
				metadata, (m3rhs_to_k3_expr trig_args [] aggsum_calc)
			
		| Calculus.CalcRing.Val( Calculus.Lift(lift_v, lift_calc) )	-> ;
			
		| Calculus.CalcRing.Val( Calculus. External(mapn, ins, outs, ext_type, _) ) ->
		  (* Note: no need for lambda construction since all in vars are bound. *)
      (* TODO: schema+value types *)
      let (inv,outv) = (m3_var_to_k3_var ins, m3_var_to_k3_var outs) in
			
      let singleton_init_code = Calculus.expr_is_singleton init_aggecalc in
      let init_expr = m3rhs_to_k3_expr trig_args outv init_aggecalc in
			
			let new_metadata = metadata@[mapn, inv, outv] in
			let skip = List.mem (mapn, inv, outv) metadata in
			
			let map_expr = map_to_expr mapn ins outs in			   
			let r = map_access_to_expr skip map_expr init_expr true singleton_init_code []
			in new_metadata, r

    | Calculus.CalcRing.Sum( c1::sum_arg2::sum_args_tl )	->
				let c2 = if sum_args_tl = [] then sum_arg2
								 else Calculus.CalcRing.Sum( sum_arg2::sum_args_tl ) in
				bin_op (fun x y -> K3.SR.Add (x,y)) c1 c2							
    
		| Calculus.CalcRing.Prod( c1::prod_arg2::prod_args_tl )	->	  
				let c2 = if prod_args_tl = [] then prod_arg2
						 else Calculus.CalcRing.Prod( prod_arg2::prod_args_tl ) in
				bin_op (fun x y -> K3.SR.Mult (x,y)) c1 c2	            
    
		| Calculus.CalcRing.Neg( neg_arg ) -> ;
		
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Eq, c1, c2 ) ) -> bin_op (fun x y -> K3.SR.Eq  (x,y)) c1 c2
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Lt, c1, c2 ) ) -> bin_op (fun x y -> K3.SR.Lt  (x,y)) c1 c2
    | Calculus.CalcRing.Val( Calculus.Cmp( Types.Lte,c1, c2 ) ) -> bin_op (fun x y -> K3.SR.Leq (x,y)) c1 c2
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Gt, c1, c2 ) ) -> bin_op (fun x y -> K3.SR.Leq (x,y)) c2 c1
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Gte,c1, c2 ) ) -> bin_op (fun x y -> K3.SR.Lt  (x,y)) c2 c1
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Neq,c1, c2 ) ) -> bin_op (fun x y -> K3.SR.Neq (x,y)) c1 c2
  end

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

and op_to_expr trig_args metadata (expected_sch1,expected_sch2) op c c1 c2 =
  let aux ecalc expected_sch = 
      (ListAsSet.inter (snd (Calculus.schema_of_expr ecalc)) expected_sch, 
       expr_is_singleton ecalc) in
  let (outv1, c1_sing) = aux c1 expected_sch1 in
  let (outv2, c2_sing) = aux c2 expected_sch2 in
  let (schema, c_sing) = (snd (Calculus.schema_of_expr c)), (expr_is_singleton c)
  in 
	match (c_sing, c1_sing, c2_sing) with
    | (true, false, _) 
		| (true,     _, false) 
		| (false, true, true) ->
            failwith "invalid parent singleton"
        
    | (true, _, _) -> 
         Debug.print "TRACK-K3-SINGLETON" (fun () -> string_of_expr c );
         let (meta,expr) = calc_to_singleton_k3_expr trig_args metadata c
         in ([],meta,expr)
    | (false, true, false) 
		| (false, false, true) ->
	      (* TODO: types *)
	      Debug.print "TRACK-K3-SINGLETON" 
						(fun () -> string_of_expr (if c1_sing then c1 else c2));
	      let outsch, meta, ce = 
	         calc_to_k3_expr trig_args metadata 
	                      (if c1_sing then expected_sch2 else expected_sch1)
	                      (if c1_sing then c2 else c1) in
	      let meta2, inline = 
	         calc_to_singleton_k3_expr 
	                      trig_args 
	                      meta
	                      (if c1_sing then c1 else c2) in
	      let (v,v_t,l,r) =
	        if c1_sing then "v2",K3.SR.TFloat,inline,K3.SR.Var("v2",K3.SR.TFloat)
	        else "v1",K3.SR.TFloat,K3.SR.Var("v1",K3.SR.TFloat),inline in
	      let (op_sch, op_result) = (op outsch l r) in
	      let fn = bind_for_apply_each
	        trig_args ((varId_to_varIdType outsch)@[v,v_t]) op_result
	      in 
	         if outsch = [] then (op_sch, meta2, K3.SR.Apply(fn,ce))
	         else                (op_sch, meta2, K3.SR.Map(fn, ce))
        
    | (false, false, false) ->
      (* TODO: types *)
      (* Note: there is no difference to the nesting whether this op
       * is a product or a join *)
      let sch1, meta, oute = calc_to_k3_expr trig_args metadata expected_sch1 c1 in
      let sch2, meta2, ine = calc_to_k3_expr trig_args meta     expected_sch2 c2 in
      let ret_schema = ListAsSet.union sch1 sch2 in
      let (l,r) = (K3.SR.Var("v1",K3.SR.TFloat), K3.SR.Var("v2",K3.SR.TFloat)) in
      let (op_sch, op_result) = (op ret_schema l r) in
      let applied_expr = 
         if (sch1 = []) && (sch2 = []) then
            K3.SR.Apply(bind_for_apply_each trig_args ["v1",K3.SR.TFloat] (
               K3.SR.Apply(bind_for_apply_each trig_args ["v2",K3.SR.TFloat] (
                  op_result
               ), ine)
            ), oute)
         else if (sch1 = []) || (sch2 = []) then
            let (singleton_v,singleton_e,set_v,set_e) = 
               if sch1 = [] 
               then "v1",oute,(varId_to_varIdType sch2)@["v2",K3.SR.TFloat],ine
               else "v2",ine, (varId_to_varIdType sch1)@["v1",K3.SR.TFloat],oute
            in
               K3.SR.Map(bind_for_apply_each trig_args set_v (
                  K3.SR.Apply(bind_for_apply_each trig_args [singleton_v,K3.SR.TFloat] (
                     op_result
                  ), singleton_e)
               ), set_e)
         else
            let nested = bind_for_apply_each
              trig_args ((varId_to_varIdType sch2)@["v2",K3.SR.TFloat]) op_result in 
            let inner = bind_for_apply_each
              trig_args ((varId_to_varIdType sch1)@["v1",K3.SR.TFloat]) (K3.SR.Map(nested, ine)) 
            in Flatten(Map(inner, oute))
      in op_sch, meta2, applied_expr

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

and calc_to_k3_expr trig_args metadata expected_schema calc =
  let tuple op schema c1 c2 =
    (* TODO: schema types *)
    let outsch = if Debug.active "NARROW-SCHEMAS" 
                 then (ListAsSet.inter expected_schema schema)
                 else schema
    in if outsch = []
       then [], (op c1 c2)
       else outsch, 
            K3.SR.Tuple((varId_to_k3_expr outsch)@[op c1 c2])
  in 
  let bin_op f c1 c2 = (
    op_to_expr trig_args 
               metadata 
               (* Figure out the expected schema for the left and right
                  hand sides of the expressions.  This starts with the
                  schema that the outer expression expects:
                  For both, we have to add any join terms -- variables present
                  on both sides of the expression.
                  Then we also have to take any output variables from the lhs
                  that are expected by the rhs and add them to the lhs expected
                  schema. 
                  Finally, we take out all the trigger arguments.
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
                        (Calculus.string_of_expr calc)^
                        (list_to_string (fun x->x) common_schema)^"; "^
                        (list_to_string (fun x->x) c2_in_schema)^
                        (Calculus.string_of_expr c2)
                     );
                     (  ListAsSet.union common_schema c2_in_schema, 
                        common_schema ))
               (tuple f) 
               calc c1 c2 
    ) in 
  begin match calc with
    | Calculus.CalcRing.Val( Calculus.Value(Arithmetic.AConst(i))) -> 
			[], metadata, K3.SR.Const(i)
    | Calculus.CalcRing.Val( Calculus.Value(Arithmetic.AVar(x)))   -> 
			[], metadata, K3.SR.Var(x, TFloat) (* TODO: var type *)
    | Calculus.CalcRing.Val( Calculus.Value(Arithmetic.AFn(x)))    -> 
			failwith "(* TODO: function M3 -> K3 *)"
		
		| Calculus.CalcRing.Val( Calculus.CalcRing.AggSum( outv, aggsum_calc ) )  ->
			outv, metadata, (m3rhs_to_k3_expr trig_args outv aggsum_calc)
			
		| Calculus.CalcRing.Val( Calculus.Lift(lift_v, lift_calc) )	-> ;
			
		| Calculus.CalcRing.Val( Calculus.Rel(mapn, outv, rel_type) ) -> ;
		

    | Calculus.CalcRing.Val( Calculus. External(mapn, ins, outs, ext_type, _) ) ->
		  (* TODO: schema, value types *)
			let (inv,outv) = (m3_var_to_k3_var ins, m3_var_to_k3_var outs) in
			
      let skip = List.mem (mapn, inv, outv) metadata in			
			let singleton           = (Calculus.expr_is_singleton calc)          in
      let singleton_init_code = (Calculus.expr_is_singleton init_aggecalc) in
			let patv = M3P.get_extensions (M3P.get_ecalc init_aggecalc) in
			
			let map_expr = map_to_expr mapn ins outs ext_type in
      let r = map_access_to_expr skip map_expr singleton patv in    (*ReplaceInit*)
      
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
			 then 
					(outv, new_metadata, r)
			 else 
					let projected = (
	            let agg_fn = create_agg_fn trig_args outv "v" in
	            let gb_fn = create_proj_fn trig_args (outv@["v"]) retv in
							
	            if retv = [] 
	            then Aggregate(agg_fn, init_val, r)
	            else GroupByAggregate(agg_fn, init_val, gb_fn, r)
					) in 
					(retv, new_metadata, projected)
		
		| Calculus.CalcRing.Sum( c1::sum_arg2::sum_args_tl )	->
				let c2 = if sum_args_tl = [] then sum_arg2
								 else Calculus.CalcRing.Sum( sum_arg2::sum_args_tl ) in
				bin_op (fun x y -> K3.SR.Add (x,y)) c1 c2							
    
		| Calculus.CalcRing.Prod( c1::prod_arg2::prod_args_tl )	->	  
				let c2 = if prod_args_tl = [] then prod_arg2
						 else Calculus.CalcRing.Prod( prod_arg2::prod_args_tl ) in
				bin_op (fun x y -> K3.SR.Mult (x,y)) c1 c2	            
    
		| Calculus.CalcRing.Neg( neg_arg ) -> ;
		
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Eq, c1, c2 ) ) -> 
				bin_op (fun x y -> K3.SR.Eq  (x,y)) c1 c2
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Lt, c1, c2 ) ) -> 
				bin_op (fun x y -> K3.SR.Lt  (x,y)) c1 c2
    | Calculus.CalcRing.Val( Calculus.Cmp( Types.Lte, c1, c2 ) ) -> 
				bin_op (fun x y -> K3.SR.Leq  (x,y)) c1 c2
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Gt, c1, c2 ) ) -> 
				bin_op (fun x y -> K3.SR.Leq  (x,y)) c2 c1
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Gte, c1, c2 ) ) -> 
				bin_op (fun x y -> K3.SR.Lt  (x,y)) c2 c1
		| Calculus.CalcRing.Val( Calculus.Cmp( Types.Neq, c1, c2 ) ) -> 
				bin_op (fun x y -> K3.SR.Neq  (x,y)) c1 c2
  end 

(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

and m3rhs_to_k3_expr trig_args lhs_outv calc : K3.SR.expr_t =
   (* invokes calc_to_k3_expr on calc.
    * based on aggregate metadata:
    * -- simply uses the collection
    * -- applies bigsums to collection.
    *    we want to apply structural recursion optimizations in this case.
    * -- projects to lhs vars 
    *)
    let rhs_outv, _, rhs_expr = (calc_to_k3_expr trig_args [] lhs_outv calc) in
    let doesnt_need_aggregate = (ListAsSet.seteq rhs_outv lhs_outv) in
    
		let agg_fn = create_agg_fn trig_args rhs_outv "v" in
		
    if (Calculus.expr_is_singleton calc) || (rhs_outv = []) then rhs_expr
    else if doesnt_need_aggregate then 
		(
      if rhs_outv = lhs_outv then rhs_expr
      else K3.SR.Map( create_proj_fn trig_args (rhs_outv@["v"]) (lhs_outv@["v"]),
               			  rhs_expr)
    )
    else if M3P.get_full_agg (M3P.get_agg_meta calc) then
        K3.SR.Aggregate(agg_fn, init_val, rhs_expr) 
    else
        (* projection to lhs vars + aggregation *)
        let gb_fn = create_proj_fn trig_args (rhs_outv@["v"]) lhs_outv in 
				K3.SR.GroupByAggregate(agg_fn, init_val, gb_fn, rhs_expr)
  


(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)
(**********************************************************************)

(**********************************
 * Incremental section
 **********************************)

(* Incremental evaluation types *)
 
(* Top-level statement, computes and applies (i.e. persists) map deltas.
 *
 * collection, increment statement
 *)
type stmt_type_t = UpdateStmt | ReplaceStmt

type m3_stmt_t = {
   target_map  : expr_t;
   update_type : stmt_type_t;
   update_expr   : expr_t
}

(* Statement expression construction:
 * -- creates structural representations of the the incr and init value exprs,
 *    composes them in an update expression, merging in the case of delta slices.
 * -- invokes the update expression w/ existing values/slices, binding+looping
 *    persistent collection input vars, and applying updates.
*)
let collection_stmt (trig_args: k3_var_t list) (m3_trig_stmt : m3_stmt_t) : k3_stmt_t =
    (* Helpers *)
    let fn_arg_expr v t = (v,t),K3.SR.Var(v,t) in

    (*old*)
		(* let ((mapn, lhs_inv, lhs_outv, init_aggcalc), stmt_type, incr_aggcalc, _) = m3_trig_stmt in *)
		let (target_map, update_type, incr_aggcalc) = m3_trig_stmt in
		let (mapn, ins, outs, map_type, _) = Plan.expand_ds_name target_map in
		
		let (lhs_inv, lhs_outv) = (m3_var_to_k3_var     ins, m3_var_to_k3_var     outs) in
    let (in_el,   out_el)   = (varIdType_to_k3_expr ins, varIdType_to_k3_expr outs) in 


		let incr_single = expr_is_singleton incr_aggcalc in
    let incr_k3expr =
    		(* Note: we can only use calc_to_singleton_k3_expr if there are
         * no loop or bigsum vars in calc *) 
        if M3P.get_singleton incr_aggcalc then 
						snd (calc_to_singleton_k3_expr trig_args [] incr_aggcalc)
        else 
						m3rhs_to_k3_expr trig_args lhs_outv incr_aggcalc
    in
    
    
    (* TODO: use typechecker to compute types here *)
    let out_tier_t = K3.SR.Collection(K3.SR.TTuple((List.map snd outs)@[K3.SR.TFloat])) in

    let collection = map_to_expr mapn ins outs map_type in
    
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
            (Lambda(AVar(fst ca, snd ca),Add(ce,incr_k3expr)), init_e)
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
              trig_args ((varId_to_varIdType lhs_outv)@[ma]) merge_body in
            (* slice update: merge current slice with delta slice *)
            let merge_expr = Map(merge_fn, incr_k3expr)
            in (Lambda(AVar(fst ca, snd ca),merge_expr), zero_init)
        in
        match (incr_single, init_single) with
        | (true,true) -> singleton_aux (Add(incr_k3expr,init_expr))

        | (true,false) ->
            (* Note: we don't need to bind lhs_outv for init_expr since
             * incr_k3expr is a singleton, implying that lhs_outv are all
             * bound vars.
             * -- TODO: how can the init val be a slice then? if there are
             *    no loop out vars, and all in vars are bound, and all bigsums 
             *    are fully aggregated, this case should never occur. 
             *    Check this. *)
            (* Look up slice w/ out vars for init lhs expr *)
            singleton_aux (Add(Lookup(init_expr,out_el), incr_k3expr))

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
                      trig_args ((varId_to_varIdType lhs_inv)@[la]) loop_fn_body,
                    Slice(collection, ins, pat_ve))
    in
    let loop_update_aux ins outs delta_slice =
        let ua,ue = fn_arg_expr "updated_v" TFloat in
        let update_body = map_value_update_expr collection ins outs ue in
        let loop_update_fn = bind_for_apply_each
            trig_args ((varId_to_varIdType lhs_outv)@[ua]) update_body
        in Iterate(loop_update_fn, delta_slice)
    in
    let rhs_basic_merge collection_expr =
      match stmt_type with
         | M3.Stmt_Update -> Apply(update_expr, collection_expr)
         | M3.Stmt_Replace -> incr_k3expr
    in
    let rhs_test_merge collection_expr =
      match stmt_type with
         | M3.Stmt_Update -> 
            IfThenElse(Member(collection_expr, out_el),
                       Apply(update_expr, Lookup(collection_expr, out_el)),
                       sing_init_expr)
         | M3.Stmt_Replace -> incr_k3expr
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
      (*Comment(M3Common.code_of_stmt m3stmt, statement_expr))*)


let collection_trig (m3_trig: m3_trigger_t) : k3_trigger_t =
	let (m3_event, m3_trig_stmts) = m3_trig in
	let (k3_event_type, k3_rel_name, k3_trig_args) = m3_event_to_k3_event m3_event 	in
	let k3_trig_stmts = List.map (collection_stmt k3_trig_args) !m3_trig_stmts 
	in
  (k3_event_type, k3_rel_name, k3_trig_args, 
   if (Debug.active "RUNTIME-BIGSUMS") then
       	List.filter (fun (c,_) -> match c with 
																	PC(_,_,_,_) -> false | InPC(_,_,_) -> false | _ -> true) 
										k3_trig_stmts
   else k3_trig_stmts
	)




let m3_to_k3 (( _, m3_prog_schema, m3_prog_trigs, _ ):m3_prog_t) : (k3_progr_t) =
	 let k3_prog_schema = m3_schema_to_k3_schema m3_prog_schema in
   let m3_prog_trigs_prime, patterns = M3Compiler.prepare_triggers !m3_prog_trigs in
	 let k3_prog_trigs = List.map collection_trig m3_prog_trigs_prime in
			( k3_prog_schema, patterns,	k3_prog_trigs )
			
