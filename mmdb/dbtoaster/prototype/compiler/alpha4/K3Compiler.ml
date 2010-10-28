open K3.SR

module Make = functor (CG : K3Codegen.CG) ->
struct

open CG

let rec compile_k3_expr e =
    let compile_op o l r = op o (compile_k3_expr l) (compile_k3_expr r) in
    begin match e with
    | Const (c) -> const c
    | Var(v,t) -> var v
    | Tuple(field_l) -> tuple (List.map compile_k3_expr field_l)
    | Project(e,fields) -> project (compile_k3_expr e) fields
    | Singleton(e) -> singleton (compile_k3_expr e)
    | Combine(l,r) -> combine (compile_k3_expr l) (compile_k3_expr r) 
    | Add(l,r)  -> compile_op add_op l r
    | Mult(l,r) -> compile_op mult_op l r
    | Eq(l,r)   -> compile_op eq_op l r
    | Neq(l,r)  -> compile_op neq_op l r
    | Lt(l,r)   -> compile_op lt_op l r
    | Leq(l,r)  -> compile_op leq_op l r 

    | IfThenElse0(cond,v) -> compile_op ifthenelse0_op cond v

    | IfThenElse(p,t,e) -> ifthenelse
        (compile_k3_expr p) (compile_k3_expr t) (compile_k3_expr e)

    | Block(e_l) -> block (List.map compile_k3_expr e_l)
    | Iterate(fn_e, c_e) -> iterate (compile_k3_expr fn_e) (compile_k3_expr c_e)
    
    | Lambda(arg_id, arg_t, b_e,sa) -> lambda arg_id sa (compile_k3_expr b_e)
    | AssocLambda(arg1_id,arg1_t,arg2_id,arg2_t,b_e) -> assoc_lambda arg1_id arg2_id (compile_k3_expr b_e)
    | Apply(fn_e, arg_e) -> apply (compile_k3_expr fn_e) (compile_k3_expr arg_e)
    
    | Map(fn_e, c_e) -> map (compile_k3_expr fn_e) (compile_k3_expr c_e) 
    | Flatten(c_e) -> flatten (compile_k3_expr c_e) 
    | Aggregate(fn_e, init_e, c_e) ->
        begin match List.map compile_k3_expr [fn_e;init_e;c_e] with
        | [x;y;z] -> aggregate x y z
        | _ -> failwith "invalid aggregate compilation"
        end

    | GroupByAggregate(fn_e, init_e, gb_e, c_e) -> 
        begin match List.map compile_k3_expr [fn_e;init_e;gb_e;c_e] with
        | [w;x;y;z] -> group_by_aggregate w x y z
        | _ -> failwith "invalid group-by aggregate compilation"
        end

    | Member(m_e, ke_l) -> exists (compile_k3_expr m_e) (List.map compile_k3_expr ke_l)  
    | Lookup(m_e, ke_l) -> lookup (compile_k3_expr m_e) (List.map compile_k3_expr ke_l)
    | Slice(m_e, sch, idk_l) ->
        let index l e =
          let (pos,found) = List.fold_left (fun (c,f) x ->
            if f then (c,f) else if x = e then (c,true) else (c+1,false))
            (0, false) l
          in if not(found) then raise Not_found else pos
        in
        let v_l, k_l = List.split idk_l in
        let idx_l = List.map (index (List.map fst sch)) v_l in
        slice (compile_k3_expr m_e) (List.map compile_k3_expr k_l) idx_l

    | SingletonPC(id,t)      -> get_value id
    | OutPC(id,outs,t)       -> get_out_map id
    | InPC(id,ins,t,init_e)  -> get_in_map id
    | PC(id,ins,outs,t,init_e) -> get_map id

    | PCUpdate(m_e, ke_l, u_e) ->
        begin match m_e with
        | SingletonPC _ -> failwith "invalid bulk update of value"
        | OutPC(id,outs,t) -> update_out_map id (compile_k3_expr u_e)
        | InPC(id,ins,t,init_e) -> update_in_map id (compile_k3_expr u_e)
        | PC(id,ins,outs,t,init_e) -> update_map id
            (List.map compile_k3_expr ke_l) (compile_k3_expr u_e)
        | _ -> failwith "invalid map to bulk update"
        end

    | PCValueUpdate(m_e, ine_l, oute_l, u_e) ->
        begin match (m_e, ine_l, oute_l) with
        | (SingletonPC(id,_),[],[]) -> update_value id (compile_k3_expr u_e)
        
        | (OutPC(id,_,_), [], e_l) -> update_out_map_value id
            (List.map compile_k3_expr e_l) (compile_k3_expr u_e) 
        
        | (InPC(id,_,_,_), e_l, []) -> update_in_map_value id
            (List.map compile_k3_expr e_l) (compile_k3_expr u_e)
        
        | (PC(id,_,_,_,_), ie_l, oe_l) -> update_map_value id
            (List.map compile_k3_expr ie_l) (List.map compile_k3_expr oe_l)
            (compile_k3_expr u_e)
        | _ -> failwith "invalid map value to update"
        end
    end

let compile_triggers trigs : code_t list =
  List.flatten (List.map (fun (event, rel, args, cs) ->
    let _,stmts = List.split cs in
    List.map compile_k3_expr stmts) trigs)

let compile_query ((schema,patterns,trigs) : K3.SR.program)
                  (sources: M3.relation_input_t list)
                  (toplevel_queries : string list)
                  (out_file_name : Util.GenericIO.out_t) =
  let trig_rels = Util.ListAsSet.no_duplicates
     (List.map (fun (_,rel,_,_) -> rel) trigs) in
  let ctrigs = compile_triggers trigs in
  let sources_and_adaptors =
      List.fold_left (fun acc (s,f,rel,a) ->
         match (List.mem rel trig_rels, List.mem_assoc (s,f) acc) with
           | (false,_) -> acc
           | (_,false) -> ((s,f),[rel,a])::acc
           | (_, true) ->
           	let existing = List.assoc (s,f) acc
            in ((s,f), ((rel,a)::existing))::(List.remove_assoc (s,f) acc))
     	[] sources
  in
   let csource =
      List.map (fun ((s,f),ra) -> CG.source s f ra) sources_and_adaptors
   in Util.GenericIO.write out_file_name 
     (fun out_file -> output (main schema patterns csource ctrigs
                              toplevel_queries) out_file; 
                      output_string out_file "\n")

end