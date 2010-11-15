module Make = functor (CG : K3Codegen.CG) ->
struct

open CG
open K3.SR
open K3Builder
open K3Typechecker
open K3Optimizer

let rec compile_k3_expr e =
    let rcr = compile_k3_expr in
    let tc_fn_rt e = 
        let r = typecheck_expr e in match r with
            | Fn(_,rt) -> rt
            | _ -> failwith "invalid function"
    in
    let debug e = Some(e) in
    let compile_op o l r = op ~expr:(debug e) o (rcr l) (rcr r) in
    begin match e with
    | Const (c) -> const ~expr:(debug e) c
    | Var(v,t) -> var ~expr:(debug e) v
    | Tuple(field_l) -> tuple ~expr:(debug e) (List.map rcr field_l)
    | Project(e,fields) -> project ~expr:(debug e) (rcr e) fields
    | Singleton(e) -> singleton ~expr:(debug e) (rcr e) (typecheck_expr e)
    | Combine(l,r) -> combine ~expr:(debug e) (rcr l) (rcr r) 
    | Add(l,r)  -> compile_op add_op l r
    | Mult(l,r) -> compile_op mult_op l r
    | Eq(l,r)   -> compile_op eq_op l r
    | Neq(l,r)  -> compile_op neq_op l r
    | Lt(l,r)   -> compile_op lt_op l r
    | Leq(l,r)  -> compile_op leq_op l r 

    | IfThenElse0(cond,v) -> compile_op ifthenelse0_op cond v

    | IfThenElse(p,t,e) -> ifthenelse ~expr:(debug e) (rcr p) (rcr t) (rcr e)

    | Block(e_l) -> block ~expr:(debug e) (List.map rcr e_l)
    | Iterate(fn_e, c_e) -> iterate ~expr:(debug e) (rcr fn_e) (rcr c_e)
    
    | Lambda(arg_e, b_e) -> lambda ~expr:(debug e) arg_e (rcr b_e)
    
    | AssocLambda(arg1_e,arg2_e,b_e) ->
        assoc_lambda ~expr:(debug e) arg1_e arg2_e (rcr b_e)
    
    | Apply(fn_e, arg_e) -> apply ~expr:(debug e) (rcr fn_e) (rcr arg_e)
    
    | Map(fn_e, c_e) -> map ~expr:(debug e) (rcr fn_e) (tc_fn_rt fn_e) (rcr c_e) 
    | Flatten(c_e) -> flatten ~expr:(debug e) (rcr c_e) 
    | Aggregate(fn_e, init_e, c_e) ->
        begin match List.map rcr [fn_e;init_e;c_e] with
        | [x;y;z] -> aggregate ~expr:(debug e) x y z
        | _ -> failwith "invalid aggregate compilation"
        end

    | GroupByAggregate(fn_e, init_e, gb_e, c_e) -> 
        begin match List.map rcr [fn_e;init_e;gb_e;c_e] with
        | [w;x;y;z] -> group_by_aggregate ~expr:(debug e) w x y z
        | _ -> failwith "invalid group-by aggregate compilation"
        end

    | Member(m_e, ke_l) -> exists ~expr:(debug e) (rcr m_e) (List.map rcr ke_l)  
    | Lookup(m_e, ke_l) -> lookup ~expr:(debug e) (rcr m_e) (List.map rcr ke_l)
    | Slice(m_e, sch, idk_l) ->
        let index l e =
          let (pos,found) = List.fold_left (fun (c,f) x ->
            if f then (c,f) else if x = e then (c,true) else (c+1,false))
            (0, false) l
          in if not(found) then raise Not_found else pos
        in
        let v_l, k_l = List.split idk_l in
        let idx_l = List.map (index (List.map fst sch)) v_l in
        slice ~expr:(debug e) (rcr m_e) (List.map rcr k_l) idx_l

    | SingletonPC(id,t)      -> get_value ~expr:(debug e) id
    | OutPC(id,outs,t)       -> get_out_map ~expr:(debug e) id
    | InPC(id,ins,t)         -> get_in_map ~expr:(debug e) id
    | PC(id,ins,outs,t)      -> get_map ~expr:(debug e) id

    | PCUpdate(m_e, ke_l, u_e) ->
        begin match m_e with
        | SingletonPC _     -> failwith "invalid bulk update of value"
        | OutPC(id,outs,t)  -> update_out_map ~expr:(debug e) id (rcr u_e)
        | InPC(id,ins,t)    -> update_in_map ~expr:(debug e) id (rcr u_e)
        | PC(id,ins,outs,t) -> update_map ~expr:(debug e) id
            (List.map rcr ke_l) (rcr u_e)
        | _ -> failwith "invalid map to bulk update"
        end

    | PCValueUpdate(m_e, ine_l, oute_l, u_e) ->
        begin match (m_e, ine_l, oute_l) with
        | (SingletonPC(id,_),[],[]) -> update_value ~expr:(debug e) id (rcr u_e)
        
        | (OutPC(id,_,_), [], e_l) -> update_out_map_value ~expr:(debug e) id
            (List.map rcr e_l) (rcr u_e) 
        
        | (InPC(id,_,_), e_l, []) -> update_in_map_value ~expr:(debug e) id
            (List.map rcr e_l) (rcr u_e)
        
        | (PC(id,_,_,_), ie_l, oe_l) -> update_map_value ~expr:(debug e) id
            (List.map rcr ie_l) (List.map rcr oe_l)
            (rcr u_e)
        | _ -> failwith "invalid map value to update"
        end
    end

let compile_triggers trigs : code_t list =
  List.map (fun (event, rel, args, cs) ->
      let stmts = List.map compile_k3_expr
        (List.map (fun (_,e) ->
            let optimize e = simplify_collections 
                (lift_ifs args (inline_collection_functions [] e)) in
            let rec fixpoint e =
                let new_e = optimize e in
                if e = new_e then e else fixpoint new_e    
            in fixpoint e) cs) 
      in trigger event rel args stmts)
    trigs

let compile_query (((schema,m3prog) : M3.prog_t),
                    (sources: M3.relation_input_t list))
                  (toplevel_queries : string list)
                  (out_file_name : Util.GenericIO.out_t) =
  let m3ptrigs,patterns = M3Compiler.prepare_triggers m3prog in
  let (_,_,trigs) = collection_prog (schema,m3ptrigs) patterns in
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