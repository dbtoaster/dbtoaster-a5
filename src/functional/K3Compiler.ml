module K = K3

module Make = functor (CG : K3Codegen.CG) ->
struct

open Types
open Schema
open CG
open K3Typechecker


let rec compile_k3_expr e =
    let rcr = compile_k3_expr in
    let tc_fn_rt e = 
        let r = typecheck_expr e in match r with
            | K.Fn(_,rt) -> rt
            | _ -> failwith "invalid function"
    in
    let debug e = Some(e) in
    let compile_op o l r = op ~expr:(debug e) o (rcr l) (rcr r) in
    begin match e with
    | K.Const (c) -> const ~expr:(debug e) c
    | K.Var(v,t) -> var ~expr:(debug e) v t
    | K.Tuple(field_l) -> tuple ~expr:(debug e) (List.map rcr field_l)
    | K.Project(e,fields) -> project ~expr:(debug e) (rcr e) fields
    | K.Singleton(e) -> singleton ~expr:(debug e) (rcr e) (typecheck_expr e)
    | K.Combine(l) -> combine ~expr:(debug e) (List.map rcr l) 
    | K.Add(l,r)  -> compile_op add_op l r
    | K.Mult(l,r) -> compile_op mult_op l r
    | K.Eq(l,r)   -> compile_op eq_op l r
    | K.Neq(l,r)  -> compile_op neq_op l r
    | K.Lt(l,r)   -> compile_op lt_op l r
    | K.Leq(l,r)  -> compile_op leq_op l r 

    | K.IfThenElse0(cond,v) -> compile_op ifthenelse0_op cond v
    
    | K.Comment(c, cexp) -> comment ~expr:(debug e) c (rcr cexp)

    | K.IfThenElse(p,t,e) -> ifthenelse ~expr:(debug e) (rcr p) (rcr t) (rcr e)

    | K.Block(e_l) -> block ~expr:(debug e) (List.map rcr e_l)
    | K.Iterate(fn_e, c_e) -> iterate ~expr:(debug e) (rcr fn_e) (rcr c_e)
    
    | K.Lambda(arg_e, b_e) -> lambda ~expr:(debug e) arg_e (rcr b_e)
    
    | K.AssocLambda(arg1_e,arg2_e,b_e) ->
        assoc_lambda ~expr:(debug e) arg1_e arg2_e (rcr b_e)
    
    | K.ExternalLambda(fn_id,fn_arg,fn_t) ->
        external_lambda ~expr:(debug e) fn_id fn_arg fn_t
    
    | K.Apply(fn_e, arg_e) -> apply ~expr:(debug e) (rcr fn_e) (rcr arg_e)
    
    | K.Map(fn_e, c_e) -> map ~expr:(debug e) (rcr fn_e) 
                              (tc_fn_rt fn_e) (rcr c_e) 
    | K.Flatten(c_e) -> flatten ~expr:(debug e) (rcr c_e) 
    | K.Aggregate(fn_e, init_e, c_e) ->
        begin match List.map rcr [fn_e;init_e;c_e] with
        | [x;y;z] -> aggregate ~expr:(debug e) x y z
        | _ -> failwith "invalid aggregate compilation"
        end

    | K.GroupByAggregate(fn_e, init_e, gb_e, c_e) -> 
        begin match List.map rcr [fn_e;init_e;gb_e;c_e] with
        | [w;x;y;z] -> group_by_aggregate ~expr:(debug e) w x y z
        | _ -> failwith "invalid group-by aggregate compilation"
        end

    | K.Member(m_e, ke_l) -> exists ~expr:(debug e) (rcr m_e) 
                                    (List.map rcr ke_l)  
    | K.Lookup(m_e, ke_l) -> lookup ~expr:(debug e) (rcr m_e) 
                                    (List.map rcr ke_l)
    | K.Slice(m_e, sch, idk_l) ->
        let index l e =
          let (pos,found) = List.fold_left (fun (c,f) x ->
            if f then (c,f) else if x = e then (c,true) else (c+1,false))
            (0, false) l
          in if not(found) then raise Not_found else pos
        in
        let v_l, k_l = List.split idk_l in
        let idx_l = List.map (index (List.map fst sch)) v_l in
        slice ~expr:(debug e) (rcr m_e) (List.map rcr k_l) idx_l

    | K.Filter(fn_e, c_e)      -> filter ~expr:(debug e) (rcr fn_e) (rcr c_e) 
    | K.SingletonPC(id,t)      -> get_value ~expr:(debug e) t id
    | K.OutPC(id,outs,t)       -> get_out_map ~expr:(debug e) outs t id
    | K.InPC(id,ins,t)         -> get_in_map ~expr:(debug e) ins t id
    | K.PC(id,ins,outs,t)      -> get_map ~expr:(debug e) (ins,outs) t id

    | K.PCUpdate(m_e, ke_l, u_e) ->
        begin match m_e with
        | K.SingletonPC _     -> failwith "invalid bulk update of value"
        | K.OutPC(id,outs,t)  -> update_out_map ~expr:(debug e) id (rcr u_e)
        | K.InPC(id,ins,t)    -> update_in_map ~expr:(debug e) id (rcr u_e)
        | K.PC(id,ins,outs,t) -> update_map ~expr:(debug e) id
            (List.map rcr ke_l) (rcr u_e)
        | _ -> failwith "invalid map to bulk update"
        end

    | K.PCValueUpdate(m_e, ine_l, oute_l, u_e) ->
        begin match (m_e, ine_l, oute_l) with
        | (K.SingletonPC(id,_),[],[]) -> update_value ~expr:(debug e) 
                                                      id (rcr u_e)
        
        | (K.OutPC(id,_,_), [], e_l) -> update_out_map_value ~expr:(debug e) id
            (List.map rcr e_l) (rcr u_e) 
        
        | (K.InPC(id,_,_), e_l, []) -> update_in_map_value ~expr:(debug e) id
            (List.map rcr e_l) (rcr u_e)
        
        | (K.PC(id,_,_,_), ie_l, oe_l) -> update_map_value ~expr:(debug e) id
            (List.map rcr ie_l) (List.map rcr oe_l)
            (rcr u_e)
        | _ -> failwith "invalid map value to update"
        end
    | K.PCElementRemove(m_e, ine_l, oute_l) ->
        begin match (m_e, ine_l, oute_l) with    
        | (K.OutPC(id,_,_), [], e_l) -> remove_out_map_element ~expr:(debug e) 
                                                               id
            (List.map rcr e_l)
        | (K.InPC(id,_,_), e_l, []) -> remove_in_map_element ~expr:(debug e) id
            (List.map rcr e_l)
        | (K.PC(id,_,_,_), ie_l, oe_l) -> remove_map_element ~expr:(debug e) id
            (List.map rcr ie_l) (List.map rcr oe_l)
        | _ -> failwith "invalid map value to remove"
        end
    | K.Unit -> unit_operation
    end

let compile_triggers_noopt (trigs:K3.trigger_t list) : code_t list =
   List.map (fun (event, cs) ->
      let stmts = List.map (fun x -> 
         compile_k3_expr (K3.annotate_collections x)) cs 
      in
         trigger event stmts
   ) trigs

let compile_triggers (trigs:K3.trigger_t list) : code_t list =
  List.map (fun (event, cs) ->
      let args = List.map fst (event_vars event) in
      let stmts = List.map compile_k3_expr
        (List.map (K3Optimizer.optimize args) cs) 
      in trigger event stmts)
    trigs
    
let compile_k3_to_code ((dbschema,(schema,patterns),trigs,
                         toplevel_queries) : K3.prog_t): 
                          code_t =
   let ctrigs = compile_triggers_noopt trigs in
   let (tables,streams) =
      Schema.partition_sources_by_type dbschema
   in
   let compiled_tables  = List.map (fun (s,ra) -> CG.source s ra) tables  in
   let compiled_streams = List.map (fun (s,ra) -> CG.source s ra) streams in
   let compiled_tlqs    = List.map (fun (n,q) -> (n,q,compile_k3_expr q)) 
                                   toplevel_queries in
      (main  schema patterns compiled_tables compiled_streams ctrigs 
             compiled_tlqs)

;;

let compile_query_to_string prog: string =
  to_string (compile_k3_to_code prog)

end


let optimize_prog ?(optimizations=[]) (schema, patterns, trigs) =
  let opt_trigs = List.map (fun (event, _, cs) ->
    let opt_cs = List.map (fun (i,e) ->
      i, K3Optimizer.optimize ~optimizations:optimizations 
                              (List.map fst (Schema.event_vars event))
                              e) cs
    in (event,opt_cs)) trigs
  in (schema, patterns, opt_trigs)

(* TODO
let compile_query_to_program ?(disable_opt = false)
                             ?(optimizations = [])
                             ((schema,m3prog) : M3.prog_t) : program 
  =
   let m3ptrigs,patterns = M3Compiler.prepare_triggers m3prog in
   let p = collection_prog (schema,m3ptrigs) patterns in
   if disable_opt then p else optimize_prog ~optimizations:optimizations p
*)
