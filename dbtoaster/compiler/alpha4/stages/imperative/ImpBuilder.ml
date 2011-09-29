(* Imperative expression construction.
 *
 * This module builds imperative expressions (i.e. Imperative.imp_t) from K3
 * expressions.
 * We use an internal intermediate representation based on annotated K3
 * expressions to flatten and linearize the K3 tree structure into procedural
 * code.
 *
 * Submodules include:
 *   AnnotatedK3: a tree structure for annotating K3 expressions.
 *
 *   DirectIRBuilder: an implementation of annotated K3 expressions (IR), where
 *     annotations are unique symbols associated with K3 expressions that
 *     cannot be inlined (i.e. implemented as a right-hand value) in C++.
 *     This includes any expression resulting in a collection. 
 *
 *   Imp: the main construction module which turns an IR tree into imperative
 *     expressions. This first linearizes the IR tree, with the result yielded
 *     at the end of the resulting sequence of IR nodes. Imperative expression
 *     construction works backwards from results, pushing result symbols upwards
 *     to eliminate temporary symbols and unnecessary assignments,
 *     particularly for lambdas and their arguments and return values.
 *
 *)

open Util
open M3
open K3.SR
open Format

(* An annotated K3 module.
 *
 * This module defines a type 'a ir_t that represents a tagged
 * K3 expression tree.
 *
 * 'a ir_t is a tree datastructure that captures aggregated K3 expressions.
 * That is an arbitrary portion of a K3 expression tree may be represented by
 * a single ir_t node. Each ir_t node includes metadata, whose type is given
 * by the parameter 'a. When combined with the tree compression achieved by
 * ir_t nodes, we can thus assign metadata to arbitrary parts of K3 expressions.
 *
 * Nodes in the ir_t tree are of type ir_tag_t, which are of two variants:
 *   Decorated: an ir_t node representing a tagged K3 node.
 *   Undecorated: an ir_t node representing a K3 expression tree, where the
 *     entire tree has one piece of metadata associated with it. The semantics
 *     of the association is user-defined, e.g. the metadata may associate with
 *     every node of the K3 expression or only the root (or any other part).
 *)
module AnnotatedK3 =
struct
  open Imperative
  module K = K3.SR
  module T = K3Typechecker
  type op_t = Add | Mult | Eq | Neq | Lt | Leq | If0

  type arg_t = K.arg_t
  type expr_t = K.expr_t

  type ir_tag_t = Decorated of node_tag_t | Undecorated of expr_t
  and node_tag_t =
    (* No consts or vars, since these are always undecorated leaves *)
   | Op of op_t
   | Tuple
   | Projection of int list
   | Singleton
   | Combine
   | Lambda of arg_t
   | AssocLambda of arg_t * arg_t 
   | Apply

   | Block  
   | Iterate
   | IfThenElse

   | Map
   | Aggregate
   | GroupByAggregate
   | Flatten
   | Ext (* = Flatten(Map) *)

   | Member
   | Lookup
   | Slice  of int list

   | PCUpdate
   | PCValueUpdate 
 

  type 'a ir_t =
      Leaf of 'a * ir_tag_t
    | Node of 'a * ir_tag_t * 'a ir_t list

  type 'a linear_code_t = ('a * (ir_tag_t * ('a list))) list

  (* Helpers *)
  let sym_counter = ref 0
  let gensym () = incr sym_counter; "__v"^(string_of_int (!sym_counter))
  let tag_of_ir ir = match ir with | Leaf(_,t) -> t | Node (_,t,_) -> t 
  let meta_of_ir ir = match ir with | Leaf(m,_) -> m | Node (m,_,_) -> m
  let children_of_ir ir = match ir with | Leaf _ -> [] | Node (_,_,c) -> c
  
  let string_of_op op = match op with
    | Add -> "Add" | Mult -> "Mult"
    | Eq -> "Eq" | Neq -> "Neq" | Lt -> "Lt" | Leq -> "Leq" | If0 -> "If0"

  let string_of_ir_tag t = match t with
   | Op(op) -> string_of_op op
   | Tuple -> "Tuple"
   | Projection(idx) ->
     "Projection("^(String.concat "," (List.map string_of_int idx))^")"

   | Singleton -> "Singleton"
   | Combine -> "Combine"
   | Lambda(v) -> "Lambda("^(K.string_of_arg v)^")"
   | AssocLambda(v1,v2) -> 
      "AssocLambda("^(K.string_of_arg v1)^", "^(K.string_of_arg v2)^")"
   | Apply -> "Apply"

   | Block -> "Block"
   | Iterate -> "Iterate"
   | IfThenElse -> "IfThenElse"

   | Map -> "Map"
   | Aggregate -> "Aggregate"
   | GroupByAggregate -> "GroupByAggregate"
   | Flatten -> "Flatten"
   | Ext -> "Ext" 

   | Member -> "Member"
   | Lookup -> "Lookup"
   | Slice(idx) -> 
     "Slice("^(String.concat "," (List.map string_of_int idx))^")"

   | PCUpdate -> "Update"
   | PCValueUpdate -> "ValueUpdate"
  
  let string_of_tag t = match t with
    | Undecorated e -> K.string_of_expr e
    | Decorated ir_tag -> string_of_ir_tag ir_tag
  
  let rec string_of_k3ir ?(string_of_meta = (fun _ -> "")) i = match i with
    | Leaf(m, t)    -> "[{"^(string_of_meta m)^"}"^(string_of_tag t)^"]"
    | Node(m, n, t) -> 
      "{"^(string_of_meta m)^"}"^
      (string_of_tag n)^
      (list_to_string (string_of_k3ir ~string_of_meta:string_of_meta) t)
end


module Common =
struct
  open Imperative
  open AnnotatedK3

  type 'ext_type decl_t = id_t * 'ext_type type_t
  type 'ext_type imp_metadata =
    (* valid sym option * return value sym *)
    TypedSym of id_t option * 'ext_type decl_t

  (* K3/IR helpers *)
  let arg_of_lambda leaf_tag = match leaf_tag with
    | Decorated(Lambda(x)) -> x
    | Undecorated(K.Lambda(x,_)) -> x
    | _ -> failwith "invalid lambda"

  let arg_of_assoc_lambda leaf_tag = match leaf_tag with
    | Decorated(AssocLambda(x,y)) -> (x,y)
    | Undecorated(K.AssocLambda(x,y,_)) -> (x,y)
    | _ -> failwith "invalid assoc lambda"

  let return_type_of_lambda t = match t with
    | Host(K.Fn(_,b_t)) -> Host(b_t)
    | _ -> failwith "invalid lambda type"

  let return_type_of_assoc_lambda t = match t with
    | Host(K.Fn(_,K.Fn(_,b_t))) -> Host(b_t)
    | _ -> failwith "invalid assoc lambda type"

  (* Metadata helpers *)
  let mk_meta ?(valid=None) sym ty = TypedSym (valid, (sym, ty))
  let valid_sym_of_meta meta = match meta with | TypedSym(v,_) -> v
  let sym_of_meta meta = match meta with | TypedSym (_,s) -> fst s
  let type_of_meta meta = match meta with | TypedSym (_,(_,t)) -> t 

  let k3_var_of_meta meta =
    K.Var(sym_of_meta meta, host_type (type_of_meta meta))

  let push_meta_valid meta cmeta = match meta, cmeta with
    | TypedSym(v,_), TypedSym(_,st) -> TypedSym(v,st)

  let push_meta meta cmeta = match meta, cmeta with
    | TypedSym (v,(s,_)), TypedSym (_,(_,t)) -> TypedSym (v, (s,t))

  let push_lambda_meta meta cmeta = match meta, cmeta with
    | TypedSym (v, (s,_)), TypedSym (_, (_,t)) ->
      TypedSym (v, (s,return_type_of_lambda t))

  let push_assoc_lambda_meta meta cmeta = match meta, cmeta with
    | TypedSym (v, (s,_)), TypedSym (_, (_,t)) ->
      TypedSym (v, (s,return_type_of_assoc_lambda t))
 
  let decl_of_meta force meta =
    Some(force, sym_of_meta meta, type_of_meta meta)

  let meta_of_arg arg meta =
    begin match arg with
    | AVar(arg_sym, ty) ->
      (mk_meta arg_sym (Host ty)), [], Some(true, arg_sym, (Host ty))
    | _ -> meta, [arg], (decl_of_meta true meta)
    end

  (* assumes meta has the same type as arg2, as in aggregation *)    
  let meta_of_assoc_arg arg1 arg2 meta =
    begin match arg2 with
    | AVar(arg2_sym, ty) ->
        let m = mk_meta arg2_sym (Host ty)
        in [m; m], [arg1], [decl_of_meta true m; decl_of_meta true m]
    | _ ->
        let d = decl_of_meta true meta
        in [meta; meta], [arg1; arg2], [d; d]
    end


  (* IR construction helpers *)
  (* returns:
   * flat list of triples for undecorated nodes
   * flat list of exprs for all nodes
   * flat list of children from all nodes *) 
  let undecorated_of_list cl =
    (* ull = list of sym, expr, children triples for undecorated nodes
     * ecl = list of expr, children pairs for all nodes *)
    let ull, ecl = List.split (List.map
      (function Leaf(meta, Undecorated(f)) -> [meta, f, []], [f, []]
       | Leaf(meta, Decorated _) -> [], [k3_var_of_meta meta, []] 
       | Node(meta, Undecorated(f), cir) -> [meta, f, cir], [f, cir]
       | Node(meta, Decorated _, cir) as n -> [], [k3_var_of_meta meta, [n]]) cl)
    in
    let el, cll = List.split (List.flatten ecl) in
    List.flatten ull, el, List.flatten cll 

  let k3op_of_op_t op l r = match op with
    | Add  -> K.Add(l,r)
    | Mult -> K.Mult(l,r)
    | Eq   -> K.Eq(l,r)
    | Neq  -> K.Neq(l,r)
    | Lt   -> K.Lt(l,r)
    | Leq  -> K.Leq(l,r)
    | If0  -> K.IfThenElse0(l,r)

  let slice_of_map mapc keyse indices =
    begin match mapc with
    | Leaf(_, Undecorated(mape)) ->
      let (mapn, schema) = match mape with
        | K.InPC(mapn, ins, _) -> (mapn, ins)
        | K.OutPC(mapn, outs, _) -> (mapn, outs)
        | K.PC(mapn, ins, _, _) -> (mapn, ins)
        | K.Lookup(K.PC(mapn, _, outs, _), _) -> (mapn, outs)
        | _ -> failwith "invalid map for slicing"
      in
      List.iter (fun x -> 
         if x >= (List.length schema) then failwith (
               "invalid slice index on "^mapn^".  slice schema: "^
               (list_to_string string_of_int indices)^" out of "^
               (list_to_string fst schema)
            )
      ) indices;
      let fields = List.combine
        (List.map (fun i -> fst (List.nth schema i)) indices) keyse
      in K.Slice(mape, schema, fields)
    | _ -> failwith "invalid map code"
    end

  let build_code irl undec_f dec_f =
    let ul, el, cl = undecorated_of_list irl in
    if (List.length ul) <> (List.length irl) then dec_f el cl
    else
      let x,y = List.split (List.map (fun (x,y,z) -> (x,y),z) ul)
      in undec_f x (List.flatten y)

  let tag_of_undecorated metadata e cir = match cir with
    | [] -> Leaf(metadata, Undecorated(e))
    | _ -> Node(metadata, Undecorated(e), cir)

  (* IR constructors *)
  let undecorated_ir metadata e = Leaf(metadata, Undecorated(e))

  let tuple_ir metadata sub_ir =
    build_code sub_ir
      (fun fieldse cir ->
        let e = K3.SR.Tuple(List.map snd fieldse)
        in tag_of_undecorated metadata e cir)
      (fun ce cir ->
        let e = K3.SR.Tuple(ce)
        in Node(metadata, Undecorated(e), cir))

  let project_ir metadata projections sub_ir =
    build_code sub_ir
      (fun tu cir -> match tu with
        | [sym, te] ->
          let e = K3.SR.Project(te, projections)
          in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid tuple expression")
      (fun ce cir ->
        let e = K3.SR.Project(List.hd ce, projections)
        in Node(metadata, Undecorated(e), cir))

  let singleton_ir metadata sub_ir =
    build_code sub_ir
      (fun eu cir -> match eu with
        | [sym, elem] ->
          let e = K3.SR.Singleton(elem)
          in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid singleton element expression")
      (fun ce cir -> 
        let e = K3.SR.Singleton(List.hd ce)
        in Node(metadata, Undecorated(e), cir))
        
  let combine_ir metadata sub_ir =
    build_code sub_ir
      (fun lru cir -> match lru with
        | [(sym1, le); (_, re)] ->
          let e = K3.SR.Combine(le,re) in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid combine expressions")
      (fun ce cir -> match ce with
        | [le; re] ->
          let e = K3.SR.Combine(le,re)
          in Node(metadata, Undecorated(e), cir)
        | _ -> failwith "invalid combine expressions")

  let op_ir ?(decorated=false) metadata o sub_ir =
    if decorated then Node(metadata, Decorated(Op(o)), sub_ir) else
    build_code sub_ir
      (fun lru cir -> match lru with
        | [(sym1, le); (_, re)] ->
          let e = k3op_of_op_t o le re in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid binop expressions")
      (fun ce cir -> match ce with
        | [le; re] ->
          let e = k3op_of_op_t o le re
          in Node(metadata, Undecorated(e), cir)
        | _ -> failwith "invalid binop expressions")

  let ifthenelse_ir metadata sub_ir =
    Node(metadata, Decorated(IfThenElse), sub_ir)
  
  let block_ir metadata sub_ir =
    build_code sub_ir
      (fun ucl cir ->
        let e = K3.SR.Block (List.map snd ucl)
        in tag_of_undecorated metadata e cir)
      (fun _ _ -> Node(metadata, Decorated(Block), sub_ir))
  
  let iterate_ir metadata sub_ir =
    build_code sub_ir
      (fun fcu cir -> match fcu with
        | [(sym, fe); (_, ce)] ->
          let e = K3.SR.Iterate(fe, ce) in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid iterate expressions")
      (fun _ _ -> Node(metadata, Decorated(Iterate), sub_ir))

  let lambda_ir metadata arg sub_ir =
    build_code sub_ir
      (fun bu cir -> match bu with
        | [(sym, be)] ->
          let e = K3.SR.Lambda(arg, be) in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid lambda body")
      (fun _ _ -> Node(metadata, Decorated(Lambda(arg)), sub_ir))

  let assoc_lambda_ir metadata arg1 arg2 sub_ir =
    build_code sub_ir
      (fun bu cir -> match bu with
        | [(sym, be)] ->
          let e = K3.SR.AssocLambda(arg1, arg2, be)
          in tag_of_undecorated metadata e cir
       | _ -> failwith "invalid assoc lambda body")
      (fun _ _ -> Node(metadata, Decorated(AssocLambda(arg1, arg2)), sub_ir))

  let apply_ir metadata sub_ir = Node(metadata, Decorated(Apply), sub_ir)

  let map_ir metadata sub_ir = Node(metadata, Decorated(Map), sub_ir)
  
  let aggregate_ir metadata sub_ir =
    Node(metadata, Decorated(Aggregate), sub_ir)

  let gb_aggregate_ir metadata sub_ir =
    Node(metadata, Decorated(GroupByAggregate), sub_ir)

  let flatten_ir metadata sub_ir = Node(metadata, Decorated(Flatten), sub_ir)

  let ext_ir metadata sub_ir = Node(metadata, Decorated(Ext), sub_ir)

  let member_ir metadata sub_ir =
    build_code sub_ir
      (fun mku cir -> match mku with
        | mape::keyse ->
          let e = K3.SR.Member(snd mape, List.map snd keyse)
          in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid member expressions")
      (fun _ _ -> Node(metadata, Decorated(Member), sub_ir))
    
  let lookup_ir metadata sub_ir = 
    build_code sub_ir
      (fun mku cir -> match mku with
        | mape::keyse ->
          let e = K3.SR.Lookup(snd mape, List.map snd keyse)
          in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid lookup expressions")
      (fun _ _ -> Node(metadata, Decorated(Lookup), sub_ir))

  let slice_ir metadata indices sub_ir =
    build_code sub_ir
      (fun mku cir -> match mku with
        | mape::keyse ->
          let e = slice_of_map (List.hd sub_ir) (List.map snd keyse) indices
          in tag_of_undecorated metadata e cir
        | _ -> failwith "invalid slice expressions")
      (fun _ _ -> Node(metadata, Decorated(Slice(indices)), sub_ir))

  let update_ir metadata m_e m_ty sub_ir =
    let r id = Node(metadata, Decorated(PCUpdate), sub_ir) in
    begin match m_e with
    | SingletonPC _ -> failwith "invalid bulk update of value"
    | OutPC(id,_,_)  | InPC(id,_,_)  | PC(id,_,_,_) -> r id
    | _ -> failwith "invalid map to bulk update"
    end

  let update_value_ir metadata m_e m_ty sub_ir =
    let r id = Node(metadata, Decorated(PCValueUpdate), sub_ir) in
    begin match m_e with
    | SingletonPC (id,_) | OutPC(id,_,_)  | InPC(id,_,_)  | PC(id,_,_,_) -> r id
    | _ -> failwith "invalid map value to update"
    end

end



module DirectIRBuilder =
struct
  open Common
  open Imperative
  open AnnotatedK3

  let rec get_value_expr expr = match expr with
    | K.Lambda(_,be) -> get_value_expr be
    | K.Apply(fn_e,_) -> get_value_expr fn_e
    | K.IfThenElse(p,t,e) -> (get_value_expr t)@(get_value_expr e)
    | K.Block(l) -> get_value_expr (List.nth l (List.length l - 1))
    | _ -> [expr]

  let is_filter_map expr = match expr with
    | K.Map(K.Lambda(_,map_e),_) ->
      let v_e = get_value_expr map_e in
      List.for_all (fun e -> match e with | IfThenElse0 _ -> true | _ -> false) v_e
    | _ -> false

  (* IR (i.e., annotated K3 construction) from a K3 expression.
   * Implemented as a fold over K3 expression nodes, with no top-down
   * accumulator, and a bottom-up accumulator of IR nodes, with type:
   *   (sym * 'ext_type Imperative.type_t) ir_t 
   *)
  let ir_of_expr e : ('exp_type imp_metadata) ir_t =
    let dummy_init =
      Leaf(mk_meta "" (Host TInt), Undecorated(K.Const(CFloat(0.0)))) in
    let fold_f _ parts e =
      let meta e =
        try mk_meta (gensym()) (Host (T.typecheck_expr e))
        with Failure _ ->
          (print_endline ("could not typecheck:");
           print_endline (string_of_expr e);
           failwith "failed to build imperative IR")
      in
      let metadata =  meta e in
      let fst () = List.hd parts in
      let snd () = List.nth parts 1 in
      let thd () = List.nth parts 2 in
      let fth () = List.nth parts 3 in
      let sfst () = List.hd (fst()) in
      let ssnd () = List.hd (snd()) in
      let sthd () = List.hd (thd()) in
      let sfth () = List.hd (fth()) in
      match e with
      | K.Const _  | K.Var _ -> undecorated_ir metadata e

      | K.Tuple _   -> tuple_ir metadata (List.flatten parts)
      | K.Project (_, p) -> project_ir metadata p [sfst()]
            
      | K.Singleton _ -> singleton_ir metadata [sfst()]
      | K.Combine _   -> combine_ir metadata [sfst();ssnd()]
      | K.Add _       -> op_ir metadata Add [sfst(); ssnd()]
      | K.Mult _      -> op_ir metadata Mult [sfst(); ssnd()]
      | K.Eq _        -> op_ir metadata Eq [sfst(); ssnd()]
      | K.Neq _       -> op_ir metadata Neq [sfst(); ssnd()]
      | K.Lt _        -> op_ir metadata Lt [sfst(); ssnd()]
      | K.Leq _       -> op_ir metadata Leq [sfst(); ssnd()]
      | K.IfThenElse0 _ ->
        let decorated = match (type_of_meta metadata) with
          | Host TFloat | Host TInt -> false
          | _ -> true
        in op_ir ~decorated:decorated metadata If0 [sfst(); ssnd()] 

      | K.Comment(_,cexp) -> sfst()
      | K.Block l      -> block_ir metadata (fst())

      | K.Iterate _    -> iterate_ir metadata [sfst(); ssnd()]
      | K.IfThenElse _ -> ifthenelse_ir metadata [sfst(); ssnd(); sthd()]

      | K.Lambda (arg,_) -> lambda_ir metadata arg [sfst()]
      | K.AssocLambda (arg1,arg2,_) -> assoc_lambda_ir metadata arg1 arg2 [sfst()]

      | K.Apply _  -> apply_ir metadata [sfst();ssnd()]

      (* Null filtering. *)
      | K.Map _ ->
        let map_meta =
          if not(is_filter_map e) then metadata
          else mk_meta ~valid:(Some(gensym())) (sym_of_meta metadata) (type_of_meta metadata)
        in map_ir map_meta [sfst();ssnd()]

      | K.Aggregate _ -> aggregate_ir metadata [sfst(); ssnd(); sthd()]

      | K.GroupByAggregate _ ->
        gb_aggregate_ir metadata [sfst();ssnd();sthd();sfth()] 

      (* Ext = Flat(Map) *)
      | K.Flatten(K.Map _) ->
        begin match sfst() with
          | Node(m,t,c) -> ext_ir metadata c
          | _ -> failwith "invalid map IR node"
        end

      | K.Flatten _ -> flatten_ir metadata [sfst()]

      | K.Member _ -> member_ir metadata ([sfst()]@snd())  
      | K.Lookup _ -> lookup_ir metadata ([sfst()]@snd())
      | K.Slice (_,sch,idk_l)      ->
        let index l e =
          let (pos,found) = List.fold_left (fun (c,f) x ->
            if f then (c,f) else if x = e then (c,true) else (c+1,false))
            (0, false) l
          in if not(found) then raise Not_found else pos
        in
        let v_l, _ = List.split idk_l in
        let idx_l = List.map (index (List.map (fun (x,y) -> x) sch)) v_l in
        Debug.print "K3-DEBUG-SLICE" (fun () ->
          "SLICE : "^(K.string_of_expr e)^" \nproduces list: "^
          (list_to_string string_of_int idx_l)^"\n"
        );
           slice_ir metadata idx_l ([sfst()]@snd())

      | K.SingletonPC _ | K.OutPC _ | K.InPC _ | K.PC _ -> undecorated_ir metadata e

      | K.PCUpdate (m_e,_,_) -> 
        update_ir metadata m_e (T.typecheck_expr m_e) ([sfst()]@snd()@[sthd()])

      | K.PCValueUpdate (m_e,_,_,_) -> 
        update_value_ir metadata
          m_e (T.typecheck_expr m_e) ([sfst()]@snd()@thd()@[sfth()])

    in K.fold_expr fold_f (fun x _ -> x) None dummy_init e
end


module Imp =
struct
  open AnnotatedK3
  open Imperative
  open Common
  module AK = AnnotatedK3
  module K = K3.SR
  module KT = K3Typechecker

  (* Helpers *)

  let bind_arg arg expr =
    match arg with
    | AVar(id,ty) -> [Decl(None, (id, (Host ty)), Some(expr))]
    | ATuple(id_ty_l) ->
      begin match expr with
      | Tuple(_, e_l) ->
        List.map2 (fun (id,ty) e ->
          Decl(None, (id, (Host ty)), Some(e))) id_ty_l e_l
    
      | Var(tty, tvar) ->
        snd (List.fold_left (fun (i,acc) (id,ty) ->
            let t_access = Fn(None, TupleElement(i), [Var(tty, tvar)]) in
            (i+1, acc@[Decl(None, (id, (Host ty)), Some(t_access))]))
          (0,[]) id_ty_l) 
    
      | _ -> failwith "invalid tuple apply" 
      end

  (* IR tree linearization, returning a list of:
   *    (node metadata * (tag * child node metadata))     *)
  let linearize_ir c =
    let rec aux acc c = match c with
      | Leaf(meta, l) -> [meta, (l,[])]@acc
      | Node(meta, tag, children) ->
        (List.fold_left aux acc children)@
        [meta, (tag, List.map meta_of_ir children)]
    in aux [] c

  (* Declaration initialization *)
  let vars_of_expr expr =
    let fold_f top parts e = match e with
      | Var (_,v) -> [v]
      | _ -> List.flatten parts
    in fold_expr fold_f (fun x _ -> x) None [] expr

  let rec inline_decls env imp =
    let rec aux envacc dacc eacc i = match i with
      | Expr(None, BinOp(None, Assign, Var(None, ((id,_) as d)), e)) ->
        if not (List.mem_assoc d dacc) then envacc, dacc, eacc, [i]
        else 
          let denv = List.map fst (List.assoc d dacc) in
          if List.for_all (fun v ->
               List.mem (fst v) denv  && not(List.mem_assoc v dacc))
             (vars_of_expr e)
          then
            envacc, List.remove_assoc d dacc, eacc@[Decl(None, d, Some(e))], []
          else envacc, dacc, eacc, [i]

      | Block (None,l) ->
        let w,x,y,z = List.fold_left (fun (envacc,dacc,eacc,iacc) li ->
          let a,b,c,d = aux envacc dacc eacc li
          in a,b,c,(iacc@d)) (envacc,dacc,eacc,[]) l
        in
        let runb, lunb = List.partition (fun (d,_) -> List.mem d envacc) x in
        let rinit, linit = List.partition (fun e -> match e with 
          | Decl(_,d,_) -> List.mem d envacc
          | _ -> failwith "invalid initialized declaration") y
        in
        let local_decls = List.map (fun (d,_) -> Decl(None, d, None)) lunb
        in envacc, runb, rinit, [Block(None, local_decls@linit@z)]

      | Decl(None, d, None) -> envacc@[d], dacc@[d, envacc], eacc, []
      | _ -> envacc, dacc, eacc, [i]
    in
    let _, unbound_decls, init_decls, rest = aux env [] [] imp in
    let decls = List.map (fun (d,_) -> Decl(None, d, None)) unbound_decls in
      begin match rest with
       | [] -> List.hd (decls@init_decls)
       | [Block(None, l)] -> Block(None, decls@init_decls@l)
       | [x] -> Block(None, decls@init_decls@[x])
       | _ -> failwith "invalid declaration initialization"  
      end

  let inline_decls_of_imp_list env il =
    begin match inline_decls env (Block(None, il)) with
    | Block(None, l) -> l
    | _ -> failwith "invalid declaration initialization"
    end    

  (* Imperative expression construction, from an IR tree. *)
  let imp_of_ir (env : 'ext_type_t var_t list)
                 ir : ('a option, 'ext_type, 'ext_fn) imp_t list =
    let flat_ir = linearize_ir ir in

    Debug.print "IMP-IR" (fun () ->
      String.concat "\n" (List.map (fun (meta,(tag,c)) -> 
        (sym_of_meta meta)^": "^(String.concat "," (List.map sym_of_meta c))^"\n"^
        (sym_of_meta meta)^": "^(string_of_tag tag)) flat_ir));
    
    let gc_op op = match op with
      | AK.Add -> Add | AK.Mult -> Mult | AK.Eq -> Eq | AK.Neq -> Neq
      | AK.Lt -> Lt | AK.Leq -> Leq | AK.If0 -> If0
    in
    let rec gc_binop meta op l r = BinOp(meta, op, gc_expr l, gc_expr r)
    and gc_expr e : ('a option, 'ext_type, 'ext_fn) expr_t =
      let imp_meta = None in
      match e with
      | K.Const c          -> Const (imp_meta, c)
      | K.Var (v,t)        -> Var (imp_meta,(v,Host t))
      | K.Tuple fields     -> Tuple(imp_meta, List.map gc_expr fields)
      | K.Add(l,r)         -> gc_binop imp_meta Add  l r
      | K.Mult(l,r)        -> gc_binop imp_meta Mult l r
      | K.Eq(l,r)          -> gc_binop imp_meta Eq   l r
      | K.Neq(l,r)         -> gc_binop imp_meta Neq  l r
      | K.Lt(l,r)          -> gc_binop imp_meta Lt   l r
      | K.Leq(l,r)         -> gc_binop imp_meta Leq  l r
      | K.IfThenElse0(l,r) -> gc_binop imp_meta If0  l r
      | K.Project(e,idx)   -> Fn (imp_meta, Projection(idx), [gc_expr e])
      | K.Singleton(e)     -> Fn (imp_meta, Singleton, [gc_expr e])
      | K.Combine(l,r)     -> Fn (imp_meta, Combine, List.map gc_expr [l;r])
      
      | K.Member(m,k)      -> Fn (imp_meta, Member, List.map gc_expr (m::k))
      | K.Lookup(m,k)      -> Fn (imp_meta, Lookup, List.map gc_expr (m::k))
      | K.Slice(m,sch,fe)  ->
         let pos l e =
            let idx = ref (-1) in
            let rec aux l = match l with
              |  [] -> -1
              | h::t ->
                incr idx;
                if h = e then raise Not_found else aux t
            in try aux l with Not_found -> !idx
         in
         let idx = List.map (pos (List.map fst sch)) (List.map fst fe)
         in Fn (imp_meta, Slice(idx), List.map gc_expr (m::(List.map snd fe)))

      | K.SingletonPC(id,_) 
      | K.OutPC(id,_,_) | K3.SR.InPC(id,_,_) | K3.SR.PC(id,_,_,_) ->
        Var(imp_meta, (id, Host(KT.typecheck_expr e)))

      (* Lambdas assume caller has performed arg binding *)
      | K.Lambda(arg, body) -> gc_expr body
      | K.AssocLambda(arg1, arg2, body) -> gc_expr body

      | _ -> failwith ("invalid imperative expression: "^(K.string_of_expr e))
    in

    (* Code generation for tagged nodes *)
    let rec gc_tag meta t cmeta =
      (* Helpers *)

      let cmetai i = List.nth cmeta i in
      let ciri i = List.assoc (cmetai i) flat_ir in
      let ctagi i = fst (ciri i) in
      
      (* Symbol pushdown/reuse semantics *)
      (* possible_decl : force * id * type option
       * -- possible_decl should be None if the symbol should not be defined
       *    locally (i.e. any pushdowns should not be defined)
       * -- force indicates whether to force declaration of the symbol, given
       *    that the tag cg will bind to the symbol
       *)  
      let child_meta, args_to_bind, possible_decls =
        let last = (List.length cmeta) - 1 in
        let pushi i = push_meta meta (cmetai i) in
        let pushli i = push_lambda_meta meta (cmetai i) in
        let decl_f = decl_of_meta in 
        let arg_f = meta_of_arg in
        let assoc_arg_f = meta_of_assoc_arg in
        match t with
        | Lambda _ | AssocLambda _ -> [pushi 0], [], [None]
        
        (* Apply always pushes down the return symbol to the function, and
         * in the case of single args, push down the arg symbol *)
        (* TODO: multi-arg pushdown/binding *)
        | Apply ->
          let arg = arg_of_lambda (ctagi 0) in
          let arg_meta, rem_bindings, arg_decl = arg_f arg (cmetai 1) in
          [pushli 0; arg_meta], rem_bindings, [None; arg_decl]
        
        (* Blocks push down the symbol to the last element *)    
        | AK.Block ->
          let r = (List.rev (List.tl (List.rev cmeta)))@[pushi last]
          in r, [], List.map (fun x -> None) r
        
        (* Conditionals push down the symbol to both branches *)
        | AK.IfThenElse ->
          [cmetai 0; pushi 1; pushi 2], [], [decl_f false (cmetai 0); None; None]

        (* Iterate needs a child symbol, but this should not be used since
         * the return type is unit. Maps yield the new collection as the symbol
         * thus does not pass it on to any children. *)
        | Iterate | Map ->
          let fn_meta = match valid_sym_of_meta meta with
            | None -> push_lambda_meta (cmetai 0) (cmetai 0)
            | Some _ -> push_meta_valid meta (cmetai 0)
          in (fn_meta::(List.tl cmeta)),
             [arg_of_lambda (ctagi 0)], [None; decl_f false (cmetai 1)]

        (* Ext passes on its symbol to children to perform direct concatenation
         * of the collection resulting from each ext lambda invocation to the
         * return value *)
        | AK.Ext ->
          [pushi 0; (cmetai 1)], [arg_of_lambda (ctagi 0)], [None; decl_f false (cmetai 1)]
         
        (* 
        | FilterMap arg ->
          cmeta, [arg], [None; None; decl_f false (cmetai 2)]
        *)

        | Aggregate ->
            let arg1, arg2 = arg_of_assoc_lambda (ctagi 0) in
            let coll_meta = cmetai 2 in
            let assoc_arg_meta, rem_bindings, arg_decls =
                assoc_arg_f arg1 arg2 (cmetai 1)
            in (assoc_arg_meta@[coll_meta]), rem_bindings,
                 (arg_decls@[decl_f false coll_meta])

        (*
         * -- 4 syms: agg fn return, init return, gb return, collection return
         * -- Node sym should not be passed down, it is a new map. Map should
         *    be assigned to after agg body runs.
         * -- collection sym is unchanged
         * -- group-by arg can be made element, group-by sym is used to bind
         *    agg fn state arg via map member/lookup
         * -- init val sym is used to bind agg fn state arg on first run
         * -- agg fn elem arg must be bound to element, could be avoided if
         *    gb arg is same as agg fn elem arg
         * -- agg fn state arg must be bound to map lookup/init val sym. if
         *    state is a single var, we can push to init code
         * -- summary:
         *   ++ push single gb arg to elem (not here, in cg body)
         *   ++ avoid binding agg fn elem arg if same as gb arg (in body)
         *   ++ push single agg fn state var to init return sym (and map lookup
         *      sym in cg body), otherwise use new sym. skip binding if pushed.
         *) 
        | GroupByAggregate ->
            let arg1, arg2 = arg_of_assoc_lambda (ctagi 0) in
            let arg3 = arg_of_lambda (ctagi 2) in
            let arg2_binding, init_meta = match arg2 with
              | AVar(id,ty) -> [], mk_meta id (Host ty)
              | _ -> let m = cmetai 1 in [arg2], m 
            in
            let x = mk_meta (sym_of_meta (cmetai 0)) (type_of_meta init_meta) in
            let y =
              let gb_t = match type_of_meta (cmetai 2) with
                | Host(K.Fn(_,rt)) -> Host rt
                | Host(K.TInt) -> Host K.TInt (* let this through for untyped compilation *)
                | _ -> failwith "invalid group by function type"
              in mk_meta (sym_of_meta (cmetai 2)) gb_t
            in
            let z = cmetai 3 in
            let dx, dy, dz = decl_f false x, decl_f true y, decl_f false z in  
              ([x; init_meta; y; z],
                ([arg1]@arg2_binding@[arg3]), [dx; decl_f true init_meta; dy; dz])
  
        | _ -> cmeta, [], List.map (fun m -> decl_f false m) cmeta
      in

      (* Recursive call to generate child imperative or expression code *)
      let cuie = List.map2 (fun cmeta meta_to_use ->
        let (ctag, ccm) = List.assoc cmeta flat_ir in
        gc_meta (meta_to_use, (ctag, ccm))) cmeta child_meta
      in

      let unique l = List.fold_left
        (fun acc e -> if List.mem e acc then acc else acc@[e]) [] l
      in
      let child_decls = unique (List.flatten (List.map2
        (fun (used,i,e) decl_opt ->
          match i,e, decl_opt with
          | Some _, None, Some(force,id,ty) ->
            if force || used then [Decl(None, (id,ty), None)] else []
          | None, Some _, Some(true, id,ty) -> [Decl(None, (id,ty), None)]
          | _,_,_ -> []) cuie possible_decls))
      in
    
      let cie = List.map (fun (_,x,y) -> x,y) cuie in

      (* Imp construction helpers *) 
      let imp_of_list l =
        if List.length l = 1 then List.hd l else Block(None, l) in
      let unwrap x = match x with Some(y) -> y | _ -> failwith "invalid value" in
      let list_i l = function 0 -> List.hd l | i -> List.nth l i in

      let match_ie_pair ie error_str f_i f_e = match ie with
        | Some(i), None -> f_i i
        | None, Some(e) -> f_e e
        | _,_ -> failwith error_str
      in
      
      let cimps, cexprs = List.split cie in
      let cvals = List.map2 (fun meta iep ->
        match_ie_pair iep "invalid child value"
          (fun i -> Var (None, (sym_of_meta meta, type_of_meta meta)))
          (fun e -> e))
          child_meta cie
      in
      
      let cimpo, cexpro = list_i cimps, list_i cexprs in
      let cdecli i =
        let m = list_i child_meta i in
        Var (None, (sym_of_meta m, type_of_meta m)) in
      let cexpri = list_i cvals in
      let cused i = let (x,_,_) = list_i cuie i in x in

      let match_ie idx = match_ie_pair (cimpo idx, cexpro idx) in
      let assign_if_expr idx error_str = match_ie idx error_str
        (fun i -> i)
        (fun e -> [Expr(None, BinOp(None, Assign, cdecli idx, e))])
      in

      let meta_sym, meta_ty = sym_of_meta meta, type_of_meta meta in

      let crv i =
        let mk_var id ty = Var(None, (id,ty)) in
        begin match cdecli i with
        | Var(_,(id,ty)) ->
          begin match ty with
          | Host(K3.SR.Fn(_,x)) -> mk_var id (Host x)
          | _ -> mk_var id ty
          end
        | _ -> failwith "invalid child value"
        end
      in

      let expr =
        let m = None in
        match t with
        | AK.Op(op)    ->
          if op = AK.If0 && (valid_sym_of_meta meta) <> None then None
          else Some(BinOp(m, gc_op op, cexpri 0, cexpri 1))

        | AK.Tuple           -> Some(Tuple(m, cvals))
        | AK.Projection(idx) -> Some(Fn(m, Projection(idx), [cexpri 0]))

        | AK.Singleton  -> Some(Fn(m, Singleton, [cexpri 0]))
        | AK.Combine    -> Some(Fn(m, Combine, [cexpri 0; cexpri 1]))     
        | AK.Member     -> Some(Fn(m, Member, cvals))
        | AK.Lookup     -> Some(Fn(m, Lookup, cvals))
        | AK.Slice(idx) -> Some(Fn(m, Slice(idx), cvals))
        | _ -> None
      in

      let imp, imp_meta_used = match t with
      | AK.Op(AK.If0) when (valid_sym_of_meta meta) <> None ->
        let valid_var = Var(None, (unwrap (valid_sym_of_meta meta), Host TFloat)) in
        let meta_var = Var(None, (sym_of_meta meta, type_of_meta meta)) in
        Some([IfThenElse(None,
          BinOp(None, Neq, cexpri 0, Const(None, CFloat(0.0))),
          Block(None,
            [Expr(None, BinOp(None, Assign, valid_var, Const(None, CFloat(1.0))));
             Expr(None, BinOp(None, Assign, meta_var, cexpri 1))]),
          Block(None, []))]),
        true
         
      | Lambda _ | AssocLambda _ ->
        (* lambdas have no expression form, they must use their arg bindings
           and assign result value to their indicated symbol *)
        begin match cimpo 0, cexpro 0 with
          | Some(i), None -> Some(i), (cused 0)
          | _, _ -> failwith "invalid tagged function body"
        end

      | Apply ->
        Debug.print "IMP-DEBUG-APPLY" (fun _ -> string_of_k3ir ir);
        let decls = match cimpo 1, cexpro 1 with
          | Some(i), None -> 
            if args_to_bind = [] then i
            else i@(bind_arg (List.hd args_to_bind) (cdecli 1))
          | None, Some(e) ->
            if args_to_bind = [] then
               [Expr(None, BinOp(None, Assign, cdecli 1, e))]
            else bind_arg (List.hd args_to_bind) e
          | _, _ -> failwith "invalid apply argument"
        in
        let used = match_ie 0 "invalid apply function"
          (fun _ -> cused 0) (fun _ -> true) in
        let body = assign_if_expr 0 "invalid apply function"
        in Some(decls@body), used

      | AK.Block -> 
        let rest, last =
          let x = List.rev (List.combine cimps cexprs)
          in List.rev (List.tl x), List.hd x
        in
        let imp_of_pair ieo_pair = match ieo_pair with
          | Some(i), None -> i
          | _,_ -> failwith "invalid block element"
        in
        let li, meta_used = match last with
          | Some(i), None -> i, (cused ((List.length cmeta)-1))
          | None, Some(e) ->
            [Expr(None, BinOp(None, Assign,
                                Var(None, (meta_sym, meta_ty)), e))],
            true
          | _, _ -> failwith "invalid block return"
        in Some((List.flatten (List.map imp_of_pair rest))@li), meta_used

      | AK.IfThenElse ->
        let branch i = imp_of_list
          (assign_if_expr i "invalid condition branch code") in
        let branch_used i = match_ie i "invalid condition branch"
          (fun _ -> cused i) (fun _ -> true) in
        let meta_used = branch_used 1 || branch_used 2 in
        Some(match_ie 0 "invalid condition code"
            (fun i -> i@[IfThenElse(None, cdecli 0, branch 1, branch 2)])
            (fun e -> [IfThenElse(None, e, branch 1, branch 2)])),
          meta_used

      | Iterate ->
        let arg = List.hd args_to_bind in
        let elem, elem_ty, elem_f, decls = match arg with
            | AVar(id,ty) -> id, (Host ty), true, []
            | ATuple(it_l) ->
              let t = Host(K.TTuple(List.map snd it_l)) in 
              let x = gensym() in x, t, false, bind_arg arg (Var(None,(x,t)))
        in
        let fn_body = match_ie 0 "invalid iterate function"
          (fun i -> i) (fun e -> failwith "invalid iterate function") in
        let loop_body = imp_of_list (decls@fn_body) in
          Some(match_ie 1 "invalid iterate collection"
            (fun i -> i@[For(None, ((elem, elem_ty), elem_f), cdecli 1, loop_body)])
            (fun e -> [For(None, ((elem, elem_ty), elem_f), e, loop_body)])), false

      | Map | AK.Ext ->
        (* TODO: in-loop multi-var bindings from collections of tuples *)
        let is_ext = match t with | AK.Ext -> true | _ -> false in
        let is_filter = valid_sym_of_meta meta <> None in
        let arg = List.hd args_to_bind in
        let elem, elem_ty, elem_f, elem_decls = match arg with
            | AVar(id,ty) -> id, Host(ty), true, []
            | ATuple(it_l) ->
              let t = Host(K.TTuple(List.map snd it_l)) in 
              let x = gensym() in x, t, false, bind_arg arg (Var (None,(x,t)))
        in
        (* Loops must define any declarations used by the map/ext lambda, such as
         * nested map temporary collection declarations, inside the loop body.
         * This is not achieved by a child declaration, which
         * is always defined prior to the expression's code. Thus we handle
         * map lambda declarations as a special case here *)
        let body_decls =
          let (ctag, gcmeta) = ciri 0 in
          if is_ext || not(cused 0) || gcmeta = [] then [] else
            let d_t =
              match type_of_meta (cmetai 0) with
              | Host (K.Fn(_,rt)) -> Host(rt)
              | _ -> failwith "invalid map/ext lambda type while building imp"
            in   
            let fn_rv_decl =
              let d = sym_of_meta (cmetai 0), d_t in [Decl(None, d, None)] in
            let valid_decl = match valid_sym_of_meta meta with
              | None -> []
              | Some(vsym) -> [Decl(None, (vsym, Host TFloat), None)] 
            in valid_decl@fn_rv_decl
        in
        let mk_body e =
          let op = if is_ext then Concat else ConcatElement in
          let append_imp =
            Expr(None, Fn(None, op, [Var (None, (meta_sym, meta_ty)); e])) in
          if is_filter then
            let valid_var =
              Var(None, (unwrap (valid_sym_of_meta meta), Host TFloat))
            in [IfThenElse(None,
                  BinOp(None, Neq, valid_var, Const(None, CFloat(0.0))),
                  append_imp, Block(None,[]))]
          else if (is_ext && not(cused 0)) || not(is_ext) then [append_imp]
          else []
        in
        let fn_body = match_ie 0 "invalid map/ext function"
          (fun i ->
            let nb = mk_body (crv 0) in
            match i with | [Block(m,l)] -> [Block(m,l@nb)] | _ -> i@nb)
          (fun e -> if not(is_ext) || not(cused 0) then mk_body e
                    else failwith "invalid ext expression body")
        in
        let loop_body = imp_of_list (elem_decls@body_decls@fn_body) in
        let mk_loop e = For(None, ((elem, elem_ty), elem_f), e, loop_body) in
          Some(match_ie 1 "invalid map/ext collection"
             (fun i -> i@[mk_loop (cdecli 1)]) (fun e -> [mk_loop e])),
          (if is_ext then (cused 0) else true)

      | Aggregate -> 
        let elem, elem_ty, elem_f, decls = match args_to_bind with
          | [AVar(id,ty)] -> id, (Host ty), true, []
          | [ATuple(it_l) as arg1] ->
            let t = Host(K.TTuple(List.map snd it_l)) in 
            let x = gensym() in x, t, false, bind_arg arg1 (Var (None,(x,t)))
          | [AVar(id,ty); arg2] -> id, (Host ty), true, bind_arg arg2 (cdecli 1)
          | [ATuple(it_l) as arg1; arg2] ->
            let t = Host(K.TTuple(List.map snd it_l)) in
            let x = gensym() in
              x, t, false, (bind_arg arg1 (Var (None,(x,t))))@(bind_arg arg2 (cdecli 1))
          | _ -> failwith "invalid aggregate args"
        in     
        let fn_body = assign_if_expr 0 "invalid aggregate function" in
        let loop_body = imp_of_list (decls@fn_body) in
        let pre, ce =
          let init_pre = assign_if_expr 1 "invalid agg init code" in
          let collection_pre, collection_e =
            match_ie 2 "invalid agg collection"
              (fun i -> i, (cdecli 2)) (fun e -> [], e) 
           in (collection_pre@init_pre), collection_e in
        let post = [Expr(None,
          BinOp(None, Assign, Var(None,(meta_sym, meta_ty)), cdecli 0))]
        in
        Some(pre@[For(None, ((elem, elem_ty), elem_f), ce, loop_body)]@post), true
      
      | GroupByAggregate ->
        let get_elem arg = match arg with
          | AVar(id, ty) -> id, (Host ty), true, []
          | ATuple(it_l) ->
            let t = Host(K.TTuple(List.map snd it_l)) in 
            let x = gensym() in x, t, false, bind_arg arg (Var (None, (x,t)))
        in
        let eg_decls elem_arg g_arg =
          let e, e_ty, e_f, edecls = get_elem g_arg in
          let eadecls =
            if elem_arg <> g_arg then bind_arg elem_arg (Var (None, (e,e_ty))) else []
          in e, e_ty, e_f, edecls, eadecls
        in
        let (elem, elem_ty, elem_f, edecls, eadecls), sdecls =
          match args_to_bind with
          | [elem_arg; g_arg] -> (eg_decls elem_arg g_arg), []
          | [elem_arg; state_arg; g_arg] -> 
            (eg_decls elem_arg g_arg), (bind_arg state_arg (cdecli 1))
          | _ -> failwith "invalid group by aggregate arguments"
        in
        (* Invoke gb function *)
        let gb_body = edecls@(assign_if_expr 2 "invalid group by function") in
        (* Retrieve group state or initialize if needed *)
        let pre_fn_body = 
          let state_init = assign_if_expr 1 "invalid gb agg init" in
            [IfThenElse(None,
               Fn(None, Member, [Var (None, (meta_sym, meta_ty)); cdecli 2]),
               Expr(None, BinOp(None, Assign, cdecli 1,
                 Fn(None, Lookup, [Var (None, (meta_sym, meta_ty)); cdecli 2]))),
               imp_of_list (state_init))] 
        in
        (* Bind agg fn decls, invoke agg fn, and assign to result map *)
        let mk_body e = Expr(None, Fn(None,
          MapValueUpdate, [Var (None,(meta_sym, meta_ty)); cdecli 2; e])) in
        let fn_body = eadecls@sdecls@(match_ie 0 "invalid gb agg function"
          (fun i -> i@[mk_body (cdecli 0)]) (fun e -> [mk_body e]))
        in
        let loop_body = imp_of_list (gb_body@[Block(None, pre_fn_body@fn_body)]) in
        let pre, ce =
          match_ie 3 "invalid gb agg collection"
            (fun i -> i, (cdecli 3)) (fun e -> [], e) 
        in Some(pre@[For(None, ((elem, elem_ty), elem_f), ce, loop_body)]), true
      
      | Flatten -> 
        let outer_elem, outer_ty = gensym(), meta_ty in
        let outer_pre,outer_src = match_ie 0 "invalid flatten nested collection"
          (fun i -> i,cdecli 0) (fun e -> [],e) in
        let inner_elem, inner_ty = match meta_ty with
          | Host(Collection(x)) -> gensym(), Host x
          | _ -> failwith "invalid flatten collection" 
        in
        (* HACK: explicitly use a declared value to ensure correct substitution
         * in loops. This can be fixed by performing general substitution of
         * the loop element in the loop body, rather than just in the body's
         * declarations.
         *)
        let inner_var_id, inner_var_ty, inner_var =
          let x = gensym() in x, inner_ty, Var(None, (x,inner_ty)) in 
        let inner_decl =
          Decl(None, (inner_var_id, inner_var_ty),
            Some(Var (None, (inner_elem, inner_ty)))) in
        let inner_body = Expr(None,
          Fn(None, ConcatElement, [Var (None, (meta_sym, meta_ty)); inner_var])) in 
        let inner = 
          For(None, ((inner_elem, inner_ty), false),
            Var(None, (outer_elem, outer_ty)), Block(None, [inner_decl; inner_body]))
        in Some(outer_pre@
             [For(None, ((outer_elem, outer_ty), false), outer_src, inner)]),
           true

      | AK.PCUpdate ->
        let pre = List.map unwrap (List.filter ((<>) None) cimps) in
          Some((List.flatten pre)@
            [Expr(None, Fn(None, MapUpdate, cvals))]), false
      
      | AK.PCValueUpdate ->
        let pre = List.map unwrap (List.filter ((<>) None) cimps) in
          Some((List.flatten pre)@
           [Expr(None, Fn(None, MapValueUpdate, cvals))]), false

      | _ -> None, false
    
      in 
      begin match imp, expr with
        | None, Some(e) -> 
          let pre = List.map unwrap (List.filter ((<>) None) cimps) in
          let b = child_decls@(List.flatten pre)@
            [Expr(None, BinOp(None, Assign, Var (None, (meta_sym, meta_ty)), e))]
          in
          if List.length b = 1 then (false, None, Some(e))
          else (true, Some(inline_decls_of_imp_list env b), None)
        
        | Some(i), None -> 
            let ro = Some(
              if child_decls = [] then i
              else [(Block(None, inline_decls_of_imp_list env (child_decls@i)))]) 
            in imp_meta_used, ro, expr
        | _, _ -> failwith "invalid tag compilation"
      end
    and gc_meta (meta, (tag, cmeta))
          : (bool * (('a option, 'ext_type, 'ext_fn) imp_t list) option 
                  * (('a option, 'ext_type, 'ext_fn) expr_t) option) =
      match tag with
      | Decorated t -> gc_tag meta t cmeta
      | Undecorated e -> 
        begin match cmeta with
          | [] -> false, None, Some(gc_expr e)
          | _ ->
            (* Recursive call to generate child imperative or expression code *)
            let cimp = List.flatten (List.map (fun m -> 
              let (ctag, ccm) = List.assoc m flat_ir in
              let (used,io,eo) = gc_meta (m, (ctag, ccm)) in
              let decl = Decl(None, (sym_of_meta m, type_of_meta m), None) in
              begin match io,eo with
                | Some(i), None -> if used then [decl]@i else i
                | None, Some(e) ->
                  [decl; Expr(None, BinOp(None, Assign,
                                Var (None, (sym_of_meta m, type_of_meta m)), e))]
                | _,_ -> failwith "invalid child code"
              end) cmeta)
            in
            let r = [Expr(None, BinOp(None, Assign,
                     Var (None, (sym_of_meta meta, type_of_meta meta)), gc_expr e))]
            in true, Some(inline_decls_of_imp_list env (cimp@r)), None
        end 
    in
    (* Top-down code generation (i.e. from top of K3 expression, which is the
     * bottom of the linearized list)  *)
    let root_meta, root_tc = List.nth flat_ir ((List.length flat_ir)-1) in
    let root_decl =
      let decl_meta = None in
      Decl(decl_meta, (sym_of_meta root_meta, type_of_meta root_meta), None) in
    let r = match gc_meta (root_meta, root_tc) with
      | meta_used, Some(i), None ->
        (if meta_used then [root_decl] else [])@i
      | _, None, Some(e) ->
        [root_decl; Expr(None, BinOp(None, Assign,
            Var (None, (sym_of_meta root_meta, type_of_meta root_meta)), e))]
      | _, _, _ -> failwith "invalid code"   
    in inline_decls_of_imp_list env r

end