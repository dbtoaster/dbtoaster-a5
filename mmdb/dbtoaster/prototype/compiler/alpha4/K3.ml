(*
 * TODO: add flattens where appropriate for joins/products
 * TODO: add lambda argument variant for variable, and tuple of vars
 *)

module M3P = M3.Prepared
open M3
open M3Common

(* Signatures *)
module type SRSig =
sig
    type id_t = M3.var_id_t
    type coll_id_t = M3.map_id_t
    
    type prebind   = M3.Prepared.pprebind_t
    type inbind    = M3.Prepared.pinbind_t
    type m3schema  = M3.var_t list
    type extension = M3.var_t list
    type pattern   = int list

    (*
    type fn_id_t = string
    type iap_meta  = pattern * m3schema * m3schema * extension
    type ext_fn_id = IndexAndProject of iap_meta | Symbol of fn_id_t
    *)

    type type_t =
	      Unit | Float | Int
        | TTuple     of type_t list     (* unnamed records *)
	    | Collection of type_t          (* collections *)
	    | Fn         of type_t * type_t (* arg * body *)

    type schema = (id_t * type_t) list

    type expr_t =
   
	   (* Terminals *)
	     Const         of M3.const_t
	   | Var           of id_t        * type_t
	
	   (* Tuples, i.e. unnamed records *)
	   | Tuple         of expr_t list
	   | Project       of expr_t      * int list
	
	   (* Collection construction *)
	   | Singleton     of expr_t
	   | Combine       of expr_t      * expr_t 
	
	   (* Arithmetic and comparison operators, conditionals *) 
	   | Add           of expr_t      * expr_t
	   | Mult          of expr_t      * expr_t
	   | Eq            of expr_t      * expr_t
	   | Neq           of expr_t      * expr_t
	   | Lt            of expr_t      * expr_t
	   | Leq           of expr_t      * expr_t
	   | IfThenElse0   of expr_t      * expr_t

       (* Control flow: conditionals, sequences, side-effecting iterations *)
	   | IfThenElse    of expr_t      * expr_t   * expr_t
	   | Block         of expr_t list 
       | Iterate       of expr_t      * expr_t  
	
	   (* Functions *)
	   | Lambda        of id_t        * type_t   * expr_t * bool
	   | AssocLambda   of id_t        * type_t   * id_t   * type_t * expr_t
	   | Apply         of expr_t      * expr_t
	
	   (* Structural recursion operators *)
	   | Map              of expr_t      * expr_t 
	   | Flatten          of expr_t 
	   | Aggregate        of expr_t      * expr_t   * expr_t
	   | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t

       (* Tuple collection operators *)
       | Member      of expr_t      * expr_t list  
       | Lookup      of expr_t      * expr_t list
       | Slice       of expr_t      * schema      * (id_t * expr_t) list

	   (* Persistent collections *)
	   | SingletonPC   of coll_id_t   * type_t
	   | OutPC         of coll_id_t   * schema   * type_t
	   | InPC          of coll_id_t   * schema   * type_t    * expr_t
	   | PC            of coll_id_t   * schema   * schema    * type_t * expr_t

	   | PCUpdate      of expr_t      * expr_t list * expr_t
	   | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 
    
	   (*| External      of ext_fn_id*)

    (* Construction from M3 *)
    val calc_to_singleton_expr : M3.Prepared.calc_t -> expr_t
    
    val op_to_expr : (M3.var_t list -> expr_t -> expr_t -> expr_t) ->
        M3.Prepared.calc_t -> M3.Prepared.calc_t -> M3.Prepared.calc_t -> expr_t
    
    val calc_to_expr : M3.Prepared.calc_t -> expr_t

    (* K3 methods *)
    val type_as_string : type_t -> string
    val typecheck_expr : expr_t -> type_t
    
    (* Helpers *)
    val collection_of_list : expr_t list -> expr_t
    val collection_of_float_list : float list -> expr_t
    
    (* Incremental section *)
    type statement = expr_t * expr_t
    type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list
    type program = M3.map_type_t list * M3Common.Patterns.pattern_map * trigger list

    val m3rhs_to_expr : M3.var_t list -> M3.Prepared.aggecalc_t -> expr_t

    val collection_stmt : M3.var_t list -> M3.Prepared.stmt_t -> statement
    
    val collection_trig : M3.Prepared.trig_t -> trigger
    
    val collection_prog :
        M3.Prepared.prog_t -> M3Common.Patterns.pattern_map -> program

end


module SR : SRSig =
struct

(* Metadata for code generation *)

(* From M3.Prepared *)
type prebind   = M3.Prepared.pprebind_t
type inbind    = M3.Prepared.pinbind_t
type m3schema  = M3.var_t list
type extension = M3.var_t list

type pattern   = int list
 
(* prefix bindings, infix bindings, lhs schema, rhs schema, output schema,
 * lhs->rhs extension, rhs->output extension
 * 
 * -- how are lhs->rhs extensions different from infix bindings? *)
type expr_meta =
   prebind * inbind * m3schema * m3schema * m3schema * extension * extension

(* index pattern, pattern schema, output schema, rhs->output extension *)
(*
type iap_meta = pattern * m3schema * m3schema * extension
*)

(*------------------------------------------------------------------------------
 *
 * Structural recursion
 *
 *----------------------------------------------------------------------------*)

type id_t = M3.var_id_t
type coll_id_t = M3.map_id_t

(* Types
 * Collections:
 * -- define schemas, and implicitly refer to a value (e.g. they are mappings),
 *    that is we do not represent the value in the schema 
 *)
type type_t =
      Unit | Float | Int | TTuple of type_t list
    | Collection of type_t
    | Fn of type_t * type_t (* arg * body *)

(* External functions *)
(*
type ext_fn_type_t = type_t list * type_t   (* arg, ret type *)

type fn_id_t = string
type symbol_table = (fn_id_t, ext_fn_type_t) Hashtbl.t

type ext_fn_id = IndexAndProject of iap_meta | Symbol of fn_id_t

let ext_fn_symbols : symbol_table = Hashtbl.create 100
*)

(* Collections *)
type schema = (id_t * type_t) list

(* Expression AST *)
(* Notes:
 * -- we currently only support external collections, which are collections
 *    of records consisting of a set of named variables, and a single unnamed
 *    variable.
 * -- a singleton external collection is treated as a singleton of the
 *    unnamed variable.
 * -- we don't need tuple projection for M3 programs, but add it for
 *    user-defined structural recursion programs. Note projection applies to
 *    tuples only, collection projection is done through a map operation.
 * TODO: proper conditionals
 *)
type expr_t =
   
   (* Terminals *)
     Const         of M3.const_t
   | Var           of id_t        * type_t

   (* Tuples, i.e. unnamed records *)
   | Tuple         of expr_t list
   | Project       of expr_t      * int list

   (* Collection construction *)
   | Singleton     of expr_t
   | Combine       of expr_t      * expr_t 

   (* Arithmetic and comparison operators, conditionals *) 
   | Add           of expr_t      * expr_t
   | Mult          of expr_t      * expr_t
   | Eq            of expr_t      * expr_t
   | Neq           of expr_t      * expr_t
   | Lt            of expr_t      * expr_t
   | Leq           of expr_t      * expr_t
   | IfThenElse0   of expr_t      * expr_t

   (* Control flow: conditionals, sequences, side-effecting iterations *)
   | IfThenElse    of expr_t      * expr_t   * expr_t
   | Block         of expr_t list 
   | Iterate       of expr_t      * expr_t  
     
   (* Functions *)
   | Lambda        of id_t        * type_t   * expr_t    * bool
   | AssocLambda   of id_t        * type_t   * id_t      * type_t * expr_t
   | Apply         of expr_t      * expr_t

   (* Structural recursion operators *)
   | Map              of expr_t      * expr_t 
   | Flatten          of expr_t
   | Aggregate        of expr_t      * expr_t   * expr_t
   | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t

   (* Tuple collection accessors
    * -- we assume a call-by-value semantics, thus for persistent collections,
    *    the collections are repeatedly retrieved from the persistent store,
    *    and our code explicitly avoids this by passing them as arguments to
    *    functions.
    * -- these can operate on either native collections or persistent
    *    collections. It is the responsibility of the code generator to
    *    produce the correct implementation on the actual datatype representing
    *    the collection.
    *)
   | Member      of expr_t      * expr_t list  
   | Lookup      of expr_t      * expr_t list

   (* Additional notes:
    * -- code generator should instantiate the collection datastructure used
    *    during evaluation, whether they are lists or ValuationMaps.
    * -- code generator should strip any secondary indexes as needed. *)
   | Slice       of expr_t      * schema      * (id_t * expr_t) list
   

   (* Persistent collections
    * -- these are collections of tuples, with all but one named fields
    * -- map name, in var, out var schema, unnamed field type, init val expr *)

   (* Persistent collection types w.r.t in/out vars *)
   | SingletonPC   of coll_id_t   * type_t                  (* initially 0 *)
   | OutPC         of coll_id_t   * schema   * type_t       (* initially 0 *)
   | InPC          of coll_id_t   * schema   * type_t    * expr_t
   | PC            of coll_id_t   * schema   * schema    * type_t * expr_t

   (* map, key (optional, used for double-tiered), tier *)
   (* Notes:
    * -- map updates should create the persistent collection datastructure
    *    maintained in the database.
    * -- map updates should add any secondary indexes as needed *)
   | PCUpdate      of expr_t      * expr_t list * expr_t

   (* map, in key (optional), out key, value *)   
   | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 

   (*| External      of ext_fn_id*)


(* Helpers *)

(* TODO: types *)
(* Reverse vars to preserve bind order *)
let schema_to_expr e vars = List.fold_left (fun acc v ->
    Lambda(v,Float,acc,true)) e (List.rev vars)

let rec fold_leaves f acc e =
    let fold_op e_l = List.fold_left (fun a e -> fold_leaves f a e) acc e_l in
    begin match e with
      Const            c                    -> f e acc
    | Var              (id,t)               -> f e acc
    | Tuple            e_l                  -> fold_op e_l
    | Project          (ce, idx)            -> fold_leaves f acc ce
    | Singleton        ce                   -> f e (fold_leaves f acc ce)
    | Combine          (ce1,ce2)            -> fold_op [ce1; ce2]
    | Add              (ce1,ce2)            -> fold_op [ce1; ce2]
    | Mult             (ce1,ce2)            -> fold_op [ce1; ce2]
    | Eq               (ce1,ce2)            -> fold_op [ce1; ce2]
    | Neq              (ce1,ce2)            -> fold_op [ce1; ce2]
    | Lt               (ce1,ce2)            -> fold_op [ce1; ce2]
    | Leq              (ce1,ce2)            -> fold_op [ce1; ce2]
    | IfThenElse0      (ce1,ce2)            -> fold_op [ce1; ce2]
    | IfThenElse       (pe,te,ee)           -> fold_op [pe;te;ee]
    | Block            e_l                  -> fold_op e_l
    | Iterate          (fn_e, ce)           -> fold_op [fn_e; ce]
    | Lambda           (v,v_t,ce,_)         -> fold_leaves f acc ce
    | AssocLambda      (v1,v1_t,v2,v2_t,be) -> fold_leaves f acc be
    | Apply            (fn_e,arg_e)         -> fold_op [fn_e; arg_e]
    | Map              (fn_e,ce)            -> fold_op [fn_e; ce]
    | Flatten          ce                   -> fold_leaves f acc ce
    | Aggregate        (fn_e,i_e,ce)        -> fold_op [fn_e; i_e; ce]
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> fold_op [fn_e; i_e; ge; ce]
    | SingletonPC      (id,t)               -> f e acc
    | OutPC            (id,outs,t)          -> f e acc
    | InPC             (id,ins,t,ie)        -> f e acc
    | PC               (id,ins,outs,t,ie)   -> f e acc
    | Member           (me,ke)              -> fold_op (me::ke)  
    | Lookup           (me,ke)              -> fold_op (me::ke)
    | Slice            (me,sch,pat_ve)      -> fold_op (me::(List.map snd pat_ve))
    | PCUpdate         (me,ke,te)           -> fold_op ([me]@ke@[te])
    | PCValueUpdate    (me,ine,oute,ve)     -> fold_op ([me]@ine@oute@[ve])
    (*| External         efn_id               -> f e acc*)
    end
    
let find_vars v e =
    let aux e acc = match e with
        | Var (id,t) -> if v = id then (id,t)::acc else acc
        | Const c  -> acc
        | Singleton (ce) -> acc
        | SingletonPC _ -> acc
        | InPC(id,ins,t,ie) -> acc
        | OutPC(id,outs,t) -> (List.filter (fun (x,t) -> v=x) outs)@acc
        | PC(id,ins,outs,t,ie)  -> (List.filter (fun (x,t) -> v=x) outs)@acc
        (*| External efn_id -> acc*)
        | _ -> failwith "invalid leaf" 
    in fold_leaves aux [] e

(* Native collection constructors *)
let collection_of_list (l : expr_t list) =
    if l = [] then failwith "invalid list for construction" else
    List.fold_left (fun acc v -> Combine(acc,Singleton(v)))
        (Singleton(List.hd l)) (List.tl l)

let collection_of_float_list (l : float list) =
    if l = [] then failwith "invalid list for construction" else
    List.fold_left (fun acc v -> Combine(acc,Singleton(Const(CFloat(v)))))
        (Singleton(Const(CFloat(List.hd l)))) (List.tl l)


(*******************************************************************************
 * Construction from M3
*******************************************************************************)

let map_to_expr mapn ins outs init_expr =
    (* TODO: value types *)
    begin match ins, outs with
    | ([],[]) -> SingletonPC(mapn,Float)
    | ([], x) -> OutPC(mapn,outs,Float)
    | (x, []) -> InPC(mapn,ins,Float,init_expr)
    | (x,y)   -> PC(mapn,ins,outs,Float,init_expr)
    end

let map_value_update_expr map_expr ine_l oute_l ve =
    begin match map_expr with
    | SingletonPC _ -> PCValueUpdate(map_expr, [], [], ve)
    | OutPC _ -> PCValueUpdate(map_expr, [], oute_l, ve)
    | InPC _ -> PCValueUpdate(map_expr, ine_l, [], ve)
    | PC _ -> PCValueUpdate(map_expr, ine_l, oute_l, ve)
    | _ -> failwith "invalid map expression for value update" 
    end

let map_update_expr map_expr e_l te =
    begin match map_expr with
    | OutPC _ -> PCUpdate(map_expr, [], te)
    | InPC _ -> PCUpdate(map_expr, [], te)
    | PC _ -> PCUpdate(map_expr, e_l, te)
    | _ -> failwith "invalid map expression for update"
    end

let map_access_to_expr map_expr singleton init_singleton out_patv =
    let aux sch t init_expr =
        (* TODO: use typechecker to compute type *)
        let ke = List.map (fun (v,t) -> Var(v,t)) sch in 
        let map_t = Collection(TTuple((List.map snd sch)@[t])) in
        let map_v,map_var = "slice",Var("slice", map_t) in
        let access_expr = if singleton then Lookup(map_var,ke)
            else let p_ve = List.map (fun v -> 
                    let t = List.assoc v sch in (v,(Var(v,t)))) out_patv 
                 in Slice(map_var,sch,p_ve)
        in Lambda(map_v, map_t,
            IfThenElse(Member(map_var,ke), access_expr, init_expr), false)
    in
    let init_aux map_expr ins outs ie =
        let ine_l = List.map (fun (v,t) -> Var(v,t)) ins in
        let oute_l = List.map (fun (v,t) -> Var(v,t)) outs in
        let (iv_v, iv_t, iv_e) =
            (* TODO: use typechecker to compute type *)
            let t = if init_singleton then Float else
                begin match map_expr with
                | InPC _ -> Collection(TTuple((List.map snd ins)@[Float]))
                | OutPC  _| PC _ -> Collection(TTuple((List.map snd outs)@[Float]))
                | _ -> failwith "invalid map type for initial values"
                end
            in "init_val", t, Var("init_val", t)
        in
        (* Helper to update the db w/ a singleton init val *)
        let update_singleton_aux rv_f =
            let update_expr = 
                map_value_update_expr map_expr ine_l oute_l iv_e
            in Apply(Lambda(iv_v, iv_t, Block([update_expr; rv_f iv_e]),false), ie)
        in
        (* Helper to build an index on a init val slice, and update the db *)
        let index_slice_aux rv_f =
            (* Note: assume CG will do indexing as necessary for PCUpdates *)
            let update_expr = map_update_expr map_expr ine_l iv_e
            in Block([update_expr; rv_f iv_e])
        in
        if singleton && init_singleton then
            (* ivc eval + value update + value rv *)
            update_singleton_aux (fun x -> x)

        else if init_singleton then
            (* ivc eval + value update + slice construction + slice rv *)
            update_singleton_aux (fun iv_e ->
                begin match map_expr with
                | OutPC _ | PC _ -> Singleton(Tuple(oute_l@[iv_e])) 
                | _ -> failwith "invalid map type for slice lookup"
                end)

        else if singleton then
            (* ivc eval + slice update + value lookup rv *)
            let ke = match map_expr with
                | InPC _ -> ine_l | OutPC _ -> oute_l  | PC _ -> oute_l
                | _ -> failwith "invalid map type for initial values" in
            let lookup_expr_f rv_e = Lookup(rv_e, ke)
            in Apply(Lambda(iv_v, iv_t, index_slice_aux lookup_expr_f, false), ie)
        
        else
            (* ivc eval + slice update + slice rv *)
            Apply(Lambda(iv_v, iv_t, index_slice_aux (fun x -> x), false), ie)
    in
    begin match map_expr with
    | SingletonPC(id,t) -> map_expr
    | OutPC(id,outs,t) ->
        let init_expr = init_aux map_expr [] outs (Const(CFloat(0.0)))
        in Apply(aux outs t init_expr, map_expr)

    | InPC(id,ins,t,ie) ->
        let init_expr = init_aux map_expr ins [] ie
        in Apply(aux ins t init_expr, map_expr)

    | PC(id,ins,outs,t,ie) ->
        let init_expr = init_aux map_expr ins outs ie in
        let nested_t =
            (* TODO: use typechecker to compute type *)
            let ins_t = List.map snd ins in
            let outs_t = List.map snd outs
            in Collection(TTuple(ins_t@[Collection(TTuple(outs_t@[t]))]))
        in
        let in_el = List.map (fun (v,t) -> Var(v,t)) ins in
        let map_v,map_var = "m",Var("m", nested_t) in
        let out_access_fn = Apply(aux outs t init_expr, Lookup(map_var,in_el)) in
        let access_fn = Lambda(map_v, nested_t,
            IfThenElse(Member(map_var,in_el), out_access_fn, init_expr), false)
        in Apply(access_fn, map_expr)
        
    | _ -> failwith "invalid map for map access"
    end

let rec calc_to_singleton_expr calc : expr_t =
    let recur f c1 c2 =
        f (calc_to_singleton_expr c1) (calc_to_singleton_expr c2) in 
    begin match (M3P.get_calc calc) with
        | M3.Const(i)   -> Const(i)
        | M3.Var(x)     -> Var(x, Float) (* TODO: var type *)

        | M3.MapAccess(mapn, inv, outv, init_aggecalc) ->
            (* Note: no need for lambda construction since all in vars are
             * bound. *)
            (* TODO: schema+value types *)
            let s_l = List.map (List.map (fun v -> (v,Float))) [inv; outv] in
            let (ins,outs) = (List.hd s_l, List.hd (List.tl s_l)) in
            let singleton_init_code = 
               (M3P.get_singleton (M3P.get_ecalc init_aggecalc)) ||
               (M3P.get_full_agg (M3P.get_agg_meta init_aggecalc))
            in
            let ie = calc_to_expr (M3P.get_ecalc init_aggecalc) in
            let map_expr = map_to_expr mapn ins outs ie
            in map_access_to_expr map_expr true singleton_init_code []

        | M3.Add (c1, c2) -> recur (fun x y -> Add(x,y))  c1 c2 
        | M3.Mult(c1, c2) -> recur (fun x y -> Mult(x,y)) c1 c2
        | M3.Lt  (c1, c2) -> recur (fun x y -> Lt(x,y))   c1 c2
        | M3.Leq (c1, c2) -> recur (fun x y -> Leq(x,y))  c1 c2
        | M3.Eq  (c1, c2) -> recur (fun x y -> Eq(x,y))   c1 c2
        | M3.IfThenElse0(c1, c2) -> recur (fun x y -> IfThenElse0(x,y)) c1 c2
    end

and op_to_expr op c c1 c2 : expr_t =
    let aux ecalc = (calc_schema c, M3P.get_singleton c) in
    let (outv1, c1_sing) = aux c1 in
    let (outv2, c2_sing) = aux c2 in
    let (schema, c_sing, c_prod) =
        calc_schema c, M3P.get_singleton c, M3P.get_product c
    in match (c_sing, c1_sing, c2_sing) with
        | (true, false, _) | (true, _, false) | (false, true, true) ->
            failwith "invalid parent singleton"
        
        | (true, _, _) -> calc_to_singleton_expr c

        | (_, true, false) | (_, false, true) ->
            (* TODO: types *)
            let inline = calc_to_singleton_expr (if c1_sing then c1 else c2) in
            let (v,v_t,l,r,schema) =
                if c1_sing then "v2",Float,inline,Var("v2",Float),outv2
                else "v1",Float,Var("v1",Float),inline,outv1 in
            let fn = schema_to_expr (Lambda(v,v_t,op schema l r,false)) schema
            in Map(fn, calc_to_expr (if c1_sing then c2 else c1))
        
        | (_, false, false) ->
            (* TODO: types *)
            let (l,r) = (Var("v1",Float), Var("v2",Float)) in
            let nested =
                schema_to_expr (Lambda("v2",Float,op schema l r,false)) outv2 in
            let outer = schema_to_expr
                (Lambda("v1",Float,Map(nested, calc_to_expr c2),false)) outv1
            in Map(outer, calc_to_expr c1)

and calc_to_expr calc : expr_t =
    let tuple op schema c1 c2 =
        (* TODO: schema types *)
        Tuple((List.map (fun v -> (Var (v, Float))) schema)@[op c1 c2])
    in  
    begin match (M3P.get_calc calc) with
        | M3.Const(i)   -> Const(i)
        | M3.Var(x)     -> Var(x, Float) (* TODO: var type *)

        | M3.MapAccess(mapn, inv, outv, init_aggecalc) ->
            (* TODO: schema, value types *)
            let s_l = List.map (List.map (fun v -> (v,Float))) [inv; outv] in
            let (ins,outs) = (List.hd s_l, List.hd (List.tl s_l)) in
            let ie = calc_to_expr (M3P.get_ecalc init_aggecalc) in
            let map_expr = map_to_expr mapn ins outs ie in
            let singleton_init_code = 
               (M3P.get_singleton (M3P.get_ecalc init_aggecalc)) ||
               (M3P.get_full_agg (M3P.get_agg_meta init_aggecalc)) in
            let patv = M3P.get_extensions (M3P.get_ecalc init_aggecalc)
            in map_access_to_expr map_expr
                (M3P.get_singleton calc) singleton_init_code patv

        | M3.Add (c1, c2) -> op_to_expr (tuple (fun c1 c2 -> Add (c1,c2))) calc c1 c2 
        | M3.Mult(c1, c2) -> op_to_expr (tuple (fun c1 c2 -> Mult(c1,c2))) calc c1 c2
        | M3.Eq  (c1, c2) -> op_to_expr (tuple (fun c1 c2 -> Eq  (c1,c2))) calc c1 c2
        | M3.Lt  (c1, c2) -> op_to_expr (tuple (fun c1 c2 -> Lt  (c1,c2))) calc c1 c2
        | M3.Leq (c1, c2) -> op_to_expr (tuple (fun c1 c2 -> Leq (c1,c2))) calc c1 c2
        | M3.IfThenElse0(c1, c2) ->
            let short_circuit_if = M3P.get_short_circuit calc in
            if short_circuit_if then
                 op_to_expr (tuple (fun c1 c2 -> IfThenElse0(c1,c2))) calc c1 c2
            else op_to_expr (tuple (fun c2 c1 -> IfThenElse0(c1,c2))) calc c2 c1
    end 

(*******************************************************************************
 * Structural recursion methods
 *******************************************************************************)

let rec type_as_string t =
    match t with
      Unit -> "Unit" | Float -> "Float" | Int -> "Int"
    | TTuple(t_l) -> "Tuple("^(String.concat " ; " (List.map type_as_string t_l))^")"
    | Collection(c_t) -> "Collection("^(type_as_string c_t)^")"
    | Fn(a,b) -> "( "^(type_as_string a)^" -> "^(type_as_string b)^" )"

(* Typechecking:
 * -- value types are assumed to be floats
 * -- tuple projection is typed based on the arity of indexes requested, that is
 *    single index projections yield field itself, while multiple index
 *    projections yield a sub-tuple
 * -- singletons have no schema since all variables are bound, they are typed
 *    by their values
 * -- collections types are implicitly appended with their value type, that is:
 *      C([x,y]) : C([x,y,Float])
 * -- currying is suffix-based, e.g.
 *       ((fun x y z -> x+y+z) a b) = (fun x -> x+a+b)
 *)

(* TODO: Lifting
 * -- we should apply lifting once, before typechecking, rather than lifting for
 *    typechecking as needed.
 *
 * -- when is it safe to lift? collections of functions are generally not
 *    safe for function lifting, because functions define equivalence classes
 *    and it is the class being "indexed"/named/referenced as part of the
 *    collection record (i.e. functions are data).
 * 
 * Map lifting:
 * -- map(f : Fn(b,Fn(c,d)), Lambda(g, h : C([b,c])))
 *      = Lambda(g,map(f,h)) : Fn(g,d)
 *    here Lambda(g,h) can be lifted since maps only apply to collections,
 *    and there is a single function yielding the collection
 *
 * -- map(f : Fn(a,Fn(b,Fn(c,d))), g : C([b,c]))
 *      = Lambda(a,map(f' : Fn(b,Fn(c,d)), g : C([b,c]))) : Fn(a,d)
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *    here f can be partially applied to g, and lifted since f is common to
 *    all collection elements. Note that map uses suffix currying.
 *
 * -- map(f : Fn(a,Fn(b,Fn(c,d))), Lambda(g, h : C([b,c])))
 *      = Lambda(g,Lambda(a,map(f' : Fn(b,Fn(c,d)),h))) : Fn(g,Fn(a,d))
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *    Note lifting occurs right-to-left, that is the collection function is
 *    lifted before any partially evaluated map function. 
 *
 * Aggregation lifting:
 *
 * -- aggregate(f : Fn(b,Fn(c,Fn(d,d))), i:d, Lambda(g, h : C([b,c])))
 *      = Lambda(g,aggregate(f,i,h))
 *
 * -- aggregate(f: Fn(a,Fn(b,Fn(c,Fn(d,d)))), i:d, g : C([b,c]))
 *      = Lambda(a,aggregate(f',i,g))
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *
 * -- aggregate(f: Fn(a,Fn(b,Fn(c,Fn(d,d)))), i:d, Lambda(g, h : C([b,c])))
 *      = Lambda(g,Lambda(a,aggregate(f' : Fn(b,Fn(c,Fn(d,d))), i:d, h : C([b,c]))))
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *
 * Flatten lifting:
 *
 * -- flatten(Lambda(a,C(C(b)))) = Lambda(a,flatten(C(C(b))))
 *
 *
 * Unsafe:
 * -- implicit lifting from collections, e.g.
 *       FFn([w,x,y], Lambda(z,C([a,b]))) : C([w,x,y,Fn(z,C([a,b,Float]))])
 *       = Lambda(z,FFn([w,x,y,C([a,b])])) : Fn(z,C([w,x,y,C([a,b,Float])]))
 *
 * -- lifting left-to-right, recursively, e.g.:
 *      C([Fn(a,b),Fn(c,Fn(d,e)),Float]) = Fn(a, Fn(c, (Fn(d, C([b,e,Float])))))
 *
 *      C([Fn(a,b),Fn(Fn(c,C(d)),Fn(e,f)),Float]) =
 *          Fn(a, Fn(Fn(c,C(d)), Fn(e, C([b,f,Float]))))
 *
 * -- conditionals can be incorrectly lifted w.r.t types, e.g.
 *        Map( f : Fn(a,Fn(b,Fn(c,Fn(d,e))) , C([c,d]) )
 *          : Fn(a, b, Map(f: Fn(c,Fn(d,e)), ...))
 *      where f : Lambda(a, IfThenElse(F,
 *                  Lambda(b,Lambda(c,..)), Lambda(b,Lambda(c,...))))
 *    TODO:
 *    this is because we're typing based on what a lifting simplification would
 *    do, rather than actually applying this simplification, and simply typing
 *    each expression. We should *not* have the typechecker recreate the effect
 *    of optimizations. Conditions should not admit lambda lifting.
 *
 * DEPRECATED, we no longer have FFns.
 * Finite function lifting:
 * -- FFn([w,x], Lambda(y,y+3)) = Lambda(y,FFn([w,x],y+3))
 *    types: C([w,x,Fn(a,b)]) = Fn(a,C([w,x,b]))
 *    Finite functions use a single function for all records, enabling lifting 
 *
 *    ++ applies recursively:
 *    FFn([w,x], Lambda(y,Lambda(z,y+z+3))) = Lambda(y,Lambda(z,FFn([w,x],y+z+3)))
 *    types: C([w,x,Fn(a,Fn(b,c))]) = Fn(a,Fn(b,C([w,x,c])))
 *
 *    ++ holds for higher-order:
 *    FFn([w,x], Lambda(y, Apply y 3)) = Lambda(y,FFn([w,x], Apply y 3))
 *    types: C([w,x,Fn(Fn(a,b),c)]) = Fn(Fn(a,b),C([w,x,c]))
 *
 *)
let rec typecheck_expr e : type_t =
    let recur = typecheck_expr in

    (* General helpers *)
    let sub_list start fin l = snd (List.fold_left (fun (cnt,acc) e ->
        if cnt < start || cnt >= fin
        then (cnt+1,acc) else (cnt+1,acc@[e])) (0,[]) l) in
        
    let back l = let x = List.rev l in (List.hd x, List.rev (List.tl x)) in

    let type_as_schema t =
        match t with TTuple(t_l) -> t_l | _ -> [t] in

    (* Type flattening helpers *)
    let is_flat t = match t with | Float | Int -> true | _ -> false in
    let flat t = if is_flat t then t else failwith "invalid flat expression" in
    let promote t1 t2 = match t1,t2 with
        | (Float, Int) | (Int, Float) -> Float
        | (Int, Int) -> Int
        | (x,y) -> if x = y then x else failwith "could not promote types"
    in
    (* returns t2 if it can safely be considered t1 (i.e. t1 < t2 in the type
     * lattice) otherwise fails *)
    let valid_promote t1 t2 = if (promote t1 t2) <> t1
        then failwith "potential loss of precision" else t2 in 
    let unify_types l = List.fold_left promote (List.hd l) (List.tl l) in 

    (* Type checking primitives *)
    let tc_schema sch =
        let t_l = List.map snd sch in
        let invalid = List.mem Unit t_l in
        if invalid then failwith "invalid schema" else t_l
    in 

    let tc_schema_exprs sch e_l =
        let sch_t = tc_schema sch in
        if List.for_all (fun x->x)
            (List.map2 (fun t (_,t2) -> (promote t t2) = t2)
                (List.map recur e_l) sch)
        then sch_t
        else failwith "invalid expression list schema"
    in

    let tc_op ce1 ce2 =
        match recur ce1, recur ce2 with
        | (Unit,_) | (_,Unit) -> failwith "invalid operands"
        | (a,b) -> promote (flat a) (flat b)
    in

    (* performs suffix fn app validation, and suffix currying *)
    let tc_schema_app coll_fn fn_t sch_t : type_t =
        (* fn_t: [a,b,c,d,e,f,g,h] sch_t: [d,e,f,g] *)
        let fin = (List.length fn_t)-1 in
        let start = fin - (List.length sch_t) in
        let sub_fn_t = sub_list start fin fn_t in
        let ret_t = List.nth fn_t fin in
            if sub_fn_t = sch_t then
                let ret_fn_a_t = sub_list 0 start fn_t in
                List.fold_left (fun acc_t t ->
                    Fn(t,acc_t)) (coll_fn ret_t) ret_fn_a_t
            else failwith "invalid schema function application"
    in

    (* Lambda helpers *)
    let tc_fnarg v v_t e =
        let vars = find_vars v e in
        if vars = [] then v_t
        else let found_t = unify_types (List.map snd vars) in
        try valid_promote found_t v_t
        with Failure _ ->
            failwith "inconsistent function argument and usage types" 
    in

    (* Multivariate function helpers *)
    (* flattens the arg types of a multivariate function, i.e.
     *    linearize(Fn(a,Fn(b,Fn(c,Fn(d,e))))) = [a,b,c,d,e] *)
    let rec linearize_mvf e : type_t list =
        match e with | Fn(a,b) -> a::(linearize_mvf b) | _ -> [e] in

    (* reconstructs a multivariate function from its linearization *)
    let rebuild_mvf rt tl : type_t =
        List.fold_left (fun acc_t t -> Fn(t,acc_t)) rt (List.rev tl)
    in

    (* Map/Aggregate helpers *)
    let apply_suffix coll_fn fn_tl t : type_t = match t with
        | Collection(c_t) -> tc_schema_app coll_fn fn_tl (type_as_schema c_t)
        | _ -> tc_schema_app coll_fn fn_tl [t]
    in

    let lift_fn f t =
        let (ret_t, rest_t) = back (linearize_mvf t) in f ret_t rest_t
    in
    let lift_schema_app coll_fn fn_tl fn_c_t =
        (* apply suffix currying for fn_t, ret_t,
         * rebuilding mvf with prefix of c_t and fn_t *)
        lift_fn (fun ret_t rest_t ->
            rebuild_mvf (apply_suffix (coll_fn ret_t) fn_tl ret_t) rest_t) fn_c_t
    in
    
    (* Note: the accumulator arg appears as the last argument, i.e. similar to
     * OCaml's List.fold_right/
     * Lifting: same as map, except for additional checks against initial
     * aggregate
     * -- suffix currying
     * -- implicitly lifts multivariate collection functions and
     *    partial agg function application *)
    let tc_agg fn_e i_e ce gb_tc_f =
        let (fn_t,i_t,c_t) = (recur fn_e, recur i_e, recur ce) in
        let agg_fn_tl = linearize_mvf fn_t in
        if List.length agg_fn_tl < 3 then failwith "invalid aggregate function"
        else
        (* For aggregate functions, we can ignore the accumulator argument,
         * this will always be bound by the initial value within the aggregation.
         * Thus we drop the last type for the function we're lifting (this is
         * the same as dropping the accumulator type, which must be the same
         * as the return type).
         *)
        let agg_ret_t, acc_t, fn_tl =
            let x,y = back agg_fn_tl in
            let y_l = (List.length y)-1 in (x, List.nth y y_l, y)
        in
        let valid_agg_fn = List.for_all (fun (x,y) -> x=y)
            [acc_t,agg_ret_t; acc_t,i_t; agg_ret_t,i_t]
        in
        if not(valid_agg_fn) then
            failwith "invalid accumulator/agg function return/initial value"
        else
            begin match c_t with
            | Unit -> failwith "invalid aggregate collection"
            | Fn _ -> lift_schema_app
                (fun c_ret_t fn_ret_t -> gb_tc_f c_ret_t fn_ret_t) fn_tl c_t
            | _ -> gb_tc_f c_t (apply_suffix (fun t -> t) fn_tl c_t)
            end
    in

    (* Persistent collection operation helpers *)
    let tc_pcop f me ke =
        let (m_t, k_t) = (recur me, List.map recur ke) in
        let m_f ret_t rest_t =
            match ret_t with
            | Collection(TTuple(kv_tl)) ->
                let (mv_t, t_l) = back kv_tl in
                begin try ignore(List.map2 valid_promote t_l k_t);
                          rebuild_mvf (f mv_t kv_tl) rest_t
                      with Failure _ -> failwith "invalid map op elements/key"
                end
            | _ -> failwith "invalid map op"
        in lift_fn m_f m_t
    in

    match e with
      Const        c ->
        begin match c with | CFloat _ -> Float (*| CInt _ -> Int *) end
    | Var          (id,t)     -> t
    | Tuple         e_l       ->
        let t_l = List.map recur e_l in
        let invalid = List.mem Unit t_l in
        if invalid then failwith "invalid tuple value" else TTuple(t_l)

    | Project      (ce, idx)  ->
        let c_t = recur ce in
        begin match c_t with
        | TTuple(t_l) ->
            let len = List.length t_l in
            let valid = List.for_all ((>) len) idx in
            if valid then
                let ret_tl = List.map (List.nth t_l) idx in 
                if (List.length idx) = 1 then List.hd ret_tl else TTuple(ret_tl)
            else failwith "invalid tuple projection index"
        | _ -> failwith "invalid tuple projection"
        end

    | Singleton       (ce)      -> Collection(
        match recur ce with Unit -> failwith "invalid singleton" | x -> x)

    | Combine         (ce1,ce2) ->
        let ce1_t, ce2_t = (recur ce1, recur ce2) in
        begin match ce1_t,ce2_t with
        | (Collection(c1_t), Collection(c2_t)) ->
            if (c1_t <> Unit && c2_t <> Unit) && (c1_t = c2_t) then ce1_t
            else failwith "invalid collections for combine"
        | _ -> failwith "invalid combine of non-collections" 
        end

    | Add          (ce1,ce2) -> tc_op ce1 ce2
    | Mult         (ce1,ce2) -> tc_op ce1 ce2
    | Eq           (ce1,ce2) -> tc_op ce1 ce2
    | Neq          (ce1,ce2) -> tc_op ce1 ce2
    | Lt           (ce1,ce2) -> tc_op ce1 ce2
    | Leq          (ce1,ce2) -> tc_op ce1 ce2
    | IfThenElse0  (ce1,ce2) ->
        (* Check ce1, ce2 are compatible singletons, return 'then' type *)
        ignore(tc_op ce1 ce2); recur ce2

    (* TODO: it is unsafe to lift functions above a condition,
     * how do we ensure expressions above this condition don't do it?
     * -- I don't think we can... *)
    | IfThenElse   (pe, te, ee) ->
        let (p_t, t_t, e_t) = (recur pe, recur te, recur ee) in
        if is_flat p_t then
            try promote t_t e_t with Failure _ ->
            failwith "mismatch branch types for condition"
        else failwith "invalid condition predicate"

    | Block        (e_l) ->
        let t_l = List.map recur e_l in List.nth t_l ((List.length t_l)-1)  

    | Iterate      (fn_e, ce) ->
        (* TODO: almost the same as map typechecking... lift *)
        let (fn_t, c_t) = recur fn_e, recur ce in
        let fn_tl = linearize_mvf fn_t in
        let coll_fn c_ret_t fn_ret_t = match c_ret_t, fn_ret_t with
          | (Unit, _) -> failwith "invalid iterate collection"
          | (Collection _, Unit) -> Unit
          | (_,_) -> failwith "invalid iteration"
        in
        if List.length fn_tl < 2 then failwith "invalid iterate function"
        else
            begin match c_t with
            | Unit -> failwith "invalid iterate collection"
            | Fn _ -> lift_schema_app coll_fn fn_tl c_t
            | _ -> apply_suffix (coll_fn c_t) fn_tl c_t
            end
        

    | Lambda       (v,v_t,ce,_)    ->
        (* assumes v : Int if v is not used in ce *)
        (* TODO: type inference for v *)
        Fn(tc_fnarg v v_t ce, recur ce)
    
    | AssocLambda  (v1,v1_t,v2,v2_t,be) ->
        (* assumes v1,v2 : Int if v1/v2 is not used in be *)
        (* TODO: type inference for v1, v2 *)
        Fn(tc_fnarg v1 v1_t be, Fn(tc_fnarg v2 v2_t be, recur be))
    
    | Apply        (fn_e,arg_e) ->
        let arg_t = recur arg_e in
        let fn_t = recur fn_e in
        begin match fn_t with
            | Fn(expected, ret) when arg_t = expected -> ret
            | _ -> failwith "invalid function application"
        end


    (* Note: we can support side-effecting iteration by allowing the
     * map function to return unit.
     * This yields unit, and not a collection type. *)
    | Map          (fn_e,ce) ->
        (* -- suffix currying, e.g.:
         *    A(A(L(x,L(y,L(z, x+y+z))), a), b) = L(x,x+a+b)
         *    ++ this is needed for aggregate simplification.
         * -- implicitly lifts multivariate collection functions and partial
         *    map function application *)
        let (fn_t, c_t) = (recur fn_e, recur ce) in
        let fn_tl = linearize_mvf fn_t in
        let coll_fn c_ret_t fn_ret_t = match c_ret_t, fn_ret_t with
            | (Unit,_) -> failwith "invalid map collection type"
            | (_,Unit) -> Unit
            | (Collection(_), _) -> Collection(fn_ret_t)
            | (_,_) -> fn_ret_t
        in
        if List.length fn_tl < 2 then failwith "invalid map function"
        else
	        begin match c_t with
            | Unit -> failwith "invalid map collection"
	        | Fn _ -> lift_schema_app coll_fn fn_tl c_t
	        | _ -> apply_suffix (coll_fn c_t) fn_tl c_t
	        end

    | Aggregate    (fn_e,i_e,ce) ->
        tc_agg fn_e i_e ce (fun c_ret_t agg_t ->
            match c_ret_t, agg_t with
            | (Unit,_) -> failwith "invalid aggregate collection"
            | (_,Unit) -> failwith "invalid aggregate type"
            | (_,_) -> agg_t)

    | GroupByAggregate (fn_e, i_e, ge, ce) ->
        let g_t, c_t = (recur ge, recur ce) in
        let g_tl = linearize_mvf g_t in
        let gb_tc_f c_ret_t agg_t =
            let g_ret_t = List.nth g_tl ((List.length g_tl)-1) in
            if c_ret_t = Unit then failwith "invalid aggregate collection"
            else
            begin match (apply_suffix (fun t -> t) g_tl c_ret_t, g_ret_t) with
            | (_, Unit) -> failwith "invalid group by value"
            | (Fn _,_) -> failwith "invalid group by fn application"
            | (_,TTuple(t_l)) -> Collection(TTuple(t_l@[agg_t]))
            | (_,_) -> Collection(TTuple([g_ret_t;agg_t]))
            end
        in tc_agg fn_e i_e ce gb_tc_f 

    | Flatten      ce ->
        let c_t = recur ce in
        let tc_aux f t = match t with Fn (_,_) -> lift_fn f t | _ -> f t [] in 
        let l1 t rest_t = match t with
            | Collection(Collection(x)) -> rebuild_mvf (Collection(x)) rest_t 
            | _ -> failwith "invalid flatten"
        in tc_aux l1 c_t
    
    (* Persistent collections define records ,i.e. variables and their types,
     * and may contain nested collections, e.g. C(w:int,x:float,y:C(z:float))
     * -- no lifting, in general this is not safe here *)

    | SingletonPC   (id,t) ->
        if t = Unit then failwith "invalid singleton PC" else t
    
    | OutPC         (id,outs,t) -> Collection(TTuple((tc_schema outs)@[t]))
    | InPC          (id,ins,t,ie) -> Collection(TTuple((tc_schema ins)@[t]))
    | PC            (id,ins,outs,t,ie) ->
        let ins_t = tc_schema ins in
        let outs_t = tc_schema outs
        in Collection(TTuple(ins_t@[Collection(TTuple(outs_t@[t]))]))

    | Member    (me, ke) -> tc_pcop (fun _ _ -> Float) me ke 
    | Lookup    (me, ke) -> tc_pcop (fun mv_t _ -> mv_t) me ke

    | Slice     (me, sch, pat_ve) ->
        let valid_pattern = List.for_all (fun (v,e) ->
            List.mem_assoc v sch &&
            (let t = List.assoc v sch in (promote t (recur e)) = t)) pat_ve in
        if valid_pattern
        then let ke = List.map (fun (v,t) -> Var(v,t)) sch
             in tc_pcop (fun mv_t kv_tl -> Collection(TTuple(kv_tl))) me ke
        else failwith "inconsistent schema,pattern for map slice" 

    | PCUpdate     (me,ke,te)           ->
        begin match me,ke with
	    | (OutPC _,[]) | (InPC _,[]) ->
            if (recur me) = (recur te) then Unit
            else failwith "invalid map update tier type"

	    | (PC(id,ins,outs,t,ie),_) ->
            let (m_t, t_t) = (recur me, recur te) in
            let valid_tier = match m_t with
                | Collection(TTuple(kv_tl)) -> (fst (back kv_tl)) = t_t
                | _ -> failwith "internal error, expected collection type for map"
            in
            if valid_tier then
                try ignore(tc_schema_exprs ins ke); Unit
                with Failure x -> failwith ("map update key: "^x) 
            else failwith "invalid tier for map update"

        | _ -> failwith "invalid target for map update"
        end

    | PCValueUpdate(me,ine,oute,ve)     ->
        let aux sch v_t ke ve =
            if (promote v_t (recur ve)) = v_t then
	            try tc_schema_exprs sch ke
	            with Failure x -> failwith ("map value update: "^x)
            else failwith "map value update: invalid update value expression"
        in 
        begin match me with
        | SingletonPC(id,t) -> Unit
        | OutPC (id,outs,t) -> ignore(aux outs t oute ve); Unit 
        | InPC (id,ins,t,_) -> ignore(aux ins t ine ve); Unit
        | PC(id,ins,outs,t,_) ->
            ignore(aux ins t ine ve); ignore(aux outs t oute ve); Unit
        | _ -> failwith "invalid target for map value update"
        end

    (*| External     efn_id ->
        begin match efn_id with
        | Symbol(id) ->
            begin try let (arg_t, ret_t) = Hashtbl.find ext_fn_symbols id
                      in rebuild_mvf ret_t arg_t
                  with Not_found -> failwith "invalid external function"
            end 
        | _ -> failwith "externals not yet supported"
        end
    *)

(* TODO:
 * -- lifting simplifications
 * -- cross product composition
 * -- structural recursion simplifications
 *)


(* Incremental evaluation types *)
 
(* Top-level statement.
 * computes and applies (i.e. persists) increments for a map.
 *
 * collection, increment statement
 *)
type statement = expr_t * expr_t

(* Trigger functions *) 
type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list

type program = M3.map_type_t list * M3Common.Patterns.pattern_map * trigger list

let m3rhs_to_expr lhs_outv paggcalc : expr_t =
   (* invokes calc_to_structure on calculus part of paggcalc.
    * based on aggregate metadata:
    * -- simply uses the collection
    * -- applies bigsums to collection.
    *    we want to apply structural recursion optimizations in this case.
    * -- projects to lhs vars 
    *)
    let ecalc = M3P.get_ecalc paggcalc in
    let rhs_outv = calc_schema ecalc in
    
    (* TODO: simplify rhs, e.g. lift lambdas *)
    let rhs_expr = calc_to_expr ecalc in
    let init_val = Const(CFloat(0.0)) in
    let agg_fn = AssocLambda("v1", Float, "v2", Float,
                    Add(Var("v1", Float), Var("v2", Float))) in
    let gb_fn =
        let lhs_tuple = Tuple(List.map (fun v -> Var(v,Float)) lhs_outv) in
        schema_to_expr (Lambda("v",Float, lhs_tuple,false)) rhs_outv
    in
    if (M3P.get_singleton ecalc) || (rhs_outv = lhs_outv) then rhs_expr
    else if M3P.get_full_agg (M3P.get_agg_meta paggcalc) then
        (* TODO: simplify aggregate w/ struct rec *)
        Aggregate (agg_fn, init_val, rhs_expr) 
    else
        (* projection to lhs vars + aggregation *)
        GroupByAggregate(agg_fn, init_val, gb_fn, rhs_expr)

(* Statement expression construction:
 * -- creates structural representations of the the incr and init value exprs,
 *    composes them in an update expression, merging in the case of delta slices.
 * -- invokes the update expression w/ existing values/slices, binding+looping
 *    persistent collection input vars, and applying updates.
*)
let collection_stmt trig_args m3stmt : statement =
    (* Helpers *)
    let schema vars = List.map (fun v -> (v,Float)) vars in
    let vars_expr vars = List.map (fun v -> Var(v,Float)) vars in
    let fn_arg_expr v t = v,t,Var(v,t) in

    let ((mapn, lhs_inv, lhs_outv, init_aggcalc), incr_aggcalc, sm) = m3stmt in
	let ((incr_expr, incr_single),(init_expr,init_single)) =
        let expr_l = List.map (fun aggcalc ->
            let calc = M3P.get_ecalc aggcalc in
            let s = M3P.get_singleton calc ||
                    (M3P.get_full_agg (M3P.get_agg_meta aggcalc))
            in
            (* Note: we can only use calc_to_singleton_expr if there are
             * no loop or bigsum vars in calc *) 
            ((if M3P.get_singleton calc then calc_to_singleton_expr calc
              else m3rhs_to_expr lhs_outv aggcalc),s))
            [incr_aggcalc; init_aggcalc]
        in (List.hd expr_l, List.hd (List.tl expr_l))
    in
    
    let (ins,outs) = (schema lhs_inv, schema lhs_outv) in
    let (in_el, out_el) = (vars_expr lhs_inv, vars_expr lhs_outv) in 

    (* TODO: use typechecker to compute types here *)
    let out_tier_t = Collection(TTuple(List.map snd outs@[Float])) in

    let collection = map_to_expr mapn ins outs init_expr in

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
            let cv,ct,ce = fn_arg_expr "current_v" Float in
            (Lambda(cv,ct,Add(ce,incr_expr),false), init_e)
        in
        let slice_t = Collection(TTuple((List.map snd outs)@[Float])) in
        let slice_aux init_f =
            let cv,ct,ce = fn_arg_expr "current_slice" slice_t in
            let mv,mt,me = fn_arg_expr "dv" Float in
            (* merge fn: check if the delta exists in the current slice and
             * increment, otherwise compute an initial value and increment  *)
            let merge_body =
                IfThenElse(Member(ce,out_el),
                    Add(Lookup(ce, out_el), me), (init_f me)) in
            let merge_fn = schema_to_expr (Lambda(mv,mt,merge_body,false)) lhs_outv in
            (* slice update: merge current slice with delta slice *)
            let merge_expr = Map(merge_fn, incr_expr)
            in (Lambda(cv,ct,merge_expr,false), zero_init)
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

    let loop_in_aux (lv,lt,le) loop_fn_body =
        let patv = Util.ListAsSet.inter lhs_inv trig_args in
        let pat_ve = List.map (fun v -> (v,Var(v,Float))) patv in
        let in_coll = if (List.length patv) = (List.length lhs_inv)
            then Singleton(Lookup(collection, List.map snd pat_ve))
            else Slice(collection, ins, pat_ve) in
        let loop_fn = schema_to_expr (Lambda(lv,lt,loop_fn_body,false)) lhs_inv
        in Iterate(loop_fn, in_coll)
    in
    let loop_update_aux ins outs delta_slice =
        let uv,ut,ue = fn_arg_expr "updated_v" Float in
        let update_body = map_value_update_expr collection ins outs ue in
        let loop_update_fn = schema_to_expr (Lambda(uv,ut,update_body,false)) lhs_outv
        in Iterate(loop_update_fn, delta_slice)
    in
    let statement_expr =
        begin match lhs_inv, lhs_outv, incr_single with
        | ([],[],false) -> failwith "invalid slice update on a singleton map"

        | ([],[],_) ->
            let rhs_expr = (Apply(update_expr, collection))
            in map_value_update_expr collection [] [] rhs_expr

        | (x,[],false) -> failwith "invalid slice update on a singleton out tier"

        | (x,[],_) ->
            let lv,lt,le = fn_arg_expr "existing_v" Float in
            let rhs_expr = Apply(update_expr, le) 
            in loop_in_aux (lv,lt,le)
                (map_value_update_expr collection in_el [] rhs_expr) 

        | ([],x,false) ->
            (* We explicitly loop, updating each incremented value in the rhs slice,
             * since the rhs_expr is only the delta slice and not a full merge with
             * the current slice.
             *)
            let rhs_expr = Apply(update_expr, collection)
            in loop_update_aux [] out_el rhs_expr

        | ([],x,_) ->
            let rhs_expr = 
                IfThenElse(Member(collection, out_el),
                Apply(update_expr, Lookup(collection, out_el)), sing_init_expr)
            in map_value_update_expr collection [] out_el rhs_expr

        | (x,y,false) ->
            (* Use a value update loop to persist the delta slice *)
            let lv,lt,le = fn_arg_expr "existing_slice" out_tier_t in
            let rhs_expr = Apply(update_expr, le) in
            let update_body = loop_update_aux in_el out_el rhs_expr in
            loop_in_aux (lv,lt,le) update_body

        | (x,y,_) ->
            let lv,lt,le = fn_arg_expr "existing_slice" out_tier_t in
            let rhs_expr = IfThenElse(Member(le, out_el),
                Apply(update_expr, Lookup(le, out_el)), sing_init_expr)
            in loop_in_aux (lv,lt,le)
                (map_value_update_expr collection in_el out_el rhs_expr)
        end
    in
    (collection, statement_expr)


let collection_trig m3trig : trigger =
    let (evt, rel, args, stmts) = m3trig
    in (evt, rel, args, List.map (collection_stmt args) stmts)

let collection_prog m3prog patterns : program =
    let (schema, triggers) = m3prog
    in (schema, patterns, List.map collection_trig triggers)

end
