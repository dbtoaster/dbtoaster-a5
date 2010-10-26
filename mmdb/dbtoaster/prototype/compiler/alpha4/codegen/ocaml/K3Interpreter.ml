open Util
open M3
open Values
open Database
open Sources
open Sources.Adaptors


(*
module CG : K3Codegen.CG with type db_t = NamedK3Database.db_t =
struct
    open K3Value

    module Env = K3Valuation
    module MC  = K3ValuationMap
    module DB  = NamedK3Database

    type value_t = K3Value.t
    type env_t   = K3Valuation.t
    type db_t    = DB.db_t

    type code_t  = env_t -> db_t -> value_t 
    type op_t    = value_t -> value_t -> value_t

    (* Helpers *)
    let back l = let x = List.rev l in (List.hd x, List.rev (List.tl x))

    let rec apply_list fv l =
        match fv,l with
        | (Fun _, []) -> fv
        | (v, []) -> v
        | (Fun (f,_), h::t) -> apply_list (f h) t
        | (_,_) -> failwith "invalid schema application"

    (* Map/aggregate tuple+multivariate function evaluation helpers.
     * uses fields of a tuple to evaluate a multivariate function if the
     * function is a schema application.
     *)
    
    (* returns the value yielded by the mvf. *)
    let apply_mv_map_fn_lc fn v =
        match fn, v with
        | Fun (f,true), Tuple(t_v) -> apply_list fn t_v
        | Fun (f,false), _ -> f v
        | _ -> failwith "invalid map function" 

    (* returns the a k-v pair corresponding to a MapCollection entry *)
    let apply_mv_map_fn_mc fn k v =
        let aux rv = match rv with
            | Tuple(t_v) -> let nv,nk = back t_v in (nk,nv)
            | _ -> failwith "invalid map collection entry"
        in
        match fn with
        | Fun (f,true) -> aux (apply_list fn (k@[v]))
        | Fun (f,false) -> aux (f (Tuple(k@[v])))
        | _ -> failwith "invalid map function"

    (* assumes the accumulator is the last argument, preceded by the tuple.
     * returns the value yielded by the mvf. *)
    let apply_mv_agg_fn_lc fn acc v =
        match fn, v with
        | Fun (f,true), Tuple(t_v) -> apply_list fn (t_v@[acc])
        | Fun (f,false), _ -> apply_list fn [v;acc]
        | _,_ -> failwith "invalid aggregation function"
    
    (* assumes the accumulator is the last argument, preceded by the tuple.
     * returns the value yielded by the mvf. *)
    let apply_mv_agg_fn_mc fn k v acc =
        match fn with
        | Fun (f,true) -> apply_list fn (k@[v;acc])
        | Fun (f,false) -> apply_list fn [Tuple(k@[v]);acc]
        | _ -> failwith "invalid aggregation function"


    let apply_fn_list th db l = List.map (fun f -> f th db) l
    
    let mc_to_lc m = MC.fold (fun k v acc -> (Tuple(k@[v]))::acc) [] m

    let match_kv_tuple k_v t_v =
        try fst (List.fold_left (fun (run,rem) v -> match run, rem with
            | (true,[]) -> (run, rem) (* could terminate early on success *)
            | (false, _) -> raise Not_found (* early termination for failure *)
            | (_,_) -> (run && (List.hd rem) = v, List.tl rem))
            (true,k_v) t_v)
        with Not_found -> false

    let tuple_fields t_v idx = snd (List.fold_left (fun (cur,acc) v ->
        (cur+1, if List.mem cur idx then acc@[v] else acc)) (0,[]) t_v)

    (* Operators *)
    let promote_op f_op i_op x y =
        match x,y with
        | (Float f, Int i) -> Float(f_op f (float_of_int i))
        | (Int i, Float f) -> Float(f_op (float_of_int i) f)
        | (Float f1, Float f2) -> Float(f_op f1 f2)
        | (Int i1, Int i2) -> Int(i_op i1 i2)
        | (_,_) -> failwith "invalid arithmetic operands" 
	
    (* Comparisons:
     * -- supports base types, tuples and lists. Tuple and list comparison
     *    works as with standard OCaml semantics (structural comparisons) *)
    (* TODO:
     * -- collections have list semantics here, e.g. element order matters.
     *    Generalize to support set/bag semantics as needed. *)
    let int_op op x y =
        match (x,y) with
        | Unit,_ | _,Unit -> failwith "invalid comparison to unit"
        | _ -> if (op x y) then Int(1) else Int(0)
	
	let add_op  = promote_op (+.) (+)
	let mult_op = promote_op ( *. ) ( * )
	let lt_op   = (int_op (< ))
	let leq_op  = (int_op (<=))
	let eq_op   = (int_op (= ))
	
	let ifthenelse0_op cond v = match cond with
        | Int(bcond) ->
            if bcond <> 0 then v
            else (match v with Int _ -> Int(0) | Float _ -> Float(0.0)
                    | _ -> failwith "invalid then clause value")
        | _ -> failwith "invalid predicate value"


    (* Terminals *)
    let const k = fun th db -> match k with CFloat(x) -> Float(x)
    let var v = fun th db -> 
        if (Env.bound v th) then Env.value v th
        else failwith ("Var("^v^"): theta="^(Env.to_string th))

    (* Tuples *)
    let tuple field_l = fun th db -> Tuple(apply_fn_list th db field_l)
    let project tuple idx = fun th db -> match tuple th db with
        | Tuple(t_v) -> Tuple(tuple_fields t_v idx)
        | _ -> failwith "invalid tuple for projection"

    (* Native collections *)
    let singleton el = fun th db -> ListCollection([el th db])
    
    (* combines two collections.
     * -- promotes map collections to list collections *)
    let combine c1 c2 = fun th db ->
        begin match (c1 th db, c2 th db) with
        | (ListCollection(c1), ListCollection(c2)) -> ListCollection(c1@c2)
        | ListCollection l, MapCollection m -> ListCollection(l@(mc_to_lc m))
        | MapCollection m, ListCollection l -> ListCollection((mc_to_lc m)@l)
        | MapCollection m1, MapCollection m2 -> MapCollection(MC.union m1 m2)
        | _,_ -> failwith "invalid collections to combine"
        end

    (* Arithmetic, comparision operators *)
    (* op type, lhs, rhs -> op *)
    let op op_fn l r = fun th db -> op_fn (l th db) (r th db)

    (* Control flow *)
    (* predicate, then clause, else clause -> condition *)
    (* TODO: support empty and singleton as true/false values *) 
    let ifthenelse pred t e = fun th db ->
        let valid = match (pred th db) with
	        | Float x -> x <> 0.0 | Int x -> x <> 0
	        | _ -> failwith "invalid predicate value"
        in if valid then t th db else e th db

    (* statements -> block *)    
    let block stmts =
        let idx = List.length stmts - 1 in
        (fun th db -> let rvs = apply_fn_list th db stmts in List.nth rvs idx)
 
    (* Functions *)
    (* arg, schema application, body -> fn *)
    let lambda v schema_app body = fun th db ->
        let fn value = body (Env.add th v value) db
        in Fun(fn, schema_app)
    
    (* arg1, type, arg2, type, body -> assoc fn *)
    let assoc_lambda v1 v2 body = fun th db ->
        let fn1 vl1 = Fun((fun vl2 ->
            body (Env.add (Env.add th v1 vl1) v2 vl2) db), false)
        in Fun(fn1, false)

    (* fn, arg -> evaluated fn *)
    let apply fn arg = fun th db -> match fn th db with
        | Fun (f,false) -> f (arg th db)
        | _ -> failwith "invalid function for function application"
    
    (* Structural recursion operations *)
    (* map fn, collection -> map *)
    let map map_fn collection = fun th db ->
        match map_fn th db, collection th db with
        | (Fun (f,sa), ListCollection l) ->
            ListCollection(List.map (apply_mv_map_fn_lc (Fun(f,sa))) l)
        | (Fun (f,sa), MapCollection m) ->
            MapCollection(MC.mapi (apply_mv_map_fn_mc (Fun (f,sa))) m)
        | (_,_) -> failwith "invalid map function/collection"
    
    (* agg fn, initial agg, collection -> agg *)
    (* Note: accumulator is the last arg to agg_fn *)
    let aggregate agg_fn init_agg collection = fun th db ->
        match agg_fn th db, collection th db with
        | (Fun (f,sa), ListCollection l) ->
            List.fold_left (apply_mv_agg_fn_lc (Fun(f,sa))) (init_agg th db) l
        | (Fun (f,sa), MapCollection m) ->
            MC.fold (apply_mv_agg_fn_mc (Fun (f,sa))) (init_agg th db) m
        | (_,_) -> failwith "invalid agg function/collection"
    
    (* agg fn, initial agg, grouping fn, collection -> agg *)
    (* Perform group-by aggregation by using a temporary MapCollection,
     * which currently is a hash table, thus we have hash-based aggregation. *)
    let group_by_aggregate agg_fn init_agg gb_fn collection =
        let gb init_v gb_acc agg_f gb_key =
            let gb_agg = if MC.mem gb_key gb_acc
                         then MC.find gb_key gb_acc else init_v in
            let new_gb_agg = agg_f gb_agg
            in MC.add gb_key new_gb_agg gb_acc
        in
        let lc_gb init_v f g mc v =
            let gb_key = match g v with Tuple(t_v) -> t_v | x -> [x]
            in gb init_v mc (fun a -> f a v) gb_key
        in
        let mc_gb init_v f g k v mc =
            let gb_key = let x,y = (g k v) in x@[y]
            in gb init_v mc (f k v) gb_key
        in
        (fun th db ->
        let init_v = init_agg th db in
        match agg_fn th db, gb_fn th db, collection th db with
        | Fun (f,fsa), Fun (g,gsa), ListCollection l ->
            ListCollection(mc_to_lc
            (List.fold_left (lc_gb init_v
                (apply_mv_agg_fn_lc (Fun(f,fsa)))
                (apply_mv_map_fn_lc (Fun(g,gsa)))) (MC.empty_map()) l))
        
        | Fun (f,fsa), Fun (g,gsa), MapCollection m ->
            MapCollection(
            MC.fold (mc_gb init_v
                (apply_mv_agg_fn_mc (Fun(f,fsa)))
                (apply_mv_map_fn_mc (Fun(g,gsa)))) (MC.empty_map()) m)
        
        | _,_,_ -> failwith "invalid group by aggregation")
    
    (* nested collection -> flatten *)
    (* Note: this only works for ListCollections, since MapCollections are
     * collections of tuples, thus can never be nested simply (i.e. C(C(V)) ). 
     *)
    let flatten nested_collection = fun th db ->
        match nested_collection th db with
        | ListCollection(nl) ->
            let r = List.fold_left (fun acc l_v -> match l_v with
                | ListCollection(l) -> acc@l
                | MapCollection(m) -> acc@(mc_to_lc m)
                | _ -> failwith "invalid nested collection") [] nl
            in ListCollection(r)

        | _ -> failwith "invalid flatten on non-collection"

    (* Tuple collection operators *)
    (* TODO: write documentation on datatypes + implementation requirements
     * for these methods *)
    let tcollection_op lc_f mc_f tcollection key_l = fun th db ->
        let k_v = apply_fn_list th db key_l in
        match tcollection th db with
        | ListCollection l -> lc_f l k_v
        | MapCollection m -> mc_f m k_v
        | _ -> failwith "invalid tuple collection"

    (* map, key -> bool/float *)
    let exists tcollection key_l =
        let lc_f l k_v =
	        let r = List.exists (fun v -> match v with
	            | Tuple(t_v) -> match_kv_tuple k_v t_v
	            | _ -> failwith "invalid tuple collection") l
	        in if r then Int(1) else Int(0)
        in
        let mc_f m k_v = if MC.mem k_v m then Int(1) else Int(0)
        in tcollection_op lc_f mc_f tcollection key_l

    (* map, key -> map value *)
    let lookup tcollection key_l =
        let idx = List.length key_l in
        (* returns the unnamed value (i.e. the last value) from the tuple *)        
        let rv v = match v with 
            | Tuple(t_v) -> List.nth t_v idx
            | _ -> failwith "invalid tuple collection"
        in
        let lc_f l k_v =
            (* return the first value found for a matching key *)
            rv (List.find (fun v -> match v with
                | Tuple(t_v) -> match_kv_tuple k_v t_v
                | _ -> failwith "invalid tuple collection") l)
        in
        let mc_f m k_v = MC.find k_v m
        in tcollection_op lc_f mc_f tcollection key_l
    
    (* map, partial key, pattern -> slice *)
    (* Note: converts slices to lists, to ensure expression evaluation is
     * done on list collections. *)
    let slice tcollection pkey_l pattern =
        let lc_f l pk_v = ListCollection(List.filter (fun v -> match v with
            | Tuple(t_v) -> match_kv_tuple pk_v (tuple_fields t_v pattern)
            | _ -> failwith "invalid tuple collection") l)
        in
        let mc_f m pk_v = ListCollection(mc_to_lc (MC.slice pattern pk_v m))
        in tcollection_op lc_f mc_f tcollection pkey_l


    (* Persistent collections *)

    (* Database retrieval methods *)
    let get_value id db   = DB.get_value id db
    let get_in_map id db  = DB.get_value id db
    let get_out_map id db = DB.get_value id db
    let get_map id db     = DB.get_value id db

    (* Database update methods *)
    let get_kv x = match x with
        | Tuple(l) -> let x = List.rev l in (List.rev (List.tl x), List.hd x)
        | _ -> failwith "invalid tuple"

    let get_update_map c pats = match c with
        | ListCollection(l) ->
            MapCollection(MC.from_list (List.map get_kv l) pats)
        | MapCollection(m) ->
            MapCollection(List.fold_left MC.add_secondary_index m pats) 
        | _ -> failwith "invalid single_map_t" 

    (* Database retrieval methods *)
    let get_value id   = fun th db -> DB.get_value id db
    let get_in_map id  = fun th db -> DB.get_value id db
    let get_out_map id = fun th db -> DB.get_value id db
    let get_map id     = fun th db -> DB.get_value id db

    (* Database udpate methods *)

    (* persistent collection id, value -> update *)
    let update_value id value =
        fun th db -> DB.update_value id (value th db) db; Unit
    
    (* persistent collection id, in key, value -> update *)
    let update_in_map_value id in_kl value = fun th db ->
        DB.update_map id
            (apply_fn_list th db in_kl) (value th db) db; Unit
    
    (* persistent collection id, out key, value -> update *)
    let update_out_map_value id out_kl value = fun th db ->
        DB.update_map id
            (apply_fn_list th db out_kl) (value th db) db; Unit
    
    (* persistent collection id, in key, out key, value -> update *)
    let update_map_value id in_kl out_kl value = fun th db ->
        DB.update_io_map id
            (apply_fn_list th db in_kl) (apply_fn_list th db out_kl)
            (value th db) db;
        Unit

    (* persistent collection id, update collection -> update *)
    let update_in_map id collection = fun th db ->
        let in_pats = DB.get_in_patterns id db
        in DB.update_value id (get_update_map (collection th db) in_pats) db; Unit
    
    let update_out_map id collection = fun th db ->
        let out_pats = DB.get_out_patterns id db
        in DB.update_value id (get_update_map (collection th db) out_pats) db; Unit

    (* persistent collection id, key, update collection -> update *)
    let update_map id in_kl collection = fun th db ->
        let out_pats = DB.get_out_patterns id db in
        DB.update_map id (apply_fn_list th db in_kl)
           (get_update_map (collection th db) out_pats) db;
        Unit

    (* fn id -> code
     * -- code generator should be able to hooks to implementations of
     *    external functions, e.g. invoke function call *)
    let ext_fn fn_id = failwith "external functions not yet supported"



    (* TODO: statement codegen *)
end
*)










module MK3CG : K3Codegen.CG with type db_t = NamedM3Database.db_t =
struct

    module Env = M3Valuation
    module MC  = M3ValuationMap
    module DB  = NamedM3Database

    type single_map_t = DB.single_map_t
    type map_t        = DB.map_t

    type value_t = 
        | Unit
        | Float          of float
        | Int            of int
        | Tuple          of float list
        | Fun            of (value_t -> value_t) * bool 
        | List           of float list
        | TupleList      of (float list * float) list
        (* Note: removing single map list, this is no longer needed since we
         * no longer perform in-tier loops in K3.
        | SingleMapList  of (float list * single_map_t) list
         *)    
        | SingleMap      of single_map_t
        | DoubleMap      of map_t

    type slice_env_t = (string * value_t) list
    
    type env_t       = M3Valuation.t * slice_env_t 
    type db_t        = DB.db_t

    type code_t      = env_t -> db_t -> value_t 
    type op_t        = value_t -> value_t -> value_t

    (* Slice environment *)
    let has_map_var v (_,th) = List.mem_assoc v th
    let get_map_var v (_,th) = List.assoc v th

    (* Helpers *)
    let value_of_float x = Float(x)
    let value_of_const_t x = match x with CFloat(y) -> Float(y)
    
    let float_of_value x = match x with
        | Float f -> f | Int i -> float_of_int i
        | _ -> failwith "invalid float"

    let float_of_const_t x = match x with CFloat(f) -> f 
    
    let const_t_of_value x = match x with
        | Float f -> CFloat f | Int i -> CFloat (float_of_int i)
        | _ -> failwith "invalid const_t"

    let const_t_of_float x = CFloat(x)

    let tuple_of_kv (k,v) = Tuple(k@[v])
    
    let pop_back l =
        let x,y = List.fold_left
	        (fun (acc,rem) v -> match rem with 
	            | [] -> (acc, [v]) | _ -> (acc@[v], List.tl l)) 
	        ([], List.tl l) l
        in x, List.hd y

    let kv_of_tuple t = match t with
        | Tuple(t_v) -> pop_back t_v 
        | _ -> failwith "invalid tuple"

    let rec apply_list fv l =
        match fv,l with
        | (Fun _, []) -> fv
        | (v, []) -> v
        | (Fun (f,_), h::t) -> apply_list (f h) t
        | (_,_) -> failwith "invalid schema application"

    (* Map/aggregate tuple+multivariate function evaluation helpers.
     * uses fields of a tuple to evaluate a multivariate function if the
     * function is a schema application.
     *)
    
    (* returns the value yielded by the mvf. *)
    let apply_mv_map_fn_lc fn v =
        match fn, v with
        | Fun (f,true), Tuple(t_v) ->
            apply_list fn (List.map value_of_float t_v)
        | Fun (f,false), _ -> f v
        | Fun _ , _ -> failwith "invalid map fn argument"
        | _ -> failwith "invalid map function" 

    (* assumes the accumulator is the last argument, preceded by the tuple.
     * returns the value yielded by the mvf. *)
    let apply_mv_agg_fn_lc fn acc v =
        match fn, v with
        | Fun (f,true), Tuple(t_v) ->
            apply_list fn ((List.map value_of_float t_v)@[acc])
        | Fun (f,false), _ -> apply_list fn [v;acc]
        | _,_ -> failwith "invalid aggregation function"

    let apply_mv_map_fn_smlc fn v mv =
        match fn, v, mv with
        | Fun (f,true), Tuple(t_v), SingleMap(m) ->
            apply_list fn ((List.map value_of_float t_v)@[mv])
        | Fun (f,false), _, SingleMap(m) -> apply_list fn [v;mv]
        | Fun _, _, _ -> failwith "invalid single map argument" 
        | _ -> failwith "invalid map function"

    let apply_fn_list th db l = List.map (fun f -> f th db) l

    let mc_to_tlc m = MC.fold (fun k v acc ->
        let t_v = (List.map float_of_const_t k, float_of_const_t v)
        in t_v::acc) [] m
        
    let mc_to_smlc m = MC.fold (fun k v acc ->
        (List.map float_of_const_t k, v)::acc) [] m

    let match_key_prefix prefix k =
        try fst (List.fold_left (fun (run,rem) v -> match run, rem with
            | (true,[]) -> (run, rem) (* could terminate early on success *)
            | (false, _) -> raise Not_found (* early termination for failure *)
            | (_,_) -> (run && (List.hd rem) = v, List.tl rem))
            (true,prefix) k)
        with Not_found -> false

    let tuple_fields t_v idx = snd (List.fold_left (fun (cur,acc) v ->
        (cur+1, if List.mem cur idx then acc@[v] else acc)) (0,[]) t_v)


    (* Operators *)
    let promote_op f_op i_op x y =
        match x,y with
        | (Float f, Int i) -> Float(f_op f (float_of_int i))
        | (Int i, Float f) -> Float(f_op (float_of_int i) f)
        | (Float f1, Float f2) -> Float(f_op f1 f2)
        | (Int i1, Int i2) -> Int(i_op i1 i2)
        | (_,_) -> failwith "invalid arithmetic operands" 
    
    (* Comparisons:
     * -- supports base types, tuples and lists. Tuple and list comparison
     *    works as with standard OCaml semantics (structural comparisons) *)
    (* TODO:
     * -- collections have list semantics here, e.g. element order matters.
     *    Generalize to support set/bag semantics as needed. *)
    let int_op (op : 'a -> 'a -> bool) x y =
        match (x,y) with
        | Unit,_ | _,Unit -> failwith "invalid comparison to unit"
        | _,_ -> if op x y then Int(1) else Int(0) 
    
    let add_op  = promote_op (+.) (+)
    let mult_op = promote_op ( *. ) ( * )
    let lt_op   = (int_op (< ))
    let leq_op  = (int_op (<=))
    let eq_op   = (int_op (= ))
    
    let ifthenelse0_op cond v = 
        let aux v = match v with
            | Int _ -> Int(0) | Float _ -> Float(0.0)
            | _ -> failwith "invalid then clause value"
        in match cond with
        | Float(bcond) -> if bcond <> 0.0 then v else aux v
        | Int(bcond) -> if bcond <> 0 then v else aux v
        | _ -> failwith "invalid predicate value"


    (* Terminals *)
    let const k = fun th db -> value_of_const_t k
    let var v = fun th db -> 
        if (Env.bound v (fst th)) then value_of_const_t (Env.value v (fst th))
        else if has_map_var v th then get_map_var v th 
        else failwith ("Var("^v^"): theta="^(Env.to_string (fst th)))

    (* Tuples *)
    let tuple field_l = fun th db ->
        Tuple(List.map float_of_value (apply_fn_list th db field_l))

    let project tuple idx = fun th db -> match tuple th db with
        | Tuple(t_v) -> Tuple(tuple_fields t_v idx)
        | _ -> failwith "invalid tuple for projection"

    (* Native collections *)
    
    (* assumes numeric element *)
    let singleton el = fun th db -> match el th db with
        | Tuple(t_v) -> TupleList([kv_of_tuple (Tuple t_v)])
        | x -> List([float_of_value x])
    
    (* combines two collections.
     * -- applies only to lists and tuple lists, requiring map conversion *)
    let combine c1 c2 = fun th db ->
        begin match (c1 th db, c2 th db) with
        | (List(c1), List(c2)) -> List(c1@c2)
        | (TupleList(c1), TupleList(c2)) -> TupleList(c1@c2)
        | _,_ -> failwith "invalid collections to combine"
        end

    (* Arithmetic, comparision operators *)
    (* op type, lhs, rhs -> op *)
    let op op_fn l r = fun th db -> op_fn (l th db) (r th db)

    (* Control flow *)
    (* predicate, then clause, else clause -> condition *) 
    let ifthenelse pred t e = fun th db ->
        let valid = match (pred th db) with
            | Float x -> x <> 0.0 | Int x -> x <> 0
            | _ -> failwith "invalid predicate value"
        in if valid then t th db else e th db

    (* statements -> block *)    
    let block stmts =
        let idx = List.length stmts - 1 in
        (fun th db -> let rvs = apply_fn_list th db stmts in List.nth rvs idx)
 
    (* Functions *)
    (* arg, schema application, body -> fn *)
    let lambda v schema_app body = fun th db ->
        let f_aux f = body (Env.add (fst th) v (CFloat f), snd th) db in
        let map_aux m = body (fst th, (v,m)::(snd th)) db in
        let fn v = match v with
            | Float(x) -> f_aux x
            | Int(x) -> f_aux (float_of_int x)
            (* We still bind map vars during map accesses, so still need
             * support for adding single/double maps to environments *)
            | SingleMap(m) -> map_aux v
            | DoubleMap(m) -> map_aux v
            | _ -> failwith "invalid function argument"
        in Fun(fn, schema_app)
    
    (* arg1, type, arg2, type, body -> assoc fn *)
    (* M3 assoc functions should never need to use slices *)
    let assoc_lambda v1 v2 body = fun th db ->
        let aux vl1 vl2 =
            let new_th = (Env.add (Env.add (fst th)
                v1 (const_t_of_value vl1)) v2 (const_t_of_value vl2), snd th)
            in body new_th db
        in
        let fn1 vl1 = Fun((fun vl2 -> aux vl1 vl2), false)
        in Fun(fn1, false)

    (* fn, arg -> evaluated fn *)
    let apply fn arg = fun th db -> match fn th db  with
        | Fun (f,false) -> f (arg th db)
        | _ -> failwith "invalid function for function application"
    
    (* Structural recursion operations *)
    (* map fn, collection -> map *)
    let map map_fn collection = fun th db ->
        let aux fn v_f rv_f =
            List.map (fun v -> rv_f (apply_mv_map_fn_lc fn (v_f v))) in
        match map_fn th db, collection th db with
        | (Fun (f,sa), List l) ->
            List(aux (Fun(f,sa)) value_of_float float_of_value l)
        
        | (Fun (f,sa), TupleList l) ->
            TupleList(aux (Fun(f,sa)) tuple_of_kv kv_of_tuple l)

        (*
        | (Fun (f,sa), SingleMapList l) ->
            List.map (fun (k_v,m) -> 
                apply_mv_map_fn_smlc (Fun(f,sa)) (Tuple k_v) (SingleMap m)) l
        *)

        | (Fun _, _) -> failwith "invalid map collection"
        | (_,_) -> failwith "invalid map function"
    
    (* agg fn, initial agg, collection -> agg *)
    (* Note: accumulator is the last arg to agg_fn *)
    let aggregate agg_fn init_agg collection = fun th db ->
        let aux fn v_f l = List.fold_left
            (fun acc v -> apply_mv_agg_fn_lc fn acc (v_f v)) (init_agg th db) l
        in
        match agg_fn th db, collection th db with
        | (Fun (f,sa), List l) -> aux (Fun(f,sa)) value_of_float l
        | (Fun (f,sa), TupleList l) -> aux (Fun(f,sa)) tuple_of_kv l
        | (Fun _, _) -> failwith "invalid agg collection"        
        | (_,_) -> failwith "invalid agg function"

    (* agg fn, initial agg, grouping fn, collection -> agg *)
    (* Perform group-by aggregation by using a temporary MapCollection,
     * which currently is a hash table, thus we have hash-based aggregation. *)
    let group_by_aggregate agg_fn init_agg gb_fn collection =
        let gb init_v gb_acc agg_f gb_key =
            let gb_agg = if MC.mem gb_key gb_acc
                 then (value_of_const_t (MC.find gb_key gb_acc)) else init_v in
            let new_gb_agg = agg_f gb_agg
            in MC.add gb_key (const_t_of_value new_gb_agg) gb_acc
        in
        let lc_gb init_v f g mc kv =
            let gb_key = List.map const_t_of_float
	            (match g (tuple_of_kv kv) with
	                | Tuple(t_v) -> t_v | x -> [float_of_value x])
            in gb init_v mc (fun a -> f a (tuple_of_kv kv)) gb_key
        in
        (fun th db ->
        let init_v = init_agg th db in
        match agg_fn th db, gb_fn th db, collection th db with
        | Fun (f,fsa), Fun (g,gsa), TupleList l -> TupleList(mc_to_tlc
            (List.fold_left (lc_gb init_v
                (apply_mv_agg_fn_lc (Fun(f,fsa)))
                (apply_mv_map_fn_lc (Fun(g,gsa)))) (MC.empty_map()) l))

        | Fun _, Fun _, _ -> failwith "invalid gb-agg collection"
        | Fun _, _, _ -> failwith "invalid group-by function"
        | _,_,_ -> failwith "invalid group by aggregation function")
    
    (* nested collection -> flatten *)
    let flatten nested_collection =
        failwith "flattening should not be needed for M3"

    (* Tuple collection operators *)
    (* TODO: write documentation on datatypes + implementation requirements
     * for these methods *)
    let tcollection_op lc_f smc_f dmc_f tcollection key_l = fun th db ->
        let k_v = apply_fn_list th db key_l in
        match tcollection th db with
        | TupleList l -> lc_f l (List.map float_of_value k_v)
        | SingleMap m -> smc_f m (List.map const_t_of_value k_v)
        | DoubleMap m -> dmc_f m (List.map const_t_of_value k_v)
        | _ -> failwith "invalid tuple collection"

    (* map, key -> bool/float *)
    let exists tcollection key_l =
        let lc_f l k =
            let r = List.exists (fun (tk,tv) -> match_key_prefix k tk) l
            in if r then Int(1) else Int(0)
        in
        let mc_f m k_v = if MC.mem k_v m then Int(1) else Int(0)
        in tcollection_op lc_f mc_f mc_f tcollection key_l

    (* map, key -> map value *)
    let lookup tcollection key_l =
        (* returns the (unnamed) value from the first tuple with a matching key *)
        let lc_f l k = value_of_float (snd
            (List.find (fun (tk,tv) -> match_key_prefix k tk) l)) in
        let smc_f m k_v = value_of_const_t (MC.find k_v m) in
        let dmc_f m k_v = SingleMap(MC.find k_v m)
        in tcollection_op lc_f smc_f dmc_f tcollection key_l
    
    (* map, partial key, pattern -> slice *)
    (* Note: converts slices to lists, to ensure expression evaluation is
     * done on list collections. *)
    let slice tcollection pkey_l pattern =
        let lc_f l pk = TupleList(List.filter
            (fun (tk,tv) -> match_key_prefix pk (tuple_fields tk pattern)) l)
        in
        let smc_f m pk = TupleList(mc_to_tlc (MC.slice pattern pk m)) in
        let dmc_f m pk =
            (*SingleMapList(mc_to_smlc (MC.slice pattern pk m))*)
            failwith "slicing not supported for double maps"
        in tcollection_op lc_f smc_f dmc_f tcollection pkey_l


    (* Database retrieval methods *)
    let get_value id = fun th db -> match DB.get_value id db with
        Some(x) -> value_of_const_t x | None -> Float(0.0)

    let get_in_map id  = fun th db -> SingleMap(DB.get_in_map id db)
    let get_out_map id = fun th db -> SingleMap(DB.get_out_map id db)
    let get_map id     = fun th db -> DoubleMap(DB.get_map id db)

    (* Database udpate methods *)
    let const_t_kv (k,v) = (List.map const_t_of_float k, const_t_of_float v)

    let get_update_value th db v = const_t_of_value (v th db)
    let get_update_key th db kl =
        List.map const_t_of_value (apply_fn_list th db kl)

    let get_update_map c pats = match c with
        | TupleList(l) -> MC.from_list (List.map const_t_kv l) pats
        | SingleMap(m) -> List.fold_left MC.add_secondary_index m pats
        | _ -> failwith "invalid single_map_t" 

    (* persistent collection id, value -> update *)
    let update_value id value = fun th db ->
        DB.update_value id (get_update_value th db value) db; Unit
    
    (* persistent collection id, in key, value -> update *)
    let update_in_map_value id in_kl value = fun th db ->
        DB.update_in_map_value id
            (get_update_key th db in_kl)
            (get_update_value th db value) db;
        Unit
    
    (* persistent collection id, out key, value -> update *)
    let update_out_map_value id out_kl value = fun th db ->
        DB.update_out_map_value id
            (get_update_key th db out_kl)
            (get_update_value th db value) db;
        Unit
    
    (* persistent collection id, in key, out key, value -> update *)
    let update_map_value id in_kl out_kl value = fun th db ->
        DB.update_map_value id
            (get_update_key th db in_kl)
            (get_update_key th db out_kl)
            (get_update_value th db value) db;
        Unit

    (* persistent collection id, update collection -> update *)
    let update_in_map id collection = fun th db ->
        let in_pats = DB.get_in_patterns id db
        in DB.update_in_map id (get_update_map (collection th db) in_pats) db;
        Unit
    
    let update_out_map id collection = fun th db ->
        let out_pats = DB.get_out_patterns id db
        in DB.update_out_map id (get_update_map (collection th db) out_pats) db;
        Unit

    (* persistent collection id, key, update collection -> update *)
    let update_map id in_kl collection = fun th db ->
        let out_pats = DB.get_out_patterns id db in
        DB.update_map id (get_update_key th db in_kl)
           (get_update_map (collection th db) out_pats) db;
        Unit    
    
    let ext_fn fn_id = failwith "External functions not yet supported"
    
    (* Statement code generation *)

    let singleton_update outv incr init init_singleton =
        let out_l = List.map var outv in
        let update = lambda "current_v" false
            (op add_op (var "current_v") incr) in
        let sing_init =
            if init_singleton then op add_op incr init
            else op add_op incr (lookup init out_l)
        in (update, sing_init)
    
    let slice_update outv incr init init_singleton =
        let get_tlc v = match v with
            | TupleList(l) -> l | _ -> failwith "invalid out tier" in
        let out_l = List.map var outv in
        let init_v th db =
            if init_singleton then float_of_value (init th db)
            else float_of_value ((lookup init out_l) th db)
        in
        let update = lambda "current_slice" false
            (fun th db ->
                let c_s = get_tlc ((var "current_slice") th db) in
                let d_s = get_tlc (incr th db) in
                let aux acc (dk,dv) =
                    if List.mem_assoc dk acc then
                        (dk, (List.assoc dk acc)+.dv)::(List.remove_assoc dk acc)
                    else (dk, (init_v th db)+.dv)::acc
                in TupleList(List.fold_left aux c_s d_s)) 
        in (update, const (CFloat(0.0)))
    
    let stmt_loop db_f inv patv pat body =
        let direct = (List.length patv) = (List.length inv) in
        let var_list th db vl =
            List.map (fun v -> const_t_of_value ((var v) th db)) vl in 
        let coll_f th db =         
            if direct then [var_list th db inv]
            else MC.slice_keys pat (var_list th db patv) (db_f db)
        in
        let loop_f th db img =
            let new_th = List.fold_left (fun acc (v,k) -> Env.add acc v k)
                (fst th) (List.combine inv img)
            in ignore(body (new_th, snd th) db)
        in fun th db -> List.iter (loop_f th db) (coll_f th db); Unit 

    let singleton_statement mapn inv outv patv pat update sing_init =
        let in_l = List.map var inv in
        let out_l = List.map var outv in
        match inv, outv with
        | [],[] -> update_value mapn (apply update (get_value mapn)) 

        | x,[] ->
            let body = update_in_map_value mapn in_l
                (apply update (lookup (get_in_map mapn) in_l))
            in stmt_loop (DB.get_in_map mapn) inv patv pat body

        | [],x ->
            let m = get_out_map mapn in
            let update_code =
                ifthenelse (exists m out_l)
                    (apply update (lookup m out_l)) sing_init
            in update_out_map_value mapn out_l update_code

        | x,y ->
            let body =
                let m = lookup (get_map mapn) in_l in
                let update_code =
	                (ifthenelse (exists m out_l)
	                    (apply update (lookup m out_l)) sing_init)
                in update_map_value mapn in_l out_l update_code
            in stmt_loop (DB.get_map mapn) inv patv pat body

    
    let statement mapn inv outv patv pat update sing_init =
        let in_l = List.map var inv in
        match inv, outv with
        | [],[] -> failwith "invalid slice update on a singleton map"
        | x,[] -> failwith "invalid slice update on a singleton out tier"
        
        | [],x -> update_out_map mapn (apply update (get_out_map mapn))

        | x,y ->
            let body = update_map
                mapn in_l (apply update (lookup (get_map mapn) in_l))
            in stmt_loop (DB.get_map mapn) inv patv pat body 

end