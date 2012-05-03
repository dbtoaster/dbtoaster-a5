open Types
open K3
open Values
open Values.K3Value
open Sources
open Sources.Adaptors
open Database

module K3CG : K3Codegen.CGI
    with type db_t = NamedK3Database.db_t and
         type value_t = K3Value.t
=
struct

    type value_t = K3Value.t

    module Env = K3Valuation
    
    module MC  = K3ValuationMap
    module DB  = NamedK3Database

    (* Use runtime for main method *)
    module RT  = Runtime.Make(DB)
    open RT

    type slice_env_t = (string * value_t) list
    
    type env_t       = Env.t * slice_env_t 
    type db_t        = DB.db_t

    type eval_t      = env_t -> db_t -> value_t
    type code_t      =
          Eval of eval_t
        | Trigger of Schema.event_t * (Types.const_t list -> db_t -> unit)
        | Main of (unit -> unit) 

    type op_t        = value_t -> value_t -> value_t

    (* Slice environment *)
    let is_env_value v (_,th) = List.mem_assoc v th
    let get_env_value v (_,th) = List.assoc v th

    (* Helpers *)
    let get_expr expr_opt = match expr_opt with
        | None -> ""
        | Some(e) -> " at code: "^(string_of_expr e)

    let get_eval e = match e with
        | Eval(e) -> e
        | _ -> failwith "unable to eval expr"

    let get_trigger e = match e with
        | Trigger(x,y) -> (x,y)
        | _ -> failwith "unable to eval trigger"

    let rec is_flat (t:K3.type_t) = match t with
        | TBase _ -> true
        | TTuple t -> List.for_all is_flat t
        | _ -> false

    let value_of_const_t x = 
      if Debug.active "HASH-STRINGS" then
         match x with | CString(s) -> BaseValue(CInt(Hashtbl.hash s))
                      | _          -> BaseValue(x)
      else BaseValue(x)
    
    let float_of_value x = match x with
        | BaseValue(c) -> float_of_const c
        | _ -> failwith ("invalid float: "^(string_of_value x))

    let float_of_const_t x = 
      match x with CFloat(f)    -> f 
                 | CString(s)   -> 
                     if Debug.active "HASH-STRINGS" then 
                        (float_of_int (Hashtbl.hash s))
                     else failwith ("invalid float: "^(string_of_const x))
                 | CInt(i)      -> float_of_int i
                 | CBool(true)  -> 1.
                 | CBool(false) -> 0.

    let const_of_value x = match x with
      | BaseValue(c) -> c
      | _ -> failwith ("invalid const_t: "^(string_of_value x))

    let value_of_tuple x = Tuple(x)
    let tuple_of_value x = match x with
        | Tuple(y) -> y
        | _ -> failwith ("invalid tuple: "^(string_of_value x))

    let pop_back l =
        let x,y = List.fold_left
            (fun (acc,rem) v -> match rem with 
                | [] -> (acc, [v]) | _ -> (acc@[v], List.tl rem)) 
            ([], List.tl l) l
        in x, List.hd y

    let tuple_of_kv (k,v) = Tuple(k@[v])

    let kv_of_tuple t = match t with
        | Tuple(t_v) -> pop_back t_v 
        | _ -> failwith ("invalid tuple: "^(string_of_value t))

    (* Map/aggregate tuple+multivariate function evaluation helpers.
     * uses fields of a tuple to evaluate a multivariate function if the
     * function is a schema application.
     *)

    (* Nested, multivariate function application *)
    let rec apply_list fv l =
        match fv,l with
        | (Fun _, []) -> fv
        | (v, []) -> v
        | (Fun f, h::t) -> apply_list (f h) t
        | (_,_) -> failwith "invalid schema application"
    
    let apply_fn_list th db l = List.map (fun f -> (get_eval f) th db) l

    (* Persistent collection converters to temporary values *)
    let smc_to_tlc m = MC.fold (fun k v acc ->
        let t_v = k@[v] in (value_of_tuple t_v)::acc) [] m
        
    let dmc_to_smlc m = MC.fold (fun k v acc -> (k, v)::acc) [] m

    let dmc_to_c m = MC.fold(fun k m1 acc ->
        let v = TupleList(smc_to_tlc m1) in (Tuple(k@[v]))::acc) [] m

    let smlc_to_c smlc = List.map (fun (k,m) ->
        Tuple(k@[TupleList(smc_to_tlc m)])) smlc 

    let nmc_to_c nm = smc_to_tlc nm

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
    (* Comparisons:
     * -- supports base types, tuples and lists. Tuple and list comparison
     *    works as with standard OCaml semantics (structural comparisons) *)
    (* TODO:
     * -- collection comparisons have list semantics here, e.g. element order
     * matters. Generalize to support set/bag semantics as needed. *)
    let int_op (op : const_t -> const_t -> const_t) x y =
        match (x,y) with
        | Unit,_ | _,Unit -> failwith "invalid comparison to unit"
        | _,_ -> if (op (const_of_value x) 
                        (const_of_value y)) = CBool(true)
                 then BaseValue(CInt(1))
                 else BaseValue(CInt(0))
    
    let bin_op (op : const_t -> const_t -> const_t) x y =
      match (x,y) with
         | (BaseValue(cx), BaseValue(cy)) -> BaseValue(op cx cy)
         | (_,_) -> failwith "invalid arithmetic operation"
    
    let add_op  = bin_op Arithmetic.sum
    let mult_op = bin_op Arithmetic.prod
    let lt_op   = (int_op Arithmetic.cmp_lt)
    let leq_op  = (int_op Arithmetic.cmp_lte)
    let eq_op   = (int_op Arithmetic.cmp_eq)
    let neq_op  = (int_op Arithmetic.cmp_neq)
    
    let ifthenelse0_op cond v = 
        let aux v = match v with
            | BaseValue(CInt(_)) -> BaseValue(CInt(0))
            | BaseValue(CFloat(_)) -> BaseValue(CFloat(0.))
            | _ -> failwith "invalid then clause value"
        in match cond with
        | BaseValue(CFloat(bcond)) -> if bcond <> 0.0 then v else aux v
        | BaseValue(CInt(bcond)) -> if bcond <> 0 then v else aux v
        | _ -> failwith "invalid predicate value"


    (* Terminals *)
    let const ?(expr = None) k = Eval(fun th db -> value_of_const_t k)
    let var ?(expr = None) v _ = Eval(fun th db -> 
        if (Env.bound v (fst th)) then Env.value v (fst th)
        else if is_env_value v th then get_env_value v th 
        else failwith ("Var("^v^"): theta="^(Env.to_string (fst th))))

    (* Tuples *)
    let tuple ?(expr = None) field_l = Eval(fun th db ->
        Tuple(apply_fn_list th db field_l))

    let project ?(expr = None) tuple idx = Eval(fun th db ->
        match (get_eval tuple) th db with
        | Tuple(t_v) -> Tuple(tuple_fields t_v idx)
        | _ -> failwith ("invalid tuple for projection"^(get_expr expr)))

    (* Native collections *)
    
    let singleton ?(expr = None) el el_t =
        let rv_f v = match el_t with
            | TTuple t ->
                if List.for_all is_flat t then TupleList([v])
                else ListCollection([v])
            | TBase(TFloat)    -> FloatList([v])
            | TBase(TInt)      -> FloatList([v])
            | TBase(_)         -> failwith "Unsupported base type in K3"
            | Collection _ -> ListCollection([v])
            | Fn (args_t,body_t) ->
                (* TODO: ListCollection(v) ? *)
                failwith "first class functions not supported yet"
            | TUnit -> failwith "cannot create singleton of unit expression"
        in Eval(fun th db ->
        begin match (get_eval el) th db with
        | Tuple _ | BaseValue _ 
        | FloatList _ | TupleList _ | EmptyList
        | ListCollection _ | MapCollection _ as v -> rv_f v
        | SingleMapList l -> rv_f (ListCollection(smlc_to_c l))
        | SingleMap m -> rv_f (TupleList(smc_to_tlc m))
        | DoubleMap m -> rv_f (ListCollection(dmc_to_c m))
        | v -> failwith ("invalid singleton value: "^
                         (string_of_value v)^(get_expr expr))
        end)
    
    let combine_impl ?(expr = None) c1 c2 =
        begin match c1, c2 with
        | EmptyList, _ -> c2
        | _, EmptyList -> c1
        | FloatList(c1), FloatList(c2) -> FloatList(c1@c2)
        | TupleList(c1), TupleList(c2) -> TupleList(c1@c2)
        | SingleMapList(c1), SingleMapList(c2) -> SingleMapList(c1@c2)
        | ListCollection(c1), ListCollection(c2) -> ListCollection(c1@c2)
        
        | SingleMap(m1), SingleMap(m2) ->
            TupleList((smc_to_tlc m1)@(smc_to_tlc m2))
        
        | DoubleMap(m1), DoubleMap(m2) ->
            ListCollection((dmc_to_c m1)@(dmc_to_c m2))

        | MapCollection(m1), MapCollection(m2) ->
            ListCollection((nmc_to_c m1)@(nmc_to_c m2))

        | _,_ -> failwith ("invalid collections to combine"^(get_expr expr))
        end

    let combine ?(expr = None) c1 c2 = Eval(fun th db ->
        combine_impl ~expr:expr ((get_eval c1) th db) ((get_eval c2) th db))

    (* Arithmetic, comparision operators *)
    (* op type, lhs, rhs -> op *)
    let op ?(expr = None) op_fn l r = Eval(fun th db ->
        op_fn ((get_eval l) th db) ((get_eval r) th db))

    (* Control flow *)
    (* predicate, then clause, else clause -> condition *) 
    let ifthenelse ?(expr = None) pred t e = Eval(fun th db ->
        let valid = match ((get_eval pred) th db) with
            | BaseValue(CFloat(x)) -> x <> 0.0 
            | BaseValue(CInt(x))   -> x <> 0
            | _ -> failwith ("invalid predicate value"^(get_expr expr))
        in if valid then (get_eval t) th db else (get_eval e) th db)

    (* statements -> block *)    
    let block ?(expr = None) stmts =
        let idx = List.length stmts - 1 in
        Eval(fun th db ->
          let rvs = apply_fn_list th db stmts in List.nth rvs idx)
 
    (* iter fn, collection -> iteration *)
    let iterate ?(expr = None) iter_fn collection = Eval(fun th db ->
        let function_value = (get_eval iter_fn) th db in
        let collection_value = (get_eval collection) th db in
        Debug.print "LOG-INTERPRETER-ITERATE" (fun () ->
          "Iterating over "^(K3Value.to_string collection_value)
        );
        let aux fn l = ((List.iter (fun v -> 
          Debug.print "LOG-INTERPRETER-ITERATE" (fun () ->
            "   ["^(K3Value.to_string v)^"]"
          );
          match fn v with
            | Unit -> ()
            | _ -> failwith ("invalid iteration"^(get_expr expr))) l); Unit)
        in
        begin match function_value, collection_value with
        | (_, EmptyList) -> Unit
        | (Fun f, FloatList l) -> aux f l
        | (Fun f, TupleList l) -> aux f l
        | (Fun f, ListCollection l) -> aux f l

        | (Fun f, SingleMapList l) ->  (List.iter (fun (k,m) ->
             Debug.print "LOG-INTERPRETER-ITERATE" (fun () ->
               "   ["^(String.concat ";" (List.map K3Value.to_string k))^
               "] -> "^(K3Value.to_string (SingleMap(m)))
             );
            match f (Tuple(k@[SingleMap m])) with
            | Unit -> ()
            | _ -> failwith ("invalid iteration"^(get_expr expr))) l); Unit

        (* Currently we convert to a list since SliceableMap doesn't implement
         * an 'iter' function. TODO: we could easily add this. *)
        | (Fun f, SingleMap m) -> aux f (smc_to_tlc m)
        | (Fun f, DoubleMap m) -> aux f (dmc_to_c m)
        | (Fun f, MapCollection m) -> aux f (nmc_to_c m)
        | (Fun _, _) -> failwith ("invalid iterate collection"^(get_expr expr))
        | _ -> failwith ("invalid iterate function"^(get_expr expr))
        end)

    (* Functions *)
    let bind_float expr arg th f =
        begin match arg with
        | AVar(var,_) -> Env.add (fst th) var (BaseValue(CFloat(f))), snd th
        | ATuple(vt_l) -> 
            failwith ("cannot bind a float to a tuple"^(get_expr expr))
        end

    let bind_tuple expr arg th t = begin match arg with
        | AVar(v,vt) ->
            begin match vt with
            | TTuple(_) -> fst th, (v,value_of_tuple t)::(snd th)
            | _ -> failwith ("cannot bind tuple to "^v^(get_expr expr))
            end
        | ATuple(vt_l) ->
            begin try 
            (List.fold_left2 (fun acc (v,_) tf -> Env.add acc v tf)
                (fst th) vt_l t, snd th)
            with Invalid_argument _ ->
                failwith ("could not bind tuple arg to value: "^
                          (string_of_value (value_of_tuple t))^(get_expr expr))
            end
         end

    let bind_value expr arg th m = begin match arg with
        | AVar(var,_) -> fst th, (var,m)::(snd th)
        | ATuple(vt_l) ->
            failwith ("cannot bind a value to a tuple"^(get_expr expr))
        end

    let bind_arg expr arg th v = begin match v with
        | BaseValue(CFloat(x)) -> bind_float expr arg th x
        | BaseValue(CInt(x)) -> bind_float expr arg th (float_of_int x)
        | Tuple t -> bind_tuple expr arg th t
        | _ -> bind_value expr arg th v
        end

    (* arg, schema application, body -> fn *)
    let lambda ?(expr = None) arg body = Eval(fun th db ->
        let fn v = (get_eval body) (bind_arg expr arg th v) db
        in Fun fn)
    
    (* arg1, type, arg2, type, body -> assoc fn *)
    (* M3 assoc functions should never need to use slices *)
    let assoc_lambda ?(expr = None) arg1 arg2 body = Eval(fun th db ->
        let aux vl1 vl2 =
            let new_th = bind_arg expr arg2 (bind_arg expr arg1 th vl1) vl2 
            in (get_eval body) new_th db
        in let fn1 vl1 = Fun(fun vl2 -> aux vl1 vl2)
        in Fun fn1)
				
		(* arg, external function id, external function return type -> external fn *)
    let external_lambda ?(expr = None) fn_id arg fn_t = Eval(fun th db ->
        let fn v = 
						let fn_args = begin match v with
		        | BaseValue(c) -> [c]
		        | Tuple t -> List.map const_of_value t
		        | _ -> failwith "Arguments of external functions can be only values or tuple of values"
		        end in
						try 
							let external_fn = 
								StringMap.find fn_id (!Arithmetic.arithmetic_functions) in
							let ret_c = (snd external_fn) fn_args in
							let ret_t = K3.TBase(Types.type_of_const ret_c) in
							if( fn_t <> ret_t) then
									 failwith ("Unexpected return value type for external function: "^
											fn_id^" : "^(K3.string_of_type fn_t)^" <> "^(K3.string_of_type ret_t));
							BaseValue(ret_c)
			      with Not_found -> failwith ("No external function named: "^fn_id)
        in Fun fn)

    (* fn, arg -> evaluated fn *)
    let apply ?(expr = None) fn arg = Eval(fun th db ->
        begin match (get_eval fn) th db, (get_eval arg) th db with
        | Fun f, x -> f x
        | _ -> failwith ("invalid function for fn app"^(get_expr expr))
        end)
    
    (* Collection operations *)
    
    (* map fn, collection -> map *)
    (* picks the return collection implementation based on the return type
     * of the map function *)
    let map ?(expr = None) map_fn map_rt collection =
        let rv_f v = match map_rt with
            | TBase(TFloat) -> FloatList(v) 
            | TBase(TInt)   -> FloatList(v)
            | TBase(_)      -> failwith "Unsupported base type in K3"
            | TTuple tl ->
                if is_flat map_rt then TupleList(v)
                else ListCollection(v)
            | Collection t -> ListCollection(v)
            | Fn (args_t,body_t) ->
                (* TODO: ListCollection(v) ? *)
                failwith "first class functions not supported yet"
            | TUnit -> failwith "map function cannot return unit type"
        in
        Eval(fun th db ->
        let aux fn  = List.map (fun v -> fn v) in
        match (get_eval map_fn) th db, (get_eval collection) th db with
        | _, EmptyList       -> rv_f []
        | Fun f, FloatList l -> rv_f (aux f l)
        | Fun f, TupleList l -> rv_f (aux f l)
        | Fun f, SingleMap m -> rv_f (aux f (smc_to_tlc m))
        | Fun f, DoubleMap m -> rv_f (aux f (dmc_to_c m))
        | Fun f, SingleMapList s -> rv_f (aux f (smlc_to_c s))
        | Fun f, ListCollection l -> rv_f (aux f l)
        | Fun f, MapCollection m -> rv_f (aux f (nmc_to_c m))
        | (Fun _, x) -> failwith ("invalid map collection: "^
                                  (string_of_value x)^(get_expr expr))
        | _ -> failwith ("invalid map function"^(get_expr expr)))
    
    (* agg fn, initial agg, collection -> agg *)
    (* Note: accumulator is the last arg to agg_fn *)
    let aggregate ?(expr = None) agg_fn init_agg collection = Eval(fun th db ->
        let aux fn v_f l = List.fold_left
            (fun acc v -> apply_list fn ((v_f v)::[acc])) 
            ((get_eval init_agg) th db) l
        in
        match (get_eval agg_fn) th db, (get_eval collection) th db with
        | _, EmptyList            -> ((get_eval init_agg) th db)
        | Fun _ as f, FloatList l -> aux f (fun x -> x) l
        | Fun _ as f, TupleList l -> aux f (fun x -> x) l
        | Fun _ as f, ListCollection l -> aux f (fun x -> x) l
        | Fun _ as f, SingleMapList l ->
            aux f (fun (k,m) -> Tuple (k@[SingleMap m])) l
        | Fun _ as f, SingleMap m -> aux f (fun x -> x) (smc_to_tlc m)
        | Fun _ as f, DoubleMap m -> aux f (fun x -> x) (dmc_to_c m)
        | Fun _ as f, MapCollection m -> aux f (fun x -> x) (nmc_to_c m)
        | Fun _, v -> failwith ("invalid agg collection: "^
                                  (string_of_value v)^(get_expr expr))        
        | _ -> failwith ("invalid agg function"^(get_expr expr)))

    (* agg fn, initial agg, grouping fn, collection -> agg *)
    (* Perform group-by aggregation by using a temporary SliceableMap,
     * which currently is a hash table, thus we have hash-based aggregation. *)
    let group_by_aggregate ?(expr = None) agg_fn init_agg gb_fn collection =
        let apply_gb_agg init_v gb_acc agg_f gb_key =
            let gb_agg = if MC.mem gb_key gb_acc
                 then MC.find gb_key gb_acc else init_v in
            let new_gb_agg = agg_f gb_agg
            in MC.add gb_key new_gb_agg gb_acc
        in
        let apply_gb init_v agg_f gb_f gbc t =
            let gb_key = match gb_f t with | Tuple(t_v) -> t_v | x -> [x]
            in apply_gb_agg init_v gbc (fun a -> agg_f [t; a]) gb_key
        in
        Eval(fun th db ->
        let init_v = (get_eval init_agg) th db in
        match (get_eval agg_fn) th db,
              (get_eval gb_fn) th db,
              (get_eval collection) th db
        with
        | Fun f, Fun g, TupleList l -> TupleList(smc_to_tlc
            (List.fold_left (apply_gb
                init_v (apply_list (Fun f)) g) (MC.empty_map()) l))
        
        | Fun f, Fun g, EmptyList -> TupleList([])

        | Fun _, Fun _, v -> failwith ("invalid gb-agg collection: "^
            (string_of_value v)^(get_expr expr))
        
        | Fun _, gb, _ -> failwith ("invalid group-by function: "^
            (string_of_value gb)^(get_expr expr))
        
        | f,_,_ -> failwith ("invalid group by agg fn: "^
            (string_of_value f)^(get_expr expr)))


    (* nested collection -> flatten *)
    let flatten ?(expr = None) nested_collection =
        (*
        let flatten_inner_map k m acc =
          MC.fold (fun k2 v acc -> acc@[Tuple(k@k2@[v])]) acc m
        in
        *)
        Eval(fun th db ->
        match ((get_eval nested_collection) th db) with
        | FloatList _ ->
            failwith ("cannot flatten a FloatList"^(get_expr expr))
        
        | TupleList _ ->
            failwith ("cannot flatten a TupleList"^(get_expr expr))
        
        | SingleMap _ ->
            failwith ("cannot flatten a SingleMap"^(get_expr expr))

        | ListCollection l -> 
            if (List.length l) > 0 then
              List.fold_left (combine_impl ~expr:expr) (List.hd l) (List.tl l)
            else EmptyList
        
        | EmptyList -> EmptyList            

        (* Note: for now we don't flatten tuples with collection fields,
         * such as SingleMapList, DoubleMap or MapCollections.
         * We need to add pairwith to first push the outer part of the
         * tuple into the collection, and then we can flatten.
         *)
        (* Flattening applied to SingleMapList and DoubleMap can create a
         * TupleList since the input is exactly of depth two.
        | SingleMapList l ->
            let r = List.fold_left
              (fun acc (k,m) -> flatten_inner_map k m acc) [] l
            in TupleList(r)

        | DoubleMap m ->
            let r = MC.fold flatten_inner_map [] m
            in TupleList(r)
        *)

        | _ -> failwith ("invalid collection to flatten"^(get_expr expr)))
        
        

    (* Tuple collection operators *)
    (* TODO: write documentation on datatypes + implementation requirements
     * for these methods *)
    let tcollection_op expr ex_s lc_f smc_f dmc_f nmc_f tcollection key_l =
        Eval(fun th db ->
        let k = apply_fn_list th db key_l in
        try begin match (get_eval tcollection) th db with
            | TupleList l -> lc_f l k
            | SingleMap m -> smc_f m k
            | DoubleMap m -> dmc_f m k
            | MapCollection m -> nmc_f m k
            | v -> failwith ("invalid tuple collection: "^(string_of_value v))
            end
        with Not_found ->
            print_string
               ("Collection operation failed: "^
                (ListExtras.ocaml_of_list string_of_value k)^" in "^
                (string_of_value ((get_eval tcollection) th db))^
                "\n"^(get_expr expr)^"\n");
            failwith "Collection Operation Failed"
        )
            (* failwith ("collection operation failed: "^ex_s^(get_expr expr)))*)

    (* map, key -> bool/float *)
    let exists ?(expr = None) tcollection key_l =
        let lc_f l k =
            let r = List.exists (fun t ->
                match_key_prefix k (tuple_of_value t)) l
            in if r then BaseValue(CInt(1)) 
                    else BaseValue(CInt(0))
        in
        let mc_f m k_v = if MC.mem k_v m then BaseValue(CInt(1)) 
                                         else BaseValue(CInt(0))
        in tcollection_op expr "exists" lc_f mc_f mc_f mc_f tcollection key_l

    (* map, key -> map value *)
    let lookup ?(expr = None) tcollection key_l =
        (* returns the (unnamed) value from the first tuple with a matching key *)
        let lc_f l k =
            let r = List.find (fun t ->
                match_key_prefix k (tuple_of_value t)) l in
            let r_t = tuple_of_value r in
            let r_len = List.length r_t 
            in List.nth r_t (r_len-1)
        in
        let smc_f m k_v = MC.find k_v m in
        let dmc_f m k_v = SingleMap(MC.find k_v m) in
        let nmc_f m k_v = MC.find k_v m in
        tcollection_op expr "lookup" lc_f smc_f dmc_f nmc_f tcollection key_l
    
    (* map, partial key, pattern -> slice *)
    (* Note: converts slices to lists, to ensure expression evaluation is
     * done on list collections. *)
    let slice ?(expr = None) tcollection pkey_l pattern =
        let lc_f l pk = TupleList(List.filter (fun t ->
            match_key_prefix pk (tuple_fields (tuple_of_value t) pattern)) l)
        in
        let smc_f m pk = TupleList(smc_to_tlc (MC.slice pattern pk m)) in
        let dmc_f m pk = SingleMapList(dmc_to_smlc (MC.slice pattern pk m)) in
        let nmc_f m pk = ListCollection(nmc_to_c (MC.slice pattern pk m))
        in tcollection_op expr "slice" lc_f smc_f dmc_f nmc_f tcollection pkey_l


    (* Database retrieval methods *)
    let get_value ?(expr = None) (_) id = Eval(fun th db ->
        match DB.get_value id db with | Some(x) -> x | None -> 
               BaseValue(CFloat(0.0)))

    let get_in_map ?(expr = None) (_) (_) id =
        Eval(fun th db -> SingleMap(DB.get_in_map id db))
    
    let get_out_map ?(expr = None) (_) (_) id =
        Eval(fun th db -> SingleMap(DB.get_out_map id db))
    
    let get_map ?(expr = None) (_) (_) id =
        Eval(fun th db -> DoubleMap(DB.get_map id db))

    (* Database udpate methods *)
    let get_update_value th db v = (get_eval v) th db
    let get_update_key th db kl = apply_fn_list th db kl

    let get_update_map c pats = match c with
        | TupleList(l) -> MC.from_list (List.map kv_of_tuple l) pats
        | SingleMap(m) -> List.fold_left MC.add_secondary_index m pats
        | _ -> failwith "invalid single_map_t" 

    (* persistent collection id, value -> update *)
    let update_value ?(expr = None) id value = Eval(fun th db ->
      let v = (get_update_value th db value) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nUPDATE '"^id^"'[-][-] := "^(K3Value.to_string v)
        );
        DB.update_value id v db; Unit)
    
    (* persistent collection id, in key, value -> update *)
    let update_in_map_value ?(expr = None) id in_kl value = Eval(fun th db ->
      let key = (get_update_key th db in_kl) in
      let v = (get_update_value th db value) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nUPDATE '"^id^"'["^
            (String.concat "; " (List.map K3Value.to_string key))^
            "][-] := "^(K3Value.to_string v)
        );
        DB.update_in_map_value id key v db; Unit)
    
    (* persistent collection id, out key, value -> update *)
    let update_out_map_value ?(expr = None) id out_kl value = Eval(fun th db ->
      let key = (get_update_key th db out_kl) in
      let v = (get_update_value th db value) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nUPDATE '"^id^"'[-]["^
           (String.concat "; " (List.map K3Value.to_string key))^
           "] := "^(K3Value.to_string v)
        );
        DB.update_out_map_value id key v db; Unit)
    
    (* persistent collection id, in key, out key, value -> update *)
    let update_map_value ?(expr = None) id in_kl out_kl value = 
      Eval(fun th db ->
      let in_key = (get_update_key th db in_kl) in
      let out_key = (get_update_key th db out_kl) in
      let v = (get_update_value th db value) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nUPDATE '"^id^"'["^
           (String.concat "; " (List.map K3Value.to_string in_key))^
           "]["^
           (String.concat "; " (List.map K3Value.to_string out_key))^
           "] := "^(K3Value.to_string v)
        );
        DB.update_map_value id in_key out_key v db; Unit)

    (* persistent collection id, update collection -> update *)
    let update_in_map ?(expr = None) id collection = Eval(fun th db ->
        let in_pats = DB.get_in_patterns id db
        in DB.update_in_map id
             (get_update_map ((get_eval collection) th db) in_pats) db;
        Unit)
    
    let update_out_map ?(expr = None) id collection = Eval(fun th db ->
        let out_pats = DB.get_out_patterns id db
        in DB.update_out_map id
             (get_update_map ((get_eval collection) th db) out_pats) db;
        Unit)

    (* persistent collection id, key, update collection -> update *)
    let update_map ?(expr = None) id in_kl collection = Eval(fun th db ->
        let out_pats = DB.get_out_patterns id db in
        DB.update_map id (get_update_key th db in_kl)
           (get_update_map ((get_eval collection) th db) out_pats) db;
        Unit)

    (* Database remove methods *)    
    (* persistent collection id, in key -> remove *)
    let remove_in_map_element ?(expr = None) id in_kl = Eval(fun th db ->
      let key = (get_update_key th db in_kl) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nREMOVE '"^id^"'["^
            (String.concat "; " (List.map K3Value.to_string key))^
            "][-]"
        );
        DB.remove_in_map_element id key db; Unit)
    
    (* persistent collection id, out key -> remove *)
    let remove_out_map_element ?(expr = None) id out_kl = Eval(fun th db ->
      let key = (get_update_key th db out_kl) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nREMOVE '"^id^"'[-]["^
           (String.concat "; " (List.map K3Value.to_string key))^
           "]"
        ); 
        DB.remove_out_map_element id key db; Unit)
  

    (* persistent collection id, in key, out key -> remove *)
    let remove_map_element ?(expr = None) id in_kl out_kl = 
      Eval(fun th db ->
      let in_key = (get_update_key th db in_kl) in
      let out_key = (get_update_key th db out_kl) in
        Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> 
           "\nREMOVE '"^id^"'["^
           (String.concat "; " (List.map K3Value.to_string in_key))^
           "]["^
           (String.concat "; " (List.map K3Value.to_string out_key))^
           "]"
        );
        DB.remove_map_element id in_key out_key db; Unit)  
    
    (*
    let ext_fn fn_id = failwith "External functions not yet supported"
    *)
    
    (* Top level code generation *)
    let trigger event stmt_block =
      let translate_tuple = 
         if Debug.active "HASH-STRINGS" then
            List.fold_right (fun (_,vart) translate_rest ->
               match vart with 
                  | TString -> (fun x -> 
                     (BaseValue(match List.hd x with
                        | CString(s) -> CInt(Hashtbl.hash s)
                        | _ -> failwith "Expecting a string as input"
                     ))::(translate_rest (List.tl x)))
                  | _ -> (fun x -> (BaseValue(List.hd x))::
                                       (translate_rest (List.tl x)))
            ) (Schema.event_vars event) (fun _ -> [])
         else
            (List.map (fun x -> BaseValue(x)))
      in
      Trigger (event, (fun tuple db -> 
        let theta = Env.make (List.map fst (Schema.event_vars event))
                             (translate_tuple tuple), [] in
          List.iter (fun cstmt -> 
            Debug.print "LOG-INTERPRETER-TRIGGERS" (fun () -> 
               "Processing trigger "^(Schema.string_of_event event));
            match (get_eval cstmt) theta db with
            | Unit -> ()
            | _ as d -> failwith 
               ("trigger "^(Schema.string_of_event event)^
                " returned non-unit datum: "^
                  (Values.K3Value.string_of_value d)
               )) stmt_block))

    (* Sources, for now files only. *)
    type source_impl_t = FileSource.t

    let source src (rel_adaptors : (Schema.adaptor_t * Schema.rel_t) list) =
       let src_impl = match src with
           | Schema.FileSource(_) ->
               FileSource.create src 
               (List.map (fun (adaptor, rel) -> 
                   (rel, (Adaptors.create_adaptor adaptor))) rel_adaptors)
               (ListExtras.ocaml_of_list (fun (_,x) -> Schema.string_of_rel x) 
                                         rel_adaptors)
           | Schema.SocketSource(_) -> failwith "Sockets not yet implemented."
           | Schema.PipeSource(_)   -> failwith "Pipes not yet implemented."
           | Schema.NoSource        -> failwith "Unsourced relation"
       in (src_impl, None, None)

    let main dbschema schema patterns sources triggers toplevel_queries =
      Main (fun () ->
        Random.init(12345);
        let db = DB.make_empty_db schema patterns in
        let trigger_exec = 
          List.fold_left 
            (fun trigger_exec trig -> (get_trigger trig)::trigger_exec)
            [] triggers
        in
        let dispatcher =
          (fun evt -> 
            Debug.exec "STEP-INTERPRETER" (fun () ->
               match evt with None -> ()
               | Some(event, tuple) -> 
                  print_endline ((DB.db_to_string db)^"\n\n");
                  print_string ((Schema.string_of_event event)^
                                " <- ["^(String.concat "; " 
                                   (List.map Types.string_of_const tuple)
                                )^"]");
                  ignore (read_line ());
            );
            match evt with 
            | Some(event, tuple) when List.mem_assoc event trigger_exec ->
               (List.assoc event trigger_exec) tuple db; true
            | Some _  | None -> false)
        in
        let mux = List.fold_left
          (fun m (source,_,_) -> FileMultiplexer.add_stream m source)
          (FileMultiplexer.create ()) sources
        in
        let db_tlq = List.map DB.string_to_map_name toplevel_queries in
          synch_main db mux db_tlq dispatcher (RT.default_args ()) ())

    (* For the interpreter, output evaluates the main function and redirects
     * stdout to the desired channel. *)
    let output main out_chan = match main with
      | Main(main_f) ->
          Unix.dup2 Unix.stdout (Unix.descr_of_out_channel out_chan); main_f ()
      | _ -> failwith "invalid M3 interpreter main code"
      
   let to_string (code:code_t): string =
      failwith "Interpreter can't output a string"

   let debug_string (code:code_t): string =
      failwith "Interpreter can't output a string"
 
    let rec eval c vars vals db = match c with
      | Eval(f) -> f (Env.make vars (List.map value_of_const_t vals), []) db
      | Trigger(evt,trig_fn) -> trig_fn vals db; Unit
      | Main(f) -> f(); Unit
end