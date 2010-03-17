open M3Common

(* (PARTIAL) VARIABLE VALUATIONS *)
module type ValuationSig =
sig
   type key = string
   type t
   
   val make : var_t list -> const_t list -> t
   val vars : t -> string list
   val bound : string -> t -> bool
   val value : string -> t -> const_t
   
   val consistent : t -> t -> bool
   val extend : t -> t -> var_t list -> t
   val apply : t -> key list -> const_t list
   val to_string : t -> string
end

module Valuation : ValuationSig =
struct
   (* the keys are variable names *)
   module StringMap = Map.Make (String)

   type key = StringMap.key
   type t = const_t StringMap.t

   (* Note: ordered result *)
   let make (vars: var_t list) (values: const_t list) : t =
      List.fold_left (fun acc (k,v) -> StringMap.add k v acc)
         StringMap.empty (List.combine vars values)

   (* Note: ordered result *)
   let to_list m = StringMap.fold (fun s n l -> (s,n)::l) m []

   let vars theta = StringMap.fold (fun k _ acc -> k::acc) theta []

   let bound var theta = StringMap.mem var theta
   
   let value var theta = StringMap.find var theta

   let consistent (m1: t) (m2: t) : bool =
      List.for_all (fun (k,v) ->
        (not(StringMap.mem k m2)) || ((StringMap.find k m2) = v))
        (to_list m1)

   (* extends m1 by given vars from m2.
    * assumes that m1 and m2 are consistent. *)
   (* Note: ordered result *)
   let extend (m1: t) (m2: t) (ext : var_t list) : t =
      List.fold_left (fun acc k -> StringMap.add k (value k m2) acc) m1 ext

   let apply (m: t) (l: key list) = List.map (fun x -> StringMap.find x m) l

   let to_string (theta:t) : string =
      StringMap.fold (fun k v acc -> acc^(if acc = "" then "" else " ")^
         k^"->"^(string_of_const v)) theta ""
end

(* Map whose keys are valuations, and values are polymorphic *)
module ValuationMap = SliceableMap.Make(struct
   type t = const_t
   let to_string = string_of_const
end)

(* ValuationMap whose values are aggregates *)
module AggregateMap =
struct
   module VM = ValuationMap

   type agg_t = const_t
   
   type key = VM.key
   type t = agg_t VM.t

   let string_of_aggregate = string_of_const

   (* Slice methods for calculus evaluation *)

   (* Indexed aggregate, iterates over all partial keys given by pattern,
    * and aggregate over key-value pairs corresponding to each partial key.
    * assumes add has replace semantics, so that fold & add removes duplicates.
    * returns a new map, does not modify in-place. *)
   let indexed_aggregate aggf pat m =
      let aux pk kv nm =
         let (nk,nv) = aggf pk kv in VM.add nk nv nm
      in VM.fold_index aux pat m (VM.empty_map())
      
   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let concat_keys (current_outv: var_t list) (desired_outv: var_t list)
         (theta: Valuation.t) (extensions: var_t list) (m: t) =
      let key_refiner k v =
         let ext_theta = Valuation.extend
            (Valuation.make current_outv k) theta extensions in
         (* normalizes the key orderings to that of the variable ordering outv. *)
         let new_k = Valuation.apply ext_theta desired_outv in
         (new_k, v)
   in VM.mapi key_refiner m

   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let project_keys (pat: ValuationMap.pattern) (pat_outv: var_t list)
        (desired_outv: var_t list) (theta: Valuation.t)
        (extensions: var_t list) (m: t) =
      let aux pk kv =
         let aggv = List.fold_left (fun x (y,z) -> c_sum x z) (CFloat(0.0)) kv in
         let ext_theta =
            Valuation.extend (Valuation.make pat_outv pk) theta extensions in
         (* Normalizes the key orderings to that of the variable ordering desired_outv.
          * This only performs reordering, that is we assume pat_outv has the
          * same set of variables as desired_outv. *)
         let new_k = Valuation.apply ext_theta desired_outv in
            (new_k, aggv)
      in
         (* in the presence of bigsum variables, there may be duplicates
          * after key_refinement. These have to be summed up using aggregate. *)
         indexed_aggregate aux pat m

   (* filters slice entries to those consistent in terms of given variables, and theta *) 
   let filter (outv: var_t list) (theta: Valuation.t) (m: t) : t =
      let aux k v = (Valuation.consistent theta (Valuation.make outv k)) in
         VM.filteri aux m

end

module Database =
struct
   open Patterns

   module VM  = ValuationMap
   module AM  = AggregateMap
   module DBM = SliceableMap.Make(struct type t = string let to_string x = x end)
   
   type slice_t = AM.t
   type dbmap_t = AM.t VM.t
   type db_t    = dbmap_t DBM.t

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string string_of_const k

   let slice_to_string (m: slice_t) =
      VM.to_string key_to_string AM.string_of_aggregate m

   let dbmap_to_string (m: dbmap_t) =
      VM.to_string key_to_string slice_to_string m

   let db_to_string (db: db_t) : string =
      DBM.to_string (fun x -> String.concat "," x) dbmap_to_string db

   let get_patterns patterns mapn =
      (get_in_patterns patterns mapn, get_out_patterns patterns mapn)

   let make_empty_db schema patterns: db_t =
        let f (mapn, itypes, _) =
           let (in_patterns, out_patterns) = get_patterns patterns mapn in
            ([mapn], (if(List.length itypes = 0) then
                VM.from_list [([], VM.from_list [] out_patterns)] in_patterns
                else
                   (* We defer adding out var indexes to init value computation *)
                   VM.empty_map_w_patterns in_patterns))
        in
            DBM.from_list (List.map f schema) []
    
    
   let make_empty_db_wdom schema patterns (dom: const_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let (in_patterns, out_patterns) = get_patterns patterns mapn in
            let map = VM.from_list
                        (List.map (fun t -> (t, VM.empty_map_w_patterns out_patterns))
                            (Util.k_tuples (List.length itypes) dom)) in_patterns
            in
            ([mapn], map)
        in
            DBM.from_list (List.map f schema) []

   let update mapn inv_imgs slice db : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (VM.add inv_imgs slice m) db);
(*
         let string_of_img = Util.list_to_string string_of_const in
         print_endline ("Updated the database: "^mapn^
                      " inv="^(string_of_img inv_imgs)^
                      " slice="^(slice_to_string slice)^
                      " db="^(db_to_string db));
         VM.validate_indexes (VM.find inv_imgs (DBM.find [mapn] db))
*)
      with Failure x -> failwith ("update: "^x)
              
   let update_value mapn patterns inv_img outv_img new_value db : unit =
      try
         let m = DBM.find [mapn] db in
         let new_slice = if VM.mem inv_img m
            then VM.add outv_img new_value (VM.find inv_img m) 
            else VM.from_list [(outv_img, new_value)] patterns
         in
            ignore(DBM.add [mapn] (VM.add inv_img new_slice m) db);
(*
            let string_of_img = Util.list_to_string string_of_const in
            print_endline ("Updating a db value: "^mapn^
                           " inv="^(string_of_img inv_img)^
                           " outv="^(string_of_img outv_img)^
                           " v="^(string_of_const new_value)^
                           " db="^(db_to_string db));
            VM.validate_indexes (VM.find inv_img (DBM.find [mapn] db))
*)
       with Failure x -> failwith ("update_value: "^x)
            
   let get_map mapn db = 
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_map "^mapn)

   let showmap (m: dbmap_t) = VM.to_list (VM.map VM.to_list m)
   
   let show_sorted_map (m: dbmap_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))

    let showdb_f show_f db =
       DBM.fold (fun k v acc -> acc@[(k, (show_f v))]) [] db
    
    let showdb db = showdb_f showmap db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map db)
end


(* Simple OCaml interpreter generator *)
module CG : M3Codegen.CG with type db_t = Database.db_t =
struct

type singleton_code =
   (Valuation.t -> Database.db_t -> AggregateMap.agg_t list)

type slice_code = (Valuation.t -> Database.db_t -> AggregateMap.t)

type singleton_value_function_code =
  (Valuation.t -> Database.db_t ->
     AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t)
     
type slice_value_function_code =
   (Valuation.t -> Database.db_t -> AggregateMap.t -> AggregateMap.agg_t)

type singleton_update_code =
   (Valuation.t -> AggregateMap.agg_t list ->
      Database.db_t -> AggregateMap.agg_t list)

type slice_update_code =
   (Valuation.t -> AggregateMap.t -> Database.db_t -> AggregateMap.t)

type db_update_code =
   (Valuation.t -> Database.db_t -> ValuationMap.key ->
      AggregateMap.t -> unit)

type compiled_stmt = (Valuation.t -> Database.db_t -> unit)

type compiled_trigger = (const_t list -> Database.db_t -> unit)

type compiled_code =
     Singleton              of singleton_code
   | Slice                  of slice_code
   | SingletonValueFunction of singleton_value_function_code
   | SliceValueFunction     of slice_value_function_code
   | UpdateSingleton        of singleton_update_code
   | UpdateSlice            of slice_update_code
   | DatabaseUpdate         of db_update_code
   | Statement              of compiled_stmt 
   | Trigger                of compiled_trigger              

type env_debug_code = (Valuation.t -> Database.db_t -> unit)
type slice_debug_code = (Valuation.t -> Database.db_t -> AggregateMap.t -> unit)
type singleton_debug_code = (Valuation.t -> Database.db_t -> AggregateMap.key -> AggregateMap.agg_t -> unit)

type compiled_debug_code = 
     EnvDebug        of env_debug_code 
   | SliceDebug      of slice_debug_code
   | SingletonDebug  of singleton_debug_code

(* External type names *)
type op_t = const_t -> const_t -> const_t
type code_t = compiled_code
type debug_code_t = compiled_debug_code
type db_t = Database.db_t

let get_singleton_code x =
   match x with | Singleton(c) -> c | _ -> failwith "invalid singleton code"

let get_slice_code x =
   match x with | Slice(c) -> c | _ -> failwith "invalid slice code"

let get_singleton_value_function_code x =
   match x with | SingletonValueFunction(c) -> c | _ -> failwith "invalid singleton function code"

let get_slice_value_function_code x =
   match x with | SliceValueFunction(c) -> c | _ -> failwith "invalid slice function code"

let get_update_singleton_code x =
   match x with | UpdateSingleton(c) -> c | _ -> failwith "invalid singleton code"

let get_update_slice_code x =
   match x with | UpdateSlice(c) -> c | _ -> failwith "invalid slice code"

let get_database_update_code x =
   match x with | DatabaseUpdate(c) -> c | _ -> failwith "invalid db update code"

let get_statement x =
   match x with | Statement(c) -> c | _ -> failwith "invalid statement"

let get_trigger x =
   match x with | Trigger(c) -> c | _ -> failwith "invalid statement"

(* Debug code accessors *)
let get_env_debug_code x = 
   match x with | EnvDebug(c) -> c | _ -> failwith "invalid env debug code"

let get_slice_debug_code x =
   match x with | SliceDebug(c) -> c | _ -> failwith "invalid slice debug code"

let get_singleton_debug_code x =
   match x with | SingletonDebug(c) -> c | _ -> failwith "invalid singleton debug code"

(* Debugging helpers *)
let debug_sequence cdebug ccalc =
   let df = get_env_debug_code cdebug in
   match ccalc with
    | Singleton(f) ->
       Singleton (fun theta db -> (* df theta db; *) f theta db)
    | Slice(f) -> Slice (fun theta db -> (* df theta db; *) f theta db)
    | _ -> failwith "invalid expr debug sequence"

let debug_expr incr_calc =
   EnvDebug (fun theta db ->
      print_string("\neval_pcalc "^(pcalc_to_string incr_calc)^
                   " "^(Valuation.to_string theta)^
                   " "^(Database.db_to_string db)^"   "))

let debug_singleton_rhs_expr lhs_outv =
   SingletonDebug (fun theta db k v ->
      print_endline ("End of PCALC_S; outv="^(vars_to_string lhs_outv)^
                     " k="^(Util.list_to_string string_of_const k)^
                     " v="^(string_of_const v)^
                    (" db="^(Database.db_to_string db))^
                    (" theta="^(Valuation.to_string theta))))

let debug_slice_rhs_expr rhs_outv =
   SliceDebug (fun theta db slice0 ->
      print_endline ("End of PCALC; outv="^(vars_to_string rhs_outv)^
                     " slice="^(Database.slice_to_string slice0)^
                    (" db="^(Database.db_to_string db))^
                    (" theta="^(Valuation.to_string theta))))

let debug_rhs_init () =
   SingletonDebug (fun theta db k v ->
      print_string ("@PSTMT.init{th="^(Valuation.to_string theta)
                   ^", key="^(Util.list_to_string string_of_const k)
                   ^", db="^(Database.db_to_string db)))

let debug_stmt lhs_mapn lhs_inv lhs_outv =
   EnvDebug (fun theta db ->
      print_string("\nPSTMT (th="^(Valuation.to_string theta)
                  ^", db=_, stmt=(("^lhs_mapn^" "^
                     (Util.list_to_string (fun x->x) lhs_inv)^" "
                  ^(Util.list_to_string (fun x->x) lhs_outv)^" _), _))\n"))

let const i = Singleton (fun theta db -> [i])
let singleton_var x =
   Singleton (fun theta db ->
      if (not (Valuation.bound x theta)) then
         failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
      else let v = Valuation.value x theta in [v])
      
let slice_var x =
   Slice (fun theta db ->
      if (not (Valuation.bound x theta)) then
         failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
      else let v = Valuation.value x theta in
         ValuationMap.from_list [([v], v)] [])

let null () = Singleton (fun theta db -> [])

(* Operators *)
let int_op op x y = if (op x y) then M3.CFloat(1.0) else M3.CFloat(0.0)

let add_op  = c_sum
let mult_op = c_prod
let lt_op   = (int_op (< ))
let leq_op  = (int_op (<=))
let eq_op   = (int_op (= ))

let ifthenelse0_op v cond =
   (match cond with
    | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0))
   

(* Op expressions *)
let op_singleton_expr op ce1 ce2  =
   let (ce1_i, ce2_i) = (get_singleton_code ce1, get_singleton_code ce2) in
   Singleton (fun theta db ->
      (* Since there are no bigsum vars, we don't need to evaluate RHS
       * with an extended theta *)
      let (r1,r2) = (ce1_i theta db, ce2_i theta db) in
         match (r1,r2) with
          | ([], _) | (_,[]) -> []
          | ([v1], [v2]) -> [op v1 v2]
          | _ -> failwith "compile_op: invalid singleton")
   
(* op, outv1, outv2, schema, theta_ext, schema_ext, lhs code, rhs code ->
 * op expr code *)
let op_slice_expr op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
   let (ce1_l, ce2_l) = (get_slice_code ce1, get_slice_code ce2) in
   Slice (fun theta db ->
      let res1 = ce1_l theta db in
      let f k v1 r =
         (* extend with out vars from LHS calc. This is for bigsum vars in
          * IfThenElse0, so that these bigsum vars can be used as in vars
          * for map lookups. *)
         let th = Valuation.extend theta (Valuation.make outv1 k) theta_ext in
         let r2 = ce2_l th db in
         (* perform cross product, extend out vars (slice key) to schema *)
         (* We can exploit schema monotonicity, and uniqueness of
          * map keys, implying this call to extend_keys will never do any
          * aggregation and can be simplified to key concatenation
          * and reindexing. *)
         let r3 = AggregateMap.concat_keys outv2 schema th schema_ext
            (ValuationMap.map (fun v2 -> op v1 v2) r2)
         in
            (* r, r3 have no overlap -- safe to union slices.
             * This will also union any secondary indexes. *)
            ValuationMap.union r r3
      in
         ValuationMap.fold f (ValuationMap.empty_map()) res1)

let op_slice_product_expr op ce1 ce2 =
   let (ce1_l, ce2_l) = (get_slice_code ce1, get_slice_code ce2) in
   Slice (fun theta db ->
      let res1 = ce1_l theta db in
      let res2 = ce2_l theta db
      in ValuationMap.product op res1 res2)

(* op, outv1, outv2, schema, theta_ext, schema_ext, lhs code, rhs code ->
 * op expr code *)
let op_lslice_expr op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
   let (ce1_l, ce2_i) = (get_slice_code ce1, get_singleton_code ce2) in
   Slice (fun theta db ->
      let res1 = ce1_l theta db in
      let th2 = Valuation.make outv2 (Valuation.apply theta outv2) in
      let f k v r =
        let th1 = Valuation.extend theta (Valuation.make outv1 k) theta_ext in
        begin match (ce2_i th1 db) with
         | [] -> r
         | [v2] ->
            let nv = op v v2 in
            let nk = Valuation.apply
               (Valuation.extend th2 th1 schema_ext) schema
            in ValuationMap.add nk nv r
         | _ -> failwith "compile_op: invalid singleton"
        end
      in ValuationMap.fold f (ValuationMap.empty_map()) res1) 

(* op, outv2, lhs code, rhs code -> op expr code *)
let op_lslice_product_expr op outv2 ce1 ce2 =
   let (ce1_l, ce2_i) = (get_slice_code ce1, get_singleton_code ce2) in
   Slice (fun theta db ->
      let res1 = ce1_l theta db in
      let k2 = Valuation.apply theta outv2 in
         begin match (ce2_i theta db) with
          | [] -> ValuationMap.empty_map()
          | [v2] -> ValuationMap.mapi (fun k v -> (k@k2, op v v2)) res1
          | _ -> failwith "compile_op: invalid singleton"
         end)

(* op, outv2, schema, schema_ext, lhs code, rhs code -> op expr code *)
let op_rslice_expr op outv2 schema schema_ext ce1 ce2 =
   let (ce1_i, ce2_l) = (get_singleton_code ce1, get_slice_code ce2) in
   Slice (fun theta db ->
      match ce1_i theta db with
       | [] -> ValuationMap.empty_map()
       | [v] ->
         let r = ce2_l theta db in
            AggregateMap.concat_keys outv2 schema theta schema_ext
               (ValuationMap.map (fun v2 -> op v v2) r)
       | _ -> failwith "compile_op: invalid singleton")

(* TODO: this always returns a slice *)
(* mapn, out_patterns, outv, init rhs expr -> init lookup code *)
let singleton_init_lookup mapn inv out_patterns outv cinit =
   let cinit_i = get_singleton_code cinit in
      Slice (fun theta db ->
       let inv_img = Valuation.apply theta inv in
       let init_val = cinit_i theta db in
       let outv_img = Valuation.apply theta outv in
       begin match init_val with
        | [] -> ValuationMap.empty_map()
        | [v] -> (Database.update_value
                  mapn out_patterns inv_img outv_img v db;
                ValuationMap.from_list [(outv_img, v)] out_patterns)
        | _ -> failwith "MapAccess: invalid singleton"
       end)
   
(* mapn, out_patterns, init rhs expr -> init lookup code *)
let slice_init_lookup mapn inv out_patterns cinit =
   let cinit_l = get_slice_code cinit in
      Slice (fun theta db ->
       let inv_img = Valuation.apply theta inv in
       let init_slice = cinit_l theta db in
       let init_slice_w_indexes = List.fold_left
          ValuationMap.add_secondary_index init_slice out_patterns
       in
          Database.update mapn inv_img init_slice_w_indexes db;
          init_slice_w_indexes)

(* mapn, inv, outv, init lookup code -> map lookup code *)
let singleton_lookup mapn inv outv init_val_code =
   let ivc_l = get_slice_code init_val_code in
   Singleton (fun theta db ->
      let slice = 
         let m = Database.get_map mapn db in
         let inv_img = Valuation.apply theta inv in
            if ValuationMap.mem inv_img m then
               ValuationMap.find inv_img m
            else ivc_l theta db in
      let outv_img = Valuation.apply theta outv in
         if ValuationMap.mem outv_img slice then
            [ValuationMap.find outv_img slice]
         else [])
   
(* mapn, inv, pat, patv, init lookup code -> map lookup code *)
let slice_lookup mapn inv pat patv init_val_code =
   let ivc_l = get_slice_code init_val_code in
   Slice (fun theta db ->
      let slice =
         let m = Database.get_map mapn db in
         let inv_img = Valuation.apply theta inv in
            if ValuationMap.mem inv_img m then
               ValuationMap.find inv_img m
            else ivc_l theta db in
      (* This can be a slice access for outv not in theta *)
      (* We must remove secondary indexes from lookups during calculus, 
       * evaluation, since we don't want them propagated around. *)
      let pkey = Valuation.apply theta patv in
      let lookup_slice = ValuationMap.slice pat pkey slice in
         (ValuationMap.strip_indexes lookup_slice)) 


(* m3 expr code -> m3 rhs expr code *)
let singleton_expr ccalc cdebug =
   let ccalc_i = get_singleton_code ccalc in
   let cdebug_i = get_singleton_debug_code cdebug in
   Singleton (fun theta db ->
      let r = ccalc_i theta db in
      match r with
       | [] -> []
       | [v] -> (* cdebug_i theta db [] v; *) [v] 
       | _ -> failwith "compile_pcalc2_singleton: invalid singleton")

(* m3 expr code, debug code -> m3 rhs expr code *)
let direct_slice_expr ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   let cdebug_l = get_slice_debug_code cdebug in
      Slice (fun theta db -> let r = ccalc_l theta db
             in (* cdebug_l theta db r; *) r)

(* TODO: this does not capture change from slice to singleton *)
(* m3 expr code -> m3 rhs expr code *)
let full_agg_slice_expr ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   let ccdebug_l = get_slice_debug_code cdebug in
      Singleton (fun theta db ->
         let slice0 = ccalc_l theta db in
         (* cdebug_l theta db slice0; *)
         [ValuationMap.fold (fun k v acc -> c_sum acc v)
                            (CFloat(0.0)) slice0])

(* rhs_pattern, rhs_projection, lhs_outv, rhs_ext, m3 expr code -> m3 rhs expr code *)
let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   let cdebug_l = get_slice_debug_code cdebug in
      Slice (fun theta db ->
         let slice0 = ccalc_l theta db in
         (* Note this aggregates bigsum vars present in the RHS map.
          * We use an indexed aggregation, however, since, slice0 will not
          * actually have any secondary indexes (we strip them during calculus
          * evaluation), we build an index with the necessary pattern here. *)
         (* cdebug_l theta db slice0; *)
         let slice1 = ValuationMap.add_secondary_index slice0 rhs_pattern in
            AggregateMap.project_keys rhs_pattern rhs_projection lhs_outv
               theta rhs_ext slice1)

(* Initial value computation for statements *) 
(* TODO: these compute a single value, and should be reflected in code_t *)

(* init calc code, debug code -> init code *)
let singleton_init cinit cdebug =
   let cinitf = get_singleton_code cinit in
   let cdebug_i = get_singleton_debug_code cdebug in
      SingletonValueFunction (fun theta db _ v ->
         (* cdebug_i theta2 db [] v; *)
         (match cinitf theta db with
          | [] -> v
          | [init_v] -> c_sum v init_v
          | _ -> failwith "invalid init singleton" ))

(* lhs_outv, init_ext, init calc code, debug code -> init code *)
let slice_init lhs_outv init_ext cinit cdebug =
   let cinitf = get_slice_code cinit in
   let cdebug_i = get_singleton_debug_code cdebug in 
      SingletonValueFunction (fun theta db k v ->
         let theta2 =
            Valuation.extend theta (Valuation.make lhs_outv k) init_ext in
         (* cdebug_i theta2 db k v; *)
         let init_slice = cinitf theta2 db in
         let init_v =
            if ValuationMap.mem k init_slice
            then (ValuationMap.find k init_slice) else CFloat(0.0)
         in c_sum v init_v)

(* Incremental statement evaluation *)
   
(* TODO: the generated code here takes a slice/singleton argument
 * and this is not reflected by the return type *)
(* lhs_outv, incr_m3 code, init value code, debug code -> update code *)
let singleton_update_aux f lhs_outv cincr init_value_code cdebug =
   let cincrf = get_singleton_code cincr in
   let cinitf = f init_value_code in
   let cdebug_e = get_env_debug_code cdebug in
   UpdateSingleton (fun theta current_singleton db ->
      (* cdebug_e theta db; *)
      let delta_slice = cincrf theta db in
         match (current_singleton, delta_slice) with
          | (s, []) -> s
          | ([], [delta_v]) ->
            let delta_k = Valuation.apply theta lhs_outv in
               [cinitf theta db delta_k delta_v]
          | ([current_v], [delta_v]) -> [c_sum current_v delta_v]
          | _ -> failwith "invalid singleton update")

let singleton_update = singleton_update_aux get_singleton_value_function_code

(* incr_m3 code, init value code, debug code -> update code *) 
let slice_update_aux f cincr init_value_code cdebug =
   let cincrf = get_slice_code cincr in
   let cinitf = f init_value_code in
   let cdebug_e = get_env_debug_code cdebug in
   UpdateSlice (fun theta current_slice db ->
      (* cdebug_e theta db; *)
      let delta_slice = cincrf theta db in
         ValuationMap.merge_rk (fun k v -> v) (cinitf theta db)
            (fun k v1 v2 -> c_sum v1 v2) current_slice delta_slice)

let slice_update = slice_update_aux get_singleton_value_function_code

(* Incremental update code generation *)
(* TODO: the generated code takes a inv_img and an inv slice, and returns unit.
 * This should be reflected in the resulting code type. *)

(* lhs_mapn, lhs_outv, map out patterns, singleton eval code -> db update code*)
let db_singleton_update lhs_mapn lhs_outv map_out_patterns cstmt =
   let cstmtf = get_update_singleton_code cstmt in
      DatabaseUpdate (fun theta db inv_img in_slice ->
         let outv_img = Valuation.apply theta lhs_outv in
         let singleton = 
            if ValuationMap.mem outv_img in_slice then
               [ValuationMap.find outv_img in_slice]
            else [] in
         let new_value = cstmtf theta singleton db in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value lhs_mapn map_out_patterns
                     inv_img outv_img v db
          | _ -> failwith "compile_pstmt_loop: invalid singleton")

(* TODO: partitioning metadata *)
(* lhs_mapn, slice eval code -> db update code *)
let db_slice_update lhs_mapn cstmt =
   let cstmtf = get_update_slice_code cstmt in 
   DatabaseUpdate (fun theta db inv_img in_slice ->
      (* Subslice based on bound out vars *)
      (* Partitioning to work slice is not cheaper for now, since
       * creating the unchanged partition creates a new slice, which
       * is essentially the same as in merge *)
      (*
      let out_pkey = Valuation.apply theta out_patv in
      let (work_slice, unchanged_slice) =
         ValuationMap.partition_slice out_pat out_pkey in_slice in
      let new_slice = ValuationMap.union
         unchanged_slice (cstmtf theta work_slice db)
      *)
      let new_slice = cstmtf theta in_slice db
      in Database.update lhs_mapn inv_img new_slice db)


(* Top-level M3 program structure *)
(* TODO: this code returns unit, and should be reflected in resulting code_t *)
(* lhs_mapn, lhs_ext, patv, pat, direct, db update code -> statement code *)
let statement lhs_mapn lhs_inv lhs_ext patv pat direct db_update_code =
   let db_f = get_database_update_code db_update_code in
   Statement (fun theta db ->
      let lhs_map = Database.get_map lhs_mapn db in
      let iifilter db inv_img =
        let inv_theta = Valuation.make lhs_inv inv_img in
        let new_theta = Valuation.extend theta inv_theta lhs_ext in
        let slice = ValuationMap.find inv_img lhs_map in
           db_f new_theta db inv_img slice       
      in
      let pkey = Valuation.apply theta patv in
      let inv_imgs = if direct then [pkey]
                     else ValuationMap.slice_keys pat pkey lhs_map
      in List.iter (iifilter db) inv_imgs)

(* TODO: this code expects a tuple arg, returns unit, and should be reflected
 * in resulting code_t *)
(* trigger args, statement code block -> trigger code *)
let trigger trig_args stmt_block =
   let cblock = List.map get_statement stmt_block in
   Trigger (fun tuple db ->
      let theta = Valuation.make trig_args tuple in
         List.iter (fun cstmt -> cstmt theta db) cblock)

let eval_trigger trigger tuple db = (get_trigger trigger) tuple db

end
