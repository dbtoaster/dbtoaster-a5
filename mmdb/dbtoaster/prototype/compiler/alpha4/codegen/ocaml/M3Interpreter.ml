open M3
open M3Common
open M3OCaml
open M3OCaml.Adaptors

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
   | Toplevel               of (unit -> unit)

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
   (*let df = get_env_debug_code cdebug in*)
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
       print_endline ("singleton_init_lookup: "^mapn^" "^
                      (vars_to_string inv)^" "^
                      (vars_to_string outv)^" "^
                      (Util.list_to_string string_of_const outv_img));
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
       print_endline ("slice_init_lookup: "^mapn^" "^
                      (vars_to_string inv));
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
   
let clean_var_name var = var

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
   (*let cdebug_i = get_singleton_debug_code cdebug in*)
   Singleton (fun theta db ->
      let r = ccalc_i theta db in
      match r with
       | [] -> []
       | [v] -> (* cdebug_i theta db [] v; *) [v] 
       | _ -> failwith "compile_pcalc2_singleton: invalid singleton")

(* m3 expr code, debug code -> m3 rhs expr code *)
let direct_slice_expr ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   (*let cdebug_l = get_slice_debug_code cdebug in*)
      Slice (fun theta db -> let r = ccalc_l theta db
             in (* cdebug_l theta db r; *) r)

(* TODO: this does not capture change from slice to singleton *)
(* m3 expr code -> m3 rhs expr code *)
let full_agg_slice_expr ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   (*let ccdebug_l = get_slice_debug_code cdebug in*)
      Singleton (fun theta db ->
         let slice0 = ccalc_l theta db in
         (* cdebug_l theta db slice0; *)
         [ValuationMap.fold (fun k v acc -> c_sum acc v)
                            (CFloat(0.0)) slice0])

(* rhs_pattern, rhs_projection, lhs_outv, rhs_ext, m3 expr code -> m3 rhs expr code *)
let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   (*let cdebug_l = get_slice_debug_code cdebug in*)
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
   (*let cdebug_i = get_singleton_debug_code cdebug in*)
      SingletonValueFunction (fun theta db _ v ->
         (* cdebug_i theta2 db [] v; *)
         (match cinitf theta db with
          | [] -> v
          | [init_v] -> c_sum v init_v
          | _ -> failwith "invalid init singleton" ))

(* lhs_outv, init_ext, init calc code, debug code -> init code *)
let slice_init lhs_outv init_ext cinit cdebug =
   let cinitf = get_slice_code cinit in
   (*let cdebug_i = get_singleton_debug_code cdebug in *)
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
   (*let cdebug_e = get_env_debug_code cdebug in*)
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
   (*let cdebug_e = get_env_debug_code cdebug in*)
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
let trigger event rel trig_args stmt_block =
   let cblock = List.map get_statement stmt_block in
   Trigger (fun tuple db ->
      let theta = Valuation.make trig_args tuple in
         List.iter (fun cstmt -> cstmt theta db) cblock)

(* No sources for interpreter *)
type source_impl_t = File of in_channel * adaptor list
   | Socket of Unix.file_descr * adaptor list

let source src framing (rel_adaptors : (string * adaptor_t) list) =
   let src_impl = match src with
    | FileSource(fn) ->
       let adaptor_impls = List.map (fun (r,a) -> create_adaptor a) rel_adaptors
       in File(open_in fn, adaptor_impls)
       
    | SocketSource(_) -> failwith "Sockets not yet implemented."
   in (src_impl, None, None)

(* No top level code generated for the interpreter *)
let main schema patterns sources triggers =
   failwith "interpreter should be directly invoked"

(* No file output for interpreter *)
let output out_chan = failwith "interpreter cannot write source code"

let eval_trigger trigger tuple db = (get_trigger trigger) tuple db

end
