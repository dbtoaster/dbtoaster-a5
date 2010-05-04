(* M3 Interpreter.
 * A backend that interprets M3 programs.
 * -- internal code generation functions produce lambdas to perform evaluation.
 * -- implements eval_trigger, invoking lambda produced for a trigger.
 * -- no main function to produce source code.
 * -- uses lib/ocaml/*, an OCaml library with standard data structures for maps,
 *    valuations (i.e. environments/scopes) and databases.
 * TODO: describe architectural overview of M3 evaluation here.
*)

open Util
open M3
open Expression
open Database
open Sources
open Sources.Adaptors

(* Simple OCaml interpreter generator *)
module CG : M3Codegen.CG with type db_t = Database.db_t =
struct

type singleton_code =
   (Valuation.t -> Database.db_t -> AggregateMap.agg_t)

type slice_code = (Valuation.t -> Database.db_t -> AggregateMap.t)

type singleton_value_function_code =
  (Valuation.t -> Database.db_t ->
     AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t)
     
type slice_value_function_code =
   (Valuation.t -> Database.db_t -> AggregateMap.t -> AggregateMap.agg_t)

type singleton_update_code =
   (Valuation.t -> Database.db_t -> AggregateMap.agg_t list -> AggregateMap.agg_t)

type slice_update_code =
   (Valuation.t -> Database.db_t -> AggregateMap.t -> AggregateMap.t)

type db_update_code =
   (Valuation.t -> Database.db_t -> ValuationMap.key ->
      AggregateMap.t -> unit)

type compiled_stmt = (Valuation.t -> Database.db_t -> unit)

type compiled_trigger = 
    (pm_t * rel_id_t * (const_t list -> Database.db_t -> unit))

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
type singleton_result_debug_code = (Valuation.t -> Database.db_t -> AggregateMap.agg_t -> unit)

type compiled_debug_code = 
     EnvDebug        of env_debug_code 
   | SliceDebug      of slice_debug_code
   | SingletonDebug  of singleton_debug_code
   | SingletonResultDebug of singleton_result_debug_code

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
   match x with | Trigger(_,_,c) -> c | _ -> failwith "invalid statement"

let get_trigger_with_evt x = 
   match x with | Trigger(t) -> t | _ -> failwith "invalid statement"

(* Debug code accessors *)
let get_env_debug_code x = 
   match x with | EnvDebug(c) -> c | _ -> failwith "invalid env debug code"

let get_slice_debug_code x =
   match x with | SliceDebug(c) -> c | _ -> failwith "invalid slice debug code"

let get_singleton_debug_code x =
   match x with | SingletonDebug(c) -> c | _ -> failwith "invalid singleton debug code"

let get_singleton_result_debug_code x =
   match x with | SingletonResultDebug(c) -> c | _ -> failwith "invalid singleton result debug code"

(* Debugging helpers *)
let debug = false

let debug_sequence cdebug cresdebug ccalc =
   let df = get_env_debug_code cdebug in
   match ccalc with
    | Singleton(f) -> Singleton
       (fun theta db ->
          if debug then df theta db;
          let r = f theta db in
          if debug then (get_singleton_result_debug_code cresdebug) theta db r; r)
    | Slice(f) -> Slice
       (fun theta db ->
          if debug then df theta db;
          let r = f theta db in
          if debug then ((get_slice_debug_code cresdebug) theta db r); r)
    | _ -> failwith "invalid expr debug sequence"

let debug_expr (incr_calc:Prepared.calc_t) =
   EnvDebug (fun theta db ->
      print_endline("eval_pcalc "^(M3Common.code_of_calc incr_calc)^
                    " "^(Valuation.to_string theta)^
                    " "^(Database.db_to_string db)))

let debug_expr_result incr_calc ccalc =
   match ccalc with
    | Singleton(f) -> SingletonResultDebug(fun theta db v ->
       let v_str = AggregateMap.string_of_aggregate v in
       print_endline ("result ("^(M3Common.code_of_calc incr_calc)^"): S: "^v_str))

    | Slice(f) -> SliceDebug(fun theta db slice ->
          print_endline ("result ("^(M3Common.code_of_calc incr_calc)^
                        "): L: "^(Database.smap_to_string slice)))
    | _ -> failwith "invalid ccalc for result debugging"

let debug_singleton_rhs_expr lhs_outv =
   SingletonDebug (fun theta db k v ->
      print_endline ("End of PCALC_S; outv="^(M3Common.vars_to_string lhs_outv)^
                     " k="^(Util.list_to_string M3Common.string_of_const k)^
                     " v="^(M3Common.string_of_const v)^
                    (" db="^(Database.db_to_string db))^
                    (" theta="^(Valuation.to_string theta))))

let debug_slice_rhs_expr rhs_outv =
   SliceDebug (fun theta db slice0 ->
      print_endline ("End of PCALC; outv="^(M3Common.vars_to_string rhs_outv)^
                     " slice="^(Database.smap_to_string slice0)^
                    (" db="^(Database.db_to_string db))^
                    (" theta="^(Valuation.to_string theta))))

let debug_rhs_init () =
   SingletonDebug (fun theta db k v ->
      print_endline ("@PSTMT.init{th="^(Valuation.to_string theta)
                   ^", key="^(Util.list_to_string M3Common.string_of_const k)
                   ^", db="^(Database.db_to_string db)))

let debug_stmt lhs_mapn lhs_inv lhs_outv =
   EnvDebug (fun theta db ->
      print_endline("PSTMT (th="^(Valuation.to_string theta)^
                    ", db=_, stmt=(("^lhs_mapn^" "^
                    (Util.list_to_string (fun x->x) lhs_inv)^" "^
                    (Util.list_to_string (fun x->x) lhs_outv)^" _), _))"))

let debug_singleton_init_lookup mapn inv outv outv_img =
   print_endline ("singleton_init_lookup: "^mapn^" "^
                  (M3Common.vars_to_string inv)^" "^
                  (M3Common.vars_to_string outv)^" "^
                  (Util.list_to_string M3Common.string_of_const outv_img))

let debug_singleton_init_lookup_result r =
    print_endline ("singleton_init_lookup result: "^
                   (Database.smap_to_string r))

let debug_singleton_lookup outv_img lookup_slice =
   print_endline ("singleton_lookup: "^
                  (Util.list_to_string M3Common.string_of_const outv_img)^
                  " "^(Database.smap_to_string lookup_slice))

let debug_slice_init_lookup mapn inv =
    print_endline ("slice_init_lookup: "^mapn^
                   " "^(M3Common.vars_to_string inv))

let debug_slice_lookup patv pkey slice lookup_slice =
   print_endline ("slice_lookup: "^
                  (Util.list_to_string M3Common.string_of_const pkey)^
                  " "^(M3Common.vars_to_string patv)^
                  " "^(Database.smap_to_string slice)^
                  " "^(Database.smap_to_string lookup_slice))

let const i = Singleton (fun theta db -> i)
let singleton_var x =
   Singleton (fun theta db ->
      if (not (Valuation.bound x theta)) then
         failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
      else let v = Valuation.value x theta in v)
      
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

let ifthenelse0_op cond v =
   (match cond with
    | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0))

let ifthenelse0_bigsum_op v cond =
   (match cond with
    | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0))
   
(* Op expressions *)
let op_singleton_expr prebind op ce1 ce2  =
   let (ce1_i, ce2_i) = (get_singleton_code ce1, get_singleton_code ce2) in
   Singleton (fun theta db ->
      let th = Valuation.bind theta prebind in
      let (r1,r2) = (ce1_i th db, ce2_i th db)
      in op r1 r2)
   
(* op, outv1, outv2, schema, theta_ext, schema_ext, lhs code, rhs code ->
 * op expr code *)
let op_slice_expr prebind inbind op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
   let (ce1_l, ce2_l) = (get_slice_code ce1, get_slice_code ce2) in
   (* Note non-empty schema_ext is not sufficient for semijoin, as keys may
    * still require reordering to be done via key refinement *) 
   let semijoin = outv2 = schema in
   Slice (fun theta db ->
      let th = Valuation.bind theta prebind in
      let res1 = ce1_l th db in
      let f k v1 r =
         (* extend with out vars from LHS calc. This is for bigsum vars in
          * IfThenElse0, so that these bigsum vars can be used as in vars
          * for map lookups. *)
         let th2 =
            let tmp_th = Valuation.extend th (Valuation.make outv1 k)
               (Util.ListAsSet.union theta_ext schema_ext) in
            let (bvars,bvals) = 
               List.split (List.map (fun (v,idx) -> (v, List.nth k idx)) inbind)
            in Valuation.extend tmp_th (Valuation.make bvars bvals) bvars
         in
         let r2 = try ce2_l th2 db with Failure x -> failwith ("op_slice: "^x) in
         (* perform cross product, extend out vars (slice key) to schema *)
         (* We can exploit schema monotonicity, and uniqueness of
          * map keys, implying this call to extend_keys will never do any
          * aggregation and can be simplified to key concatenation
          * and reindexing. *)
         let r3 =
            if semijoin then (ValuationMap.map (fun v2 -> op v1 v2) r2)
            else AggregateMap.concat_keys outv2 schema th2 schema_ext
                    (ValuationMap.map (fun v2 -> op v1 v2) r2)
         in
            (* r, r3 have no overlap -- safe to union slices.
             * This will also union any secondary indexes. *)
            ValuationMap.union r r3
      in
         ValuationMap.fold f (ValuationMap.empty_map()) res1)

(* Cross-product op
 * -- safe to bind w/ var bindings only, no bigsums in cross-products *)
let op_slice_product_expr prebind op ce1 ce2 =
   let (ce1_l, ce2_l) = (get_slice_code ce1, get_slice_code ce2) in
   Slice (fun theta db ->
      let th = Valuation.bind theta prebind in
      let res1 = ce1_l th db in
      let res2 = ce2_l th db
      in ValuationMap.product op res1 res2)

(* op, outv1, outv2, schema, theta_ext, schema_ext, lhs code, rhs code ->
 * op expr code
 * -- semijoin optimization: nothing to be done here, RHS is a singleton,
 *    and no schema extension (i.e. semijoin) implies schema is made up
 *    of bigsum vars, which must be accessed while looping over the bigsum
 *    slice. *)
let op_lslice_expr prebind inbind op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
   let (ce1_l, ce2_i) = (get_slice_code ce1, get_singleton_code ce2) in
   Slice (fun theta db ->
      let th = Valuation.bind theta prebind in
      let res1 = ce1_l th db in
      let f k v r =
        let (bvars,bvals) = 
           List.split (List.map (fun (v,idx) -> (v, List.nth k idx)) inbind) in
        let th1 = 
           let tmp_th = Valuation.extend th (Valuation.make outv1 k)
              (Util.ListAsSet.union theta_ext schema_ext) in
           Valuation.extend tmp_th (Valuation.make bvars bvals) bvars in
        let th2 =
           let tmp_th = Valuation.extend th (Valuation.make bvars bvals) bvars
           in Valuation.make outv2 (Valuation.apply tmp_th outv2) in
        let nv = op v (ce2_i th1 db) in
        let nk = Valuation.apply (Valuation.extend th2 th1 schema_ext) schema
        in ValuationMap.add nk nv r
      in ValuationMap.fold f (ValuationMap.empty_map()) res1) 

(* op, outv2, lhs code, rhs code -> op expr code *)
let op_lslice_product_expr prebind op outv2 ce1 ce2 =
   let (ce1_l, ce2_i) = (get_slice_code ce1, get_singleton_code ce2) in
   Slice (fun theta db ->
      let th = Valuation.bind theta prebind in
      let res1 = ce1_l th db in
      let v2 = ce2_i th db in
      let k2 = Valuation.apply th outv2
      in ValuationMap.mapi (fun k v -> (k@k2, op v v2)) res1)

(* op, outv2, schema, schema_ext, lhs code, rhs code -> op expr code *)
let op_rslice_expr prebind op outv2 schema schema_ext ce1 ce2 =
   let (ce1_i, ce2_l) = (get_singleton_code ce1, get_slice_code ce2) in
   Slice (fun theta db ->
      let th = Valuation.bind theta prebind in
      let v = ce1_i th db in
      let r = ce2_l th db in
         AggregateMap.concat_keys outv2 schema th schema_ext
            (ValuationMap.map (fun v2 -> op v v2) r))

(* mapn, out_patterns, outv, init rhs expr -> init lookup code *)
(* TODO: optimize, can we return a key-val pair here, rather than a slice
 * and differentiate k-v from slice from call points?
 * -- right now we do this so we have a single representation of IVC, 
 *    and this would require handling more cases wherever we use IVC. *)
(*
let singleton_init_lookup mapn inv out_patterns outv cinit =
   let cinit_i = get_singleton_code cinit in
      Slice (fun theta db ->
       let inv_img = Valuation.apply theta inv in
       let init_val = cinit_i theta db in
       let outv_img = Valuation.apply theta outv in
       if debug then debug_singleton_init_lookup mapn inv outv outv_img;
       Database.update_value mapn out_patterns inv_img outv_img init_val db;
       let r = 
          let img = if outv = [] then inv_img else outv_img in
          ValuationMap.from_list [(img, init_val)] out_patterns in
       if debug then debug_singleton_init_lookup_result r; r)
*)

let singleton_init_lookup mapn inv out_patterns outv cinit =
   let cinit_i = get_singleton_code cinit in
      Singleton (fun theta db ->
       let inv_img = Valuation.apply theta inv in
       let init_val = cinit_i theta db in
       let outv_img = Valuation.apply theta outv in
       if debug then debug_singleton_init_lookup mapn inv outv outv_img;
       Database.update_value mapn out_patterns inv_img outv_img init_val db;
       init_val)
   
(* mapn, out_patterns, init rhs expr -> init lookup code *)
let slice_init_lookup mapn inv out_patterns cinit =
   let cinit_l = get_slice_code cinit in
      Slice (fun theta db ->
       let inv_img = Valuation.apply theta inv in
       let init_slice = cinit_l theta db in
       let init_slice_w_indexes = List.fold_left
          ValuationMap.add_secondary_index init_slice out_patterns
       in
       if debug then debug_slice_init_lookup mapn inv;
       Database.update mapn inv_img init_slice_w_indexes db;
       init_slice_w_indexes)

(* mapn, inv, outv, init lookup code -> map lookup code *)
let singleton_lookup_and_init mapn inv outv init_val_code =
   let ivc_l = get_singleton_code init_val_code in
   let aux slice_f vars theta db = 
      let slice = slice_f theta db in
      let img = Valuation.apply theta vars in 
      if debug then debug_singleton_lookup img slice;
      if ValuationMap.mem img slice then ValuationMap.find img slice
      else ivc_l theta db
   in
   if inv = [] && outv = [] then Singleton (fun th db ->
      match Database.get_value mapn db with
       | Some(x) -> x | _ -> ivc_l th db)

   else if outv = [] then
      Singleton (aux (fun th db -> Database.get_in_map mapn db) inv)

   else if inv = [] then
      Singleton (aux (fun th db -> Database.get_out_map mapn db) outv)

   else
      Singleton (aux (fun theta db ->
         let m = try Database.get_map mapn db
            with Failure x -> failwith ("singleton_lookup_and_init: "^x) in
         let inv_img = Valuation.apply theta inv in
            if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
            else let outv_img = Valuation.apply theta outv
            in ValuationMap.from_list [(outv_img, ivc_l theta db)] []) outv)

(* mapn, inv, outv, init lookup code -> map lookup code *)
let singleton_lookup mapn inv outv init_val_code =
   let ivc_l = get_slice_code init_val_code in
   let aux slice_f vars theta db = 
      let slice = slice_f theta db in
      let img = Valuation.apply theta vars in 
      let lookup_slice =
         if ValuationMap.mem img slice then slice else ivc_l theta db in
      if debug then debug_singleton_lookup img lookup_slice;
      ValuationMap.find img lookup_slice
   in
   if inv = [] && outv = [] then Singleton (fun th db ->
      match Database.get_value mapn db with
       | Some(x) -> x | _ -> ValuationMap.find [] (ivc_l th db))

   else if outv = [] then
      Singleton (aux (fun th db -> Database.get_in_map mapn db) inv)

   else if inv = [] then
      Singleton (aux (fun th db -> Database.get_out_map mapn db) outv)

   else
      Singleton (aux (fun theta db ->
         let m = try Database.get_map mapn db
                 with Failure x -> failwith ("singleton_lookup: "^x) in
         let inv_img = Valuation.apply theta inv in
            if ValuationMap.mem inv_img m
            then ValuationMap.find inv_img m else ivc_l theta db) outv)

let slice_lookup_aux pat patv slice_f theta db =
   let slice = slice_f theta db in
   (* This can be a slice access for outv not in theta *)
   (* We must remove secondary indexes from lookups during calculus, 
    * evaluation, since we don't want them propagated around. *)
   let pkey = Valuation.apply theta patv in
   let lookup_slice = ValuationMap.slice pat pkey slice in
      if debug then debug_slice_lookup patv pkey slice lookup_slice;
      (ValuationMap.strip_indexes lookup_slice) 

(* mapn, inv, pat, patv, init lookup code -> map lookup code *)
let slice_lookup_sing_init mapn inv outv pat patv init_val_code =
   let ivc_l = get_singleton_code init_val_code in
   if inv = [] then
   Slice (slice_lookup_aux pat patv (fun th db -> Database.get_out_map mapn db))
   else
   Slice (slice_lookup_aux pat patv (fun theta db ->
      let m = try Database.get_map mapn db
              with Failure x -> failwith ("slice_lookup_sing_init: "^x) in
      let inv_img = Valuation.apply theta inv in
         if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
         else let outv_img = Valuation.apply theta outv in
         ValuationMap.from_list [(outv_img, ivc_l theta db)] [] )) 

(* mapn, inv, pat, patv, init lookup code -> map lookup code *)
let slice_lookup mapn inv pat patv init_val_code =
   let ivc_l = get_slice_code init_val_code in
   if inv = [] then
   Slice (slice_lookup_aux pat patv (fun th db -> Database.get_out_map mapn db))
   else
   Slice (slice_lookup_aux pat patv (fun theta db ->
      let m = try Database.get_map mapn db
              with Failure x -> failwith ("slice_lookup: "^x) in
      let inv_img = Valuation.apply theta inv in
         if ValuationMap.mem inv_img m
         then ValuationMap.find inv_img m else ivc_l theta db)) 


(* m3 expr code -> m3 rhs expr code as singleton *)
let singleton_expr ccalc cdebug =
   let ccalc_i = get_singleton_code ccalc in
   let cdebug_i = get_singleton_debug_code cdebug in
   Singleton (fun theta db ->
      let v = ccalc_i theta db in
      if debug then cdebug_i theta db [] v; v)

(* m3 expr code, debug code -> m3 rhs expr code as slice *)
let direct_slice_expr ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   let cdebug_l = get_slice_debug_code cdebug in
      Slice (fun theta db -> let r = ccalc_l theta db
             in if debug then cdebug_l theta db r; r)

(* m3 expr code -> m3 rhs expr code as singleton *)
let full_agg_slice_expr ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   let cdebug_l = get_slice_debug_code cdebug in
      Singleton (fun theta db ->
         let slice0 = ccalc_l theta db in
         if debug then cdebug_l theta db slice0;
         (ValuationMap.fold (fun k v acc -> c_sum acc v) (CFloat(0.0)) slice0))

(* rhs_pattern, rhs_projection, lhs_outv, rhs_ext, m3 expr code -> m3 rhs expr code
 * rhs_ext are trigger vars for delta computation with top level rhs slice exprs,
 * and bound out vars for init value computation on map lookups. Both extensions
 * restrict the computation to those matching the triggering tuple.
 *)
let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
   let ccalc_l = get_slice_code ccalc in
   let cdebug_l = get_slice_debug_code cdebug in
      Slice (fun theta db ->
         let slice0 = ccalc_l theta db in
         (* Note this aggregates bigsum vars present in the RHS map.
          * We use an indexed aggregation, however, since, slice0 will not
          * actually have any secondary indexes (we strip them during calculus
          * evaluation), we build an index with the necessary pattern here. *)
         if debug then cdebug_l theta db slice0;
         let slice1 = ValuationMap.add_secondary_index slice0 rhs_pattern in
            AggregateMap.project_keys rhs_pattern rhs_projection lhs_outv
               theta rhs_ext slice1)

(* Initial value computation for statements, evaluating the RHS initial value
 * expression for a statement, producing the initial value for an out tier.
 * The generated code produces a single value, and has the following signature:
 *    theta -> db -> out tier key -> out tier delta value -> out tier initial value
 *) 

(* init calc code, debug code -> init code *)
let singleton_init cinit cdebug =
   let cinitf = get_singleton_code cinit in
   let cdebug_i = get_singleton_debug_code cdebug in
      SingletonValueFunction (fun theta db _ v ->
         if debug then cdebug_i theta db [] v;
         let init_v = cinitf theta db in c_sum v init_v)

(* lhs_outv, init_ext, init calc code, debug code -> init code
 * init_ext are LHS out vars that do not appear in the init expr's schema,
 * where given loop vars are the same as in the schema, should only be
 * bound LHS out vars.
 *)
let slice_init lhs_outv init_ext cinit cdebug =
   let cinitf = get_slice_code cinit in
   let cdebug_i = get_singleton_debug_code cdebug in
      SingletonValueFunction (fun theta db k v ->
         let theta2 =
            Valuation.extend theta (Valuation.make lhs_outv k) init_ext in
         if debug then cdebug_i theta2 db k v;
         let init_slice = cinitf theta2 db in
         let init_v =
            if ValuationMap.mem k init_slice
            then (ValuationMap.find k init_slice) else CFloat(0.0)
         in c_sum v init_v)

(* Incremental statement evaluation, computing the RHS delta statement.
 * The generated code has the following signature:
 *   theta -> db -> singleton|slice -> singleton|slice *)
   
(* TODO: the generated code here takes a slice/singleton argument
 * and this is not reflected by the return type *)
(* lhs_outv, incr_m3 code, init value code, debug code -> update code *)
let singleton_update_aux f lhs_outv cincr init_value_code cdebug =
   let cincrf = get_singleton_code cincr in
   let cinitf = f init_value_code in
   let cdebug_e = get_env_debug_code cdebug in
   UpdateSingleton (fun theta db current_singleton ->
      if debug then cdebug_e theta db; 
      let delta_v = cincrf theta db in
         match current_singleton with
          | [] -> let delta_k = Valuation.apply theta lhs_outv in
                  cinitf theta db delta_k delta_v
          | [current_v] -> c_sum current_v delta_v
          | _ -> failwith "invalid singleton update")

let singleton_update = singleton_update_aux get_singleton_value_function_code

(* incr_m3 code, init value code, debug code -> update code *) 
let slice_update_aux f cincr init_value_code cdebug =
   let cincrf = get_slice_code cincr in
   let cinitf = f init_value_code in
   let cdebug_e = get_env_debug_code cdebug in
   UpdateSlice (fun theta db current_slice ->
      if debug then cdebug_e theta db;
      let delta_slice = cincrf theta db in
         ValuationMap.merge_rk (fun k v -> v) (cinitf theta db)
            (fun k v1 v2 -> c_sum v1 v2) current_slice delta_slice)

let slice_update = slice_update_aux get_singleton_value_function_code

(* Incremental update code generation, applying any delta and init values
 * computed to the given map's in tier.
 * The generated code has the following signature:
 *    theta -> db -> in var valuation -> in tier -> unit *)

(* lhs_mapn, lhs_outv, map out patterns, singleton eval code -> db update code*)
let db_singleton_update lhs_mapn lhs_outv map_out_patterns cstmt =
   let cstmtf = get_update_singleton_code cstmt in
      DatabaseUpdate (fun theta db inv_img in_slice ->
         let outv_img = Valuation.apply theta lhs_outv in
         let singleton = 
            if ValuationMap.mem outv_img in_slice then
               [ValuationMap.find outv_img in_slice]
            else [] in
         let v = cstmtf theta db singleton in
         Database.update_value lhs_mapn map_out_patterns inv_img outv_img v db)

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
         unchanged_slice (cstmtf theta db work_slice)
      *)
      let new_slice = cstmtf theta db in_slice
      in Database.update lhs_mapn inv_img new_slice db)


(* Top-level M3 program structure *)
(* TODO: this code returns unit, and should be reflected in resulting code_t *)
(* lhs_mapn, lhs_ext, patv, pat, direct, db update code -> statement code *)
let statement lhs_mapn lhs_inv lhs_outv lhs_ext patv pat direct db_update_code =
   let db_f = get_database_update_code db_update_code in
   (* TODO: remove from_list for singleton, push singleton into update code *)
   if lhs_inv = [] && lhs_outv = [] then
   Statement (fun theta db ->
        let slice = match Database.get_value lhs_mapn db with
           | Some(x) -> (ValuationMap.from_list [([], x)] [])
           | _ -> ValuationMap.from_list [] [] 
        in db_f theta db [] slice)
   
   else if lhs_inv = [] then
   Statement (fun theta db ->
        let slice = Database.get_out_map lhs_mapn db 
        in db_f theta db [] slice)
   
   (* TODO: remove from_list for singleton, push singleton into update code *)
   else if lhs_outv = [] then
   Statement (fun theta db ->
      let lhs_map = Database.get_in_map lhs_mapn db in
      let iifilter db inv_img =
        let inv_theta = Valuation.make lhs_inv inv_img in
        let new_theta = Valuation.extend theta inv_theta lhs_ext in
        let v = ValuationMap.find inv_img lhs_map
        in db_f new_theta db inv_img (ValuationMap.from_list [([], v)] [])
      in
      let pkey = Valuation.apply theta patv in
      let inv_imgs = if direct then [pkey]
                     else ValuationMap.slice_keys pat pkey lhs_map
      in List.iter (iifilter db) inv_imgs)
   
   else
   Statement (fun theta db ->
      let lhs_map = try Database.get_map lhs_mapn db with Failure x -> failwith ("stmt get_map: "^x) in
      let iifilter db inv_img =
        let inv_theta = Valuation.make lhs_inv inv_img in
        let new_theta = Valuation.extend theta inv_theta lhs_ext in
        let slice = ValuationMap.find inv_img lhs_map
        in db_f new_theta db inv_img slice       
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
   Trigger (event, rel, (fun tuple db ->
      let theta = Valuation.make trig_args tuple in
         List.iter (fun cstmt -> cstmt theta db) cblock))

(* No sources for interpreter *)
type source_impl_t = File of in_channel * adaptor list
   | Socket of Unix.file_descr * adaptor list

let source src framing (rel_adaptors : (string * adaptor_t) list) =
   let src_impl = match src with
    | FileSource(fn) ->
       let adaptor_impls = List.map (fun (r,a) -> create_adaptor a) rel_adaptors
       in File(open_in fn, adaptor_impls)
       
    | SocketSource(_) -> failwith "Sockets not yet implemented."
    | PipeSource(_)   -> failwith "Pipes not yet implemented."
   in (src_impl, None, None)

(* No top level code generated for the interpreter *)
let main schema patterns sources triggers =
   failwith "interpreter should be directly invoked"

(* No file output for interpreter *)
let output out_chan = failwith "interpreter cannot write source code"

let eval_trigger trigger tuple db = (get_trigger trigger) tuple db

type trigger_evaluator = (M3.const_t list -> Database.db_t -> unit)

let event_evaluator (triggers:code_t list) (db:db_t) = 
  let ((insert_trigs, delete_trigs):
        (trigger_evaluator StringMap.t * trigger_evaluator StringMap.t)) = 
    List.fold_left 
      (fun ((insert_trig, delete_trig):
              (trigger_evaluator StringMap.t * trigger_evaluator StringMap.t)) 
           (trig:compiled_code) ->
        let create_trigger (map:trigger_evaluator StringMap.t): 
                           (trigger_evaluator StringMap.t) =
          let (_, (rel:rel_id_t), (t_code:trigger_evaluator)) = 
            get_trigger_with_evt trig in
          StringMap.add rel t_code map 
        in
        match get_trigger_with_evt trig with 
          | (M3.Insert, _, _) -> 
              (create_trigger insert_trig, delete_trig)
          | (M3.Delete, _, _) -> 
              (insert_trig, create_trigger delete_trig)
    ) (StringMap.empty, StringMap.empty) triggers
  in
    (fun evt ->
      match evt with 
        | Some(Insert, rel, tuple) -> 
          ((StringMap.find rel insert_trigs) tuple db; true)
        | Some(Delete, rel, tuple) -> 
          ((StringMap.find rel delete_trigs) tuple db; true)
        | None -> false
    )

end
