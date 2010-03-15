open M3

let vars_to_string vs = Util.list_to_string (fun x->x) vs

(* (PARTIAL) VARIABLE VALUATIONS *)
module type ValuationSig =
sig
   type key = string
   type t

   val string_of_const_t : const_t -> string
   
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

   (* TODO: change when const_t is generalized *)
   let string_of_const_t = string_of_int

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
         k^"->"^(string_of_const_t v)) theta ""
end

(* Map whose keys are valuations, and values are polymorphic *)
module ValuationMap = SliceableMap.Make(struct type t = const_t let to_string = string_of_int end)

(* ValuationMap whose values are aggregates *)
module AggregateMap =
struct
   module VM = ValuationMap

   type agg_t = const_t
   
   type key = VM.key
   type t = agg_t VM.t

   let string_of_aggregate = string_of_int

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
         let aggv = List.fold_left (fun x (y,z) -> x+z) 0 kv in
         let ext_theta =
            Valuation.extend (Valuation.make pat_outv pk) theta extensions in
         (* normalizes the key orderings to that of the variable ordering outv. *)
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

(* Patterns *)
type pattern =
     In of (var_t list * ValuationMap.pattern)
   | Out of (var_t list * ValuationMap.pattern)

type pattern_map = (string * pattern list) list

let index l x =
   let pos = fst (List.fold_left (fun (run, cur) y ->
      if run >= 0 then (run, cur) else ((if x=y then cur else run), cur+1))
      (-1, 0) l)
   in if pos = -1 then raise Not_found else pos

let make_in_pattern dimensions accesses =
   In(accesses, List.map (index dimensions) accesses)

let make_out_pattern dimensions accesses =
   Out(accesses, List.map (index dimensions) accesses)

let get_pattern = function | In(x,y) | Out(x,y) -> y

let get_pattern_vars = function | In(x,y) | Out(x,y) -> x

let empty_pattern_map() = []

let get_filtered_patterns filter_f pm mapn =
   let map_patterns = if List.mem_assoc mapn pm
                      then List.assoc mapn pm else []
   in List.map get_pattern (List.filter filter_f map_patterns)

let get_in_patterns pm mapn = get_filtered_patterns
   (function | In _ -> true | _ -> false) pm mapn

let get_out_patterns pm mapn = get_filtered_patterns
   (function | Out _ -> true | _ -> false) pm mapn

let get_out_pattern_by_vars pm mapn vars = List.hd (get_filtered_patterns
   (function Out(x,y) -> x = vars | _ -> false) pm mapn)

let add_pattern pm (mapn,pat) =
   let existing = if List.mem_assoc mapn pm then List.assoc mapn pm else [] in
   let new_pats = pat::(List.filter (fun x -> x <> pat) existing) in
      (mapn, new_pats)::(List.remove_assoc mapn pm)

let merge_pattern_maps p1 p2 =
   let aux pm (mapn, pats) =
      if List.mem_assoc mapn pm then
         List.fold_left (fun acc p -> add_pattern acc (mapn, p)) pm pats
      else (mapn, pats)::pm
   in List.fold_left aux p1 p2

let singleton_pattern_map (mapn,pat) = [(mapn, [pat])]

let patterns_to_string pm =
   let patlist_to_string pl = List.fold_left (fun acc pat ->
      let pat_str = String.concat "," (
         match pat with | In(x,y) | Out(x,y) ->
            List.map (fun (a,b) -> a^":"^b)
               (List.combine (List.map string_of_int y) x))
      in
      acc^(if acc = "" then acc else " / ")^pat_str) "" pl
   in
   List.fold_left (fun acc (mapn, pats) ->
      acc^"\n"^mapn^": "^(patlist_to_string pats)) "" pm


module Database =
struct
   module VM  = ValuationMap
   module AM  = AggregateMap
   module DBM = SliceableMap.Make(struct type t = string let to_string x = x end)
   
   type slice_t = AM.t
   type dbmap_t = AM.t VM.t
   type db_t    = dbmap_t DBM.t

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string Valuation.string_of_const_t k

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
         let string_of_img = Util.list_to_string Valuation.string_of_const_t in
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
            let string_of_img = Util.list_to_string Valuation.string_of_const_t in
            print_endline ("Updating a db value: "^mapn^
                           " inv="^(string_of_img inv_img)^
                           " outv="^(string_of_img outv_img)^
                           " v="^(Valuation.string_of_const_t new_value)^
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


(* M3 preparation and compilation *)
module M3P = M3.Prepared

let rec calc_schema calc =
   let op c1 c2 = Util.ListAsSet.union (calc_schema c1) (calc_schema c2) in
   match calc with
      MapAccess(mapn, inv, outv, init_calc) -> outv
    | Add (c1, c2)        -> op c1 c2
    | Mult(c1, c2)        -> op c1 c2
    | Lt  (c1, c2)        -> op c1 c2
    | Leq (c1, c2)        -> op c1 c2
    | Eq  (c1, c2)        -> op c1 c2
    | IfThenElse0(c1, c2) -> op c2 c1
    | Null(outv)          -> outv
    | Const(i)            -> []
    | Var(x)              -> [x]

let rec pcalc_to_string calc =
   let ots op e1 e2 =
      op^"("^(pcalc_to_string (fst e1))^", "^(pcalc_to_string (fst e2))^")" in
   match calc with
      M3P.MapAccess(mapn, inv, outv, init_aggecalc) ->
         "MapAccess("^mapn^", "^(vars_to_string inv)^", "^
         (vars_to_string outv)^", "^(pcalc_to_string (fst (fst init_aggecalc)))^")"
    | M3P.Add (e1, e2)        -> ots "Add"  e1 e2
    | M3P.Mult(e1, e2)        -> ots "Mult" e1 e2
    | M3P.Lt  (e1, e2)        -> ots "Lt"   e1 e2
    | M3P.Leq (e1, e2)        -> ots "Leq"  e1 e2
    | M3P.Eq  (e1, e2)        -> ots "Eq"   e1 e2
    | M3P.IfThenElse0(e1, e2) -> ots "IfThenElse0" e1 e2
    | M3P.Null(outv)          -> "Null("^(vars_to_string outv)^")"
    | M3P.Const(i)            -> string_of_int i
    | M3P.Var(x)              -> x

let rec pcalc_schema (calc : M3P.pcalc_t) =
   let op c1 c2 =
      Util.ListAsSet.union (pcalc_schema (fst c1)) (pcalc_schema (fst c2)) in
   match calc with
      M3P.MapAccess(mapn, inv, outv, init_ecalc) -> outv
    | M3P.Add (c1, c2)        -> op c1 c2
    | M3P.Mult(c1, c2)        -> op c1 c2
    | M3P.Lt  (c1, c2)        -> op c1 c2
    | M3P.Leq (c1, c2)        -> op c1 c2
    | M3P.Eq  (c1, c2)        -> op c1 c2
    | M3P.IfThenElse0(c1, c2) -> op c2 c1
    | M3P.Null(outv)          -> outv
    | M3P.Const(i)            -> []
    | M3P.Var(x)              -> [x]

let prepare_triggers (triggers : trig_t list)
   : (M3P.ptrig_t list * pattern_map) =
   let get_singleton meta = snd meta in
   let get_extensions meta = fst meta in
   let rec prepare_calc (lhs_vars: var_t list)
                        (theta_vars : var_t list) (calc : calc_t)
         : (M3P.ecalc_t * pattern_map) =
      let recur = prepare_calc lhs_vars in
      let prepare_aux c propagate defv theta_ext
            : (M3P.ecalc_t * var_t list * pattern_map) =
         let outv = calc_schema c in
         let ext = Util.ListAsSet.diff (if propagate then defv else outv)
                      (if propagate then outv else defv) in
         let (pc, pm) = recur (theta_vars@theta_ext) c in 
         (* Override metadata, assumes recursive call to prepare_calc has
          * already done this for its children. *)
         let new_pc = (fst pc, (ext, get_singleton (snd pc)))
         in (new_pc, outv, pm)
      in
      let prepare_op (f : M3P.ecalc_t -> M3P.ecalc_t -> M3P.pcalc_t)
                     (c1: calc_t) (c2: calc_t)
            : (M3P.ecalc_t * pattern_map)
      =
         let (e1, c1_outv, p1_patterns) = prepare_aux c1 false theta_vars [] in
         let (e2, _, p2_patterns) =
            prepare_aux c2 true c1_outv (get_extensions (snd e1)) in
         let patterns = merge_pattern_maps p1_patterns p2_patterns in
         let singleton = (get_singleton (snd e1)) && (get_singleton (snd e2))
         (* Safe to use empty theta extensions, since this will get overriden
          * by recursive calls for binary ops *) 
         in ((f e1 e2, ([], singleton)), patterns) 
      in
      match calc with
        | Const(c)      -> ((M3P.Const(c), ([], true)), empty_pattern_map())
        | Var(v)        -> 
           (* Bigsum vars are not LHS vars, and are slices. *)
           ((M3P.Var(v), ([], List.mem v lhs_vars)), empty_pattern_map())

        | Add  (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Add  (e1, e2)) c1 c2
        | Mult (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Mult (e1, e2)) c1 c2
        | Leq  (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Leq  (e1, e2)) c1 c2
        | Eq   (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Eq   (e1, e2)) c1 c2
        | Lt   (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Lt   (e1, e2)) c1 c2
        | IfThenElse0 (c1,c2) -> prepare_op (fun e1 e2 -> M3P.IfThenElse0 (e2, e1)) c2 c1
        
        | MapAccess (mapn, inv, outv, init_calc) ->

           (* Determine slice or singleton.
            * singleton: no in vars, and fully bound out vars. *)
           let bound_outv = Util.ListAsSet.inter outv theta_vars in
           let full_agg = ((List.length bound_outv) = (List.length outv)) in
           let singleton = (List.length inv = 0) && full_agg in
           
           (* Use map scope for lhs vars during recursive prepare for initial
            * value calculus. *)
           let init_lhs_vars =
              Util.ListAsSet.union (Util.ListAsSet.union theta_vars inv) outv in
           
           let (init_ecalc, patterns) =
              prepare_calc init_lhs_vars theta_vars init_calc in
           let new_patterns = 
              if (List.length outv) = (List.length bound_outv) then patterns
              else merge_pattern_maps patterns (singleton_pattern_map
                      (mapn, make_out_pattern outv bound_outv)) in
           let new_init_aggecalc =
              (((fst init_ecalc), (bound_outv, get_singleton (snd init_ecalc))), full_agg)
           in
           let r = (M3P.MapAccess(mapn, inv, outv, new_init_aggecalc), ([], singleton))
           in (r, new_patterns)
        
        (* TODO: are null slices singletons? *)
        | Null (outv) -> ((M3P.Null(outv), ([], true)), empty_pattern_map())
   in

   let prepare_stmt theta_vars (stmt : stmt_t) : (M3P.pstmt_t * pattern_map) =

      let ((lmapn, linv, loutv, init_calc), incr_calc) = stmt in

      let partition v1 v2 =
         (Util.ListAsSet.inter v1 v2, Util.ListAsSet.diff v1 v2) in
      let (bound_inv, loop_inv) = partition linv theta_vars in
      let (bound_outv, loop_outv) = partition loutv theta_vars in
      
      (* For compile_pstmt_loop, extend with loop in vars *)
      let theta_w_loopinv = Util.ListAsSet.union theta_vars loop_inv in
      let theta_w_lhs = Util.ListAsSet.union
         (Util.ListAsSet.union theta_vars linv) loutv
      in
      let full_agg = (List.length loop_outv) = 0 in 

      (* Checking bigsum vars for slices is now done locally by passing down
       * LHS vars through M3 preparation. *)
      let init_ext = (Util.ListAsSet.diff loutv (calc_schema init_calc)) in
      
      (* Set up top-level extensions for an entire incr/init RHS.
       * Incr M3 is extended by bound out variables.
       * Init M3 is extended by LHS out vars that do not appear in the RHS out vars. *)
      let prepare_stmt_ext calc ext ext_theta =
         let calc_theta_vars = Util.ListAsSet.union theta_w_loopinv
            (if ext_theta then ext else []) in
         let (c, patterns) = prepare_calc theta_w_lhs calc_theta_vars calc in
         let csingle = get_singleton (snd c) in 
         let new_c = ((fst c, (ext, csingle)), full_agg) in
            print_endline ("singleton="^(string_of_bool csingle)^
                           " calc="^(pcalc_to_string (fst c)));
            (new_c, patterns) 
      in

      let (init_ca, init_patterns) = prepare_stmt_ext init_calc init_ext true in
      let (incr_ca, incr_patterns) = prepare_stmt_ext incr_calc bound_outv false in

      let pstmt = ((lmapn, linv, loutv, init_ca), incr_ca, loop_inv) in

      let extra_patterns =
         let extras =
            (if (List.length bound_inv) = (List.length linv) then []
             else [(lmapn, (make_in_pattern linv bound_inv))])@
            (if (List.length bound_outv) = (List.length loutv) then []
             else [(lmapn, (make_out_pattern loutv bound_outv))])
         in List.fold_left add_pattern (empty_pattern_map()) extras
      in

      let patterns = List.fold_left merge_pattern_maps (empty_pattern_map())
        ([ init_patterns; incr_patterns ]@[extra_patterns])
      in
         (pstmt, patterns)
   in

   let prepare_block (trig_args : var_t list) (bl : stmt_t list)
         : (M3P.pstmt_t list * pattern_map) =
      let bl_pat_l = List.map (prepare_stmt trig_args) bl in
      let (pbl, patterns_l) = List.split bl_pat_l in
      let patterns = List.fold_left
         merge_pattern_maps (empty_pattern_map()) patterns_l
      in (pbl, patterns)
   in

   let prepare_trig (t : trig_t) : (M3P.ptrig_t * pattern_map) =
      let (ev,rel,args,block) = t in
      let (pblock, patterns) = prepare_block args block in
         ((ev, rel, args, pblock), patterns)
   in
   let blocks_maps_l = List.map prepare_trig triggers in
   let (pblocks, pms) = List.split blocks_maps_l in
      (pblocks, List.fold_left merge_pattern_maps (empty_pattern_map()) pms)

      
(* Simple OCaml code generator *)
type slice_code = (Valuation.t -> Database.db_t -> AggregateMap.t)
type slice_update_code =
   (Valuation.t -> AggregateMap.t -> Database.db_t -> AggregateMap.t)

type singleton_code =
   (Valuation.t -> Database.db_t -> AggregateMap.agg_t list)

type singleton_update_code =
   (Valuation.t -> AggregateMap.agg_t list ->
      Database.db_t -> AggregateMap.agg_t list)

type compiled_stmt = (Valuation.t -> Database.db_t -> unit)

type compiled_code =
     Singleton of singleton_code
   | Slice of slice_code
   | UpdateSlice of slice_update_code
   | UpdateSingleton of singleton_update_code

let get_singleton_code x =
   match x with | Singleton(c) -> c | _ -> failwith "invalid singleton code"

let get_slice_code x =
   match x with | Slice(c) -> c | _ -> failwith "invalid slice code"

let rec compile_pcalc patterns (incr_ecalc) : compiled_code =
   let get_extensions ecalc = fst (snd ecalc) in
   let singleton = snd (snd incr_ecalc) in
   let int_op op x y = if (op x y) then 1 else 0 in
   let compile_op op m1 m2 : compiled_code =
      let aux ecalc = (pcalc_schema (fst ecalc), get_extensions ecalc,
                         compile_pcalc patterns ecalc) in
      let (outv1, theta_ext, cm1) = aux m1 in
      let (outv2, schema_ext, cm2) = aux m2 in
      let schema = Util.ListAsSet.union outv1 outv2 in
         if singleton then
         let (cm1_i, cm2_i) = (get_singleton_code cm1, get_singleton_code cm2)
         in Singleton (fun theta db ->
            (* Since there are no bigsum vars, we don't need to evaluate RHS
             * with an extended theta *)
            let (r1,r2) = (cm1_i theta db, cm2_i theta db) in
            match (r1,r2) with
             | ([], _) | (_,[]) -> []
             | ([v1], [v2]) -> [op v1 v2]
             | _ -> failwith "compile_op: invalid singleton")
         else
         begin match (cm1, cm2) with
          | (Singleton(_), Singleton(_)) -> failwith "invalid non-singleton"
          | (Singleton(cm1_i), Slice(cm2_l)) ->
             Slice (fun theta db ->
                match cm1_i theta db with
                | [] -> ValuationMap.empty_map()
                | [v] ->
                   let r = cm2_l theta db in
                      AggregateMap.concat_keys outv2 schema theta schema_ext
                         (ValuationMap.map (fun v2 -> op v v2) r)
                | _ -> failwith "compile_op: invalid singleton")

          | (Slice(cm1_l), Singleton(cm2_i)) ->
             Slice (fun theta db ->
                let res1 = cm1_l theta db in
                let th2 = Valuation.make outv2 (Valuation.apply theta outv2) in
                let f k v r =
                  let th1 = Valuation.extend theta (Valuation.make outv1 k) theta_ext in
                  begin match (cm2_i th1 db) with
                   | [] -> r
                   | [v2] ->
                      let nv = op v v2 in
                      let nk = Valuation.apply
                         (Valuation.extend th2 th1 schema_ext) schema
                      in ValuationMap.add nk nv r
                   | _ -> failwith "compile_op: invalid singleton"
                  end
                in ValuationMap.fold f (ValuationMap.empty_map()) res1) 

          | (Slice(cm1_l), Slice(cm2_l)) ->
             Slice (fun theta db ->
             let res1 = cm1_l theta db in
             let f k v1 r =
                (* extend with out vars from LHS calc. This is for bigsum vars in
                 * IfThenElse0, so that these bigsum vars can be used as in vars
                 * for map lookups. *)
                let th = Valuation.extend theta (Valuation.make outv1 k) theta_ext in
                let r2 = cm2_l th db in
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
           
           | _ -> failwith "compile_op: invalid compiled code"
         end
   in
   let ccalc : compiled_code =
      match (fst incr_ecalc) with
        M3P.MapAccess(mapn, inv, outv, init_aggecalc) ->
            let cinit = compile_pcalc2
               patterns (snd init_aggecalc) outv (fst init_aggecalc) in
            let out_patterns = get_out_patterns patterns mapn in
            (* Note: we extend theta for this map access' init value comp
             * with bound out vars, thus we can use the extension to find
             * bound vars for the partial key here *)
            let patv = fst (snd (fst init_aggecalc)) in
            let pat = List.map (index outv) patv in
            (* code that returns the initial value slice *)
            let init_val_slice_code =
               match cinit with
                | Singleton(cinit_i) ->
                   (fun theta db inv_img ->
                   let init_val = cinit_i theta db in
                   let outv_img = Valuation.apply theta outv in
                   begin match init_val with
                      | [] -> ValuationMap.empty_map()
                      | [v] -> (Database.update_value
                                  mapn out_patterns inv_img outv_img v db;
                                ValuationMap.from_list [(outv_img, v)] out_patterns)
                      | _ -> failwith "MapAccess: invalid singleton"
                   end)
                
                | Slice(cinit_l) ->
                   (fun theta db inv_img ->
                   let init_slice = cinit_l theta db in
                   let init_slice_w_indexes = List.fold_left
                      ValuationMap.add_secondary_index init_slice out_patterns
                   in
                      Database.update mapn inv_img init_slice_w_indexes db;
                      init_slice_w_indexes)
                 
                 | _ -> failwith "invalid initial value code"
            in
            (* code that returns the map slice we're accessing *)
            let slice_access_code =
               (fun theta db ->
                  let m = Database.get_map mapn db in
                  let inv_img = Valuation.apply theta inv in
                     if ValuationMap.mem inv_img m then
                        ValuationMap.find inv_img m
                     else init_val_slice_code theta db inv_img)
            in
            (* code that does the final stage of the lookup on the slice *)
            if singleton then Singleton (fun theta db ->
               let slice = slice_access_code theta db in
               let outv_img = Valuation.apply theta outv in
                  if ValuationMap.mem outv_img slice then
                     [ValuationMap.find outv_img slice]
                  else [])
            else Slice (fun theta db ->
               let slice = slice_access_code theta db in
                  (* This can be a slice access for outv not in theta *)
                  (* We must remove secondary indexes from lookups during calculus, 
                   * evaluation, since we don't want them propagated around. *)
                  let pkey = Valuation.apply theta patv in
                  let lookup_slice = ValuationMap.slice pat pkey slice in
                     (ValuationMap.strip_indexes lookup_slice))

      | M3P.Add (c1, c2) -> compile_op ( + )         c1 c2
      | M3P.Mult(c1, c2) -> compile_op ( * )         c1 c2
      | M3P.Lt  (c1, c2) -> compile_op (int_op (< )) c1 c2
      | M3P.Leq (c1, c2) -> compile_op (int_op (<=)) c1 c2
      | M3P.Eq  (c1, c2) -> compile_op (int_op (= )) c1 c2
      | M3P.IfThenElse0(c1, c2) ->
           compile_op (fun v cond -> if (cond<>0) then v else 0) c2 c1
        
      | M3P.Null(outv) ->
         if singleton then Singleton (fun theta db -> [])
         else Slice (fun theta db -> ValuationMap.empty_map())

      | M3P.Const(i)   ->
         if singleton then Singleton (fun theta db -> [i])
         else Slice (fun theta db -> ValuationMap.from_list [([], i)] [])

      | M3P.Var(x)     ->
         if singleton then Singleton (fun theta db ->
            if (not (Valuation.bound x theta)) then
               failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
            else let v = Valuation.value x theta in [v])
         else Slice (fun theta db ->
            if (not (Valuation.bound x theta)) then
               failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
            else let v = Valuation.value x theta in ValuationMap.from_list [([v], v)] [])
      
      in
(*
      let debug theta db =
         print_string("\neval_pcalc "^(pcalc_to_string incr_calc)^
                      " "^(Valuation.to_string theta)^
                      " "^(Database.db_to_string db)^"   ")
      in
*)
      if singleton then
         let ccalc_i = get_singleton_code ccalc
         in Singleton (fun theta db -> (*debug theta db;*) ccalc_i theta db)
      else let ccalc_l = get_slice_code ccalc
         in Slice (fun theta db -> (*debug theta db;*) ccalc_l theta db)


(* Optimizations:
 * -- aggregates blindy over entire slice for fully bound lhs_outv 
 * -- skips aggregating when rhs_outv = lhs_outv *)
and compile_pcalc2 patterns lhs_bound lhs_outv ecalc : compiled_code =
   let rhs_outv = pcalc_schema (fst ecalc) in
   (* project slice w/ rhs schema to lhs schema, extending by out vars that are bound *)
   let rhs_ext = fst (snd ecalc) in
   let rhs_projection = Util.ListAsSet.inter rhs_outv lhs_outv in
   let rhs_pattern = List.map (index rhs_outv) rhs_projection in
   let ccalc = compile_pcalc patterns ecalc in
   let singleton = snd (snd ecalc) in
      if singleton then let ccalc_i = get_singleton_code ccalc in
(*
      let debug theta db k v =
         print_endline ("End of PCALC_S; outv="^(vars_to_string lhs_outv)^
                       " k="^(Util.list_to_string Valuation.string_of_const_t k)^
                       " v="^(Valuation.string_of_const_t v)^
                      (" db="^(Database.db_to_string db))^
                      (" theta="^(Valuation.to_string theta)))
      in
*)
      Singleton (fun theta db ->
         let r = ccalc_i theta db in
         match r with
          | [] -> []
          | [v] -> (* debug theta db k v; *) [v] 
          | _ -> failwith "compile_pcalc2_singleton: invalid singleton")

      else let ccalc_l = get_slice_code ccalc in
(*
      let debug theta db slice0 =
          print_endline ("End of PCALC; outv="^(vars_to_string rhs_outv)^
                         " slice="^(Database.slice_to_string slice0)^
                        (" db="^(Database.db_to_string db))^
                        (" theta="^(Valuation.to_string theta)))
      in
*)
      begin if rhs_outv = lhs_outv then
      Slice (fun theta db -> ccalc_l theta db)
      else if lhs_bound then
      Singleton (fun theta db ->
         let slice0 = ccalc_l theta db in
            [ValuationMap.fold (fun k v acc -> acc+v) 0 slice0])
      else
      Slice (fun theta db ->
         let slice0 = ccalc_l theta db in
         (* Note this aggregates bigsum vars present in the RHS map.
          * We use an indexed aggregation, however, since, slice0 will not
          * actually have any secondary indexes (we strip them during calculus
          * evaluation), we build an index with the necessary pattern here. *)
         (* debug theta db slice0; *)
         let slice1 = ValuationMap.add_secondary_index slice0 rhs_pattern in
            AggregateMap.project_keys rhs_pattern rhs_projection lhs_outv
               theta rhs_ext slice1)
      end


(* Optimizations: handles
 * -- current_slice singleton
 * -- delta_slice singleton, only if all map accesses are singletons, i.e.
 *    no bigsum vars, all out vars of map accesses are bound, and no in vars
 *    in any maps (otherwise there may be initial values). *)
let compile_pstmt patterns
   ((lhs_mapn, lhs_inv, lhs_outv, init_aggecalc), incr_aggecalc, _)
      : compiled_code
   =
   let aux aggecalc : compiled_code =
      let singleton = snd (snd (fst aggecalc)) in
      print_endline ((if singleton then "singleton" else "slice")^
                     " calc="^(pcalc_to_string (fst (fst aggecalc))));
      compile_pcalc2 patterns (snd aggecalc) lhs_outv (fst aggecalc)
   in
   let (cincr, cinit) = (aux incr_aggecalc, aux init_aggecalc) in
   let init_ext = fst (snd (fst init_aggecalc)) in
   let init_value_code =
(*
      let debug theta2 db k =
         print_string ("@PSTMT.init{th="^(Valuation.to_string theta2)
                      ^", key="^(Util.list_to_string string_of_int k)
                      ^", db="^(Database.db_to_string db));
      in
*)
      match cinit with
      | Singleton(cinitf) ->
         (fun theta db _ v ->
            (* debug theta2 db []; *)
            (match cinitf theta db with
             | [] -> v
             | [init_v] -> v+init_v
             | _ -> failwith "invalid init singleton" )) 
      | Slice(cinitf) ->
         (fun theta db k v ->
            let theta2 =
               Valuation.extend theta (Valuation.make lhs_outv k) init_ext
            in
            (* debug theta2 db k; *)
            let init_slice = cinitf theta2 db in
            let init_v =
               if ValuationMap.mem k init_slice
               then (ValuationMap.find k init_slice) else 0
            in v + init_v)
      | _ -> failwith "invalid init value code"
   in
(*
   let debug =
      print_string("\nPSTMT (th="^(Valuation.to_string theta)
                  ^", db=_, stmt=(("^lhs_mapn^" "^
                     (Util.list_to_string (fun x->x) lhs_inv)^" "
                  ^(Util.list_to_string (fun x->x) lhs_outv)^" _), _))\n")
   in
*)
      match cincr with
       | Slice(cincrf) -> UpdateSlice
          (fun theta current_slice db ->
             let delta_slice = cincrf theta db in
                ValuationMap.merge_rk
                   (fun k v -> v) (init_value_code theta db) (fun k v1 v2 -> v1+v2)
                   current_slice delta_slice)

       | Singleton(cincrf) -> UpdateSingleton
          (fun theta current_singleton db ->
             let delta_slice = cincrf theta db in
             match (current_singleton, delta_slice) with
              | (s, []) -> s
              | ([], [delta_v]) ->
                 let delta_k = Valuation.apply theta lhs_outv in
                    [init_value_code theta db delta_k delta_v]
              | ([current_v], [delta_v]) -> [current_v+delta_v]
              | _ -> failwith "invalid singleton update")

       | _ -> failwith "compile_pstmt: invalid update"


let compile_pstmt_loop patterns trig_args pstmt : compiled_stmt =
   let ((lhs_mapn, lhs_inv, lhs_outv, _), _, lhs_ext) = pstmt in
   let cstmt = compile_pstmt patterns pstmt in
   let patv = Util.ListAsSet.inter lhs_inv trig_args in
   let pat = List.map (index lhs_inv) patv in
   let direct = (List.length patv) = (List.length lhs_inv) in 
   let map_out_patterns = get_out_patterns patterns lhs_mapn in
   (* Output pattern for partitioning *)
   (*
   let out_patv = Util.ListAsSet.inter lhs_outv trig_args in
   let out_pat = List.map (index lhs_outv) out_patv  in
   *)
   let db_update_code =
      match cstmt with
       | UpdateSlice (cstmtf) ->
          (fun theta db inv_img in_slice ->
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

       | UpdateSingleton (cstmtf) ->
          (fun theta db inv_img in_slice ->
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

       | _ -> failwith "compile_pstmt_loop: invalid update code."
   in
   (fun theta db ->
      let lhs_map = Database.get_map lhs_mapn db in
      let iifilter db inv_img =
        let inv_theta = Valuation.make lhs_inv inv_img in
        let new_theta = Valuation.extend theta inv_theta lhs_ext in
        let slice = ValuationMap.find inv_img lhs_map in
           db_update_code new_theta db inv_img slice       
      in
      let pkey = Valuation.apply theta patv in
      let inv_imgs = if direct then [pkey]
                     else ValuationMap.slice_keys pat pkey lhs_map
      in List.iter (iifilter db) inv_imgs)
          

let compile_ptrig (ptrig, patterns) =
   let aux ptrig =
      let (event, rel, trig_args, pblock) = ptrig in
      let aux2 = compile_pstmt_loop patterns trig_args in
      let cblock = List.map aux2 pblock in
      (fun tuple db ->
         let theta = Valuation.make trig_args tuple in
            List.iter (fun cstmt -> cstmt theta db) cblock)
   in
      List.map aux ptrig

