open M3

let vars_to_string vs = Util.list_to_string (fun x->x) vs

(* (PARTIAL) VARIABLE VALUATIONS *)
module type ValuationSig =
sig
   type key = string
   type t

   type extension_key = Positional of int | Named of string list
   type valuation_extension = string list

   val named_extension : string list -> extension_key
   val pos_extension : int -> extension_key
   val add_extension : extension_key -> valuation_extension -> valuation_extension
   val find_extension : extension_key -> valuation_extension
   
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
   module StringSMap = SuperMap.Make(StringMap)

   type key = StringMap.key
   type t = const_t StringMap.t

   type extension_key = Positional of int | Named of string list
   type valuation_extension = string list
   type extension_cache = (extension_key, valuation_extension) Hashtbl.t
   
   let named_extension vl = Named(vl)
   let pos_extension id = Positional(id)

   let valuation_extensions = Hashtbl.create 10 
   let add_extension k v = (Hashtbl.replace valuation_extensions k v; v)
   let find_extension k = Hashtbl.find valuation_extensions k

   (* TODO: change when const_t is generalized *)
   let string_of_const_t = string_of_int
    
   (* Note: ordered result *)
   let make (vars: var_t list) (values: const_t list) : t =
      StringSMap.from_list (List.combine vars values)

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

   (* TODO: does order matter here? *)
   let to_string (theta:t) : string =
      StringSMap.to_string (fun x->x) (fun v -> (string_of_const_t v)) theta
end


(* Map whose keys are valuations, and values are polymorphic *)
module ValuationMap = SliceableMap.Make(struct type t = const_t let to_string = string_of_int end)

(* ValuationMap whose values are aggregates *)
module AggregateMap =
struct
   module VM = ValuationMap

   type t = int VM.t

   let string_of_aggregate = string_of_int

   (* Slice methods for calculus evaluation *)

   (* assumes add has replace semantics, so that fold & add removes duplicates
    * when starting with an empty slice.
    * returns new slice, does not modify arg slice *)
   let aggregate (m: t) : t =
      try
      let aux secondaries k v nm = 
         let v2 = if(VM.mem k nm) then VM.find k nm else 0 in
         VM.add k (v + v2) nm
      in 
      VM.fold aux (VM.empty_map_w_secondaries m) m
      with Failure x -> failwith ("aggregate: "^x)

   (* assumes slice data structure can contain duplicates following
    * a merge. Slice contains dups until aggregated. *)
   (* TODO: optimize double new slice creation in merge, and then in aggregate *)
   let combine (m1: t) (m2: t) : t = aggregate (VM.union m1 m2)

   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let extend_keys (current_outv: var_t list) (desired_outv: var_t list)
         (theta: Valuation.t) (extensions: var_t list) (m: t) =
      let key_refiner k v =
         let ext_theta = Valuation.extend
            (Valuation.make current_outv k) theta extensions in
         (* normalizes the key orderings to that of the variable ordering outv. *)
         let new_k = Valuation.apply ext_theta desired_outv in
         (new_k, v)
   in
      (* in the presence of bigsum variables, there may be duplicates
         after key_refinement. These have to be summed up using aggregate. *)
      (* TODO: optimize double slice creation, in map and then aggregate *)
      aggregate (VM.mapi key_refiner m)
      
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
   (*
       print_string ("Updating the database: "^mapn^"->"^
                      (Util.list_to_string string_of_int inv_imgs)^"->"^
                      (Slice.slice_to_string slice));
   *)
       let m = DBM.find [mapn] db in
          ignore(DBM.add [mapn]
             (try VM.add inv_imgs slice m
              with Failure x -> failwith ("update: "^x)) db)
            
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
       DBM.fold (fun _ k v acc -> acc@[(k, (show_f v))]) [] db
    
    let showdb db = showdb_f showmap db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map db)
end


(* M3 preparation and compilation *)
module M3P = M3.Prepared

let rec calc_schema calc =
   let op c1 c2 = Util.ListAsSet.union (calc_schema c1) (calc_schema c2)
   in
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

let prepare_triggers (triggers : trig_t list) : (M3P.ptrig_t list * pattern_map) =
   let pcalc_id = ref 0 in
   let prepare() = incr pcalc_id; !pcalc_id in
      
   let rec prepare_op f (theta_vars : var_t list) (c1: calc_t) (c2: calc_t)
         : (M3P.pcalc_t * pattern_map) =
      let aux2 c ldef defv theta_ext =
         let id = prepare() in
         let outv = calc_schema c in
         let ext = Util.ListAsSet.diff
            (if ldef then defv else outv) (if ldef then outv else defv) in
         let (pc, pm) = prepare_calc (theta_vars@theta_ext) c in
         let _ = Valuation.add_extension (Valuation.pos_extension(id)) ext
         in ((pc, id), (outv, ext), pm)
      in
      let ((pc1, pc1_id), (c1_outv, c1_ext), p1_patterns) =
         aux2 c1 false theta_vars [] in
      let (pcid2, _, p2_patterns) = aux2 c2 true c1_outv c1_ext in
      let patterns = merge_pattern_maps p1_patterns p2_patterns in
         (f (pc1, pc1_id) pcid2, patterns) 

   and prepare_calc (theta_vars : var_t list) (calc : calc_t)
         : (M3P.pcalc_t * pattern_map) =
      match calc with
        | Const(c)      -> (M3P.Const(c), empty_pattern_map())
        | Var(v)        -> (M3P.Var(v), empty_pattern_map())
        | Add  (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Add  (e1, e2)) theta_vars c1 c2
        | Mult (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Mult (e1, e2)) theta_vars c1 c2
        | Leq  (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Leq  (e1, e2)) theta_vars c1 c2
        | Eq   (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Eq   (e1, e2)) theta_vars c1 c2
        | Lt   (c1,c2)  -> prepare_op (fun e1 e2 -> M3P.Lt   (e1, e2)) theta_vars c1 c2
        | IfThenElse0 (c1,c2) -> prepare_op
           (fun e1 e2 -> M3P.IfThenElse0 (e2, e1)) theta_vars c2 c1
        
        | MapAccess (mapn, inv, outv, init_calc) ->
(*
           print_endline ("prepare init: "^(vars_to_string outv)^
              " th: "^(vars_to_string theta_vars));
*)
           let maid = prepare() in
           let _ = Valuation.add_extension (Valuation.pos_extension maid)
              (Util.ListAsSet.inter outv theta_vars)
           in
           let (init_pcalc, patterns) = prepare_calc theta_vars init_calc in

           let new_patterns = merge_pattern_maps patterns
              (singleton_pattern_map 
                 (mapn, make_out_pattern outv (Util.ListAsSet.inter theta_vars outv)))
           in 
              (M3P.MapAccess(mapn, inv, outv, (init_pcalc, maid)), new_patterns)
        
        | Null (outv) -> (M3P.Null(outv), empty_pattern_map())
   in

   let prepare_stmt theta_vars (stmt : stmt_t) : (M3P.pstmt_t * pattern_map) =
      let gen_ext ext =
        let id = prepare() in
        ignore(Valuation.add_extension (Valuation.pos_extension(id)) ext);
        (id, ext) 
      in

      let ((lmapn, linv, loutv, init_calc), incr_calc) = stmt in
      let (psid, ps_extensions) = gen_ext (Util.ListAsSet.diff linv theta_vars) in
      let new_theta_vars = theta_vars@ps_extensions in

      let gen_calc_ext calc ext ext_theta =
         let (id, _) = gen_ext ext in
         let (c, patterns) = prepare_calc
            (new_theta_vars@(if ext_theta then ext else [])) calc
         in ((c, id), patterns) 
      in
         
      let (init_cid, init_patterns) = gen_calc_ext init_calc
         (Util.ListAsSet.diff loutv (calc_schema init_calc)) true in
      let (incr_cid, incr_patterns) = gen_calc_ext incr_calc
         (Util.ListAsSet.inter loutv new_theta_vars) false
      in
      let pstmt = (((lmapn, linv, loutv, init_cid), incr_cid), psid) in

      let extra_patterns = List.fold_left add_pattern (empty_pattern_map()) [
         (lmapn, (make_in_pattern linv (Util.ListAsSet.inter theta_vars linv)));
         (lmapn, (make_out_pattern loutv (Util.ListAsSet.inter theta_vars loutv))) ]
      in
      let patterns = List.fold_left merge_pattern_maps (empty_pattern_map())
        ([ init_patterns; incr_patterns ]@[extra_patterns])
      in
         (pstmt, patterns)
   in

   let prepare_block (trig_args : var_t list) (bl : stmt_t list)
         : (M3P.pstmt_t list * pattern_map) =
      let (pbl, patsl) = List.split (List.map (prepare_stmt trig_args) bl) in
      let patterns = List.fold_left merge_pattern_maps (empty_pattern_map()) patsl
      in (pbl, patterns)
   in

   let prepare_trig (t : trig_t) : (M3P.ptrig_t * pattern_map) =
      let (ev,rel,args,block) = t in
      let (pblock, patterns) = prepare_block args block in
         ((ev, rel, args, pblock), patterns)
   in
   let (pblocks, pms) = List.split (List.map prepare_trig triggers) in
      (pblocks, List.fold_left merge_pattern_maps (empty_pattern_map()) pms)


let rec pcalc_to_string calc =
   let ots op e1 e2 = op^"("^(pcalc_to_string (fst e1))^", "^(pcalc_to_string (fst e2))^")"
   in
   match calc with
      M3P.MapAccess(mapn, inv, outv, init_ecalc) ->
         "MapAccess("^mapn^", "^(vars_to_string inv)^", "^
         (vars_to_string outv)^", "^(pcalc_to_string (fst init_ecalc))^")"
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
   let op c1 c2 = Util.ListAsSet.union (pcalc_schema (fst c1)) (pcalc_schema (fst c2))
   in
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

      
(* Simple OCaml code generator *)
let rec compile_pcalc patterns incr_calc =
   let int_op op x y = if (op x y) then 1 else 0 in
   let compile_op op m1 m2 =
      let aux ecalc = (pcalc_schema (fst ecalc),
         (try
         Valuation.find_extension (Valuation.pos_extension (snd ecalc))
         with Not_found -> failwith "compile_op"),
         compile_pcalc patterns (fst ecalc)) in
      let (outv1, theta_ext, cm1) = aux m1 in
      let (outv2, schema_ext, cm2) = aux m2 in
      let schema = Util.ListAsSet.union outv1 outv2 in
      (fun theta db ->
         let res1 = cm1 theta db in
         let f _ k v1 r =
            (* extend with out vars from LHS calc. This is for bigsum vars in IfThenElse0 *)
            let th = Valuation.extend theta (Valuation.make outv1 k) theta_ext in
            let r2 = cm2 th db in
            (* perform cross product, extend out vars (slice key) to schema *)
            let r3 = AggregateMap.extend_keys outv2 schema th schema_ext
               (ValuationMap.map (fun v2 -> op v1 v2) r2)
            in
               (* r, r3 have no overlap -- safe to union slices.
                * This will also union any secondary indexes. *)
               ValuationMap.union r r3
         in
         ValuationMap.fold f (ValuationMap.empty_map_w_secondaries res1) res1)
   in
   let ccalc =
      match incr_calc with
        M3P.MapAccess(mapn, inv, outv, init_ecalc) ->
            let cinit = compile_pcalc2 patterns outv init_ecalc in
            let out_patterns = get_out_patterns patterns mapn in
            (* Note: we extend theta for init value comp with bound out vars,
             * so we can use them for the partial key here *)
            let patv = try Valuation.find_extension
                  (Valuation.pos_extension (snd init_ecalc))
               with Not_found -> failwith "compile_pcalc MapAccess"
            in
            let pat = List.map (index outv) patv in
            (fun theta db ->
            let m = Database.get_map mapn db in
            (* yna note: all in vars must be bound for lookup *)
            let inv_imgs = Valuation.apply theta inv in
            let slice2 =
               if (ValuationMap.mem inv_imgs m) then ValuationMap.find inv_imgs m
               else
                  let init_slice = cinit theta db in
                  let init_slice_w_indexes = List.fold_left
                     ValuationMap.add_secondary_index init_slice out_patterns
                  in
                     Database.update mapn inv_imgs init_slice_w_indexes db;
                     init_slice_w_indexes
            in
               (* yna note: this can be a slice access for outv not in theta *)
               (* We must remove secondary indexes from lookups during calculus, 
                * evaluation, since we don't want them propagated around. *)
               let pkey = Valuation.apply theta patv in
               let lookup_slice = ValuationMap.slice pat pkey slice2 in
                  (ValuationMap.strip_indexes lookup_slice))

      | M3P.Add (c1, c2) -> compile_op ( + )         c1 c2
      | M3P.Mult(c1, c2) -> compile_op ( * )         c1 c2
      | M3P.Lt  (c1, c2) -> compile_op (int_op (< )) c1 c2
      | M3P.Leq (c1, c2) -> compile_op (int_op (<=)) c1 c2
      | M3P.Eq  (c1, c2) -> compile_op (int_op (= )) c1 c2
      | M3P.IfThenElse0(c1, c2) ->
           compile_op (fun v cond -> if (cond<>0) then v else 0) c2 c1
        
      | M3P.Null(outv) -> (fun theta db -> ValuationMap.empty_map())
      | M3P.Const(i)   -> (fun theta db -> ValuationMap.from_list [([], i)] [])
      | M3P.Var(x)     ->
         (fun theta db ->
            if (not (Valuation.bound x theta)) then
               failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
            else let v = Valuation.value x theta in ValuationMap.from_list [([v], v)] [])
      
      in
      (fun theta db ->
(*
       print_string("\neval_pcalc "^(pcalc_to_string incr_calc)^
                " "^(Valuation.to_string theta)
                ^" "^(Database.db_to_string db)^"   ");
*)
       ccalc theta db)

and compile_pcalc2 patterns lhs_outv ecalc =
   let rhs_outv = pcalc_schema (fst ecalc) in
   let ccalc = compile_pcalc patterns (fst ecalc) in
   (* project slice w/ rhs schema to lhs schema, extending by out vars that are bound *)
   let rhs_extensions =
      try
         Valuation.find_extension (Valuation.pos_extension (snd ecalc))
         with Not_found -> failwith "eval_pcalc2 rhs"
   in
      (fun theta db ->
       (* There may be loop vars in lhs_outv, i.e. they are not in theta,
        * thus evaluating the incr calc outputs a slice.
        * Note all in vars that are loop vars are bound in theta here. *)
       let slice0 = ccalc theta db in
(*
       print_string ("End of PCALC; outv="^(vars_to_string rhs_outv)^
                 " slice="^(Slice.slice_to_string slice0)^
                 (" db="^(Database.db_to_string db))^
                 (" theta="^(Valuation.to_string theta)));
*)
       AggregateMap.extend_keys rhs_outv lhs_outv theta rhs_extensions slice0)

let compile_pstmt patterns
   (((lhs_mapn, lhs_inv, lhs_outv, init_ecalc), incr_ecalc), psid)
=
   let cincr = compile_pcalc2 patterns lhs_outv incr_ecalc in
   let cinit = compile_pcalc2 patterns lhs_outv init_ecalc in
   (* extend init theta by out vars that are not in the init calc, i.e. are bound *)
   let theta_extensions =
      try
      Valuation.find_extension (Valuation.pos_extension (snd init_ecalc))
      with Not_found -> failwith "eval_pstmt init"
   in
      (fun theta current_slice db ->
(*
       print_string("\nPSTMT (th="^(Valuation.to_string theta)
       ^", db=_, stmt=(("^lhs_mapn^" "^(Util.list_to_string (fun x->x) lhs_inv)^" "
              ^(Util.list_to_string (fun x->x) lhs_outv)^" _), _))\n");
*)
       let delta_slice = cincr theta db in
       let f2 k v2 = 
          let theta2 = Valuation.extend
             theta (Valuation.make lhs_outv k) theta_extensions
          in
(*
          print_string ("@PSTMT.init{th="^(Valuation.to_string theta2)
                    ^", key="^(Util.list_to_string string_of_int k)
                    ^", db="^(Database.db_to_string db));
*)
          let a = cinit theta2 db in
          v2 + (if (ValuationMap.mem k a) then (ValuationMap.find k a) else 0)
       in
       let new_slice = ValuationMap.merge_rk
          (fun v->v) f2 (fun v1 v2 -> v1+v2) current_slice delta_slice
       in
       new_slice)

let compile_pstmt_loop patterns trig_args pstmt =
   let (((lhs_mapn, lhs_inv, lhs_outv, _), _), lhs_pid) = pstmt in
   (* extend by those in vars that are loop vars *)
   let theta_extensions = 
      try
      Valuation.find_extension (Valuation.pos_extension lhs_pid)
      with Not_found -> failwith "eval_pstmt_loop theta_extension"
   in
   let cstmt = compile_pstmt patterns pstmt in
   let patv = Util.ListAsSet.inter lhs_inv trig_args in
   let pat = List.map (index lhs_inv) patv in
   let direct = (List.length patv) = (List.length lhs_inv) in
   let out_patv = Util.ListAsSet.inter lhs_outv trig_args in
   let out_pat = List.map (index lhs_outv) out_patv  in
      (fun theta db ->
         let lhs_map = Database.get_map lhs_mapn db in
         let iifilter db inv_img =
           let inv_theta = Valuation.make lhs_inv inv_img in
           let new_theta = Valuation.extend theta inv_theta theta_extensions in
           let slice = ValuationMap.find inv_img lhs_map in
           (* Subslice based on bound out vars *)
           (* TODO: what if there are no loop out vars?
            * optimize for non-slice calculus evaluation *)
           (* Partitioning to work slice is not cheaper for now, since
            * creating the unchanged partition creates a new slice, which
            * is essentially the same as in merge *)
           (*
           let out_pkey = Valuation.apply theta out_patv in
           let (work_slice, unchanged_slice) =
              ValuationMap.partition_slice out_pat out_pkey slice in
           let new_slice = ValuationMap.union
              unchanged_slice (cstmt new_theta work_slice db)
           *)
           let new_slice = cstmt new_theta slice db
           in Database.update lhs_mapn inv_img new_slice db
         in
         let pkey = Valuation.apply theta patv in
         let inv_imgs = if direct then [pkey]
                        else ValuationMap.slice_keys pat pkey lhs_map
         in List.iter (iifilter db) inv_imgs 
      )

let compile_ptrig (ptrig, patterns) =
   let aux ptrig =
      let (_, _, trig_args, pblock) = ptrig in
      let cblock = List.map (compile_pstmt_loop patterns trig_args) pblock in
      (fun tuple db ->
        let theta = Valuation.make trig_args tuple in
             List.iter (fun cstmt -> cstmt theta db) cblock)
   in
      List.map aux ptrig
