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
module ValuationMap = HashMap.Make(struct type t = const_t list end)

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
      let aux k v nm = 
         let v2 = if(VM.mem k nm) then VM.find k nm else 0 in
         VM.add k (v + v2) nm
      in 
      VM.fold aux (VM.empty_map()) m

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

module Database =
struct
   module VM  = ValuationMap
   module AM  = AggregateMap
   module DBM = HashMap.Make(struct type t = string end)
   
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
      DBM.to_string (fun x->x) dbmap_to_string db

    let make_empty_db schema: db_t =
        let f (mapn, itypes, _) =
            (mapn, (if(List.length itypes = 0) then
                VM.from_list [([], VM.from_list [])]
                else VM.empty_map()))
        in
            DBM.from_list (List.map f schema)
    
    
    let make_empty_db_wdom schema (dom: const_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let map = VM.from_list
                        (List.map (fun t -> (t, VM.empty_map()))
                            (Util.k_tuples (List.length itypes) dom))
            in
            (mapn, map)
        in
            DBM.from_list (List.map f schema)
            
    let update mapn inv_imgs slice db : unit =
    (*
       print_string ("Updating the database: "^mapn^"->"^
                      (Util.list_to_string string_of_int inv_imgs)^"->"^
                      (Slice.slice_to_string slice));
    *)
       let m = DBM.find mapn db in
          ignore(DBM.add mapn (VM.add inv_imgs slice m) db)
            
    let get_map mapn db = 
       try DBM.find mapn db
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

let prepare_triggers (triggers : trig_t list) : M3P.ptrig_t list =
   let pcalc_id = ref 0 in
   let prepare() = incr pcalc_id; !pcalc_id in

   let rec prepare_op f (theta_vars : var_t list) (c1: calc_t) (c2: calc_t)
         : M3P.pcalc_t =
      let aux2 c ldef defv theta_ext =
         let id = prepare() in
         let outv = calc_schema c in
         let ext = Util.ListAsSet.diff
            (if ldef then defv else outv) (if ldef then outv else defv) in
         let pc = prepare_calc (theta_vars@theta_ext) c in
         let _ = Valuation.add_extension (Valuation.pos_extension(id)) ext
         in ((pc, id), (outv, ext))
      in
      let ((pc1, pc1_id), (c1_outv, c1_ext)) = aux2 c1 false theta_vars [] in
         f (pc1, pc1_id) (fst (aux2 c2 true c1_outv c1_ext)) 

   and prepare_calc (theta_vars : var_t list) (calc : calc_t)
         : M3P.pcalc_t =
      match calc with
        | Const(c)      -> M3P.Const(c)
        | Var(v)        -> M3P.Var(v)
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
           let init_pcalc = prepare_calc theta_vars init_calc in
              M3P.MapAccess(mapn, inv, outv, (init_pcalc, maid))
        
        | Null (outv) -> M3P.Null(outv)
   in

   let prepare_stmt theta_vars (stmt : stmt_t) : M3P.pstmt_t =
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
         let c = prepare_calc (new_theta_vars@(if ext_theta then ext else [])) calc in
            (c, id) 
      in
         
      let init_cid = gen_calc_ext init_calc
         (Util.ListAsSet.diff loutv (calc_schema init_calc)) true in
      let incr_cid = gen_calc_ext incr_calc
         (Util.ListAsSet.inter loutv new_theta_vars) false
      in
         (((lmapn, linv, loutv, init_cid), incr_cid), psid)
   in

   let prepare_block (trig_args : var_t list) (bl : stmt_t list)
         : M3P.pstmt_t list =
      List.map (prepare_stmt trig_args) bl
   in

   let prepare_trig (t : trig_t) : M3P.ptrig_t =
      let (ev,rel,args,block) = t in (ev,rel,args,prepare_block args block)
   in
      List.map prepare_trig triggers


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
let rec compile_pcalc incr_calc =
   let int_op op x y = if (op x y) then 1 else 0 in
   let compile_op op m1 m2 =
      let aux ecalc = (pcalc_schema (fst ecalc),
         (try
         Valuation.find_extension (Valuation.pos_extension (snd ecalc))
         with Not_found -> failwith "compile_op"),
         compile_pcalc (fst ecalc)) in
      let (outv1, theta_ext, cm1) = aux m1 in
      let (outv2, schema_ext, cm2) = aux m2 in
      let schema = Util.ListAsSet.union outv1 outv2 in
      (fun theta db ->
         let res1 = cm1 theta db in
         let f k v1 r =
            let th = Valuation.extend theta (Valuation.make outv1 k) theta_ext in
            let r2 = cm2 th db in
            let r3 = AggregateMap.extend_keys outv2 schema th schema_ext
               (ValuationMap.map (fun v2 -> op v2 v2) r2)
            in
               (* r, r3 have no overlap -- safe to merge slices *)
               ValuationMap.union r r3
         in
         ValuationMap.fold f (ValuationMap.empty_map()) res1)
   in
   let ccalc =
      match incr_calc with
        M3P.MapAccess(mapn, inv, outv, init_ecalc) ->
            let cinit = compile_pcalc2 outv init_ecalc in
            (fun theta db ->
            let m = Database.get_map mapn db in
            let inv_imgs = Valuation.apply theta inv in
            let slice2 =
               if (ValuationMap.mem inv_imgs m) then ValuationMap.find inv_imgs m
               else
                  let init_slice = cinit theta db in
                     Database.update mapn inv_imgs init_slice db;
                     init_slice 
            in
               AggregateMap.filter outv theta slice2)

      | M3P.Add (c1, c2) -> compile_op ( + )         c1 c2
      | M3P.Mult(c1, c2) -> compile_op ( * )         c1 c2
      | M3P.Lt  (c1, c2) -> compile_op (int_op (< )) c1 c2
      | M3P.Leq (c1, c2) -> compile_op (int_op (<=)) c1 c2
      | M3P.Eq  (c1, c2) -> compile_op (int_op (= )) c1 c2
      | M3P.IfThenElse0(c1, c2) ->
           compile_op (fun v cond -> if (cond<>0) then v else 0) c2 c1
        
      | M3P.Null(outv) -> (fun theta db -> ValuationMap.empty_map())
      | M3P.Const(i)   -> (fun theta db -> ValuationMap.from_list [([], i)])
      | M3P.Var(x)     ->
         (fun theta db ->
            if (not (Valuation.bound x theta)) then
               failwith ("CALC/case Var("^x^"): theta="^(Valuation.to_string theta))
            else let v = Valuation.value x theta in ValuationMap.from_list [([v], v)])
      
      in
      (fun theta db ->
(*
       print_string("\neval_pcalc "^(pcalc_to_string incr_calc)^
                " "^(Valuation.to_string theta)
                ^" "^(Database.db_to_string db)^"   ");
*)
       ccalc theta db)

and compile_pcalc2 lhs_outv ecalc =
   let rhs_outv = pcalc_schema (fst ecalc) in
   let ccalc = compile_pcalc (fst ecalc) in
   let rhs_extensions =
      try
         Valuation.find_extension (Valuation.pos_extension (snd ecalc))
         with Not_found -> failwith "eval_pcalc2 rhs"
   in
      (fun theta db ->
       let slice0 = ccalc theta db in
(*
       print_string ("End of PCALC; outv="^(vars_to_string rhs_outv)^
                 " slice="^(Slice.slice_to_string slice0)^
                 (" db="^(Database.db_to_string db))^
                 (" theta="^(Valuation.to_string theta)));
*)
       AggregateMap.extend_keys rhs_outv lhs_outv theta rhs_extensions slice0)

let compile_pstmt (((lhs_mapn, lhs_inv, lhs_outv, init_ecalc), incr_ecalc), psid) =
   let cincr = compile_pcalc2 lhs_outv incr_ecalc in
   let cinit = compile_pcalc2 lhs_outv init_ecalc in
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
       let new_slice = ValuationMap.mergei
          (fun k v -> v) f2 (fun k v1 v2 -> v1+v2) current_slice delta_slice
       in
       new_slice)

let compile_pstmt_loop pstmt trig_args =
   let (((lhs_mapn, lhs_inv, _, _), _), lhs_pid) = pstmt in
   let theta_extensions = 
      try
      Valuation.find_extension (Valuation.pos_extension lhs_pid)
      with Not_found -> failwith "eval_pstmt_loop theta_extension"
   in
   let cstmt = compile_pstmt pstmt in
      (fun theta db ->
         let lhs_map = Database.get_map lhs_mapn db in
         let iifilter db inv_img =
            let inv_theta = Valuation.make lhs_inv inv_img in
            (if (Valuation.consistent theta inv_theta) then
               let new_theta = Valuation.extend theta inv_theta theta_extensions in
               let slice = ValuationMap.find inv_img lhs_map in
               let new_slice = cstmt new_theta slice db in
                  Database.update lhs_mapn inv_img new_slice db)
         in
         List.iter (iifilter db) (ValuationMap.dom lhs_map))

let compile_ptrig trig_args pblock =
   let f pstmt = compile_pstmt_loop pstmt trig_args in
   let cblock = List.map f pblock in
   (fun tuple db ->
      let theta = Valuation.make trig_args tuple in
         List.iter (fun cstmt -> cstmt theta db) cblock)
