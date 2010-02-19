open M3


(* General map types and helper functions *)
let vars_to_string vs = Util.list_to_string (fun x->x) vs

module ListMap = Map.Make (struct
   type t = const_t list
   let compare = compare
end)

module   ListSMap = SuperMap.Make(  ListMap)

(* (PARTIAL) VARIABLE VALUATIONS *)
module Valuation =
struct
   (* the keys are variable names *)
   module StringMap = Map.Make (String)
   module StringSMap = SuperMap.Make(StringMap)

   type key = StringMap.key
   type t = const_t StringMap.t

   type extension_key = string list
   type valuation_extension = string list
   type extension_cache = (extension_key, valuation_extension) Hashtbl.t
   
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


module Slice =
struct
  
   (* Slices need not preserve orderings between entries, thus
    * we can use an unordered data structure such as a Hashtbl *)
   type slice_key = const_t list
   type slice_t   = (slice_key, int) Hashtbl.t

   (* Slice helpers *)
   let empty_slice ()     = Hashtbl.create 10
   let mem  k sl          = Hashtbl.mem sl k
   let find k sl          = Hashtbl.find sl k
   let add  k v sl        = (Hashtbl.replace sl k v; sl)
   let fold f acc sl      = Hashtbl.fold f sl acc

   (* TODO: refactor, common to both slices and maps *)
   let safe_add k v sl =
      if ((mem k sl) && ((find k sl) <> v)) then
         failwith "Slice.safe_add"
      else add k v sl

   (* TODO: refactor, common to both slices and maps *)
   let from_list l = List.fold_left (fun sl (k,v) -> safe_add k v sl) (empty_slice()) l

   (* map applying f over the range of the slice *) 
   let map f sl =
      let aux k v nsl = add k (f v) nsl in
      fold aux (empty_slice()) sl

   (* map that applies f to both keys and values
    * returns a new slice, does not modify args in-place *)
   let map_kv f sl =
      let aux k v nsl = let (nk,nv) = f k v in add nk nv nsl in
         fold aux (empty_slice()) sl

   (* merge two slices, preserving duplicates
    * returns a new slice, does not modify args in-place *)
   let merge sl1 sl2 =
      let aux k v msl = Hashtbl.add msl k v; msl in
      fold aux (Hashtbl.copy sl1) sl2

   let slice_to_list sl = Hashtbl.fold (fun k v l -> (k,v)::l) sl []
   
   let slice_kv_to_string ks vs sl =
      let f k v acc =
        (if acc = "" then "" else acc^" ")^(ks k)^"->"^(vs v)^";"
      in 
      "["^(fold f "" sl)^"]"
   
   (* filter that applies f to both keys and values.
    * assumes add has replace semantics
    * returns new slice, does not modify arg slice *)
   let slice_filter_kv f sl =
      let aux k v nsl = if (f k v) then add k v nsl else nsl in
         fold aux (empty_slice()) sl
   
   (* assumes key is an int list, i.e. const_t is int *) 
   let key_to_string k = Util.list_to_string Valuation.string_of_const_t k

   (* assumes const_t is int *)
   let slice_to_string (slice : slice_t) =
      slice_kv_to_string key_to_string string_of_int slice
   
   (* Slice methods for calculus evaluation *)

   (* assumes add has replace semantics, so that fold & add removes duplicates
    * when starting with an empty slice.
    * returns new slice, does not modify arg slice *)
   let aggregate (sl: slice_t) : slice_t =
      let aux k v nsl = 
         let v2 = if(mem k nsl) then find k nsl else 0 in
         add k (v + v2) nsl
      in 
      fold aux (empty_slice()) sl

   (* assumes slice data structure can contain duplicates following
    * a merge. Slice contains dups until aggregated. *)
   (* TODO: optimize double new slice creation in merge, and then in aggregate *)
   let combine (sl1: slice_t) (sl2: slice_t) : slice_t =
      aggregate (merge sl1 sl2)

   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let extend_keys current_outv desired_outv theta extensions slice =
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
      aggregate (map_kv key_refiner slice)

   (* filters slice entries to those consistent in terms of given variables, and theta *) 
   let filter (outv: var_t list) (theta: Valuation.t) (slice: slice_t) : slice_t =
      let aux k v = (Valuation.consistent theta (Valuation.make outv k))
      in
      slice_filter_kv aux slice

   (* Merges two slices, reconciling entries to a single entry.
    * Assumes each individual slice has unique entries.
    * Merges in two phases, first phase transforms and reconciles if necessary,
    * second phase ignores entries already reconciled, transforms and
    * merges remaining. *)
   (* TODO: implement ignores with a hashset/bitvector *)
   (* yna note: in our usage, f2 is much more expensive to evaluate than f1,
    * since f2 blindly performs initial value computation. Thus we should
    * invoke f2 as few times as possible, making it the slice we ignore
    * whenever we can *)
   let mergei f1 f2 f12 sl1 sl2 =
      let aux f sl k v (ignores, msl) =
         if mem k sl then
            (* reconcile and ignore later *)
            (add k 0 ignores, add k (f12 k (find k sl) v) msl)
         else (ignores, add k (f k v) msl)
      in
      let aux2 f ignores k v msl =
        if mem k ignores then msl else add k (f k v) msl
      in
      let (ignores, partial) =
        fold (aux f1 sl2) (empty_slice(), empty_slice()) sl1
      in
         fold (aux2 f2 ignores) partial sl2   
end

module DbMap =
struct
   type map_t = Slice.slice_t ListMap.t
   
   let dbmap_map          = ListMap.map
   let dbmap_to_list      = ListSMap.to_list
   let dbmap_kv_to_string = ListSMap.to_string 
   
   let mem  = ListMap.mem
   let find = ListMap.find
   let add  = ListMap.add
   let fold = ListMap.fold
   
   (* Map construction *)
   let empty_map () = ListMap.empty

   (* TODO: refactor, common to both slices and maps *)
   let safe_add k v m =
      if ((mem k m) && ((find k m) <> v)) then
         failwith "DbMap.safe_add"
      else add k v m

   (* TODO: refactor, common to both slices and maps *)
   let from_list l = List.fold_left (fun m (k,v) -> safe_add k v m) (empty_map()) l
   
   (* Map methods for calculus evaluation *)

   let dom (m: map_t) = let aux k v nd = (k::nd) in fold aux m []

   (* assumes key is an int list, i.e. const_t is int *) 
   let key_to_string k = Util.list_to_string string_of_int k

   (* assumes const_t is int *)
   let nmap_to_string (m: map_t) =
      dbmap_kv_to_string key_to_string Slice.slice_to_string m
      
   let showmap (m : map_t) = dbmap_to_list (dbmap_map Slice.slice_to_list m)
   
   let show_sorted_map (m : map_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))
end

module Database =
struct
    type db_t    = (string, DbMap.map_t) Hashtbl.t

    let concat_string s t delim =
        (if (String.length s = 0) then "" else delim)^t

    let db_to_string (db:db_t) : string =
        let to_string key_to_string val_to_string db =
            let elem_to_string k v = (key_to_string k)^"->"^(val_to_string v) in
                Hashtbl.fold
                    (fun k v s -> concat_string s " " (elem_to_string k v)) db ""
        in
        "["^(to_string (fun x->x) DbMap.nmap_to_string db)^"]"

    let safe_add db k v =
        begin
            (if (Hashtbl.mem db k) && ((Hashtbl.find db k) <> v) then
                failwith "Database.safe_add"
            else Hashtbl.replace db k v);
            db
        end

    let from_list l = 
        List.fold_left (fun db (k,v) -> safe_add db k v) (Hashtbl.create 10) l

    let make_empty_db schema: db_t =
        let f (mapn, itypes, _) =
            (mapn, (if(List.length itypes = 0) then
                DbMap.from_list [([], Slice.from_list [])]
                else DbMap.from_list []))
        in
            from_list (List.map f schema)
    
    
    let make_empty_db_wdom schema (dom: const_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let map = DbMap.from_list
                        (List.map (fun t -> (t, Slice.from_list []))
                            (Util.k_tuples (List.length itypes) dom))
            in
            (mapn, map)
        in
            from_list (List.map f schema)

    let update mapn inv_imgs slice db : unit =
    (*
       print_string ("Updating the database: "^mapn^"->"^
                      (Util.list_to_string string_of_int inv_imgs)^"->"^
                      (Slice.slice_to_string slice));
    *)
        let m = Hashtbl.find db mapn in
            Hashtbl.replace db mapn (DbMap.add inv_imgs slice m)
            
    let get_map mapn db =
        try Hashtbl.find db mapn
        with Not_found -> failwith ("Database.get_map "^mapn)
        
    let showdb_f show_f db =
        Hashtbl.fold (fun k v acc -> acc@[(k, (show_f v))]) db []
    
    let showdb db = showdb_f DbMap.showmap db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f DbMap.show_sorted_map db)
end;;

(* type aliases *)
type slice_t = Slice.slice_t
type map_t   = DbMap.map_t
type db_t    = Database.db_t


(* THE INTERPRETER *)

let rec calc_to_string calc =
   let ots op c1 c2 = op^"("^(calc_to_string c1)^", "^(calc_to_string c2)^")"
   in
   match calc with
      MapAccess(mapn, inv, outv, init_calc) ->
         "MapAccess("^mapn^", "^(vars_to_string inv)^", "^
         (vars_to_string outv)^", "^(calc_to_string init_calc)^")"
    | Add (c1, c2)        -> ots "Add" c1 c2
    | Mult(c1, c2)        -> ots "Mult" c1 c2
    | Lt  (c1, c2)        -> ots "Lt" c1 c2
    | Leq (c1, c2)        -> ots "Leq" c1 c2
    | Eq  (c1, c2)        -> ots "Eq" c1 c2
    | IfThenElse0(c1, c2) -> ots "IfThenElse0" c1 c2
    | Null(outv)          -> "Null("^(vars_to_string outv)^")"
    | Const(i)            -> string_of_int i
    | Var(x)              -> x

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

(* Evaluates an expression, i.e. the right-hand side of an M3 statement.
   The resulting slice is consistent with theta.
   May extend other maps of the database through initial value computation
   on expression leaves.
*)
let rec eval_calc (incr_calc: calc_t) (theta: Valuation.t) (db: db_t)
                  : slice_t =
(*
   print_string("\neval_calc "^(calc_to_string incr_calc)^
                " "^(Valuation.to_string theta)
                          ^" "^(Database.db_to_string db)^"   ");
*)
   (* compose: evaluate m1 first, and run m2 on the resulting database.
      Note: databases are now mutable, thus m1 will update the db in place *)
   let rec do_op op (m1:calc_t) (m2:calc_t) (theta:Valuation.t) (db:db_t)
             : slice_t =

      let outv1 = calc_schema m1 in
      let outv2 = calc_schema m2 in
      let schema = Util.ListAsSet.union outv1 outv2 in
      let theta_extensions = Util.ListAsSet.diff outv1 (Valuation.vars theta) in
      let schema_extensions = Util.ListAsSet.diff outv1 outv2 in
      
      let res1 = eval_calc m1 theta db in
      
      let f r (k,v1) =
         
         (* TODO: add positional information to M3 AST. *)
         (* yna note: valuation lookups in expressions seem to need positional information
          * about the calc expr. For example the same calc expr can appear in
          * multiple times in an AST, yet theta will be different for the
          * specific point in the AST, thus may need to be extended less (note
          * that theta is monotonically growing from LHS calc exprs to RHS calc
          * exprs in the AST. For now, we don't bother to optimize for this,
          * since the lookup key is large (size of the calc expr), and doing
          * the lookup may be more expensive than just combining valuations. *)
         (* yna note: there are a few options:
          * i. compute the vars to extend theta once per do_op
          * ii. lookup the vars to extend theta
          * iii. combine as currently.
          * we should use option i. or ii., favoring i. if the lookup is
          * expensive, and/or we don't want to keep positional information.
          * it is still worthwhile to use i. *if* the function "f" is evaluated
          * many times, which may happen because res1 is a slice. in the case
          * res1 is a one-element slice, it doesn't make much difference to
          * use option i. or iii.
          * if we're doing lookups, we can use a key of, both of which can
          * be computed once per do_op:
          *    theta vars * outv1
          * this isn't significantly cheaper than option i., thus I think
          * lookups for calc expressions should only be done with positional
          * information. *)

         let th = Valuation.extend theta (Valuation.make outv1 k) theta_extensions in
         let r2 = eval_calc m2 th db in

         (* Since schema is a superset of outv2, we can just extend the
          * valuations of r2 for vars in (outv1 - outv2), because any overlap of
          * outv2 & th vars is known to be consistent. *)
         (* yna note: valuation lookups.
          * we have the same issue with needing positional information here as
          * above. again we can determine o2 and schema once per do_op call,
          * and use this information in complete_keys. *)

         let r3 = Slice.extend_keys outv2 schema th schema_extensions
                     (Slice.map (fun v2 -> op v1 v2) r2)
         in
            (* r, r3 have no overlap so it should be safe to merge slices *) 
            Slice.merge r r3
      in
      (* TODO: optimize, iterate directly over slice *)
      List.fold_left f (Slice.from_list []) (Slice.slice_to_list res1)
         (* the variable ordering is not necessarily the same as in
            the map outv spec. *)
   in
   (* check a condition *)
   let int_op op x y = if (op x y) then 1 else 0
   in
   match incr_calc with
      MapAccess(mapn, inv, outv, init_calc) ->
      (
         let m        : map_t        = Database.get_map mapn db in
         let inv_imgs : const_t list = Valuation.apply theta inv
         in
         let slice2 : slice_t = (
            if (DbMap.mem inv_imgs m) then
                DbMap.find inv_imgs m
            else
                (* initial value comp'n for leaves of the expression tree. *)
                let init_slice = eval_calc2 outv init_calc theta db
                in
                    Database.update mapn inv_imgs init_slice db;
                    init_slice
         ) in
         (* TODO: can we optimize between rhs_outv and theta?
          * Perhaps even in Slice.filter?
          * The only kind of optimization is a data structure that
          * allows direct retrieval for values of rhs_outv rather than
          * scanning and filtering in Slice.filter *)
         Slice.filter outv theta slice2
      )
    | Add (c1, c2)        -> do_op ( + )         c1 c2 theta db
    | Mult(c1, c2)        -> do_op ( * )         c1 c2 theta db
    | Lt  (c1, c2)        -> do_op (int_op (< )) c1 c2 theta db
    | Leq (c1, c2)        -> do_op (int_op (<=)) c1 c2 theta db
    | Eq  (c1, c2)        -> do_op (int_op (= )) c1 c2 theta db
    | IfThenElse0(c1, c2) -> do_op (fun v cond -> if (cond<>0) then v else 0)
                                   c2 c1 theta db
    | Null(outv)          -> Slice.empty_slice()
    | Const(i)            -> Slice.from_list [([], i)]
    | Var(x)              -> if (not (Valuation.bound x theta)) then
                                  failwith ("CALC/case Var("^x^"): theta="^
                                            (Valuation.to_string theta))
                                  (* Slice.empty_slice() *)
                             else let v = Valuation.value x theta in
                                  Slice.from_list [([v], v)]
                             (* note: x is not necessarily an out-var!
                                remove from the slice keys later! *)

(* postprocess the results of eval_calc.
   extends the keys of slice back to the signature of the lhs map *)
and eval_calc2 lhs_outv (calc: calc_t) (theta: Valuation.t) (db: db_t)
                : slice_t =

   let rhs_outv = calc_schema calc in
   let slice0   = eval_calc calc theta db in
(*
   print_string ("End of CALC; outv="^(vars_to_string rhs_outv)^
                 " slice="^(Slice.slice_to_string slice0)^
                 (" db="^(Database.db_to_string db))^
                 (" theta="^(Valuation.to_string theta)));
*)
   (* Vars in rhs_outv intersect theta are known to be consistent
    * since calc is evaluated with theta. However rhs_outv may contain out vars
    * from the db. Thus we simply need to merge:
    *     lhs_outv cap rhs_outv) with lhs_outv cap theta *)
   (* TODO: this can be computed once per calc expr, but we need positional
    * info per calc expr. *) 
   let rhs_extensions =
      Util.ListAsSet.inter lhs_outv (Valuation.vars theta)
   in
   Slice.extend_keys rhs_outv lhs_outv theta rhs_extensions slice0

(* yna note: theta contains union of all tuple-vars and in-vars *) 
let eval_stmt ((lhs_mapn, lhs_inv, lhs_outv, init_calc), incr_calc)
              (theta: Valuation.t) (current: slice_t) (db: db_t)
              : slice_t =
(*
   print_string("\nSTMT (th="^(Valuation.to_string theta)
   ^", db=_, stmt=(("^lhs_mapn^" "^(Util.list_to_string (fun x->x) lhs_inv)^" "
              ^(Util.list_to_string (fun x->x) lhs_outv)^" _), _))\n");
*)
   let (delta:slice_t) = eval_calc2 lhs_outv incr_calc theta db
   in
   let f2 k v2 =
      (* yna note: valuation lookup: lhs_mapn * lhs_outv *)
      let theta_extensions =
         try Valuation.find_extension(lhs_mapn::lhs_outv)
         with Not_found ->
            Valuation.add_extension (lhs_mapn::lhs_outv)
               (Util.ListAsSet.diff lhs_outv (Valuation.vars theta))
      in
      let theta2 =
         Valuation.extend theta (Valuation.make lhs_outv k) theta_extensions
      in
(*
      print_string ("@STMT.init{th="^(Valuation.to_string theta2)
                    ^", key="^(Util.list_to_string string_of_int k)
                    ^", db="^(Database.db_to_string db));
*)
      let a = (eval_calc2 lhs_outv init_calc theta2 db)
      in
      if (Slice.mem k a) then (Slice.find k a)+v2
      else
(*
         failwith "ARGH, initial value computation did not find value"
*)
         v2 + (if (Slice.mem k a) then (Slice.find k a) else 0)
   in
   let new_slice =
       Slice.mergei (fun k v -> v) f2 (fun k v1 v2 -> v1+v2) current delta
   in
   new_slice


(* TODO: can we use a data structure that lets us get only those map entries
 * whose intersection of in-vars and tuple-vars, match up value-wise,
 * rather than scan+filter *)
let eval_stmt_loop stmt (theta: Valuation.t) (db: db_t) =
   let ((lhs_mapn, lhs_inv, _, _), _) = stmt in
(*
   print_string("\nSTMT-LOOP "^(Valuation.to_string theta)
        ^" <db> (("^lhs_mapn^", "^(Util.list_to_string (fun x->x) lhs_inv)
        ^", <lhs_outv>), <init>), <incr>) --   ");
*)
   let lhs_map = Database.get_map lhs_mapn db in
   let theta_extensions =
      try Valuation.find_extension (lhs_mapn::lhs_inv)
      with Not_found ->
         Valuation.add_extension (lhs_mapn::lhs_inv)
            (Util.ListAsSet.diff lhs_inv (Valuation.vars theta))
   in
   let ii_filter db inv_img =
      let inv_theta = (Valuation.make lhs_inv inv_img) in
      (if (Valuation.consistent theta inv_theta) then
         let new_theta = Valuation.extend theta inv_theta theta_extensions in
         let slice = DbMap.find inv_img lhs_map in
         let new_slice = eval_stmt stmt new_theta slice db in
            Database.update lhs_mapn inv_img new_slice db)
   in
   List.iter (ii_filter db) (DbMap.dom lhs_map)


let eval_trig (trig_args: var_t list) (tuple: const_t list) db block =
    let theta = Valuation.make trig_args tuple in
    let f db stmt = eval_stmt_loop stmt theta db in
        List.iter (f db) block

