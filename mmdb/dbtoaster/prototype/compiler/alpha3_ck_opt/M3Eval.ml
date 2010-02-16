open M3


(* General map types and helper functions *)

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
   type t = int StringMap.t

   (* Note: ordered result *)
   let make (vars: var_t list) (values: const_t list) : t =
      StringSMap.from_list (List.combine vars values)

   (* Note: ordered result *)
   let to_list m = StringMap.fold (fun s n l -> (s,n)::l) m []

   let bound var theta = StringMap.mem var theta
   
   let value var theta = StringMap.find var theta

   let consistent (m1: t) (m2: t) : bool =
      List.for_all (fun (k,v) ->
        (not(StringMap.mem k m2)) || ((StringMap.find k m2) = v))
        (to_list m1)

   let safe_add k v m =
      if ((StringMap.mem k m) && ((StringMap.find k m) <> v)) then
         failwith "Valuation.safe_add"
      else StringMap.add k v m

   (* adds m2 to m1.  assumes that m1 and m2 are consistent. *)
   (* Note: ordered result *)
   (* Note: consistency check in safe_add can be avoided if m1 and m2
    * are disjoint, in which case we can just merge *)
   let combine (m1: t) (m2: t) : t = StringMap.fold safe_add m1 m2

   let apply (m: t) (l: key list) = List.map (fun x -> StringMap.find x m) l

   (* TODO: does order matter here? *)
   let to_string (theta:t) : string =
      StringSMap.to_string (fun x->x) (fun v -> (string_of_int v)) theta
end


module ValuationMap =
struct
   type 'a t = 'a ListMap.t

   let make = ListSMap.from_list

   let aggregate (l: (ListMap.key * int) list) : 'a t =
      let aux m (k,v) = 
         let v2 = if(ListMap.mem k m) then ListMap.find k m
                  else 0 in
         ListMap.add k (v + v2) m
      in 
      List.fold_left aux ListMap.empty l

   let combine (sl1: 'a t) (sl2: 'a t) : 'a t =
      aggregate ((ListSMap.to_list sl1)@(ListSMap.to_list sl2))

   let key_to_string k = Util.list_to_string string_of_int k

   (* assumes const_t is int *)
   let to_string (val_to_string_f: 'a -> string) (m: 'a ListMap.t) =
      ListSMap.to_string key_to_string val_to_string_f m

   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let complete_keys current_outv desired_outv theta slice =
      let key_refiner (k,v) =
         (* TODO: simplify combine if theta, current_outv are disjoint,
          * or already consistent *)
         let ext_theta = Valuation.combine theta
                                           (Valuation.make current_outv k)
                         (* normalizes the key orderings to that of the
                            variable ordering outv. *)
         in
         let new_k = Valuation.apply ext_theta desired_outv in
         (new_k, v)
   in
      (* in the presence of bigsum variables, there may be duplicates
         after key_refinement. These have to be summed up using aggregate. *)
   aggregate (List.map key_refiner (ListSMap.to_list slice))

   let filter (outv: var_t list) (theta: Valuation.t) (slice: 'a t) : ('a t) =
      let aux k v = (Valuation.consistent theta (Valuation.make outv k))
      in
      ListSMap.filteri aux slice
end


type slice_t = int ListMap.t
type map_t   = slice_t ListMap.t (* nested map *)

let slice_to_string slice = ValuationMap.to_string string_of_int slice 
let nmap_to_string  m     = ValuationMap.to_string slice_to_string m

let vars_to_string vs = Util.list_to_string (fun x->x) vs

let lshowmap  m =   ListSMap.to_list (  ListMap.map ListSMap.to_list m)

module Database =
struct
    type db_t    = (string, map_t) Hashtbl.t

    let concat_string s t delim =
        (if (String.length s = 0) then "" else delim)^t

    (* TODO: is fold order important here? *)
    let db_to_string (db:db_t) : string =
        let to_string key_to_string val_to_string db =
            let elem_to_string k v = (key_to_string k)^"->"^(val_to_string v) in
                Hashtbl.fold
                    (fun k v s -> concat_string s " " (elem_to_string k v)) db ""
        in
        "["^(to_string (fun x->x) nmap_to_string db)^"]"

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
                ListSMap.from_list [([], ListSMap.from_list [])]
                else ListSMap.from_list []))
        in
            from_list (List.map f schema)
    
    
    let make_empty_db_wdom schema (dom: const_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let map = ListSMap.from_list
                        (List.map (fun t -> (t, ListSMap.from_list []))
                            (Util.k_tuples (List.length itypes) dom))
            in
            (mapn, map)
        in
            from_list (List.map f schema)

    let update_db mapn inv_imgs slice db : unit =
    (*
       print_string ("Updating the database: "^mapn^"->"^
                      (Util.list_to_string string_of_int inv_imgs)^"->"^
                      (slice_to_string slice));
    *)
        let m = Hashtbl.find db mapn in
            Hashtbl.replace db mapn (ListMap.add inv_imgs slice m)
            
    let get_map mapn db =
        try Hashtbl.find db mapn
        with Not_found -> failwith ("Database.get_map "^mapn)
        
    (* TODO: is fold order important here? *)
    let showdb db =
        Hashtbl.fold (fun k v acc -> acc@[(k, (lshowmap v))]) db []
end;;

open Database;;


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

(*
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
    | IfThenElse0(c1, c2) -> op c1 c2
    | Null(outv)          -> outv
    | Const(i)            -> []
    | Var(x)              -> [x]
*)



(* Evaluates an expression, i.e. the right-hand side of an M3 statement.
   The resulting slice is consistent with theta.
   May extend other maps of the database through initial value computation
   on expression leaves.
*)
let rec eval_calc (incr_calc: calc_t)
                  (theta: Valuation.t) (db: db_t)
                  : (var_t list * slice_t) =
(*
   print_string("\neval_calc "^(calc_to_string incr_calc)^
                " "^(Valuation.to_string theta)
                          ^" "^(db_to_string db)^"   ");
*)
   (* compose: evaluate m1 first, and run m2 on the resulting database.
      Note: databases are now mutable, thus m1 will update the db in place *)
   let rec do_op op (m1:calc_t) (m2:calc_t) (theta:Valuation.t) (db:db_t)
             : (var_t list * slice_t) =
      let (outv1, res1) = eval_calc m1 theta db in
      let f (_, r) (k,v1) =
         (* TODO: simplify combine if theta, outv1 are disjoint, or already consistent *)
         let th = Valuation.combine theta (Valuation.make outv1 k) in
         let (o2, r2) = eval_calc m2 th db in
         let schema = Util.ListAsSet.union outv1 o2 in
         let r3 = ValuationMap.complete_keys o2 schema th
                     (ListMap.map (fun v2 -> op v1 v2) r2)
         in
         (schema, (ListSMap.combine r r3)) (* r, r3 have no overlap *)
      in
      List.fold_left f ([], (ListSMap.from_list []))
                     (ListSMap.to_list res1)
         (* the variable ordering is not necessarily the same as in
            the map outv spec. *)
   in
   (* check a condition *)
   let int_op op x y = if (op x y) then 1 else 0
   in
   match incr_calc with
      MapAccess(mapn, inv, outv, init_calc) ->
      (
         let m        : map_t        = get_map mapn db in
         let inv_imgs : const_t list = Valuation.apply theta inv
         in
         let (rhs_outv, (slice2: slice_t)) = (
            if (ListMap.mem inv_imgs m) then
                (outv, (ListMap.find inv_imgs m))
            else
                (* initial value comp'n for leaves of the expression tree. *)
                let init_slice = eval_calc2 outv init_calc theta db
                in
                    update_db mapn inv_imgs init_slice db;
                    (outv, init_slice)
         ) in
         let slice3 = (ValuationMap.filter rhs_outv theta slice2)
         in
         (rhs_outv, slice3)
      )
    | Add (c1, c2)        -> do_op ( + )         c1 c2 theta db
    | Mult(c1, c2)        -> do_op ( * )         c1 c2 theta db
    | Lt  (c1, c2)        -> do_op (int_op (< )) c1 c2 theta db
    | Leq (c1, c2)        -> do_op (int_op (<=)) c1 c2 theta db
    | Eq  (c1, c2)        -> do_op (int_op (= )) c1 c2 theta db
    | IfThenElse0(c1, c2) -> do_op (fun v cond -> if (cond<>0) then v else 0)
                                   c2 c1 theta db
    | Null(outv)          -> (outv, ListMap.empty)
    | Const(i)            -> ([], ListSMap.from_list [([], i)])
    | Var(x)              -> if (not (Valuation.bound x theta)) then
                                  failwith ("CALC/case Var("^x^"): theta="^
                                            (Valuation.to_string theta))
(*
                                  ([x], ListMap.empty)
*)
                             else let v = Valuation.value x theta in
                                  ([x], ListSMap.from_list [([v], v)])
                             (* note: x is not necessarily an out-var!
                                remove from the slice keys later! *)


(* postprocess the results of eval_calc.
   extends the keys of slice back to the signature of the lhs map *)
and eval_calc2 lhs_outv (calc: calc_t) (theta: Valuation.t) (db: db_t)
                : slice_t =
   let (rhs_outv, slice0) = (eval_calc calc theta db) in
(*
   print_string ("End of CALC; outv="^(vars_to_string rhs_outv)^
                 " slice="^(slice_to_string slice0)^
                 ("db="^(db_to_string db)));
*)
   let slice1 = ValuationMap.complete_keys rhs_outv lhs_outv theta slice0
   in
   slice1


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
      (* TODO: simplify combine if theta, lhs_outv are disjoint *)
      let theta2 = Valuation.combine theta (Valuation.make lhs_outv k)
      in
(*
      print_string ("@STMT.init{th="^(Valuation.to_string theta2)
                    ^", key="^(Util.list_to_string string_of_int k)
                    ^", db="^(db_to_string db));
*)
      let a = (eval_calc2 lhs_outv init_calc theta2 db)
      in
      if (ListMap.mem k a) then (ListMap.find k a)+v2
      else
(*
         failwith "ARGH, initial value computation did not find value"
*)
         v2 + (if (ListMap.mem k a) then (ListMap.find k a) else 0)
   in
   let new_slice = ListSMap.mergei (fun k v -> v) f2 (fun k v1 v2 -> v1+v2)
                                   current delta
   in
   new_slice



let eval_stmt_loop stmt (theta: Valuation.t) (db: db_t) : unit =
   let ((lhs_mapn, lhs_inv, _, _), _) = stmt in
(*
   print_string("\nSTMT-LOOP "^(Valuation.to_string theta)
        ^" <db> (("^lhs_mapn^", "^(Util.list_to_string (fun x->x) lhs_inv)
        ^", <lhs_outv>), <init>), <incr>) --   ");
*)
   let lhs_map = get_map lhs_mapn db in
   let ii_filter db inv_img : unit =
      let inv_theta  = (Valuation.make lhs_inv inv_img) in
      (if (Valuation.consistent theta inv_theta) then
         let slice = ListMap.find inv_img lhs_map in
         let new_slice =
            (* TODO: simplify combine if theta, inv_theta are disjoint *)
            (eval_stmt stmt (Valuation.combine theta inv_theta) slice db)
         in
            update_db lhs_mapn inv_img new_slice db)
   in
   List.iter (ii_filter db) (ListSMap.dom lhs_map)


let eval_trig (trig_args: var_t list) (tuple: const_t list) db block =
    let theta = Valuation.make trig_args tuple in
    let f db stmt = eval_stmt_loop stmt theta db in
        List.iter (f db) block

