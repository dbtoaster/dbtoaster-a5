
(* General map types and helper functions *)

module StringMap = Map.Make (String);;
(* the keys are variable or map names *)

module ListMap = Map.Make (struct
   type t = const_t list
   let compare = compare
end);;


let list_to_lmap l =
   List.fold_left (fun m (k,v) -> ListMap.add k v m) ListMap.empty l;;

let list_to_smap l =
   List.fold_left (fun m (k,v) -> StringMap.add k v m) StringMap.empty l;;

(* This does not work -- an OCAML bug?
(* if there are duplicate keys, they get overwritten *)
let list_to_map add empty l =
   List.fold_left (fun m (k,v) -> add k v m) empty l;;

let list_to_lmap = list_to_map   (ListMap.add)   (ListMap.empty);;

let list_to_smap = list_to_map (StringMap.add) (StringMap.empty);;
*)


let foldable_to_list fold_f foldable =
   fold_f (fun s n l -> (s,n)::l) foldable [];;

let map_to_list fold_f f m =
   List.map (fun (s,n) -> (s, f n)) (foldable_to_list fold_f m);;


let list_to_string elem_to_string l =
   let tup_f str x = (elem_to_string x)^str
   in
   "["^(List.fold_left tup_f "" l)^"]";;

let map_to_string fold_f key_to_string val_to_string m =
   let tup_f k v str = ("("^(key_to_string k)^", " ^(val_to_string v)^")")^str
   in
   "["^(fold_f tup_f m "")^"]";;

(* assumes const_t is int *)
let lmap_to_string (val_to_string_f: 'a -> string) (m: 'a ListMap.t) =
   map_to_string ListMap.fold (fun k -> list_to_string string_of_int k)
                 val_to_string_f m
;;


(* VARIABLE VALUATIONS *)
module Valuation =
struct
   type t = int StringMap.t

   let make (vars: var_t list) (values: const_t list) : t =
      list_to_smap (List.combine vars values)

   (* assume that theta 1 and 2 agree on the keys they have in common *)
   let combine (theta1:t) (theta2:t) : t =
      StringMap.fold (fun k v t -> StringMap.add k v t) theta1 theta2

   let to_list (theta:t) = map_to_list StringMap.fold (fun x->x) theta

   let to_string (theta:t) = map_to_string StringMap.fold (fun x->x)
                                     (fun v -> (string_of_int v)) theta

   let consistent (theta1: t) (theta2: t) : bool =
      List.for_all (fun (x,y) -> (not(StringMap.mem x theta2)) ||
                                    ((StringMap.find x theta2) = y))
                   (map_to_list StringMap.fold (fun x->x) theta1)

   let apply (theta:t) l = List.map (fun x -> StringMap.find x theta) l

   let dom (theta:t) = let (dom, rng) = List.split (to_list theta) in dom
end;;


type slice_t = int ListMap.t;;
type map_t   = slice_t ListMap.t;;
type db_t    = map_t StringMap.t;;


let sshowmap  m = foldable_to_list StringMap.fold m;;
let showslice m = foldable_to_list   ListMap.fold m;;
let lshowmap  m = map_to_list ListMap.fold   showslice  m;;
let gshowmap  m = map_to_list StringMap.fold lshowmap   m;;


let merge (reconcile_f: 'a -> 'a -> 'a)
          (m1: 'a ListMap.t) (m2: 'a ListMap.t): 'a ListMap.t =
   ListMap.fold (fun k v m -> ListMap.add k
      (if (ListMap.mem k m) then
         (reconcile_f (ListMap.find k m) v)
       else v) m) m1 m2
;;


module Slice =
struct
   type t = slice_t

   let make = list_to_lmap

   let aggregate l : t =
      let aux m (k,v) = 
         let v2 = if(ListMap.mem k m) then ListMap.find k m
                  else 0 in
         ListMap.add k (v+v2) m
      in 
      List.fold_left aux ListMap.empty l

   let merge (sl1:t) (sl2:t) : t = merge (+) sl1 sl2

   let to_string (m:t) : string = list_to_string
      (fun (k,v) -> "("^(list_to_string string_of_int k)^", "
                       ^(string_of_int v)^")")
      (showslice m)

   (* valuations are consistent *)
   let complete_keys current_outv desired_outv theta slice =
      let key_refiner (k,v) =
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
   aggregate (List.map key_refiner (showslice slice))

   (* assumes there is no overlap between the variable domains of the slices. *)
   let product op (* operation for combining values *)
               (sl1:t) (sl2:t) : t =
      let l1 = (showslice sl1) in
      let l2 = (showslice sl2) in
      let aux (k1, v1) = List.map (fun (k2, v2) -> ((k1 @ k2), (op v1 v2))) l2
      in
      make (List.flatten (List.map aux l1))

   let filter (outv: var_t list) (theta: Valuation.t) (slice:t) : t =
      let aux (k,_) = (Valuation.consistent theta (Valuation.make outv k))
      in
      make (List.filter aux (showslice slice))
end;;


let make_empty_db schema: db_t =
   let f (mapn, itypes, rtypes) =
      (mapn, (if(List.length itypes = 0) then
                 list_to_lmap [([], list_to_lmap [])]
              else list_to_lmap []))
   in
   list_to_smap (List.map f schema)
;;

let db_to_string (db:db_t) : string =
   map_to_string StringMap.fold (fun x->x)
                 (fun m -> lmap_to_string Slice.to_string m) db
;;



(* THE INTERPRETER *)


(* Evaluates an expression, i.e. the right-hand side of an M3 statement.
   The resulting slice is consistent with theta.
   May extend other maps of the database through initial value computation
   on expression leaves.
*)
let rec eval_calc (incr_calc: calc_t)
                  (theta: Valuation.t)
                  (db: db_t)
                : (var_t list * slice_t * db_t) =
(*
   print_string("(CALC <incr> "^(Valuation.to_string theta)
                              ^" "^(db_to_string db)^")    ");
*)
   (* do arithmetics *)
   let do_op op m1 m2 =
      let (outv1, res1, db1) = eval_calc m1 theta db in
      let (outv2, res2, db2) = eval_calc m2 theta db1 in
         ((outv1@outv2), (Slice.product op res1 res2), db2)
         (* the variable ordering is not necessarily the same as in
            the map outv spec. *)
   in
   (* check a condition *)
   let int_op op x y = if (op x y) then 1 else 0
   in
   match incr_calc with
      MapAccess(mapn, inv, outv, init_calc) ->
      (
         let m        : map_t        = (StringMap.find mapn db) in
         let inv_imgs : const_t list = Valuation.apply theta inv
         in
         let (rhs_outv, slice2, db2) = (
            if (ListMap.mem inv_imgs m) then
                (outv, (ListMap.find inv_imgs m), db)
            else
                (* initial value comp'n for leaves of the expression tree. *)
                let (init_outv, init_slice, db1) = eval_calc init_calc theta db
                in
                (init_outv, init_slice,
                 StringMap.add mapn (ListMap.add inv_imgs init_slice m) db1)
          ) in
          let slice3 = (Slice.filter rhs_outv theta slice2)
          in
          (rhs_outv, slice3, db2)
      )
    | Add (c1, c2)        -> do_op ( + )         c1 c2
    | Mult(c1, c2)        -> do_op ( * )         c1 c2
    | Lt  (c1, c2)        -> do_op (int_op (< )) c1 c2
    | Leq (c1, c2)        -> do_op (int_op (<=)) c1 c2
    | Eq  (c1, c2)        -> do_op (int_op (= )) c1 c2
    | IfThenElse0(c1, c2) -> do_op (fun x y -> if (x<>0) then y else 0) c1 c2
    | Null(outv)          -> (outv, (Slice.make []), db)
    | Const(i)            -> ([], Slice.make [([], i)], db)
    | Var(x) -> let v = StringMap.find x theta in
                ([x], Slice.make [([v], v)], db)
                (* note: x is not necessarily an out-var! remove from the
                   slice keys later! *)
;;



(* postprocess the results of eval_calc.
   extends the keys of slice back to the signature of the lhs map *)
let eval_calc2 lhs_outv (calc: calc_t) (theta: Valuation.t) (db: db_t)
                : (slice_t * db_t) =
   let (rhs_outv, slice0, db0) = (eval_calc calc theta db) in
(*
   print_string ((list_to_string (fun x->x) lhs_outv)^" vs. "
                ^(list_to_string (fun x->x) rhs_outv));
*)
   let slice1 = Slice.complete_keys rhs_outv lhs_outv theta slice0
   in
   (slice1, db0)
;;


let eval_stmt (theta: Valuation.t) (db: db_t)
              ((lhs_mapn, lhs_inv, lhs_outv, init_calc), incr_calc) : db_t =
(*
   print_string("STMT "^(list_to_string
        (fun (k,v) -> "("^k^", "^(string_of_int v)^")") (sshowmap theta))
        ^" <db> (("^lhs_mapn^" <lhs_inv> <lhs_outv> <init_calc>), incr_calc) --   ");
*)
   let lhs_inv_imgs = Valuation.apply theta lhs_inv in
   let lhs_map      = (StringMap.find lhs_mapn db)
   in
   let (old_slice, db1) =
      if (ListMap.mem lhs_inv_imgs lhs_map) then
         ((ListMap.find lhs_inv_imgs lhs_map), db)
      else
         (* initial value computation. This is the case where information
            is arriving bottom-up in out-vars; possibly some these valuations
            are new. *)
         eval_calc2 lhs_outv init_calc theta db
   in
   let (delta, db2) = eval_calc2 lhs_outv incr_calc theta db1 in
   let new_slice    = Slice.merge old_slice delta in
   let m = ListMap.add lhs_inv_imgs new_slice lhs_map
           (* overwrite; this adds to the slice if it already existed. *)
   in
   StringMap.add lhs_mapn m db2
;;




let eval_stmt_loop (theta: Valuation.t) (db: db_t)
                   ((lhs_mapn, lhs_inv, lhs_outv, init_calc), incr_calc)
                   : db_t =
(*
   print_string("STMT-LOOP "^(list_to_string
        (fun (k,v) -> "("^k^", "^(string_of_int v)^")") (sshowmap theta))
        ^" <db> (("^lhs_mapn^", "^(list_to_string (fun x->x) lhs_inv)
        ^", "^(list_to_string (fun x->x) lhs_outv)^", <init>), <incr>) --   ");
*)
   let lhs_map       = (StringMap.find lhs_mapn db) in
   let (inv_imgs, _) = List.split
      (map_to_list ListMap.fold (fun x->x) lhs_map)
   in
   let ii_filter db inv_img : db_t =
      let inv_theta  = (Valuation.make lhs_inv inv_img) in
      if (Valuation.consistent theta inv_theta) then
        (eval_stmt (Valuation.combine theta inv_theta) db
                ((lhs_mapn, lhs_inv, lhs_outv, init_calc), incr_calc))
      else db
   in
   List.fold_left ii_filter db inv_imgs
;;


let eval_trig (trig_args: var_t list) (tuple: const_t list) db block =
   let theta = Valuation.make trig_args tuple
   in
   List.fold_left (fun db stmt -> eval_stmt_loop theta db stmt) db block;;





