open M3

(* List helper functions *)

let union l1 l2 = l1@(List.filter (fun k -> (not (List.mem k l1))) l2)

let list_to_string elem_to_string l =
   "["^(List.fold_left (fun str x -> str^" "^(elem_to_string x)) "" l)^" ]"


(* General map types and helper functions *)

module StringMap = Map.Make (String)
(* the keys are variable or map names *)

module ListMap = Map.Make (struct
   type t = const_t list
   let compare = compare
end)

module StringSMap = SuperMap.Make(StringMap)
module   ListSMap = SuperMap.Make(  ListMap)

(* (PARTIAL) VARIABLE VALUATIONS *)
module Valuation =
struct
   type t = int StringMap.t

   let make (vars: var_t list) (values: const_t list) : t =
      StringSMap.from_list (List.combine vars values)

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

   let key_to_string k = list_to_string string_of_int k

   (* assumes const_t is int *)
   let to_string (val_to_string_f: 'a -> string) (m: 'a ListMap.t) =
      ListSMap.to_string key_to_string val_to_string_f m

   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let complete_keys current_outv desired_outv theta slice =
      let key_refiner (k,v) =
         let ext_theta = StringSMap.combine theta
                                           (Valuation.make current_outv k)
                         (* normalizes the key orderings to that of the
                            variable ordering outv. *)
         in
         let new_k = StringSMap.apply ext_theta desired_outv in
         (new_k, v)
   in
      (* in the presence of bigsum variables, there may be duplicates
         after key_refinement. These have to be summed up using aggregate. *)
   aggregate (List.map key_refiner (ListSMap.to_list slice))

   let filter (outv: var_t list) (theta: Valuation.t) (slice: 'a t) : ('a t) =
      let aux k v = (StringSMap.consistent theta (Valuation.make outv k))
      in
      ListSMap.filteri aux slice
end


type slice_t = int ListMap.t
type map_t   = slice_t ListMap.t (* nested map *)
type db_t    = map_t StringMap.t

let slice_to_string slice = ValuationMap.to_string string_of_int slice 
let nmap_to_string  m     = ValuationMap.to_string slice_to_string m
let db_to_string (db:db_t) : string = StringSMap.to_string (fun x->x)
                                       nmap_to_string db
let vars_to_string vs = list_to_string (fun x->x) vs

let lshowmap  m =   ListSMap.to_list (  ListMap.map ListSMap.to_list m)
let gshowmap  m = StringSMap.to_list (StringMap.map lshowmap  m)


let make_empty_db schema: db_t =
   let f (mapn, itypes, _) =
      (mapn, (if(List.length itypes = 0) then
                 ListSMap.from_list [([], ListSMap.from_list [])]
              else ListSMap.from_list []))
   in
   StringSMap.from_list (List.map f schema)


let rec k_tuples k (src: 'a list) : 'a list list =
   if (k <= 0) then [[]]
   else
   List.flatten (List.map (fun t -> List.map (fun x -> x::t) src)
                (k_tuples (k-1) src))


let make_empty_db_wdom schema (dom: const_t list): db_t =
   let f (mapn, itypes, rtypes) =
      let map = ListSMap.from_list
                   (List.map (fun t -> (t, ListSMap.from_list []))
                             (k_tuples (List.length itypes) dom))
      in
      (mapn, map)
   in
   StringSMap.from_list (List.map f schema)



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
   let op c1 c2 = union (calc_schema c1) (calc_schema c2)
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


let update_db mapn inv_imgs slice db =
(*
   print_string ("Updating the database: "^mapn^"->"^
                  (list_to_string string_of_int inv_imgs)^"->"^
                  (slice_to_string slice));
*)
   let m = StringMap.find mapn db in
   StringMap.add mapn (ListMap.add inv_imgs slice m) db



(* Evaluates an expression, i.e. the right-hand side of an M3 statement.
   The resulting slice is consistent with theta.
   May extend other maps of the database through initial value computation
   on expression leaves.
*)
let rec eval_calc (incr_calc: calc_t)
                  (theta: Valuation.t) (db: db_t)
                  : (var_t list * slice_t * db_t) =
(*
   print_string("\neval_calc "^(calc_to_string incr_calc)^
                " "^(Valuation.to_string theta)
                          ^" "^(db_to_string db)^"   ");
*)
   (* compose: evaluate m1 first, and run m2 on the resulting database *)
   let rec do_op op (m1:calc_t) (m2:calc_t) (theta:Valuation.t) (db:db_t)
             : (var_t list * slice_t * db_t) =
      let (outv1, res1, db1) = eval_calc m1 theta db
      in
      let f (_, r, d) (k,v1) =
         let th = StringSMap.combine theta (Valuation.make outv1 k) in
         let (o2, r2, d2) = eval_calc m2 th d in
         let schema = union outv1 o2 in
         let r3 = ValuationMap.complete_keys o2 schema th
                     (ListMap.map (fun v2 -> op v1 v2) r2)
         in
         (schema, (ListSMap.combine r r3), d2) (* r, r3 have no overlap *)
      in
      List.fold_left f ([], (ListSMap.from_list []), db1)
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
         let m        : map_t        = (StringMap.find mapn db) in
         let inv_imgs : const_t list = StringSMap.apply theta inv
         in
         let (rhs_outv, (slice2: slice_t), db2) = (
            if (ListMap.mem inv_imgs m) then
                (outv, (ListMap.find inv_imgs m), db)
            else
            (
                (* initial value comp'n for leaves of the expression tree. *)
                let (init_outv, init_slice, db1) = eval_calc init_calc theta db
                in
                let init_slice2 = ValuationMap.complete_keys
                            init_outv outv theta init_slice
                in
                (outv, init_slice2,
                 (update_db mapn inv_imgs init_slice2 db1))
            )
         ) in
         let slice3 = (ValuationMap.filter rhs_outv theta slice2)
         in
         (rhs_outv, slice3, db2)
      )
    | Add (c1, c2)        -> do_op ( + )         c1 c2 theta db
    | Mult(c1, c2)        -> do_op ( * )         c1 c2 theta db
    | Lt  (c1, c2)        -> do_op (int_op (< )) c1 c2 theta db
    | Leq (c1, c2)        -> do_op (int_op (<=)) c1 c2 theta db
    | Eq  (c1, c2)        -> do_op (int_op (= )) c1 c2 theta db
    | IfThenElse0(c1, c2) -> do_op (fun v cond -> if (cond<>0) then v else 0)
                                   c2 c1 theta db
    | Null(outv)          -> (outv, ListMap.empty, db)
    | Const(i)            -> ([], ListSMap.from_list [([], i)], db)
    | Var(x)              -> if (not (StringMap.mem x theta)) then
                                  failwith ("CALC/case Var("^x^"): theta="^
                                            (Valuation.to_string theta))
(*
                                  ([x], ListMap.empty, db)
*)
                             else let v = StringMap.find x theta in
                                  ([x], ListSMap.from_list [([v], v)], db)
                             (* note: x is not necessarily an out-var!
                                remove from the slice keys later! *)


(* postprocess the results of eval_calc.
   extends the keys of slice back to the signature of the lhs map *)
let eval_calc2 lhs_outv (calc: calc_t) (theta: Valuation.t) (db: db_t)
                : (slice_t * db_t) =
   let (rhs_outv, slice0, db0) = (eval_calc calc theta db) in
(*
   print_string ("End of CALC; outv="^(vars_to_string rhs_outv)^
                 " slice="^(slice_to_string slice0)^
                 (if (db=db0) then "" else ("db="^(db_to_string db0))));
*)
   let slice1 = ValuationMap.complete_keys rhs_outv lhs_outv theta slice0
   in
   (slice1, db0)


let eval_stmt ((lhs_mapn, lhs_inv, lhs_outv, init_calc), incr_calc)
              (theta: Valuation.t) (current: slice_t) (db: db_t)
              : (slice_t * db_t) =
(*
   print_string("\nSTMT (th="^(Valuation.to_string theta)
   ^", db=_, stmt=(("^lhs_mapn^" "^(list_to_string (fun x->x) lhs_inv)^" "
              ^(list_to_string (fun x->x) lhs_outv)^" _), _))\n");
*)
   let ((delta:slice_t), db1) = eval_calc2 lhs_outv incr_calc theta db
   in
   let f2 k v2 =
      let theta2 = StringSMap.combine theta (Valuation.make lhs_outv k)
      in
(*
      print_string ("@STMT.init{th="^(Valuation.to_string theta2)
                    ^", key="^(list_to_string string_of_int k)
                    ^", db="^(db_to_string db1));
*)
      let a = (fst (eval_calc2 lhs_outv init_calc theta2 db1))
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
   (new_slice, db1)


let eval_stmt_loop stmt (theta: Valuation.t) (db: db_t) : db_t =
   let ((lhs_mapn, lhs_inv, _, _), _) = stmt in
(*
   print_string("\nSTMT-LOOP "^(Valuation.to_string theta)
        ^" <db> (("^lhs_mapn^", "^(list_to_string (fun x->x) lhs_inv)
        ^", <lhs_outv>), <init>), <incr>) --   ");
*)
   let lhs_map = StringMap.find lhs_mapn db
   in
   let ii_filter db inv_img : db_t =
      let inv_theta  = (Valuation.make lhs_inv inv_img) in
      if (StringSMap.consistent theta inv_theta) then
         let slice = ListMap.find inv_img lhs_map
         in
         let (new_slice, new_db) =
            (eval_stmt stmt (StringSMap.combine theta inv_theta) slice db)
         in
         update_db lhs_mapn inv_img new_slice new_db
      else db
   in
   List.fold_left ii_filter db (ListSMap.dom lhs_map)


let eval_trig (trig_args: var_t list) (tuple: const_t list) db block =
   let theta = Valuation.make trig_args tuple in
   let f db stmt = eval_stmt_loop stmt theta db
   in
   List.fold_left f db block


