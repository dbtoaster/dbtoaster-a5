
module type S =
sig
   type key
   type 'a t  (* 'a is the type of the range of the map; 'a t is the map *)

   (* raises an exception if the key is already in the map. *)
   val safe_add:   key -> 'a -> ('a t) -> ('a t)

   val from_list:  ((key * 'a) list) -> ('a t)
   val to_list:    ('a t) -> ((key * 'a) list)
   val to_string:  ('a t) -> string

   (* checks whether the two input maps agree on the keys they have in common *)
   (*val consistent: ('a t) -> ('a t) -> bool*)

   (* adds m2 to m1.  assumes that m1 and m2 are consistent. *)
   val combine:    ('a t) -> ('a t) -> ('a t)

   (* applies the map to each element of a list. Raises an exception
      (Not found)if an element of the input list is not in the map domain. *)
   (*val apply:      ('a t) -> (key list) -> ('a list)*)

   val dom:        ('a t) -> (key list)
   val rng:        ('a t) -> ('a list)

   (* like List.filter *)
   val filteri:    (key -> 'a -> bool) -> ('a t) -> ('a t)
   val filter:     (       'a -> bool) -> ('a t) -> ('a t)

   (* performs an outer join of m1 and m2 on their keys;
      applies f1 to those keys that only occur in m1;
              f2 to those keys that only occur in m2; and
              f12 to those that occur in both. *)
   val mergei:     (key -> 'a -> 'c) -> (key -> 'b -> 'c)
                   -> (key -> 'a -> 'b -> 'c) -> ('a t) -> ('b t) -> ('c t)

   (* just like merge but the keys are not provided to f1,f2,f12. *)
   val merge:      ('a -> 'c) -> ('b -> 'c)
                   -> ('a -> 'b -> 'c) -> ('a t) -> ('b t) -> ('c t)

   (* executes function f on each element of map m; the result of f is
      itself a map; the nested map is flattened by merging the keys with
      _injective_ function keyext_f. *)
   val ext:        (key -> 'a -> 'a t) -> (key -> key -> key) -> ('a t)
                   -> ('a t)
end

module Make = functor (T : Map.S) ->
struct
   type key = T.key
   type 'a t = 'a T.t

   let safe_add k v m =
      if ((T.mem k m) && ((T.find k m) <> v)) then
         failwith "SuperMap.safe_add"
      else T.add k v m

   let from_list l = List.fold_left (fun m (k,v) -> safe_add k v m) T.empty l

   let to_list m = T.fold (fun s n l -> (s,n)::l) m []

   let to_string key_to_string val_to_string m =
      let list_to_string elem_to_string l =
         let tup_f str x = str^" "^(elem_to_string x) in
            "["^(List.fold_left tup_f "" l)^" ]"
      in
      let elem_to_string (k,v) =
         (key_to_string k)^"->" ^(val_to_string v)^";"
      in
      list_to_string elem_to_string (to_list m)

   (*
   let consistent (m1: 'a t) (m2: 'a t) : bool =
      List.for_all (fun (k,v) -> (not(T.mem k m2)) || ((T.find k m2) = v))
                   (to_list m1)
   *)

   let combine (m1: 'a t) (m2: 'a t) : 'a t = T.fold safe_add m1 m2

   (*
   let apply (m: 'a t) (l: key list) = List.map (fun x -> T.find x m) l
   *)

   let dom (m: 'a t) = let (dom, rng) = List.split (to_list m) in dom
   let rng (m: 'a t) = let (dom, rng) = List.split (to_list m) in rng

   let filteri f m = from_list (List.filter (fun (k,v) -> f k v) (to_list m))
   let filter  f m = from_list (List.filter (fun (k,v) -> f   v) (to_list m))

   let mergei f1 f2 f12 m1 m2 =
      let union l1 l2 = l1@(List.filter (fun k -> (not (List.mem k l1))) l2)
      in
      let combined_dom = union (dom m1) (dom m2)
      in
      let f k =
         if (T.mem k m1) then
            if (T.mem k m2) then f12 k (T.find k m1) (T.find k m2)
            else                 f1  k (T.find k m1)
         else                    f2  k               (T.find k m2)
      in
      from_list (List.map (fun k -> (k, f k)) combined_dom)

   let merge f1 f2 f12 m1 m2 =
      mergei (fun k x -> f1 x) (fun k y -> f2 y) (fun k x y -> f12 x y) m1 m2

   let ext (f: key -> 'a -> 'a t) (keyext_f: key -> key -> key)
           (m: 'a t) : ('a t) =
      let aux (k1, v1) =
         List.map (fun (k2, v2) -> ((keyext_f k1 k2), v2))
                  (to_list (f k1 v1))
      in
      from_list (List.flatten (List.map aux (to_list m)))
end

