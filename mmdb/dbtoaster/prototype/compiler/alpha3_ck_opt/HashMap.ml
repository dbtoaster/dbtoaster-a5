module type MapKey = sig type t end

module type S =
sig
end

module Make = functor (M : MapKey) ->
struct
   type key      = M.t
   type 'a t     = (key, 'a) Hashtbl.t
   
   (* Map helpers *)
   let empty_map ()  = Hashtbl.create 10
   let mem  k m      = Hashtbl.mem m k
   let find k m      = Hashtbl.find m k
   let add  k v m    = (Hashtbl.replace m k v; m)
   let fold f acc m  = Hashtbl.fold f m acc

   let safe_add k v m =
      if ((mem k m) && ((find k m) <> v)) then
         failwith "HashMap.safe_add"
      else add k v m

   (* map that applies f to both keys and values
    * returns a new slice, does not modify args in-place *)
   let mapi f m =
      fold (fun k v nm -> let (nk,nv) = f k v in add nk nv nm) (empty_map()) m

   (* map applying f over the range of the slice *) 
   let map f m = mapi (fun k v -> (k, f v)) m

   let to_string ks vs m =
      let f k v acc =
        (if acc = "" then "" else acc^" ")^(ks k)^"->"^(vs v)^";"
      in 
      "["^(fold f "" m)^"]"

   let from_list l = List.fold_left
      (fun m (k,v) -> safe_add k v m) (empty_map()) l

   let to_list m = Hashtbl.fold (fun k v l -> (k,v)::l) m []

   (* Merges two maps, preserving duplicates.
    * returns a new map, does not modify args in-place *)
   let union m1 m2 =
      let aux k v nm = Hashtbl.add nm k v; nm in
      fold aux (Hashtbl.copy m1) m2

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
   let mergei f1 f2 f12 m1 m2 =
      let aux f m k v (ignores, nm) =
         if mem k m then
            (* reconcile and ignore later *)
            (add k 0 ignores, add k (f12 k (find k m) v) nm)
         else (ignores, add k (f k v) nm)
      in
      let aux2 f ignores k v nm =
        if mem k ignores then nm else add k (f k v) nm
      in
      let (ignores, partial) =
        fold (aux f1 m2) (empty_map(), empty_map()) m1
      in
         fold (aux2 f2 ignores) partial m2
         
   let merge f1 f2 f12 m1 m2 =
      mergei (fun k v -> f1 v) (fun k v -> f2 v) (fun k v1 v2 -> f12 v1 v2) m1 m2

   (* filter that applies f to both keys and values.
    * assumes add has replace semantics
    * returns new slice, does not modify arg slice *)
   let filteri f m =
      let aux k v nm = if (f k v) then add k v nm else nm in
         fold aux (empty_map()) m
         
   let filter f m = filteri (fun k v -> f v) m

   (* Domain and range return duplicate-free lists *)
   let dom m = let aux k v nd = (k::nd) in fold aux [] m
   let rng m =
      let aux k v nd = if List.mem v nd then nd else (v::nd) in
         fold aux [] m
end