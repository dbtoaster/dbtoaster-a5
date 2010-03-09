module type MapKey = sig type t val to_string: t -> string end

module type S =
sig
   type key
   type partial_key
   type pattern

   type 'a t
   
   val empty_map : unit -> 'a t
   val mem  : key -> 'a t -> bool
   val find : key -> 'a t -> 'a
   val add  : key -> 'a -> 'a t -> 'a t
   val fold : (key -> 'a -> 'b -> 'b) -> 'b -> 'a t -> 'b
   
   val safe_add : key -> 'a -> 'a t -> 'a t
   
   val mapi : (key -> 'a -> key * 'a) -> 'a t -> 'a t
   val map  : ('a -> 'a) -> 'a t -> 'a t
   
   val to_string : (key -> string) -> ('a -> string) -> 'a t -> string
   val from_list : (key * 'a) list -> 'a t
   
   val union  : 'a t -> 'a t -> 'a t
   val mergei :
      (key -> 'a -> 'a) -> (key -> 'a -> 'a) -> (key -> 'a -> 'a -> 'a) ->
      'a t -> 'a t -> 'a t

   val merge:
      ('a -> 'a) -> ('a -> 'a) -> ('a -> 'a -> 'a) -> 'a t -> 'a t -> 'a t

   val merge_rk:
      ('a -> 'a) -> ('a -> 'a) -> ('a -> 'a -> 'a) -> 'a t -> 'a t -> 'a t
   
   val filteri : (key -> 'a -> bool) -> 'a t -> 'a t
   val filter  : ('a -> bool) -> 'a t -> 'a t
   
   val partitioni: (key -> 'a -> bool) -> 'a t -> 'a t * 'a t
   val partition: ('a -> bool) -> 'a t -> 'a t * 'a t

   val dom : 'a t -> key list
   val rng : 'a t -> 'a list
   
   (* Secondary index methods *)
   val slice : pattern -> partial_key -> 'a t -> 'a t
   val slice_keys : pattern -> partial_key -> 'a t -> key list
   val partition_slice : pattern -> partial_key -> 'a t -> 'a t * 'a t

   val fold_index : (partial_key -> (key * 'a) list -> 'b -> 'b) -> pattern -> 'a t -> 'b -> 'b

   (* removes secondary indices *)
   val strip_indexes : 'a t -> 'a t
   
   val add_secondary_index : 'a t -> pattern -> 'a t
   val extend_secondary_indexes: 'a t -> 'a t -> 'a t
   val union_secondary_indexes: 'a t -> 'a t -> 'a t
   
   val validate_indexes : 'a t -> unit 
end

module Make = functor (M : MapKey) ->
struct
   type key            = M.t list
   type partial_key    = M.t list
   type pattern        = int list
   
   module SecondaryIndex  = Map.Make(struct type t = partial_key let compare = Pervasives.compare end)
   module IndexMap        = Map.Make(struct type t = pattern let compare = Pervasives.compare end)

   type 'a primary_t   = (key, 'a) Hashtbl.t
   type secondary_t    = (key list) SecondaryIndex.t
   type secondaries    = secondary_t IndexMap.t 
   type 'a t           =  'a primary_t * secondaries
   
   
   (* Secondary index helpers *)
   let project k pat = List.map (fun i ->
      try List.nth k i with Failure _ ->
         failwith ("SliceableMap.project "^(string_of_int i)^" "^
                   (string_of_int (List.length k))^" "^
                   (String.concat "," (List.map M.to_string k)))
      ) pat 
   
   (* Adds a pk->key mapping to a secondary index *)
   let add_secondary k pat si =
      let pk = project k pat in
      let existing = if SecondaryIndex.mem pk si
         then SecondaryIndex.find pk si else []
      in SecondaryIndex.add pk (k::existing) si

   (* Check k exists in some kl for the secondary *) 
   let mem_secondary si k =
      SecondaryIndex.fold (fun pk kl acc -> acc || (List.mem k kl)) si false

   (* Builds a secondary index w/ the given pattern for all rows *)
   let build_secondary_index m pat =
      try
      Hashtbl.fold (fun k v si -> add_secondary k pat si)
         (fst m) SecondaryIndex.empty
      with Failure x -> failwith ("build_secondary: "^x)
   
   (* Adds a secondary index to the map *)
   let add_secondary_index m pat =
      if IndexMap.mem pat (snd m) then m
      else
         let new_secondary = build_secondary_index m pat in
            (fst m, IndexMap.add pat new_secondary (snd m))

   (* Ensures m1 has the set of secondary indexes of m2, i.e. builds
    * all indexes for m1 (based on m1's primary alone) that are
    * present only in m2.
    * Note this does not actually copy m2's secondary indexes.  *)
   let extend_secondary_indexes m1 m2 =
      IndexMap.fold (fun pat si acc -> add_secondary_index acc pat) (snd m2) m1

   (* Unions two secondary indexes, i.e. concats primary key lists for
    * common partial keys *)
   let union_secondary si1 si2 =
      SecondaryIndex.fold (fun pk kl acc ->
         let existing = if SecondaryIndex.mem pk acc
            then SecondaryIndex.find pk acc else []
         in SecondaryIndex.add pk (existing@kl) acc)
         si2 si1

   (* Unions a secondary index with any equivalent pattern in a sliceable map *)
   let union_secondary_index pat si m =
      if IndexMap.mem pat (snd m) then
         let msi = IndexMap.find pat (snd m) in
         let new_msi = union_secondary msi si in
            (fst m, IndexMap.add pat new_msi (snd m))
      else
         (fst m, IndexMap.add pat si (snd m)) 

   (* Unions secondary indexes of two maps, duplicating common keys in the
    * secondary indexes.
    * Handles disjoint sets of patterns *)
   let union_secondary_indexes m1 m2 =
      let new_m1 = extend_secondary_indexes m1 m2 in
      let new_m2 = extend_secondary_indexes m2 m1 in
         IndexMap.fold union_secondary_index (snd new_m2) new_m1

   let secondary_to_string si =
      let key_to_string = Util.list_to_string M.to_string in
      SecondaryIndex.fold (fun pk kl acc ->
         acc^" "^(key_to_string pk)^"->["^
            (String.concat ";" (List.map key_to_string kl))^"]")
         si ""

   let validate_indexes m =
      let aux k v =
         let valid = IndexMap.fold
            (fun pat si acc -> acc && (mem_secondary si k)) (snd m) true
         in if not(valid) then
            (failwith ("validate_indexes "^(Util.list_to_string M.to_string k)))
      in Hashtbl.iter aux (fst m)


   (* Map helpers *)
   let empty_map () = (Hashtbl.create 10, IndexMap.empty)
   
   let init_secondaries sim =
      IndexMap.map (fun si -> SecondaryIndex.empty) sim
      
   let init_secondaries_w_patterns patterns =
      List.fold_left
         (fun acc p -> IndexMap.add p SecondaryIndex.empty acc)
         IndexMap.empty patterns

   let empty_map_w_patterns patterns =
      (Hashtbl.create 10, init_secondaries_w_patterns patterns)

   let empty_map_w_secondaries m =
      (Hashtbl.create 10, init_secondaries (snd m))

   let get_patterns m = IndexMap.fold (fun k v acc -> k::acc) (snd m) []
   
   let string_of_patterns m = IndexMap.fold (fun k v acc ->
      acc^" "^(String.concat "," (List.map string_of_int k))) (snd m) ""

   let mem  k m     = Hashtbl.mem (fst m) k
   let find k m     = Hashtbl.find (fst m) k
   
   let add k v m = 
      let new_second = if Hashtbl.mem (fst m) k then (snd m) else
         let aux pat si acc =
            IndexMap.add pat (add_secondary k pat si) acc
         in IndexMap.fold aux (snd m) IndexMap.empty
      in
         Hashtbl.replace (fst m) k v;
         (fst m, new_second)

   (* f must accept secondaries as first argument *)
   let fold f acc m = Hashtbl.fold (f (snd m)) (fst m) acc

   let safe_add k v m =
      if ((mem k m) && ((find k m) <> v)) then
         failwith "SliceableMap.safe_add"
      else add k v m

   (* map that applies f to both keys and values
    * returns a new slice, does not modify args in-place *)
   (* TODO: this currently rebuilds secondary indices with add -- optimize *)
   let mapi f m =
      fold (fun _ k v nm -> let (nk,nv) = f k v in add nk nv nm)
         (empty_map_w_secondaries m) m

   (* map that applies f to v only
    * since k cannot change, we can avoid rebuilding secondary indexes *)
   let map f m =
      let new_m = Hashtbl.fold
         (fun k v nm -> Hashtbl.replace nm k (f v); nm) (fst m) (Hashtbl.create 100)
      in (new_m, (snd m))

   let to_string ks vs m =
      let f _ k v acc =
        (if acc = "" then "" else acc^" ")^(ks k)^"->"^(vs v)^";"
      in 
      "["^(fold f "" m)^"]<pat="^(string_of_patterns m)^">"

   let from_list l patterns = List.fold_left
      (fun m (k,v) -> safe_add k v m) (empty_map_w_patterns patterns) l

   let to_list m = Hashtbl.fold (fun k v l -> (k,v)::l) (fst m) []

   let copy m = (Hashtbl.copy (fst m), (snd m))

   (* Merges two maps, preserving duplicates.
    * Modifies the first argument in place.
    * Note: this bulk copies m2's secondary indexes, and can avoid
    * adding to any secondary index while adding to the primary. *)
   let union m1 m2 =
      try
      let aux sim k v nm =
         let (nprim, nsecond) = nm in
         Hashtbl.add nprim k v;
         (* For debugging, ensure key already exists in the secondary index union *)
         (*
         (let valid = IndexMap.fold
            (fun pat si acc -> acc && (mem_secondary si k)) nsecond true
          in if not(valid) then failwith "SliceableMap.union");
         *)
         (nprim, nsecond)
      in
      fold aux (union_secondary_indexes m1 m2) m2
      with Failure x -> failwith ("union: "^x)

   (* Merges two slices, reconciling entries to a single entry.
    * This is a generic merge where fs can remap keys, thus we rebuild secondary
    * indexes from scratch.
    * Merges in two phases, first phase transforms and reconciles if necessary,
    * second phase ignores entries already reconciled, transforms and
    * merges remaining. *)
   (* TODO: implement ignores with a hashset/bitvector *)
   (* yna note: in our usage, f2 is much more expensive to evaluate than f1,
    * since f2 blindly performs initial value computation. Thus we should
    * invoke f2 as few times as possible, making it the slice we ignore
    * whenever we can *)
   let mergei f1 f2 f12 m1 m2 =
      let aux f m secondaries k v (ignores, nm) =
         if mem k m then
            ((Hashtbl.add ignores k 0; ignores),
             add k (f12 k (find k m) v) nm)
         else (ignores, add k (f k v) nm) in
      let aux2 f ignores secondaries k v nm =
        if Hashtbl.mem ignores k then nm else add k (f k v) nm in
      let merged_map = extend_secondary_indexes
         (empty_map_w_secondaries m1) (empty_map_w_secondaries m2) in
      let (ignores, partial) =
         fold (aux f1 m2) (Hashtbl.create 10, merged_map) m1
      in fold (aux2 f2 ignores) partial m2
   
   (* In-place merge, assumes add has replace semantics,
    * Exploits that f1...f12 cannot remap keys, but f12 gets passed the key. *)
   let merge_rk f1 f2 f12 m1 m2 =
      let aux _ k v m =
         if mem k m then add k (f12 (find k m) v) m
         else add k (f2 k v) m
      in fold aux (map f1 (extend_secondary_indexes m1 m2)) m2
         
   let merge f1 f2 f12 m1 m2 =
      merge_rk f1 f2 (fun k v1 v2 -> f12 v1 v2) m1 m2

   (* filter that applies f to both keys and values.
    * assumes add has replace semantics
    * returns new slice, does not modify arg slice *)
   (* TODO: bulk copy secondary indexes rather than rebuild via add *)
   (* TODO: use a secondary index structure w/ efficient deletion *)
   let filteri f m =
      let aux secondaries k v nm = if (f k v) then add k v nm else nm in
         fold aux (empty_map_w_secondaries m) m
         
   let filter f m = filteri (fun k v -> f v) m

   let partitioni f m =
      let aux secondaries k v (lnm, rnm) =
         if f k v then (add k v lnm, rnm)
         else (lnm, add k v rnm)
      in fold aux (empty_map_w_secondaries m, empty_map_w_secondaries m) m
      
   let partition f m = partitioni (fun k v -> f v) m

   (* Domain and range return duplicate-free lists *)
   let dom m = let aux secondaries k v nd = (k::nd) in fold aux [] m
   let rng m =
      let aux secondaries k v nd = if List.mem v nd then nd else (v::nd) in
         fold aux [] m
         
   (* Secondary index methods *)
   let get_keys sim pat pkey =
      let index = if IndexMap.mem pat sim then IndexMap.find pat sim
         else ((*print_endline "Pattern not found.";*) SecondaryIndex.empty)
      in if SecondaryIndex.mem pkey index
         then SecondaryIndex.find pkey index
         else ((*print_endline ("Not found: "^(secondary_to_string index));*) [])

   let slice pat pkey m =
      let aux acc k = add k (find k m) acc
      in List.fold_left aux (empty_map_w_secondaries m) (get_keys (snd m) pat pkey)

   let slice_keys pat pkey m = get_keys (snd m) pat pkey
      
   let partition_slice pat pkey m = 
      let kd = slice_keys pat pkey m in
         (slice pat pkey m, filteri (fun k v -> not(List.mem k kd)) m)

   let fold_index f pat m acc =
      let aux pk kl acc =
         let kv = List.map (fun k -> (k, find k m)) kl in f pk kv acc
      in SecondaryIndex.fold aux (IndexMap.find pat (snd m)) acc

   (* Removes secondary indexes, used during map lookups in calculus *)
   let strip_indexes (p,s) = (p, IndexMap.empty)

end