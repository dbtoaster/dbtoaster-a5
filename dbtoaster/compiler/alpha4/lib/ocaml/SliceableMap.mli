module type MapKey = sig
   type t
   val to_string: t -> string
end

module type S =
sig
   type key_elt
   type key         = key_elt list
   type partial_key = key_elt list
   type pattern     = int list

   type 'a t
   type secondary_t

   (* Map construction *)
   (* Empty map, no secondary indexes *)   
   val empty_map : unit -> 'a t
   
   (* Empty map, maintaining secondary indexes for the given patterns *)
   val empty_map_w_patterns : pattern list -> 'a t
   
   (* Tests whether the map is empty *)
   val empty: 'a t -> bool

   (* Tests binding presence in the map *)
   val mem  : key -> 'a t -> bool
   
   (* Returns map value, thows Not_found exception if binding does not exist *) 
   val find : key -> 'a t -> 'a
   
   (* Adds the binding via an in-place modification of the map.
    * Replaces any previous bindings (see Hashtbl.replace) *)
   val add  : key -> 'a -> 'a t -> 'a t
   
   (* Same as add, except this ensures a different binding does not exist.
    * Throws a Failure exception otherwise. *)
   val safe_add : key -> 'a -> 'a t -> 'a t
   
   (* Fold over primary map structure *)
   val fold : (key -> 'a -> 'b -> 'b) -> 'b -> 'a t -> 'b   
   
   (* Map key/value pairs by given function to a new map *)
   val mapi : (key -> 'a -> key * 'b) -> 'a t -> 'b t
   
   (* In-place mapping of key-value pairs *)
   val map  : ('a -> 'b) -> 'a t -> 'b t 
   
   (* Same as map, but mapping function gets passed the key. Note
    * this only replaces the binding, and does not construct a new
    * key, as seen in mapi *)  
   val map_rk : (key -> 'a -> 'b) -> 'a t -> 'b t 

   (* Merges two maps, preserving duplicates as hidden bindings. *)
   val union  : 'a t -> 'a t -> 'a t

   (* Computes a Cartesian product of two maps, applying the given function
    * on each pair of values in the product.
    * TODO: right now this drops all secondary indexes, which is fine for
    * our needs, but a general implementation should maintain existing
    * indexes to their new key positions. *)
   val product : ('a -> 'a -> 'a) -> 'a t -> 'a t -> 'a t

   (* Merges and reconciles two maps, mapping each binding encountered
    * during merge. Constructs merged map from scratch, allowing 
    * modification of bindings by mapping functions. *)
   val mergei :
      (key -> 'a -> 'a) -> (key -> 'a -> 'a) -> (key -> 'a -> 'a -> 'a) ->
      'a t -> 'a t -> 'a t

   (* Same as merge, except assumes keys do not change during mappings.
    * Result is an in-place modification of second map argument. *)
   val merge:
      ('a -> 'a) -> ('a -> 'a) -> ('a -> 'a -> 'a) -> 'a t -> 'a t -> 'a t

   (* Same as merge above where mappers are passed keys. Note unlike
    * mergei, we do not build the result from scratch *)
   val merge_rk:
      (key -> 'a -> 'a) -> (key -> 'a -> 'a) -> (key -> 'a -> 'a -> 'a) ->
      'a t -> 'a t -> 'a t

   (* Filters the primary map to bindings passing the argument function *)   
   val filteri : (key -> 'a -> bool) -> 'a t -> 'a t
   val filter  : ('a -> bool) -> 'a t -> 'a t
   
   (* Partitions the primary map to bindings passing the argument function *)
   val partitioni: (key -> 'a -> bool) -> 'a t -> 'a t * 'a t
   val partition: ('a -> bool) -> 'a t -> 'a t * 'a t

   (* Map domain and range retrieval *)
   val dom : 'a t -> key list
   val rng : 'a t -> 'a list
   
   (* Construction helpers, preserves primary only *) 
   val from_list : (key * 'a) list -> pattern list -> 'a t
   val to_list : 'a t -> (key * 'a) list
   
   (* Stringification of primary map *)
   val to_string : (key -> string) -> ('a -> string) -> 'a t -> string

   
   (* Secondary index methods *)
   
   (* Sliced lookup for a given pattern, and partial key, returning
    * a map with the same secondary indexes as the argument. *)
   val slice : pattern -> partial_key -> 'a t -> 'a t

   (* Returns a list of all full keys matching a given pattern and partial key *)
   val slice_keys : pattern -> partial_key -> 'a t -> key list
   
   (* Partitions the map in two for the given pattern and partial key *)
   val partition_slice : pattern -> partial_key -> 'a t -> 'a t * 'a t

   (* Folds over a secondary index for the given pattern.
    * Fold function applies to a partial key, the list of full keys
    * and the accumulated value *)
   val fold_index : (partial_key -> (key * 'a) list -> 'b -> 'b) ->
                       pattern -> 'a t -> 'b -> 'b
   
   (* Returns a new map with a secondary index for the given pattern *)
   val add_secondary_index : 'a t -> pattern -> 'a t
   
   (* Ensures first map supports at least the same secondary index
    * patterns as the second map *)
   val extend_secondary_indexes: 'a t -> 'a t -> 'a t

   (* Removes all secondary indexes from the map *)
   val strip_indexes : 'a t -> 'a t

   (* Ensures all entries in the primary index are present in each
    * secondary index, otherwise throws a Failure exception *)   
   val validate_indexes : 'a t -> unit 
end

module Make (M : MapKey) : S with type key_elt = M.t 
