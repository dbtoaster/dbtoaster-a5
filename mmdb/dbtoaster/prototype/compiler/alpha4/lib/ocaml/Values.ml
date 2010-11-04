(* Values.
 * A module implementing categories of basic values used in compiled programs.
 * Currently these categories are:
 * -- ConstTValue: M3.const_t
 * -- FloatValue: float
 * -- K3Value: floats, ints, tuples, list + map collections
 *
 * Description of module usage:
 * -- variables are bound to values.
 * -- maps are keyed by values.
 * -- databases store values.
 * -- interpreted code yields values.
 *)

open M3
open Util

module type Value =
sig
    type t
    val zero : t
    val compare : t -> t -> int
    val to_string : t -> string
end

module type Valuation = functor (V : Value) ->
sig
   type key = string
   type value_t = V.t
   type t 
   
   val make : var_t list -> V.t list -> t
   val vars : t -> string list
   val bound : string -> t -> bool
   val value : string -> t -> V.t
   
   val consistent : t -> t -> bool
   val add : t -> var_t -> V.t -> t
   val bind : t -> (var_t * var_t) list -> t
   val extend : t -> t -> var_t list -> t
   val apply : t -> key list -> V.t list
   val to_string : t -> string
end


module AbstractValuation : Valuation = functor(V : Value) ->
struct
   (* the keys are variable names *)
   module StringMap = Map.Make (String)

   type key = StringMap.key
   type value_t = V.t
   type t = V.t StringMap.t

   (* Note: ordered result *)
   let make vars values  =
      List.fold_left (fun acc (k,v) -> StringMap.add k v acc)
         StringMap.empty (List.combine vars values)

   let to_string theta : string =
      StringMap.fold (fun k v acc -> acc^(if acc = "" then "" else " ")^
         k^"->"^(V.to_string v)) theta ""

   (* Note: ordered result *)
   let to_list m = StringMap.fold (fun s n l -> (s,n)::l) m []

   let vars theta = StringMap.fold (fun k _ acc -> k::acc) theta []

   let bound var theta = StringMap.mem var theta
   
   let value var theta = StringMap.find var theta

   let consistent theta1 theta2 =
      List.for_all (fun (k,v) ->
        (not(StringMap.mem k theta2)) || ((StringMap.find k theta2) = v))
        (to_list theta1)

   (* adds a single binding *)
   let add theta var value = StringMap.add var value theta

   (* extends m with the given bindings, which rename existing valuations *)
   let bind theta bindings =
      List.fold_left (fun acc (decl,def) ->
         StringMap.add decl (value def acc) acc) theta bindings

   (* extends m1 by given vars from m2.
    * assumes that m1 and m2 are consistent. *)
   (* Note: ordered result *)
   let extend theta1 theta2 ext =
      List.fold_left (fun acc k ->
        StringMap.add k (value k theta2) acc) theta1 ext

   let apply theta l = List.map (fun x ->
      try StringMap.find x theta
      with Not_found ->
        failwith ("No valuation for "^x^" in "^(to_string theta))) l

end

(* TODO: replace usage of ConstTValue with FloatValue to remove boxing from
 * generated code *)
module FloatValue : Value with type t = float =
struct
    type t = float
    let zero = 0.0
    let compare = Pervasives.compare
    let to_string = string_of_float
end

module ConstTValue : Value with type t = const_t =
struct
    type t = const_t
    let zero = CFloat(0.0)
    let compare = Pervasives.compare
    let to_string = M3Common.string_of_const
end

(* M3 valuation. Variables in M3 programs are bound to const_t. *)
module M3Valuation = AbstractValuation(ConstTValue)

(* M3 map.
 * Keys are const_t, and values are parameterized within the map. *)
module M3ValuationMap : SliceableMap.S with type key_elt = const_t 
   = SliceableMap.Make(ConstTValue)

(* M3ValuationMap whose values are const_t (i.e. an implementation of a single-tier map.
 * -- we do not need this for K3, since all of these functions are
 *    inlined as structural recursion methods *)
(* TODO: module signature, remove types from bodies *)
module AggregateMap =
struct
   module Env = M3Valuation
   module Map = M3ValuationMap

   type agg_t = const_t
   
   type key = Map.key
   type t = agg_t Map.t

   let string_of_aggregate = M3Common.string_of_const

   (* Slice methods for calculus evaluation *)

   (* Indexed aggregate, iterates over all partial keys given by pattern,
    * and aggregate over key-value pairs corresponding to each partial key.
    * assumes add has replace semantics, so that fold & add removes duplicates.
    * returns a new map, does not modify in-place. *)
   let indexed_aggregate aggf pat m =
      let aux pk kv nm =
         let (nk,nv) = aggf pk kv in Map.add nk nv nm
      in Map.fold_index aux pat m (Map.empty_map())
      
   (* assumes valuation theta and (Env.make current_outv k)
      are consistent *)
   let concat_keys (current_outv: var_t list) (desired_outv: var_t list)
         (theta: Env.t) (extensions: var_t list) (m: t) =
      let key_refiner k v =
         let ext_theta = Env.extend
            (Env.make current_outv k) theta extensions in
         (* normalizes the key orderings to that of the variable ordering outv. *)
         let new_k = Env.apply ext_theta desired_outv in
         (new_k, v)
   in Map.mapi key_refiner m

   (* assumes valuation theta and (Env.make current_outv k)
      are consistent *)
   let project_keys (pat: M3ValuationMap.pattern) (pat_outv: var_t list)
        (desired_outv: var_t list) (theta: Env.t)
        (extensions: var_t list) (m: t) =
      let aux pk kv =
         let aggv = List.fold_left (fun x (y,z) -> c_sum x z) (CFloat(0.0)) kv in
         let ext_theta =
            Env.extend (Env.make pat_outv pk) theta extensions in
         (* Normalizes the key orderings to that of the variable ordering desired_outv.
          * This only performs reordering, that is we assume pat_outv has the
          * same set of variables as desired_outv. *)
         let new_k = Env.apply ext_theta desired_outv in
            (new_k, aggv)
      in
         (* in the presence of bigsum variables, there may be duplicates
          * after key_refinement. These have to be summed up using aggregate. *)
         indexed_aggregate aux pat m

   (* filters slice entries to those consistent in terms of given variables, and theta *) 
   let filter (outv: var_t list) (theta: Env.t) (m: t) : t =
      let aux k v = (Env.consistent theta (Env.make outv k)) in
         Map.filteri aux m

end

(* Simplified K3 value *)
module rec SimpleK3Value :
sig
  type t =
    | Unit
    | Float          of float
    | Int            of int
    | Tuple          of t list
    | Fun            of (t -> t) 
    | ListCollection of t list

    val zero : t
    val compare : t -> t -> int
    val to_string : t -> string
end =
struct
  type t =
    | Unit
    | Float          of float
    | Int            of int
    | Tuple          of t list
    | Fun            of (t -> t) 
    | ListCollection of t list
  
  let zero = Float(0.0)
  let compare = Pervasives.compare

  let rec string_of_value v =
    begin match v with
    | Unit -> "unit"
    | Float(f) -> string_of_float f
    | Int(i) -> string_of_int i
    | Tuple(fl) -> "("^(String.concat "," (List.map string_of_value fl))^")"
    | Fun(f) -> "<fun>"
    | ListCollection(vl) ->
      "ListCollection("^(String.concat ","
        (List.map string_of_value vl))^")"
    end
    
  let to_string = string_of_value
end
and K3SValuationMap : SliceableMap.S with type key_elt = SimpleK3Value.t
    = SliceableMap.Make(SimpleK3Value)

module K3SValuation = AbstractValuation(SimpleK3Value)


(* K3 values, includes base types, unit, tuples and several types of collections,
 * including named simple lists, such as FloatList and TupleList, whose contents
 * are always flat elements, 1- and 2-level persistent collections
 * (SingleMap, DoubleMap), as well as arbitrarily nested collections, namely
 * ListCollection and an indexed MapCollection.
 *
 * Note explictly defined type, which is a subtype of both Value above, and
 * Database.SliceableInOutMap. This is necessary since values include variants
 * that are single and double maps. *)
module rec K3Value :
sig
    type single_map_t = t K3ValuationMap.t
    and map_t = single_map_t K3ValuationMap.t
    and t = 
    | Unit
    | Float          of float
    | Int            of int
    | Tuple          of t list
    | Fun            of (t -> t) 

    (* Persistent collection values, these are yielded on accessing
     * the persistent store. *)
    | SingleMap      of single_map_t
    | DoubleMap      of map_t

    (* Core internal collections used during processing.
     * -- these are flat collections, i.e. value_t is expected to
     *    be a base type, such as float/int for FloatLists, and
     *    tuple(float/int) for TupleLists
     * -- SingleMaps and DoubleMaps are converted to these internal types
     *    by tuple collection accessors (mem/lookup/slice) *)
    | FloatList      of t list (* float list *)
    | TupleList      of t list (* float list list *)
     
    (* slicing a double map yields a SingleMapList of key * smap entries *)
    | SingleMapList  of (t list * single_map_t) list

    (* Arbitrarily nested collection based on lists *)
    | ListCollection     of t list

    (* Arbitrarily nested collections of tuples *)
    | MapCollection      of t K3ValuationMap.t

    module Map : SliceableMap.S
       with type key_elt = t and
            type 'a t = 'a K3ValuationMap.t

    val zero : t
    val compare : t -> t -> int
    val string_of_value : t -> string
    val string_of_smap : single_map_t -> string
    val string_of_map : map_t -> string
    val to_string : t -> string
end =
struct
    module Map = K3ValuationMap
    type single_map_t = t K3ValuationMap.t
    and map_t = single_map_t K3ValuationMap.t
    and t = 
    | Unit
    | Float          of float
    | Int            of int
    | Tuple          of t list
    | Fun            of (t -> t) 
    | SingleMap      of single_map_t
    | DoubleMap      of map_t
    | FloatList      of t list
    | TupleList      of t list
    | SingleMapList  of (t list * single_map_t) list
    | ListCollection of t list
    | MapCollection  of t K3ValuationMap.t

    let zero = Float(0.0)
    let compare = Pervasives.compare

    let rec key_to_string k = Util.list_to_string string_of_value k
    and string_of_vmap sm   = K3ValuationMap.to_string key_to_string string_of_value sm
    and string_of_smap sm   = string_of_vmap sm 
    and string_of_map m     = K3ValuationMap.to_string key_to_string string_of_smap m 

    and string_of_value v =
      begin match v with
      | Unit -> "unit"
      | Float(f) -> string_of_float f
      | Int(i) -> string_of_int i
      | Tuple(fl) -> "("^(String.concat "," (List.map string_of_value fl))^")"
      | Fun(f) -> "<fun>"

      | SingleMap(sm) -> "SingleMap("^(string_of_smap sm)^")"
      | DoubleMap(dm) -> "DoubleMap("^(string_of_map dm)^")"

      | FloatList(fl) ->
          "["^(String.concat ";" (List.map string_of_value fl))^"]"
      
      | TupleList(kvl) ->
          "["^(String.concat ";" (List.map string_of_value kvl))^"]"

      | SingleMapList(sml) ->
          ("["^(List.fold_left (fun acc (k,m) ->
                (if acc = "" then "" else acc^";")^
                (string_of_value (Tuple k))^","^
                (string_of_value (SingleMap m)))
               "" sml)^"]")
            
      | ListCollection(vl) -> "ListCollection("^(String.concat ","
                            (List.map string_of_value vl))^")"

      | MapCollection(m) -> "MapCollection("^(string_of_vmap m)^")"
      end

    let to_string = string_of_value
end
and K3ValuationMap : SliceableMap.S with type key_elt = K3Value.t
    = SliceableMap.Make(K3Value)


(* K3 Valuations allow variables to be bound to any K3 value, e.g.
 * collections, tuples, etc. *)
module K3Valuation = AbstractValuation(K3Value)
