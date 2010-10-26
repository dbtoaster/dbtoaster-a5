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
module M3ValuationMap = SliceableMap.Make(ConstTValue)

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

(* K3 values, includes base types, unit, tuples and two types of collections,
 * list collections for computation and structural recursion, map
 * collections for persistent collections, that are collections of records.
 * Note, map collections can be used for any collection of records, not
 * just persistent collections, indeed it should be up to the
 * compiler/code generator to figure out which type to use, for example based
 * on the use of member/lookup/slice operations on the record collection. *)
module rec K3Value : 
sig
    type t =
        | Unit
        | Float          of float
        | Int            of int
        | Tuple          of t list
        | ListCollection of t list
        | MapCollection  of t K3ValuationMap.t
        | Fun            of (t -> t) * bool (* schema application *)

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
        | ListCollection of t list
        | MapCollection  of t K3ValuationMap.t
        | Fun            of (t -> t) * bool (* schema application *)

    let zero = Float(0.0)
    let compare = Pervasives.compare
    let rec to_string a = match a with
        | Unit -> "unit"
        | Float(x) -> string_of_float x
        | Int(x) -> string_of_int x
        | Tuple(v_t) ->
            "<"^(String.concat "," (List.map to_string v_t))^">"
        | ListCollection(v_l) ->
            "["^(String.concat ";" (List.map to_string v_l))^"]"
        | MapCollection(v_m) -> ""
        | Fun(fn,_) -> "<fun>"
end
and K3ValuationMap : SliceableMap.S with type key_elt = K3Value.t
    = SliceableMap.Make(K3Value)

(* K3 Valuations allow variables to be bound to any K3 value, e.g.
 * collections, tuples, etc. *)
module K3Valuation = AbstractValuation(K3Value)
