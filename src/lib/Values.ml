(** Values.
 *)

open Types
open Constants

module type Value =
sig
    type t
    val zero : t
    val zero_of_type : type_t -> t
    val compare : t -> t -> int
    val to_string : t -> string
end

module type Valuation = functor (V : Value) ->
sig
   type key = string
   type value_t = V.t
   type t 
   
   val make : string list -> V.t list -> t
   val vars : t -> string list
   val bound : string -> t -> bool
   val value : string -> t -> V.t
   
   val consistent : t -> t -> bool
   val add : t -> string -> V.t -> t
   val bind : t -> (string * string) list -> t
   val extend : t -> t -> string list -> t
   val apply : t -> key list -> V.t list
   val to_string : t -> string
end


module AbstractValuation : Valuation = functor(V : Value) ->
struct
   (* the keys are variable names *)
   (**/**)
   module StringMap = Map.Make (String)
   (**/**)

   type key = StringMap.key
   type value_t = V.t
   type t = V.t StringMap.t

   (* Note: ordered result *)
   let make vars values  =
      List.fold_left (fun acc (k,v) -> StringMap.add k v acc)
         StringMap.empty (List.combine vars values)

   let to_string theta : string =
      StringMap.fold (fun k v acc -> acc^(if acc = "" then "" else "\n")^
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

(* K3 values, includes base types, unit, tuples and several types of 
 * collections, including named simple lists, such as FloatList and TupleList, 
 * whose contents are always flat elements, 1- and 2-level persistent 
 * collections (SingleMap, DoubleMap), as well as arbitrarily nested 
 * collections, namely ListCollection and an indexed MapCollection.
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
    | BaseValue      of Constants.const_t
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
    val zero_of_type : type_t -> t
    val compare : t -> t -> int
    val string_of_value : ?sep:string -> t -> string
    val string_of_smap : ?sep:string -> single_map_t -> string
    val string_of_map : ?sep:string -> map_t -> string
    val to_string : t -> string
    val to_hashtbl : t -> 
         (Constants.const_t list, Constants.const_t list) Hashtbl.t
end =
struct
    module Map = K3ValuationMap
    type single_map_t = t K3ValuationMap.t
    and map_t = single_map_t K3ValuationMap.t
    and t = 
    | Unit
    | BaseValue      of Constants.const_t
    | Tuple          of t list
    | Fun            of (t -> t) 
    | SingleMap      of single_map_t
    | DoubleMap      of map_t
    | FloatList      of t list
    | TupleList      of t list
    | SingleMapList  of (t list * single_map_t) list
    | ListCollection of t list
    | MapCollection  of t K3ValuationMap.t

    let zero = BaseValue(CFloat(0.0))
    let zero_of_type zt = BaseValue(Constants.zero_of_type zt)
    let compare = Pervasives.compare

    let rec key_to_string k = ListExtras.ocaml_of_list string_of_value k
    and string_of_vmap ?(sep = ";\n") sm = 
       K3ValuationMap.to_string ~sep:sep key_to_string string_of_value sm
    and string_of_smap ?(sep = ";\n") sm = 
       string_of_vmap ~sep:sep sm 
    and string_of_map  ?(sep = ";\n") m  =
       K3ValuationMap.to_string ~sep:sep key_to_string string_of_smap m 

    and string_of_value ?(sep = ";\n") v =
      begin match v with
      | Unit -> "unit"
      | BaseValue(c) -> if Debug.active "PARSEABLE-VALUES"
                        then sql_of_const c
                        else string_of_const c
      | Tuple(fl) -> "("^(String.concat "," (List.map (string_of_value ~sep:sep)
                                                       fl))^")"
      | Fun(f) -> "<fun>"

      | SingleMap(sm) -> "SingleMap("^(string_of_smap ~sep:sep sm)^")"
      | DoubleMap(dm) -> "DoubleMap("^(string_of_map ~sep:sep dm)^")"

      | FloatList(fl) -> 
         "["^(String.concat ";" (List.map (string_of_value ~sep:sep) fl))^"]"
      
      | TupleList(kvl) ->
         "TupleList([" ^ 
         (String.concat sep (List.map (fun tuple -> 
            match tuple with
               | Tuple(fl) -> 
                  let (keys, values) = ListExtras.split_at_last fl in
                  (string_of_value ~sep:sep (FloatList keys))^"->"^
                  (string_of_value ~sep:sep (if values <> []
                                             then List.hd values
                                             else BaseValue(CInt(1))))
               | _ -> failwith "TupleList contains elements other than tuples"
         ) kvl)) ^ "])"

      | SingleMapList(sml) ->
          ("SingleMapList(["^(List.fold_left (fun acc (k,m) ->
                (if acc = "" then "" else acc^";")^
                (string_of_value ~sep:sep (Tuple k))^","^
                (string_of_value ~sep:sep (SingleMap m)))
               "" sml)^"])")
      | ListCollection(vl) -> "ListCollection("^(String.concat ","
                            (List.map (string_of_value ~sep:sep) vl))^")"

      | MapCollection(m) -> "MapCollection("^(string_of_vmap ~sep:sep m)^")"
      end

    let to_string v = string_of_value v
   
    let base_const_of v = match v with
      | BaseValue(c) -> c
      | _ -> failwith "Invalid argument: not a K3 base value!"  
   
    let is_const_zero c = 
       (c = Constants.zero_of_type (type_of_const c))
   
    let to_hashtbl v = match v with
       | Unit -> Hashtbl.create 10
       | BaseValue(c) ->
          let hashtbl = Hashtbl.create 10 in
          if not (is_const_zero c) then Hashtbl.add hashtbl [] [c];
          hashtbl
       | SingleMap(sm) ->
          K3ValuationMap.fold (fun keys value tbl ->
             let key_consts = List.map base_const_of keys in
             let value_const = base_const_of value in
             if not (is_const_zero value_const) then
                Hashtbl.replace tbl key_consts [value_const];
             tbl
         ) (Hashtbl.create 10) sm
       | TupleList(kvl) -> 
          List.fold_left (fun tbl tuple -> match tuple with
             | Tuple(fl) -> 
                let (keys, values) = ListExtras.split_at_last fl in
                let key_consts = List.map base_const_of keys in
                let value_consts = List.map base_const_of values in
                (* TODO: allow zero tuples in the future *)
                if value_consts <> [] && 
                      not (is_const_zero (List.hd value_consts)) then
                   Hashtbl.replace tbl key_consts value_consts;
                tbl      
             | _ -> failwith "TupleList contains elements other than tuples"
          ) (Hashtbl.create 10) kvl
      | _ -> failwith 
         "Unsupported (yet) conversion of a K3Value into a hash table"   

end
and K3ValuationMap : SliceableMap.S with type key_elt = K3Value.t
    = SliceableMap.Make(K3Value)


(* K3 Valuations allow variables to be bound to any K3 value, e.g.
 * collections, tuples, etc. *)
module K3Valuation = AbstractValuation(K3Value)
