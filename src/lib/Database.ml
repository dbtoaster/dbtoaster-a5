(* Databases.
 *
 * Databases are persistent stores of values and maps for M3/K3 programs.
 * We have two types of databases, consisting of different types of maps:
 * i. M3DBs have 1-level maps and 2-level maps.
 * ii. K3DBs contain maps whose values and keys may be arbitrarily nested.
 *     K3DBs are also M3DBs through subtyping, that is a K3 database can be
 *     used as an M3 DB, but supports additional methods related solely to
 *     deeply nested maps.
 *
 * Databases are parameterized over:
 * -- naming which must meet the MapName type. Current implementations include
 *    textual, and integer naming.
 * -- maps implementations, which for M3 DBs must meet M3MapSpec, and for K3 DBs
 *    must meet K3MapSpec. Current implementations of M3 maps include ConstTMaps
 *    and MK3Maps, which are sliceable maps of const_t values and K3 values
 *    respectively. The InOutMaps module uses SliceableMaps, parameterized by
 *    values to implement both types of M3 maps.
 *    We currently have one K3 map implementation, as SliceableMaps of K3
 *    values, which includes all the functionality of MK3Maps. 
 *
 * This module provides two pure M3 database implementations
 * -- SimpleNameableM3Database, a db with string names, and single maps
 *    implicitly stored as double maps with an additional empty key.
 * -- NameableM3Database, a db that stores values, single maps and
 *    double maps separately to avoid empty list lookup overhead present
 *    in a SimpleNameableM3Database
 * -- both maps use M3.const_t as values
 *
 * This module also provides a K3 database implementation:
 * -- NameableK3Database
 *
 * In summary:
 * -- databases are functors of names and maps
 * -- names are strings or integers
 * -- InOutMaps implement M3 maps, and are functors of values (i.e. map contents)
 * -- K3 maps implement all the functionality of M3 maps, and additionally a
 *    deeply nested map type and accessors.
 * -- K3 databases implement all the functionality of M3 databases, and
 *    can additionally maintain deeply nested maps.
 *
 *)

open Values
open Calculus

(***************************
 * DB Name parameterization
 ***************************)
(* Maps can be accessed by name or unique id in generated code *)
module type MapName =
sig
   type t
   val to_string : t -> string
   val of_string : string -> t
   val compare : t -> t -> int
end

(* Implementations *)
module NamedMap =
struct
   type t = string
   let to_string x = x
   let of_string x = x
   let compare = Pervasives.compare
end

module RefMap =
struct
   type t = int
   let to_string = string_of_int
   let of_string = Hashtbl.hash 
   let compare = Pervasives.compare
end


(**************************************************
 * DB content parameterization
 * -- all M3 databases must define
 *    basic operations on single and double maps
 **************************************************)

module type M3MapSpec =
sig
	type value_t
   type key_t     = value_t list
   type pattern_t = int list

   type single_map_t
   type map_t

   (* initializer *)
   val zero : value_t
	val zero_of_type : Types.type_t -> value_t
	
   val string_of_value : value_t -> string
   val string_of_smap : ?sep:string -> single_map_t -> string
   val string_of_map : ?sep:string -> map_t -> string

   val make_smap : (key_t * value_t) list -> pattern_t list -> single_map_t
   val make_map  : pattern_t list -> map_t  
   val make_map_from_smaps : (key_t * single_map_t) list -> pattern_t list -> map_t

   val smap_to_list : single_map_t -> (key_t * value_t) list
   val map_to_list : map_t -> (key_t * single_map_t) list

   val smap_has_entry : key_t -> single_map_t -> bool
   val get_smap_entry : key_t -> single_map_t -> value_t

   val map_has_entry : key_t -> map_t -> bool
   val get_map_entry : key_t -> map_t -> single_map_t

   val add_smap_entry : key_t -> value_t -> single_map_t -> single_map_t
   val add_map_entry : key_t -> single_map_t -> map_t -> map_t

   val remove_smap_entry : key_t -> single_map_t -> single_map_t
   val remove_map_entry : key_t -> map_t -> map_t

   val smap_is_empty: single_map_t -> bool
   val map_is_empty: map_t -> bool
end



(* Implementation:
 * -- M3 map with single and double maps as SliceableMaps
 *)
module type SliceableInOutMap = 
sig
    type t
    
    module Map : SliceableMap.S with type key_elt = t

    type single_map_t = t Map.t
    type map_t = single_map_t Map.t

    val zero : t
    val zero_of_type : Types.type_t -> t
    val string_of_value : t -> string
    val string_of_smap : ?sep:string -> single_map_t -> string
    val string_of_map : ?sep:string -> map_t -> string
end

module InOutMaps(M : SliceableInOutMap) : M3MapSpec
    with type value_t      = M.t and
         type single_map_t = M.single_map_t and
         type map_t        = M.map_t
=
struct
    (**/**)
    include M
    (**/**)

    type value_t   = t
    type key_t     = value_t list
    type pattern_t = int list
    
    let make_smap tuples patterns = Map.from_list tuples patterns
    let make_map patterns = Map.empty_map_w_patterns patterns
    let make_map_from_smaps smaps patterns = Map.from_list smaps patterns

    let smap_to_list smap = Map.to_list smap
    let map_to_list map = Map.to_list map

    let smap_has_entry key map = Map.mem key map
    let get_smap_entry key map = Map.find key map
    
    let map_has_entry key map = Map.mem key map
    let get_map_entry key map = Map.find key map

    let add_smap_entry key value smap = Map.add key value smap
    let add_map_entry key slice map = Map.add key slice map

    let remove_smap_entry key smap = Map.remove key smap
    let remove_map_entry key map = Map.remove key map

    let smap_is_empty smap = Map.empty smap
    let map_is_empty map = Map.empty map
end


(* MK3 maps are used to implement the M3 part of a K3 database *)
module MK3Maps : M3MapSpec
    with type value_t      = K3Value.t and
         type single_map_t = K3Value.t K3ValuationMap.t and
         type map_t        = (K3Value.t K3ValuationMap.t) K3ValuationMap.t
= InOutMaps(K3Value)


(******************
 * K3 databases
 ******************)

module type K3MapSpec =
sig
   include M3MapSpec

   type nested_map_t  

   val is_single_map : value_t -> bool
   val is_map : value_t -> bool
   val is_nested_map : value_t -> bool

   val smap_of_value : value_t -> single_map_t
   val value_of_smap : single_map_t -> value_t

   val map_of_value : value_t -> map_t
   val value_of_map : map_t -> value_t

   val nested_map_of_value : value_t -> nested_map_t
   val value_of_nested_map : nested_map_t -> value_t
   
   val make_nested_map : (key_t * value_t) list -> pattern_t list -> nested_map_t
   val nested_map_has_entry : key_t -> nested_map_t -> bool
   val get_nested_map_entry : key_t -> nested_map_t -> value_t
   val add_nested_map_entry : key_t -> value_t -> nested_map_t -> nested_map_t
   val remove_nested_map_entry : key_t -> nested_map_t -> nested_map_t
end

module K3Maps : K3MapSpec 
    with type value_t      = K3Value.t and
         type single_map_t = K3Value.t K3ValuationMap.t and
         type map_t        = (K3Value.t K3ValuationMap.t) K3ValuationMap.t and
         type nested_map_t = K3Value.t K3ValuationMap.t
=
struct
   (**/**)
    module Map = K3ValuationMap
    include MK3Maps
   (**/**)
    open K3Value

    (* Additional K3 spec functions *)
    type nested_map_t = value_t Map.t

    let is_single_map v = match v with SingleMap _ -> true | _ -> false
    let is_map v = match v with DoubleMap _ -> true | _ -> false
    let is_nested_map v = match v with MapCollection _ -> true | _ -> false
    
    let value_of_smap m = SingleMap(m)
    let smap_of_value v = match v with
        | SingleMap(m) -> m
        | _ -> failwith ("invalid single map: "^(string_of_value v))

    let value_of_map m = DoubleMap(m)

    let map_of_value v = match v with
        | DoubleMap m -> m
        | _ -> failwith ("invalid double map: "^(string_of_value v))
    
    let value_of_nested_map nm = MapCollection(nm)
    let nested_map_of_value v = match v with
        | MapCollection(m) -> m
        | _ -> failwith ("invalid nested map: "^(string_of_value v))

    let make_nested_map contents patterns = Map.from_list contents patterns
    let nested_map_has_entry key nmap = Map.mem key nmap
    let get_nested_map_entry key nmap = Map.find key nmap
    let add_nested_map_entry key value nmap = Map.add key value nmap
    let remove_nested_map_entry key nmap = Map.remove key nmap
end

(* Implementation common to all M3 DBs
 * -- e.g. re-exporting types and methods from module parameters
 *)
module DBCommon(N : MapName)(S : M3MapSpec) =
struct
   module M3CP         = Patterns
   module DBM          = SliceableMap.Make(N)
   
   type value_t        = S.value_t
   type key_t          = S.key_t
   
   type map_name_t     = N.t
   type db_key_t       = map_name_t list
      
   type dom_t          = value_t list
   type schema_t       = K3.map_t list
   type pattern_t      = S.pattern_t
   type patterns_t     = Patterns.pattern_map

   type single_map_t   = S.single_map_t
   type map_t          = S.map_t

   type list_smap      = (key_t * value_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (db_key_t * list_map) list

   let zero = S.zero

   let map_name_to_string = N.to_string
   let string_to_map_name = N.of_string

   let value_to_string    = S.string_of_value
   let key_to_string k    = ListExtras.ocaml_of_list value_to_string k

   let smap_to_string ?(sep = ";\n") (m: single_map_t) = S.string_of_smap ~sep:sep m
   let map_to_string ?(sep = ";\n") (m: map_t) = S.string_of_map ~sep:sep m

   (* Debugging and testing helpers *)
   let showsmap (s: single_map_t) = S.smap_to_list s
   
   let show_sorted_smap (s: single_map_t) =
      List.sort (fun (k1,_) (k2,_) -> compare k1 k2) (showsmap s)

   let showmap (m: map_t) = 
      List.map (fun (k,sm) -> k, S.smap_to_list sm) (S.map_to_list m)
   
   let show_sorted_map (m: map_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))
end

module type K3DB =
sig
   type value_t
   type key_t          = value_t list
   
   type map_name_t
   type db_key_t       = map_name_t list

   type dom_t          = value_t list
   type schema_t       = K3.map_t list
   type pattern_t      = int list
   type patterns_t     = Patterns.pattern_map

   type list_smap      = (key_t * value_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (db_key_t * list_map) list

   type single_map_t
   type map_t

   type db_t

   val zero : value_t

   (* Names to strings *)
   val map_name_to_string : map_name_t -> string
   val string_to_map_name : string -> map_name_t

   (* Content to strings *)
   val value_to_string : value_t -> string 
   val smap_to_string : ?sep:string -> single_map_t -> string
   val map_to_string : ?sep:string -> map_t -> string
   val db_to_string : db_t -> string

   val get_in_patterns : map_name_t -> db_t -> pattern_t list
   val get_out_patterns : map_name_t -> db_t -> pattern_t list

   val make_empty_db : schema_t -> patterns_t -> db_t
   (*val make_empty_db_wdom : schema_t -> patterns_t -> dom_t -> db_t*)

   (* Membership tests *)
   val has_map     : map_name_t -> db_t -> bool
   val has_in_map  : map_name_t -> db_t -> bool
   val has_out_map : map_name_t -> db_t -> bool
   val has_value   : map_name_t -> db_t -> bool

   (* Getters *)
   val get_map     : map_name_t -> db_t -> map_t
   val get_in_map  : map_name_t -> db_t -> single_map_t 
   val get_out_map : map_name_t -> db_t -> single_map_t
   val get_value   : map_name_t -> db_t -> value_t option

   (* Bulk updates *)
   val update_map : map_name_t -> key_t -> single_map_t -> db_t -> unit
   val update_in_map : map_name_t -> single_map_t -> db_t -> unit
   val update_out_map : map_name_t -> single_map_t -> db_t -> unit

   (* Single value updates *)
   val update_map_value : map_name_t -> key_t -> key_t -> value_t -> db_t -> unit
   val update_in_map_value : map_name_t -> key_t -> value_t -> db_t -> unit
   val update_out_map_value : map_name_t -> key_t -> value_t -> db_t -> unit
   val update_value: map_name_t -> value_t -> db_t -> unit

   (* Debugging and testing helpers *)
   val showsmap         : single_map_t -> list_smap
   val show_sorted_smap : single_map_t -> list_smap
   val showmap          : map_t -> list_map
   val show_sorted_map  : map_t -> list_map
   val showdb           : db_t -> named_list_map
   val show_sorted_db   : db_t -> named_list_map
   (* Additional K3DB accessors *)
   type nested_map_t

   val has_nested_map : map_name_t -> db_t -> bool
   val get_nested_map : map_name_t -> db_t -> nested_map_t
   val update_nested_map :
      map_name_t -> key_t list -> pattern_t list list -> value_t -> db_t -> unit


   (* Remove elements *)
   val remove_map_element : map_name_t -> key_t -> key_t -> db_t -> unit
   val remove_in_map_element : map_name_t -> key_t -> db_t -> unit
   val remove_out_map_element : map_name_t -> key_t -> db_t -> unit

end

module NameableK3Database(N : MapName)(S : K3MapSpec) : K3DB
    with type map_name_t   = N.t
    and  type db_key_t     = N.t list
    and  type value_t      = S.value_t
    and  type single_map_t = S.single_map_t
    and  type map_t        = S.map_t
    and  type nested_map_t = S.nested_map_t
=
struct
   (**/**)
   include DBCommon(N)(S)
   (**/**)

   type nested_map_t   = S.nested_map_t
   type db_t           = (value_t DBM.t) * patterns_t  

   let db_to_string ((m,_): db_t) : string =
      let dbkey_to_s db_k = String.concat "," (List.map N.to_string db_k) in
      let dbv_to_string v =
         if S.is_single_map v then smap_to_string (S.smap_of_value v)
         else if S.is_map v then map_to_string (S.map_of_value v)
         else value_to_string v
      in (DBM.to_string dbkey_to_s dbv_to_string  m)

   let get_patterns patterns mapn =
      (M3CP.get_in_patterns patterns mapn, M3CP.get_out_patterns patterns mapn)

   let get_in_patterns mapn (_,pats) =
      M3CP.get_in_patterns pats (map_name_to_string mapn)
   
   let get_out_patterns mapn (_,pats) =
      M3CP.get_out_patterns pats (map_name_to_string mapn)

   let make_empty_db (schema:K3.map_t list) (patterns:Patterns.pattern_map):
                     db_t =
       let f (mapn, itypes, otypes, mapt) =
          let (in_pat, out_pat) = get_patterns patterns mapn in
          let init_slice = match (itypes,otypes) with
            | ([],[]) -> S.zero_of_type mapt
            | (x,[]) -> S.value_of_smap (S.make_smap [] in_pat)
            | ([],x) -> S.value_of_smap (S.make_smap [] out_pat)
            | (x,y) ->  S.value_of_map (S.make_map in_pat)
          in ([N.of_string mapn], init_slice) 
       in (DBM.from_list (List.map f schema) [], patterns)


    (* Retrieval helper *)
    let get_aux mapn (db,_) =
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_aux "^(N.to_string mapn))


    (* Persistent collections *)
    let has_map mapn (db,pm) =
        (DBM.mem [mapn] db) && S.is_map (get_aux mapn (db,pm))

    (* TODO: separate in and out single maps *)
    let has_in_map mapn (db,pm) =
        (DBM.mem [mapn] db) && S.is_single_map (get_aux mapn (db,pm)) 

    let has_out_map mapn (db,pm) =
        (DBM.mem [mapn] db) && S.is_single_map (get_aux mapn (db,pm))

    let has_value mapn (db,pm) =
        if DBM.mem [mapn] db then
            let v = get_aux mapn (db,pm) in
            not (S.is_map v || S.is_single_map v || S.is_nested_map v)
        else false

    let has_nested_map mapn (db,pm) =
        (DBM.mem [mapn] db) && S.is_nested_map (get_aux mapn (db,pm))

    (* Database retrieval methods *)
    let get_map id db     = S.map_of_value (get_aux id db)
    let get_in_map id db  = S.smap_of_value (get_aux id db)
    let get_out_map id db = S.smap_of_value (get_aux id db)
    let get_value id db   = Some(get_aux id db)
    let get_nested_map id db = S.nested_map_of_value(get_aux id db)


    (* Database udpate methods *)
    let update_db_value mapn value (db,_) = ignore(DBM.add [mapn] value db)

    let update_db_map_entry mapn img value (db,_) =
       let m = S.map_of_value (DBM.find [mapn] db) in
       ignore(DBM.add [mapn] (S.value_of_map (S.add_map_entry img value m)) db)

    let update_db_smap_entry mapn img new_value (db,_) =
       let m = S.smap_of_value (DBM.find [mapn] db) in
       ignore(DBM.add [mapn] (S.value_of_smap (S.add_smap_entry img new_value m)) db)

    (* Generic nested update *)
    let update_db_value_chain mapn imgs patterns new_value (db,_) =
      let rec init imgs pats = match imgs,pats with
        | ([h1],[h2]) -> S.value_of_smap (S.make_smap [h1,new_value] h2)
        | ([h1;t1],[h2;t2]) -> S.value_of_map
           (S.make_map_from_smaps [h1,(S.smap_of_value new_value)] h2)
        | (h1::t1, h2::t2) -> S.value_of_nested_map 
           (S.make_nested_map [h1, (init t1 t2)] h2)
        | _,_ -> failwith "invalid nested update init" 
      in
      let rec aux rem_imgs rem_pats v =
        match rem_imgs, rem_pats with
        | ([x],_) -> S.value_of_smap
           (S.add_smap_entry x new_value (S.smap_of_value v))
        | ([h;t],[h2;t2]) ->
            let m = S.map_of_value v in
            let new_nested = 
               if S.map_has_entry h m
               then aux [t] [t2] (S.value_of_smap (S.get_map_entry h m))
               else init [t] [t2]
            in S.value_of_map (S.add_map_entry h (S.smap_of_value new_nested) m)
        | (h::t,h2::t2) ->
            let nm = S.nested_map_of_value v in
            let new_nested =
               if S.nested_map_has_entry h nm
               then aux t t2 (S.get_nested_map_entry h nm)
               else init t t2
            in S.value_of_nested_map (S.add_nested_map_entry h new_nested nm)
        | _,_ -> failwith "invalid nested update add"
      in ignore(DBM.add [mapn] (aux imgs patterns (DBM.find [mapn] db)) db) 

    let update_db_nested_value_entry mapn inv_img outv_img new_value (db,pats) =
      let (in_pats,out_pats) = get_patterns pats (map_name_to_string mapn) in
      update_db_value_chain
         mapn [inv_img; outv_img] [in_pats;out_pats] new_value (db,pats)

    let update_map mapn img smap db =
        update_db_map_entry mapn img smap db
    
    let update_in_map mapn smap db =
        update_db_value mapn (S.value_of_smap smap) db
    
    let update_out_map mapn smap db =
        update_db_value mapn (S.value_of_smap smap) db

    let update_map_value = update_db_nested_value_entry
    let update_in_map_value = update_db_smap_entry    
    let update_out_map_value = update_db_smap_entry
    let update_value = update_db_value

    let update_nested_map = update_db_value_chain

    (* Database remove methods *)

    let remove_db_smap_entry mapn img (db,_) =
       let m = S.smap_of_value (DBM.find [mapn] db) in
       ignore(DBM.add [mapn] (S.value_of_smap (S.remove_smap_entry img m)) db)

    (* Generic nested remove *)
    let remove_db_element_chain mapn imgs patterns (db,_) =
      let rec aux rem_imgs rem_pats v =
        match rem_imgs, rem_pats with
        | ([x],_) -> S.value_of_smap
           (S.remove_smap_entry x (S.smap_of_value v))
        | ([h;t],[h2;t2]) ->
            let m = S.map_of_value v in
            let new_nested = aux [t] [t2] (S.value_of_smap (S.get_map_entry h m)) in 
                let new_map = 
                        if (S.smap_is_empty (S.smap_of_value new_nested)) then
                            S.remove_map_entry h m
                        else
                            S.add_map_entry h (S.smap_of_value new_nested) m in
                S.value_of_map (new_map)
        | (h::t,h2::t2) ->
            let nm = S.nested_map_of_value v in
            let new_nested = aux t t2 (S.get_nested_map_entry h nm) in 
                let new_map = 
                        if (S.map_is_empty (S.map_of_value new_nested)) then
                            S.remove_nested_map_entry h nm
                        else
                            S.add_nested_map_entry h new_nested nm in
                S.value_of_nested_map (new_map)
        | _,_ -> failwith "invalid nested remove"
      in ignore(DBM.add [mapn] (aux imgs patterns (DBM.find [mapn] db)) db) 

    let remove_db_nested_entry mapn inv_img outv_img (db,pats) =
      let (in_pats,out_pats) = get_patterns pats (map_name_to_string mapn) in
      remove_db_element_chain
         mapn [inv_img; outv_img] [in_pats;out_pats] (db,pats)

    let remove_map_element = remove_db_nested_entry
    let remove_in_map_element = remove_db_smap_entry    
    let remove_out_map_element = remove_db_smap_entry

    (* Debugging and testing helpers *)
    let showdb_f show_f db = failwith "NYI"
    let showdb db = failwith "NYI"
    let show_sorted_db db = failwith "NYI"
end

(*************************************
 * K3 database implementations
 * -- use these modules in your code.
 **************************************)

module type NamedK3DB = K3DB
    with type map_name_t   = NamedMap.t
    and  type db_key_t     = NamedMap.t list
    and  type value_t      = K3Maps.value_t
    and  type single_map_t = K3Maps.single_map_t
    and  type map_t        = K3Maps.map_t
    and  type nested_map_t = K3Maps.nested_map_t

module type RefK3DB = K3DB
    with type map_name_t   = RefMap.t
    and  type db_key_t     = RefMap.t list
    and  type value_t      = K3Maps.value_t
    and  type single_map_t = K3Maps.single_map_t
    and  type map_t        = K3Maps.map_t
    and  type nested_map_t = K3Maps.nested_map_t

module NamedK3Database : NamedK3DB = NameableK3Database(NamedMap)(K3Maps)
module K3Database : RefK3DB = NameableK3Database(RefMap)(K3Maps)

