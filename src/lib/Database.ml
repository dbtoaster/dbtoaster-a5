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

   val string_of_value : value_t -> string
   val string_of_smap : single_map_t -> string
   val string_of_map : map_t -> string

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
    val string_of_value : t -> string
    val string_of_smap : single_map_t -> string
    val string_of_map : map_t -> string
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
end

module CInOut : SliceableInOutMap
    with type t            = ConstTValue.t and
         module Map        = M3ValuationMap and
         type single_map_t = ConstTValue.t M3ValuationMap.t and
         type map_t        = (ConstTValue.t M3ValuationMap.t) M3ValuationMap.t
=
struct
    type t            = ConstTValue.t
    module Map        = M3ValuationMap
    type single_map_t = t Map.t
    type map_t        = single_map_t Map.t

    let zero               = ConstTValue.zero
    let string_of_value    = ConstTValue.to_string
    let key_to_string k    = ListExtras.ocaml_of_list string_of_value k
    let string_of_smap sm  = Map.to_string key_to_string string_of_value sm
    let string_of_map m    = Map.to_string key_to_string string_of_smap m 
end

(* ConstTMaps are used in M3 databases *)
module ConstTMaps : M3MapSpec 
    with type value_t      = CInOut.t and
         type single_map_t = CInOut.t CInOut.Map.t and
         type map_t        = (CInOut.t CInOut.Map.t) CInOut.Map.t
= InOutMaps(CInOut)

(* MK3 maps are used to implement the M3 part of a K3 database *)
module MK3Maps : M3MapSpec
    with type value_t      = K3Value.t and
         type single_map_t = K3Value.t K3ValuationMap.t and
         type map_t        = (K3Value.t K3ValuationMap.t) K3ValuationMap.t
= InOutMaps(K3Value)


(********************
 * Databases
 ********************)

(* M3 databases *)

module type M3DB =
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
   val smap_to_string : single_map_t -> string
   val map_to_string : map_t -> string
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

   let smap_to_string (m: single_map_t) = S.string_of_smap m
   let map_to_string (m: map_t) = S.string_of_map m

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


(********************************************
 * Parameterized M3 Database implementations
 ********************************************)

module SimpleNameableM3Database(N : MapName)(S : M3MapSpec) : M3DB
    with type map_name_t   = N.t and
         type db_key_t     = N.t list and
         type value_t      = S.value_t and
         type single_map_t = S.single_map_t and
         type map_t        = S.map_t
=
struct
   (**/**)
   include DBCommon(N)(S)
   (**/**)

   type db_t = map_t DBM.t * patterns_t

   let get_db db = fst db
   let get_db_patterns db = snd db 

   let db_to_string (db: db_t) : string =
      let db_key_to_string db_k = String.concat "," (List.map N.to_string db_k)
      in DBM.to_string db_key_to_string map_to_string (get_db db)

   let get_patterns patterns mapn =
      (M3CP.get_in_patterns patterns mapn, M3CP.get_out_patterns patterns mapn)

   let get_in_patterns mapn (_,pats) =
      M3CP.get_in_patterns pats (map_name_to_string mapn)
   
   let get_out_patterns mapn (_,pats) =
      M3CP.get_out_patterns pats (map_name_to_string mapn)

   let make_empty_db (schema:K3.map_t list) (patterns:Patterns.pattern_map): 
                     db_t =
        let f (mapn, itypes, _) =
           let (in_patterns, out_patterns) = get_patterns patterns mapn in
           let init_slice =
              if(List.length itypes = 0) then
                 S.make_map_from_smaps
                    [([], (S.make_smap [] out_patterns))] in_patterns
              else
                 (* We defer adding out var indexes to init value computation *)
                 S.make_map in_patterns 
           in ([N.of_string mapn], init_slice)
        in
            (DBM.from_list (List.map f schema) [], patterns)
    
   let make_empty_db_wdom schema patterns (dom: S.value_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let (in_patterns, out_patterns) = get_patterns patterns mapn in
            let map = S.make_map_from_smaps
                        (List.map (fun t -> (t, S.make_smap [] out_patterns))
                            (ListExtras.k_tuples (List.length itypes) dom)) in_patterns
            in
            ([N.of_string mapn], map)
        in
            (DBM.from_list (List.map f schema) [], patterns)

   (* Membership accessors *)
   let has_map mapn db = DBM.mem [mapn] (get_db db)
   let has_in_map      = has_map
   let has_out_map     = has_map
   let has_value       = has_map

   let get_map mapn db = 
       try DBM.find [mapn] (get_db db)
       with Not_found -> failwith ("Database.get_map "^(N.to_string mapn))

   (* Implements in maps as single_map_t by swapping in/out tier *)
   let get_in_map mapn db = 
      let m = get_map mapn db in
      if S.map_has_entry [] m then S.get_map_entry [] m
      else failwith ("Database.get_in_map "^(N.to_string mapn))
   
   let get_out_map mapn db =
      let m = get_map mapn db in
      if S.map_has_entry [] m then S.get_map_entry [] m
      else failwith ("Database.get_out_map "^(N.to_string mapn))

   let get_value mapn db =
      let m = get_map mapn db in
      let slice =
         if S.map_has_entry [] m then S.get_map_entry [] m
         else failwith ("Database.get_value "^(N.to_string mapn))
      in if S.smap_has_entry [] slice
         then Some(S.get_smap_entry [] slice) else None


   (* Map tier updates *)
   let update_map mapn inv_imgs slice db : unit =
      try
         let m = DBM.find [mapn] (get_db db) in
         ignore(DBM.add [mapn] (S.add_map_entry inv_imgs slice m) (get_db db));
(*
         print_endline ("Updated the database: "^(N.to_string mapn)^
                      " inv="^(key_to_string inv_imgs)^
                      " slice="^(smap_to_string slice)^
                      " db="^(db_to_string db));
         Map.validate_indexes (Map.find inv_imgs (DBM.find [mapn] (get_db db)))
*)
      with Failure x -> failwith ("update_map: "^x)

   (* Implements in maps as single_map_t by swapping in/out tier *)
   let update_in_map mapn slice db : unit =
      try update_map mapn [] slice db
      with Failure x -> failwith ("update_in_map: "^x)

   let update_out_map mapn slice db : unit =
      try update_map mapn [] slice db
      with Failure x -> failwith ("update_out_map: "^x)

   (* Value updates *)              
   let update_map_value mapn inv_img outv_img new_value db : unit =
      try
         let m = DBM.find [mapn] (get_db db) in
         let new_slice = if S.map_has_entry inv_img m
            then S.add_smap_entry outv_img new_value (S.get_map_entry inv_img m) 
            else
            let out_patterns = M3CP.get_out_patterns
                (get_db_patterns db) (map_name_to_string mapn)
            in S.make_smap [(outv_img, new_value)] out_patterns
         in ignore(DBM.add [mapn]
                      (S.add_map_entry inv_img new_slice m) (get_db db));
(*
            print_endline ("Updating a db value: "^(N.to_string mapn)^
                           " inv="^(key_to_string inv_img)^
                           " outv="^(key_to_string outv_img)^
                           " v="^(value_to_string new_value)^
                           " db="^(db_to_string db));
            Map.validate_indexes (Map.find inv_img (DBM.find [mapn] (get_db db)))
*)
       with Failure x -> failwith ("update_map_value: "^x)

   (* Implements in maps as single_map_t by swapping in/out tier *)
   let update_in_map_value mapn img new_value db : unit =
      try update_map_value mapn [] img new_value db
      with Failure x -> failwith ("update_in_map_value: "^x)

   let update_out_map_value mapn img new_value db : unit =
      try update_map_value mapn [] img new_value db
      with Failure x -> failwith ("update_out_map_value: "^x)
   
   let update_value mapn new_value db : unit =
      try update_map_value mapn [] [] new_value db
      with Failure x -> failwith ("update_value: "^x)

   (* Debugging and testing helpers *)
   let showdb_f show_f db =
      DBM.fold (fun k v acc -> acc@[(k, (show_f v))]) [] (get_db db)
    
   let showdb db = showdb_f showmap db

   let show_sorted_db db =
      List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
         (showdb_f show_sorted_map db)
end


module NameableM3Database(N : MapName)(S : M3MapSpec) : M3DB
    with type map_name_t   = N.t and
         type db_key_t     = N.t list and
         type value_t      = S.value_t and
         type single_map_t = S.single_map_t and
         type map_t        = S.map_t
=
struct
   (**/**)
   include DBCommon(N)(S)
   (**/**)

   (* mutable value *)
   type mv_t           = value_t ref
   type db_t           = (map_t DBM.t) * (single_map_t DBM.t) *
                         (mv_t DBM.t) * patterns_t  

   let db_to_string ((mio,ms,mv,_): db_t) : string =
      let dbkey_to_s db_k = String.concat "," (List.map N.to_string db_k) in
      let aggref_to_s ar = (value_to_string !ar) in 
      (DBM.to_string dbkey_to_s map_to_string mio)^
      (DBM.to_string dbkey_to_s smap_to_string ms)^
      (DBM.to_string dbkey_to_s aggref_to_s mv)

   let get_patterns patterns mapn =
      (M3CP.get_in_patterns patterns mapn, M3CP.get_out_patterns patterns mapn)

   let get_in_patterns mapn (_,_,_,pats) =
      M3CP.get_in_patterns pats (map_name_to_string mapn)
   
   let get_out_patterns mapn (_,_,_,pats) =
      M3CP.get_out_patterns pats (map_name_to_string mapn)

   let make_empty_db (schema:K3.map_t list) (patterns:Patterns.pattern_map):
                     db_t =
        let io_f (mapn, itypes, _) =
           let (in_patterns, _) = get_patterns patterns mapn in
           (* We defer adding out var indexes to init value computation *)
           let init_slice = S.make_map in_patterns 
           in ([N.of_string mapn], init_slice)
        in
        let s_f (mapn, itypes, otypes) =
           let (in_pat, out_pat) = get_patterns patterns mapn in
           let init_slice =
              S.make_smap [] (if itypes = [] then out_pat else in_pat)
           in ([N.of_string mapn], init_slice)
        in 
        let val_f (mapn, _, _) = ([N.of_string mapn], ref(S.zero)) in
        let io_maps = 
            List.filter (fun (_,i,o) -> i <> [] && o <> []) schema in
        let in_maps = 
            List.filter (fun (_,i,o) -> i = [] && o <> []) schema in 
        let out_maps = 
            List.filter (fun (_,i,o) -> i <> [] && o = []) schema in
        let values = 
            List.filter (fun (_,i,o) -> i = [] && o = []) schema in
            (DBM.from_list (List.map io_f io_maps) [],
             DBM.from_list (List.map s_f (in_maps@out_maps)) [],
             DBM.from_list (List.map val_f values) [],
             patterns)

   (* Membership accessors *)
   let has_map mapn (db,_,_,_)     = DBM.mem [mapn] db
   let has_in_map mapn (_,db,_,_)  = DBM.mem [mapn] db
   let has_out_map mapn (_,db,_,_) = DBM.mem [mapn] db
   let has_value mapn (_,_,db,_)   = DBM.mem [mapn] db

   (* Getters *)
   let get_map mapn (db,_,_,_) = 
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_map "^(N.to_string mapn))
       
   let get_in_map mapn (_,db,_,_) = 
      try DBM.find [mapn] db
      with Not_found -> failwith ("Database.get_in_map "^(N.to_string mapn)) 
   
   let get_out_map mapn (_,db,_,_) =
      try DBM.find [mapn] db
      with Not_found -> failwith ("Database.get_out_map "^(N.to_string mapn))

   let get_value mapn (_,_,db,_) =
      try Some(!(DBM.find [mapn] db))
      with Not_found -> failwith ("Database.get_value "^(N.to_string mapn))


   (* Map tier updates *)
   let update_map mapn inv_imgs slice (db,_,_,_) : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (S.add_map_entry inv_imgs slice m) db);
(*
         print_endline ("Updated the database: "^(N.to_string mapn)^
                      " inv="^(key_to_string inv_imgs)^
                      " slice="^(smap_to_string slice)^
                      " db="^(db_to_string db));
         Map.validate_indexes (Map.find inv_imgs (DBM.find [mapn] db))
*)
      with Failure x -> failwith ("update: "^x)
              
   let update_smap mapn slice (_,db,_,_) : unit =
      ignore(DBM.add [mapn] slice db)
   
   let update_in_map mapn slice db : unit =
      try update_smap mapn slice db
      with Failure x -> failwith ("update_in_map: "^x)

   let update_out_map mapn slice db : unit =
      try update_smap mapn slice db
      with Failure x -> failwith ("update_out_map: "^x)


   (* Value updates *)
   let update_map_value mapn inv_img outv_img new_value (db,_,_,pats) : unit =
      try
         let m = DBM.find [mapn] db in
         let new_slice = if S.map_has_entry inv_img m
            then S.add_smap_entry outv_img new_value (S.get_map_entry inv_img m) 
            else let out_patterns =
                     M3CP.get_out_patterns pats (map_name_to_string mapn)
                in S.make_smap [(outv_img, new_value)] out_patterns
         in ignore(DBM.add [mapn] (S.add_map_entry inv_img new_slice m) db);
(*
            print_endline ("Updating a db value: "^(N.to_string mapn)^
                           " inv="^(key_to_string inv_img)^
                           " outv="^(key_to_string outv_img)^
                           " v="^(value_to_string new_value)^
                           " db="^(db_to_string db));
            Map.validate_indexes (Map.find inv_img (DBM.find [mapn] db))
*)
       with Failure x -> failwith ("update_value: "^x)

   let update_smap_value mapn img new_value (_,db,_,_) : unit =
      let s = DBM.find [mapn] db in
      ignore(DBM.add [mapn] (S.add_smap_entry img new_value s) db)

   let update_in_map_value mapn img new_value db : unit =
      try update_smap_value mapn img new_value db
      with Failure x -> failwith ("update_in_map_value: "^x)
   
   let update_out_map_value mapn img new_value db : unit =
      try update_smap_value mapn img new_value db
      with Failure x -> failwith ("update_out_map_value: "^x)

   let update_value mapn new_value (_,_,db,_) : unit =
      try let m = DBM.find [mapn] db in m := new_value
      with Failure x -> failwith ("update_value: "^x)

   (* Debugging and testing helpers *)
   let showval (v : S.value_t ref) = [([], !v)]

   let showdb_f show_mio_f show_ms_f show_mv_f (mio,ms,mv,_) =
       (DBM.fold (fun k v acc -> acc@[(k, show_mio_f v)]) [] mio)@
       (DBM.fold (fun k v acc -> acc@[(k, [([], show_ms_f v)])]) [] ms)@
       (DBM.fold (fun k v acc -> acc@[(k, [([], show_mv_f v)])]) [] mv)
    
   let showdb db = showdb_f showmap showsmap showval db

   let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map show_sorted_smap showval db)
end

(************************************
 * M3 Database implementations
 * -- use these modules in your code 
 ************************************)
module type NamedM3DB = M3DB
    with type map_name_t   = NamedMap.t and
         type value_t      = ConstTMaps.value_t and
         type single_map_t = ConstTMaps.single_map_t and
         type map_t        = ConstTMaps.map_t

module type RefM3DB = M3DB
    with type map_name_t   = RefMap.t and
         type value_t      = ConstTMaps.value_t and
         type single_map_t = ConstTMaps.single_map_t and
         type map_t        = ConstTMaps.map_t

module NamedSimpleM3Database : NamedM3DB = SimpleNameableM3Database(NamedMap)(ConstTMaps)
module SimpleM3Database : RefM3DB = SimpleNameableM3Database(RefMap)(ConstTMaps)

module NamedM3Database : NamedM3DB = NameableM3Database(NamedMap)(ConstTMaps)   
module M3Database : RefM3DB = NameableM3Database(RefMap)(ConstTMaps)



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
end

module type K3DB =
sig
   include M3DB

   (* Additional K3DB accessors *)
   type nested_map_t

   val has_nested_map : map_name_t -> db_t -> bool
   val get_nested_map : map_name_t -> db_t -> nested_map_t
   val update_nested_map :
      map_name_t -> key_t list -> pattern_t list list -> value_t -> db_t -> unit
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
       let f (mapn, itypes, otypes) =
          let (in_pat, out_pat) = get_patterns patterns mapn in
          let init_slice = match (itypes,otypes) with
            | ([],[]) -> S.zero
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

