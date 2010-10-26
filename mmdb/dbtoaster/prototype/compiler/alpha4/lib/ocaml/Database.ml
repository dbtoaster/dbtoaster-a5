(* Databases.
 * Databases are persistent stores of values and maps for M3/K3 programs.
 * We have two types of databases M3DBs and K3DBs, consisting of different types
 * of maps. K3DBs contain maps whose values and keys may be arbitrarily nested,
 * M3DBs have 1-level maps and 2-level maps.
 * Databases also support:
 * -- functorized naming, currently with textual, and integer naming. 
 *)
open M3
open Util
open Values

(* DB Name functorization *)
(* Maps can be accessed by name or unique id in generated code *)
module type MapName =
sig
   type t
   val to_string : t -> string
   val of_string : string -> t
   val compare : t -> t -> int
end

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

(* DB content functorization *)
module type M3DBSpec =
sig
   type value_t

   module Map : SliceableMap.S with type key_elt = value_t

   type single_map_t = value_t Map.t
   type map_t = single_map_t Map.t

   val string_of_value : value_t -> string

   (* initializer *)
   val zero : value_t
end

module M3Spec =
struct
    module Map = M3ValuationMap

    type value_t = const_t
    type single_map_t = value_t Map.t
    type map_t = single_map_t Map.t
    
    let string_of_value = M3Common.string_of_const
    
    let zero = CFloat(0.0)
end

module MK3Spec =
struct
    module Map = K3ValuationMap

    type value_t = K3Value.t
    type single_map_t = value_t Map.t
    type map_t = single_map_t Map.t
    
    let string_of_value = K3Value.to_string
    
    let zero = K3Value.zero
end

(* M3 databases *)

module type M3DB =
sig
   type value_t        
   type key_t          = value_t list
   
   type dom_t          = value_t list
   type schema_t       = map_type_t list
   type pattern_t      = int list
   type patterns_t     = M3Common.Patterns.pattern_map

   type map_name_t
   type db_key_t       = map_name_t list
   
   type single_map_t
   type map_t
   type db_t
   
   type list_smap      = (key_t * value_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (db_key_t * list_map) list
   
   val zero : value_t

   val value_to_string : value_t -> string 
   
   val map_name_to_string : map_name_t -> string
   val string_to_map_name : string -> map_name_t

   val smap_to_string : single_map_t -> string
   val map_to_string : map_t -> string
   val db_to_string : db_t -> string

   val get_in_patterns : map_name_t -> db_t -> pattern_t list
   val get_out_patterns : map_name_t -> db_t -> pattern_t list

   val make_empty_db : schema_t -> patterns_t -> db_t
   (*val make_empty_db_wdom : schema_t -> patterns_t -> dom_t -> db_t*)

   val update_map : map_name_t -> key_t -> single_map_t -> db_t -> unit
   val update_in_map : map_name_t -> single_map_t -> db_t -> unit
   val update_out_map : map_name_t -> single_map_t -> db_t -> unit

   val update_map_value : map_name_t -> key_t -> key_t -> value_t -> db_t -> unit
   val update_in_map_value : map_name_t -> key_t -> value_t -> db_t -> unit
   val update_out_map_value : map_name_t -> key_t -> value_t -> db_t -> unit
   val update_value: map_name_t -> value_t -> db_t -> unit

   val has_map     : map_name_t -> db_t -> bool
   val has_smap    : map_name_t -> db_t -> bool
   val has_value   : map_name_t -> db_t -> bool

   val get_map     : map_name_t -> db_t -> map_t
   val get_in_map  : map_name_t -> db_t -> single_map_t 
   val get_out_map : map_name_t -> db_t -> single_map_t
   val get_value   : map_name_t -> db_t -> value_t option

   val showsmap         : single_map_t -> list_smap
   val show_sorted_smap : single_map_t -> list_smap
   val showmap          : map_t -> list_map
   val show_sorted_map  : map_t -> list_map
   val showdb           : db_t -> named_list_map
   val show_sorted_db   : db_t -> named_list_map
end

module SimpleNameableM3Database(N : MapName)(S : M3DBSpec) : M3DB
    with type map_name_t   = N.t and
         type db_key_t     = N.t list and
         type value_t      = S.value_t and
         type single_map_t = S.single_map_t and
         type map_t        = S.map_t
=
struct
   module M3CP = M3Common.Patterns

   module Map          = S.Map
   module DBM          = SliceableMap.Make(N)
   
   type value_t        = S.value_t
   type key_t          = value_t list
   
   type dom_t          = value_t list
   type schema_t       = map_type_t list
   type pattern_t      = int list
   type patterns_t     = M3Common.Patterns.pattern_map

   type map_name_t     = N.t
   type db_key_t       = map_name_t list
      
   type single_map_t   = S.single_map_t
   type map_t          = S.map_t
   type db_t           = map_t DBM.t * patterns_t

   type list_smap      = (key_t * value_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (db_key_t * list_map) list

   let get_db db = fst db
   let get_db_patterns db = snd db 

   let zero = S.zero

   let value_to_string    = S.string_of_value
   let map_name_to_string = N.to_string
   let string_to_map_name = N.of_string

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string value_to_string k

   let smap_to_string (m: single_map_t) =
      Map.to_string key_to_string value_to_string m

   let map_to_string (m: map_t) =
      Map.to_string key_to_string smap_to_string m

   let db_to_string (db: db_t) : string =
      let db_key_to_string db_k = String.concat "," (List.map N.to_string db_k)
      in DBM.to_string db_key_to_string map_to_string (get_db db)

   let get_patterns patterns mapn =
      (M3CP.get_in_patterns patterns mapn, M3CP.get_out_patterns patterns mapn)

   let get_in_patterns mapn (_,pats) =
      M3CP.get_in_patterns pats (map_name_to_string mapn)
   
   let get_out_patterns mapn (_,pats) =
      M3CP.get_out_patterns pats (map_name_to_string mapn)

   let make_empty_db schema patterns: db_t =
        let f (mapn, itypes, _) =
           let (in_patterns, out_patterns) = get_patterns patterns mapn in
           let init_slice =
              if(List.length itypes = 0) then
                 Map.from_list [([], Map.from_list [] out_patterns)] in_patterns
              else
                 (* We defer adding out var indexes to init value computation *)
                 Map.empty_map_w_patterns in_patterns 
           in ([N.of_string mapn], init_slice)
        in
            (DBM.from_list (List.map f schema) [], patterns)
    
    
   let make_empty_db_wdom schema patterns (dom: S.value_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let (in_patterns, out_patterns) = get_patterns patterns mapn in
            let map = Map.from_list
                        (List.map (fun t -> (t, Map.empty_map_w_patterns out_patterns))
                            (Util.k_tuples (List.length itypes) dom)) in_patterns
            in
            ([N.of_string mapn], map)
        in
            (DBM.from_list (List.map f schema) [], patterns)

   (* Map tier updates *)
   let update_map mapn inv_imgs slice db : unit =
      try
         let m = DBM.find [mapn] (get_db db) in
         ignore(DBM.add [mapn] (Map.add inv_imgs slice m) (get_db db));
(*
         print_endline ("Updated the database: "^(N.to_string mapn)^
                      " inv="^(key_to_string inv_imgs)^
                      " slice="^(smap_to_string slice)^
                      " db="^(db_to_string db));
         Map.validate_indexes (Map.find inv_imgs (DBM.find [mapn] (get_db db)))
*)
      with Failure x -> failwith ("update_map: "^x)

   let update_in_map mapn slice db : unit = failwith "Not needed."

   let update_out_map mapn slice db : unit =
      try update_map mapn [] slice db
      with Failure x -> failwith ("update_out_map: "^x)

   (* Value updates *)              
   let update_map_value mapn inv_img outv_img new_value db : unit =
      try
         let m = DBM.find [mapn] (get_db db) in
         let new_slice = if Map.mem inv_img m
            then Map.add outv_img new_value (Map.find inv_img m) 
            else
            let out_patterns = M3CP.get_out_patterns
                (get_db_patterns db) (map_name_to_string mapn)
            in Map.from_list [(outv_img, new_value)] out_patterns
         in ignore(DBM.add [mapn] (Map.add inv_img new_slice m) (get_db db));
(*
            print_endline ("Updating a db value: "^(N.to_string mapn)^
                           " inv="^(key_to_string inv_img)^
                           " outv="^(key_to_string outv_img)^
                           " v="^(value_to_string new_value)^
                           " db="^(db_to_string db));
            Map.validate_indexes (Map.find inv_img (DBM.find [mapn] (get_db db)))
*)
       with Failure x -> failwith ("update_map_value: "^x)

   let update_in_map_value mapn img new_value db : unit =
      try update_map_value mapn img [] new_value db
      with Failure x -> failwith ("update_in_map_value: "^x)

   let update_out_map_value mapn img new_value db : unit =
      try update_map_value mapn [] img new_value db
      with Failure x -> failwith ("update_out_map_value: "^x)
   
   let update_value mapn new_value db : unit =
      try update_map_value mapn [] [] new_value db
      with Failure x -> failwith ("update_value: "^x)

   let has_map mapn db = DBM.mem [mapn] (get_db db)
   let has_smap = has_map
   let has_value = has_map

   let get_map mapn db = 
       try DBM.find [mapn] (get_db db)
       with Not_found -> failwith ("Database.get_map "^(N.to_string mapn))
       
   let get_in_map mapn db = failwith ("Database.get_in_map "^(N.to_string mapn))
   
   let get_out_map mapn db =
      let m = get_map mapn db in
      if Map.mem [] m then Map.find [] m
      else failwith ("Database.get_out_map "^(N.to_string mapn))
      
   let get_value mapn db =
      let m = get_map mapn db in
      let slice =
         if Map.mem [] m then Map.find [] m
         else failwith ("Database.get_out_map "^(N.to_string mapn))
      in if Map.mem [] slice then Some(Map.find [] slice) else None

   let showsmap (s: single_map_t) = Map.to_list s
   
   let show_sorted_smap (s: single_map_t) =
      List.sort (fun (k1,_) (k2,_) -> compare k1 k2) (showsmap s)

   let showmap (m: map_t) = Map.to_list (Map.map Map.to_list m)
   
   let show_sorted_map (m: map_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))

    let showdb_f show_f db =
       DBM.fold (fun k v acc -> acc@[(k, (show_f v))]) [] (get_db db)
    
    let showdb db = showdb_f showmap db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map db)
end


module NameableM3Database(N : MapName)(S : M3DBSpec) : M3DB
    with type map_name_t   = N.t and
         type db_key_t     = N.t list and
         type value_t      = S.value_t and
         type single_map_t = S.single_map_t and
         type map_t        = S.map_t
=
struct
   module M3CP = M3Common.Patterns

   module Map          = S.Map
   module DBM          = SliceableMap.Make(N)
   
   type value_t        = S.value_t
   type key_t          = value_t list

   type dom_t          = value_t list
   type schema_t       = map_type_t list
   type pattern_t      = int list
   type patterns_t     = M3Common.Patterns.pattern_map

   type map_name_t     = N.t   
   type db_key_t       = map_name_t list
      
   type mv_t           = value_t ref
   type single_map_t   = S.single_map_t
   type map_t          = S.map_t

   type db_t           = (map_t DBM.t) * (single_map_t DBM.t) *
                         (mv_t DBM.t) * patterns_t  

   type list_smap      = (key_t * value_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (db_key_t * list_map) list

   let zero = S.zero

   let value_to_string    = S.string_of_value
   let map_name_to_string = N.to_string
   let string_to_map_name = N.of_string

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string value_to_string k

   let smap_to_string (m: single_map_t) =
      Map.to_string key_to_string value_to_string m

   let map_to_string (m: map_t) =
      Map.to_string key_to_string smap_to_string m

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

   let make_empty_db schema patterns: db_t =
        let io_f (mapn, itypes, _) =
           let (in_patterns, _) = get_patterns patterns mapn in
           (* We defer adding out var indexes to init value computation *)
           let init_slice = Map.empty_map_w_patterns in_patterns 
           in ([N.of_string mapn], init_slice)
        in
        let s_f (mapn, itypes, otypes) =
           let (in_pat, out_pat) = get_patterns patterns mapn in
           let init_slice =
              Map.empty_map_w_patterns (if itypes = [] then out_pat else in_pat)
           in ([N.of_string mapn], init_slice)
        in 
        let val_f (mapn, _, _) = ([N.of_string mapn], ref(S.zero)) in
        let io_maps = List.filter (fun (_,i,o) -> i <> [] && o <> []) schema in
        let in_maps = List.filter (fun (_,i,o) -> i = [] && o <> []) schema in 
        let out_maps = List.filter (fun (_,i,o) -> i <> [] && o = []) schema in
        let values = List.filter (fun (_,i,o) -> i = [] && o = []) schema in
            (DBM.from_list (List.map io_f io_maps) [],
             DBM.from_list (List.map s_f (in_maps@out_maps)) [],
             DBM.from_list (List.map val_f values) [],
             patterns)

   (* Map tier updates *)
   let update_map mapn inv_imgs slice (db,_,_,_) : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (Map.add inv_imgs slice m) db);
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
         let new_slice = if Map.mem inv_img m
            then Map.add outv_img new_value (Map.find inv_img m) 
            else let out_patterns =
                     M3CP.get_out_patterns pats (map_name_to_string mapn)
                in Map.from_list [(outv_img, new_value)] out_patterns
         in ignore(DBM.add [mapn] (Map.add inv_img new_slice m) db);
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
      ignore(DBM.add [mapn] (Map.add img new_value s) db)

   let update_in_map_value mapn img new_value db : unit =
      try update_smap_value mapn img new_value db
      with Failure x -> failwith ("update_in_map_value: "^x)
   
   let update_out_map_value mapn img new_value db : unit =
      try update_smap_value mapn img new_value db
      with Failure x -> failwith ("update_out_map_value: "^x)
   
   let update_value mapn new_value (_,_,db,_) : unit =
      try let m = DBM.find [mapn] db in m := new_value
      with Failure x -> failwith ("update_value: "^x)

   let has_map mapn (db,_,_,_) = DBM.mem [mapn] db

   let has_smap mapn (_,db,_,_) = DBM.mem [mapn] db

   let has_value mapn (db,_,_,_) = DBM.mem [mapn] db

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

   let showval (v : S.value_t ref) = [([], !v)]

   let showsmap (s : single_map_t) = Map.to_list s
   
   let show_sorted_smap (s : single_map_t) =
      List.sort (fun (k1,_) (k2,_) -> compare k1 k2) (Map.to_list s)

   let showmap (m: map_t) = Map.to_list (Map.map Map.to_list m)   
   
   let show_sorted_map (m: map_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))

    let showdb_f show_mio_f show_ms_f show_mv_f (mio,ms,mv,_) =
       (DBM.fold (fun k v acc -> acc@[(k, show_mio_f v)]) [] mio)@
       (DBM.fold (fun k v acc -> acc@[(k, [([], show_ms_f v)])]) [] ms)@
       (DBM.fold (fun k v acc -> acc@[(k, [([], show_mv_f v)])]) [] mv)
    
    let showdb db = showdb_f showmap showsmap showval db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map show_sorted_smap showval db)
end

(* Database implementations *)
module type NamedM3DB = M3DB
    with type map_name_t   = NamedMap.t and
         type value_t      = M3Spec.value_t and
         type single_map_t = M3Spec.single_map_t and
         type map_t        = M3Spec.map_t

module type RefM3DB = M3DB
    with type map_name_t   = RefMap.t and
         type value_t      = M3Spec.value_t and
         type single_map_t = M3Spec.single_map_t and
         type map_t        = M3Spec.map_t

module NamedSimpleM3Database : NamedM3DB = SimpleNameableM3Database(NamedMap)(M3Spec)
module SimpleM3Database : RefM3DB = SimpleNameableM3Database(RefMap)(M3Spec)

module NamedM3Database : NamedM3DB = NameableM3Database(NamedMap)(M3Spec)   
module M3Database : RefM3DB = NameableM3Database(RefMap)(M3Spec)



(* K3 databases
 * -- named maps (i.e. database entries) can be arbitrarily nested values *)

module type K3DBSpec =
sig
   type value_t

   module Map : SliceableMap.S with type key_elt = value_t

   type map_t = value_t Map.t
   val is_map : value_t -> bool
   val map_of_value : value_t -> map_t
   val value_of_map : map_t -> value_t

   val string_of_value : value_t -> string

   (* initializer *)
   val zero : value_t
end

module K3Spec =
struct
    module Map = K3ValuationMap

    type value_t = K3Value.t
    type map_t = value_t Map.t
    
    let is_map v = match v with K3Value.MapCollection _ -> true | _ -> false
    
    let map_of_value v = match v with
        | K3Value.MapCollection(m) -> m
        | _ -> failwith "invalid map"
    
    let value_of_map m = K3Value.MapCollection(m)
    
    let string_of_value = K3Value.to_string
    
    let zero = K3Value.zero
end

module type K3DB =
sig
   type value_t        
   type key_t          = value_t list
   
   type dom_t          = value_t list
   type schema_t       = map_type_t list
   type pattern_t      = int list
   type patterns_t     = M3Common.Patterns.pattern_map

   type map_name_t
   type db_key_t       = map_name_t list

(* type map_t *)
   type db_t
   
   type list_map       = (key_t * value_t) list
   type named_list_map = (db_key_t * list_map) list
   
   val zero : value_t

   val map_name_to_string : map_name_t -> string
   val string_to_map_name : string -> map_name_t

   val value_to_string : value_t -> string 
(* val map_to_string : map_t -> string *)
   val db_to_string : db_t -> string

   val get_in_patterns : map_name_t -> db_t -> pattern_t list
   val get_out_patterns : map_name_t -> db_t -> pattern_t list

   val make_empty_db : schema_t -> patterns_t -> db_t

   val update_value  : map_name_t -> value_t -> db_t -> unit
   val update_map    : map_name_t -> key_t -> value_t -> db_t -> unit
   val update_io_map : map_name_t -> key_t -> key_t -> value_t -> db_t -> unit
   val update_nested_map :
      map_name_t -> key_t list -> pattern_t list list -> value_t -> db_t -> unit

   val has_value : map_name_t -> db_t -> bool
   val get_value : map_name_t -> db_t -> value_t
end

module NameableK3Database(N : MapName)(S : K3DBSpec) : K3DB
    with type map_name_t   = N.t
    and  type db_key_t     = N.t list
    and  type value_t      = S.value_t
(*
    and  type map_t        = S.map_t
*)
=
struct
   module M3CP = M3Common.Patterns

   module Map          = S.Map
   module DBM          = SliceableMap.Make(N)
   
   type value_t        = S.value_t
   type key_t          = value_t list

   type dom_t          = value_t list
   type schema_t       = map_type_t list
   type pattern_t      = int list
   type patterns_t     = M3Common.Patterns.pattern_map

   type map_name_t     = N.t   
   type db_key_t       = map_name_t list
      
   type map_t          = S.map_t

   type db_t           = (value_t DBM.t) * patterns_t  

   type list_map       = (key_t * value_t) list
   type named_list_map = (db_key_t * list_map) list

   let zero = S.zero

   let map_name_to_string = N.to_string
   let string_to_map_name = N.of_string

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let value_to_string    = S.string_of_value
   let key_to_string k = Util.list_to_string value_to_string k

   let map_to_string (m: map_t) =
      Map.to_string key_to_string value_to_string m

   let db_to_string ((m,_): db_t) : string =
      let dbkey_to_s db_k = String.concat "," (List.map N.to_string db_k) in
      let dbv_to_string v = if S.is_map v
         then map_to_string (S.map_of_value v) else value_to_string v
      in (DBM.to_string dbkey_to_s dbv_to_string  m)

   let get_patterns patterns mapn =
      (M3CP.get_in_patterns patterns mapn, M3CP.get_out_patterns patterns mapn)

   let get_in_patterns mapn (_,pats) =
      M3CP.get_in_patterns pats (map_name_to_string mapn)
   
   let get_out_patterns mapn (_,pats) =
      M3CP.get_out_patterns pats (map_name_to_string mapn)

   let make_empty_db schema patterns: db_t =
       let f (mapn, itypes, otypes) =
          let (in_pat, out_pat) = get_patterns patterns mapn in
          let init_slice = match (itypes,otypes) with
            | ([],[]) -> S.zero
            | (x,[]) -> S.value_of_map (Map.empty_map_w_patterns in_pat)
            | ([],x) -> S.value_of_map (Map.empty_map_w_patterns out_pat)
            | (x,y) ->  S.value_of_map (Map.empty_map_w_patterns in_pat)
          in ([N.of_string mapn], init_slice) 
       in (DBM.from_list (List.map f schema) [], patterns)

   let update_value mapn new_value (db,_) = ignore(DBM.add [mapn] new_value db)

   let update_map mapn img new_value (db,_) =
	  let m = S.map_of_value (DBM.find [mapn] db) in
	  ignore(DBM.add [mapn] (S.value_of_map (Map.add img new_value m)) db)

   (* Generic nested update *)
   let update_nested_map mapn imgs patterns new_value (db,_) =
      let rec init imgs pats = match imgs,pats with
        | ([],[]) -> new_value
        | (h1::t1, h2::t2) ->
            S.value_of_map (Map.from_list [h1, (init t1 t2)] h2)
        | _,_ -> failwith "invalid nested update init" 
      in
      let rec aux rem_imgs rem_pats v =
        let m = S.map_of_value v in
        match rem_imgs, rem_pats with
        | ([x],_) -> S.value_of_map (Map.add x new_value m)
        | (h::t,h2::t2) ->
            let new_nested =
               if Map.mem h m then aux t t2 (Map.find h m) else init t t2
            in S.value_of_map (Map.add h new_nested m)
        | _,_ -> failwith "invalid nested update add"
      in ignore(DBM.add [mapn] (aux imgs patterns (DBM.find [mapn] db)) db) 

   let update_io_map mapn inv_img outv_img new_value (db,pats) =
      let (in_pats,out_pats) = get_patterns pats (map_name_to_string mapn) in
      update_nested_map
         mapn [inv_img; outv_img] [in_pats;out_pats] new_value (db,pats)

   let has_value mapn (db,_) = DBM.mem [mapn] db

   let get_value mapn (db,_) =
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_value "^(N.to_string mapn))

end

module type NamedK3DB = K3DB
    with type map_name_t   = NamedMap.t
    and  type value_t      = K3Spec.value_t
(*  and  type map_t        = K3Spec.map_t *)

module type RefK3DB = K3DB
    with type map_name_t   = RefMap.t
    and  type value_t      = K3Spec.value_t
(*  and  type map_t        = K3Spec.map_t *)

module NamedK3Database = NameableK3Database(NamedMap)(K3Spec)
module K3Database = NameableK3Database(RefMap)(K3Spec)