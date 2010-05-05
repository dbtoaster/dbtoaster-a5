open M3
open Util
open Expression

module type ToastedDB =
sig
   type value_t      = const_t
   type key_t        = const_t list
   
   type db_key_t
   
   type single_map_t
   type map_t
   type db_t
   
   type list_smap      = (key_t * const_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (db_key_t * list_map) list
   
   type dom_t      = const_t list
   type schema_t   = map_type_t list
   type pattern_t  = int list
   type patterns_t = M3Common.Patterns.pattern_map
   
   val smap_to_string : single_map_t -> string
   val map_to_string : map_t -> string
   val db_to_string : db_t -> string

   val make_empty_db : schema_t -> patterns_t -> db_t
   (*val make_empty_db_wdom : schema_t -> patterns_t -> dom_t -> db_t*)

   val update_map : string -> key_t -> single_map_t -> db_t -> unit
   val update_map_value :
      string -> pattern_t list -> key_t -> key_t -> value_t -> db_t -> unit

   val update_in_map : string -> single_map_t -> db_t -> unit
   val update_in_map_value : string -> key_t -> value_t -> db_t -> unit
   
   val update_out_map : string -> single_map_t -> db_t -> unit
   
   (* TODO: remove patterns from this interface if we remove SimpleDatabase.
    * It is only used for initial output tier construction. *)
   val update_out_map_value :
      string -> pattern_t list -> key_t -> value_t -> db_t -> unit
   
   val update_value: string -> value_t -> db_t -> unit

   val has_map     : string -> db_t -> bool
   val has_smap    : string -> db_t -> bool
   val has_value   : string -> db_t -> bool

   val get_map     : string -> db_t -> map_t
   val get_in_map  : string -> db_t -> single_map_t 
   val get_out_map : string -> db_t -> single_map_t
   val get_value   : string -> db_t -> AggregateMap.agg_t option

   val showsmap         : single_map_t -> list_smap
   val show_sorted_smap : single_map_t -> list_smap
   val showmap          : map_t -> list_map
   val show_sorted_map  : map_t -> list_map
   val showdb           : db_t -> named_list_map
   val show_sorted_db   : db_t -> named_list_map
end

module SimpleDatabase : ToastedDB
with type single_map_t = AggregateMap.t and
     type map_t = AggregateMap.t ValuationMap.t and
     type db_key_t = string list
=
struct
   open M3Common.Patterns

   module VM  = ValuationMap
   module AM  = AggregateMap
   module Nameable = struct
      type t = string
      let to_string x = x
      let compare = Pervasives.compare
   end
   module DBM = SliceableMap.Make(Nameable)
   
   type value_t = const_t
   type key_t   = const_t list
   
   type db_key_t = string list
      
   type single_map_t = AM.t
   type map_t        = AM.t VM.t
   type db_t         = map_t DBM.t

   type list_smap      = (key_t * const_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (string list * list_map) list
   
   type dom_t      = const_t list
   type schema_t   = map_type_t list
   type pattern_t  = int list
   type patterns_t = M3Common.Patterns.pattern_map

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string M3Common.string_of_const k

   let smap_to_string (m: single_map_t) =
      VM.to_string key_to_string AM.string_of_aggregate m

   let map_to_string (m: map_t) =
      VM.to_string key_to_string smap_to_string m

   let db_to_string (db: db_t) : string =
      DBM.to_string (fun x -> String.concat "," x) map_to_string db

   let get_patterns patterns mapn =
      (get_in_patterns patterns mapn, get_out_patterns patterns mapn)

   let make_empty_db schema patterns: db_t =
        let f (mapn, itypes, _) =
           let (in_patterns, out_patterns) = get_patterns patterns mapn in
           let init_slice =
              if(List.length itypes = 0) then
                 VM.from_list [([], VM.from_list [] out_patterns)] in_patterns
              else
                 (* We defer adding out var indexes to init value computation *)
                 VM.empty_map_w_patterns in_patterns 
           in ([mapn], init_slice)
        in
            DBM.from_list (List.map f schema) []
    
    
   let make_empty_db_wdom schema patterns (dom: const_t list): db_t =
        let f (mapn, itypes, rtypes) =
            let (in_patterns, out_patterns) = get_patterns patterns mapn in
            let map = VM.from_list
                        (List.map (fun t -> (t, VM.empty_map_w_patterns out_patterns))
                            (Util.k_tuples (List.length itypes) dom)) in_patterns
            in
            ([mapn], map)
        in
            DBM.from_list (List.map f schema) []

   let update_map mapn inv_imgs slice db : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (VM.add inv_imgs slice m) db);
(*
         let string_of_img = Util.list_to_string AM.string_of_aggregate in
         print_endline ("Updated the database: "^mapn^
                      " inv="^(string_of_img inv_imgs)^
                      " slice="^(smap_to_string slice)^
                      " db="^(db_to_string db));
         VM.validate_indexes (VM.find inv_imgs (DBM.find [mapn] db))
*)
      with Failure x -> failwith ("update_map: "^x)
              
   let update_map_value mapn patterns inv_img outv_img new_value db : unit =
      try
         let m = DBM.find [mapn] db in
         let new_slice = if VM.mem inv_img m
            then VM.add outv_img new_value (VM.find inv_img m) 
            else VM.from_list [(outv_img, new_value)] patterns
         in ignore(DBM.add [mapn] (VM.add inv_img new_slice m) db);
(*
            let string_of_img = Util.list_to_string AM.string_of_aggregate in
            print_endline ("Updating a db value: "^mapn^
                           " inv="^(string_of_img inv_img)^
                           " outv="^(string_of_img outv_img)^
                           " v="^(AM.string_of_aggregate new_value)^
                           " db="^(db_to_string db));
            VM.validate_indexes (VM.find inv_img (DBM.find [mapn] db))
*)
       with Failure x -> failwith ("update_map_value: "^x)

   let update_in_map mapn slice db : unit = failwith "Not needed."

   let update_in_map_value mapn img new_value db : unit =
      try update_map_value mapn [] img [] new_value db
      with Failure x -> failwith ("update_in_map_value: "^x)
   
   let update_out_map mapn slice db : unit =
      try update_map mapn [] slice db
      with Failure x -> failwith ("update_out_map: "^x)

   let update_out_map_value mapn patterns img new_value db : unit =
      try update_map_value mapn patterns [] img new_value db
      with Failure x -> failwith ("update_out_map_value: "^x)
   
   let update_value mapn new_value db : unit =
      try update_map_value mapn [] [] [] new_value db
      with Failure x -> failwith ("update_value: "^x)


   let has_map mapn db = DBM.mem [mapn] db
   let has_smap = has_map
   let has_value = has_map

   let get_map mapn db = 
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_map "^mapn)
       
   let get_in_map mapn db = failwith ("Database.get_in_map "^mapn)
   
   let get_out_map mapn db =
      let m = get_map mapn db in
      if ValuationMap.mem [] m then ValuationMap.find [] m
      else failwith ("Database.get_out_map "^mapn)
      
   let get_value mapn db =
      let m = get_map mapn db in
      let slice =
         if ValuationMap.mem [] m then ValuationMap.find [] m
         else failwith ("Database.get_out_map "^mapn)
      in if ValuationMap.mem [] slice then Some(ValuationMap.find [] slice) else None

   let showsmap (s: single_map_t) = VM.to_list s
   
   let show_sorted_smap (s: single_map_t) =
      List.sort (fun (k1,_) (k2,_) -> compare k1 k2) (showsmap s)

   let showmap (m: map_t) = VM.to_list (VM.map VM.to_list m)
   
   let show_sorted_map (m: map_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))

    let showdb_f show_f db =
       DBM.fold (fun k v acc -> acc@[(k, (show_f v))]) [] db
    
    let showdb db = showdb_f showmap db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map db)
end


module Database : ToastedDB
with type single_map_t = AggregateMap.t and
     type map_t = AggregateMap.t ValuationMap.t and
     type db_key_t = string list
=
struct
   open M3Common.Patterns

   module VM  = ValuationMap
   module AM  = AggregateMap
   module Nameable = struct
      type t = string
      let to_string x = x
      let compare = Pervasives.compare
   end
   module DBM = SliceableMap.Make(Nameable)
   
   type value_t = const_t
   type key_t   = const_t list
   
   type db_key_t = string list
      
   type single_map_t = AM.t
   
   type mv_t         = AM.agg_t ref
   type map_t        = AM.t VM.t
   type db_t         = (map_t DBM.t) * (single_map_t DBM.t) * (mv_t DBM.t)  

   type list_smap      = (key_t * const_t) list
   type list_map       = (key_t * list_smap) list
   type named_list_map = (string list * list_map) list
   
   type dom_t      = const_t list
   type schema_t   = map_type_t list
   type pattern_t  = int list
   type patterns_t = M3Common.Patterns.pattern_map

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string M3Common.string_of_const k

   let smap_to_string (m: single_map_t) =
      VM.to_string key_to_string AM.string_of_aggregate m

   let map_to_string (m: map_t) =
      VM.to_string key_to_string smap_to_string m

   let db_to_string ((mio,ms,mv): db_t) : string =
      let dbkey_to_s = String.concat "," in
      let aggref_to_s ar = (AM.string_of_aggregate !ar) in 
      (DBM.to_string dbkey_to_s map_to_string mio)^
      (DBM.to_string dbkey_to_s smap_to_string ms)^
      (DBM.to_string dbkey_to_s aggref_to_s mv)

   let get_patterns patterns mapn =
      (get_in_patterns patterns mapn, get_out_patterns patterns mapn)

   let make_empty_db schema patterns: db_t =
        let io_f (mapn, itypes, _) =
           let (in_patterns, _) = get_patterns patterns mapn in
           (* We defer adding out var indexes to init value computation *)
           let init_slice = VM.empty_map_w_patterns in_patterns 
           in ([mapn], init_slice)
        in
        let s_f (mapn, itypes, otypes) =
           let (in_pat, out_pat) = get_patterns patterns mapn in
           let init_slice =
              VM.empty_map_w_patterns (if itypes = [] then out_pat else in_pat)
           in ([mapn], init_slice)
        in 
        let val_f (mapn, _, _) = ([mapn], ref(CFloat(0.0))) in
        let io_maps = List.filter (fun (_,i,o) -> i <> [] && o <> []) schema in
        let in_maps = List.filter (fun (_,i,o) -> i = [] && o <> []) schema in 
        let out_maps = List.filter (fun (_,i,o) -> i <> [] && o = []) schema in
        let values = List.filter (fun (_,i,o) -> i = [] && o = []) schema in
            (DBM.from_list (List.map io_f io_maps) [],
             DBM.from_list (List.map s_f (in_maps@out_maps)) [],
             DBM.from_list (List.map val_f values) [])

   let update_map mapn inv_imgs slice (db,_,_) : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (VM.add inv_imgs slice m) db);
(*
         let string_of_img = Util.list_to_string AM.string_of_aggregate in
         print_endline ("Updated the database: "^mapn^
                      " inv="^(string_of_img inv_imgs)^
                      " slice="^(smap_to_string slice)^
                      " db="^(db_to_string db));
         VM.validate_indexes (VM.find inv_imgs (DBM.find [mapn] db))
*)
      with Failure x -> failwith ("update: "^x)
              
   let update_map_value mapn patterns inv_img outv_img new_value (db,_,_) : unit =
      try
         let m = DBM.find [mapn] db in
         let new_slice = if VM.mem inv_img m
            then VM.add outv_img new_value (VM.find inv_img m) 
            else VM.from_list [(outv_img, new_value)] patterns
         in ignore(DBM.add [mapn] (VM.add inv_img new_slice m) db);
(*
            let string_of_img = Util.list_to_string AM.string_of_aggregate in
            print_endline ("Updating a db value: "^mapn^
                           " inv="^(string_of_img inv_img)^
                           " outv="^(string_of_img outv_img)^
                           " v="^(AM.string_of_aggregate new_value)^
                           " db="^(db_to_string db));
            VM.validate_indexes (VM.find inv_img (DBM.find [mapn] db))
*)
       with Failure x -> failwith ("update_value: "^x)

   let update_smap mapn slice (_,db,_) : unit =
      ignore(DBM.add [mapn] slice db)
   
   let update_smap_value mapn img new_value (_,db,_) : unit =
      let s = DBM.find [mapn] db in
      ignore(DBM.add [mapn] (VM.add img new_value s) db)

   let update_in_map mapn slice db : unit =
      try update_smap mapn slice db
      with Failure x -> failwith ("update_in_map: "^x)

   let update_in_map_value mapn img new_value db : unit =
      try update_smap_value mapn img new_value db
      with Failure x -> failwith ("update_in_map_value: "^x)
   
   let update_out_map mapn slice db : unit =
      try update_smap mapn slice db
      with Failure x -> failwith ("update_out_map: "^x)

   let update_out_map_value mapn patterns img new_value db : unit =
      try update_smap_value mapn img new_value db
      with Failure x -> failwith ("update_out_map_value: "^x)
   
   let update_value mapn new_value (_,_,db) : unit =
      try let m = DBM.find [mapn] db in m := new_value
      with Failure x -> failwith ("update_value: "^x)

   let has_map mapn (db,_,_) = DBM.mem [mapn] db

   let has_smap mapn (_,db,_) = DBM.mem [mapn] db

   let has_value mapn (db,_,_) = DBM.mem [mapn] db

   let get_map mapn (db,_,_) = 
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_map "^mapn)
       
   let get_in_map mapn (_,db,_) = 
      try DBM.find [mapn] db
      with Not_found -> failwith ("Database.get_in_map "^mapn) 
   
   let get_out_map mapn (_,db,_) =
      try DBM.find [mapn] db
      with Not_found -> failwith ("Database.get_out_map "^mapn)
      
   let get_value mapn (_,_,db) =
      try Some(!(DBM.find [mapn] db))
      with Not_found -> failwith ("Database.get_value "^mapn)

   let showval (v : AggregateMap.agg_t ref) = [([], !v)]

   let showsmap (s : single_map_t) = VM.to_list s
   
   let show_sorted_smap (s : single_map_t) =
      List.sort (fun (k1,_) (k2,_) -> compare k1 k2) (VM.to_list s)

   let showmap (m: map_t) = VM.to_list (VM.map VM.to_list m)   
   
   let show_sorted_map (m: map_t) =
      let sort_slice =
        List.sort (fun (outk1,_) (outk2,_) -> compare outk1 outk2)
      in 
      List.sort (fun (ink1,_) (ink2,_) -> compare ink1 ink2)
         (List.map (fun (ink,sl) -> (ink, sort_slice sl)) (showmap m))

    let showdb_f show_mio_f show_ms_f show_mv_f (mio,ms,mv) =
       (DBM.fold (fun k v acc -> acc@[(k, show_mio_f v)]) [] mio)@
       (DBM.fold (fun k v acc -> acc@[(k, [([], show_ms_f v)])]) [] ms)@
       (DBM.fold (fun k v acc -> acc@[(k, [([], show_mv_f v)])]) [] mv)
    
    let showdb db = showdb_f showmap showsmap showval db

    let show_sorted_db db =
       List.sort (fun (n1,_) (n2,_) -> compare n1 n2)
          (showdb_f show_sorted_map show_sorted_smap showval db)
end