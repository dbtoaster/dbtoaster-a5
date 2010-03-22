open M3Common


(* (PARTIAL) VARIABLE VALUATIONS *)
module type ValuationSig =
sig
   type key = string
   type t
   
   val make : var_t list -> const_t list -> t
   val vars : t -> string list
   val bound : string -> t -> bool
   val value : string -> t -> const_t
   
   val consistent : t -> t -> bool
   val extend : t -> t -> var_t list -> t
   val apply : t -> key list -> const_t list
   val to_string : t -> string
end

module Valuation : ValuationSig =
struct
   (* the keys are variable names *)
   module StringMap = Map.Make (String)

   type key = StringMap.key
   type t = const_t StringMap.t

   (* Note: ordered result *)
   let make (vars: var_t list) (values: const_t list) : t =
      List.fold_left (fun acc (k,v) -> StringMap.add k v acc)
         StringMap.empty (List.combine vars values)

   (* Note: ordered result *)
   let to_list m = StringMap.fold (fun s n l -> (s,n)::l) m []

   let vars theta = StringMap.fold (fun k _ acc -> k::acc) theta []

   let bound var theta = StringMap.mem var theta
   
   let value var theta = StringMap.find var theta

   let consistent (m1: t) (m2: t) : bool =
      List.for_all (fun (k,v) ->
        (not(StringMap.mem k m2)) || ((StringMap.find k m2) = v))
        (to_list m1)

   (* extends m1 by given vars from m2.
    * assumes that m1 and m2 are consistent. *)
   (* Note: ordered result *)
   let extend (m1: t) (m2: t) (ext : var_t list) : t =
      List.fold_left (fun acc k -> StringMap.add k (value k m2) acc) m1 ext

   let apply (m: t) (l: key list) = List.map (fun x -> StringMap.find x m) l

   let to_string (theta:t) : string =
      StringMap.fold (fun k v acc -> acc^(if acc = "" then "" else " ")^
         k^"->"^(string_of_const v)) theta ""
end

(* Map whose keys are valuations, and values are polymorphic *)
module ValuationMap = SliceableMap.Make(struct
   type t = const_t
   let to_string = string_of_const
end)

(* ValuationMap whose values are aggregates *)
module AggregateMap =
struct
   module VM = ValuationMap

   type agg_t = const_t
   
   type key = VM.key
   type t = agg_t VM.t

   let string_of_aggregate = string_of_const

   (* Slice methods for calculus evaluation *)

   (* Indexed aggregate, iterates over all partial keys given by pattern,
    * and aggregate over key-value pairs corresponding to each partial key.
    * assumes add has replace semantics, so that fold & add removes duplicates.
    * returns a new map, does not modify in-place. *)
   let indexed_aggregate aggf pat m =
      let aux pk kv nm =
         let (nk,nv) = aggf pk kv in VM.add nk nv nm
      in VM.fold_index aux pat m (VM.empty_map())
      
   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let concat_keys (current_outv: var_t list) (desired_outv: var_t list)
         (theta: Valuation.t) (extensions: var_t list) (m: t) =
      let key_refiner k v =
         let ext_theta = Valuation.extend
            (Valuation.make current_outv k) theta extensions in
         (* normalizes the key orderings to that of the variable ordering outv. *)
         let new_k = Valuation.apply ext_theta desired_outv in
         (new_k, v)
   in VM.mapi key_refiner m

   (* assumes valuation theta and (Valuation.make current_outv k)
      are consistent *)
   let project_keys (pat: ValuationMap.pattern) (pat_outv: var_t list)
        (desired_outv: var_t list) (theta: Valuation.t)
        (extensions: var_t list) (m: t) =
      let aux pk kv =
         let aggv = List.fold_left (fun x (y,z) -> c_sum x z) (CFloat(0.0)) kv in
         let ext_theta =
            Valuation.extend (Valuation.make pat_outv pk) theta extensions in
         (* Normalizes the key orderings to that of the variable ordering desired_outv.
          * This only performs reordering, that is we assume pat_outv has the
          * same set of variables as desired_outv. *)
         let new_k = Valuation.apply ext_theta desired_outv in
            (new_k, aggv)
      in
         (* in the presence of bigsum variables, there may be duplicates
          * after key_refinement. These have to be summed up using aggregate. *)
         indexed_aggregate aux pat m

   (* filters slice entries to those consistent in terms of given variables, and theta *) 
   let filter (outv: var_t list) (theta: Valuation.t) (m: t) : t =
      let aux k v = (Valuation.consistent theta (Valuation.make outv k)) in
         VM.filteri aux m

end

module Database =
struct
   open Patterns

   module VM  = ValuationMap
   module AM  = AggregateMap
   module DBM = SliceableMap.Make(struct type t = string let to_string x = x end)
   
   type slice_t = AM.t
   type dbmap_t = AM.t VM.t
   type db_t    = dbmap_t DBM.t

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string string_of_const k

   let slice_to_string (m: slice_t) =
      VM.to_string key_to_string AM.string_of_aggregate m

   let dbmap_to_string (m: dbmap_t) =
      VM.to_string key_to_string slice_to_string m

   let db_to_string (db: db_t) : string =
      DBM.to_string (fun x -> String.concat "," x) dbmap_to_string db

   let get_patterns patterns mapn =
      (get_in_patterns patterns mapn, get_out_patterns patterns mapn)

   let make_empty_db schema patterns: db_t =
        let f (mapn, itypes, _) =
           let (in_patterns, out_patterns) = get_patterns patterns mapn in
            ([mapn], (if(List.length itypes = 0) then
                VM.from_list [([], VM.from_list [] out_patterns)] in_patterns
                else
                   (* We defer adding out var indexes to init value computation *)
                   VM.empty_map_w_patterns in_patterns))
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

   let update mapn inv_imgs slice db : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (VM.add inv_imgs slice m) db);
(*
         let string_of_img = Util.list_to_string string_of_const in
         print_endline ("Updated the database: "^mapn^
                      " inv="^(string_of_img inv_imgs)^
                      " slice="^(slice_to_string slice)^
                      " db="^(db_to_string db));
         VM.validate_indexes (VM.find inv_imgs (DBM.find [mapn] db))
*)
      with Failure x -> failwith ("update: "^x)
              
   let update_value mapn patterns inv_img outv_img new_value db : unit =
      try
         let m = DBM.find [mapn] db in
         let new_slice = if VM.mem inv_img m
            then VM.add outv_img new_value (VM.find inv_img m) 
            else VM.from_list [(outv_img, new_value)] patterns
         in
            ignore(DBM.add [mapn] (VM.add inv_img new_slice m) db);
(*
            let string_of_img = Util.list_to_string string_of_const in
            print_endline ("Updating a db value: "^mapn^
                           " inv="^(string_of_img inv_img)^
                           " outv="^(string_of_img outv_img)^
                           " v="^(string_of_const new_value)^
                           " db="^(db_to_string db));
            VM.validate_indexes (VM.find inv_img (DBM.find [mapn] db))
*)
       with Failure x -> failwith ("update_value: "^x)
            
   let get_map mapn db = 
       try DBM.find [mapn] db
       with Not_found -> failwith ("Database.get_map "^mapn)

   let showmap (m: dbmap_t) = VM.to_list (VM.map VM.to_list m)
   
   let show_sorted_map (m: dbmap_t) =
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

(* Data sources *)
module Adaptors =
struct
   type adaptor = framing_t * (string -> event list)
   type adaptor_generator = (string * string) list -> adaptor
   type adaptor_registry = (string, adaptor_generator) Hashtbl.t
   
   let adaptors = Hashtbl.create 10

   let get_source_type a = fst a
   let get_adaptor a = snd a

   let create_adaptor ((name, params) : string * ((string * string) list)) : adaptor =
      if Hashtbl.mem adaptors name then ((Hashtbl.find adaptors name) params)
      else failwith ("No adaptor named "^name^" found")

   let add name (generator : adaptor_generator) =
      Hashtbl.add adaptors name generator
   
   let to_string reg = Hashtbl.fold (fun k v acc -> acc^" "^k) adaptors ""
end

(*
module type SyncSource = 
sig
   type t

   val create: Adaptor.adaptor list -> string -> t 
   val has_next: t -> bool
   val next: t -> event
end

module type SyncMultiplexer = functor (S : SyncSource) ->
sig
   type t = S.t list
   val create : unit -> t
   val add_stream: t -> S.t -> t
   val remove_stream: t -> S.t -> t
   val has_next: t -> bool
   val next: t -> event
end

module FileSource : Sync =
struct
   type fs_t = Binary of in_channel | Text of in_channel | Lines of in_channel
   type t = fs_t * int * Adaptor.adaptor list * (event list ref)

   let escalate_fs_t cur a =
      match (cur_t, get_source_type a) with
       | ("b", _) -> "b"
       | ("t", Binary) -> "b"
       | ("t", _) -> "t"
       | ("l", Binary) -> "b"
       | ("l", Text) -> "t"
       | _ -> "l"
       
   let create adaptors filename =
      let ic = open_in filename in
      let fs =
         let base = List.fold_left escalate_fs_t "l" adaptors
         in match base with
          | "b" -> Binary(open_in_bin filename)
          | "t" -> Text(open_in filename)
          | "l" -> Lines(open_in filename)
      in (ns, in_channel_length (get_channel ns), adaptors, ref []) 

   let get_input = match fs_t with
      | Binary(ic) | Text(ic) -> input ic buf pos len
      | Lines(ic) -> input_line ic

   let has_next fs =
      let (ns,len,_,buf) = fs in not(!buf = [] && ((pos_in (get_channel ns)) = len))

   let next fs =
      let (ns,len,a,buf) = fs in
         if !buf = [] then
            let e = List.hd !buf in buf := List.tl !buf; ((ns,len,a,buf), e)
         else
         let events = ref [] in
         let tuple = ref "" in
         while events = [] do
            tuple := tuple^(get_input ns);
            events := List.flatten (List.map (fun x -> x !tuple) a)
         done;
         buf := List.tl !events;
         ((ns,len,a,buf), List.hd !events)
end

module SM : SyncMultiplexer = functor (S : Sync) ->
struct
   type t = S.t list
   
   let create () = []

   let add_stream fm s = fm@[s]
   
   let remove_stream fm s = List.filter (fun x -> x <> s) fm
   
   let has_next fm = List.exists S.has_next fm
   
   let next fm =
      let nfm = ref fm in
      let i = ref (Random.int (List.length !nfm)) in
      let event = ref None in
      while !event = None do
         let s = List.nth !nfm !i in
            if S.has_next s then event := Some(S.next s);
            nfm := List.filter (fun x -> S.has_next x) !nfm;
            if !event = None then i := Random.int(List.length !nfm);
      done;
      match !event with Some(ev) -> (!nfm, ev) | _ -> failwith "No event found."
end

module FileMultiplexer = SM(FileSource)
*)