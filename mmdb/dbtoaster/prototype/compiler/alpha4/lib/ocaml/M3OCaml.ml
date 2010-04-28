open M3
open Util

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
   val bind : t -> (var_t * var_t) list -> t
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

   let to_string (theta:t) : string =
      StringMap.fold (fun k v acc -> acc^(if acc = "" then "" else " ")^
         k^"->"^(M3Common.string_of_const v)) theta ""

   (* Note: ordered result *)
   let to_list m = StringMap.fold (fun s n l -> (s,n)::l) m []

   let vars theta = StringMap.fold (fun k _ acc -> k::acc) theta []

   let bound var theta = StringMap.mem var theta
   
   let value var theta = StringMap.find var theta

   let consistent (m1: t) (m2: t) : bool =
      List.for_all (fun (k,v) ->
        (not(StringMap.mem k m2)) || ((StringMap.find k m2) = v))
        (to_list m1)

   (* extends m with the given bindings, which rename existing valuations *)
   let bind (m: t) (bindings: (var_t * var_t) list) =
      List.fold_left (fun acc (decl,def) ->
         StringMap.add decl (value def acc) acc) m bindings

   (* extends m1 by given vars from m2.
    * assumes that m1 and m2 are consistent. *)
   (* Note: ordered result *)
   let extend (m1: t) (m2: t) (ext : var_t list) : t =
      List.fold_left (fun acc k -> StringMap.add k (value k m2) acc) m1 ext

   let apply (m: t) (l: key list) = List.map (fun x ->
      try StringMap.find x m
      with Not_found -> failwith ("No valuation for "^x^" in "^(to_string m))) l

end

(* Map whose keys are valuations, and values are polymorphic *)
module ValuationMap = SliceableMap.Make(struct
   type t = const_t
   let to_string = M3Common.string_of_const
end)

(* ValuationMap whose values are aggregates *)
module AggregateMap =
struct
   module VM = ValuationMap

   type agg_t = const_t
   
   type key = VM.key
   type t = agg_t VM.t

   let string_of_aggregate = M3Common.string_of_const

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
   open M3Common.Patterns

   module VM  = ValuationMap
   module AM  = AggregateMap
   module DBM = SliceableMap.Make(struct type t = string let to_string x = x end)
   
   type slice_t = AM.t
   type dbmap_t = AM.t VM.t
   type db_t    = dbmap_t DBM.t

   let concat_string s t delim =
      (if (String.length s = 0) then "" else delim)^t

   let key_to_string k = Util.list_to_string M3Common.string_of_const k

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

   let update mapn inv_imgs slice db : unit =
      try
         let m = DBM.find [mapn] db in
         ignore(DBM.add [mapn] (VM.add inv_imgs slice m) db);
(*
         let string_of_img = Util.list_to_string AM.string_of_aggregate in
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
            let string_of_img = Util.list_to_string AM.string_of_aggregate in
            print_endline ("Updating a db value: "^mapn^
                           " inv="^(string_of_img inv_img)^
                           " outv="^(string_of_img outv_img)^
                           " v="^(AM.string_of_aggregate new_value)^
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

(* Adaptors
 * -- adaptors are associated with a single stream/relation, and this
 *    should explicitly be tracked in sources.
 * -- adaptors return events, that is whether the event is an insertion
 *    or deletion, and a list of constants making up a tuple. They do
 *    not specify which stream/relation the event relates to, this is
 *    added by sources given the associations they maintain between
 *    adaptors+relations *)
module Adaptors =
struct
   type adaptor = string -> event list
   type adaptor_generator = (string * string) list -> adaptor
   type adaptor_registry = (string, adaptor_generator) Hashtbl.t
   
   let adaptors = Hashtbl.create 10

   let get_source_type a = fst a
   let get_adaptor a = snd a

   let create_adaptor (name, params) : adaptor =
      if Hashtbl.mem adaptors name then ((Hashtbl.find adaptors name) params)
      else failwith ("No adaptor named "^name^" found")

   let add name (generator : adaptor_generator) =
      Hashtbl.add adaptors name generator
   
   let to_string reg = Hashtbl.fold (fun k v acc -> acc^" "^k) adaptors ""
end

(* Source sigs *)
module type SyncSource = 
sig
   type t

   val create: source_t -> framing_t -> (rel_id_t * Adaptors.adaptor) list ->
               string -> t 
   val has_next: t -> bool
   val next: t -> t * (stream_event option)
end

module type SyncMultiplexer = functor (S : SyncSource) ->
sig
   type t = (S.t ref) list
   val create : unit -> t
   val add_stream: t -> S.t -> t
   val remove_stream: t -> S.t -> t
   val has_next: t -> bool
   val next: t -> t * (stream_event option)
end

module type AsyncSource =
sig
   type t
   type handler = stream_event -> unit

   val create: source_t -> framing_t -> (rel_id_t * Adaptors.adaptor) list -> t
   val set_handler: t -> handler -> t
   val process: t -> unit
   val process_one: t -> unit
   val ready: t -> bool
end

module type AsyncMultiplexer = functor (AS : AsyncSource) ->
sig
   type t = AS.t list
   val add_stream: t -> AS.t -> t
   val remove_stream: t -> AS.t -> t
   val process: t -> unit
   val process_one: t -> unit
   val ready: t -> bool
end

(* Source implementations
 * -- sources maintain associations of adaptors to relations, generating
 *    stream_events from adaptor events (i.e., adaptors themselves cannot
 *    specify to which stream an event should be inserted/deleted) *)
module FileSource : SyncSource =
struct
   type fs_t = framing_t * in_channel
   type t = fs_t option * (string * Adaptors.adaptor) list * (stream_event list ref) * string

   let create source_t framing rels_adaptors name =
      match source_t with
       | FileSource(filename) ->
         let ns = Some(framing, open_in filename)
         in (ns, rels_adaptors, ref [], name)
       | PipeSource(pipe_cmd) ->
         let ns = Some(framing, Unix.open_process_in pipe_cmd)
         in (ns, rels_adaptors, ref [], name)
       | _ -> failwith "invalid file source" 

   (* TODO: variable sized frames
    * returns (None,"") at end of file *)
   let get_input ns = match ns with
      | None -> failwith "invalid access on finished source"
      | Some(fr, inc) ->
         begin try match fr with
             | FixedSize(len) ->
                let buf = String.create len in
                really_input inc buf 0 len; (Some(fr,inc), buf)
             | Delimited(s) ->
                (* TODO: handle \n\r *)
                if s = "\n" then (Some(fr, inc), input_line inc) else
                let delim_len = String.length s in
                let tok = String.create delim_len in
                let buf = ref (String.create 1024) in
                let pos = ref 0 in
                   while (really_input inc tok 0 delim_len; tok <> s) do
                      if ( (!pos) + delim_len >= (String.length (!buf)) ) then
                         buf := (!buf)^(String.create 1024);
                      for i = 0 to delim_len do (!buf).[(!pos)+i] <- tok.[i] done;
                      pos := (!pos) + delim_len;
                   done;
                   (Some(fr,inc), !buf)
             | VarSize(off_to_size, off_to_end) ->
                failwith "VarSize frames not yet implemented."
          with End_of_file -> (close_in inc; (None, ""))
         end 

   let has_next fs = let (ns,_,buf,_) = fs in not((ns = None) && (!buf = []))

   (* returns (_, None) at end of file *)
   let next fs : t * (stream_event option) =
      let (ns,ra,buf,name) = fs in
         if !buf <> [] then
            let e = List.hd (!buf)
            in buf := List.tl (!buf); ((ns,ra,buf,name), Some(e))
         else
         begin
            let (new_ns, tuple) = get_input ns in
            if tuple <> "" then
               let aux r (pm,cl) = (pm,r,cl) in
               let events = List.flatten (List.map
                  (fun (r,f) -> List.map (aux r) (f tuple)) ra) in
               if List.length events > 0 then
                 ( buf := List.tl events;
                   ((new_ns,ra,buf,name), Some(List.hd events)) )
               else
                 ((new_ns,ra,buf,name), None)
            else ((new_ns,ra,buf,name), None)
         end
end

module SM : SyncMultiplexer = functor (S : SyncSource) ->
struct
   type t = (S.t ref) list
   
   let create () = []

   let add_stream fm s = fm@[(ref s)]
   
   let remove_stream fm s = List.filter (fun x -> !x <> s) fm
   
   let has_next fm = List.exists (fun x -> S.has_next !x) fm
   
   (* returns (_, None) at end of file *)
   let next fm : t * (stream_event option) =
      let nfm = ref fm in
      let i = ref (Random.int (List.length !nfm)) in
      let event = ref None in
      while ((List.length (!nfm)) > 0) && (!event = None) do
         let s = List.nth !nfm !i in
         let next = (S.next !s) in
         event := snd next;
         s := fst next;
         nfm := List.filter (fun x -> S.has_next !x) (!nfm);
         if (List.length (!nfm) > 0) then i := Random.int(List.length (!nfm));
      done;
      (!nfm, !event)
end

module FileMultiplexer = SM(FileSource)

let string_of_evt (action:M3.pm_t) 
                  (relation:string) 
                  (tuple:M3.const_t list): string =
  (match action with Insert -> "Inserting" | Delete -> "Deleting")^
  " "^relation^(Util.list_to_string M3Common.string_of_const tuple);;

let main_args () =
  (ParseArgs.parse (ParseArgs.compile
    [
      (["-v"], ("VERBOSE",ParseArgs.NO_ARG),"","Show all updates");
      (["-r"], ("RESULT",ParseArgs.ARG),"<db|map|value>","Set result type");
      (["-o"], ("OUTPUT",ParseArgs.ARG),"<output file>", "Set output file")
    ]));;

let synch_main 
      (db:Database.db_t)
      (initial_mux:FileMultiplexer.t)
      (toplevel_queries:string list)
      (dispatcher:((M3.pm_t*M3.rel_id_t*M3.const_t list) option) -> bool)
      (arguments:ParseArgs.arguments_t)
      (): unit = 
  let log_evt = 
    if ParseArgs.flag_bool arguments "VERBOSE" then
      (fun evt -> match evt with None -> () | Some(pm,rel,t) ->
        print_endline (string_of_evt pm rel t))
    else (fun evt -> ()) 
  in
  let result_chan = match (ParseArgs.flag_val arguments "OUTPUT") with
      | None -> stdout
      | Some(x) -> try open_out x with Sys_error _ -> 
    print_endline ("Failed to open output file: "^x); stdout
  in
  let log_results = match (ParseArgs.flag_val arguments "RESULT") with
    | None -> (fun chan -> ())
    | Some(x) -> 
      let output_endline c s = output_string c (s^"\n") in
      begin match (String.lowercase x) with
        | "db" -> (fun chan -> output_endline chan 
            ("db: "^(Database.db_to_string db)))
        | "map" -> (fun chan -> output_endline chan
            (String.concat "\n" 
              (List.map (fun q -> q^": "^
                (Database.dbmap_to_string (Database.get_map q db))) 
              toplevel_queries)))
        | "value" -> (fun chan -> output_endline chan
            (String.concat "\n"
              (List.map (fun q -> 
                let q_map = (Database.get_map q db) in
                let r = 
                  if ValuationMap.mem [] q_map then 
                    (let ot = ValuationMap.find [] q_map in
                      if ValuationMap.mem [] ot then ValuationMap.find [] ot 
                      else CFloat(0.0))
                  else CFloat(0.0)
                in AggregateMap.string_of_aggregate r)
              toplevel_queries)))
        | _ -> (fun chan -> ())
      end
  in
  let mux = ref initial_mux in
  let start = Unix.gettimeofday() in
    while FileMultiplexer.has_next !mux do
      let (new_mux,evt) = FileMultiplexer.next !mux in
        (log_evt evt;
        let output = dispatcher evt in
        if output then log_results result_chan)
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Typles: "^(string_of_float (finish -. start)));
  print_endline (string_of_list0 "\n" 
          (fun q -> q^": "^(Database.dbmap_to_string (Database.get_map q db))) 
          toplevel_queries)
;;

(* TODO: RandomSource *)
(* TODO: multiplexer of random sources *)

(* TODO: unit tests for file sources + multiplexer *)
