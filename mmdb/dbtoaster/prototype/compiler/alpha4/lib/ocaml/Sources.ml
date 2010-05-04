open M3
open Util
open Expression
open Database

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
  let db_access_f = List.map (fun q ->
     if Database.has_map q db then
        (q,"map", (fun () -> Database.map_to_string (Database.get_map q db)))
     (* TODO: no distinction between in/out maps... fix in db if really needed *)
     else if Database.has_smap q db then
        (q,"map", (fun () ->
           q^": "^(Database.smap_to_string (Database.get_in_map q db))))
     else (q,"value", (fun () ->
        AggregateMap.string_of_aggregate
           (match Database.get_value q db with
             | Some(x) -> x | _ -> (CFloat(0.0)))))
     ) toplevel_queries
  in
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
      let y = String.lowercase x in
      if y = "db" then
         (fun chan -> output_endline chan ("db: "^(Database.db_to_string db)))
      else
         let valid_f = List.filter (fun (_,z,_) -> y = z) db_access_f in
         (fun chan -> output_endline chan
         (String.concat "\n"
            (List.map (fun (q,_,f) -> f()) valid_f)))
  in
  let mux = ref initial_mux in
  let start = Unix.gettimeofday() in
    while FileMultiplexer.has_next !mux do
      let (new_mux,evt) = FileMultiplexer.next !mux in
        (log_evt evt;
        let output = dispatcher evt in
        if output then log_results result_chan;
        mux := new_mux)
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Tuples: "^(string_of_float (finish -. start)));
  print_endline (String.concat "\n"
     (List.map (fun (q,_,f) -> q^": "^(f())) db_access_f))
;;

(* TODO: RandomSource *)
(* TODO: multiplexer of random sources *)

(* TODO: unit tests for file sources + multiplexer *)
