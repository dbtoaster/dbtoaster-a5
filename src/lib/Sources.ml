open Schema
open Type
open Constants
open Values

type stream_event_t = event_t * const_t list
type ordered_stream_event_t = int * stream_event_t
type adaptor_event_t = AInsert | ADelete

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
   type event = (int * adaptor_event_t * const_t list) 
   type adaptor = string -> event list
   type adaptor_generator = Schema.rel_t -> (string * string) list -> adaptor
   type adaptor_registry = (string, adaptor_generator) Hashtbl.t
   
   let adaptors = Hashtbl.create 10

   let get_source_type a = fst a
   let get_adaptor a = snd a

   let create_adaptor rel_sch (name, params) : adaptor =
      if Hashtbl.mem adaptors name 
      then ((Hashtbl.find adaptors name) rel_sch params)
      else failwith ("No adaptor named "^name^" found")

   let add name (generator : adaptor_generator) =
      Hashtbl.add adaptors name generator
   
   let to_string reg = Hashtbl.fold (fun k v acc -> acc^" "^k) adaptors ""
end

(* Source sigs *)
module type SyncSource = 
sig
   type t

   val create: source_t -> (Schema.rel_t * Adaptors.adaptor) list ->
               string -> t 
   val has_next: t -> bool
   val next: t -> t * (ordered_stream_event_t option)
   val name_of_source: t -> string
end

module type SyncMultiplexer = functor (S : SyncSource) ->
sig
   type t
   val create : unit -> t
   val add_stream: t -> S.t -> t
   val remove_stream: t -> S.t -> t
   val has_next: t -> bool
   val next: t -> t * (stream_event_t option)
end

module type AsyncSource =
sig
   type t
   type handler = ordered_stream_event_t -> unit

   val create: source_t -> (Schema.rel_t * Adaptors.adaptor) list -> t
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
   type t = fs_t option * (Schema.rel_t * Adaptors.adaptor) list * 
            (ordered_stream_event_t list ref) * string

   let create source_t rels_adaptors name: t =
      match source_t with
       | FileSource(filename, framing) ->
         let ns = Some(framing, open_in filename)
         in (ns, rels_adaptors, ref [], name)
       | PipeSource(pipe_cmd, framing) ->
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
                     for i = 0 to delim_len do 
                        (!buf).[(!pos)+i] <- tok.[i] 
                     done;
                     pos := (!pos) + delim_len;
                  done;
                  (Some(fr,inc), !buf)
         with End_of_file -> (close_in inc; (None, ""))
         end 

   let has_next fs = let (ns,_,buf,_) = fs in not((ns = None) && (!buf = []))

   (* returns (_, None) at end of file *)
   let next (fs:t) : t * (ordered_stream_event_t option) =
      let (ns,ra,buf,name) = fs in
         if !buf <> [] then
            let e = List.hd (!buf)
            in buf := List.tl (!buf); ((ns,ra,buf,name), Some(e))
         else
         begin
            let (new_ns, tuple) = get_input ns in
            if tuple <> "" then
               let (events:ordered_stream_event_t list) = List.flatten (List.map
                  (fun (rel, adaptor) -> 
                     List.map (fun (order,event,fields) -> 
                        (  order,
                           (begin match event with
                              | AInsert -> InsertEvent(rel)
                              | ADelete -> DeleteEvent(rel)
                              end, 
                           fields)
                        )) (adaptor tuple)
                     ) ra) in
               if List.length events > 0 then
                 ( buf := List.tl events;
                   ((new_ns,ra,buf,name), Some(List.hd events)) )
               else
                 ((new_ns,ra,buf,name), None)
            else ((new_ns,ra,buf,name), None)
         end
   
   let name_of_source ((_,_,_,name):t) = name
end

module SM : SyncMultiplexer = functor (S : SyncSource) ->
struct
   
   type source = { 
      src : S.t ref;
      buf : ordered_stream_event_t option ref;
   }
   
   type t = source list
   
   let create () = []

   let add_stream fm s = fm@[{ src = ref s; buf = ref None }]
   
   let remove_stream fm s = List.filter (fun { src = x } -> !x <> s) fm
   
   let source_has_next { src = s; buf = b } = 
      (!b <> None) || (S.has_next !s)
   
   let has_next fm = List.exists (fun x -> source_has_next x) fm
   
   let fill_buffers fm : unit = 
      List.iter (fun { src = s; buf = b } ->
         if (!b = None) && (S.has_next !s) then (
            let new_s, new_b = S.next !s in
               b := new_b;
               s := new_s;
         )
      ) fm
   
   let filter_done fm = List.filter source_has_next fm
   
   let count_candidates = List.fold_left (fun (ord, cnt) { buf = b } ->
      match !b with None -> (ord, cnt)
         | Some(evt_ord, _) ->
            if (evt_ord = ord) then (ord, cnt+1)
            else if (evt_ord < ord) then (evt_ord, 1)
            else (ord, cnt)
   ) (max_int, 0)
   
   (* returns (_, None) at end of file *)
   let next fm : t * (stream_event_t option) =
      let curr_fm = ref fm in
      let event = ref None in
      while (List.length !curr_fm > 0) && (!event = None) do (
         Debug.print "LOG-SOURCES" (fun () -> "[SOURCES] Filling Buffers");
         fill_buffers fm;
         let ord, cnt = count_candidates fm in
         if cnt > 0 then (
            Debug.print "LOG-SOURCES" (fun () -> 
               "[SOURCES] "^(string_of_int cnt)^" sources at order "^
               (string_of_int ord)
            );
            let rec find_nth c = (function 
               | [] -> failwith "Fewer events in multiplexer than expected"
               | ({ buf = b })::rest -> 
                  begin match !b with
                     | None -> find_nth c rest
                     | Some(evt_ord, evt_data) -> 
                        if evt_ord = ord then
                           if c <= 0 then (b := None; (evt_data))
                           else find_nth (c-1) rest
                        else find_nth c rest
                  end
            ) in
            event := Some(find_nth (Random.int cnt) !curr_fm)
         );
         Debug.print "LOG-SOURCES" (fun () -> "[SOURCES] Filtering sources");
         curr_fm := filter_done !curr_fm
      );
      done;
      Debug.print "LOG-SOURCES" (fun () -> "[SOURCES] Returning event");
      (!curr_fm, !event)
end

module FileMultiplexer = SM(FileSource)

(* TODO: RandomSource *)
(* TODO: multiplexer of random sources *)

(* TODO: unit tests for file sources + multiplexer *)
