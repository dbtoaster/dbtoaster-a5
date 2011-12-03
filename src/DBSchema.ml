open GlobalTypes

type rel_t = string * var_t list * type_t
type event_type_t = InsertEvent | DeleteEvent
type event_t = event_type_t * rel_t

type framing_t =
   | Delimited of string (* delimiter *)
   | FixedSize of int    (* frame size *)

type source_t = 
   | NoSource
   | FileSource of string * framing_t (* filename *)
   | PipeSource of string * framing_t (* command *)
   | SocketSource of Unix.inet_addr * int * framing_t  (* port *)

type adaptor_t = string * (string * string) list

type source_info_t = (source_t * (adaptor_t * rel_t) list)

type t = source_info_t list ref

let empty_db:t = ref []

let add_rel (db:t) ?(source = NoSource) ?(adaptor = ("",[])) (rel:rel_t) = 
   if List.mem_assoc source !db then
      let source_rels = List.assoc source !db in
      db := (source, (adaptor, rel)::source_rels) ::
               (List.remove_assoc source !db)
   else
      db := (source, [adaptor, rel]) :: !db

let rels (db:t): rel_t list =
   List.fold_left (fun old (_, rels) -> old@(List.map snd rels)) [] !db

let rel (db:t) (reln:string): rel_t =
   List.find (fun (cmpn,_,_) -> reln == cmpn) (rels db)