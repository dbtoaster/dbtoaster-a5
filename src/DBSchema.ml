open GlobalTypes

type rel_t = string * var_t list * var_t
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

type source_info_t = ((source_t * framing_t) * (adaptor_t * rel_t) list)

type t = source_info_t list ref

let empty_db:t = ref []

let add_rel (db:t) 
            ?(source = NoSource) 
            ?(framing = FixedSize(0)) 
            ?(decoder = ("",[]))
            (rel:rel_t) = 
   if List.mem_assoc (source,framing) !db then
      let source_rels = List.assoc (source,framing) !db in
      db := ((source, framing), (decoder, rel)::source_rels) ::
               (List.remove_assoc (source,framing) !db)
   else
      db := ((source, framing), [decoder, rel]) :: !db

let rels (db:t): rel_t list =
   List.fold_left (fun old (_, rels) -> old@(List.map snd rels)) [] !db

let rel (db:t) (reln:string): rel_t =
   List.find (fun (cmpn,_,_) -> reln == cmpn) (rels db)
   