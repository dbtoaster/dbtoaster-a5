open Types

type rel_type_t = 
   | StreamRel           (* Dynamic relation.  The standard for DBT *)
   | TableRel            (* Static relation; DBT does not generate update 
                            triggers for Tables *)
type rel_t = 
   string *              (* Relation name *)
   var_t list *          (* Relation schema *)
   rel_type_t *          (* Metadata about how to handle the relation *)
   type_t                (* The relation's data type (usually the data is the
                            multiplicity of tuples in the bag, so this is nearly
                            always going to be a TInt) *)
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
   List.find (fun (cmpn,_,_,_) -> reln == cmpn) (rels db)

let string_of_schema (sch:t):string =
   ListExtras.string_of_list ~sep:"\n" (fun (source, rels) ->
      begin match source with
         | NoSource -> "Sourceless"
         | FileSource(file, _) -> file
         | PipeSource(file, _) -> "| "^file
         | SocketSource(bindaddr, port, _) -> 
            if bindaddr = (Unix.inet_addr_any) then
               "*:"^(string_of_int port)
            else
               (Unix.string_of_inet_addr bindaddr)^":"^(string_of_int port)
      end^"\n"^
      (ListExtras.string_of_list ~sep:"\n" 
         (fun ((aname,aparams),(reln,relsch,relt,_)) ->
            "   "^reln^"("^
               (ListExtras.string_of_list ~sep:", " string_of_var relsch)^
            ")"^begin 
               if Debug.active "PRINT-VERBOSE" then
                  match relt with
                     | TableRel  -> " initialized using "
                     | StreamRel -> " updated using "
               else
                  match relt with
                     | TableRel  -> " := "
                     | StreamRel -> " << "
            end^aname^"("^(ListExtras.string_of_list ~sep:", "
               (fun (pname,pval) -> pname^" := '"^pval^"'") aparams
            )^")"
         ) rels
      )
   ) !sch
