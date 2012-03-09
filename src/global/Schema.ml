(**
   Global definitions for the schema of a database/set of streams
*)

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

type event_t =
 | InsertEvent of rel_t
 | DeleteEvent of rel_t
 | SystemInitializedEvent  (* Invoked when the system has been initialized, once
                              all static tables have been loaded. *)

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

let empty_db ():t = ref []

let add_rel (db:t) ?(source = NoSource) ?(adaptor = ("",[])) (rel:rel_t) = 
   if List.mem_assoc source !db then
      let source_rels = List.assoc source !db in
      db := (source, (adaptor, rel)::source_rels) ::
               (List.remove_assoc source !db)
   else
      db := (source, [adaptor, rel]) :: !db

let rels (db:t): rel_t list =
   List.fold_left (fun old (_, rels) -> old@(List.map snd rels)) [] !db

let table_rels (db:t): rel_t list =
   (List.filter (fun (_,_,rt,_) -> rt == TableRel) (rels db))

let stream_rels (db:t): rel_t list =
   (List.filter (fun (_,_,rt,_) -> rt == StreamRel) (rels db))

let rel (db:t) (reln:string): rel_t =
   List.find (fun (cmpn,_,_,_) -> reln = cmpn) (rels db)

let event_vars (event:event_t): var_t list =
   begin match event with
      | InsertEvent(_,relv,_,_) -> relv
      | DeleteEvent(_,relv,_,_) -> relv
      | SystemInitializedEvent -> []
   end

let events_equal (a:event_t) (b:event_t): bool =
   begin match (a,b) with
      | (SystemInitializedEvent, SystemInitializedEvent) -> true
      | (InsertEvent(an,_,_,_), InsertEvent(bn,_,_,_)) -> an = bn
      | (DeleteEvent(an,_,_,_), DeleteEvent(bn,_,_,_)) -> an = bn
      | _ -> false
   end

let string_of_rel ((reln,relsch,_,_):rel_t): string =
   (reln^"("^(ListExtras.string_of_list ~sep:", " string_of_var relsch)^")")

let string_of_event (event:event_t) =
   begin match event with 
      | InsertEvent(rel)       -> "ON + "^(string_of_rel rel)
      | DeleteEvent(rel)       -> "ON - "^(string_of_rel rel)
      | SystemInitializedEvent -> "ON SYSTEM READY"
   end

let code_of_framing (framing:framing_t):string = begin match framing with
      | Delimited("\n") -> "LINE DELIMITED"
      | Delimited(x)    -> "'"^x^"' DELIMITED"
      | FixedSize(i)    -> "FIXEDWIDTH "^(string_of_int i)
   end

let code_of_source (source:source_t):string = begin match source with
      | NoSource -> ""
      | FileSource(file, framing) -> 
         "FROM FILE '"^file^"' "^(code_of_framing framing)
      | PipeSource(file, framing) -> 
         "FROM PIPE '"^file^"' "^(code_of_framing framing)
      | SocketSource(addr, port, framing) -> 
         "FROM SOCKET "^(if addr = Unix.inet_addr_any then "" else
                         "'"^(Unix.string_of_inet_addr addr)^"' ")^
         (string_of_int port)^" "^(code_of_framing framing)
   end

let code_of_adaptor ((aname, aparams):adaptor_t):string = 
   (String.uppercase aname)^(
      if aparams = [] then (if aname <> "" then "()" else "")
      else "("^(ListExtras.string_of_list ~sep:", " (fun (k,v) ->
         k^" := '"^v^"'") aparams)^")")

let code_of_rel (reln, relv, relt, _): string =
   "CREATE "^(match relt with 
      | StreamRel -> "STREAM"
      | TableRel  -> "TABLE"
   )^" "^reln^"("^(ListExtras.string_of_list ~sep:", " (fun (varn,vart) ->
      varn^" "^(string_of_type vart)
   ) relv)^")"

let code_of_schema (sch:t):string =
   ListExtras.string_of_list ~sep:"\n\n" (fun (source, rels) ->
      let source_string = code_of_source source in
         ListExtras.string_of_list ~sep:"\n\n" (fun (adaptor,rel) ->
            (code_of_rel rel)^"\n  "^source_string^"\n  "^
            (code_of_adaptor adaptor)^";"
         ) rels
   ) !sch

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
