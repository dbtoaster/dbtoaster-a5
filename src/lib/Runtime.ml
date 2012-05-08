open Types
open Arithmetic
open Values
open Database
open Sources
open StandardAdaptors

let string_of_stream_event evt = 
 begin match evt with None -> "" | Some(evt, tuple) ->
     ( (Schema.string_of_event evt)^
       (ListExtras.ocaml_of_list Types.string_of_const tuple) )
 end
;;

module Make = functor(DB : K3DB) ->
struct

type args_t = { 
   verbose : bool ref; 
   result  : string option ref; 
   output  : string ref
}

let default_args (): args_t = {
   verbose = ref false;
   result = ref None;
   output = ref "-"
}

let main_args (): args_t =
   let (args:args_t) = default_args () in
   let specs:(Arg.key * Arg.spec * Arg.doc) list = Arg.align [
      (  "-v", 
         (Arg.Set(args.verbose)),
         "               Show all updates" );
      (  "-r",
         (Arg.String((fun x -> args.result := Some(x)))),
         "<db|map|value> Set state to be output");
      (  "-o",
         (Arg.Set_string(args.output)),
         "file           Set the output file (default stdout)")
   ] in
      Arg.parse (specs) (fun x -> ()) "query [opts]";
      args
;;
let synch_main 
      (db:DB.db_t)
      (initial_mux:FileMultiplexer.t)
      (toplevel_queries:DB.map_name_t list)
      (dispatcher:(stream_event_t option) -> bool)
      (arguments:args_t)
      (): unit = 
  let (output_separator,title_separator) = 
    if Debug.active "SINGLE-LINE-MAP-OUTPUT" then (";",": ") else (";\n",":\n")
  in
  let db_access_f = List.map (fun q ->
     if DB.has_map q db then
        (q,"map", (fun () -> (DB.map_name_to_string q)^title_separator^
           (DB.map_to_string ~sep:output_separator (DB.get_map q db))))
     
     (* Note: no distinction between in/out maps... 
              fix in db if really needed *)
     else if (DB.has_in_map q db) then
        (q,"map", (fun () -> (DB.map_name_to_string q)^title_separator^
           (DB.smap_to_string ~sep:output_separator (DB.get_in_map q db))))

     else if DB.has_out_map q db then
        (q,"map", (fun () -> (DB.map_name_to_string q)^title_separator^
           (DB.smap_to_string ~sep:output_separator (DB.get_out_map q db))))
     
     else (q,"value", (fun () ->
        (DB.map_name_to_string q)^": "^(
           DB.value_to_string
              (match DB.get_value q db with | Some(x) -> x | _ -> DB.zero))))
     ) toplevel_queries
  in
  let log_evt = 
    if !(arguments.verbose) 
      then (fun evt -> print_endline (string_of_stream_event evt))
      else (fun evt -> ())
  in
  let result_chan = match !(arguments.output) with
      | "-" -> stdout
      | x -> try open_out x with Sys_error _ -> 
    print_endline ("Failed to open output file: "^x); stdout
  in
  let log_results = match !(arguments.result) with
    | None -> (fun chan -> ())
    | Some(x) -> 
      let output_endline c s = output_string c (s^"\n") in
      let y = String.lowercase x in
      if y = "db" then
         (fun chan -> output_endline chan ("db: "^(DB.db_to_string db)))
      else
         let valid_f = List.filter (fun (_,z,_) -> y = z) db_access_f in
         (fun chan -> output_endline chan
         (String.concat "\n"
            (List.map (fun (q,_,f) -> f()) valid_f)))
  in
  let mux = ref initial_mux in
  let start = Unix.gettimeofday() in
    while FileMultiplexer.has_next !mux do
      let (new_mux,(evt:stream_event_t option)) = 
         try 
            FileMultiplexer.next !mux 
         with Failure(x) -> failwith ("Parse Error ["^x^"]")
      in
        (  log_evt evt;
           let output = dispatcher evt in
           try 
              if output then log_results result_chan;
              mux := new_mux
           with Failure(x) -> failwith ("Dispatch Error ["^x^"]: "^
                                        (string_of_stream_event evt))
        )
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Processing time: "^(string_of_float (finish -. start)));
  print_endline (String.concat "\n"
     (List.map (fun (q,_,f) -> f()) db_access_f))

end
;;

let run_adaptors output_dir sources =
  StandardAdaptors.initialize();
  let sql_string_of_float f =
    let r = string_of_float f in
    if r.[(String.length r)-1] = '.' then r^"0" else r in
  let rel_outputs = Hashtbl.create 10 in
  let add_rel_output r = 
    if Hashtbl.mem rel_outputs r then ()
    else Hashtbl.replace rel_outputs r
           (open_out (Filename.concat output_dir (r^".dbtdat")))
  in
  let sources_and_adaptors = try List.fold_left (fun acc (s,rel,a) ->
      add_rel_output ("Insert"^(Schema.name_of_rel rel));
      add_rel_output ("Delete"^(Schema.name_of_rel rel));
      if List.mem_assoc s acc then
          let existing = List.assoc s acc
        in (s, ((rel,a)::existing))::(List.remove_assoc s acc)
      else (s,[rel,a])::acc)
    [] sources
    with Failure(x) -> failwith "Parse error extracting adaptor"
  in
  let sources_t = try List.map (fun (src,rel_adaptors) ->
    let src_impl = match src with
      | Schema.FileSource _ ->
        FileSource.create src 
          (List.map (fun (rel,adaptor) -> 
             (rel, (Adaptors.create_adaptor adaptor))) rel_adaptors)
          (ListExtras.ocaml_of_list (fun (x,_) -> Schema.string_of_rel x)
                                    rel_adaptors)
      | Schema.SocketSource _ -> failwith "Sockets not yet implemented."
      | Schema.PipeSource _   -> failwith "Pipes not yet implemented."
      | Schema.NoSource       -> failwith "Query with unsourced stream."
    in src_impl) sources_and_adaptors
    with Failure(x) -> failwith "Parse error in adaptor"
  in
  let mux = ref (List.fold_left
      (fun m source -> FileMultiplexer.add_stream m source)
      (FileMultiplexer.create ()) sources_t) in
  let output_event evt = 
    try 
       match evt with
       | None -> ()
       | Some(event_name,t) ->
         let aux r = 
           let rel_chan =
             try Hashtbl.find rel_outputs r
             with Not_found -> failwith ("no output file initialized for "^r)
           in let s = (String.concat "," (List.map (fun c ->
                          match c with 
                            | CString(s) -> "\""^s^"\""
                            | CFloat(f) -> sql_string_of_float f
                            | _ -> Types.string_of_const c) t))
           in output_string rel_chan (s^"\n")
         in
         begin match event_name with
         | Schema.InsertEvent(rel) -> aux ("Insert"^(Schema.name_of_rel rel))
         | Schema.DeleteEvent(rel) -> aux ("Delete"^(Schema.name_of_rel rel))
         | Schema.SystemInitializedEvent -> ()
         end  
           
    with Failure(x) -> 
      failwith ("Parse Error ("^x^"): '"^(string_of_stream_event evt)^"'")
  in
  let start = Unix.gettimeofday() in
    while FileMultiplexer.has_next !mux do
       try 
         let (new_mux,evt) = FileMultiplexer.next !mux in
           output_event evt;
           mux := new_mux
       with Failure(x) -> failwith ("Parse Error ("^x^")")
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Processing time: "^(string_of_float (finish -. start)));
  Hashtbl.iter (fun rel chan -> close_out chan) rel_outputs
