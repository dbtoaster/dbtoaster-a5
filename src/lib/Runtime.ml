open Type
open Constants
open Arithmetic
open Values
open Database
open Sources
open StandardAdaptors

let string_of_stream_event evt = 
 begin match evt with None -> "" | Some(evt, tuple) ->
     ( (Schema.string_of_event evt)^
       (ListExtras.ocaml_of_list Constants.string_of_const tuple) )
 end
;;

module DB = NamedK3Database
module DBCheck = DBChecker.DBAccess

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

let synch_init (table_sources:FileSource.t list) (db:DB.db_t): unit = 
   Debug.print "LOG-INTERPRETER-UPDATES" (fun () -> "Synchronous Init");
   List.iter (fun initial_source ->
      let source = ref initial_source in
      let not_done = ref true in
      let update_map map_name fields change =
         Debug.print "LOG-INTERPRETER-UPDATES" (fun () ->
            "INIT "^map_name^" <- "^(ListExtras.ocaml_of_list 
                                       Constants.string_of_const
                                       fields));
         let map = (DB.get_out_map map_name db) in
         let key = (List.map (fun x -> K3Value.BaseValue(x)) fields) in
         let old_val =
            if not (K3ValuationMap.mem key map) then 0 else
               begin match (K3ValuationMap.find key map) with
                  | K3Value.BaseValue(CInt(i)) -> i
                  | _ -> failwith ("Interpreter error: static map has "^
                                   "non-integer value")
               end
         in
         DB.update_out_map_value 
            map_name key (K3Value.BaseValue(CInt(old_val + change))) db
      in
      Debug.print "LOG-INTERPRETER-UPDATES" (fun () ->
         "LOADING STREAM "^(FileSource.name_of_source !source)
      );
      while !not_done do 
         let (new_source,event) = FileSource.next !source in
         begin match event with
            | Some(_, (Schema.InsertEvent(reln,_,_),fields)) -> 
                  update_map reln fields 1
            | Some(_, (Schema.DeleteEvent(reln,_,_),fields)) -> 
                  update_map reln fields (-1)
            | None -> not_done := false
            | _ -> failwith "Unexpected event during initialization"
         end; 
         source := new_source
      done
   ) table_sources

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
      (toplevel_query_accessors:(string * (DB.db_t -> K3Value.t)) list)
      (initialize_db:(DB.db_t -> unit))
      (dispatcher:(stream_event_t option) -> bool)
      (arguments:args_t)
      (db_checker:DBCheck.db_session_t option)
      (): unit = 
  let (output_separator,title_separator) = 
    if Debug.active "SINGLE-LINE-MAP-OUTPUT" then (";",": ") else (";\n",":\n")
  in
  let db_access_f = List.map (fun (q_name,q_access_f) ->
         (String.lowercase q_name, (fun () -> 
           let v_str = (K3Value.string_of_value ~sep:output_separator
                                                (q_access_f db))
           in
              q_name^
              (if String.contains v_str '\n' then title_separator else ": ")^
              v_str
         ))
     ) toplevel_query_accessors
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
      else if y = "tlqs" then
         (fun chan -> output_endline chan
            (String.concat "\n"
               (List.map (fun (_,f) -> f()) db_access_f)))
      else
         let desired_tlq_accessor = List.assoc y db_access_f in
         (fun chan -> output_endline chan (desired_tlq_accessor ()))
             
  in
  let mux = ref initial_mux in
  let start = Unix.gettimeofday() in 
    initialize_db db;
    let _ = dispatcher (Some(Schema.SystemInitializedEvent, [])) in
    while FileMultiplexer.has_next !mux do
      let (new_mux,(evt:stream_event_t option)) = 
         try 
            FileMultiplexer.next !mux 
         with Failure(x) -> failwith ("Parse Error ["^x^"]")
      in
        (  log_evt evt;
           let output = dispatcher evt in
           try 
              begin match db_checker with
                 | Some(dbc) -> 
                     let tlq_results = 
                        List.map (fun (q_name,q_access_f) ->
                           (q_name, q_access_f db)
                        ) toplevel_query_accessors 
                     in (
                        try 
                           DBCheck.check_result dbc tlq_results
                        with Failure(msg) ->
                           print_endline "========= DB State =========";
                           print_endline ((DB.db_to_string db)^"\n\n");
                           failwith msg
                     )
                 | None -> ()
              end;                               
              if output then log_results result_chan;
              mux := new_mux
           with Failure(x) -> failwith ("Dispatch Error ["^x^"]: "^
                                        (string_of_stream_event evt))
        )
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Processing time: "^(string_of_float (finish -. start)));
  print_endline ("---------- Results ------------");
  print_endline (String.concat "\n"
     (List.map (fun (_,f) -> f()) db_access_f))

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
             (rel, (Adaptors.create_adaptor rel adaptor))) rel_adaptors)
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
                            | _ -> Constants.string_of_const c) t))
           in output_string rel_chan (s^"\n")
         in
         begin match event_name with
         | Schema.InsertEvent(rel) -> aux ("Insert"^(Schema.name_of_rel rel))
         | Schema.DeleteEvent(rel) -> aux ("Delete"^(Schema.name_of_rel rel))
         | Schema.CorrectiveUpdate _ -> 
               failwith "Ocaml Runtime does not support distributed execution"
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
