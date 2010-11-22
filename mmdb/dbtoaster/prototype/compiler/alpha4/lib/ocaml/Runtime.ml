open M3
open Util
open Values
open Database
open Sources
open StandardAdaptors

module Make = functor(DB : M3DB) ->
struct
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
      (db:DB.db_t)
      (initial_mux:FileMultiplexer.t)
      (toplevel_queries:DB.map_name_t list)
      (dispatcher:((M3.pm_t*M3.rel_id_t*M3.const_t list) option) -> bool)
      (arguments:ParseArgs.arguments_t)
      (): unit = 
  let db_access_f = List.map (fun q ->
     if DB.has_map q db then
        (q,"map", (fun () -> DB.map_to_string (DB.get_map q db)))
     
     (* Note: no distinction between in/out maps... fix in db if really needed *)
     else if (DB.has_in_map q db) then
        (q,"map", (fun () -> (DB.map_name_to_string q)^": "^
           (DB.smap_to_string (DB.get_in_map q db))))

     else if DB.has_out_map q db then
        (q,"map", (fun () -> (DB.map_name_to_string q)^": "^
           (DB.smap_to_string (DB.get_out_map q db))))
     
     else (q,"value", (fun () ->
        DB.value_to_string
           (match DB.get_value q db with | Some(x) -> x | _ -> DB.zero)))
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
         (fun chan -> output_endline chan ("db: "^(DB.db_to_string db)))
      else
         let valid_f = List.filter (fun (_,z,_) -> y = z) db_access_f in
         (fun chan -> output_endline chan
         (String.concat "\n"
            (List.map (fun (q,_,f) -> f()) valid_f)))
  in
  let string_of_evt evt = match evt with
    | None -> "[null]"
    | Some(Insert,rel,t) -> 
      "Insert_"^rel^" "^(Util.list_to_string M3Common.string_of_const t)
    | Some(Delete,rel,t) -> 
      "Delete_"^rel^" "^(Util.list_to_string M3Common.string_of_const t)
  in
  let mux = ref initial_mux in
  let start = Unix.gettimeofday() in
    while FileMultiplexer.has_next !mux do
      try 
         let (new_mux,evt) = FileMultiplexer.next !mux in
           (  log_evt evt;
              try 
                 let output = dispatcher evt in
                 if output then log_results result_chan;
                 mux := new_mux
              with Failure(x) -> failwith ("Parse Error ["^x^"]: "^(string_of_evt evt))
           )
      with Failure(x) -> failwith ("Parse Error ["^x^"]")
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Processing time: "^(string_of_float (finish -. start)));
  print_endline (String.concat "\n"
     (List.map (fun (q,_,f) -> (DB.map_name_to_string q)^": "^(f())) db_access_f))

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
  let sources_and_adaptors = try List.fold_left (fun acc (s,f,rel,a) ->
      add_rel_output rel;
      if List.mem_assoc (s,f) acc then
          let existing = List.assoc (s,f) acc
        in ((s,f), ((rel,a)::existing))::(List.remove_assoc (s,f) acc)
      else ((s,f),[rel,a])::acc)
    [] sources
    with Failure(x) -> failwith "Parse error extracting adaptor"
  in
  let sources_t = try List.map (fun ((src,framing),rel_adaptors) ->
    let src_impl = match src with
      | FileSource(fn) ->
        FileSource.create src framing 
          (List.map (fun (rel,adaptor) -> 
             (rel, (Adaptors.create_adaptor adaptor))) rel_adaptors)
          (list_to_string fst rel_adaptors)
      | SocketSource(_) -> failwith "Sockets not yet implemented."
      | PipeSource(_)   -> failwith "Pipes not yet implemented."
    in src_impl) sources_and_adaptors
    with Failure(x) -> failwith "Parse error in adaptor"
  in
  let mux = ref (List.fold_left
      (fun m source -> FileMultiplexer.add_stream m source)
      (FileMultiplexer.create ()) sources_t) in
  let string_of_evt evt = match evt with
    | None -> "[null]"
    | Some(Insert,rel,t) -> 
      "Insert_"^rel^" "^(Util.list_to_string M3Common.string_of_const t)
    | Some(Delete,rel,t) -> 
      "Delete_"^rel^" "^(Util.list_to_string M3Common.string_of_const t)
  in
  let output_event evt = 
    try 
       match evt with
       | None -> ()
       | Some(pm,rel,t) -> 
           let rel_chan =
             try Hashtbl.find rel_outputs rel
             with Not_found -> failwith ("no output file initialized for "^rel)
           in let s = (String.concat "," (List.map (fun c ->
                          match c with CFloat(f) -> sql_string_of_float f) t))
           in if pm = Insert then output_string rel_chan (s^"\n") else ()
    with Failure(x) -> 
      failwith ("Parse Error ("^x^"): '"^(string_of_evt evt)^"'")
  in
  let start = Unix.gettimeofday() in
    while FileMultiplexer.has_next !mux do
       try 
         let (new_mux,evt) = FileMultiplexer.next !mux in
           output_event evt;
           mux := new_mux
       with Failure(x) -> 
         failwith ("Parse Error ("^x^")")
    done;
  let finish = Unix.gettimeofday () in
  print_endline ("Processing time: "^(string_of_float (finish -. start)));
  Hashtbl.iter (fun rel chan -> close_out chan) rel_outputs
