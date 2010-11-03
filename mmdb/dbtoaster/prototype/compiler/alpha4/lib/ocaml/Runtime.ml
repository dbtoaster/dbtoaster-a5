open M3
open Util
open Values
open Database
open Sources

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
     (List.map (fun (q,_,f) -> (DB.map_name_to_string q)^": "^(f())) db_access_f))
end
;;
