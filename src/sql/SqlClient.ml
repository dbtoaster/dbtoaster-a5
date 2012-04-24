open Types

module type Interface = sig
   type sql_channel_t
   ;;
   val create: ?database:string option -> 
               ?username:string option ->
               ?password:string option ->
               ?flags:string list ->
                  unit -> sql_channel_t
   
   val destroy: sql_channel_t -> unit

   val create_table: sql_channel_t -> ?temporary:bool ->
                        string -> Types.var_t list -> unit
   val insert: sql_channel_t -> string -> Types.var_t list -> 
               Types.const_t list list -> unit
   val query: sql_channel_t -> Sql.select_t -> Types.const_t list list
end 

module Postgres : Interface = struct
   type sql_channel_t = in_channel * out_channel
   ;;
   
   let put ((_,chan):sql_channel_t) (msg:string):unit = 
      output_string chan msg; flush chan
   ;;
   
   let get ((chan,_):sql_channel_t):string = 
      let data = ref "" in (
         try 
            while true do 
               data := (!data) ^ (input_line chan) ^"\n"
            done
         with Sys_blocked_io -> ()
      ); 
      Debug.print "LOG-POSTGRES" (fun () -> 
         "PSQL: "^(!data)
      );
      !data
   ;;

   let convert_schema (sch:Types.var_t list):string = 
      "("^
         (ListExtras.string_of_list ~sep:", " (fun (vn,vt) ->  
            vn ^ " " ^ 
            (match vt with 
              TAny | TExternal(_) -> failwith "Unsupported type in Sql client"
            | TBool | TInt -> "int"
            | TFloat -> "float"
            | TString -> "varchar(1000)"
            )
         ) sch)^
      ")"   
   ;;
   let convert_row ?(sep = ", ") (data:Types.const_t list):string =
      ListExtras.string_of_list ~sep:sep (function 
         | CBool(true) -> "1"
         | CBool(false) -> "0"
         | CInt(av) -> string_of_int av
         | CFloat(av) -> string_of_float av
         | CString(av) -> "'"^av^"'"
      ) data
   ;;
   
   let create ?(database = None) ?(username = None) ?(password = None) 
              ?(flags=[]) (): sql_channel_t =
      let full_flags = 
         (  match database with None -> [] | Some(s) -> [s]) @
         (  match username with None -> [] | Some(s) -> ["-U"; s]) @
         flags @ 
         ["-n"; "-q"; "-S"]
      in
      let (in_chan,out_chan) = 
         Unix.open_process ("psql "^(ListExtras.string_of_list ~sep:" " 
                                                               (fun x -> x)
                                                               full_flags))
      in 
         Unix.set_nonblock (Unix.descr_of_in_channel in_chan);
         (in_chan,out_chan)
   
   let destroy x = let _ = Unix.close_process x in ()
   
   let create_table (channel: sql_channel_t) ?(temporary = false) (tname:string) 
                    (tschema:Types.var_t list): unit =
      let cmd = "CREATE " ^
         (if temporary then "TEMPORARY " else "")^
         "TABLE "^tname^
         (convert_schema tschema)^
         ";\n"
      in
         put channel cmd
   ;;
   let insert (channel:sql_channel_t) (tname:string) (tschema:Types.var_t list)
              (data:Types.const_t list list): unit =
      let cmd = 
         "INSERT INTO "^tname^"("^
         (ListExtras.string_of_list ~sep:", " fst tschema)^
         ") VALUES "^(
            String.concat " " 
                          (List.map (fun row -> " ("^(convert_row row)^")") 
                                    data)
         )^";\n"
      in
         put channel cmd
   ;;
   let query (channel:sql_channel_t) (query:Sql.select_t) =
      let cmd = (Sql.string_of_select query)^"\n" in
         put channel cmd;
         Unix.sleep 1;
         print_endline (get channel);
         []
end
