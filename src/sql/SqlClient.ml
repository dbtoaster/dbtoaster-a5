open Type
open Constants

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
                     string -> Type.var_t list -> unit
   val insert: sql_channel_t -> string -> Type.var_t list -> 
               Constants.const_t list list -> unit
   val delete: sql_channel_t -> string -> Type.var_t list -> 
               Constants.const_t list list -> unit
   val query: ?field_separator:string -> sql_channel_t -> Sql.select_t -> 
              Type.var_t list -> Constants.const_t list list
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
            let _ = Unix.select [(Unix.descr_of_in_channel chan)] 
                                [] [] (-1.0) 
            in
            while true do 
               data := (!data) ^ (input_line chan) ^"\n"
            done
         with Sys_blocked_io -> ()
      ); 
      !data
   ;;

   let convert_schema (sch:Type.var_t list):string = 
      "("^
         " _id serial, "^
         (ListExtras.string_of_list ~sep:", " (fun (vn,vt) ->  
            vn ^ " " ^ 
            (match vt with 
               | TAny | TExternal(_) -> 
                  failwith "Unsupported type in Sql client"
               | TBool   -> "int"
               | TInt    -> "bigint"
               | TFloat  -> "float"
               | TChar   -> "char(1)"
               | TString -> "varchar(1000)"
               | TDate   -> "date"
               | TInterval _ -> "interval"
            )
         ) sch)^
      ")"   
   ;;

   let trim str = 
      let str = Str.replace_first (Str.regexp "^[ ]+") "" str in
      Str.replace_first (Str.regexp "[ ]+$") "" str;;           

   let const_of_string (_str:string) (const_type:Type.type_t) : 
                       Constants.const_t =
      let str = trim _str in
      match const_type with
         | TBool -> 
            begin match str with
               | "t" -> CBool(true)
               | "f" -> CBool(false)
               | _ -> failwith 
                  "Unsupported boolean representation in Sql client"
            end
         | TInt  -> CInt(int_of_string (trim str))
         | TFloat -> CFloat(float_of_string (trim str))
         | TChar -> 
            if ((String.length str) = 1) then   
               CChar(String.get str 0)
            else
               failwith "Invalid character"
         | TString -> 
            let rec find_non_space s i incr_op = 
               if String.get s i <> ' ' then i
               else find_non_space s (incr_op i 1) incr_op
            in
            let lstart = 
               find_non_space str 0 (+) in
            let rstart =
               find_non_space str ((String.length str)-1) (-) in
            CString(
               if lstart > rstart then "" else
                  String.sub str lstart (rstart-lstart+1)
            )
         | TDate   -> 
            if (Str.string_match
                  (Str.regexp "\\([0-9]+\\)-\\([0-9]+\\)-\\([0-9]+\\)") 
                   str 0)
            then (
               let y = (int_of_string (Str.matched_group 1 str)) in
               let m = (int_of_string (Str.matched_group 2 str)) in
               let d = (int_of_string (Str.matched_group 3 str)) in
               if (m > 12) then failwith 
                  ("Invalid month ("^(string_of_int m)^") in date: "^str);
               if (d > 31) then failwith
                  ("Invalid day ("^(string_of_int d)^") in date: "^str);
               CDate(y, m, d)
            ) else
               failwith ("Improperly formatted date: "^str)  
         | TInterval _ -> 
            failwith ("Converting string to interval currently not " ^
                      "supported in Sql client")
         | TAny | TExternal(_) -> failwith "Unsupported type in Sql client"

   let convert_field = (function x -> Constants.sql_of_const x)
 
   let convert_row ?(sep = ", ") (data:Constants.const_t list):string =
      ListExtras.string_of_list ~sep:sep convert_field data
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
         Unix.open_process ("PGOPTIONS='--client-min-messages=warning' " ^
                            "psql -tq "^(ListExtras.string_of_list ~sep:" " 
                                                               (fun x -> x)
                                                               full_flags))
      in 
         Unix.set_nonblock (Unix.descr_of_in_channel in_chan);
         (in_chan,out_chan)
   
   let destroy x = let _ = Unix.close_process x in ()
   
   let create_table (channel: sql_channel_t) ?(temporary = false) 
                    (tname:string) (tschema:Type.var_t list): unit =
      let cmd = "CREATE " ^
         (if temporary then "TEMPORARY " else "")^
         "TABLE "^tname^
         (convert_schema tschema)^
         ";\n"
      in
         Debug.print "LOG-SQLCLIENT" (fun () -> 
            "[SQL client] Create table: " ^ cmd
         );
         put channel cmd
   ;;

   let drop_table (channel: sql_channel_t) (tname:string): unit =
      let cmd = "DROP TABLE IF EXISTS "^tname^";\n"
      in
         Debug.print "LOG-SQLCLIENT" (fun () ->
            "[SQL client] Drop table: " ^ cmd
         );
         put channel cmd
   ;;

   let insert (channel:sql_channel_t) (tname:string) (tschema:Type.var_t list)
              (data:Constants.const_t list list): unit =
      let cmd = 
         "INSERT INTO " ^ tname ^ "("^
         (ListExtras.string_of_list ~sep:", " fst tschema) ^
         ") VALUES " ^ (
            String.concat ", " 
                          (List.map (fun row -> " (" ^ (convert_row row) ^ ")") 
                                    data)
         ) ^ ";\n"
      in
         Debug.print "LOG-SQLCLIENT" (fun () ->
            "[SQL client] Insert into: " ^ cmd
         );
         put channel cmd
   ;;

   let delete (channel:sql_channel_t) (tname:string) (tschema:Type.var_t list)
              (data:Constants.const_t list list): unit =
      List.iter (fun tuple ->
         let cmd =
            "DELETE FROM " ^ tname ^
            " WHERE _id = (SELECT max(_id) FROM " ^ tname ^ " WHERE " ^
            (String.concat " AND " (
               List.map2 (fun (vn,_) c -> 
                  vn ^ "=" ^ (convert_field c)) tschema tuple
            )) ^ ");\n"
         in
            Debug.print "LOG-SQLCLIENT" (fun () ->
               "[SQL client] Delete from: " ^ cmd
            );
            put channel cmd
      ) data
   ;;

   let query ?(field_separator:string = "|") (channel:sql_channel_t) 
             (query:Sql.select_t) (schema:Type.var_t list) : 
             Constants.const_t list list =
      let cmd = (Sql.string_of_select query)^"\n" in
         Debug.print "LOG-SQLCLIENT" (fun () ->
            "[SQL client] Querying: " ^ cmd
         );
         put channel cmd;
         let lines = Str.split (Str.regexp "\n") (get channel) in
         Debug.print "LOG-SQLCLIENT" (fun () ->
            "[SQL client] Query result: \n\t" ^
            (ListExtras.string_of_list ~sep:"\n\t" (fun x -> x) lines)
         );
         List.fold_left (fun tuple_list line ->
            if (trim line = "") then tuple_list
            else begin
               let fields = Str.split (Str.regexp field_separator) line in 
               tuple_list @ [
                  List.map2 (fun fstr (_, ftype) ->
                     const_of_string fstr ftype
                  ) fields schema
                ]
            end
         ) [] lines
         
end
