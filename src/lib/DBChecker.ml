(**
  Validation of top-level K3 databases using a DBMS (PostgreSQL). 
*)

open Database
open SqlClient
open Values

module DB = NamedK3Database

module AbstractDBAccess = functor (I : Interface) ->
struct

   type query_t = {
      stmt    : Sql.select_t;
      schema  : Type.var_t list;    
   }
    
   type db_session_t = {
      client  : I.sql_channel_t ref;
      tables  : Sql.table_t list ref;
      queries : query_t list ref;
   }
   
   type hashtbl_t = (Constants.const_t list, Constants.const_t list) Hashtbl.t
   
     
   let get_table (db_session : db_session_t) 
                 (name : string) : Sql.table_t = 
      List.find (fun (table_name, _, _, _) -> 
         table_name = name
      ) !(db_session.tables)
   
    
   let convert_schema (schema : Sql.schema_t) : Type.var_t list =
      List.map (fun (_,n,t) -> (n,t)) schema
   
          
   let get_schema (db_session : db_session_t) 
                  (table_name : string) : Type.var_t list =
      let (_, schema, _, _) = get_table db_session table_name in
         convert_schema schema
    
   
   let add_table (db_session : db_session_t) (table : Sql.table_t) : unit =
      db_session.tables := table :: !(db_session.tables);
      let (name, schema, _, _) = table in
      let schema_sql = convert_schema schema in
         I.create_table !(db_session.client) ~temporary:true name schema_sql
   
    
   let add_query (db_session : db_session_t) (query : Sql.select_t) : unit =
      db_session.queries := { stmt = query; 
                              schema = convert_schema (
                                 Sql.select_schema !(db_session.tables) query
                              ) 
                            } :: !(db_session.queries) 
   
    
   let add_program (db_session : db_session_t) 
                   (sql_program : Sql.file_t) : unit =
      let (tables, queries) = sql_program in
         List.iter (fun table -> add_table db_session table) tables;
         List.iter (fun query -> add_query db_session query) queries
   
    
   let init ?(database : string option = None) 
            ?(username : string option = None) 
            ?(password : string option = None) 
            ?(flags : string list = [])
            (sql_program : Sql.file_t) : db_session_t =
      let db_client = I.create ~database:database ~username:username 
                               ~password:password ~flags:flags () 
      in
      let db_session = { client  = ref db_client;
                         tables  = ref [];
                         queries = ref []  } 
      in
         add_program db_session sql_program;
         db_session     
   
    
   let handle_tuple (db_session : db_session_t)
                    (event:Schema.event_t) 
                    (tuple:Constants.const_t list) : unit =
      match event with
         | Schema.InsertEvent(rel_name, _, _) ->
            let schema = get_schema db_session rel_name in
            I.insert !(db_session.client) rel_name schema [tuple]
         | Schema.DeleteEvent(rel_name, _, _) ->
            let schema = get_schema db_session rel_name in
            I.delete !(db_session.client) rel_name schema [tuple]
         | _ -> ()

      
   let to_hashtbl (tuples : Constants.const_t list list) : hashtbl_t = 
      let hashtbl = Hashtbl.create 10 in      
      List.iter (fun tuple ->
         let (key, value) = ListExtras.split_at_last tuple in
         if (value <> []) then Hashtbl.replace hashtbl key value         
      ) tuples;
      hashtbl
   
   let print_hashtbl (title : string) (hashtbl : hashtbl_t) : unit =
      print_endline("\n"^title);
      Hashtbl.iter (fun keys values -> 
         print_endline 
            ("[" ^ 
             (ListExtras.string_of_list Constants.string_of_const keys) ^
             "] -> " ^
             (ListExtras.string_of_list Constants.string_of_const values))
      ) hashtbl
   
   let cmp_hashtbl ?(default = (fun _ -> Constants.CFloat(0.))) 
                   (hashtbl_a : hashtbl_t) (hashtbl_b : hashtbl_t) : bool =
      let keys tbl = Hashtbl.fold (fun k _ l -> k::l) tbl [] in
      let all_keys = ListAsSet.union (keys hashtbl_a) (keys hashtbl_b) in
      List.for_all (fun key ->
         let value_a = try List.hd (Hashtbl.find hashtbl_a key) 
                       with Not_found -> default key in
         let value_b = try List.hd (Hashtbl.find hashtbl_b key)
                       with Not_found -> default key in
         let error =
            Constants.Math.div2 Type.TFloat  
               (Constants.Math.sum value_a (Constants.Math.neg value_b))
               (Constants.Math.sum value_a value_b)  
         in
            Debug.print "LOG-DBCHECK-TEST" (fun () -> 
               (ListExtras.ocaml_of_list Constants.string_of_const key)^
               " -> "^(Constants.string_of_const value_a)^" // "^
               (Constants.string_of_const value_b)^" ; error: "^
               (Constants.string_of_const error)
            );
            Constants.Math.cmp_gt error (Constants.CFloat(1e-6)) <> 
                                            Constants.CBool(true) 
      ) all_keys
  
   let check_result (db_session : db_session_t)
                    (tlq_results : (string * K3Value.t) list) : unit =
      let i = ref 0 in
      List.iter (fun q -> 
         i := !i + 1;
         let prefix = if List.length !(db_session.queries) > 1 
                      then "QUERY_"^(string_of_int !i)^"_" else "" in
         let (real_query,real_schema) = 
            (* Can we use SqlToCalculus.cast_query_to_aggregate here? *)
            if Sql.is_agg_query q.stmt then
               (q.stmt, q.schema)
            else
               let (targets, sources, condition, gb, opts) = 
                  match q.stmt with
                  | Sql.Select(targets,sources,cond,gb_vars,opts) -> 
                     (targets,sources,cond,gb_vars,opts) 
                  | _ -> 
                     failwith ("DBChecker currently does not support UNION")
               in
                  (Sql.Select( targets @ ["COUNT", 
                        Sql.Aggregate(Sql.CountAgg(
                                      (if List.mem Sql.Select_Distinct opts
                                       then Some([]) else None)),
                                      Sql.Const(Constants.CInt(1)))],
                     sources, condition,
                     List.map (fun (tn,te) -> 
                        (None, tn, Sql.expr_type te !(db_session.tables) 
                                                    sources)) 
                        targets,
                        
                     List.filter (fun x -> x <> Sql.Select_Distinct) opts
                  ), (q.schema @ ["COUNT", Type.TInt]))
         in
         let db_result = I.query !(db_session.client) real_query real_schema in
         let db_hashtbl = to_hashtbl db_result in
         let target_name = 
            let targets = 
               match real_query with
               | Sql.Select(targets,_,_,_,_) -> 
                  targets 
               | _ -> 
                  failwith ("DBChecker currently does not support UNION")
            in
            prefix^(fst (List.nth targets ((List.length targets) - 1)))
         in
            try 
               let k3_result = snd (
                  List.find (fun (name, _) ->
                     name = target_name
                  ) tlq_results
               ) in
               
               let k3_hashtbl = K3Value.to_hashtbl k3_result in
               Debug.print "LOG-DBCHECK-RESULT" (fun () ->
                  "[DBCheck] Result: " ^ (K3Value.to_string k3_result)
               );
               if (not (cmp_hashtbl db_hashtbl k3_hashtbl)) 
               then begin
                  print_hashtbl "========= Expected =========" db_hashtbl;
                  print_hashtbl "========= Result   =========" k3_hashtbl;
                  if not (Debug.active "DBCHECK-CONTINUE-ON-MISMATCH") then
                     failwith "Wrong results"
               end
               else print_string("|"); flush stdout
            with Not_found -> 
               failwith ("Bug in DBChecker: unable to find query "^target_name)
      ) !(db_session.queries)
   
end

module DBAccess = AbstractDBAccess(Postgres)