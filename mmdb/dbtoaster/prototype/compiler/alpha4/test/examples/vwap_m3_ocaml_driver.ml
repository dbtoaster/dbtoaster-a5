let seed = 12345;;
Random.init seed;;

let randl n lb ub = let r = ref [] in
   for i = 1 to n do r := ((CFloat(float(lb + (Random.int (ub-lb)))))::!r) done; !r
in

(* Handlers *)
let insert t = on_insert_BID t in
let delete t = on_delete_BID t in

let exchange_processor input_file query =
   let orders_in = open_in input_file in
   let bids_order_book = Hashtbl.create 10000 in
   let asks_order_book = Hashtbl.create 10000 in
   (* start timing *)
   let tuples = ref 0 in
   let start = Unix.gettimeofday() in
   (try
      while true do
         let orders_line = input_line orders_in in
         let orders_fields = Str.split (Str.regexp (Str.quote ",")) orders_line in
         let order_id = int_of_string(List.nth orders_fields 1) in
         let event_type = List.nth orders_fields 2 in
         let tuple = List.fold_left (fun acc i ->
            acc@[CFloat(float_of_string(List.nth orders_fields i))]) [] ([4;3])
         in
         incr tuples;
         let (do_query, bids_event, old_tuple, new_tuple) = 
            match event_type with
            | "B" ->
               Hashtbl.replace bids_order_book order_id tuple; 
               (true, true, [], tuple)

            | "S" ->
               Hashtbl.replace asks_order_book order_id tuple;
               (true, false, [], tuple)

            | "E" ->
               let in_bids = Hashtbl.mem bids_order_book order_id in
               let in_asks = not(in_bids) && (Hashtbl.mem asks_order_book order_id) in
               begin if not(in_bids || in_asks) then
                  (false, false, [], [])
               else
                  let book = if in_bids then bids_order_book else asks_order_book in
                  let old_tuple = Hashtbl.find book order_id in
                  let new_tuple = List.map2 (fun v d ->
                     match d with
                        (*| CInt(x) -> c_sum v (CInt(-x))*)
                        | CFloat(x) -> c_sum v (CFloat(-.x)))
                     old_tuple tuple
                  in
                     if (List.nth new_tuple 1) = CFloat(0.0) then Hashtbl.remove book order_id;
                     (true, in_bids, old_tuple, new_tuple)
               end
               
               
            | "F" | "D" ->
               let in_bids = Hashtbl.mem bids_order_book order_id in
               let in_asks = not(in_bids) && (Hashtbl.mem asks_order_book order_id) in
               begin if not(in_bids || in_asks) then
                  (false, false, [], [])
               else
                  let book = if in_bids then bids_order_book else asks_order_book in
                  let r = (true, in_bids, [], Hashtbl.find book order_id) in
                     Hashtbl.remove book order_id; r
               end

            | "X" | "C" | "T" ->
            (* Do nothing with these for now... *) (false, false, [], [])
            
            | _ -> failwith "Invalid exchange message"
         in
            begin if do_query then
               query event_type bids_event old_tuple new_tuple;
               if ((!tuples) mod 10000) = 0 then 
                  print_endline ("Processed "^(string_of_int (!tuples))^" tuples.");
            end
      done;

      (* should never reach here... *)
      failwith "I/O exception"
    with End_of_file ->
      close_in orders_in);
   (* finish timing *)
   let finish = Unix.gettimeofday() in
   print_endline ("Tuples: "^(string_of_int (!tuples))^
       " in "^(string_of_float (finish -. start)))
in

let vwap_query result_chan_opt
               vwap_on_bids event_type bids_event old_tuple new_tuple
   =
   (match event_type with
    | "B" -> begin if vwap_on_bids then insert new_tuple end
    | "S" -> begin if not(vwap_on_bids) then insert new_tuple end
    | "E" ->
       begin if vwap_on_bids = bids_event then
          (delete old_tuple; insert new_tuple)
      end
    | "F" | "D" -> begin if vwap_on_bids = bids_event then delete new_tuple end
    | "X" | "C" | "T" -> (* Do nothing here for now... *) ()
    | _ -> failwith "Invalid exchange message");
   (* TODO: output query results... *)
   (match result_chan_opt with
    | None -> ()
    | Some(rc) ->
       output_string rc ((Database.dbmap_to_string (Database.get_map "q" db))^"\n"))
in    


let random_processor () =
    let num_tuples = 10000 in
    let start = Unix.gettimeofday() in
    for i = 0 to num_tuples do
       let tuple = randl 2 1 10 in
       insert_trig tuple
    done;
    let finish = Unix.gettimeofday() in
    print_endline ("Tuples: "^(string_of_int num_tuples)^
       " in "^(string_of_float (finish -. start)))
    (* Database.show_sorted_db db; *)
in

let main () =
   begin if Array.length (Sys.argv) < 2 then
      random_processor()
   else
      let result_chan = if (Array.length Sys.argv) > 2
         then Some(open_out (Array.get Sys.argv 2)) else None
      in
      let query_handler = vwap_query result_chan true in
         exchange_processor (Array.get Sys.argv 1) query_handler
   end
in

main();;

