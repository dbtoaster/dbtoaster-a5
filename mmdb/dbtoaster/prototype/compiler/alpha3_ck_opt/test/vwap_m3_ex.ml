open M3;;
open M3Eval;;
open Unix;;

(* SELECT avg(b2.price * b2.volume) 
FROM   bids b2
WHERE  k * (select sum(volume) from bids) > (select sum(volume) from bids b1 where b1.price > b2.price)

q = SUM(P1 * V1, R(P1, V1) AND k * SUM(V2, R(P2,V2)) > SUM(V3, R(P3,V3) P3 > P1))

dq/dR(a,b) =  SUM(P1 * V1, (d/dR(a,b)) R(P1, V1) AND k * SUM(V2, R(P2,V2)) > SUM(V3, R(P3,V3) P3 > P1))

           =  SUM(a * b, k * SUM(V2, R(P2,V2)) > SUM(V3, R(P3,V3) P3 > a))
            + SUM(P1 * V1, R(P1, V1) AND (k * SUM(V2, R(P2,V2)) <= SUM(V3, R(P3,V3) P3 > P1)) AND (k * SUM(V2, R(P2,V2))+b  > SUM(V3, R(P3,V3) P3 > P1) + IF a > P1 THEN b))
            - SUM(P1 * V1, R(P1, V1) AND (k * SUM(V2, R(P2,V2))  > SUM(V3, R(P3,V3) P3 > P1)) AND (k * SUM(V2, R(P2,V2))+b <= SUM(V3, R(P3,V3) P3 > P1) + IF a > P1 THEN b))
            + SUM(a  * b , R(a , b ) AND (k * SUM(V2, R(P2,V2)) <= SUM(V3, R(P3,V3) P3 > P1)) AND (k * SUM(V2, R(P2,V2))+b  > SUM(V3, R(P3,V3) P3 > P1) + IF a > a  THEN b))
            - SUM(a  * b , R(a , b ) AND (k * SUM(V2, R(P2,V2))  > SUM(V3, R(P3,V3) P3 > P1)) AND (k * SUM(V2, R(P2,V2))+b <= SUM(V3, R(P3,V3) P3 > P1) + IF a > a  THEN b))

           =  a * b IF k * q1 > q2[a]
            + BIGSUM_P1(q3[P1,V1], (k * q1 <= q2[P1]) AND (k * q1+b  > q2[P1] + IF a > P1 THEN b))
            - BIGSUM_P1(q3[P1,V1], (k * q1  > q2[P1]) AND (k * q1+b <= q2[P1] + IF a > P1 THEN b))


q1 = SUM(V2, R(P2,V2))
q1 := 0
dq1/dR(a,b) = V2

q2[P1] = SUM(V3, R(P3,V3) AND P3 > P1)
q2[P1] := BIGSUM_P3(q4[P3], P3 > P1)
dq2[P1]/dR(a,b) = IF a > P1 THEN b

q3[P1] = SUM(P1 * V1, R(P1, V1))
q3[P1] := 0
dq3[a]/dR(a,b) = a * b

q4[P1] = SUM(V1, R(P1, V1))
q4[P1] := 0
dq4[a]/dR(a,b) = b
*)
let init_q2 cmp_var free_var = 
  try
    IfThenElse0(
                 (
                   Lt(Var(cmp_var), Var(free_var))
                 ),
                 MapAccess("q4", [], [free_var], (Const(0)))
               )
  with Not_found -> print_string "Not_found in init_q2\n";(Const(0));;
  

let cmp_q2 free_var cmp_var cmp_op lhs_offset rhs_offset valid_val = 
  try
    IfThenElse0(
      (cmp_op
        (Mult(Const(4), Add(MapAccess("q2", [cmp_var], [], (init_q2 cmp_var free_var)), rhs_offset)))
        (Add(MapAccess("q1", [], [], (Const(0))), Mult(Const(4), lhs_offset)))
      ),
      valid_val
    )
  with Not_found -> print_string "Not_found in cmp_q\n2";(Const(0));;


let prog_vwap: prog_t = 
  (
  [ ("q",  [],       []       );
    ("q1", [],       []       );
    ("q2", [VT_Int], []       );
    ("q3", [],       [VT_Int] );
    ("q4", [],       [VT_Int] ) ],
  [
      (Insert, "BID", ["a"; "b"],
        [
          (("q", [], [], (Const(0))), (cmp_q2 "c" "a" (fun x y -> Lt(x, y)) (Const(0)) (Const(0)) (Mult(Var("a"), Var("b")))));
          (("q", [], [], (Const(0))), 
            (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Const(0)) (Const(0))
              (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Var("b")) (IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")))
                (MapAccess("q3", [], ["d"], (Const(0))))
              )
            )
          );
          (("q", [], [], (Const(0))), 
            (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Const(0)) (Const(0))
              (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Var("b")) (IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")))
                (Mult((MapAccess("q3", [], ["d"], (Const(0)))), Const(-1)))
              )
            )
          );
          
          (("q1", [], [], (Const(0))), Var("b"));
          
          (("q2", ["d"], [], (init_q2 "d" "c")), IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")));
          
          (("q3", [], ["a"], (Const(0))), Mult(Var("a"), Var("b")));
          
          (("q4", [], ["a"], (Const(0))), Var("b"))
        ]
      );

      (Delete, "BID", ["a"; "b"],
        [
          (("q", [], [], (Const(0))), (cmp_q2 "c" "a" (fun x y -> Lt(x, y)) (Const(0)) (Const(0)) (Mult(Mult(Var("a"), Var("b")), Const(-1)))));
          (("q", [], [], (Const(0))), 
            (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Const(0)) (Const(0))
              (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Mult(Var("b"), Const(-1))) (IfThenElse0((Lt(Var("d"), Var("a"))), Mult(Var("b"), Const(-1))))
                (MapAccess("q3", [], ["d"], (Const(0))))
              )
            )
          );
          (("q", [], [], (Const(0))), 
            (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Const(0)) (Const(0))
              (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Mult(Var("b"), Const(-1))) (IfThenElse0((Lt(Var("d"), Var("a"))), Mult(Var("b"), Const(-1))))
                (Mult(MapAccess("q3", [], ["d"], (Const(0))), Const(-1)))
              )
            )
          );
          
          (("q1", [], [], (Const(0))), Mult(Var("b"), Const(-1)));
          
          (("q2", ["d"], [], (init_q2 "d" "c")), IfThenElse0((Lt(Var("d"), Var("a"))), Mult(Var("b"), Const(-1))));
          
          (("q3", [], ["a"], (Const(0))), Mult(Mult(Var("a"), Var("b")), Const(-1)));
          
          (("q4", [], ["a"], (Const(0))), Mult(Var("b"), Const(-1)))
          
        ]
      )
  ]);;


let seed = 12345;;
Random.init seed;;

let randl n lb ub = let r = ref [] in
   for i = 1 to n do r := ((lb + (Random.int (ub-lb)))::!r) done; !r
in

(* Database *)
let db = Database.make_empty_db (fst prog_vwap) in

(* Blocks *)
let (_,_,_,insert_block) = List.hd (snd prog_vwap) in
let (_,_,_,delete_block) = List.nth (snd prog_vwap) 1 in

(* Handlers *)
let insert t = eval_trig ["a"; "b"] t db insert_block in
let delete t = eval_trig ["a"; "b"] t db delete_block in

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
            acc@[int_of_string(List.nth orders_fields i)]) [] ([4;3])
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
                  let new_tuple = List.map2 (fun v d -> v-d) old_tuple tuple in
                     if (List.nth new_tuple 1) = 0 then Hashtbl.remove book order_id;
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
            begin if do_query then query event_type bids_event old_tuple new_tuple end
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
       output_string rc ((DbMap.nmap_to_string (Database.get_map "q" db))^"\n"))
in    


let random_processor () =
    let num_tuples = 10000 in
    let start = Unix.gettimeofday() in
    for i = 0 to num_tuples do
       let tuple = randl 2 1 10 in
       eval_trig ["a"; "b"] tuple db insert_block;
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

