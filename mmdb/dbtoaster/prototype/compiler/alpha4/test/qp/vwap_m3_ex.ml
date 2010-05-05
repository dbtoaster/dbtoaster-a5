open M3
open M3Common
open M3Common.Patterns

open M3Compiler
open M3Interpreter
open M3Interpreter.CG

module Compiler = M3Compiler.Make(M3Interpreter.CG)
open Compiler;;

open Expression
open Database
open Sources;;

module DB = NamedDatabase

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
            + BIGSUM_P1(q3[P1], (k * q1 <= q2[P1]) AND (k * q1+b  > q2[P1] + IF a > P1 THEN b))
            - BIGSUM_P1(q3[P1], (k * q1  > q2[P1]) AND (k * q1+b <= q2[P1] + IF a > P1 THEN b))


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
    mk_if
       (mk_lt (mk_v(cmp_var)) (mk_v(free_var)))
       (mk_ma ("QUERY_1_1__2_init", [], [free_var], (mk_c 0.0, ())))
  with Not_found -> print_string "Not_found in init_q2\n"; (mk_c 0.0);;
  

let cmp_q2 free_var cmp_var cmp_op lhs_offset rhs_offset valid_val = 
  try
    mk_if
       (cmp_op
          (mk_prod
             (mk_c 4.0)
             (mk_sum
                (mk_ma ("QUERY_1_1__2", [cmp_var], [], (init_q2 cmp_var free_var, ())))
                (rhs_offset)))
          (mk_sum
             (mk_ma ("QUERY_1_1__3", [], [], (mk_c 0.0, ())))
             (lhs_offset)))
       (valid_val)
  with Not_found -> print_string "Not_found in cmp_q\n2"; (mk_c 0.0);;

let prog_vwap: prog_t = 
  (
  [ ("QUERY_1_1",         [],       []       );     (* q  *)
    ("QUERY_1_1__3",      [],       []       );     (* q1 *)
    ("QUERY_1_1__2",      [VT_Int], []       );     (* q2 *)
    ("QUERY_1_1__1",      [],       [VT_Int] );     (* q3 *)
    ("QUERY_1_1__2_init", [],       [VT_Int] ) ],   (* q4 *)
  [
      (Insert, "BID", ["a"; "b"],
        [
          (* q[][] += if 4*q2[a][] < q1[][] then
                        (if q1[][]+b <= 4*q2[a][] then a*b*-1.0 else 0)
                      else 0
             q2[a][] := if a < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "a" (fun x y -> mk_lt x y) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "a" (fun x y -> mk_leq y x)
                (mk_v "b") (mk_c 0.0)
                (mk_prod (mk_prod (mk_v "a") (mk_v "b")) (mk_c (-1.0)))
              )
            ), ()),
            ()
          );

          (* q[][] += if q1[][] <= 4*q2[a][] then
                        (if 4*q2[a][] < q1[][]+b then a*b else 0)
                      else 0
             q2[a][] := if a < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "a" (fun x y -> mk_leq y x) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "a" (fun x y -> mk_lt x y) (mk_v "b") (mk_c 0.0)
                (mk_prod (mk_v "a") (mk_v "b"))
              )
            ), ()),
            ()
          );

          (* q[][] += if 4*q2[d][] < q1[][] then
                        (if q1[][]+b <= 4*(q2[d][]+(if d < a then b else 0))
                         then q3[][d]*-1.0 else 0)
                      else 0
             q2[d][] := if d < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "d" (fun x y -> mk_lt x y) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "d" (fun x y -> mk_leq y x)
                (mk_v "b") (mk_if (mk_lt (mk_v "d") (mk_v "a")) (mk_v "b"))
                (mk_prod
                  (mk_ma("QUERY_1_1__1", [], ["d"], (mk_c 0.0, ())))
                  (mk_c (-1.0)))
              )
            ), ()),
            ()
          );

          (* q[][] += if q1[][] <= 4*q2[d][] then
                        (if 4*(q2[d][]+(if d < a then b else 0)) < q1[][]+b
                         then q3[][d] else 0)
                      else 0
             q2[d][] := if d < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "d"
              (fun x y -> mk_leq y x) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "d" (fun x y -> mk_lt x y) (mk_v "b")
                (mk_if (mk_lt (mk_v "d") (mk_v "a")) (mk_v "b"))
                (mk_ma ("QUERY_1_1__1", [], ["d"], (mk_c 0.0, ())))
              )
            ), ()),
            ()
          );
          
          (* q[][]   += if 4*q2[a][] < q1[][] then a*b else 0
             q2[a][] := if a < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())),
            ((cmp_q2 "c" "a"
              (fun x y -> mk_lt x y) (mk_c 0.0) (mk_c 0.0)
              (mk_prod (mk_v "a") (mk_v "b"))), ()),
            ()
          );

          (* q1[][] += b *)
          (("QUERY_1_1__3", [], [], (mk_c 0.0, ())), ((mk_v "b"), ()), ());
          
          (* q2[d][] := if d < c then q4[][c] else 0
             q2[d][] += if d < a then b else 0 *)
          (("QUERY_1_1__2", ["d"], [], (init_q2 "d" "c", ())), ((mk_if (mk_lt (mk_v "d") (mk_v "a")) (mk_v "b")), ()), ());

          (* q4[][a] += b *)
          (("QUERY_1_1__2_init", [], ["a"], (mk_c 0.0, ())), ((mk_v "b"), ()), ());
          
          (* q3[][a] += a*b *)
          (("QUERY_1_1__1", [], ["a"], (mk_c 0.0, ())), ((mk_prod (mk_v "a") (mk_v "b")), ()), ());
        ]
      );

      (Delete, "BID", ["a"; "b"],
        [
          (* q[][] += if 4*q2[a][] < q1[][] then
                        (if q1[][]+(b*-1.0) <= 4*q2[a][] then a*b*-1.0 else 0)
                      else 0
             q2[a][] := if a < c then q4[][c] else 0
          *)          
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "a" (fun x y -> mk_lt x y) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "a" (fun x y -> mk_leq y x)
                (mk_prod (mk_v "b") (mk_c (-1.0))) (mk_c 0.0)
                (mk_prod (mk_prod (mk_v "a") (mk_v "b")) (mk_c (-1.0)))
              )
            ), ()),
            ()
          );

          (* q[][] += if q1[][] <= 4*q2[a][] then
                        (if 4*q2[a][] < q1[][]+(b*-1.0) then a*b else 0)
                      else 0
             q2[a][] := if a < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "a" (fun x y -> mk_leq y x) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "a" (fun x y -> mk_lt x y)
                (mk_prod (mk_v "b") (mk_c (-1.0))) (mk_c 0.0)
                (mk_prod (mk_v "a") (mk_v "b"))
              )
            ), ()),
            ()
          );

          (* q[][] += if 4*q2[d][] < q1[][] then
                        (if q1[][]+(b*-1.0) <= 4*(q2[d][]+(if d < a then b*-1.0 else 0))
                         then q3[][d]*-1.0 else 0)
                      else 0
             q2[d][] := if d < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "d" (fun x y -> mk_lt x y) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "d" (fun x y -> mk_leq y x)
                (mk_prod (mk_v "b") (mk_c (-1.0)))
                (mk_if (mk_lt (mk_v "d") (mk_v "a")) (mk_prod (mk_v "b") (mk_c (-1.0))))
                (mk_prod
                  (mk_ma ("QUERY_1_1__1", [], ["d"], (mk_c 0.0, ())))
                  (mk_c (-1.0)))
              )
            ), ()),
            ()
          );

          (* q[][] += if q1[][] <= 4*q2[d][] then
                        (if 4*(q2[d][]+(if d < a then b*-1.0 else 0)) < q1[][]+(b*-1.0)
                         then q3[][d] else 0)
                      else 0
             q2[d][] := if d < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())), 
            ((cmp_q2 "c" "d" (fun x y -> mk_leq y x) (mk_c 0.0) (mk_c 0.0)
              (cmp_q2 "c" "d" (fun x y -> mk_lt x y)
                (mk_prod (mk_v "b") (mk_c (-1.0)))
                (mk_if (mk_lt (mk_v "d") (mk_v "a")) (mk_prod (mk_v "b") (mk_c (-1.0))))
                (mk_ma ("QUERY_1_1__1", [], ["d"], (mk_c 0.0, ())))
              )
            ), ()),
            ()
          );
          
          (* q[][] += if 4*q2[a][] < q1[][] then a*b*-1.0 else 0
             q2[a][] := if a < c then q4[][c] else 0
          *)
          (("QUERY_1_1", [], [], (mk_c 0.0, ())),
            ((cmp_q2 "c" "a" (fun x y -> mk_lt x y) (mk_c 0.0) (mk_c 0.0)
              (mk_prod (mk_prod (mk_v "a") (mk_v "b")) (mk_c (-1.0)))), ()),
            ()
          );
          
          (* q1[][] += b*-1.0 *)
          (("QUERY_1_1__3", [], [], (mk_c 0.0, ())), ((mk_prod (mk_v "b") (mk_c (-1.0))), ()), ());
          
          (* q2[d][] := if d < c then q4[][c] else 0
             q2[d][] += if d < a then b*-1.0 else 0 *)
          (("QUERY_1_1__2", ["d"], [], (init_q2 "d" "c", ())), ((mk_if (mk_lt (mk_v "d") (mk_v "a")) (mk_prod (mk_v "b") (mk_c (-1.0)))), ()), ());

          (* q4[][a] += b*-1.0 *)
          (("QUERY_1_1__2_init", [], ["a"], (mk_c 0.0, ())), ((mk_prod (mk_v "b") (mk_c (-1.0))), ()), ());

          (* q3[][a] += a*b*-1.0 *)
          (("QUERY_1_1__1", [], ["a"], (mk_c 0.0, ())), ((mk_prod (mk_prod (mk_v "a") (mk_v "b")) (mk_c (-1.0))), ()), ());
          
        ]
      )
  ]);;

type output_level = Database | Map | Value;;

let seed = 12345;;
Random.init seed;;

let randl n lb ub = let r = ref [] in
   for i = 1 to n do r := ((CFloat(lb +. (Random.float (ub-.lb))))::!r) done; !r
in

(* Code *)
let prepared_vwap = prepare_triggers (snd prog_vwap) in
let vwap_triggers = compile_ptrig prepared_vwap in
let insert_trig = List.hd vwap_triggers in
let delete_trig = List.nth vwap_triggers 1 in

(* Database *)
let db = DB.make_empty_db (fst prog_vwap) (let (_,pats) = prepared_vwap in pats) in

(* Handlers *)
let insert t = eval_trigger insert_trig t db in
let delete t = eval_trigger delete_trig t db in

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

let vwap_query validate_mode output_level result_chan_opt
               vwap_on_bids event_type bids_event old_tuple new_tuple
   =
   let output = match event_type with
    | "B" -> begin if vwap_on_bids then insert new_tuple end; vwap_on_bids
    | "S" -> begin if not(vwap_on_bids) then insert new_tuple end; not(vwap_on_bids)
    | "E" ->
       begin if not(validate_mode) && vwap_on_bids = bids_event then
          (delete old_tuple; insert new_tuple)
       end;
       not(validate_mode) && vwap_on_bids = bids_event
    | "F" | "D" ->
       begin if vwap_on_bids = bids_event then delete new_tuple end;
       vwap_on_bids = bids_event
    | "X" | "C" | "T" -> (* Do nothing here for now... *) false
    | _ -> failwith "Invalid exchange message"
   in
      (match (output, result_chan_opt) with
       | (false, _) | (_, None) -> ()
       | (true, Some(rc)) ->
          begin match output_level with
           | Database -> output_string rc ((DB.db_to_string db)^"\n")
           | Map      ->
              let r = match DB.get_value "QUERY_1_1" db with
                 | Some(x) -> ValuationMap.from_list [([], ValuationMap.from_list [([], x)] [])] []
                 | None -> ValuationMap.from_list [([], ValuationMap.from_list [([], CFloat(0.0))] [])] []
              in output_string rc ((DB.map_to_string r)^"\n")
           | Value    ->
              let r = match DB.get_value "QUERY_1_1" db with
                 | Some(x) -> x
                 | None -> CFloat(0.0)
              in output_string rc ((AggregateMap.string_of_aggregate r)^"\n")
          end)
in

let random_processor () =
    let num_tuples = 10000 in
    let start = Unix.gettimeofday() in
    for i = 0 to num_tuples do
       let tuple = randl 2 1.0 10.0 in
       eval_trigger insert_trig tuple db
    done;
    let finish = Unix.gettimeofday() in
    print_endline ("Tuples: "^(string_of_int num_tuples)^
       " in "^(string_of_float (finish -. start)))
    (* DB.show_sorted_db db; *)
in

let main () =
   let validation = ref false in
   let result_level = ref Value in
   let input_file = ref "" in
   let output_file = ref "" in
   let set_validate () = validation := true in
   let set_output_level l = match l with
      | "db"       -> result_level := Database
      | "map"      -> result_level := Map
      | "value"    -> result_level := Value
      | _ -> failwith "invalid output level"
   in
   let set_input_output s =
      match (!input_file, !output_file) with
       | ("", _) -> input_file := s
       | (_, "") -> output_file := s
       | _ -> ()
   in
   let usage = "vwap [-validate] [-output <db|map|value>] input [output]" in
   let arg_spec =
      [("-validate", Arg.Unit(set_validate), "enables validation mode");
       ("-output", Arg.Symbol(["db"; "map"; "value"], set_output_level),
          "sets the output level to one of <db|map|value>")]
   in
      Arg.parse arg_spec set_input_output usage;
      if (!input_file = "") then random_processor()
      else
      let result_chan = if !output_file = "" then None 
                        else Some(open_out !output_file) in
      let query_handler = vwap_query !validation !result_level result_chan true
      in exchange_processor !input_file query_handler
in

main();;

