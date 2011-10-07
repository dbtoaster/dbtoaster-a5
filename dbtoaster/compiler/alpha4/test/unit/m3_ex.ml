
open Unix

open M3
open M3Common.Patterns
open Util

open M3Compiler
open M3Interpreter
open M3Interpreter.CG
module Compiler = M3Compiler.Make(M3Interpreter.CG)
open Compiler;;

open Values
open Database
open Sources;;

module DB = NamedM3Database

let print_smap name smap =
   String.concat "\n"
      (List.map (fun (key, value) ->
          name^(list_to_string M3Common.string_of_const key)^
          " = "^(M3Common.string_of_const value)) smap)

let print_map name map =
  String.concat "\n"
    (List.map (fun (in_vars, inner_vals) ->
      String.concat "\n"
        (List.map (fun (out_vars, value) ->
          name^(list_to_string M3Common.string_of_const (in_vars@out_vars))^
          " = "^(M3Common.string_of_const value)) inner_vals)) map)

let print_db db = 
  String.concat "\n"
    (List.map (fun (name_l, outer_vals) -> 
      match name_l with [name] -> print_map name outer_vals
      |_ -> "[[Error: map name list lookup failure]]") db);;

(* q[y] = Sum(x, R(x,y)) *)
let prog0: prog_t =
(
[ ("q", [], [VT_Int]) ],
[ (Insert, "R", ["a"; "b"], [ (("q", [], ["b"], (mk_c 0.0,())), (mk_v "a",()), ()) ]) ])
;;

let prepared_prog0 = prepare_triggers (snd prog0);;
let cblock = List.hd (compile_ptrig prepared_prog0);;

let db = DB.make_empty_db (fst prog0) (let (_,pats) = prepared_prog0 in pats);;

patterns_to_string (let (_,pats) = prepared_prog0 in pats);;

(eval_trigger cblock [CFloat(3.0);CFloat(4.0)] db);;
(eval_trigger cblock [CFloat(2.0);CFloat(4.0)] db);;
(eval_trigger cblock [CFloat(3.0);CFloat(4.0)] db);;
(eval_trigger cblock [CFloat(1.0);CFloat(1.0)] db);;

Debug.log_unit_test "Sum Aggregate" print_db
  (DB.show_sorted_db db)
  (
    [(["q"], [([], [([CFloat(1.0)], CFloat(1.0)); ([CFloat(4.0)], CFloat(8.0));])])] 
  );;


(* root map q
    q: Out -> Int     q[][x]   = Sum(1, R(x,y) and R(y,z))
   q1: Out -> Int    q1[][y]   = Sum(1,            R(y,z))
   q2: Out -> Int    q2[][x,y] = Sum(1, R(x,y)           )

   Insert R(a,b):
   [          q[][a] := 0 += q1[][b];
     (for x)  q[][x] := 0 += q2[][x,a];
              q[][a] := 0 += Sum(1, b=a);
             q1[][a] := 0 += 1;
           q2[][a,b] := 0 += 1
   ]

   Delete R(a,b): [ ... ]
*)
let prog1: prog_t =
(
[ ("q",  [], [VT_Int]); ("q1", [], [VT_Int]); ("q2", [], [VT_Int; VT_Int]) ],
[
   (Insert, "R", ["a"; "b"],
      [
      (("q", [], ["a"],       (mk_c 0.0,())),
              (mk_ma false ("q1", [], ["b"], (mk_c 0.0, ())),()),());
      (("q", [], ["x"],       (mk_v "x",())),
              (mk_ma false ("q2", [], ["x"; "a"], (mk_c 0.0, ())),()),());
      (("q", [], ["a"],       (mk_c 0.0,())),
              (mk_if (mk_eq (mk_v "b") (mk_v "a")) (mk_c 1.0),()),());
      (("q1", [], ["a"],      (mk_c 0.0,())), (mk_c 1.0,()),());
      (("q2", [], ["a"; "b"], (mk_c 0.0,())), (mk_c 1.0,()),())
      ])
]);;

(* Code *)
let prepared_prog1 = prepare_triggers (snd prog1);;
let cblock = List.hd (compile_ptrig prepared_prog1);;

let db = DB.make_empty_db (fst prog1) (let (_,x) = prepared_prog1 in x);;

(eval_trigger cblock [CFloat(2.0);CFloat(9.0)] db);;
(eval_trigger cblock [CFloat(4.0);CFloat(3.0)] db);;
(eval_trigger cblock [CFloat(5.0);CFloat(2.0)] db);;
(eval_trigger cblock [CFloat(4.0);CFloat(2.0)] db);;
(eval_trigger cblock [CFloat(3.0);CFloat(5.0)] db);;
(eval_trigger cblock [CFloat(5.0);CFloat(5.0)] db);;
(eval_trigger cblock [CFloat(5.0);CFloat(5.0)] db);;
(eval_trigger cblock [CFloat(5.0);CFloat(4.0)] db);;


Debug.log_unit_test "Selfjoin Sum" (print_smap "q")
  (DB.show_sorted_smap (DB.get_out_map "q" db))
  (
    [([CFloat(2.0)], CFloat(0.0)); ([CFloat(3.0)], CFloat(4.0));
     ([CFloat(4.0)], CFloat(2.0)); ([CFloat(5.0)], CFloat(11.0))]
  );;




(*
    q: Out * Out -> Int         q[][x,w] = Sum(1, R(x,y) and y<z and R(z,w))
   q1:  In * Out -> Int        q1[y][w]  = Sum(1,            y<z and R(z,w)) 
   q2:  In * Out -> Int        q2[z][x]  = Sum(1, R(x,y) and y<z           )
   q3: Out * Out -> Int        q3[][x,y] = Sum(1, R(x,y))

   create table R (a int, b int);

   create view q as
   select r1.a, r2.b, sum(1)
   from r r1, r r2
   where r1.b<r2.a
   group by r1.a, r2.b;

   create view q1 as
   select dom.a as d, r2.b, sum(1)
   from dom, r r2
   where dom.a<r2.a
   group by dom.a, r2.b;

   create view q2 as
   select dom.a as d, r1.a, sum(1)
   from r r1, dom
   where r1.b<dom.a
   group by dom.a, r1.a;

   Insert R(a,b):
   [ (for w) q[][a,w] := Sum_{y,z: y<z} q3[][a,y]*q3[][z,w]
                      += q1[b][w];

     (for x) q[][x,b] := Sum_{y,z: y<z} q3[][x,y]*q3[][z,b]
                       = Sum_{y}        q3[][x,y]*q1[y][b]
                      += q2[a][x];

             q[][a,b] := Sum_{y,z: y<z} q3[][a,y]*q3[][z,b]
                      += Sum(1, b<a);

     (for y) q1[y][b] := Sum(1,            y<z and R(z,b))
                       = Sum_{z: y<z} q3[][z,b]
                      += Sum(1, y<a);

     (for z) q2[z][a] := Sum(1, R(a,y) and y<z           )
                       = Sum_{y: y<z} q3[][a,y]
                      += Sum(1, b<z);

            q3[][a,b] := 0 += Sum(1)
   ]
*)

let init_q x w = mk_if (mk_lt (mk_v "y") (mk_v "z"))
   (mk_prod (mk_ma false ("q3", [], [x; "y"], (mk_c 0.0,())))
            (mk_ma false ("q3", [], ["z"; w], (mk_c 0.0,()))))
;;

let init_q1 y w = mk_if (mk_lt (mk_v y) (mk_v "z"))
                        (mk_ma false ("q3", [], ["z"; w], (mk_c 0.0, ())));;

let init_q2 x z = mk_if (mk_lt (mk_v "y") (mk_v z))
                        (mk_ma false ("q3", [], [x; "y"], (mk_c 0.0, ())));;


let prog2: prog_t =
(
[ ("q",  [],       [VT_Int; VT_Int]);
  ("q1", [VT_Int], [VT_Int]        );
  ("q2", [VT_Int], [VT_Int]        );
  ("q3", [],       [VT_Int; VT_Int]) ],
[
   (Insert, "R", ["a"; "b"],
      [
      (("q", [], ["a"; "w"], (init_q "a" "w",())),
         (mk_ma false ("q1", ["b"], ["w"], (init_q1 "b" "w",())),()),());

      (("q", [], ["x"; "b"], (init_q "x" "b",())),
         (mk_ma false ("q2", ["a"], ["x"], (init_q2 "x" "a",())),()),());

      (("q", [], ["a"; "b"], (init_q "a" "b",())),
         (mk_if (mk_lt (mk_v "b") (mk_v "a")) (mk_c 1.0),()),());

      (("q1", ["y"], ["b"], (init_q1 "y" "b",())),
         (mk_if (mk_lt (mk_v "y") (mk_v "a")) (mk_c 1.0),()),());

      (("q2", ["z"], ["a"], (init_q2 "a" "z",())),
         (mk_if (mk_lt (mk_v "b") (mk_v "z")) (mk_c 1.0),()),());

      (("q3", [], ["a"; "b"], (mk_c 0.0,())), (mk_c 1.0,()),())
      ])
]);;

let prepared_prog2 = prepare_triggers (snd prog2);;
let cblock = List.hd (compile_ptrig prepared_prog2);;

let db = DB.make_empty_db (fst prog2) (let (_,x) = prepared_prog2 in x);;

eval_trigger cblock [CFloat(5.0);CFloat(5.0)] db;;
eval_trigger cblock [CFloat(2.0);CFloat(1.0)] db;;
eval_trigger cblock [CFloat(2.0);CFloat(1.0)] db;;
eval_trigger cblock [CFloat(4.0);CFloat(2.0)] db;;
eval_trigger cblock [CFloat(2.0);CFloat(1.0)] db;;
eval_trigger cblock [CFloat(2.0);CFloat(3.0)] db;;
eval_trigger cblock [CFloat(5.0);CFloat(3.0)] db;;
eval_trigger cblock [CFloat(5.0);CFloat(3.0)] db;;
eval_trigger cblock [CFloat(5.0);CFloat(5.0)] db;;
eval_trigger cblock [CFloat(2.0);CFloat(2.0)] db;;
eval_trigger cblock [CFloat(1.0);CFloat(2.0)] db;;

Debug.log_unit_test "Inequalities" (print_smap "q")
  (DB.show_sorted_smap (DB.get_out_map "q" db))
  (
   [([CFloat(1.0); CFloat(1.0)], CFloat(0.0)); ([CFloat(1.0); CFloat(2.0)], CFloat(1.0));
    ([CFloat(1.0); CFloat(3.0)], CFloat(2.0));  ([CFloat(1.0); CFloat(5.0)], CFloat(2.0));
    ([CFloat(2.0); CFloat(1.0)], CFloat(9.0)); ([CFloat(2.0); CFloat(2.0)], CFloat(8.0));
    ([CFloat(2.0); CFloat(3.0)], CFloat(13.0)); ([CFloat(2.0); CFloat(5.0)], CFloat(10.0));
    ([CFloat(4.0); CFloat(1.0)], CFloat(0.0)); ([CFloat(4.0); CFloat(2.0)], CFloat(1.0));
    ([CFloat(4.0); CFloat(3.0)], CFloat(2.0));  ([CFloat(4.0); CFloat(5.0)], CFloat(2.0));
    ([CFloat(5.0); CFloat(1.0)], CFloat(0.0)); ([CFloat(5.0); CFloat(2.0)], CFloat(2.0));
    ([CFloat(5.0); CFloat(3.0)], CFloat(4.0));  ([CFloat(5.0); CFloat(5.0)], CFloat(4.0));
   ]
  )
;;
(* this is correct according to Postgres *)


(* Simple benchmarking *)
let seed = 12345;;
Random.init seed;;
let randl n lb ub = let r = ref [] in
   for i = 1 to n do r := (CFloat(float(lb + (Random.int (ub-lb))))::!r) done; !r
 
let db = DB.make_empty_db (fst prog2) (let (_,x) = prepared_prog2 in x);;

let num_tuples = 10000 in
let start = Unix.gettimeofday() in
for i = 0 to num_tuples do
   let tuple = randl 2 1 5 in
   (*
   print_endline ((string_of_int i)^": "^
      (List.fold_left (fun acc v -> match v with | CFloat(f) -> acc^" "^(string_of_float f)) "" tuple));
   *)
   eval_trigger cblock tuple db
done;
let finish = Unix.gettimeofday() in
   print_endline ("Tuples: "^(string_of_int num_tuples)^
      " in "^(string_of_float (finish -. start)))
