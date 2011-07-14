
open M3;;
open M3Eval;;


(* q[y] = Sum(x, R(x,y)) *)
let prog0: prog_t =
(
[ ("q", [], [VT_Int]) ],
[ (Insert, "R", ["a"; "b"], [ (("q", [], ["b"], Const(0)), Var("a")) ]) ])
;;

let (_, _, trigger_args, block) = List.hd (snd prog0);;
let db = make_empty_db (fst prog0);;

let db = (eval_trig ["a";"b"] [3;4] db block);;
let db = (eval_trig ["a";"b"] [2;4] db block);;
let db = (eval_trig ["a";"b"] [3;4] db block);;
let db = (eval_trig ["a";"b"] [1;1] db block);;
gshowmap db = [("q", [([], [([4], 8); ([1], 1)])])] ;;





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
      (("q", [], ["a"],       Const(0)),
       MapAccess("q1", [], ["b"], (Null ["b"])));
      (("q", [], ["x"],       Var("x")),
       MapAccess("q2", [], ["x"; "a"], (Null ["x"; "a"])));
      (("q", [], ["a"],       Const(0)),
              IfThenElse0((Eq(Var("b"), Var("a"))), Const(1)));
      (("q1", [], ["a"],      Const(0)), Const(1));
      (("q2", [], ["a"; "b"], Const(0)), Const(1))
      ])
]);;

let (_, _, _, block) = List.hd (snd prog1);;
let db = make_empty_db (fst prog1);;

let db = (eval_trig ["a";"b"] [2;9] db block);;
let db = (eval_trig ["a";"b"] [4;3] db block);;
let db = (eval_trig ["a";"b"] [5;2] db block);;
let db = (eval_trig ["a";"b"] [4;2] db block);;
let db = (eval_trig ["a";"b"] [3;5] db block);;
let db = (eval_trig ["a";"b"] [5;5] db block);;
let db = (eval_trig ["a";"b"] [5;5] db block);;
let db = (eval_trig ["a";"b"] [5;4] db block);;

lshowmap (StringMap.find "q" db) =
   [([], [([5], 11); ([4], 2); ([3], 4); ([2], 0)])] ;;





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

let init_q x w = IfThenElse0( Lt(Var("y"), Var("z")),
   Mult(MapAccess("q3", [], [x; "y"], Const(0)),
        MapAccess("q3", [], ["z"; w], Const(0)))
);;

let init_q1 y w = IfThenElse0((Lt(Var(y), Var("z"))),
                     MapAccess("q3", [], ["z"; w], Const(0)));;

let init_q2 x z = IfThenElse0((Lt(Var("y"), Var(z))),
                     MapAccess("q3", [], [x; "y"], Const(0)));;


let prog2: prog_t =
(
[ ("q",  [],       [VT_Int; VT_Int]);
  ("q1", [VT_Int], [VT_Int]        );
  ("q2", [VT_Int], [VT_Int]        );
  ("q3", [],       [VT_Int; VT_Int]) ],
[
   (Insert, "R", ["a"; "b"],
      [
      (("q", [], ["a"; "w"], (init_q "a" "w")),
       MapAccess("q1", ["b"], ["w"], (init_q1 "b" "w")));

      (("q", [], ["x"; "b"], (init_q "x" "b")),
       MapAccess("q2", ["a"], ["x"], (init_q2 "x" "a")));

      (("q", [], ["a"; "b"], (init_q "a" "b")),
       IfThenElse0((Lt(Var("b"), Var("a"))), Const(1)));

      (("q1", ["y"], ["b"], (init_q1 "y" "b")),
       IfThenElse0((Lt(Var("y"), Var("a"))), Const(1)));

      (("q2", ["z"], ["a"], (init_q2 "a" "z")),
       IfThenElse0((Lt(Var("b"), Var("z"))), Const(1)));

      (("q3", [], ["a"; "b"], Const(0)), Const(1))
      ])
]);;

let (_, _, _, block) = List.hd (snd prog2);;
(*
let db = make_empty_db_wdom (fst prog2) [1;2;3;4];;
*)
let db = make_empty_db (fst prog2);;
let db = eval_trig ["a";"b"] [5;5] db block;;
let db = eval_trig ["a";"b"] [2;1] db block;;
let db = eval_trig ["a";"b"] [2;1] db block;;
let db = eval_trig ["a";"b"] [4;2] db block;;
let db = eval_trig ["a";"b"] [2;1] db block;;
let db = eval_trig ["a";"b"] [2;3] db block;;
let db = eval_trig ["a";"b"] [5;3] db block;;
let db = eval_trig ["a";"b"] [5;3] db block;;
let db = eval_trig ["a";"b"] [5;5] db block;;
let db = eval_trig ["a";"b"] [2;2] db block;;
let db = eval_trig ["a";"b"] [1;2] db block;;
lshowmap (StringMap.find "q" db) =
[([],
  [([5; 5], 4); ([5; 3], 4); ([5; 2], 2); ([5; 1], 0); ([4; 5], 2);
   ([4; 3], 2); ([4; 2], 1); ([4; 1], 0); ([2; 5], 10); ([2; 3], 13);
   ([2; 2], 8); ([2; 1], 9); ([1; 5], 2); ([1; 3], 2); ([1; 2], 1);
   ([1; 1], 0)])]
;;
(* this is correct according to Postgres *)

