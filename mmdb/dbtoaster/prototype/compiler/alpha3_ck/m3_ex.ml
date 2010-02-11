

let prog0: prog_t =
(
[ ("q", [], [VT_Int; VT_Int]) ],
[
   (Insert, "R", ["a"; "b"], [ (("q", [], ["a"; "b"], Const(0)), Var("a")) ])
])
;;

let (_, _, trigger_args, block) = List.hd (snd prog0);;

let q  = list_to_lmap [([], list_to_lmap [])];;
let db:db_t = list_to_smap [("q", q)];;


let db = (eval_trig ["a";"b"] [3;4] db block);;
gshowmap db;;







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
let prog2: prog_t =
(
[ ("q",  [], [VT_Int]); ("q1", [], [VT_Int]); ("q2", [], [VT_Int; VT_Int]) ],
[
   (Insert, "R", ["a"; "b"],
      [
      (("q", [], ["a"],       Const(0)), MapAccess("q1", [], ["b"], Const(0)));
      (("q", [], ["x"],       Var("x")),
       MapAccess("q2", [], ["x"; "a"], Var("x")));
      (("q", [], ["a"],       Const(0)),
              IfThenElse0((Eq(Var("b"), Var("a"))), Const(1)));
      (("q1", [], ["a"],      Const(0)), Const(1));
      (("q2", [], ["a"; "b"], Const(0)), Const(1))
      ])
]);;

let (_, _, _, block) = List.hd (snd prog2);;
let q  = list_to_lmap [([], list_to_lmap [])];;
let q1 = list_to_lmap [([], list_to_lmap [])];;
let q2 = list_to_lmap [([], list_to_lmap [])];;
let db:db_t = list_to_smap [("q", q); ("q1", q1); ("q2", q2)];;

let db = (eval_trig ["a";"b"] [4;2] db block);;
gshowmap db;;

let db = (eval_trig ["a";"b"] [2;2] db block);;
gshowmap db;;










let prog3: prog_t =
(
[ ("q", [], [VT_Int; VT_Int]); ("q1", [], [VT_Int]) ],
[
   (Insert, "R", ["a"; "b"],
   [
      (("q", [], [],     Const(0)), MapAccess("q1", [], ["x"], Const(0)));
                                    (* x is a bigsum variable *)
      (("q1", [], ["a"], Const(0)), Var("a"))
   ])
])
;;

let (_, _, trigger_args, block) = List.hd (snd prog3);;

let q  = list_to_lmap [([], list_to_lmap [])];;
let q1  = list_to_lmap [([], list_to_lmap [])];;
let db:db_t = list_to_smap [("q", q); ("q1", q1)];;

let db = eval_trig ["a";"b"] [1;1] db block;;
gshowmap db;;







(* the root may (query) is q

    q: Out * Out -> Int         q[][x,w] = Sum(1, R(x,y) and y<z and R(z,w))
   q1:  In * Out -> Int        q1[y][w]  = Sum(1,            y<z and R(z,w)) 
   q2:  In * Out -> Int        q2[z][x]  = Sum(1, R(x,y) and y<z           )
   q3: Out * Out -> Int        q3[][x,y] = Sum(1, R(x,y))

   Insert R(a,b):
   [ (for w) q[][a,w] := Sum(1, R(a,y) and y<z and R(z,w))
                      += q1[b][w];

     (for x) q[][x,b] := Sum(1, R(x,y) and y<z and R(z,b))
                      += q2[a][x];

             q[][a,b] := Sum(1, R(a,y) and y<z and R(z,b))
                      += Sum(1, b<a);

     (for y) q1[y][b] := Sum(1,            y<z and R(z,b))
                      += Sum(1, y<a);

     (for z) q2[z][a] := Sum(1, R(a,y) and y<z           )
                       = Sum_{y: y<z} q3[a,y]
                      += Sum(1, b<z);

            q3[][a,b] := 0 += Sum(1)
   ]

   Delete R(a,b): [ ... ]
*)
let prog1: prog_t =
(
[ ("q",  [],       [VT_Int; VT_Int]);
  ("q1", [VT_Int], [VT_Int]        );
  ("q2", [VT_Int], [VT_Int]        );
  ("q3", [],       [VT_Int; VT_Int]) ],
[
   (Insert, "R", ["a"; "b"],
      [
      (("q", [], ["a"; "w"], (Null[])),
       MapAccess("q1", ["b"], ["w"], (Null ["w"])));

      (("q", [], ["x"; "b"], (Null[])),
       MapAccess("q2", ["a"], ["x"], (Null ["x"])));

      (("q", [], ["a"; "b"], (Null[])),
       IfThenElse0((Lt(Var("b"), Var("a"))), Const(1)));

      (("q1", ["y"], ["b"], (Null["b"])),
       IfThenElse0((Lt(Var("y"), Var("a"))), Const(1)));

      (("q2", ["z"], ["a"],
        IfThenElse0((Lt(Var("y"), Var("z"))),
                     MapAccess("q3", [], ["a"; "y"], (Null ["a", "y"])))),
       IfThenElse0((Lt(Var("b"), Var("z"))), Const(1)));

      (("q3", [], ["a"; "b"], Const(0)), Const(1))
      ])
]);;

let (_, _, _, block) = List.hd (snd prog1);;
let db = make_empty_db (fst prog1);;


let db = eval_trig ["a";"b"] [4;2] db block;;
gshowmap db;;

let db = eval_trig ["a";"b"] [1;1] db block;;
gshowmap db;;

let db = eval_trig ["a";"b"] [2;1] db block;;
gshowmap db;;

gshowmap (eval_stmt_loop (make_valuation ["a";"b"] [4;2]) db (List.hd block));;



