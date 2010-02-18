#use "M3.ml";;
#use "M3Eval.ml";;

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


(* try *)
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
                (MapAccess("q3", [], ["d"], (Const(0))))
              )
            )
          );
          
          (("q1", [], [], (Const(0))), Var("b"));
          
          (("q2", ["d"], [], (init_q2 "d" "c")), IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")));
          
          (("q3", [], ["a"], (Const(0))), Mult(Var("a"), Var("b")));
          
          (("q4", [], ["a"], (Const(0))), Var("b"))
        ]
      )
  ]);;
(*with Not_found -> print_string "Not_found in initialization of prog_vwap";;*)

let (_, _, _, block) = List.hd (snd prog_vwap);;
let db = make_empty_db (fst prog_vwap);;
let db = try eval_trig ["a";"b"] [5;5] db block with Not_found -> print_string "Not_found while evaluating trigger\n";db;;
let db = try eval_trig ["a";"b"] [2;4] db block with Not_found -> print_string "Not_found while evaluating trigger\n";db;;
let db = try eval_trig ["a";"b"] [10;3] db block with Not_found -> print_string "Not_found while evaluating trigger\n";db;;
let db = try eval_trig ["a";"b"] [1;7] db block with Not_found -> print_string "Not_found while evaluating trigger\n";db;;
let db = try eval_trig ["a";"b"] [3;8] db block with Not_found -> print_string "Not_found while evaluating trigger\n";db;;
let db = try eval_trig ["a";"b"] [5;10] db block with Not_found -> print_string "Not_found while evaluating trigger\n";db;;
print_string ((db_to_string db)^"\n");;
