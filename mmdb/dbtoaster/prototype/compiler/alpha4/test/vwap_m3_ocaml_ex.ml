open M3Common
open M3Common.Patterns

open M3Compiler
open M3OCaml
open M3OCamlgen
open M3OCamlgen.CG

module Compiler = M3Compiler.Make(M3OCamlgen.CG)
open Compiler;;

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
                 MapAccess("q4", [], [free_var], (Const(CFloat(0.0))))
               )
  with Not_found -> print_string "Not_found in init_q2\n";(Const(CFloat(0.0)));;
  

let cmp_q2 free_var cmp_var cmp_op lhs_offset rhs_offset valid_val = 
  try
    IfThenElse0(
      (cmp_op
        (Mult(Const(CFloat(4.0)), Add(MapAccess("q2", [cmp_var], [], (init_q2 cmp_var free_var)), rhs_offset)))
        (Add(MapAccess("q1", [], [], (Const(CFloat(0.0)))), Mult(Const(CFloat(4.0)), lhs_offset)))
      ),
      valid_val
    )
  with Not_found -> print_string "Not_found in cmp_q\n2";(Const(CFloat(0.0)));;


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
          (("q", [], [], (Const(CFloat(0.0)))), (cmp_q2 "c" "a" (fun x y -> Lt(x, y)) (Const(CFloat(0.0))) (Const(CFloat(0.0))) (Mult(Var("a"), Var("b")))));
          (("q", [], [], (Const(CFloat(0.0)))), 
            (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Const(CFloat(0.0))) (Const(CFloat(0.0)))
              (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Var("b")) (IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")))
                (MapAccess("q3", [], ["d"], (Const(CFloat(0.0)))))
              )
            )
          );
          (("q", [], [], (Const(CFloat(0.0)))), 
            (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Const(CFloat(0.0))) (Const(CFloat(0.0)))
              (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Var("b")) (IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")))
                (Mult((MapAccess("q3", [], ["d"], (Const(CFloat(0.0))))), Const(CFloat(-1.0))))
              )
            )
          );
          
          (("q1", [], [], (Const(CFloat(0.0)))), Var("b"));
          
          (("q2", ["d"], [], (init_q2 "d" "c")), IfThenElse0((Lt(Var("d"), Var("a"))), Var("b")));
          
          (("q3", [], ["a"], (Const(CFloat(0.0)))), Mult(Var("a"), Var("b")));
          
          (("q4", [], ["a"], (Const(CFloat(0.0)))), Var("b"))
        ]
      );

      (Delete, "BID", ["a"; "b"],
        [
          (("q", [], [], (Const(CFloat(0.0)))), (cmp_q2 "c" "a" (fun x y -> Lt(x, y)) (Const(CFloat(0.0))) (Const(CFloat(0.0))) (Mult(Mult(Var("a"), Var("b")), Const(CFloat(-1.0))))));
          (("q", [], [], (Const(CFloat(0.0)))), 
            (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Const(CFloat(0.0))) (Const(CFloat(0.0)))
              (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Mult(Var("b"), Const(CFloat(-1.0)))) (IfThenElse0((Lt(Var("d"), Var("a"))), Mult(Var("b"), Const(CFloat(-1.0)))))
                (MapAccess("q3", [], ["d"], (Const(CFloat(0.0)))))
              )
            )
          );
          (("q", [], [], (Const(CFloat(0.0)))), 
            (cmp_q2 "c" "d" (fun x y -> Lt(x, y)) (Const(CFloat(0.0))) (Const(CFloat(0.0)))
              (cmp_q2 "c" "d" (fun x y -> Leq(y, x)) (Mult(Var("b"), Const(CFloat(-1.0)))) (IfThenElse0((Lt(Var("d"), Var("a"))), Mult(Var("b"), Const(CFloat(-1.0)))))
                (Mult(MapAccess("q3", [], ["d"], (Const(CFloat(0.0)))), Const(CFloat(-1.0))))
              )
            )
          );
          
          (("q1", [], [], (Const(CFloat(0.0)))), Mult(Var("b"), Const(CFloat(-1.0))));
          
          (("q2", ["d"], [], (init_q2 "d" "c")), IfThenElse0((Lt(Var("d"), Var("a"))), Mult(Var("b"), Const(CFloat(-1.0)))));
          
          (("q3", [], ["a"], (Const(CFloat(0.0)))), Mult(Mult(Var("a"), Var("b")), Const(CFloat(-1.0))));
          
          (("q4", [], ["a"], (Const(CFloat(0.0)))), Mult(Var("b"), Const(CFloat(-1.0))))
          
        ]
      )
  ]);;


let prepared_vwap = prepare_triggers (snd prog_vwap);;
let schema_vwap = fst prog_vwap;;
let patterns_vwap = snd prepared_vwap;;
let ctrigs = compile_ptrig prepared_vwap;;
let out_file = open_out "query.ml" in
   output (main schema_vwap patterns_vwap ctrigs) out_file;
   close_out out_file
