open Unix

open M3Common
open M3Common.Patterns

open M3Compiler
open M3OCamlgen
open M3OCamlgen.CG

open Expression
open Database
open Sources

module Compiler = M3Compiler.Make(M3OCamlgen.CG)
open Compiler;;

let init_q x w = IfThenElse0( Lt(Var("y"), Var("z")),
   Mult(MapAccess("q3", [], [x; "y"], Const(CFloat(0.0))),
        MapAccess("q3", [], ["z"; w], Const(CFloat(0.0))))
);;

let init_q1 y w = IfThenElse0((Lt(Var(y), Var("z"))),
                     MapAccess("q3", [], ["z"; w], Const(CFloat(0.0))));;

let init_q2 x z = IfThenElse0((Lt(Var("y"), Var(z))),
                     MapAccess("q3", [], [x; "y"], Const(CFloat(0.0))));;

let prog0: prog_t =
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
       IfThenElse0((Lt(Var("b"), Var("a"))), Const(CFloat(1.0))));

      (("q1", ["y"], ["b"], (init_q1 "y" "b")),
       IfThenElse0((Lt(Var("y"), Var("a"))), Const(CFloat(1.0))));

      (("q2", ["z"], ["a"], (init_q2 "a" "z")),
       IfThenElse0((Lt(Var("b"), Var("z"))), Const(CFloat(1.0))));

      (("q3", [], ["a"; "b"], Const(CFloat(0.0))), Const(CFloat(1.0)))
      ])
]);;

let prepared_prog0 = prepare_triggers (snd prog0);;
let schema_prog0 = fst prog0;;
let patterns_prog0 = snd prepared_prog0;;
let ctrigs = compile_ptrig prepared_prog0;;
let out_file = open_out "query.ml" in
   output (main schema_prog0 patterns_prog0 ctrigs) out_file;
   close_out out_file
