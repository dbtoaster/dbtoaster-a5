open Unix

open M3Common
open M3Common.Patterns

open M3Compiler
open M3OCaml
open M3OCamlgen
open M3OCamlgen.CG

module Compiler = M3Compiler.Make(M3OCamlgen.CG)
open Compiler;;

(* q[y] = Sum(x, R(x,y)) *)
let prog0: prog_t =
(
[ ("q", [], [VT_Int]) ],
[ (Insert, "R", ["a"; "b"], [ (("q", [], ["b"], Const(CFloat(0.0))), Var("a")) ]) ])
;;

let prepared_prog0 = prepare_triggers (snd prog0);;
let schema_prog0 = fst prog0;;
let patterns_prog0 = snd prepared_prog0;;
let ctrigs = compile_ptrig prepared_prog0;;
let out_file = open_out "query.ml" in
   output (main schema_prog0 patterns_prog0 ctrigs) out_file;
   close_out out_file
