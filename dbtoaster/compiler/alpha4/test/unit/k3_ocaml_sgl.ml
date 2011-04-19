open Util
open K3OCamlgen
open K3
open K3.SR

;;

let tcode ?(t=K3O.Float) txt = (IP.Leaf("<<"^txt^">>"),t);;
let code_list = List.map tcode ["x";"y";"z"];;
let arg_list = List.map (fun x -> ("<<"^x^">>",K3.SR.TFloat)) ["x";"y";"z"];;

module K3OC = K3Compiler.Make(K3OCamlgen.K3CG)

;;

let test_compile = DBTDebug.k3_test_compile

;;

let run_test ?(extractor = (fun x -> x)) (name:string) (test:K3.SR.expr_t) 
             (result:string) =
   Debug.log_unit_test
      ("K3 Ocaml SGL "^name) 
      (fun x -> x)
      (extractor (K3CG.debug_string (K3OC.compile_k3_expr test)))
      result

;;

let run_test_type =
   run_test ~extractor:(fun x -> String.sub x 0 (String.index x ':'))

;;

let optimize_k3 ?(depth = -1) args e =
   let optimize e = K3Optimizer.simplify_collections 
       (K3Optimizer.lift_ifs args 
         (K3Optimizer.inline_collection_functions [] e)) in
   let rec fixpoint depth e =
       let new_e = optimize e in
       if (e = new_e) || (depth = 0) then e else (fixpoint (depth-1) new_e)
   in fixpoint depth e
;;

let test_collection = (K3.SR.Var("m",(K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat) ])))) ])))))) in
let test_lookup = (K3.SR.Lookup(test_collection,[ (K3.SR.Var("QUERYE1_initE_E1__X",(K3.SR.TFloat))); (K3.SR.Var("QUERYE1_initE_E1__Y",(K3.SR.TFloat))); (K3.SR.Var("QUERYE1_initE_E1__PLAYER",(K3.SR.TFloat))) ])) in
let test_slice = K3.SR.Slice(test_lookup,[ "E2__PLAYER",(K3.SR.TFloat) ],([  ])) in
   run_test_type "nested_collection" test_collection 
      "inline_map( [float, float, float] -> inline_map( [float] -> float))";
   run_test_type "nested_collection_lookup" test_lookup
      "inline_map( [float] -> float)";
   run_test_type "nested_collection_lookup_slice" test_slice
      "inline_map( [float] -> float)"

;;

let test_data = (
   K3.SR.Apply(
      (K3.SR.Lambda(
         K3.SR.AVar("m",(K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat) ])))) ]))))),
         (K3.SR.IfThenElse(
            (K3.SR.Member((K3.SR.Var("m",(K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat) ])))) ])))))),[ (K3.SR.Var("QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat))) ])),
            (K3.SR.Apply(
               (K3.SR.Lambda(
                  K3.SR.AVar("slice",(K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat) ]))))),
                  (K3.SR.Slice((K3.SR.Var("slice",(K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat) ])))))),[ "E2__PLAYER",(K3.SR.TFloat) ],([  ])))
               )),
               (K3.SR.Lookup(
                  (K3.SR.Var("m",(K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.TFloat); (K3.SR.Collection((K3.SR.TTuple([ (K3.SR.TFloat); (K3.SR.TFloat) ])))) ])))))),
                  [ (K3.SR.Var("QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat))) ]
               ))
            )), 
            (K3.SR.Const(M3.CFloat(0.)))))
      )), 
      (K3.SR.PC("QUERY_1_1E1",[ "QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat) ],[ "QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat) ],(K3.SR.TFloat))))) in

let test_results_inline = K3.SR.IfThenElse((K3.SR.Member((K3.SR.PC("QUERY_1_1E1",[ "QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat) ],[ "QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat))) ])),(K3.SR.Slice((K3.SR.Lookup((K3.SR.PC("QUERY_1_1E1",[ "QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat) ],[ "QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat); "QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("QUERY_1_1E1_initE_E1__X",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__Y",(K3.SR.TFloat))); (K3.SR.Var("QUERY_1_1E1_initE_E1__PLAYER",(K3.SR.TFloat))) ])),[ "E2__PLAYER",(K3.SR.TFloat) ],([  ]))),(K3.SR.Const(M3.CFloat(0.)))) in
let test_args = [
   "QUERY_1_1E1_initE_E1__X";
   "QUERY_1_1E1_initE_E1__Y";
   "QUERY_1_1E1_initE_E1__PLAYER"
] in
let test_opt name opt_fn results =
   Debug.log_unit_test 
      ("K3 Ocaml SGL let lifting "^name)
      K3.SR.code_of_expr
      (opt_fn test_data)
      results
in
   test_opt "inline"   
            (K3Optimizer.inline_collection_functions []) 
            test_results_inline;
   test_opt "full"
            (optimize_k3 test_args)
            test_results_inline;
   ()
       (*
       (K3Optimizer.lift_ifs args 
         (K3Optimizer.inline_collection_functions [] e))*)