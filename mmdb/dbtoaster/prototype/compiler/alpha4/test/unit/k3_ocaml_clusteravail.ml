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
      ("K3 Ocaml VWAP "^name) 
      (fun x -> x)
      (extractor (K3CG.debug_string (K3OC.compile_k3_expr test)))
      result

;;

let run_test_type =
   run_test ~extractor:(fun x -> String.sub x 0 (String.index x ':'))

;;

let cluster_q = 
   K3.SR.Map(
      (K3.SR.Lambda(
         K3.SR.ATuple([ 
            "QUERY_1_1ASSIGNMENT1SERVER_STATUS",(K3.SR.TFloat); 
            "QUERY_1_1ASSIGNMENT1SERVER_SSID",  (K3.SR.TFloat) 
         ]),
         K3.SR.Tuple([ 
            K3.SR.Var("QUERY_1_1ASSIGNMENT1SERVER_STATUS",(K3.SR.TFloat));
            K3.SR.Var("TTID",(K3.SR.TFloat))
         ])
      )),
      (K3.SR.Flatten(
         K3.SR.PC(
            "QUERY_1_1__2",
            [],["A",TFloat],
            TFloat
         )
      ))
   )
 in
   try
      K3Optimizer.simplify_collections cluster_q
   with Failure(err) -> 
      print_string ("["^err^"]\n");
      Printexc.print_backtrace stdout;
      exit (-1)

