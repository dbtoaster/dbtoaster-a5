open Util
open K3OCamlgen
open K3
open K3.SR

;;

let tcode ?(t=K3O.Float) txt = (IP.Leaf("<<"^txt^">>"),t);;
let code_list = List.map tcode ["x";"y";"z"];;
let arg_list = List.map (fun x -> ("<<"^x^">>",K3.SR.TFloat)) ["x";"y";"z"];;

let test_compile = DBTDebug.k3_test_compile

;;

let opt_simp_test ?(extractor = K3.SR.code_of_expr) (name:string) 
                  (test:K3.SR.expr_t) (result:K3.SR.expr_t) =
   
   try
      Debug.log_unit_test
         ("K3 Ocaml Cluster Availability "^name) 
         extractor
         (K3Optimizer.simplify_collections test)
         result
   with Failure(err) -> 
      print_string ("["^err^"]\n");
      Printexc.print_backtrace stdout;
      exit (-1)

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
   opt_simp_test "Map of Flatten" cluster_q (
      (K3.SR.Flatten (
         (K3.SR.Map(
            (K3.SR.Lambda(
               (K3.SR.AVar(
                  "var1",
                  (K3.SR.Collection(
                     K3.SR.TTuple([K3.SR.TFloat;K3.SR.TFloat])
                  ))
               )),
               (K3.SR.Map(
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
                  (K3.SR.Var("var1",
                     (K3.SR.Collection(
                     K3.SR.TTuple([K3.SR.TFloat;K3.SR.TFloat])
                     ))
                  ))
               ))
            )),
            (K3.SR.PC(
               "QUERY_1_1__2",
               [],["A",TFloat],
               TFloat
            ))
         ))
      ))
   )

;;
