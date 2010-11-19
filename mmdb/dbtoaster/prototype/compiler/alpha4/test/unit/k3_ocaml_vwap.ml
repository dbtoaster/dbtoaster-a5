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

let test_left = (K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))) in
let test_right = (K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))))),(K3.SR.Const(M3.CFloat(1.))))) in
let test_mult = K3.SR.Mult(test_left,test_right) in
   run_test_type "vwap_mult_left" test_left "float";
   run_test_type "vwap_mult_right" test_right "float";
   run_test_type "vwap_mult_full" test_mult "float"

;;

let test_left = (K3.SR.Lt((K3.SR.Apply((K3.SR.Lambda(K3.SR.AVar("init_val",(K3.SR.TFloat)),(K3.SR.Block([ (K3.SR.PCValueUpdate((K3.SR.InPC("QUERY__2",[ "QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))) ],[  ],(K3.SR.Var("init_val",(K3.SR.TFloat))))); (K3.SR.Var("init_val",(K3.SR.TFloat))) ])))),(K3.SR.Aggregate((K3.SR.AssocLambda(K3.SR.ATuple([ "B2__PRICE",(K3.SR.TFloat); "v1",(K3.SR.TFloat) ]),K3.SR.AVar("accv",(K3.SR.TFloat)),(K3.SR.Add((K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("B2__PRICE",(K3.SR.TFloat))))),(K3.SR.Var("v1",(K3.SR.TFloat))))),(K3.SR.Var("accv",(K3.SR.TFloat))))))),(K3.SR.Const(M3.CFloat(0.))),(K3.SR.Slice((K3.SR.OutPC("QUERY__2_init",[ "B2__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ "B2__PRICE",(K3.SR.TFloat) ],([  ]))))))),(K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.SingletonPC("QUERY__3",(K3.SR.TFloat))))))) in
let test_right = (K3.SR.Leq((K3.SR.Add((K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.SingletonPC("QUERY__3",(K3.SR.TFloat))))),(K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))))))),(K3.SR.Add((K3.SR.Apply((K3.SR.Lambda(K3.SR.AVar("init_val",(K3.SR.TFloat)),(K3.SR.Block([ (K3.SR.PCValueUpdate((K3.SR.InPC("QUERY__2",[ "QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))) ],[  ],(K3.SR.Var("init_val",(K3.SR.TFloat))))); (K3.SR.Var("init_val",(K3.SR.TFloat))) ])))),(K3.SR.Aggregate((K3.SR.AssocLambda(K3.SR.ATuple([ "B2__PRICE",(K3.SR.TFloat); "v1",(K3.SR.TFloat) ]),K3.SR.AVar("accv",(K3.SR.TFloat)),(K3.SR.Add((K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("B2__PRICE",(K3.SR.TFloat))))),(K3.SR.Var("v1",(K3.SR.TFloat))))),(K3.SR.Var("accv",(K3.SR.TFloat))))))),(K3.SR.Const(M3.CFloat(0.))),(K3.SR.Slice((K3.SR.OutPC("QUERY__2_init",[ "B2__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ "B2__PRICE",(K3.SR.TFloat) ],([  ]))))))),(K3.SR.Mult((K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))),(K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))))),(K3.SR.Const(M3.CFloat(1.))))))))))) in
let test_mult = K3.SR.Mult(test_left,test_right) in
   run_test_type "vwap_bool_mult_left" test_left "bool";
   run_test_type "vwap_bool_mult_right" test_right "bool";
   run_test_type "vwap_bool_mult_full" test_mult "bool"

;;

let test_left = (K3.SR.Var("current_v",(K3.SR.TFloat))) in
let test_right_alamb = (K3.SR.AssocLambda(K3.SR.ATuple([ "B1__PRICE",(K3.SR.TFloat); "v1",(K3.SR.TFloat) ]),K3.SR.AVar("accv",(K3.SR.TFloat)),(K3.SR.IfThenElse((K3.SR.Member((K3.SR.InPC("QUERY__2",[ "B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("B1__PRICE",(K3.SR.TFloat))) ])),(K3.SR.Add((K3.SR.Mult((K3.SR.IfThenElse0((K3.SR.Mult((K3.SR.Lt((K3.SR.Lookup((K3.SR.InPC("QUERY__2",[ "B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("B1__PRICE",(K3.SR.TFloat))) ])),(K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.SingletonPC("QUERY__3",(K3.SR.TFloat))))))),(K3.SR.Leq((K3.SR.Add((K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.SingletonPC("QUERY__3",(K3.SR.TFloat))))),(K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))))))),(K3.SR.Add((K3.SR.Lookup((K3.SR.InPC("QUERY__2",[ "B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("B1__PRICE",(K3.SR.TFloat))) ])),(K3.SR.Mult((K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))),(K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))))),(K3.SR.Const(M3.CFloat(1.))))))))))))),(K3.SR.Var("v1",(K3.SR.TFloat))))),(K3.SR.Const(M3.CFloat(-1.))))),(K3.SR.Var("accv",(K3.SR.TFloat))))),(K3.SR.Add((K3.SR.Mult((K3.SR.IfThenElse0((K3.SR.Mult((K3.SR.Lt((K3.SR.Apply((K3.SR.Lambda(K3.SR.AVar("init_val",(K3.SR.TFloat)),(K3.SR.Block([ (K3.SR.PCValueUpdate((K3.SR.InPC("QUERY__2",[ "B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("B1__PRICE",(K3.SR.TFloat))) ],[  ],(K3.SR.Var("init_val",(K3.SR.TFloat))))); (K3.SR.Var("init_val",(K3.SR.TFloat))) ])))),(K3.SR.Aggregate((K3.SR.AssocLambda(K3.SR.ATuple([ "B2__PRICE",(K3.SR.TFloat); "v1",(K3.SR.TFloat) ]),K3.SR.AVar("accv",(K3.SR.TFloat)),(K3.SR.Add((K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("B2__PRICE",(K3.SR.TFloat))))),(K3.SR.Var("v1",(K3.SR.TFloat))))),(K3.SR.Var("accv",(K3.SR.TFloat))))))),(K3.SR.Const(M3.CFloat(0.))),(K3.SR.Slice((K3.SR.OutPC("QUERY__2_init",[ "B2__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ "B2__PRICE",(K3.SR.TFloat) ],([  ]))))))),(K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.SingletonPC("QUERY__3",(K3.SR.TFloat))))))),(K3.SR.Leq((K3.SR.Add((K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.SingletonPC("QUERY__3",(K3.SR.TFloat))))),(K3.SR.Mult((K3.SR.Const(M3.CFloat(0.25))),(K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))))))),(K3.SR.Add((K3.SR.Apply((K3.SR.Lambda(K3.SR.AVar("init_val",(K3.SR.TFloat)),(K3.SR.Block([ (K3.SR.PCValueUpdate((K3.SR.InPC("QUERY__2",[ "B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ (K3.SR.Var("B1__PRICE",(K3.SR.TFloat))) ],[  ],(K3.SR.Var("init_val",(K3.SR.TFloat))))); (K3.SR.Var("init_val",(K3.SR.TFloat))) ])))),(K3.SR.Aggregate((K3.SR.AssocLambda(K3.SR.ATuple([ "B2__PRICE",(K3.SR.TFloat); "v1",(K3.SR.TFloat) ]),K3.SR.AVar("accv",(K3.SR.TFloat)),(K3.SR.Add((K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("B2__PRICE",(K3.SR.TFloat))))),(K3.SR.Var("v1",(K3.SR.TFloat))))),(K3.SR.Var("accv",(K3.SR.TFloat))))))),(K3.SR.Const(M3.CFloat(0.))),(K3.SR.Slice((K3.SR.OutPC("QUERY__2_init",[ "B2__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ "B2__PRICE",(K3.SR.TFloat) ],([  ]))))))),(K3.SR.Mult((K3.SR.Var("QUERY__1BIDS_B1__VOLUME",(K3.SR.TFloat))),(K3.SR.IfThenElse0((K3.SR.Lt((K3.SR.Var("B1__PRICE",(K3.SR.TFloat))),(K3.SR.Var("QUERY__1BIDS_B1__PRICE",(K3.SR.TFloat))))),(K3.SR.Const(M3.CFloat(1.))))))))))))),(K3.SR.Var("v1",(K3.SR.TFloat))))),(K3.SR.Const(M3.CFloat(-1.))))),(K3.SR.Var("accv",(K3.SR.TFloat))))))))) in
let test_right_slice = (K3.SR.Slice((K3.SR.OutPC("QUERY__1",[ "B1__PRICE",(K3.SR.TFloat) ],(K3.SR.TFloat))),[ "B1__PRICE",(K3.SR.TFloat) ],([  ]))) in
let test_right = (K3.SR.Aggregate(test_right_alamb,(K3.SR.Const(M3.CFloat(0.))),test_right_slice)) in
let test_full = K3.SR.Add(test_left,test_right) in
   run_test_type "vwap_add_left" test_left "float";
   run_test_type "vwap_add_right_alamb" test_right_alamb 
                 "fn([float, float], float -> float)";
   run_test_type "vwap_add_right_slice" test_right_slice 
                 "dbentry_map( [float] -> float)";
   run_test_type "vwap_add_right" test_right "float";
   run_test_type "vwap_add_full" test_full "float"


