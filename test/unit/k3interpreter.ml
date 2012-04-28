open UnitTest
open Database
open Patterns
open K3
open K3Compiler
open Values.K3Value
open Types
;;
module Interpreter = K3Compiler.Make(K3Interpreter.K3CG)
;;
let maps = [
   "A", [], [];
   "B", [], [TFloat; TFloat];
]
;;
let pc_a = SingletonPC("A", TBase(TFloat))
let pc_b = OutPC("B", ["X", TBase(TFloat); "Y", TBase(TFloat)], TBase(TFloat))
;;
let patterns = [
   "A", [];
   "B", [Out(["X"], [0])];
]
;;
let db = NamedK3Database.make_empty_db maps patterns
;;
let test ?(env = []) msg code rval =
   let compiled = Interpreter.compile_k3_expr code in
   let (vars, vals) = List.split env in
   log_test ("K3Interpreter("^msg^")")
      Values.K3Value.string_of_value
      (  K3Interpreter.K3CG.eval 
            compiled 
            vars vals
            db)
      rval
in
let test_map ?(env = []) msg code rval =
   let compiled = Interpreter.compile_k3_expr code in
   let (vars, vals) = List.split env in
      log_collection_test ("K3Interpreter("^msg^")")
         (  K3Interpreter.K3CG.eval 
               compiled 
               vars vals
               db)
         (mk_float_collection rval)
in
   test "A simple number"
      (Const(CInt(42)))
      (BaseValue(CInt(42)));
   test "A variable" ~env:["imavar", (CFloat(42.))]
      (Var("imavar", TBase(TFloat)))
      (BaseValue(CFloat(42.)));
   test "A function invocation"
      (Apply((Lambda((AVar("aparam", TBase(TFloat))),
                     (Var("aparam", TBase(TFloat))))),
             (Const(CFloat(42.)))))
      (BaseValue(CFloat(42.)));
   test "An external function invocation - single argument"
      (Apply((ExternalLambda("/",(AVar("aparam", TBase(TFloat))),
                     TBase(TFloat))),
             (Const(CFloat(2.)))))
      (BaseValue(CFloat(0.5)));
   test "An external function invocation - multiple arguments"
      (Apply((ExternalLambda("/",
										 (ATuple(["aparam0", TBase(TFloat);"aparam1", TBase(TFloat)])),
                     TBase(TFloat))),
             (K3.Tuple([Const(CFloat(2.));Const(CFloat(5.))])) ))
      (BaseValue(CFloat(0.4)));
   test "Arithmetic"
      (Add((Mult((Const(CFloat(6.))), (Const(CFloat(9.))))),
           (Const(CFloat(-12.)))))
      (BaseValue(CFloat(42.)));
   test "Blocks"
      (Block[
         (Const(CFloat(37.)));
         (Const(CFloat(69.)));
         (Const(CFloat(42.)))
      ])
      (BaseValue(CFloat(42.)));
   test "Singleton DB Updates"
      (Block[
         PCValueUpdate(pc_a, [], [],
                       (Const(CFloat(42.))));
         pc_a
      ])
      (BaseValue(CFloat(42.)));
   test "Persistence of DB Updates"
      pc_a
      (BaseValue(CFloat(42.)));
   test_map "Mass updating a collection"
      (Block[
         PCValueUpdate(pc_b, [],
            [Const(CFloat(1.)); Const(CFloat(1.))],
            (Const(CFloat(1.))));
         PCValueUpdate(pc_b, [],
            [Const(CFloat(1.)); Const(CFloat(2.))],
            (Const(CFloat(2.))));
         PCValueUpdate(pc_b, [],
            [Const(CFloat(2.)); Const(CFloat(1.))],
            (Const(CFloat(2.))));
         PCValueUpdate(pc_b, [],
            [Const(CFloat(2.)); Const(CFloat(2.))],
            (Const(CFloat(4.))));
         PCValueUpdate(pc_b, [],
            [Const(CFloat(1.)); Const(CFloat(3.))],
            (Const(CFloat(3.))));
         pc_b
      ])
      [  [1.; 3.], 3.;
         [1.; 2.], 2.;
         [2.; 2.], 4.;
         [2.; 1.], 2.;
         [1.; 1.], 1.;
      ];
   test_map "Removing an existing element from a collection"
      (Block[
         PCElementRemove(pc_b, [],
            [Const(CFloat(1.)); Const(CFloat(1.))]);
         pc_b
      ])
      [  [1.; 3.], 3.;
         [1.; 2.], 2.;
         [2.; 2.], 4.;
         [2.; 1.], 2.;
      ];
   test_map "Removing a non-existing element from a collection"
      (Block[
         PCValueUpdate(pc_b, [],
            [Const(CFloat(1.)); Const(CFloat(1.))],
            (Const(CFloat(1.))));
         PCElementRemove(pc_b, [],
            [Const(CFloat(1.)); Const(CFloat(10.))]);
         pc_b
      ])
      [  [1.; 3.], 3.;
         [1.; 2.], 2.;
         [2.; 2.], 4.;
         [2.; 1.], 2.;
         [1.; 1.], 1.;
      ];
   test "Aggregation"
      (Aggregate((AssocLambda((ATuple(["X", TBase(TFloat); 
                                       "Y", TBase(TFloat);
                                       "v", TBase(TFloat)])),
                              (AVar("old", TBase(TFloat))),
                              (Add((Var("v", TBase(TFloat))),
                                   (Var("old", TBase(TFloat))))))),
                 (Const(CFloat(0.))),
                 pc_b))
      (BaseValue(CFloat(12.)));
   test "Aggregation 2"
      (Aggregate((AssocLambda((ATuple(["X", TBase(TFloat); 
                                       "Y", TBase(TFloat);
                                       "v", TBase(TFloat)])),
                              (AVar("old", TBase(TFloat))),
                              (Add(
                                 (Mult((Const(CFloat(-1.))),
                                       (Mult((Var("X", TBase(TFloat))),
                                             (Var("Y", TBase(TFloat))))))),
                                 (Add((Var("v", TBase(TFloat))),
                                      (Var("old", TBase(TFloat))))))))),
                 (Const(CFloat(42.))),
                 pc_b))
      (BaseValue(CFloat(42.)));
   test_map "Slicing"
      (Slice(pc_b, ["X", TBase(TFloat); "Y", TBase(TFloat)], 
                   ["X", (Const(CFloat(1.)))]))
      [  [1.; 3.], 3.;
         [1.; 2.], 2.;
         [1.; 1.], 1.;
      ];
   test_map "Group-By Aggregation"
      (GroupByAggregate((AssocLambda((ATuple(["X", TBase(TFloat); 
                                              "Y", TBase(TFloat);
                                              "v", TBase(TFloat)])),
                                     (AVar("old", TBase(TFloat))),
                                     (Add((Var("v", TBase(TFloat))),
                                          (Var("old", TBase(TFloat))))))),
                        (Const(CFloat(0.))),
                        (Lambda((ATuple(["X", TBase(TFloat); 
                                         "Y", TBase(TFloat);
                                         "v", TBase(TFloat)])),
                                (K3.Tuple[Var("X", TBase(TFloat))]))),
                        (Slice(pc_b, ["X", TBase(TFloat); "Y", TBase(TFloat)], 
                                     []))))
      [  [ 1. ], 6.;
         [ 2. ], 6.;
      ];
   ()
      
            
