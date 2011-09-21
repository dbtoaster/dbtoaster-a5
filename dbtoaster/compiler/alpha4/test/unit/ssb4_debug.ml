open M3
open K3
open K3.SR
open Util
;;

(* Debug.activate "K3-TYPECHECK-DETAIL";; *)

let pc_lookup_type = (K3.SR.Lookup((K3.SR.PC("QUERY_1_1_pNATION1_pORDERS1_pLINEITEM1_pCUSTOMER1",["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat],TFloat)),[ (K3.SR.Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",(K3.SR.TFloat))) ]))
;;
Debug.log_unit_test "PC Lookup Type"
   K3.SR.string_of_type
   (K3Typechecker.typecheck_expr pc_lookup_type)
   (Collection(TTuple[TFloat;TFloat;TFloat;TFloat;TFloat]))
;;

let buggy_apply_nesting = 
   (Apply(Lambda(AVar("current_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat])),
          Var("current_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat]))),
        Var("existing_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat]))))
;;

Debug.log_unit_test "Inner Apply Chaining"
   K3.SR.string_of_expr
   (K3Optimizer.inline_collection_functions
      [("existing_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat])),
         Lookup(
            PC("QUERY_1_1_pNATION1_pORDERS1_pCUSTOMER1",["NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;"TYPE",TFloat],TFloat),
            [Var("NATIONKEY",TFloat);
         ])
      ]
      buggy_apply_nesting
   )
   (Lookup(
      PC("QUERY_1_1_pNATION1_pORDERS1_pCUSTOMER1",["NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;"TYPE",TFloat],TFloat),
      [Var("NATIONKEY",TFloat);
   ]))

;;

let buggy_apply_opt = 
   Apply(
      Lambda(
         AVar("existing_slice",
            Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat])),
               Iterate(
                  Lambda(ATuple([
                     "QUERY_1_1NATION_CN__NATIONKEY",TFloat;
                     "QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;
                     "TYPE",TFloat;
                     "updated_v",TFloat]
                  ),
                  PCValueUpdate(
                     PC("QUERY_1_1_pNATION1_pORDERS1_pCUSTOMER1",
                        ["NATIONKEY",TFloat],
                        
                        ["QUERY_1_1NATION_CN__NATIONKEY",TFloat;
                        "QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;
                        "TYPE",TFloat],TFloat
                     ),
[Var("NATIONKEY",TFloat);
          ],[Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
          Var("QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat);
          Var("TYPE",TFloat);],Var("updated_v",TFloat))),
      buggy_apply_nesting)),
  Lookup(
    PC("QUERY_1_1_pNATION1_pORDERS1_pCUSTOMER1",["NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;"TYPE",TFloat],TFloat),
    [Var("NATIONKEY",TFloat);
    ]))
;;

Debug.log_unit_test "Apply Chaining"
   K3.SR.string_of_expr
   (K3Optimizer.optimize ["NATIONKEY"] buggy_apply_opt)
   (
   K3.SR.Iterate(
      (K3.SR.Lambda(
         K3.SR.ATuple(["QUERY_1_1NATION_CN__NATIONKEY",(K3.SR.TFloat); 
                       "QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",
                           (K3.SR.TFloat);
                       "TYPE",(K3.SR.TFloat); 
                       "updated_v",(K3.SR.TFloat) ]),
         (K3.SR.PCValueUpdate(
            (K3.SR.PC(
               "QUERY_1_1_pNATION1_pORDERS1_pCUSTOMER1",
               [ "NATIONKEY",(K3.SR.TFloat) ],
               ["QUERY_1_1NATION_CN__NATIONKEY",TFloat;
                        "QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;
                        "TYPE",TFloat],
               (K3.SR.TFloat))),
            [ (K3.SR.Var("NATIONKEY",(K3.SR.TFloat))) ],
            [ (K3.SR.Var("QUERY_1_1NATION_CN__NATIONKEY",(K3.SR.TFloat)));
              (K3.SR.Var("QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",(K3.SR.TFloat))); 
              (K3.SR.Var("TYPE",(K3.SR.TFloat))) ],
            (K3.SR.Var("updated_v",(K3.SR.TFloat))))))
      ),
      Lookup(
    PC("QUERY_1_1_pNATION1_pORDERS1_pCUSTOMER1",["NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1ORDERS_ORDERS__ORDERKEY",TFloat;"TYPE",TFloat],TFloat),
    [Var("NATIONKEY",TFloat);
    ])
   ))
;;