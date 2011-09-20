open M3
open K3
open K3.SR
open Util
;;

Debug.activate "K3-TYPECHECK-DETAIL"

;;

let pc_lookup_type = (K3.SR.Singleton((K3.SR.Lookup((K3.SR.PC("QUERY_1_1_pNATION1_pORDERS1_pLINEITEM1_pCUSTOMER1",["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat],TFloat)),[ (K3.SR.Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",(K3.SR.TFloat))) ]))))
;;
Debug.log_unit_test "PC Lookup Type"
   K3.SR.string_of_type
   (K3Typechecker.typecheck_expr pc_lookup_type)
   (Collection(TTuple[
      TFloat; 
      Collection(TTuple[TFloat;TFloat;TFloat;TFloat;TFloat])
   ]))
;;

let buggy_stack = 
(Iterate(
     Lambda(ATuple(["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat;"existing_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat ; TFloat])]),
       Iterate(
         Lambda(ATuple(["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"dv",TFloat]),
           IfThenElse(
             Member(
               Var("existing_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat ; TFloat])),
               [Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
               Var("TYPE",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
               ]),
             PCValueUpdate(
               PC("QUERY_1_1_pNATION1_pORDERS1_pLINEITEM1_pCUSTOMER1",["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat],TFloat),
               [Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat);
               ],[Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
               Var("TYPE",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
               ],
               Add(
                 Lookup(
                   Var("existing_slice",Collection(TTuple[TFloat ; TFloat ; TFloat ; TFloat ; TFloat])),
                   [Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
                   Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
                   Var("TYPE",TFloat);
                   Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
                   ]),Var("dv",TFloat))),
             PCValueUpdate(
               PC("QUERY_1_1_pNATION1_pORDERS1_pLINEITEM1_pCUSTOMER1",["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat],TFloat),
               [Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat);
               ],[Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
               Var("TYPE",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
               ],
               Add(
                 Lookup(
                   GroupByAggregate(
                     AssocLambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"SUPPLIER__NAME",TFloat;"SUPPLIER__ADDRESS",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"SUPPLIER__PHONE",TFloat;"SUPPLIER__ACCTBAL",TFloat;"SUPPLIER__COMMENT",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"PART__NAME",TFloat;"MFGR",TFloat;"BRAND",TFloat;"TYPE",TFloat;"SIZE",TFloat;"CONTAINER",TFloat;"RETAILPRICE",TFloat;"PART__COMMENT",TFloat;"QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat;"CN__NAME",TFloat;"CN__REGIONKEY",TFloat;"CN__COMMENT",TFloat;"v",TFloat]),AVar("accv",TFloat),
                       Add(Var("v",TFloat),Var("accv",TFloat))),Const(CFloat(0.)),
                     Lambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"SUPPLIER__NAME",TFloat;"SUPPLIER__ADDRESS",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"SUPPLIER__PHONE",TFloat;"SUPPLIER__ACCTBAL",TFloat;"SUPPLIER__COMMENT",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"PART__NAME",TFloat;"MFGR",TFloat;"BRAND",TFloat;"TYPE",TFloat;"SIZE",TFloat;"CONTAINER",TFloat;"RETAILPRICE",TFloat;"PART__COMMENT",TFloat;"QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat;"CN__NAME",TFloat;"CN__REGIONKEY",TFloat;"CN__COMMENT",TFloat;"v",TFloat]),
                       Tuple[Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
                         Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
                         Var("TYPE",TFloat);
                         Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat)]),
                     Flatten(
                       Map(
                         Lambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"SUPPLIER__NAME",TFloat;"SUPPLIER__ADDRESS",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"SUPPLIER__PHONE",TFloat;"SUPPLIER__ACCTBAL",TFloat;"SUPPLIER__COMMENT",TFloat;"v1",TFloat]),
                           Flatten(
                             Map(
                               Lambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"PART__NAME",TFloat;"MFGR",TFloat;"BRAND",TFloat;"TYPE",TFloat;"SIZE",TFloat;"CONTAINER",TFloat;"RETAILPRICE",TFloat;"PART__COMMENT",TFloat;"var45",TFloat]),
                                 Map(
                                   Lambda(ATuple(["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat;"CN__NAME",TFloat;"CN__REGIONKEY",TFloat;"CN__COMMENT",TFloat;"var43",TFloat]),
                                     Tuple[
                                       Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
                                       Var("SUPPLIER__NAME",TFloat);
                                       Var("SUPPLIER__ADDRESS",TFloat);
                                       Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
                                       Var("SUPPLIER__PHONE",TFloat);
                                       Var("SUPPLIER__ACCTBAL",TFloat);
                                       Var("SUPPLIER__COMMENT",TFloat);
                                       Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
                                       Var("PART__NAME",TFloat);
                                       Var("MFGR",TFloat);Var("BRAND",TFloat);
                                       Var("TYPE",TFloat);Var("SIZE",TFloat);
                                       Var("CONTAINER",TFloat);
                                       Var("RETAILPRICE",TFloat);
                                       Var("PART__COMMENT",TFloat);
                                       Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat);
                                       Var("CN__NAME",TFloat);
                                       Var("CN__REGIONKEY",TFloat);
                                       Var("CN__COMMENT",TFloat);
                                       IfThenElse0(Const(CFloat(1.)),
                                         Mult(Var("v1",TFloat),
                                           Mult(Var("var45",TFloat),
                                             Add(Var("var43",TFloat),
                                               IfThenElse0(
                                                 Eq(
                                                   Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat),
                                                   Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat)),
                                                 Const(CFloat(1.)))))))]),
                                   Slice(OutPC("NATION",["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat;"CN__NAME",TFloat;"CN__REGIONKEY",TFloat;"CN__COMMENT",TFloat],TFloat),["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat;"CN__NAME",TFloat;"CN__REGIONKEY",TFloat;"CN__COMMENT",TFloat],["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",(
                                     Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat));]))),
                               Slice(OutPC("PART",["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"PART__NAME",TFloat;"MFGR",TFloat;"BRAND",TFloat;"TYPE",TFloat;"SIZE",TFloat;"CONTAINER",TFloat;"RETAILPRICE",TFloat;"PART__COMMENT",TFloat],TFloat),["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"PART__NAME",TFloat;"MFGR",TFloat;"BRAND",TFloat;"TYPE",TFloat;"SIZE",TFloat;"CONTAINER",TFloat;"RETAILPRICE",TFloat;"PART__COMMENT",TFloat],["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",(
                                 Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat));"TYPE",(
                                 Var("TYPE",TFloat));])))),
                         Slice(OutPC("SUPPLIER",["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"SUPPLIER__NAME",TFloat;"SUPPLIER__ADDRESS",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"SUPPLIER__PHONE",TFloat;"SUPPLIER__ACCTBAL",TFloat;"SUPPLIER__COMMENT",TFloat],TFloat),["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"SUPPLIER__NAME",TFloat;"SUPPLIER__ADDRESS",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"SUPPLIER__PHONE",TFloat;"SUPPLIER__ACCTBAL",TFloat;"SUPPLIER__COMMENT",TFloat],["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",(
                           Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat));"QUERY_1_1NATION_CN__NATIONKEY",(
                           Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat));])))),
                   [Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
                   Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
                   Var("TYPE",TFloat);
                   Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
                   ]),Var("dv",TFloat))))),
         GroupByAggregate(
           AssocLambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"v",TFloat]),AVar("accv",TFloat),
             Add(Var("v",TFloat),Var("accv",TFloat))),Const(CFloat(0.)),
           Lambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"v",TFloat]),
             Tuple[Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
               Var("TYPE",TFloat);
               Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat)]),
           Flatten(
             Map(
               Lambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"v1",TFloat]),
                 Map(
                   Lambda(ATuple(["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat;"v2",TFloat]),
                     Tuple[
                       Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat);
                       Var("TYPE",TFloat);
                       Var("QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat);
                       Var("QUERY_1_1NATION_CN__NATIONKEY",TFloat);
                       Mult(Var("v1",TFloat),Var("v2",TFloat))]),
                   Slice(OutPC("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pNATION1",["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat],TFloat),["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat;"QUERY_1_1NATION_CN__NATIONKEY",TFloat],[]))),
               Slice(OutPC("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM1",["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat],TFloat),["QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat],[])))))),
     Singleton(
       Lookup(
         PC("QUERY_1_1_pNATION1_pORDERS1_pLINEITEM1_pCUSTOMER1",["QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat],["QUERY_1_1NATION_CN__NATIONKEY",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__PARTKEY",TFloat;"TYPE",TFloat;"QUERY_1_1_pNATION1_pORDERS1LINEITEM_LINEITEM__SUPPKEY",TFloat],TFloat),
         [Var("QUERY_1_1_pCUSTOMER1_pORDERS1_pLINEITEM2_pSUPPLIER1NATION_CN__NATIONKEY",TFloat);
         ]))))

;;
K3Typechecker.typecheck_expr buggy_stack