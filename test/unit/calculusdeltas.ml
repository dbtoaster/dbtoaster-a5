open Schema
open Calculus
open Type
open UnitTest
;;
Debug.activate "PARSE-CALC-WITH-FLOAT-VARS";
;;
let test ?(opt_in=false) ?(opt_out=false) ?(simplify_domains=true)
         msg event input output = 
   let input_calc = parse_calc input in
   let map_schema = (event_vars event, snd (schema_of_expr input_calc)) in
   log_test ("Delta of Expression ( "^msg^" : "^(string_of_event event)^" )")
      CalculusPrinter.string_of_expr
      ((fun x -> 
           if not opt_out && not simplify_domains then x else 
           let opts = 
              if opt_out then CalculusTransforms.default_optimizations 
              else  [ CalculusTransforms.OptSimplifyDomains ]
           in
              CalculusTransforms.optimize_expr ~optimizations:opts map_schema x)
         (CalculusDeltas.delta_of_expr 
            event 
            (if opt_in 
             then CalculusTransforms.optimize_expr map_schema input_calc
             else input_calc))
      )
      (parse_calc output)
;;

test "TPCH17 simple" (InsertEvent(schema_rel "L" ["dPK"; "dQTY"]))
   "AggSum([], P(PK) * L(PK, QTY) * (nested ^= AggSum([PK], 
       L(PK, QTY2) * QTY2)*0.5))"
   "AggSum([], P(PK) * (((PK ^= dPK) * (QTY ^= dQTY) * 
       (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)*0.5)) + 
       ((L(PK, QTY) + (PK ^= dPK) * (QTY ^= dQTY)) * 
        (PK ^= dPK) * (
            (nested ^= (AggSum([PK], L(PK, QTY2) * QTY2)*0.5) + 
                (AggSum([], (QTY2 ^= dQTY)* QTY2) * 0.5)) - 
            (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)*0.5)
   ))))"
;;

test "TPCH11" (InsertEvent(schema_rel "PARTSUPP" ["dPK"; "dSK"; "dAQ"; "dSC"]))
   "AggSum([P_NATIONKEY, P_PARTKEY], 
       ((N_VALUE ^=
           AggSum([P_NATIONKEY], 
              (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
               SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                        S_ACCTBAL, S_COMMENT) *
               (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) *
        (P_VALUE ^=
           AggSum([P_PARTKEY, P_NATIONKEY], 
              (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
               (P_PARTKEY ^= PS_PARTKEY) *
               SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                        S_ACCTBAL, S_COMMENT) *
               (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) *
        AggSum([], 
           ((__sql_inline_agg_1 ^= (0.001 * N_VALUE)) *
            {P_VALUE > __sql_inline_agg_1})) *
        P_VALUE))"
   "AggSum([P_NATIONKEY, P_PARTKEY], 
       ((( 
          ((N_VALUE ^=
               (AggSum([P_NATIONKEY], 
                   (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, 
                             PS_SUPPLYCOST) *
                    SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                             S_PHONE, S_ACCTBAL, S_COMMENT) *
                   (P_NATIONKEY ^= S_NATIONKEY) * 
                   PS_SUPPLYCOST * PS_AVAILQTY)) +
                AggSum([P_NATIONKEY], 
                   (PS_SUPPLYCOST ^= dSC) *
                   (PS_AVAILQTY ^= dAQ) *
                   (PS_SUPPKEY ^= dSK) *
                   (PS_PARTKEY ^= dPK) *                     
                   (SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                             S_PHONE, S_ACCTBAL, S_COMMENT) *
                   (P_NATIONKEY ^= S_NATIONKEY) * 
                   PS_SUPPLYCOST * PS_AVAILQTY)))) +
           (-1 *
            (N_VALUE ^= AggSum([P_NATIONKEY], 
                (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
                 SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                          S_PHONE, S_ACCTBAL, S_COMMENT) *
                 (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST *
                 PS_AVAILQTY)))))) *
         (P_VALUE ^=
             AggSum([P_PARTKEY, P_NATIONKEY], 
                (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
                (P_PARTKEY ^= PS_PARTKEY) *
                SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                         S_ACCTBAL, S_COMMENT) *
                (P_NATIONKEY ^= S_NATIONKEY) * 
                PS_SUPPLYCOST * PS_AVAILQTY))) *
         AggSum([], 
            ((__sql_inline_agg_1 ^= (0.001 * N_VALUE)) *
             {P_VALUE > __sql_inline_agg_1})) *
         P_VALUE) +
        (((N_VALUE ^= AggSum([P_NATIONKEY], 
              (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
               SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                        S_ACCTBAL, S_COMMENT) *
               (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) +
          (((N_VALUE ^=
                (AggSum([P_NATIONKEY], 
                   (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, 
                             PS_SUPPLYCOST) *
                    SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                             S_PHONE, S_ACCTBAL, S_COMMENT) *
                    (P_NATIONKEY ^= S_NATIONKEY) * 
                    PS_SUPPLYCOST * PS_AVAILQTY)) +
                 AggSum([P_NATIONKEY], 
                    (PS_SUPPLYCOST ^= dSC) *
                    (PS_AVAILQTY ^= dAQ) * 
                    (PS_SUPPKEY ^= dSK) *
                    (PS_PARTKEY ^= dPK) *                     
                    (SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, 
                              S_PHONE, S_ACCTBAL, S_COMMENT) *
                    (P_NATIONKEY ^= S_NATIONKEY) * 
                    PS_SUPPLYCOST * PS_AVAILQTY)))) +
            (-1 *
             (N_VALUE ^=
                 AggSum([P_NATIONKEY], 
                    (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, 
                              PS_SUPPLYCOST) *
                     SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                              S_PHONE, S_ACCTBAL, S_COMMENT) *
                     (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST *
                     PS_AVAILQTY))))))) *
          DOMAIN( 
            AggSum([P_PARTKEY], 
              ((PS_PARTKEY ^= dPK) * (P_PARTKEY ^= PS_PARTKEY)))) *
          (((P_VALUE ^=
                (AggSum([P_PARTKEY, P_NATIONKEY], 
                    (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY,
                              PS_SUPPLYCOST) *
                     (P_PARTKEY ^= PS_PARTKEY) *
                     SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                                S_PHONE, S_ACCTBAL, S_COMMENT) *
                     (P_NATIONKEY ^= S_NATIONKEY) * 
                     PS_SUPPLYCOST * PS_AVAILQTY)) +
                 AggSum([P_PARTKEY, P_NATIONKEY], 
                    (PS_SUPPLYCOST ^= dSC) *
                    (PS_AVAILQTY ^= dAQ) * 
                    (PS_SUPPKEY ^= dSK) *
                    (PS_PARTKEY ^= dPK) *
                    (P_PARTKEY ^= PS_PARTKEY) *
                    (SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, 
                              S_PHONE, S_ACCTBAL, S_COMMENT) *
                    (P_NATIONKEY ^= S_NATIONKEY) * 
                    PS_SUPPLYCOST * PS_AVAILQTY)))) +
            (-1 *
             (P_VALUE ^=
                 AggSum([P_PARTKEY, P_NATIONKEY], 
                    (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY,
                              PS_SUPPLYCOST) *
                    (P_PARTKEY ^= PS_PARTKEY) *
                    SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                             S_PHONE, S_ACCTBAL, S_COMMENT) *
                    (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST *
                    PS_AVAILQTY)))))) *
        AggSum([], 
           ((__sql_inline_agg_1 ^= (0.001 * N_VALUE)) *
            {P_VALUE > __sql_inline_agg_1})) *
            P_VALUE)))"
;; 

test "Employee37 dEmployee" 
   (InsertEvent("LOCATION", [var "dLID"; "dRG", Type.TString], 
                Schema.StreamRel))
   "AggSum([COUNT_DID], 
      ((__sql_inline_agg_1 ^=
         AggSum([D_LOCATION_ID], 
            ((L_REGIONAL_GROUP:string ^= 'CHICAGO') *
             LOCATION(D_LOCATION_ID, L_REGIONAL_GROUP:string)))) *
      DEPARTMENT(COUNT_DID, D_NAME, D_LOCATION_ID) *
      {__sql_inline_agg_1 > 0}))"
   "AggSum([COUNT_DID, D_LOCATION_ID], 
      ((D_LOCATION_ID ^= dLID) *
       ((__sql_inline_agg_1 ^=
          (AggSum([D_LOCATION_ID], 
             ((L_REGIONAL_GROUP:string ^= 'CHICAGO') *
              LOCATION(D_LOCATION_ID, L_REGIONAL_GROUP:string))) + 
          (AggSum([],
             (L_REGIONAL_GROUP:string ^= dRG:string) * 
             (L_REGIONAL_GROUP:string ^= 'CHICAGO'))))) +
        (-1 *
         (__sql_inline_agg_1 ^=
            AggSum([D_LOCATION_ID], 
               ((L_REGIONAL_GROUP:string ^= 'CHICAGO') *
                LOCATION(D_LOCATION_ID, L_REGIONAL_GROUP:string)))))) *
       DEPARTMENT(COUNT_DID, D_NAME, D_LOCATION_ID) * 
       {__sql_inline_agg_1 > 0}))"
;; 

test "SumADivB"
   (InsertEvent(schema_rel "R" ["dA"; "dB"]))
   "(AggSum([], (R(R_A, R_B) * R_A)) *
       AggSum([], 
          ((__sql_inline_agg_2 ^= (AggSum([], (R(R_A, R_B) * R_B)) + 1)) *
           {[/:float](__sql_inline_agg_2)})))"
   "(AggSum([], ((R_A ^= dA) * (R_B ^= dB) * R_A)) *
       (AggSum([], 
          ((__sql_inline_agg_2 ^= (AggSum([], (R(R_A, R_B) * R_B)) + 1)) *
           {[/:float](__sql_inline_agg_2)})))) +
    ((AggSum([], (R(R_A, R_B) * R_A)) + 
         AggSum([], ((R_A ^= dA) * (R_B ^= dB) * R_A))) *
     (AggSum([], 
         (
            (__sql_inline_agg_2 ^= (
               AggSum([], (R(R_A, R_B) * R_B)) + 1 + 
               AggSum([], (R_B ^= dB) * (R_A ^= dA) * R_B)))
          - (__sql_inline_agg_2 ^= (
               AggSum([], (R(R_A, R_B) * R_B)) + 1))
         ) *
         {[/:float](__sql_inline_agg_2)}
    )))"
;; 
 
test ~opt_out:true "LiftInSchema"
    (InsertEvent(schema_rel "R" ["dA"; "dB"; "dC"]))
    "((R_B ^= 42) * AggSum([R_B, R_A], (R(R_A, R_B, R_C) * R_C)))"
    "(R_A ^= dA) * (R_B ^= dB) * (R_B ^= 42) * dC"
;;

test "Lift range restriction" 
   (InsertEvent(schema_rel "L" ["dPK"; "dQTY"]))
   "AggSum([], P(PK) * 
      (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))"
   "AggSum([], P(PK) * (PK ^= dPK) * 
      ((nested ^= (AggSum([PK], L(PK, QTY2) * QTY2) + 
                  (AggSum([], (QTY2 ^= dQTY) * QTY2)))) + 
       (-1 * (nested ^= (AggSum([PK], L(PK, QTY2) * QTY2))))))"
;; 


(* Debug.activate "LOG-CANCEL-TERMS"; *)
(* Needs smart cancel_terms *)
(* test ~opt_out:true "Q17"
    (InsertEvent(schema_rel "LINEITEM" ["dPK"; "dQTY"; "dEP"; "dDUMMY"]))
   "AggSum([], 
    (LINEITEM(L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DUMMY) *
     PART(L_PARTKEY) *
    AggSum([], 
      ((__sql_inline_agg_1 ^=
         (AggSum([L_PARTKEY], 
            (LINEITEM(L_PARTKEY, L2_QUANTITY, L2_EXTENDEDPRICE, L2_DUMMY) *
              L2_QUANTITY)) *
           0.005)) *
        {L_QUANTITY < __sql_inline_agg_1})) *
    L_EXTENDEDPRICE))"
   "(PART(dPK) *
  AggSum([], 
    (((LINEITEM(dPK, L_QUANTITY, L_EXTENDEDPRICE, L_DUMMY) *
        (AggSum([], 
           ((__sql_inline_agg_1 ^=
              ((AggSum([dPK], 
                  (LINEITEM(dPK, L2_QUANTITY, L2_EXTENDEDPRICE, L2_DUMMY) *
                    L2_QUANTITY)) +
                 dQTY) *
                0.005)) *
             {L_QUANTITY < __sql_inline_agg_1})) +
          (AggSum([], 
             ((__sql_inline_agg_1 ^=
                (AggSum([dPK], 
                   (LINEITEM(dPK, L2_QUANTITY, L2_EXTENDEDPRICE, L2_DUMMY) *
                     L2_QUANTITY)) *
                  0.005)) *
               {L_QUANTITY < __sql_inline_agg_1})) *
            {-1}))) +
       ((L_DUMMY ^= dDUMMY) * (L_EXTENDEDPRICE ^= dEP) * (L_QUANTITY ^= dQTY) *
         AggSum([], 
           ((__sql_inline_agg_1 ^=
              ((AggSum([dPK], 
                  (LINEITEM(dPK, L2_QUANTITY, L2_EXTENDEDPRICE, L2_DUMMY) *
                    L2_QUANTITY)) +
                 dQTY) *
                0.005)) *
             {L_QUANTITY < __sql_inline_agg_1})))) *
      L_EXTENDEDPRICE)))"; *)

Debug.activate "BATCH-UPDATES";
Debug.activate "NO-VISUAL-DIFF"; 

test ~opt_out:true "SingleExists, no values" (BatchUpdate("R"))
  "AggSum([], EXISTS(AggSum([B], R(A,B))))"
  "AggSum([], (DOMAIN( AggSum([B], ((DELTA R)(A, B)))) *
      (EXISTS( (AggSum([B], (R(A, B))) + AggSum([B], ((DELTA R)(A, B))))) +
      (EXISTS( AggSum([B], (R(A, B)))) * {-1}))))"
;; 

test ~opt_out:true "SingleExists" (BatchUpdate("R"))
  "AggSum([], EXISTS(AggSum([B], R(A,B) * {A > 10})))"
  "AggSum([], 
   (DOMAIN( AggSum([B], ((DELTA R)(A, B) * {A > 10}))) *
      (EXISTS(
         (AggSum([B], (R(A, B) * {A > 10})) +
          AggSum([B], ((DELTA R)(A, B) * {A > 10})))) +
      (EXISTS( AggSum([B], (R(A, B) * {A > 10}))) * {-1}))))"
;;

(* (* Needs smart cancel_terms *)
test ~opt_out:true "DoubleExists" (BatchUpdate("R"))
  "AggSum([], EXISTS(AggSum([B], R(A,B) * {A > 10})) * 
              EXISTS(AggSum([B], R(A,B))))"
  "AggSum([], 
  (DOMAIN( AggSum([B], (DELTA R)(A, B))) *
    ((EXISTS( AggSum([B], (R(A, B) * {A > 10}))) *
       EXISTS( AggSum([B], R(A, B))) * {-1}) +
      (EXISTS(
         (AggSum([B], (R(A, B) * {A > 10})) +
           AggSum([B], ((DELTA R)(A, B) * {A > 10})))) *
        EXISTS( (AggSum([B], R(A, B)) + AggSum([B], (DELTA R)(A, B))))))))"
(* THIS IS WRONG, it should be:
    D1 * Xnew * Yold 
  - D1 * Xold * Yold
  + D2 * Xnew * Ynew
  - D2 * Xnew * Yold  *)
;;  *)
 
(* (* Needs smart cancel_terms *)
test ~opt_out:true "DoubleLift" (BatchUpdate("R"))
  "AggSum([], (X ^= (AggSum([B], R(A,B) * {A > 10}))) * 
              (Y ^= (AggSum([B], R(A,B)))) * {X = 0} * {Y > 3})"
  "AggSum([], 
    ((X ^= 0) * DOMAIN( AggSum([B], (DELTA R)(A,B))) *
      (((X ^= AggSum([B], (R(A,B) * {A > 10}))) *
         (Y ^= AggSum([B], R(A,B))) * {-1}) +
        ((Y ^= (AggSum([B], R(A,B)) + AggSum([B], (DELTA R)(A,B)))) *
          (X ^=
           (AggSum([B], (R(A,B) * {A > 10})) +
             AggSum([B], ((DELTA R)(A,B) * {A > 10})))))) *
      {Y > 3}))"
(* THIS IS WRONG, it should be:
    D1 * Xnew * Yold 
  - D1 * Xold * Yold
  + D2 * Xnew * Ynew
  - D2 * Xnew * Yold  *)      
;; 
 *)
test ~opt_out:true "Exists and Lift" (BatchUpdate("R"))
(*   "EXISTS(AggSum([A], R(A,B))) *
   AggSum([], (X ^=AggSum([A], R(A,B) * B)) * {X > 100})"
  "(DOMAIN( AggSum([A], (DELTA R)(A, B))) *
  ((AggSum([], 
      ((X ^= AggSum([A], (R(A, B) * B))) * EXISTS( AggSum([A], R(A, B))) *
        {X > 100})) *
     {-1}) +
    AggSum([], 
      ((X ^=
         (AggSum([A], (R(A, B) * B)) + AggSum([A], ((DELTA R)(A, B) * B)))) *
        EXISTS( (AggSum([A], R(A, B)) + AggSum([A], (DELTA R)(A, B)))) *
        {X > 100}))))"; *)
  "(DOMAIN( AggSum([A], (DELTA R)(A, B))) *
  ((EXISTS( AggSum([A], R(A, B))) *
     AggSum([], ((X ^= AggSum([A], (R(A, B) * B))) * {X > 100})) * {-1}) +
    (EXISTS( (AggSum([A], R(A, B)) + AggSum([A], (DELTA R)(A, B)))) *
      AggSum([], 
        ((X ^= (AggSum([A], (R(A, B) * B)) + AggSum([A], ((DELTA R)(A, B) * B)))) *
          {X > 100})))))"
;;  

(* Debug.activate "LOG-CALCOPT-DETAIL"; *)
(* Debug.activate "LOG-SIMPLIFY-DOMAINS"; *)
(* Debug.activate "LOG-CALCOPT-STEPS"; *)

test ~opt_out:true "Nested Exists" (BatchUpdate("R"))
  "EXISTS(
     EXISTS(AggSum([A], R(A,B))) *
     AggSum([], (X ^=AggSum([A], R(A,B) * B)) * {X > 100})
  )"
  "(DOMAIN( AggSum([A], (DELTA R)(A, B))) *
  (AggSum([], 
     ((X ^=
        (AggSum([A], (R(A, B) * B)) + AggSum([A], ((DELTA R)(A, B) * B)))) *
       {X > 100} *
       EXISTS( (AggSum([A], R(A, B)) + AggSum([A], (DELTA R)(A, B))))
       )) +
    (AggSum([], 
       ((X ^= AggSum([A], (R(A, B) * B))) * EXISTS( AggSum([A], R(A, B))) *
         {X > 100})) *
      {-1})))";
  (* "(DOMAIN( AggSum([A], (DELTA R)(A, B))) *
    ( (EXISTS( (AggSum([A], R(A, B)) + AggSum([A], (DELTA R)(A, B)))) *
        AggSum([], 
          ((X ^= (AggSum([A], (R(A, B) * B)) + AggSum([A], ((DELTA R)(A, B) * B)))) *
           {X > 100}))) +
      ((EXISTS( AggSum([A], R(A, B))) *
        AggSum([], ((X ^= AggSum([A], (R(A, B) * B))) * {X > 100}))) *
        {-1})))" *)
 ;;