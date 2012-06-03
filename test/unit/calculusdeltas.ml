open Schema
open Calculus
open UnitTest

let test ?(opt_in=false) ?(opt_out=false) msg event input output = 
   let input_calc = parse_calc input in
   let map_schema = (event_vars event, snd (schema_of_expr input_calc)) in
   log_test (msg^" : "^(string_of_event event))
      CalculusPrinter.string_of_expr
      (( if opt_out 
         then CalculusTransforms.optimize_expr map_schema
         else (fun x -> x))
         (CalculusDeltas.delta_of_expr event (
            if opt_in 
            then CalculusTransforms.optimize_expr map_schema input_calc
            else input_calc
         ))
      )
      (parse_calc output)
;;

test "TPCH17 simple: +Part" (InsertEvent(schema_rel "P" ["dPK"]))
   "AggSum([], P(PK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))"
   "AggSum([], (PK ^= dPK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))"
;;

test "TPCH17 simple: +Lineitem" (InsertEvent(schema_rel "L" ["dPK"; "dQTY"]))
   "AggSum([], P(PK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))"
   "AggSum([], P(PK) * (((PK ^= dPK) * (QTY ^= dQTY) * 
                  (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))
            + 
            ((L(PK, QTY) + (PK ^= dPK) * (QTY ^= dQTY)) * 
            AggSum([nested, PK],
						(delta_2 ^= (AggSum([PK],(
               ((PK ^= dPK) * (QTY2 ^= dQTY) * QTY2))))) * 
            (
               (nested ^= AggSum([PK], L(PK, QTY2) * QTY2) + delta_2)
                - (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))
            )))))"
;;

test "TPCH11 dPartsupp" (InsertEvent(schema_rel "PARTSUPP" ["dPK"; "dSK"; "dAQ"; "dSC"]))
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
     ((AggSum([N_VALUE, P_NATIONKEY], 
         ((delta_10 ^=
            AggSum([P_NATIONKEY], 
              ((PS_PARTKEY ^= dPK) * (PS_SUPPKEY ^= dSK) *
                (PS_AVAILQTY ^= dAQ) * (PS_SUPPLYCOST ^= dSC) *
                SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                           S_ACCTBAL, S_COMMENT) *
                (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) *
           ((N_VALUE ^=
              (AggSum([P_NATIONKEY], 
                 (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
                   SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                              S_ACCTBAL, S_COMMENT) *
                   (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY)) +
                delta_10)) +
             (-1 *
               (N_VALUE ^=
                 AggSum([P_NATIONKEY], 
                   (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
                     SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                                S_PHONE, S_ACCTBAL, S_COMMENT) *
                     (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))))))) *
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
        P_VALUE) +
       (((N_VALUE ^=
           AggSum([P_NATIONKEY], 
             (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
               SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                          S_ACCTBAL, S_COMMENT) *
               (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) +
          AggSum([N_VALUE, P_NATIONKEY], 
            ((delta_9 ^=
               AggSum([P_NATIONKEY], 
                 ((PS_PARTKEY ^= dPK) * (PS_SUPPKEY ^= dSK) *
                   (PS_AVAILQTY ^= dAQ) * (PS_SUPPLYCOST ^= dSC) *
                   SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                              S_ACCTBAL, S_COMMENT) *
                   (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) *
              ((N_VALUE ^=
                 (AggSum([P_NATIONKEY], 
                    (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
                      SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                                 S_PHONE, S_ACCTBAL, S_COMMENT) *
                      (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST *
                      PS_AVAILQTY)) +
                   delta_9)) +
                (-1 *
                  (N_VALUE ^=
                    AggSum([P_NATIONKEY], 
                      (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY,
                                  PS_SUPPLYCOST) *
                        SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                                   S_PHONE, S_ACCTBAL, S_COMMENT) *
                        (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST *
                        PS_AVAILQTY)))))))) *
         AggSum([P_VALUE, P_PARTKEY, P_NATIONKEY], 
           ((delta_8 ^=
              AggSum([P_PARTKEY, P_NATIONKEY], 
                ((PS_PARTKEY ^= dPK) * (PS_SUPPKEY ^= dSK) *
                  (PS_AVAILQTY ^= dAQ) * (PS_SUPPLYCOST ^= dSC) *
                  (P_PARTKEY ^= PS_PARTKEY) *
                  SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
                             S_ACCTBAL, S_COMMENT) *
                  (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY))) *
             ((P_VALUE ^=
                (AggSum([P_PARTKEY, P_NATIONKEY], 
                   (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST) *
                     (P_PARTKEY ^= PS_PARTKEY) *
                     SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                                S_PHONE, S_ACCTBAL, S_COMMENT) *
                     (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST * PS_AVAILQTY)) +
                  delta_8)) +
               (-1 *
                 (P_VALUE ^=
                   AggSum([P_PARTKEY, P_NATIONKEY], 
                     (PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY,
                                 PS_SUPPLYCOST) *
                       (P_PARTKEY ^= PS_PARTKEY) *
                       SUPPLIER(PS_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                                  S_PHONE, S_ACCTBAL, S_COMMENT) *
                       (P_NATIONKEY ^= S_NATIONKEY) * PS_SUPPLYCOST *
                       PS_AVAILQTY))))))) *
         AggSum([], 
           ((__sql_inline_agg_1 ^= (0.001 * N_VALUE)) *
             {P_VALUE > __sql_inline_agg_1})) *
         P_VALUE)))"
;;