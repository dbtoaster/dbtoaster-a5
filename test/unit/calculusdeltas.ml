open Schema
open Calculus
open UnitTest

let test msg event input output = 
   log_test (msg^" : "^(string_of_event event))
      string_of_expr
      (CalculusDeltas.delta_of_expr event (parse_calc input))
      (parse_calc output)
;;

test "TPCH17" (InsertEvent(schema_rel "P" ["dPK"]))
   "P(PK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))"
   "(PK ^= dPK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))"
;;

test "TPCH17" (InsertEvent(schema_rel "L" ["dPK"; "dQTY"]))
   "P(PK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))"
   "P(PK) * (((PK ^= dPK) * (QTY ^= dQTY) * 
                  (nested ^= AggSum([PK], L(PK, QTY2) * QTY2)))
            + 
            ((L(PK, QTY) + (PK ^= dPK) * (QTY ^= dQTY)) * 
            (delta_1 ^= (AggSum([PK],(
               ((PK ^= [dPK]) * (QTY2 ^= [dQTY]) * [QTY2]))))) * 
            (
               (nested ^= AggSum([PK], L(PK, QTY2) * QTY2) + delta_1)
                - (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))
            )))"
;;