open Schema
open Calculus
open UnitTest

let test ?(opts=false) msg event input output = 
   let input_calc = parse_calc input in
   log_test (msg^" : "^(string_of_event event))
      string_of_expr
      (( if opts then 
            CalculusTransforms.optimize_expr
               (event_vars event, snd (schema_of_expr input_calc))
         else (fun x -> x))
         (CalculusDeltas.delta_of_expr event input_calc)
      )
      (parse_calc output)
;;

test "TPCH17: +Part" (InsertEvent(schema_rel "P" ["dPK"]))
   "P(PK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))"
   "(PK ^= dPK) * L(PK, QTY) * (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))"
;;

test "TPCH17: +Lineitem" (InsertEvent(schema_rel "L" ["dPK"; "dQTY"]))
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
