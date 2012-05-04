open Schema
open Calculus
open UnitTest

let test ?(opt_in=false) ?(opt_out=false) msg event input output = 
   let input_calc = parse_calc input in
   let map_schema = (event_vars event, snd (schema_of_expr input_calc)) in
   log_test (msg^" : "^(string_of_event event))
      string_of_expr
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
               ((PK ^= [dPK]) * (QTY2 ^= [dQTY]) * [QTY2]))))) * 
            (
               (nested ^= AggSum([PK], L(PK, QTY2) * QTY2) + delta_2)
                - (nested ^= AggSum([PK], L(PK, QTY2) * QTY2))
            )))))"
;;
