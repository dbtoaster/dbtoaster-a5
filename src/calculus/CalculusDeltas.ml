(**
   Computing the delta of a Calculus expression.
*)

open Types
open Ring
open Arithmetic
open Calculus

module C = Calculus

let delta_var_id = ref 0

let rec delta_of_expr (delta_event:Schema.event_t) (expr:C.expr_t): C.expr_t=
   let (apply_sign, (delta_reln,delta_relv,_,_)) =
      begin match delta_event with
         | Schema.InsertEvent(rel) -> ((fun x -> x),    rel)
         | Schema.DeleteEvent(rel) -> (CalcRing.mk_neg, rel)
         | _ -> failwith "Error: Can not take delta of a non relation event"
      end
   in
   let rcr = delta_of_expr delta_event in
   Debug.print "LOG-DELTA-DETAIL" (fun () ->
      "d"^delta_reln^" of "^(C.string_of_expr expr) 
   );
   CalcRing.delta
      (fun lf ->
         match lf with
         (*****************************************)
            | Value(_) -> CalcRing.zero
         (*****************************************)
            | AggSum(gb_vars, sub_t) -> 
               let sub_t_delta = rcr sub_t in
                  if sub_t_delta = CalcRing.zero
                  then CalcRing.zero
                  else CalcRing.mk_val (AggSum(gb_vars, rcr sub_t))
         (*****************************************)
            | Rel(reln,relv,_) ->
               if delta_reln = reln then 
                  if (List.length relv) <> (List.length delta_relv)
                  then
                     raise (CalculusException(expr,
                        "Relation '"^reln^"' has an inconsistent number of vars"
                     ))
                  else
                  let definition_terms = 
                     CalcRing.mk_prod (
                        List.map (fun (dv, v) ->
                           CalcRing.mk_val (Lift(dv, 
                              CalcRing.mk_val (Value(mk_var v))))
                        ) (List.combine relv delta_relv)
                     )
                  in apply_sign definition_terms
               else CalcRing.zero
         (*****************************************)
            | External(_) -> failwith "Can not take delta of an external"
         (*****************************************)
            | Cmp(_,_,_) -> CalcRing.zero
         (*****************************************)
            | Lift(v, sub_t) ->
               let delta_term = rcr sub_t in
               if delta_term = CalcRing.zero then CalcRing.zero else (
               delta_var_id := !delta_var_id + 1; 
               let delta_var = ("delta_"^(string_of_int !delta_var_id),
                                C.type_of_expr delta_term) in
                  (* We do a slightly non-standard delta rewrite here.  Rather
                     than the standard 
                        d (A ^= B) ==> (A ^= (B + dB)) - (A ^= B)
                     we use an equivalent expression 
                        d (A ^= B) ==> 
                           (deltaVar ^= dB) * (A ^= (B + deltaVar) - (A ^= B))
                     where deltaVar is a fresh variable.  The reason for this is
                     to range-restrict the evaluation of B.  Assuming products
                     are built up as nested-loop-joins, composed left-to-right
                     (which they are in most of our current backends), then this
                     expression will only be evaluated on the domain of dB,
                     rather than the entire domain of B.  Furthermore, in most 
                     cases, dB will consist only of constants and lift 
                     statements.  In this case, the optimizations (specifically 
                     unify_lifts) will unnest the lift statements, and 
                     substitute the (now) constant dB in for deltaVar. *)
                  CalcRing.mk_prod [
                     CalcRing.mk_val (Lift(delta_var, delta_term));
                     CalcRing.mk_sum [
                        CalcRing.mk_val (
                           Lift(v, CalcRing.mk_sum [
                              sub_t; 
                              CalcRing.mk_val (Value(mk_var delta_var))
                           ])
                        );
                        CalcRing.mk_neg (CalcRing.mk_val (Lift(v, sub_t)))
                     ]
                  ]
               )
         (*****************************************) 
      ) expr

;;

let has_no_deltas (expr:C.expr_t): bool = (C.rels_of_expr expr = [])