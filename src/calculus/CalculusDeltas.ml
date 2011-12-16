open Types
open Ring
open Arithmetic
open Calculus

module C = BasicCalculus
open C

let delta_var_id = ref 0

let rec delta_of_expr 
          ?(external_delta=(fun _ _ _->failwith "Delta of external"))
          (delta_reln:string) (delta_relv:var_t list) (expr:C.expr_t): C.expr_t=
   let rcr = delta_of_expr ~external_delta:external_delta 
                           delta_reln delta_relv in
   CalcRing.delta
      (fun lf ->
         match lf with
         (*****************************************)
            | Value(_) -> CalcRing.zero
         (*****************************************)
            | AggSum(gb_vars, sub_t) -> 
               CalcRing.mk_val (AggSum(gb_vars, rcr sub_t))
         (*****************************************)
            | Rel(reln,relv,_) ->
               if delta_reln = reln then 
                  CalcRing.mk_prod (
                     List.map (fun (dv, v) ->
                        CalcRing.mk_val (Lift(dv, 
                           CalcRing.mk_val (Value(mk_var v))))
                     ) (List.combine relv delta_relv)
                  )
               else CalcRing.zero
         (*****************************************)
            | External(e) -> external_delta delta_reln delta_relv e
         (*****************************************)
            | Cmp(_,_,_) -> CalcRing.zero
         (*****************************************)
            | Lift(v, sub_t) ->
               delta_var_id := !delta_var_id + 1; 
               let delta_term = rcr sub_t in
               let delta_var = ("delta_"^(string_of_int !delta_var_id),
                                C.type_of_expr delta_term) in
                  (* We do a slightly non-standard delta rewrite here.  Rather
                     than the standard 
                        d (A ^= B) ==> (A ^= (B + dB)) - (A ^= B)
                     we use an equivalent expression 
                        d (A ^= B) ==> 
                           (deltaVar ^= dB) * (A ^= (B + deltaVar)) - (A ^= B)
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
         (*****************************************) 
      ) expr