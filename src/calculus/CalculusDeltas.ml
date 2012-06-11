(**
   Computing the delta of a Calculus expression.
*)

open Types
open Ring
open Arithmetic
open Calculus

(**/**)
module C = Calculus

let mk_delta_var = 
   FreshVariable.declare_class "calculus/CalculusDeltas"
                               "delta"

let error expr msg = raise (CalculusException(expr, msg));;
(**/**)

(* Extract lifts containing Value subexpressions *)
let extract_lifts scope expr =
   (* Remove toplevel AggSum *)
   let schema = snd (C.schema_of_expr expr) in
   Debug.print "LOG-DELTA-DETAIL" (fun () ->
      "Extracting lifts from lift delta: "^
      (ListExtras.ocaml_of_list string_of_var scope)^
      (ListExtras.ocaml_of_list string_of_var schema)^"\n"^
      (CalculusPrinter.string_of_expr expr)
   );
   (* We need to get all the extractable lifts up into the top-level product.
      This is achieved by a combination of existing CalculusTransforms: 
         NestingRewrites pulls lifts up and out of AggSums.
         FactorizePolynomial pulls lifts up and out of Sums
         AdvanceLifts assists, and moves lifts up as far left as possible.
   *)
   let opt_expr = 
      CalculusTransforms.optimize_expr ~optimizations:[
         CalculusTransforms.OptNestingRewrites; 
         CalculusTransforms.OptAdvanceLifts; 
         CalculusTransforms.OptFactorizePolynomial
      ] (scope,schema) expr in
   (* Extract lifts containing Value subexpressions *)
   List.fold_left (fun (lhs, rhs) term ->
      match term with
         | CalcRing.Val(Lift(lift_v, CalcRing.Val(Value(_)))) -> 
            if (commutes ~scope:scope rhs term)
            then (CalcRing.mk_prod [lhs; term], rhs)
            else (lhs, CalcRing.mk_prod [rhs; term])
         | _ -> (lhs, CalcRing.mk_prod [rhs; term])
   ) (CalcRing.one, CalcRing.one) (CalcRing.prod_list opt_expr)
      
(**
   [delta_of_expr delta_event expr]
   
   Compute the delta of a Calculus expression with respect to a specific event
   @param delta_event   The event with respect to which the delta is being taken
   @param expr          A Calculus expression without Externals
   @return              The delta of [expr] with respect to [delta_event]
*)
let rec delta_of_expr (delta_event:Schema.event_t) (expr:C.expr_t): C.expr_t=
   let (apply_sign, (delta_reln,delta_relv,_)) =
      begin match delta_event with
         | Schema.InsertEvent(rel) -> ((fun x -> x),    rel)
         | Schema.DeleteEvent(rel) -> (CalcRing.mk_neg, rel)
         | _ -> error expr "Error: Can not take delta of a non relation event"
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
            | Rel(reln,relv) ->
               if delta_reln = reln then 
                  if (List.length relv) <> (List.length delta_relv)
                  then
                     error expr (
                        "Relation '"^reln^"' has an inconsistent number of vars"
                     )
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
            | External(_) -> error expr "Can not take delta of an external"
         (*****************************************)
            | Cmp(_,_,_) -> CalcRing.zero
         (*****************************************)
            | Lift(v, sub_t) ->
               let delta_term = rcr sub_t in
               if delta_term = CalcRing.zero then CalcRing.zero else (
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

                  (* Optimize the delta expression *)
(*                  let delta_term_opt = CalculusTransforms.optimize_expr *)
(*                        (schema_of_expr delta_term) delta_term          *)
(*                  in                                                    *)
                  let delta_term_opt = delta_term in
                  
                  (* Extract lifts containing Value subexpressions *)
                  let scope = Schema.event_vars delta_event in
                  let (delta_lhs, delta_rhs) =
                     if Debug.active "DUMB-LIFT-DELTAS" then
                        (CalcRing.one, delta_term_opt)
                     else
                        extract_lifts scope delta_term_opt
                  in
               
                 CalcRing.mk_prod [
                    delta_lhs;
                    CalcRing.mk_sum [
                       CalcRing.mk_val (
                          Lift(v, CalcRing.mk_sum [
                             sub_t;
                             delta_rhs
                          ])
                       );
                       CalcRing.mk_neg (CalcRing.mk_val (Lift(v, sub_t)))
                    ]
                 ]
               )
         (*****************************************) 
      ) expr

;;

(**
   [has_no_deltas expr]
   
   Determine if the provided expression has a (nonzero) delta or not.
   @param expr A Calculus expression
   @return     True if the Calculus expression has no nonzero deltas
*)
let has_no_deltas (expr:C.expr_t): bool = (C.rels_of_expr expr = [])