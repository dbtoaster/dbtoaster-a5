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
   let (gb, sumfree_expr) = match expr with
      | CalcRing.Val(AggSum(gb, subexpr)) -> (gb, subexpr)
      | _ -> (snd (schema_of_expr expr), expr)
   in
   (* Make Aggsum out of the given expr and gb_vars *)
   let mk_aggsum gb_vars expr =    
      let ovars = snd (schema_of_expr expr) in
      if ListAsSet.subset ovars gb_vars then expr
      else CalcRing.Val(AggSum(ListAsSet.inter gb_vars ovars, expr))
   in
   (* Extract lifts containing Value subexpressions *)
   let (lhs, rhs) = List.fold_left (fun (lhs, rhs) term ->
      match term with
         | CalcRing.Val(Lift(_, CalcRing.Val(Value(_)))) -> 
            if commutes ~scope:scope rhs term 
            then (CalcRing.mk_prod [lhs; term], rhs)
            else (lhs, CalcRing.mk_prod [rhs; term])
         | _ -> (lhs, CalcRing.mk_prod [rhs; term])
   ) (CalcRing.one, CalcRing.one) (CalcRing.prod_list sumfree_expr) in
      (lhs, mk_aggsum gb rhs) 
      
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
                  
                  (* The original delta expression is used to obtain *)
                  (* the group-by variables to project away deltaVar *)
                  let delta_expr = CalcRing.mk_sum [
                     CalcRing.mk_val (
                        Lift(v, CalcRing.mk_sum [ sub_t; delta_term_opt ])
                     );
                     CalcRing.mk_neg (CalcRing.mk_val (Lift(v, sub_t)))
                  ] in                  
                  let gb_vars = snd (schema_of_expr delta_expr) in
                  
                  (* Extract lifts containing Value subexpressions *)
                  let scope = Schema.event_vars delta_event in
                  let (delta_lhs, delta_rhs) = 
                     extract_lifts scope delta_term_opt in               
                  
                  let delta_var = (mk_delta_var (),
                                   C.type_of_expr delta_term_opt) in
               
                  CalcRing.mk_val(AggSum(gb_vars,
	                  CalcRing.mk_prod [
	                     CalcRing.mk_val (Lift(delta_var, delta_lhs));
	                     CalcRing.mk_sum [
	                        CalcRing.mk_val (
	                           Lift(v, CalcRing.mk_sum [
	                              sub_t;
                                 CalcRing.mk_prod [
                                    delta_rhs;
                                    CalcRing.mk_val (Value(mk_var delta_var))
                                 ]
	                           ])
	                        );
	                        CalcRing.mk_neg (CalcRing.mk_val (Lift(v, sub_t)))
	                     ]
	                  ]
                  ))                 
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