(**
   Computing the delta of a Calculus expression.
*)

open Type
open Ring
open Arithmetic
open Calculus

(**/**)
module C = Calculus

let mk_delta_var = 
   FreshVariable.declare_class "calculus/CalculusDeltas"
                               "delta_exists"

let error expr msg = raise (CalculusException(expr, msg));;
(**/**)

(** Extract domains containing Value subexpressions *)
let extract_domains scope expr =
   if Debug.active "DUMB-LIFT-DELTAS" then (CalcRing.one, expr) else
   (* Remove toplevel AggSum *)
   let schema = snd (C.schema_of_expr expr) in
   Debug.print "LOG-DELTA-DETAIL" (fun () ->
      "Extracting domains from lift delta: "^
      (ListExtras.ocaml_of_list string_of_var scope)^
      (ListExtras.ocaml_of_list string_of_var schema)^"\n"^
      (CalculusPrinter.string_of_expr expr)
   );
   (* We need to get all the extractable domains up into the top-level product.
      This is achieved by a combination of existing CalculusTransforms: 
         ExtractDomains builds domains for given expressions.
         NestingRewrites pulls domains up and out of AggSums.
         FactorizePolynomial pulls domains up and out of Sums
         AdvanceLifts assists, and moves domains up as far left as possible.
   *)
   let opt_expr = 
      CalculusTransforms.optimize_expr ~optimizations:[
         CalculusTransforms.OptExtractDomains;
         CalculusTransforms.OptNestingRewrites; 
         CalculusTransforms.OptAdvanceLifts;
         CalculusTransforms.OptFactorizePolynomial
      ] (scope,schema) expr
   in
      (* Extract domain terms *)
      List.fold_left (fun (lhs, rhs) term -> match term with
         | CalcRing.Val(DomainDelta _) -> (CalcRing.mk_prod [lhs; term], rhs)
         | _ -> (lhs, CalcRing.mk_prod [rhs; term])
      ) (CalcRing.one, CalcRing.one) (CalcRing.prod_list opt_expr) 
   
(**
   [delta_of_expr delta_event expr]
   
   Compute the delta of a Calculus expression with respect to a specific event
   @param delta_event   The event with respect to which the delta is being taken
   @param expr          A Calculus expression without Externals
   @return              The delta of [expr] with respect to [delta_event]
*)
let rec delta_of_expr (delta_event:Schema.event_t) (expr:C.expr_t): C.expr_t =

   let template_batch_delta_of_rel delta_reln reln relv = 
      if delta_reln = reln then C.mk_deltarel reln relv else CalcRing.zero
   in
   let template_delta_of_rel apply_sign (delta_reln,delta_relv,_) reln relv =
      if delta_reln = reln then 
         if (List.length relv) <> (List.length delta_relv)
         then
            error expr (
               "Relation '"^reln^"' has an inconsistent number of vars"
            )
         else
         let definition_terms = 
            Calculus.value_singleton 
               (List.combine relv (List.map Arithmetic.mk_var delta_relv)) 
         in
           apply_sign definition_terms
      else CalcRing.zero
   in
   let empty_delta_of_rel (_:string) (_:var_t list) = CalcRing.zero in
   let error_delta_of_ext (_:external_t) = 
      error expr "Cannot take delta of an external"
   in
   let (delta_of_rel, delta_of_ext) =
      begin match delta_event with
         | Schema.InsertEvent(delta_rel) -> 
            (
               (template_delta_of_rel (fun x -> x) delta_rel),
               (error_delta_of_ext)
            )
         | Schema.DeleteEvent(delta_rel) -> 
            (
               (template_delta_of_rel CalcRing.mk_neg delta_rel),
               (error_delta_of_ext)
            )
         | Schema.BatchUpdate(delta_reln) ->
            (  
               (template_batch_delta_of_rel delta_reln), 
               (error_delta_of_ext)
            )            
         | Schema.CorrectiveUpdate(delta_ext_name, delta_ivars, delta_ovars,  
                                   delta_value, _) ->
            (
               (empty_delta_of_rel), 
               (fun (ext_name, ext_ivars, ext_ovars, ext_type, ext_ivc) ->
                  if ext_name = delta_ext_name
                  then Calculus.value_singleton
                     ~multiplicity:(
                        CalcRing.mk_prod (
                           (Calculus.mk_value 
                               (Arithmetic.mk_var delta_value)) :: 
                           (List.map (fun (iv,div) ->
                               Calculus.mk_cmp Eq (Arithmetic.mk_var iv) 
                                                  (Arithmetic.mk_var div)
                           ) (List.combine ext_ivars delta_ivars))
                        )
                     )
                     (List.combine ext_ovars 
                                   (List.map Arithmetic.mk_var delta_ovars))
                  else CalcRing.zero
               )
            )
         | _ -> error expr "Error: Can not take delta of a non relation event"
      end
   in
   let rcr = delta_of_expr delta_event in
   Debug.print "LOG-DELTA-DETAIL" (fun () ->
      (Schema.name_of_event delta_event)^" of "^(C.string_of_expr expr) 
   );
   (* From a given expression, extract conditions covered by the scope  *)
   let try_extract_conditions expr scope =
      let (_, schema) = C.schema_of_expr expr in
      let opt_expr = CalculusTransforms.optimize_expr (scope, schema) expr in
      let (cond, rest) =
        List.partition (function
          | CalcRing.Val(Cmp(_,_,_))
          | CalcRing.Val(CmpOrList(_,_)) as e ->
              ListAsSet.subset (fst (C.schema_of_expr e)) scope
          | _ -> false
        ) (CalcRing.prod_list opt_expr)
      in
      if cond = [] then (CalcRing.one, expr)
      else (CalcRing.mk_prod cond, CalcRing.mk_prod rest)
   in
   CalcRing.delta
      (fun lf ->
         match lf with
         (*****************************************)
            | Value(_) -> CalcRing.zero
         (*****************************************)
            | AggSum(gb_vars, sub_t) -> 
               let sub_t_delta = rcr sub_t in
               (* It's possible that the lift delta can introduce new output
                  variables, since it can pull range restrictions that normally
                  would appear only inside the lift out of the lift.
                  We need to update the group-by vars of the term accordingly.*)
               let ins = fst (Calculus.schema_of_expr sub_t) in
               let outs = snd (Calculus.schema_of_expr sub_t_delta) in
               let new_gb_vars = ListAsSet.union gb_vars
                                                 (ListAsSet.inter ins outs) in
                  if sub_t_delta = CalcRing.zero
                  then CalcRing.zero
                  else Calculus.mk_aggsum new_gb_vars sub_t_delta
         (*****************************************)
            | Rel(reln,relv) -> delta_of_rel reln relv
         (*****************************************)
            | DeltaRel(_,_) -> CalcRing.zero
         (*****************************************)
            | DomainDelta(_) -> CalcRing.zero
         (*****************************************)
            | External(e) -> delta_of_ext e
         (*****************************************)
            | Cmp(_,_,_) -> CalcRing.zero
         (*****************************************)
            | CmpOrList(_,_) -> CalcRing.zero
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
                     substitute the (now) constant dB in for deltaVar.

                     This rewriting is realised via extract_domains. Additionally,
                     any comparison appearing in dB and is covered by the scope is
                     pulled out outside the delta expression.
                  *)
                  (* Extract lifts containing Value subexpressions *)
                  let scope = Schema.event_vars delta_event in
                  (* FIXME: scope should be the running scope *)
                  let (cond, rest) = try_extract_conditions delta_term scope in
                  let (delta_lhs, delta_rhs) =
                    extract_domains scope rest
                  in                  
                    CalcRing.mk_prod [
                      cond; delta_lhs;
                      CalcRing.mk_sum [
                        Calculus.mk_lift v (CalcRing.mk_sum [sub_t; delta_rhs]);
                        CalcRing.mk_neg (Calculus.mk_lift v sub_t)
                      ]
                    ]
               )
         (*****************************************) 
(***** BEGIN EXISTS HACK *****)
            | Exists(sub_t) ->
                let delta_term = rcr sub_t in
                if delta_term = CalcRing.zero then CalcRing.zero else (
                  (* We do the same thing here as for lifts *)
                  let scope = Schema.event_vars delta_event in
                  (* FIXME: scope should be the running scope *)
                  let (cond, rest) = try_extract_conditions delta_term scope in
                  let (delta_lhs, delta_rhs) =
                    extract_domains scope rest
                  in
                    CalcRing.mk_prod [
                      cond; delta_lhs;
                      CalcRing.mk_sum [
                        Calculus.mk_exists (CalcRing.mk_sum [sub_t; delta_rhs]);
                        CalcRing.mk_neg (Calculus.mk_exists sub_t)
                      ]
                    ]
               )
(***** END EXISTS HACK *****)
         (*****************************************)
      ) expr

;;

(** Test wherther it is possible to extract lifts *)
let can_extract_domains (delta_event:Schema.event_t) (expr:C.expr_t) =    
   let delta_term = delta_of_expr delta_event expr in
      if delta_term = CalcRing.zero then false 
      else (
         let scope = Schema.event_vars delta_event in
         (fst (extract_domains scope delta_term) <> 
          CalcRing.one)
      ) 

(**
   [has_no_deltas expr]
   
   Determine if the provided expression has a (nonzero) delta or not.
   @param expr A Calculus expression
   @return     True if the Calculus expression has no nonzero deltas
*)
let has_no_deltas (expr:C.expr_t): bool = (C.rels_of_expr expr = [])
