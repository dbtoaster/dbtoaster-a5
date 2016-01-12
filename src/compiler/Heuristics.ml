(**
   A module for deciding on how to materialize a given (delta) query. It is used
     by the Compiler module to produce a Plan.plan_t datastructure.
        
     Driven by a set of heuristics rules, this module provides 
     two main functions: 
     {ul
       {- {b should_update} decides whether it is more efficient to
          incrementally maintain the given query, or to reevaluate it upon 
          each trigger call }  
       {- {b materialize} transforms the input (delta) expression by replacing
          all relations with data structures (maps) }
     }
  *)

open Type
open Arithmetic
open Calculus
open CalculusDecomposition
open CalculusTransforms
open Plan

type ds_history_t = ds_t list ref
type term_list = expr_t list


(******************************************************************************)

type heuristic_option_t =
   | NoIVC
   | NoInputVariables
   | ExtractRelationMaps
   | ForceExpMaterialization

type heuristic_options_t = heuristic_option_t list

(******************************************************************************)

let raw_curr_suffix = ref 0 

let event_is_relevant (event:Schema.event_t option) (expr:expr_t): bool =
   match event with
      | Some(Schema.InsertEvent(reln,_,_)) 
      | Some(Schema.DeleteEvent(reln,_,_)) 
      | Some(Schema.BatchUpdate(reln)) -> List.mem reln (rels_of_expr expr)
      | _  -> false 

(** Obtain the human-readable representation of the optional event parameter. 
    It calls Schema.string_of_event if the event is passed. *) 
let string_of_event (event:Schema.event_t option) : string =
   match event with
      | Some(evt) -> Schema.string_of_event evt
      | _  -> "<None>" 

let string_of_expr = CalculusPrinter.string_of_expr
let string_of_vars = ListExtras.string_of_list string_of_var

let string_of_options (heuristic_options:heuristic_options_t) : string =
   ((if List.mem NoIVC heuristic_options
     then "NoIVC: true " else "NoIVC: false ") ^
    (if List.mem NoInputVariables heuristic_options
     then "NoInputVariables: true " else "NoInputVariables: false ") ^
    (if List.mem ExtractRelationMaps heuristic_options
     then "ExtractRelationMaps: true " else "ExtractRelationMaps: false "))   
   
let covered_by_scope scope expr = 
  ListAsSet.subset (fst(Calculus.schema_of_expr expr)) scope
   

(*****************************************************************************)

type decision_options_t = 
   | MaterializeAsNewMap
   | MaterializeUnknown


(* Group the expression terms by type: 1) delta domain terms 2) delta terms 
   (exist only for batch updates); 2) relations; 3) lift/exists; 
   4) comparisons and values. The partial order inside each group 
   (deltas, relations, lifts, and values) is preserved.               *)
let group_terms_by_type (expr:expr_t) : (term_list * term_list * term_list * 
                                         term_list * term_list) =
   List.fold_right (fun term (domains, deltas, rels, lifts, vals) ->
      match CalcRing.get_val term with
         | DomainDelta _ -> (term :: domains, deltas, rels, lifts, vals)
         | DeltaRel _ -> (domains, term :: deltas, rels, lifts, vals)
         | Rel _ | External _ -> (domains, deltas, term :: rels, lifts, vals)            
(***** BEGIN EXISTS HACK *****)
         | Exists _
(***** END EXISTS HACK *****) 
         | Lift _ -> (domains, deltas, rels, term :: lifts, vals)
         | Value _ | Cmp _ -> (domains, deltas, rels, lifts, term :: vals)
         | AggSum _ -> bail_out expr 
            ("[group_terms_by_type] AggSums are supposed to be removed.")
   ) (CalcRing.prod_list expr) ([], [], [], [], []) 

let reogranize_expr (expr:expr_t): expr_t =
   let (domains, deltas, rels, lifts, vals) = group_terms_by_type expr in 
   CalcRing.mk_prod (List.flatten [ domains; deltas; rels; lifts; vals ])

(** Split an expression into five parts: 
    {ol
      {li domain delta terms }
      {li delta terms (only for batch updates)}
      {li value terms covered by the trigger variables or the delta terms }
      {li base relations, irrelevant lift expressions with respect to the 
          event relation that also contain no input variables, and 
          subexpressions with no input variables (comparisons, variables 
          and constants) }  
      {li lift expressions containing the event relation or input variables }
      {li subexpressions with input variables and the above lift variables } 
    } 
    Note: In order to minimize the need for IVC, if there is no relation at the 
    root level, then lift subexpressions containing irrelevant relations are 
    materialized separately. *)
let partition_expr (heuristic_options:heuristic_options_t) (scope:var_t list) 
                   (event:Schema.event_t option) (expr:expr_t) : 
                   (expr_t * expr_t * expr_t * expr_t * expr_t) =

   (* Sanity check - expr should be covered by the scope *)
   if not (covered_by_scope scope expr)
   then bail_out expr "expr is not covered by the scope."
   else

   let (expr_ivars, expr_ovars) = schema_of_expr expr in

   Debug.print "LOG-HEURISTICS-PARTITIONING" (fun () -> 
      "[Heuristics.partition_expr]  Asked to partition: " ^ 
      "\n\t Expr: " ^ (string_of_expr expr) ^
      "\n\t Scope: " ^ (ListExtras.string_of_list string_of_var scope) ^
      "\n\t Event: " ^ (string_of_event event) ^
      "\n\t Options: " ^ (string_of_options heuristic_options)
   ); 

   (* Preprocessing step to mark those terms that are going to be materialized 
      separately for sure, for example, lift subexpressions containing the 
      event relation. Lift/exists subexpressions containing other relations 
      are also materialized as new maps, unless HEURISTICS-PULL-IN-LIFTS is 
      set. The materialization strategies of the remaining lift/exists/value 
      terms are going to be determined later on.   *)
   let tag ?(option_no_ivc:bool = List.mem NoIVC heuristic_options)
           ?(option_pull_in_lifts:bool = Debug.active "HEURISTICS-PULL-IN-LIFTS")
           (has_root_relation:bool) (terms: expr_t list) = 
      List.map (fun term ->
         match CalcRing.get_val term with
   (***** BEGIN EXISTS HACK *****)
            | Exists(subexpr) 
   (***** END EXISTS HACK *****)
            | Lift(_, subexpr) ->
               if rels_of_expr subexpr == [] 
               then (term, MaterializeUnknown)
               else            
                  (* If there is no root relations, materialize  
                     subexpr in order to avoid the need for IVC  *)
                  if option_no_ivc && not has_root_relation
                  then (term, MaterializeAsNewMap)
                  else

                  (* subexpr containing the event relation 
                     is always materialized separately       *)
                  if event_is_relevant event subexpr
                  then (term, MaterializeAsNewMap)
                  else 
                     if option_pull_in_lifts 
                     then (term, MaterializeUnknown)
                     else (term, MaterializeAsNewMap)

            | Value _ | Cmp _ -> (term, MaterializeUnknown)

            | _ -> failwith "Not a valid term (lift, exists, or value)"
      ) terms 
   in

   (* We have to decide on which lift/exists and value terms should 
      be pulled in target_expr. Here are some examples, assuming 
      option_inputvar = false                 
        e.g. R(A,B) * (A ^= 0) --> Yes                   
             R(A,B) * (D ^= 0) * (D ^= {A=C}) --> No     
             R(A,B) * (C ^= 1) * (A ^= C) --> Yes        
             R(A,B) * (C ^= D) * (A ^= C) --> No         
      Essentially, we perform graph decomposition over the lift and 
      value terms. For each term group, we make the same decision: 
      either all terms are  going to be pulled in, or none of them. 
      The invariant that we want to preserve is the set of output 
      variables of the relation term. If input variables are allowed,
      pulling these terms in should not introduce new input variables. *)
   let partition_terms 
         ?(option_no_ivc = List.mem NoIVC heuristic_options)
         ?(option_inputvar = not (List.mem NoInputVariables heuristic_options))
         ?(option_aggressive_inputvar = 
                     Debug.active ("HEURISTICS-AGGRESSIVE-INPUTVARS"))
         ?(option_pull_in_lifts = Debug.active "HEURISTICS-PULL-IN-LIFTS")
         ?(option_pull_out_values = Debug.active "HEURISTICS-PULL-OUT-VALUES")
         (scope:var_t list) (target_expr:expr_t) (terms:expr_t list) =

      let (target_ivars, target_ovars) = schema_of_expr target_expr in

      (* Sanity check - target_expr should have no input variables 
         (it's a product of delta relations/domains or relations)   *)
      if not (covered_by_scope scope target_expr)
      then bail_out target_expr "target_expr has input variables."
      else
     
      (* Tag each term *)
      let tagged_terms = tag ~option_no_ivc:option_no_ivc
                             ~option_pull_in_lifts:option_pull_in_lifts
                             (rels_of_expr target_expr <> [] ||
                              deltarels_of_expr target_expr <> []) 
                             terms in

      (* Graph decomposition over the tagged terms *)      
      let graph_components = 
         let get_vars (term, tag) = 
            let (i, o) = C.schema_of_expr term in 
            ListAsSet.diff (ListAsSet.union i o) 
                           (ListAsSet.union scope target_ovars)
         in     
            HyperGraph.connected_unique_components get_vars tagged_terms 
      in
      let get_component term = 
         List.find (fun component_terms -> List.mem term component_terms) 
                   graph_components 
      in

      (* Partition terms *)
      List.fold_left (fun (lhs_terms, rhs_lifts, rhs_vals) (term, tag) ->
         match term with
(***** BEGIN EXISTS HACK *****)
            | CalcRing.Val(Exists _)  
(***** END EXISTS HACK *****)
            | CalcRing.Val(Lift _) -> 
               begin
                  try         
                     (* Skip the term if materialized separately *)
                     if tag = MaterializeAsNewMap 
                     then (lhs_terms, rhs_lifts @ [term], rhs_vals) 
                     else 

                     (* Get the list of connected components *)
                     let conn_components = get_component (term, tag) in
                     
                     (* Skip the term if at least one component is to be
                        materialized or option_pull_out_values is set.   *)
                     if (List.exists 
                           (fun (_, tag) -> tag = MaterializeAsNewMap)
                           conn_components) || (option_pull_out_values)
                     then (lhs_terms, rhs_lifts @ [term], rhs_vals)
                     else 
                      
                     (* Each conn_component has tag = MaterializeUnknown *)
                     let component_expr = 
                        CalcRing.mk_prod (List.map fst conn_components) in
                     let (comp_ivars, comp_ovars) = 
                        schema_of_expr component_expr in
                     let comp_vars = ListAsSet.union comp_ivars comp_ovars in

                     let lhs_expr = (CalcRing.mk_prod lhs_terms) in
                     let (_, lhs_ovars) = schema_of_expr lhs_expr in

                     (* Check if component_expr is relevant for lhs_expr *)
                     if ListAsSet.inter lhs_ovars comp_vars = [] 
                     then (lhs_terms, rhs_lifts @ [term], rhs_vals)
                     else

                     (* Add term to lhs_terms only if that does not 
                        introduce new input variables to target_expr  *)
                     if (covered_by_scope lhs_ovars component_expr)
                     then (lhs_terms @ [term], rhs_lifts, rhs_vals)
                     else 

                     (* (lhs_expr * component_expr) introduces input variables 
                        If input variables are allowed, then we should add 
                        term to lhs_terms only if that doesn't create new 
                        input variables. *)
                     if (option_inputvar && option_aggressive_inputvar &&
                         covered_by_scope (ListAsSet.union expr_ivars lhs_ovars)
                                          component_expr)
                     then (lhs_terms @ [term], rhs_lifts, rhs_vals)
                     else (lhs_terms, rhs_lifts @ [term], rhs_vals)

                  with Not_found -> failwith "The lift term cannot be found."
               end
            | CalcRing.Val(Value _) 
            | CalcRing.Val(Cmp _) ->
               if option_pull_out_values 
               then (lhs_terms, rhs_lifts, rhs_vals @ [term]) 
               else
                                 
               let lhs_expr = CalcRing.mk_prod lhs_terms in
               let (_, lhs_ovars) = schema_of_expr lhs_expr in

               (* Check if term is relevant for lhs_expr *)               
               if ListAsSet.inter lhs_ovars (fst (schema_of_expr term)) = [] 
               then (lhs_terms, rhs_lifts, rhs_vals @ [term])
               else

               (* Add term to lhs_terms only if that does not 
                  introduce new input variables to lhs_expr  *)
               if (covered_by_scope lhs_ovars term)
               then (lhs_terms @ [term], rhs_lifts, rhs_vals)
               else

               (* Add term to lhs_terms only if that 
                  doesn't create new input variables *)
               if (option_inputvar && 
                   covered_by_scope (ListAsSet.union expr_ivars lhs_ovars) term)
               then (lhs_terms @ [term], rhs_lifts, rhs_vals)    
               else (lhs_terms, rhs_lifts, rhs_vals @ [term])

            | _ -> failwith "Not a valid term (lift, exists, or value)"

      ) (CalcRing.prod_list target_expr, [], []) tagged_terms
   in  

   let (domains, deltas, rels, lifts, vals) = group_terms_by_type expr in 

   let domain_expr = CalcRing.mk_prod domains in
   let scope_delta = 
      ListAsSet.union scope (snd (schema_of_expr domain_expr)) 
   in

   (* Partition lift_terms and value_terms w.r.t. deltas *)
   let (delta_terms, rest_lifts, rest_vals) = 
      partition_terms ~option_no_ivc:true
                      ~option_inputvar:false 
                      ~option_pull_in_lifts:false
                      ~option_pull_out_values:false
                      scope_delta (CalcRing.mk_prod deltas) 
                      (lifts @ vals) 
   in
   let delta_expr = CalcRing.mk_prod delta_terms in
   let scope_rel = 
      ListAsSet.union scope_delta (snd(schema_of_expr delta_expr))
   in

   (* Partition lift_terms and value_terms w.r.t. rels *)
   let (rel_terms, lift_terms, val_terms) =
      partition_terms scope_rel (CalcRing.mk_prod rels) 
                      (rest_lifts @ rest_vals)  
   in   

   let rel_expr = CalcRing.mk_prod rel_terms in
   let lift_expr = CalcRing.mk_prod lift_terms in
   let value_expr = CalcRing.mk_prod val_terms in   

   Debug.print "LOG-HEURISTICS-PARTITIONING" (fun () -> 
     "[Heuristics]  Partitioning - done: " ^ 
      "\n\t Domain: "   ^ (string_of_expr domain_expr) ^
      "\n\t Delta: "    ^ (string_of_expr delta_expr) ^
      "\n\t Relation: " ^ (string_of_expr rel_expr  ) ^
      "\n\t Lift: "     ^ (string_of_expr lift_expr ) ^
      "\n\t Value:  "   ^ (string_of_expr value_expr) 
   ); 
   (domain_expr, delta_expr, rel_expr, lift_expr, value_expr)


(******************************************************************************)

let materializer_opts =
   ListAsSet.diff 
      CalculusTransforms.default_optimizations 
      [ CalculusTransforms.OptExtractDomains ]

let rec decompose_and_fold (scope:var_t list) 
                           (zero_value:'a) (one_value: 'a) 
                           (sum_values_fn:('a -> 'a -> 'a)) 
                           (prod_values_fn:('a -> 'a -> 'a)) 
                           (monomial_fn:(string -> var_t list -> expr_t -> 'a))
                           (prefix:string) 
                           (expr:expr_t): 'a =

   let rcr = decompose_and_fold scope zero_value one_value sum_values_fn 
                                prod_values_fn monomial_fn in
   let contains_aggsum = 
      CalcRing.fold (List.fold_left (||) false) 
                    (List.fold_left (||) false) 
                    (fun x -> x)
                    (function AggSum _ -> true | _ -> false)
   in

   (* Polynomial decomposition *)
   fst (List.fold_left (fun (ret_value, i) (schema, term) ->

      let term_opt = optimize_expr ~optimizations:materializer_opts
                                   (scope, schema) term in
         
      if not (CalcRing.try_cast_to_monomial term_opt)
      then begin
         let term_name = (prefix ^ (string_of_int i)) in
         let new_value = 
            rcr term_name (Calculus.mk_aggsum schema term_opt) 
         in
         (sum_values_fn ret_value new_value, i + 1)
      end
      else 

      (* Graph decomposition *)
      let (new_value, k) =  
         List.fold_left (fun (ret_value, j) (schema, term) ->

            let term_opt = optimize_expr ~optimizations:materializer_opts
                                         (scope, schema) term in
            let term_name = (prefix ^ (string_of_int j)) in
            
            let new_value = 
               if (not (CalcRing.try_cast_to_monomial term_opt)) ||
                  (* A sanity check that the optimizer has not broken    *)
                  (* the assumption of having an aggsum-free expression. *)
                  (contains_aggsum term_opt)                  
               then rcr term_name (Calculus.mk_aggsum schema term_opt)
               else monomial_fn term_name schema term_opt
            in
               (prod_values_fn ret_value new_value, j + 1)
         ) 
         (one_value, i) (snd (decompose_graph scope (schema, term_opt)))
      in
         (sum_values_fn ret_value new_value, k)           
   ) 
   (zero_value, 1) (decompose_poly expr))

(******************************************************************************)
type maintaining_option_t =
   | Unknown
   | UpdateExpr
   | ReplaceExpr

(** For a given expression and a trigger event, decides whether it is more 
    efficient to incrementally maintain the expression or to reevaluate it 
    upon each trigger call. 
        
    The reason for making this decision is the following: If an expression 
    contains a lift subexpression (nested aggregate) with nonzero delta, 
    the overall delta expression is not simpler than the original expression.
    In such cases, it might be beneficial to maintain the expression 
    non-incrementally, recomputing it on every update.
        
   The rule is the following: if it is possible to extract a DomainDelta term 
   when computing the delta of an expression, then we decide to incrementally 
   maintain; otherwise, we re-evaluate the expression. The rationale behind 
   this decision is that if there is one term for which we cannot restrict 
   the domain of iteration when computing its delta, then the delta computation 
   is at least as expensive as re-evaluation.
    
    Two special cases arise when the given expression does not contain lift
    subexpressions, or it does not contain relation subexpressions. In both 
    cases, this method suggests the incremental maintenance.
*)
let should_update ?(scope:var_t list = []) 
                  (event:Schema.event_t) (expr:expr_t)  : bool =
   
   Debug.print "LOG-HEURISTICS-STRATEGY-DETAIL" (fun () ->
      "[should_update] Asked to decide on:"^
      "\n\t Expr: "^(string_of_expr expr)^
      "\n\t Event: "^(Schema.string_of_event event)
   );
   
   if (Debug.active "HEURISTICS-ALWAYS-UPDATE") then true 
   else 
   
   (* A simple state machine to decide on the next maintaining option *)
   let get_next_state current_state next_state =
      match (current_state, next_state) with
         | (x, y) when x = y -> x
         | (Unknown, x) | (x, Unknown) -> x
         | (UpdateExpr, ReplaceExpr) 
         | (ReplaceExpr, UpdateExpr) -> ReplaceExpr
         | _ -> failwith "Unable to process next_state."
   in

   let expr_scope = Schema.event_vars event in

   let final_state = 
      decompose_and_fold 
        expr_scope Unknown Unknown get_next_state get_next_state
        (fun _ schema expr -> fst (
           List.fold_left (fun (local_state, local_scope) term ->
              (* We are only interested in Lift/Exists expressions *)
              let next_state = 
                match CalcRing.get_val term with
(***** BEGIN EXISTS HACK *****)
                  | Exists(subexpr)  
(***** END EXISTS HACK *****)
                  | Lift(_, subexpr) ->
                     if not (event_is_relevant (Some(event)) subexpr)
                     then local_state
                     else 
                       get_next_state
                         local_state
                         (if (CalculusDeltas.can_extract_domains 
                              event subexpr)
                          then UpdateExpr
                          else ReplaceExpr) 
                  | _ -> local_state
              in
              let next_scope = 
                ListAsSet.union local_scope (snd (schema_of_expr term))
              in                
                (next_state, next_scope)

           ) (Unknown, expr_scope) (CalcRing.prod_list (reogranize_expr expr))
        ) ) 
        "dummy"
        (* The materializer assumes expressions without Neg nodes.     
           When the calculus optimizer is disabled, this assumption    
           might be violated. We guard against these cases by calling  
           CalculusTransforms.normalize.                              *)
        (CalculusTransforms.normalize expr)
   in
   let final_decision = match final_state with
      | Unknown | UpdateExpr -> true
      | ReplaceExpr -> false 
   in
      Debug.print "LOG-HEURISTICS-STRATEGY-DETAIL" (fun () ->
         "[should_update] Decision: "^ (string_of_bool final_decision)
      );
      final_decision

(******************************************************************************)

(** Perform partial materialization of a given expression according to 
    the following rules:
    {ol
       {li {b Polynomial expansion}: Before materializing, an expression is
        expanded into a flat ("polynomial") form with unions at the top. The
        materialization procedure is performed over each individual term. }
       {li {b Graph decomposition}: If some parts of the expression are 
       disconnected in the join graph, it is always better to materialise them 
       piecewise.}
       {li {b Decorrelation of nested subexpressions}: Lift expressions with 
       non-zero delta are always materialized separetely. } 
       {li {b No input variables}: No map includes input variables in order to
        prevent creation of large expensive-to-maintain domains. } 
       {li {b Factorization} is performed after the partial materialization 
       in order to maximise reuse of common subexpressions. }
    }
    @param scope     The scope in which [expr] is materialized
    @param db_schema Schema of the database
    @param history   The history of used data structures
    @param prefix    A prefix string used to name newly created maps
    @param event     A trigger event
    @param expr      A calculus expression
    @return          A list of data structures that needs to be 
                     materialized afterwards (a todo list) together with 
                     the materialized form of the expression. 
*)                                  
let rec materialize ?(scope:var_t list = []) 
                    (heuristic_options: heuristic_options_t) 
                    (db_schema:Schema.t) (history:ds_history_t) 
                    (prefix:string) (event:Schema.event_t option) 
                    (expr:expr_t) : (ds_t list * expr_t) = 

   Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
      "[Heuristics] Asked to materialize "^
      "\n\t Expr: "^(string_of_expr expr)^
      "\n\t Event: "^(string_of_event event)^
      "\n\t Prefix: "^prefix^
      "\n\t Scope: ["^(string_of_vars scope)^"]"
   );
   
   (* Extend the scope with the trigger variables *)
   let expr_scope = match event with
      | Some(ev) -> ListAsSet.union scope (Schema.event_vars ev)
      | None -> scope
   in
   let rec merge_aggsums terms = match terms with
      | [] -> []
      | head::tail ->
         let (merged, tail_terms) = 
            List.fold_left (fun (merged, terms) term ->
               if (merged) then (merged, terms @ [term]) 
               else match (head, term) with 
                  | (CalcRing.Val(AggSum(gb1, e1)), 
                     CalcRing.Val(AggSum(gb2, e2))) 
                     when (ListAsSet.seteq gb1 gb2) &&
                          (ListAsSet.seteq (snd (schema_of_expr e1)) 
                                           (snd (schema_of_expr e2))) -> 
                     (true, 
                      terms @ [C.mk_aggsum gb1 (CalcRing.mk_sum [ e1; e2 ])])
                  | (CalcRing.Prod(CalcRing.Val(AggSum(gb1, e1)) :: 
                                   [CalcRing.Val(Value(v))]), 
                     CalcRing.Val(AggSum(gb2, e2))) 
                     when (ListAsSet.seteq gb1 gb2) && 
                          (Arithmetic.value_is_const v) &&
                          (ListAsSet.seteq (snd (schema_of_expr e1)) 
                                           (snd (schema_of_expr e2))) -> 
                     let new_e1 = CalcRing.mk_prod [ e1; C.mk_value(v) ] in
                     (true, 
                      terms @ [C.mk_aggsum gb1 (CalcRing.mk_sum [new_e1; e2])])
                  | (CalcRing.Val(AggSum(gb1, e1)), 
                     CalcRing.Prod(CalcRing.Val(AggSum(gb2, e2)) :: 
                                   [ CalcRing.Val(Value(v)) ]))
                     when (ListAsSet.seteq gb1 gb2) && 
                          (Arithmetic.value_is_const v) &&
                          (ListAsSet.seteq (snd (schema_of_expr e1)) 
                                           (snd (schema_of_expr e2))) ->
                     let new_e2 = CalcRing.mk_prod [ e2; C.mk_value(v) ] in
                     (true, 
                      terms @ [C.mk_aggsum gb1 (CalcRing.mk_sum [e1; new_e2])])
                  | _ -> (false, terms @ [term])
            ) (false, []) (merge_aggsums tail)
         in 
            if (merged) then tail_terms else (head :: tail_terms)
   in
   let (todos, mat_expr) = 
      decompose_and_fold 
            expr_scope 
            ([], CalcRing.zero) 
            ([], CalcRing.one)
            (fun (todos, mat_expr) (new_todos, new_mat_expr) -> 
               (todos @ new_todos, 
                if (Debug.active "HEURISTICS-MERGE-AGGSUMS") then
                (* merge_aggsums - a hack that allows bringing compatible
                   subexpressions back under the same AggSum to enable
                   factorization of materialized expressions (it's the 
                   opposite of CalculusTransform.nesting_rewrites); ideally,
                   the materializer shouldn't use polynomial decomposition *)  
                   let terms = CalcRing.sum_list mat_expr in
                   let new_terms = CalcRing.sum_list new_mat_expr in
                   CalcRing.mk_sum (merge_aggsums (terms @ new_terms))
                else CalcRing.mk_sum [mat_expr; new_mat_expr]))
            (fun (todos, mat_expr) (new_todos, new_mat_expr) -> 
               (todos @ new_todos, CalcRing.mk_prod [mat_expr; new_mat_expr]))
            (fun prefix schema expr -> 
                materialize_expr heuristic_options 
                                 db_schema history
                                 prefix event
                                 expr_scope schema
                                 expr)          
            prefix
            (* The materializer assumes expressions without Neg nodes.     
               When the calculus optimizer is disabled, this assumption    
               might be violated. We guard against these cases by calling  
               CalculusTransforms.normalize.                              *)
            (if not (Debug.active "CALC-NO-OPTIMIZE") then expr
             else CalculusTransforms.normalize expr)
   in
   begin
      Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
         "[Heuristics] Final materialized form: \n"^
         (CalculusPrinter.string_of_expr mat_expr)^"\n\n"
      );
      if (Debug.active "HEURISTICS-DISABLE-FINAL-OPTIMIZATION") 
      then (todos, mat_expr)
      else let schema = snd (schema_of_expr mat_expr) in
           (todos, optimize_expr (expr_scope, schema) mat_expr)
   end
    
(* Materialize an expression of the form mk_prod [ ...] *)   
and materialize_expr (heuristic_options:heuristic_options_t)
                     (db_schema:Schema.t) (history:ds_history_t) 
                     (prefix:string) (event:Schema.event_t option)
                     (scope:var_t list) (schema:var_t list) 
                     (expr:expr_t) : (ds_t list * expr_t) =

   Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
      "[Heuristics] Entering materialize_expr" ^
      "\n\t Map: "^prefix^
      "\n\t Expr: "^(string_of_expr expr)^
      "\n\t Scope: ["^(string_of_vars scope)^"]"^
      "\n\t Schema: ["^(string_of_vars schema)^"]"
   );

   (* Divide the expression into four parts *)
   let (domain_expr, delta_expr, rel_expr, lift_expr, value_expr) = 
      partition_expr heuristic_options scope event expr
   in        

   let (domain_expr_ivars, domain_expr_ovars) = schema_of_expr domain_expr in
   (* Sanity check - domain_expr should not have uncovered input variables *)
   if not (covered_by_scope scope domain_expr)
   then begin
      print_endline ("Expr: " ^ string_of_expr domain_expr);
      print_endline ("InputVars: " ^ string_of_vars domain_expr_ivars);
      failwith ("domain_expr has uncovered input variables.")
   end 
   else

   let (delta_expr_ivars, delta_expr_ovars) = schema_of_expr delta_expr in
   (* Sanity check - delta_expr should not have input variables *)
   if delta_expr_ivars <> []
   then begin
      print_endline ("Expr: " ^ string_of_expr delta_expr);
      print_endline ("InputVars: " ^ string_of_vars delta_expr_ivars);
      failwith ("delta_expr has uncovered input variables.")
   end 
   else

   let (rel_expr_ivars, rel_expr_ovars) = schema_of_expr rel_expr in
   (* Sanity check - rel_exprs should not contain any input variables,
      unless NoInputVariables = false *) 
   if (rel_expr_ivars <> [] && List.mem NoInputVariables heuristic_options)
   then begin  
      print_endline ("Expr: " ^ string_of_expr rel_expr);
      print_endline ("InputVars: " ^ string_of_vars rel_expr_ivars);
      failwith ("rel_expr has input variables.") 
   end
   else

   (**** MATERIALIZE DOMAIN_EXPR ****)
   let (todo_domains, mat_domain_expr) = 
      if domain_expr = CalcRing.one then ([], domain_expr) else

      fst (List.fold_left (fun ((todos, mats), (j, acc_scope)) term ->
         match term with
            | CalcRing.Val(DomainDelta(subexp)) ->
               let (todo, mat_subexp) =
                  if rels_of_expr subexp = [] && deltarels_of_expr subexp = []
                  then ([], subexp)   
                  else
                  let rewritten_subexp = 
                    C.rewrite_leaves
                      (fun _ lf -> match lf with 
                        | DeltaRel(rn, rv) -> C.mk_exists (C.mk_deltarel rn rv)
                        | Rel(rn, rv) -> (* C.mk_exists (C.mk_rel rn rv) *)
                            failwith "Domains with relations are unsupported"
                        | _ -> CalcRing.mk_val lf)
                      (fun _ _ -> true)
                      subexp
                  in
                  let (sub_ivars, sub_ovars) = schema_of_expr rewritten_subexp in
                  let scope_domain = ListAsSet.inter acc_scope
                     (ListAsSet.union sub_ivars sub_ovars)
                  in
                     materialize_as_external 
                        (ForceExpMaterialization :: heuristic_options) 
                        db_schema history 
                        (prefix^"_DOMAIN"^(string_of_int j))  
                        event scope_domain sub_ovars rewritten_subexp
               in
               let mat_domain = C.mk_exists mat_subexp in 
               (
                  (todos @ todo, CalcRing.mk_prod [ mats; mat_domain ]), 
                  (j + 1, ListAsSet.union acc_scope 
                                          (snd (schema_of_expr mat_domain)))
               ) 
            | _ -> bail_out term "Not a domain expression"
         ) 
         (([], CalcRing.one), (1, scope)) 
         (CalcRing.prod_list domain_expr)
      )
   in
   (* We don't extend the scope to be able to keep deltas and values together *)
   let scope_delta = scope in

   (**** MATERIALIZE DELTA_EXPR ****)
   let (todo_deltas, mat_delta_expr) = 
      if (delta_expr = CalcRing.one) then ([], CalcRing.one) else   
      (* Extend the schema with the input variables of other expressions *) 
      let schema_delta = 
         let (_, dom_ovars) = schema_of_expr mat_domain_expr in
         let rest_expr = CalcRing.mk_prod [ 
            rel_expr; lift_expr; value_expr ] in
         let (rest_ivars, rest_ovars) = schema_of_expr rest_expr in
         ListAsSet.inter 
            delta_expr_ovars
            (ListAsSet.multiunion [ schema; scope_delta; dom_ovars;
                                    rest_ivars; rest_ovars ])
      in      
         if Debug.active "HEURISTICS-NO-DELTA-MAPS" 
         then ([], C.mk_aggsum schema_delta delta_expr)
         else materialize_as_external heuristic_options db_schema 
                                      history (prefix^"_DELTA") event
                                      scope_delta schema_delta delta_expr
   in
   let scope_rel = 
      ListAsSet.multiunion [ scope; 
                             snd (schema_of_expr mat_domain_expr);
                             snd (schema_of_expr mat_delta_expr)   ]
   in
   
   (**** MATERIALIZE REL_EXPR ****)
   let (todo_rels, mat_rel_expr) = 
      if (rel_expr = CalcRing.one) then ([], CalcRing.one) else
      (* Extend the schema with the input variables of other expressions *) 
      let schema_rel = 
         let rest_expr = CalcRing.mk_prod [ lift_expr; value_expr ] in
         let (rest_ivars, rest_ovars) = schema_of_expr rest_expr in
         ListAsSet.inter 
            rel_expr_ovars
            (ListAsSet.multiunion [ schema; scope_rel; 
                                    rest_ivars; rest_ovars ])
      in
      if (Debug.active "HEURISTICS-ONLY-CYCLIC-MAPS") then 
      begin
         let (cyclic, acyclic) = 
            let terms = CalcRing.prod_list rel_expr in
            let c = CalculusDecomposition.cyclic_graph terms in
            let a = List.filter (fun x -> not (List.mem x c)) terms 
            in (c, a)
         in
         let cyclic_expr = CalcRing.mk_prod cyclic in 
         let acyclic_expr = CalcRing.mk_prod acyclic in 
         let (cyclic_ivars, cyclic_ovars) = schema_of_expr cyclic_expr in
         let (acyclic_ivars, acyclic_ovars) = schema_of_expr acyclic_expr in

         let (todo_cyclic, mat_cyclic) =
            if (cyclic_expr = CalcRing.one) then ([], CalcRing.one) else
            let schema_cyclic = 
               ListAsSet.inter 
                  cyclic_ovars 
                  (ListAsSet.multiunion 
                     [schema_rel; acyclic_ivars; acyclic_ovars])
            in
               materialize_as_external heuristic_options db_schema 
                                       history prefix event 
                                       scope_rel schema_cyclic cyclic_expr
         in
         let (todo_acyclic, mat_acyclic) = 
            if (acyclic_expr = CalcRing.one) then ([], CalcRing.one) else
            let scope_acyclic = ListAsSet.union scope_rel cyclic_ovars in
            let schema_acyclic = 
               ListAsSet.inter
                  acyclic_ovars
                  (ListAsSet.multiunion [schema_rel; scope_acyclic])
            in            
               materialize_as_external 
                     (ExtractRelationMaps :: heuristic_options) 
                     db_schema history prefix event 
                     scope_acyclic schema_acyclic acyclic_expr
         in
            (todo_cyclic @ todo_acyclic, 
             CalcRing.mk_prod [mat_cyclic; mat_acyclic])
      end
      else
         materialize_as_external heuristic_options db_schema 
                                 history prefix event 
                                 scope_rel schema_rel rel_expr
   in 

   let scope_lift = 
      ListAsSet.union scope_rel (snd (schema_of_expr mat_rel_expr))
   in

   (**** MATERIALIZE LIFT_EXPR ****)
   let (todo_lifts, mat_lift_expr) = 
      (* Extracted lifts are always materialized separately *)   
      if lift_expr = CalcRing.one then ([], lift_expr) else    

      let materialize_lift prefix scope subexpr =
         if (rels_of_expr subexpr = []) then ([], subexpr) else  
         let (sub_ivars, sub_ovars) = schema_of_expr subexpr in         
         let scope_lift = 
            ListAsSet.inter (ListAsSet.union sub_ivars sub_ovars) scope
         in
            materialize ~scope:scope_lift 
                        heuristic_options db_schema history 
                        prefix event subexpr
      in
      fst (List.fold_left (fun ((todos, mats), (j, acc_scope)) term ->

         match term with
(***** BEGIN EXISTS HACK *****)
            | CalcRing.Val(Exists(subexpr)) ->
               let (todo, mat_subexpr) = 
                  materialize_lift (prefix^"_E"^(string_of_int j)^"_") 
                                   acc_scope subexpr 
               in
               let mat_exists = Calculus.mk_exists mat_subexpr in               
               (
                  (todos @ todo, CalcRing.mk_prod [ mats; mat_exists ]), 
                  (j + 1, ListAsSet.union acc_scope 
                                          (snd (schema_of_expr mat_exists)))
               ) 
(***** END EXISTS HACK *****)
                
            | CalcRing.Val(Lift(v, subexpr)) ->
               let (todo, mat_subexpr) = 
                  materialize_lift (prefix^"_L"^(string_of_int j)^"_") 
                                   acc_scope subexpr 
               in                
               let mat_lift = Calculus.mk_lift v mat_subexpr in
               (
                  (todos @ todo, CalcRing.mk_prod [ mats; mat_lift ]),
                  (j + 1, ListAsSet.union acc_scope
                                          (snd (schema_of_expr mat_lift)))
               )

            | _  -> bail_out term "Not a lift/exists expression"
       )(([], CalcRing.one), (1, scope_lift)) (CalcRing.prod_list lift_expr)
      ) 
   in    

   Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
      "[Heuristics]  Materialized domains: "^(string_of_expr mat_domain_expr)^
      "\nMaterialized deltas: "^(string_of_expr mat_delta_expr)^
      "\nMaterialized relations: "^(string_of_expr mat_rel_expr)^
      "\nMaterialized lifts: "^(string_of_expr mat_lift_expr)
   );     
    
   (* If necessary, add aggregation to the whole materialized expression *)
   (* Hack to extend the original schema. E.g. expression {A=C} * R(C,D) *)
   (* has "D" in the output schema when evaluate with "A" and "C" in the *)
   (* scope. This hack extends the schema with "C".                      *)   
   let mat_expr = 
      CalcRing.mk_prod [ mat_domain_expr; mat_delta_expr; 
                         mat_rel_expr; mat_lift_expr; value_expr ]
   in 
   let extended_schema = 
      ListAsSet.inter (ListAsSet.union schema scope) 
                      (snd (C.schema_of_expr mat_expr))
   in
      (todo_domains @ todo_deltas @  todo_rels @ todo_lifts, 
      Calculus.mk_aggsum extended_schema mat_expr)
 

(** Perform an end-of-the line materialization on an expression.  Each stream
    relation will be turned into a map.  
    
    @param minimal_maps   (optional) Normally, only the minimal subset of the 
                          columns required to support the expression will be 
                          materialized into the generated maps.  If explicitly
                          set to false, all columns in the relation will be 
                          materialized.
    @param db_schema      Schema of the database
    @param history        The history of used data structures
    @param prefix         A prefix string used to name newly created maps
    @param expr           A calculus expression
    @return               A list of data structures that needs to be 
                          materialized afterwards (a todo list) together with 
                          the materialized form of the expression. 
*)
and extract_relations ?(minimal_maps = Debug.active "HEURISTICS-MINIMAL-MAPS")
                      (db_schema:Schema.t) (history:ds_history_t) 
                      (prefix:string) (expr_scope:var_t list) 
                      (expr_schema:var_t list) (expr:expr_t) : 
                      (ds_t list * expr_t) =                           
   let merge op _ terms: (ds_t list * expr_t) = 
      let (dses, exprs) = List.split terms in
         (List.flatten dses, op exprs)
   in
   let rcr scope schema e: (ds_t list * expr_t) = 
      extract_relations ~minimal_maps:minimal_maps 
                        db_schema history prefix
                        scope schema e
   in
   let rcr_wrap wrap scope schema e: (ds_t list * expr_t) =
      let (dses, ret_e) = rcr scope schema e in
         (dses, CalcRing.mk_val (wrap ret_e))
   in
   let is_stream rel_name = 
      let (_, _, rel_type) = Schema.rel db_schema rel_name in
         rel_type == Schema.StreamRel
   in
   let next_prefix reln = 
      raw_curr_suffix := !raw_curr_suffix + 1;
      prefix^"_raw_reln_"^(string_of_int !raw_curr_suffix)
   in
   Calculus.fold ~scope:expr_scope ~schema:expr_schema
      (merge CalcRing.mk_sum)
      (merge CalcRing.mk_prod)
      (fun _ (dses,expr) -> (dses, CalcRing.mk_neg expr))
      (fun (scope,schema) leaf -> match leaf with
         | Rel(rname,rv) when (is_stream rname) ->
            let rel_schema = 
               if minimal_maps 
               then (ListAsSet.inter rv (ListAsSet.union scope schema))
               else rv
            in
               materialize_as_external [] db_schema history 
                                       (next_prefix rname) None 
                                       scope rel_schema (CalcRing.mk_val leaf)
         | DomainDelta(subexp) -> rcr_wrap (fun x->DomainDelta(x)) 
                                           scope schema subexp
         | Exists(subexp)      -> rcr_wrap (fun x->Exists(x     )) 
                                           scope schema subexp
         | AggSum(gb_v,subexp) -> rcr_wrap (fun x->AggSum(gb_v,x)) 
                                           scope gb_v subexp
         | Lift  (v,subexp)    -> rcr_wrap (fun x->Lift  (v,x   )) 
                                           scope [] subexp
            
         | Rel _ | DeltaRel _ | External _ | Cmp _ | Value _ -> 
            ([], CalcRing.mk_val leaf)

      )
      expr

(* The final materialization step -- transforming a given expression 
   into an external map. This step also looks for the expression in 
   the history of previously materialized expressions. If not found,
   a new map is created. *)
and materialize_as_external (heuristic_options:heuristic_options_t)
                            (db_schema:Schema.t) (history:ds_history_t) 
                            (prefix:string) (event:Schema.event_t option)
                            (scope:var_t list) (schema: var_t list)
                            (expr:expr_t) : (ds_t list * expr_t) =
   
   if (rels_of_expr      expr = [] && 
       deltarels_of_expr expr = []) then ([], expr) else

   let agg_expr = Calculus.mk_aggsum schema expr in

   Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
      "Materialize as external: "^(string_of_expr agg_expr)
   );     
   
   let force = List.mem ForceExpMaterialization heuristic_options in
   
   (* Check if expr can be further decomposed     *)
   (* e.g. R(A) * S(C) * (E ^= (R(C) * S(A)))     *)   
   let (_, components) = decompose_graph scope (schema, expr) in
   
   if (not force && List.length components > 1) then
      materialize ~scope:scope
                  heuristic_options db_schema history 
                  (prefix^"_P_") event agg_expr
   else

   (* A problem might appear when we materialize expressions joining 
      three or more relations with N-1 and 1-N relationships, which 
      yields a potentially huge N x N result (e.g., observe the joins 
      around NATION in the M3 programs of TPCH5 and TPCH7). 

      To prevent that, we provide the HEURISTICS-DECOMPOSE-OVER-TABLES 
      flag that decomposes expressions in which one static table joins 
      with two or more streams. The rationale is that static tables are 
      often small, dimension tables referenced through foreign keys of 
      the stream tables. Joining one such static table with two or more 
      streams often results in a quadratic number of output tuples. *)
   let table_vars = 
      if not (Debug.active "HEURISTICS-DECOMPOSE-OVER-TABLES") then [] else 
      (* Extract names and variables of static tables *)
      let table_names = 
         List.map Schema.name_of_rel (Schema.table_rels db_schema) 
      in
      let table_vars = 
         ListAsSet.multiunion (List.map (function
            | CalcRing.Val(Rel(rn,rv)) when List.mem rn table_names -> rv
            | _ -> []) (CalcRing.prod_list expr))
      in      
      let table_scope = ListAsSet.union scope table_vars in
      (* Decompose the given expression by excluding all table variables. *)
      let (_, components) = decompose_graph table_scope (schema, expr) in
      (* Extract only components containing stream relations *)
      let rvars_components = 
         List.flatten 
            (List.map (fun (sch, cexpr) ->
               let rels = rels_of_expr cexpr in
               if (List.length rels == 0 || 
                   List.exists (fun r -> List.mem r table_names) rels) 
               then []
               else [snd (schema_of_expr cexpr)]
            ) components)
      in
      (* TODO: merge rvars components if sharing some variables (can happen 
         only with table_vars); doesn't happen with TPCH queries. *)
      let merged_rvars_components = rvars_components in
      (* If there are more than two components, we should decompose and 
         partially materialize the expression. Consider only relevant 
         table variables.  *)
      if (List.length merged_rvars_components < 2) then []
      else ListAsSet.multiunion 
              (List.map (fun rvars -> ListAsSet.inter table_vars rvars) 
                        merged_rvars_components)
   in
   if (not force && table_vars <> []) then
      materialize ~scope:(ListAsSet.union scope table_vars)
                  heuristic_options db_schema history 
                  (prefix^"_T_") event agg_expr
   else

      (*** HERE COMES THE ACTUAL MATERIALIZATION ***)   
      if (not force && List.mem ExtractRelationMaps heuristic_options) then
         extract_relations db_schema history prefix
                           scope schema expr
      else   

      (* Try to found an already existing map *)
      let (found_ds, mapping_if_found) = 
         List.fold_left (fun result i ->
            if (snd result) <> None then result 
            else (i, (cmp_exprs ~cmp_opts:CalcRing.full_cmp_opts 
                                i.ds_definition agg_expr))
         ) ( { ds_name = CalcRing.one; 
               ds_definition = CalcRing.one }, None ) !history
      in         
      begin match mapping_if_found with
         | None ->
            (* Compute the IVC expression *) 
            let expr_ivars = fst(schema_of_expr expr) in
            let (todo_ivc, ivc_expr) =
               if force then ([], None)
               else if expr_ivars <> [] then
                  let (todos, mats) = 
                     materialize ~scope:scope 
                                 [ NoIVC; NoInputVariables ] db_schema 
                                 history (prefix^"_IVC") event agg_expr
                  in 
                    (todos, Some(mats))
               else if (IVC.needs_runtime_ivc (Schema.table_rels db_schema) 
                                              agg_expr) then
                  (bail_out agg_expr
                      ("Unsupported query. " ^
                       "Cannot materialize IVC inline (yet)."))
               else ([], None)
            in
                                    
            Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
               begin match ivc_expr with
                  | None -> "[Heuristics]  ===> NO IVC <==="
                  | Some(s) -> "[Heuristics]  IVC: \n" ^ (string_of_expr s)
               end
            );
            let new_ds = {
               ds_name = Calculus.mk_external prefix expr_ivars schema
                                              (type_of_expr agg_expr) ivc_expr;
               ds_definition = agg_expr
            } in
               history := new_ds :: !history;
               (new_ds :: todo_ivc, new_ds.ds_name)
                      
         | Some(mapping) ->
            Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
               "[Heuristics] Found Mapping to: " ^
               (string_of_expr found_ds.ds_name)^
               "      With: " ^
               (ListExtras.ocaml_of_list 
                   (fun ((a, _), (b, _)) -> a ^ "->" ^ b) mapping)
            );
            ([], rename_vars mapping found_ds.ds_name)
      end