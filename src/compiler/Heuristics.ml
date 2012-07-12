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

open Types
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

type heuristic_options_t = heuristic_option_t list

(******************************************************************************)

(** A helper function for obtaining the variable list from 
    the optional event parameter. It calls Schema.event_vars 
    if the event is passed. *)
let extract_event_vars (event:Schema.event_t option) : (var_t list) =
   match event with 
      | Some(ev) ->  Schema.event_vars ev
      | None -> []

(** A helper function for extracting the relations from 
    the optional event parameter. *)
let extract_event_reln (event:Schema.event_t option) : (string option) =
   match event with
      | Some(Schema.InsertEvent((reln,_,_))) 
      | Some(Schema.DeleteEvent((reln,_,_))) -> Some(reln)
      | _  -> None

(** Obtain the human-readable representation of the optional event parameter. 
    It calls Schema.string_of_event if the event is passed. *) 
let string_of_event (event:Schema.event_t option) : string =
   match event with
      | Some(evt) -> Schema.string_of_event evt
      | _  -> "<None>" 

let string_of_expr = CalculusPrinter.string_of_expr
let string_of_vars = ListExtras.string_of_list string_of_var

let covered_by_scope scope expr = 
   let expr_ivars = fst (Calculus.schema_of_expr expr) in
      ListAsSet.diff expr_ivars scope = []


(** Reorganize a given expression such that: 1) terms depending only on the
    trigger variables are pulled out upfront; 2) the relation terms are 
    pushed to the left but after constant terms 3) comparisons and values 
    are pushed to the right. The partial order inside each of the group 
    (constants, relations, lifts, and values) is preserved. The method 
    assumes an aggsum-free product-only representation of the expression. *)
let prepare_expr (scope:var_t list) (expr:expr_t) : 
                 (term_list * term_list * term_list * term_list) =
   let expr_terms = CalcRing.prod_list expr in

   (* Extract constant terms *)
   let (const_terms, rest_terms) = 
      List.fold_left (fun (const_terms, rest_terms) term ->
         match CalcRing.get_val term with
            | Value(_) | Cmp(_, _, _) ->
               if covered_by_scope scope term
               then (const_terms @ [term], rest_terms)
               else (const_terms, rest_terms @ [term])
            | _ -> (const_terms, rest_terms @ [term])
      ) ([], []) expr_terms
   in

   (* Reorganize the rest *)
   let (rel_terms, lift_terms, value_terms) = 
       List.fold_left (fun (rels, lifts, values) term ->
          match CalcRing.get_val term with
             | Value(_) | Cmp(_, _, _) -> (rels, lifts, values @ [term])
             | Rel (_, _) | External (_) -> (rels @ [term], lifts, values)
(***** BEGIN EXISTS HACK *****)
             | Exists(_)
(***** END EXISTS HACK *****) 
             | Lift (_, _) -> (rels, lifts @ [term], values)
             | AggSum (_, _) ->
                bail_out expr ("[prepare_expr] Error: " ^ 
                               "AggSums are supposed to be removed.")
      ) ([], [], []) rest_terms
   in
      (const_terms, rel_terms, lift_terms, value_terms)    

(*****************************************************************************)

type decision_options_t = 
   | MaterializeAsNewMap
   | MaterializeUnknown


(** Split an expression into four parts: 
    {ol
      {li value terms that depends solely on the trigger variables }
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
                   (expr_t * expr_t * expr_t * expr_t) =

   Debug.print "LOG-HEURISTICS-PARTITIONING" (fun () -> 
      "[Heuristics]  Partitioning - Entering partition_expr: " ^ 
      "\n\t Expr: " ^ (string_of_expr expr)
   ); 

   let expr_ivars = fst (schema_of_expr expr) in

   let (const_terms, rel_terms, lift_terms, value_terms) = 
      prepare_expr scope expr 
   in
   
   Debug.print "LOG-HEURISTICS-PARTITIONING" (fun () -> 
      "[Heuristics]  Partitioning - after preparation: " ^ 
      "\n\t Const: "    ^ (string_of_expr (CalcRing.mk_prod const_terms)) ^
      "\n\t Relation: " ^ (string_of_expr (CalcRing.mk_prod rel_terms  )) ^
      "\n\t Lift: "     ^ (string_of_expr (CalcRing.mk_prod lift_terms )) ^
      "\n\t Value:  "   ^ (string_of_expr (CalcRing.mk_prod value_terms)) 
   ); 
      
   let maps_no_ivc = List.mem NoIVC heuristic_options in
   let inputvar_allowed = not (List.mem NoInputVariables heuristic_options) in
   let has_root_relation = (rel_terms <> []) in
   
   let final_const_expr = CalcRing.mk_prod const_terms in
      
   (* Pull in covered lift terms *)
   let (new_rl_terms, new_l_terms) = 
   begin 
      let rel_expr = CalcRing.mk_prod rel_terms in

      (* Sanity check - rel_expr should be covered by the scope *)
      if not (covered_by_scope scope rel_expr)
      then failwith "rel_expr is not covered by the scope."
      else     
         
      let rel_expr_ovars = snd (schema_of_expr rel_expr) in         
                  
      (* We have to decide on which lift terms should be  *)
      (* pulled in rel_terms. Here are some examples,     *)
      (* assuming inputvar_allowed = false                *)
      (* e.g. R(A,B) * (A ^= 0) --> Yes                   *)
      (*      R(A,B) * (D ^= 0) * (D ^= {A=C}) --> No     *)
      (*      R(A,B) * (C ^= 1) * (A ^= C) --> Yes        *)
      (*      R(A,B) * (C ^= D) * (A ^= C) --> No         *)
      (* Essentially, we perform graph decomposition over *)
      (* the lift and value terms. For each term group,   *)
      (* we make the same decision: either all terms are  *)
      (* going to be pulled in, or none of them. The      *)
      (* invariant that we want to preserve is the set of *)
      (* output variables of the relation term.           *)
      
                              
      (* Preprocessing step to mark those lifts that are  *)
      (* going to be materialized separately for sure.    *)
      (* Essentially, all lifts are singletons and are    *)
      (* always materialize separately. *)
      let lift_terms_annot = List.map (fun l_term ->
         match CalcRing.get_val l_term with
(***** BEGIN EXISTS HACK *****)
            | Exists(subexpr) -> 
               (* If there is no root relations, materialize *)
               (* subexp in order to avoid the need for IVC  *)
               if maps_no_ivc && not has_root_relation
               then (l_term, MaterializeAsNewMap)
               else

               let subexpr_rels = rels_of_expr subexpr in
               if subexpr_rels <> [] then 
               begin                  
                  (* The lift subexpression containing the event *)
                  (* relation is always materialized separately  *)
                  let lift_contains_event_rel =
                     match extract_event_reln event with
                        | Some(reln) -> List.mem reln subexpr_rels
                        | None -> false
                  in
                  if lift_contains_event_rel
                  then (l_term, MaterializeAsNewMap)
                  else 
                     if Debug.active "HEURISTICS-PULL-IN-LIFTS" 
                     then (l_term, MaterializeUnknown)
                     else (l_term, MaterializeAsNewMap)
               end
               else (l_term, MaterializeUnknown)
(***** END EXISTS HACK *****)
            | Lift(_, subexpr) -> 
               (* If there is no root relations, materialize *)
               (* subexp in order to avoid the need for IVC  *)
               if maps_no_ivc && not has_root_relation
               then (l_term, MaterializeAsNewMap)
               else
                  
               let subexpr_rels = rels_of_expr subexpr in
               if subexpr_rels <> [] then 
               begin   
                  (* The lift subexpression containing the event *)
                  (* relation is always materialized separately  *)
                  let lift_contains_event_rel =
                     match extract_event_reln event with
                        | Some(reln) -> List.mem reln subexpr_rels
                        | None -> false
                  in
                  if lift_contains_event_rel
                  then (l_term, MaterializeAsNewMap)
                  else 
                     if Debug.active "HEURISTICS-PULL-IN-LIFTS" 
                     then (l_term, MaterializeUnknown)
                     else (l_term, MaterializeAsNewMap)
               end
               else (l_term, MaterializeUnknown)

            | _ -> failwith "Not a lift term"         
      ) lift_terms in
      
      let value_terms_annot = List.map (fun v_term ->
         match CalcRing.get_val v_term with
            | Value(_) | Cmp(_, _, _) -> (v_term, MaterializeUnknown)
            | _ -> failwith "Not a value term"
      ) value_terms in
      
      (* Graph decomposition over the lift and value terms *)      
      let scope_lift =
         (* Scope is extended with rel_expr_ovars *) 
         ListAsSet.union scope rel_expr_ovars
      in
      let get_vars term_annot = 
         let i,o = C.schema_of_expr (fst term_annot) in 
         ListAsSet.diff (ListAsSet.union i o) scope_lift
      in     
      let graph_components = 
         HyperGraph.connected_unique_components get_vars 
            (lift_terms_annot @ value_terms_annot)
      in
      let get_graph_component term_annot = 
         List.find (fun terms_annot -> 
            List.mem term_annot terms_annot
         ) graph_components
      in
      
      List.fold_left (
         fun (r_terms, l_terms) (l_term, l_annot) ->
            match CalcRing.get_val l_term with
(***** BEGIN EXISTS HACK *****)
               | Exists(subexpr)  
(***** END EXISTS HACK *****)
               | Lift(_, subexpr) -> 
                  if l_annot = MaterializeAsNewMap 
                  then (r_terms, l_terms @ [l_term])
                  else begin 
                  try 
                     let graph_cmpnt = 
                        get_graph_component (l_term, l_annot) 
                     in
                     if (List.exists (fun (_, annot) -> 
                            annot = MaterializeAsNewMap) graph_cmpnt) 
                     then (r_terms, l_terms @ [l_term])
                     else begin
                        let graph_cmpnt_expr =  
                           CalcRing.mk_prod (List.map fst graph_cmpnt)
                        in
                        if covered_by_scope rel_expr_ovars graph_cmpnt_expr
                        then (r_terms @ [l_term], l_terms)
                        else if covered_by_scope 
                                    (ListAsSet.union scope rel_expr_ovars)
                                    graph_cmpnt_expr
                        then (r_terms, l_terms @ [l_term])
                        else bail_out expr 
                                "The lift term is not covered by the scope."
                     end   
                  with Not_found -> failwith "The lift term cannot be found."
                  end                 
               | _ -> failwith "Not a lift term"
      ) (rel_terms, []) lift_terms_annot                  
   end
   in
   
   let final_lift_expr = CalcRing.mk_prod new_l_terms in
   
   (* Pull in covered value terms *)
   let (new_rlv_terms, new_v_terms) =
   begin
      let rel_expr = CalcRing.mk_prod new_rl_terms in
   
      (* Sanity check - rel_expr should be covered by the scope *)
      if not (covered_by_scope scope rel_expr)
      then failwith "rel_expr is not covered by the scope."
      else                  

      let rel_lift_expr = CalcRing.mk_prod (new_rl_terms @ new_l_terms) in
      
      (* Sanity check - rel_lift_expr should be covered by the scope *)
      if not (covered_by_scope scope rel_lift_expr)
      then failwith "rel_lift_expr is not covered by the scope."
      else
      
      let rel_expr_ovars = snd (schema_of_expr rel_expr) in
      
      List.fold_left (fun (r_terms, v_terms) v_term ->
         match CalcRing.get_val v_term with
            | Value(_) | Cmp (_, _, _) ->               
               if covered_by_scope rel_expr_ovars v_term ||
                  (inputvar_allowed &&
                      covered_by_scope (ListAsSet.union rel_expr_ovars
                                                        expr_ivars)
                                        v_term)
               then (r_terms @ [v_term], v_terms) 
               else (r_terms, v_terms @ [v_term])
            | _ -> failwith "Not a value term."
      ) (new_rl_terms, []) value_terms
   end
   in
   let final_rel_expr = CalcRing.mk_prod new_rlv_terms in
   let final_value_expr = CalcRing.mk_prod new_v_terms in
      Debug.print "LOG-HEURISTICS-PARTITIONING" (fun () -> 
         "[Heuristics]  Partitioning - done: " ^ 
         "\n\t Const: "    ^ (string_of_expr final_const_expr) ^
         "\n\t Relation: " ^ (string_of_expr final_rel_expr  ) ^
         "\n\t Lift: "     ^ (string_of_expr final_lift_expr ) ^
         "\n\t Value:  "   ^ (string_of_expr final_value_expr) 
      ); 
      (final_const_expr, final_rel_expr, 
       final_lift_expr,  final_value_expr)


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
            
    In general, the subexpressions of the given expression can be divided into 
    three categories:  
    {ol
       {li base relations, irrelevant lift expressions with respect to the 
           event relation that also contain no input variables, and 
           subexpressions with no input variables (comparisons, variables 
           and constants) }  
       {li lift expressions containing the event relation or input variables }
       {li subexpressions with input variables and the above lift variables } 
    }
    The rule for making the decision is the following:
    If there is an equality constraint between the base relations (category I) 
    and lift subexpressions (category II), i.e. the variables of the lift 
    subexpressions are also output variables of the base relations, then 
    the expression should be incrementally maintained. The rationale    behind 
    this decision is that deltas of the lift subexpressions affect only a subset
    of the tuples, thus bounding the variables used in the outside expression
    and avoiding the need to iterate over the whole domain of values.
    Otherwise, if there is no overlapping between the set of variables used 
    inside the lift subexpressions and the rest of the query, the expression 
    is more efficient to reevalute on each trigger call.
    
    Two special cases arise when the given expression does not contain lift
    subexpressions, or it does not contain relation subexpressions. In both 
    cases, this method suggests the incremental maintenance.
*)
let should_update (event:Schema.event_t) (expr:expr_t)  : bool =
   
   if (Debug.active "HEURISTICS-ALWAYS-UPDATE") then true 
   else
   
   let expr_scope = Schema.event_vars event in
   
   let rec get_maintaining_option expr =      
      let rcr = get_maintaining_option in      
      let prefer_replace = Debug.active "HEURISTICS-PREFER-REPLACE" in
      (* A simple state machine to decide on the next maintaining option *)
      let get_next_state current_state next_state =
         match (current_state, next_state) with
            | (x, y) when x = y -> x
            | (Unknown, x) | (x, Unknown) -> x
            | (UpdateExpr, ReplaceExpr) 
            | (ReplaceExpr, UpdateExpr) -> if prefer_replace 
                                           then ReplaceExpr
                                           else UpdateExpr
            | _ -> failwith "Unable to process next state."
      in
      
      (* Polynomial decomposition *)
      List.fold_left ( fun state (term_schema, term) ->
        
         let term_opt = optimize_expr (expr_scope, term_schema) term in
    
         if not (CalcRing.try_cast_to_monomial term_opt) then 
            get_next_state state (rcr term_opt) 
         else

         (* Graph decomposition *)
         let maintaining_state_graph =  
            List.fold_left ( fun state_graph (subexpr_schema, subexpr) ->
                            
               (* Subexpression optimization *)                    
               let subexpr_opt = 
                  optimize_expr (expr_scope, subexpr_schema) subexpr 
               in

               if not (CalcRing.try_cast_to_monomial subexpr_opt) then 
                  get_next_state state_graph (rcr subexpr_opt)
               else
               
               let maintaining_state_local = fst (
                  List.fold_left (fun (state_local, scope_acc) term ->
                     let term_ovars = snd (schema_of_expr term) in
                     (* We are only interested in Lift expressions *)
                     match CalcRing.get_val term with
(***** BEGIN EXISTS HACK *****)
                     | Exists(subexpr)
(***** END EXISTS HACK *****)
                     | Lift(_, subexpr) ->
                        let subexpr_rels = rels_of_expr subexpr in
                        let contains_event_rel =
                           match extract_event_reln (Some(event)) with
                              | Some(reln) -> List.mem reln subexpr_rels
                              | None -> false
                        in
                        if contains_event_rel then 
                           let next_state = 
                              if ListAsSet.inter scope_acc term_ovars = [] 
                              then ReplaceExpr
                              else UpdateExpr
                           in
                              (get_next_state state_local next_state,
                               ListAsSet.union scope_acc term_ovars)
                        else (state_local,
                              ListAsSet.union scope_acc term_ovars)
                     | _ ->
                        (state_local,
                         ListAsSet.union scope_acc term_ovars)
                  ) (Unknown, expr_scope) (CalcRing.prod_list subexpr_opt)
               ) 
               in
                  get_next_state state_graph maintaining_state_local
            
            ) Unknown 
              (snd (decompose_graph expr_scope (term_schema, term_opt)))
         in
            get_next_state state maintaining_state_graph
                            
      ) Unknown (decompose_poly expr)
   in      
      match get_maintaining_option expr with
         | Unknown | UpdateExpr -> true
         | ReplaceExpr -> false 

      

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
      "[Heuristics] "^(string_of_event event)^
      "\n\t Map: "^prefix^
      "\n\t Expr: "^(string_of_expr expr)^
      "\n\t Scope: ["^(string_of_vars scope)^"]"
   );
   
   if List.mem ExtractRelationMaps heuristic_options then 
      materialize_relations db_schema
                            history
                            prefix
                            expr
   else
   let expr_scope = ListAsSet.union scope (extract_event_vars event) in
   let (todos, mat_expr) = fst (
      (* Polynomial decomposition *)
      List.fold_left ( fun ((term_todos, term_mats), i) (term_schema, term) ->

         let term_opt = optimize_expr (expr_scope, term_schema) term in
         
         if not (CalcRing.try_cast_to_monomial term_opt) 
         then begin
            Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
               "[Heuristics] Not a monomial in PD: " ^
               (string_of_expr term_opt)
            );
            let term_name = (prefix^(string_of_int i)) in
            
            let (new_term_todos, new_term_mats) = 
               materialize ~scope:scope heuristic_options 
                           db_schema history term_name event 
                           (Calculus.mk_aggsum term_schema term_opt)
            in
               ((term_todos @ new_term_todos,
                 CalcRing.mk_sum [term_mats; new_term_mats]),
                i + 1)
         end
         else begin

            Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
               "[Heuristics] PolyDecomposition Before Optimization: " ^ 
               (string_of_expr term) ^
               "\n[Heuristics] PolyDecomposition + Optimization: " ^ 
               (string_of_expr term_opt) ^
               "\n\t Scope: [" ^ (string_of_vars expr_scope) ^ "]" ^
               "\n\t Schema: [" ^ (string_of_vars term_schema) ^ "]"
            ); 

            (* Graph decomposition *)
            let ((new_term_todos, new_term_mats), k) =  
               List.fold_left ( fun ((todos, mat_expr), j) 
                                    (subexpr_schema, subexpr) ->

                  let subexpr_name = (prefix^(string_of_int j)) in
                                                
                  (* Subexpression optimization *)                    
                  let subexpr_opt = 
                     optimize_expr (expr_scope, subexpr_schema) subexpr 
                  in

                  if not (CalcRing.try_cast_to_monomial subexpr_opt) 
                  then begin
                     Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                        "[Heuristics] Not a monomial in GD: " ^
                        (string_of_expr subexpr_opt)
                     );
                     let (todos_subexpr, mat_subexpr) = 
                        materialize ~scope:scope heuristic_options 
                                    db_schema history subexpr_name event 
                                    (Calculus.mk_aggsum subexpr_schema 
                                                        subexpr_opt)
                     in
                        ((todos @ todos_subexpr,
                          CalcRing.mk_prod [mat_expr; mat_subexpr]),
                         j + 1)
                  end
                  else begin 
                                
                     Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                        "[Heuristics] Graph decomposition: " ^
                        "\n\t Expr: " ^ (string_of_expr subexpr) ^
                        "\n\t Expr opt: " ^ (string_of_expr subexpr_opt) ^
                        "\n\t Scope: [" ^ (string_of_vars expr_scope)^"]" ^
                        "\n\t Schema: [" ^ (string_of_vars subexpr_schema)^"]"^
                        "\n\t MapName: " ^ subexpr_name
                     );
        
                     (* It is possible that the optimizer breaks the   *)
                     (* assumption of having an aggsum-free expression.*)
                     let contains_aggsum expr = 
                        CalcRing.fold (List.fold_left (||) false) 
                                      (List.fold_left (||) false) 
                                      (fun x -> x)
                                      (fun lf -> begin match lf with
                                          | AggSum(_, _) -> true
                                          | _ -> false
                                       end) expr
                     in
                     let (todos_subexpr, mat_subexpr) = 
                        if contains_aggsum subexpr_opt then
                        begin
                           Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                              "[Heuristics] Not an aggsum-free expression: " ^
                              (string_of_expr subexpr_opt)
                           );
                           materialize ~scope:scope heuristic_options 
                                       db_schema history subexpr_name event 
                                       (Calculus.mk_aggsum subexpr_schema 
                                                           subexpr_opt)
                        end
                        else begin
                           materialize_expr heuristic_options db_schema history
                                            subexpr_name event expr_scope 
                                            subexpr_schema subexpr_opt
                        end 
                     in
                     Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                        "[Heuristics] Materialized form: " ^
                        (string_of_expr mat_subexpr)
                     );
                     ( ( todos @ todos_subexpr, 
                         CalcRing.mk_prod [mat_expr; mat_subexpr] ), 
                       j + 1)  
                  end
               ) (([], CalcRing.one), i)
                  (snd (decompose_graph expr_scope (term_schema, term_opt)))
            in
               ( ( term_todos @ new_term_todos, 
                   CalcRing.mk_sum [term_mats; new_term_mats] ), 
                 k)
         end           
      ) (([], CalcRing.zero), 1) (decompose_poly expr)
   )
   in begin
      Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
         "[Heuristics] Final Materialized Form: \n"^
         (CalculusPrinter.string_of_expr mat_expr)^"\n\n"
      );
      if (Debug.active "HEURISTICS-DISABLE-FINAL-OPTIMIZATION") 
      then (todos, mat_expr)
      else 
         let schema = snd (schema_of_expr mat_expr) in
            (todos, optimize_expr (expr_scope, schema) mat_expr)
   end
    
(* Materialization of an expression of the form mk_prod [ ...] *)   
and materialize_expr (heuristic_options:heuristic_options_t)
                     (db_schema:Schema.t) (history:ds_history_t) 
                     (prefix:string) (event:Schema.event_t option)
                     (scope:var_t list) (schema:var_t list) 
                     (expr:expr_t) : (ds_t list * expr_t) =

   Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
      "[Heuristics] Entering materialize_expr" ^
      "\n\t Map: "^prefix^
      "\n\t Expr: "^(string_of_expr expr)^
      "\n\t Scope: ["^(string_of_vars scope)^"]"
   );

   (* Divide the expression into four parts *)
   let (const_expr, rel_expr, lift_expr, value_expr) = 
      partition_expr heuristic_options scope event expr
   in        

   let (const_expr_ivars, const_expr_ovars) = schema_of_expr const_expr in
   let (rel_expr_ivars, rel_expr_ovars) = schema_of_expr rel_expr in
   let (lift_expr_ivars, lift_expr_ovars) = schema_of_expr lift_expr in
   let (value_expr_ivars, value_expr_ovars) = schema_of_expr value_expr in
      
   (* Sanity check - const_expr should not have input variables *)
   if ListAsSet.diff const_expr_ivars scope <> [] then begin
      print_endline ("Expr: " ^ string_of_expr const_expr);
      print_endline ("InputVars: " ^ string_of_vars const_expr_ivars);
      failwith "const_expr has input variables."
   end 
   else
   
   let scope_const = ListAsSet.union scope const_expr_ovars in
   if (List.mem NoInputVariables heuristic_options)
   then begin
      (* Sanity check - rel_exprs should not contain any input variables *) 
      if rel_expr_ivars <> [] then begin  
         print_endline ("Expr: " ^ string_of_expr rel_expr);
         print_endline ("InputVars: " ^ string_of_vars rel_expr_ivars);
         failwith ("rel_expr has input variables.") 
      end
   end;
                     
   (* Extended the schema with the input variables of other expressions *) 
   let extended_schema = 
      ListAsSet.multiunion [ schema;
                             scope_const;
                             lift_expr_ivars;
                             (* e.g. ON +S(dA)             *)
                             (*         R(A) * (C ^= S(A)) *)
                             lift_expr_ovars;
                             value_expr_ivars ]
   in

   let agg_rel_expr = Calculus.mk_aggsum extended_schema rel_expr in
   let (agg_rel_expr_ivars, agg_rel_expr_ovars) = 
      schema_of_expr agg_rel_expr 
   in

   (* Extracted lifts are always materialized separately *)   
   let (todo_lifts, mat_lift_expr) = 
      if lift_expr = CalcRing.one then ([], lift_expr) else      
      fst (
          List.fold_left (fun ((todos, mats), (j, whole_expr)) lift ->
             match (CalcRing.get_val lift) with
(***** BEGIN EXISTS HACK *****)
                | Exists(subexpr) ->
                  let (todo, mat_expr) =
                     if rels_of_expr subexpr = []
                     then ([], subexpr)
                     else begin 
                        let scope_lift = 
                           ListAsSet.union scope_const
                                           (snd (schema_of_expr whole_expr)) 
                        in
                           materialize ~scope:scope_lift heuristic_options 
                                       db_schema history 
                                       (prefix^"_E"^(string_of_int j)^"_") 
                                       event subexpr
                     end 
                  in
                  let mat_lift_expr = Calculus.mk_exists mat_expr in
                     ((todos @ todo, CalcRing.mk_prod [mats; mat_lift_expr]), 
                      (j + 1, CalcRing.mk_prod [whole_expr; mat_lift_expr])) 
(***** END EXISTS HACK *****)
                | Lift(v, subexpr) ->
                  let (todo, mat_expr) =
                     if rels_of_expr subexpr = []
                     then ([], subexpr)
                     else begin 
                        let scope_lift = 
                           ListAsSet.union scope_const
                                           (snd (schema_of_expr whole_expr)) 
                        in
                           materialize ~scope:scope_lift heuristic_options 
                                       db_schema history 
                                       (prefix^"_L"^(string_of_int j)^"_") 
                                       event subexpr
                     end 
                  in
                  let mat_lift_expr = Calculus.mk_lift v mat_expr in
                     ((todos @ todo, CalcRing.mk_prod [mats; mat_lift_expr]), 
                      (j + 1, CalcRing.mk_prod [whole_expr; mat_lift_expr]))
                | _  ->
                   bail_out lift "Not a lift expression"
          ) (([], CalcRing.one), (1, agg_rel_expr)) 
            (CalcRing.prod_list lift_expr) 
      ) 
   in  
      
   Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
      "[Heuristics]  Relation AggSum expression: " ^ 
      (string_of_expr agg_rel_expr) ^
      "\n\t Scope: [" ^ (string_of_vars scope_const) ^ "]" ^
      "\n\t Const OutVars: [" ^ (string_of_vars const_expr_ovars) ^ "]" ^
      "\n\t Lift InpVars: [" ^ (string_of_vars lift_expr_ivars) ^ "]" ^
      "\n\t Lift OutVars: [" ^ (string_of_vars lift_expr_ovars) ^ "]" ^
      "\n\t Value InpVars: [" ^ (string_of_vars value_expr_ivars) ^ "]" ^
      "\n\t Relation InVars: [" ^ (string_of_vars agg_rel_expr_ivars) ^ "]" ^
      "\n\t Relation OutVars: [" ^ (string_of_vars agg_rel_expr_ovars) ^ "]" ^
      "\n\t Original schema: [" ^ (string_of_vars schema) ^ "]" 
   ); 
      
   (* The actual materialization of agg_rel_expr *)   
   let (todos, complete_mat_expr) = 
      if (rels_of_expr rel_expr) = [] 
      then (todo_lifts, 
            CalcRing.mk_prod [ const_expr; 
                               agg_rel_expr; 
                               mat_lift_expr; 
                               value_expr ])
      else        
         (* Try to found an already existing map *)
         let (found_ds, mapping_if_found) = 
            List.fold_left (fun result i ->
               if (snd result) <> None then result 
               else (i, (cmp_exprs i.ds_definition agg_rel_expr))
            ) ( { ds_name = CalcRing.one; 
                 ds_definition = CalcRing.one}, None) !history
         in 
         begin match mapping_if_found with
            | None ->
               (* Compute the IVC expression *) 
               let (todo_ivc, ivc_expr) =
                  if agg_rel_expr_ivars <> [] then
                     let (todos, mats) =  
                        materialize [ NoIVC; NoInputVariables ]
                                    db_schema history
                                    (prefix^"_IVC")
                                    event agg_rel_expr
                    in 
                       (todos, Some(mats))
                  else if (IVC.needs_runtime_ivc (Schema.table_rels db_schema)
                                                       agg_rel_expr) then
                     (bail_out agg_rel_expr
                         ("Unsupported query. " ^
                          "Cannot materialize IVC inline (yet)."))
                  else
                     ([], None)
               in
                                       
               Debug.print "LOG-HEURISTICS-DETAIL" (fun () ->
                  begin match ivc_expr with
                     | None -> "[Heuristics]  ===> NO IVC <==="
                     | Some(s) -> "[Heuristics]  IVC: \n" ^ (string_of_expr s)
                  end
               );
               let new_ds = {
                  ds_name = Calculus.mk_external 
                               prefix
                               agg_rel_expr_ivars
                               agg_rel_expr_ovars
                               (type_of_expr agg_rel_expr)
                               ivc_expr;
                  ds_definition = agg_rel_expr;
               } in
                  history := new_ds :: !history;
                  ([new_ds] @ todo_lifts @ todo_ivc, 
                   CalcRing.mk_prod [ const_expr; 
                                      new_ds.ds_name; 
                                      mat_lift_expr; 
                                      value_expr ])
                         
            | Some(mapping) ->
               Debug.print "LOG-HEURISTICS-DETAIL" (fun () -> 
                  "[Heuristics] Found Mapping to: " ^
                  (string_of_expr found_ds.ds_name)^
                  "      With: " ^
                  (ListExtras.ocaml_of_list 
                      (fun ((a, _), (b, _)) -> a ^ "->" ^ b) mapping)
               );
               (todo_lifts, 
                CalcRing.mk_prod [ const_expr; 
                                   (rename_vars mapping found_ds.ds_name); 
                                   mat_lift_expr; 
                                   value_expr ])
         end
   in
   (* If necessary, add aggregation to the whole materialized expression *)
   let agg_mat_expr = Calculus.mk_aggsum schema complete_mat_expr in
      (todos, agg_mat_expr)


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
and materialize_relations ?(minimal_maps = true) (db_schema:Schema.t) 
                          (history:ds_history_t) (prefix:string)
                          (expr:expr_t): (ds_t list * expr_t) =
   let merge op _ terms: (ds_t list * expr_t) = 
      let (dses, exprs) = List.split terms in
         (List.flatten dses, op exprs)
   in
   let rcr e: (ds_t list * expr_t) = 
      materialize_relations ~minimal_maps:minimal_maps 
                            db_schema
                            history
                            prefix
                            e
   in
   let rcr_wrap wrap e: (ds_t list * expr_t) =
      let (dses, ret_e) = rcr e in
         (dses, CalcRing.mk_val (wrap ret_e))
   in
   let is_stream rel_name = 
      let (_, _, rel_type) = Schema.rel db_schema rel_name in
         rel_type == Schema.StreamRel
   in
   let curr_suffix = ref 0 in
   let next_prefix reln = 
      curr_suffix := !curr_suffix + 1;
      prefix^"_raw_reln_"^(string_of_int !curr_suffix)
   in
   let (expr_scope, expr_schema) = Calculus.schema_of_expr expr in
   Calculus.fold ~scope:expr_scope ~schema:expr_schema
      (merge CalcRing.mk_sum)
      (merge CalcRing.mk_prod)
      (fun _ (dses,expr) -> (dses, CalcRing.mk_neg expr))
      (fun (scope,schema) leaf -> match leaf with
         | Rel(rname,rv) when (is_stream rname) ->
            if minimal_maps then
               materialize_expr []
                                db_schema
                                history
                                (next_prefix rname)
                                None
                                scope
                                schema
                                (CalcRing.mk_val leaf)
            else
               let (dses, mat_expr) = 
                  materialize_expr []
                                   db_schema
                                   history
                                   (next_prefix rname)
                                   None
                                   scope
                                   (ListAsSet.diff rv scope)
                                   (CalcRing.mk_val leaf)
               in
               ( dses,
                 Calculus.mk_aggsum 
                    (ListAsSet.inter rv (ListAsSet.union scope schema))
                    mat_expr                  
               )
         | Exists(subexp)      -> rcr_wrap (fun x->Exists(x     )) subexp
         | AggSum(gb_v,subexp) -> rcr_wrap (fun x->AggSum(gb_v,x)) subexp
         | Lift  (v,subexp)    -> rcr_wrap (fun x->Lift  (v,x   )) subexp
            
         | Rel _ | External _ | Cmp _ | Value _ -> ([], CalcRing.mk_val leaf)

      )
      expr