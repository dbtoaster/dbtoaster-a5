(**
   Tools for transforming a SQL parse tree (SQL.expr_t) and database definition 
   into Calculus expressions, and DBToaster's internal Schema.t representation.
*)

open Ring
open Type
open Constants
open Arithmetic
open Calculus

(**/**)
module C = Calculus

let mk_var_name = 
   FreshVariable.declare_class "calculus/SqlToCalculus" "sql_inline"

let tmp_var (vn:string) (vt:type_t): var_t = 
   (mk_var_name ~inline:("_"^vn) (), vt)
(**/**)

(** 
   This utility function performs conversion from arbitrary calculus expressions
   to values (e.g., so that the expression values can be compared).  It does 
   this by lifting the expression into a newly created variable (if necessary).
   
   Note that introducing a variable necessitates its subsequent removal from the
   scope.  Thus, any expression containing the return value(s) of this function
   must be surrounded by an appropriately grouped AggSum.
   
   Also note that in most cases where this function is used below (conditions 
   and expressions), no new variables/columns will be introduced.  Thus, it is
   safe to just use an AggSum with no group by variables.
   
   It should also be safe to leave these values ungrouped, since they'll get 
   grouped together into the aggsum on the outside.  However, there are a few
   corner cases where a lift can occur outside of an aggregate, so it's better
   to be safe. 
   *)
let lift_if_necessary ?(t="agg") ?(vt=TAny) (calc:C.expr_t):
                      (value_t * C.expr_t) = 
   (* We combine values together as aggressively as possible so as to avoid 
      lifts at all costs. A lift will never be split apart anyway, so this  
      condition will bridge sub(query)graphs regardless of whether we merge 
      here or not. *)
   match (CalculusTransforms.combine_values ~aggressive:true calc) with
      | CalcRing.Val(Value(x)) -> (x, CalcRing.one)
      | _ -> 
         let v = tmp_var t (Type.escalate_type vt (C.type_of_expr calc)) in
         (  Arithmetic.mk_var v, C.mk_lift v calc  )

(**
   [lower_if_value calc]

   This utility function removes the lift around an expression if the
   expression consists only of a value. In any case, the function
   returns a value that holds the result of the expression (either
   a reference to a variable or the value itself).

   @param calc The expression to be processed
   @return     A tuple of the value holding the result of the expression
               and the expression itself
   *)
let lower_if_value (calc:C.expr_t): (value_t * C.expr_t) = 
   match calc with
   | CalcRing.Val(C.Lift(v, CalcRing.Val(Value(x)))) -> 
      (x, CalcRing.one)
   | CalcRing.Val(C.Lift(v, c)) -> (Arithmetic.mk_var v, c)
   | CalcRing.Val(AggSum(v :: [], c)) -> (Arithmetic.mk_var v, calc)
   | _ -> failwith ("Expected Val, found: " ^ (C.string_of_expr calc))

(**
   [var_of_sql_var var]
   
   Translate a bound SQL variable to a standard DBToaster variable.  This 
   involves (a consistent) renaming (of) the variable to ensure its uniqueness.
   @param var  A SQL variable
   @return     The [Type]-style variable corresponding to [var]
*)
let var_of_sql_var ((rel,vn,vt):Sql.sql_var_t):var_t = 
   begin match rel with
      (* If preprocess has been called, all the vars should be bound to a
         relation instance in a query. *)
      | Some(reln) -> (reln^"_"^vn, vt)
      | None       -> failwith ("Unbound var "^vn)
   end

(**
   [cast_query_to_aggregate tables query]
   
   Cast an arbitrary query into an aggregate query.  If the query is already an
   aggregate query, it is returned unchanged.  Otherwise, the query is turned
   into a count query.
   @param tables The schema of the database on which the query is being run
   @param stmt   A SQL query
   @return       [stmt] rewritten to be an aggregate query.
*)
let rec cast_query_to_aggregate (tables:Sql.table_t list)
                            (query:Sql.select_t):Sql.select_t =
   if Sql.is_agg_query query
   then (
      Debug.print "LOG-SQL-TO-CALC" (fun () -> 
         "Compiling Unchanged: "^(Sql.string_of_select query)
      );
      query
   )
   else (
      match query with 
      | Sql.Select(targets,sources,cond,gb_vars,having,opts) ->
         let (new_targets,new_gb_vars) = 
            Debug.print "LOG-SQL-TO-CALC" (fun () -> 
               "Casting to Aggregate: "^(Sql.string_of_select query)
            );
            if gb_vars <> [] then (
               Sql.error "Non-aggregate query with group-by variables";
            );
            (  targets @ ["COUNT", Sql.Aggregate(  
                        Sql.CountAgg(if List.mem Sql.Select_Distinct opts
                                     then Some([]) else None), 
                        (Sql.Const(CInt(1))))],
               (List.map (fun (target_name, target_expr) ->
                  let target_source = begin match target_expr with
                     | Sql.Var(v_source,v_name,_) when v_name = target_name ->
                        v_source
                     | _ -> None
                  end in
                  (  target_source, 
                     target_name, 
                     (Sql.expr_type target_expr tables sources)
                  )
               ) targets)
            )
         in
         Sql.Select(new_targets, sources, cond, new_gb_vars, having, opts)
      | Sql.Union(s1, s2) -> 
         let rcr stmt = cast_query_to_aggregate tables stmt in
         Sql.Union(rcr s1, rcr s2)
   )


(**
   [normalize_agg_target_expr gb_vars expr]
   
   Transform a SQL aggregate expression into a calculus-friendly form.  This 
   entails rewriting the expression so that terms that appear outside of the
   aggregate(s) follow these properties: 
   - Variables appearing outside of the aggregate are all group-by variables.
     (this isn't so much a rewrite as a sanity check, as the SQL expression is
     invalid otherwise).
   - Products of aggregates and non-aggregates have the non-aggregate on the
     right-hand side so that (group-by) variables appearing in the non-aggregate
     terms will be bound by the aggregate
   - A sum/subtraction operation with at least one non-aggregate child must 
     have a parent that is a product with an aggregate (so that the sum will 
     appear on the right hand side and have all variables bound).  At present,
     we will not support sums at the root level (because that makes the default 
     value of the root map something other than zero), although it should be 
     easy to support this once we have support for toplevel queries defined as 
     arbitrary expressions.
   
   @param gb_vars The group-by variables of the select statement in the context
                  of which [expr] is being evaluated
   @param expr    A SQL expression with an aggregate in it.
   @return        The normalized form of [expr].
*)
let rec normalize_agg_target_expr ?(vars_bound = false) gb_vars expr =
   let rcr ?(b = vars_bound) e =
      normalize_agg_target_expr ~vars_bound:b gb_vars e
   in
   begin match expr with 
     | Sql.Const(_) -> expr
     | Sql.Aggregate(_,_) -> expr
         
     | Sql.Var(v)   -> 
         if not (List.mem v gb_vars) then (
            Sql.error ("Non Group-by Variable "^(Sql.string_of_var v)^
                       " used outside of an aggregate expression (gb vars:"^
                       (ListExtras.ocaml_of_list Sql.string_of_var gb_vars)^")")
         ) else expr
         
     | Sql.SQLArith(lhs, op, rhs) ->
         let rcr_into_arithmetic bind = 
            let new_lhs = rcr lhs in
            let new_rhs = if bind then rcr ~b:true rhs else rcr rhs in
            Sql.SQLArith(new_lhs, op, new_rhs)
         in
         begin match op with
            | Sql.Sum | Sql.Sub ->
               (* Order doesn't matter *)
               rcr_into_arithmetic false
            | Sql.Prod ->
               (* If the lhs (or a parent) binds the group by variables, then
                  we're set. *)
               if vars_bound || (Sql.is_agg_expr lhs)
               then rcr_into_arithmetic true
               (* Otherwise, if the rhs is an aggregate, then we can just 
                  flip the order of ops (This is straight product, which 
                  commutes) *)
               else if Sql.is_agg_expr rhs
                    then Sql.SQLArith(rcr rhs, op, rcr ~b:true lhs)
               (* Finally, if we get to this point, something's wrong, because
                  we should have seen an aggregate by now *)
                    else Sql.error ("Non-aggregate masqurading as aggregate");
            | Sql.Div ->
               (* Pass these through unchanged.  We ensure ordering while doing 
                  the lifting in calc_of_expr *)
               if vars_bound || (Sql.is_agg_expr lhs)
               then rcr_into_arithmetic true
               else rcr_into_arithmetic false
         end
     | Sql.Negation(sub) -> Sql.Negation(rcr sub)
     
     | Sql.NestedQ(_) ->
         Sql.error ("Nested subqueries not supported outside of aggregates.")
      
     | Sql.ExternalFn(fn, fargs) ->
         (* Like division, we pass these through unchanged.  We will do our own
            reordering during calc_of_expr, so we simply assume that all non-
            aggregate targets will be moved to the end where the gb vars are
            bound. *)
         Sql.ExternalFn(fn, 
            List.map (fun arg -> rcr ~b:(not (Sql.is_agg_expr arg)) arg) fargs)

     | Sql.Case(cases, else_branch) -> 
         Sql.Case(List.map (fun (c,e) -> (c,rcr e)) cases, rcr else_branch)
   end


let rec calc_of_query ?(query_name = None)
                      (tables:Sql.table_t list) 
                      (query:Sql.select_t): 
                      (string * C.expr_t) list = 
   let re_hv_query = Sql.rewrite_having_query tables query in 
   let agg_query = cast_query_to_aggregate tables re_hv_query in
   Debug.print "LOG-SQL-TO-CALC" (fun () -> 
      "Cast query is now : "^
      (Sql.string_of_select agg_query)
   );
   match agg_query with
   | Sql.Union(s1, s2) -> 
      let rcr stmt = calc_of_query ~query_name:query_name tables stmt in
      let lift_stmt name stmt = CalcRing.mk_prod [CalculusDomains.mk_exists stmt; C.mk_lift (name, C.type_of_expr stmt) stmt] in
      List.map (fun ((n1, e1), (n2, e2)) -> 
                  let ln = n1 in
                  (ln, CalcRing.mk_sum [lift_stmt ln e1; lift_stmt ln e2])
               ) 
               (List.combine (rcr s1) (rcr s2))
   | Sql.Select _ -> (calc_of_select ~query_name:query_name tables agg_query)


(**
   [calc_of_query tables stmt]
   
   Translate a SQL query to the corresponding calculus expression(s).  If the 
   query is a non-aggregate query, the Calculus expression computes the bag 
   corresponding to the query result (i.e., the query is cast to an aggregate
   by adding an implicit [COUNT( * )]).  If the query is an aggregate, then 
   all non-group-by target columns generate a separate Calculus expression 
   (hence, it is possible for this function to return multiple Calculus
   expressions). Note that the method expects a SQL query without a HAVING
   clause.

   @param query_name (optional) If this string is not None, then the expected
                     output schema of the group-by variables of this query will 
                     be rebound to a relation with the specified name (e.g., 
                     [query_name,target_name,target_type])
   @param tables The schema of the database on which the query is being run
   @param stmt   A SQL query
   @return       A Calculus expression for every non-group-by target in [stmt], 
                 and the name of the corresponding target
*)
and calc_of_select ?(query_name = None)
                   (tables:Sql.table_t list) 
                   (query:Sql.select_t): 
                   (string * C.expr_t) list =   
   let (targets,sources,cond,gb_vars,opts) = 
      match query with
      | Sql.Select(targets,sources,cond,gb_vars,Sql.ConstB(true),opts) -> 
         (targets,sources,cond,gb_vars,opts) 
      | Sql.Select(_,_,_,_,_,_) -> 
         failwith ("Bug: Expected SELECT statement without HAVING clause")
      | _ -> Sql.error ("Expected select statement")
   in
   let source_calc = calc_of_sources tables sources in
   let cond_calc = calc_of_condition tables sources cond in
   Debug.print "LOG-SQL-TO-CALC" (fun () ->
      "Condition for query: \n"^
      (CalculusPrinter.string_of_expr cond_calc)^"\n"
   );
   let (agg_tgts, noagg_tgts) = 
      List.partition (fun (_,expr) -> Sql.is_agg_expr expr) targets in
   let check_gb_var_list tgt_name tgt_expr = 
      match tgt_expr with
         (* If the target expression is just a variable, then the variable might
            be present directly in the group-by variables list.  In this case, 
            we return true. *)
         | Sql.Var(v_source,v_name,_) when
            (List.exists (fun (cmp_source, cmp_name, _) ->
               (v_name = cmp_name) && 
               (  (v_source = None) || (cmp_source = None) || 
                  (v_source = cmp_source) )
            ) gb_vars) -> true
         (* If the target expression is not a variable, or is not present 
            directly in the group-by variables list, then it might be present
            by its target name. *)
         | _ -> 
            List.exists (fun (cmp_source,cmp_name,_) ->
               (tgt_name = cmp_name) && (cmp_source = None)
            ) gb_vars
   in
   (* Generate expressions to pull the group-by targets into the schema. 
      This gets used below, within the generation of the aggregate targets, 
      since each aggregate target might need to include this expression multiple
      times. (e.g., if a single target composes multiple aggregates) *)
   let (noagg_vars, noagg_terms) = List.split (List.map 
      (fun (base_tgt_name, tgt_expr) ->
      if not (check_gb_var_list base_tgt_name tgt_expr) then
         Sql.error ("Non-aggregate term '"^base_tgt_name^"' ("^
                    (Sql.string_of_expr tgt_expr)^
                    ") not in group by clause:"^
                    (ListExtras.ocaml_of_list Sql.string_of_var gb_vars))
      else
      let tgt_name = 
         if query_name = None then base_tgt_name
         else fst (var_of_sql_var (query_name,base_tgt_name,TAny)) 
      in
      Debug.print "LOG-SQL-TO-CALC" (fun () -> 
         "Computing lift for target "^
         tgt_name^
         " with expression: "^
         (Sql.string_of_expr tgt_expr)
      );
      match tgt_expr with
         | Sql.Var((vs,vn,vt) as v) 
            when ((query_name = None) && (vn = tgt_name))
              || ((fst (var_of_sql_var v)) = tgt_name)   ->
               Debug.print "LOG-SQL-TO-CALC" (fun () -> 
                  "Just a variable where '"^(fst (var_of_sql_var v))^"' = '"^
                  tgt_name^"'"
               );
               (* If we're being asked to group by a variable (with no renaming)
                  then we don't actually need to include the variable in any way
                  shape and/or form *)
               (var_of_sql_var (vs, vn, vt), CalcRing.one)
         | _ -> 
            (* If we're being asked to group by something more complex than
               just a simple variable, then we need to lift the expression
               up into a variable of the appropriate name (this also covers
               simple renaming) *)
            let tgt_var = (tgt_name, (Sql.expr_type tgt_expr tables sources)) in
            (tgt_var, calc_of_sql_expr 
               ~tgt_var:(Some(tgt_var)) tables sources tgt_expr)
   ) noagg_tgts) in
   let noagg_calc = CalcRing.mk_prod noagg_terms in
   (* There might be group-by targets that are not represented in the target
      list.  Add these to the var list artificial targets for these *)
   let new_gb_vars = noagg_vars @ (List.flatten (List.map (fun (vs,vn,vt) ->
         if not (List.exists (fun (tgt_name, tgt_expr) -> 
            if (vs = None) && (tgt_name = vn) then true
            else if tgt_expr = Sql.Var(vs,vn,vt) then true
            else false
         ) noagg_tgts)
         then [var_of_sql_var (vs,vn,vt)] else []
      ) gb_vars))
   in
   List.map (fun (tgt_name, unnormalized_tgt_expr) ->
      let tgt_expr = 
         normalize_agg_target_expr gb_vars unnormalized_tgt_expr
      in
      Debug.print "LOG-SQL-TO-CALC" (fun () -> 
         "Compiling aggregate target: "^(tgt_name)^
         " with expression: "^(Sql.string_of_expr tgt_expr)
      );
      (tgt_name, 
         calc_of_sql_expr ~materialize_query:(Some(fun agg_type agg_calc ->
            Debug.print "LOG-SQL-TO-CALC" (fun () -> 
               "Materializing target: "^(tgt_name)^
               " with calculus: "^(C.string_of_expr agg_calc)
            );
            (** Get the schema of the lifted variables as follows:
               1 - If the query is unnamed (i.e., the root query) then we take 
                   the gb variable name as written.   If the gb variable is 
                   unbound, we take it as written.
               2 - If the query is unnamed and the gb variable is bound to a 
                   source, we include the source's name in the variable's name.
               3 - Otherwise, we've rebound the variable name above, when 
                   computing noagg_calc, and we include the query name in the 
                   variable's name.
               Note that all of this must match the construction of noagg_calc 
               above.
            *)
            let ret = 
            begin match agg_type with
               | Sql.SumAgg -> 
                  C.mk_aggsum new_gb_vars
                        (CalcRing.mk_prod 
                            [source_calc; cond_calc; agg_calc; noagg_calc])
               | Sql.CountAgg(None) -> (* COUNT( * ) *)
                  C.mk_aggsum new_gb_vars 
                     (CalcRing.mk_prod [source_calc; cond_calc; noagg_calc])
               | Sql.CountAgg(Some([])) -> (* COUNT(DISTINCT) *)
                  C.mk_aggsum new_gb_vars 
                     (CalculusDomains.mk_exists
                         (CalcRing.mk_prod 
                            [source_calc; cond_calc; noagg_calc]))
               | Sql.CountAgg(Some(fields)) -> (* COUNT(DISTINCT ... ) *)
                  C.mk_aggsum new_gb_vars 
                     (CalculusDomains.mk_exists
                         (C.mk_aggsum  
                             (ListAsSet.union 
                                 new_gb_vars (List.map var_of_sql_var fields))
                             (CalcRing.mk_prod 
                                 [source_calc; cond_calc; noagg_calc])))
               | Sql.AvgAgg -> 
                  let count_var = tmp_var "average_count" TInt in
                  C.mk_aggsum new_gb_vars
                     (CalcRing.mk_prod [
                         (** The value being averaged *)
                         C.mk_aggsum new_gb_vars
                            (CalcRing.mk_prod 
                                [source_calc; cond_calc; agg_calc; noagg_calc]
                            );
                         (* The (lifted) count of the value being averaged *)
                         C.mk_lift count_var 
                            (C.mk_aggsum new_gb_vars
                                (CalcRing.mk_prod 
                                    [source_calc; cond_calc; noagg_calc]));
                         (* 1/COUNT *)
                         CalcRing.mk_prod [
                            C.mk_value (
                               Arithmetic.mk_fn "/" [
                                  (* Hack to ensure that we don't ever get NAN:
                                     make sure that the count is always >= 1 *)
                                  Arithmetic.mk_fn "listmax" [
                                     Arithmetic.mk_int 1;
                                     Arithmetic.mk_var count_var
                                  ] TInt
                               ] TFloat
                            );
                            C.mk_cmp Neq 
                               (Arithmetic.mk_int 0) 
                               (Arithmetic.mk_var count_var)
                         ]
                      ])
            end in Debug.print "LOG-SQL-TO-CALC" (fun () ->
               "Generated Calculus: \n"^
               (CalculusPrinter.string_of_expr ret)^"\n"
            ); ret
         )) tables sources tgt_expr
      )
   ) agg_tgts

(**
   [calc_of_sources tables sources]
   
   Translate the [FROM] clause of a SQL query to its corresponding Calculus
   form.  Tables are transformed into their corresponding Rel() terms, while
   nested subqueries are lifted into appropriately named variables.
   @param tables  The schema of the database on which the query is being run
   @param sources All members of the [FROM] clause of a SQL query
   @return        A Calculus expression implementing [sources].
*)
and calc_of_sources (tables:Sql.table_t list) 
                    (sources:Sql.labeled_source_t list):
                    C.expr_t =
   let rcr_q q qn = calc_of_query ~query_name:(Some(qn)) tables q in
   let (rels,subqs) = List.fold_right (fun (sname,source) (tables,subqs) ->
      begin match source with
         | Sql.Table(reln) -> ((sname,reln)::tables, subqs)
         | Sql.SubQ(q)     -> (tables, (sname,q)::subqs)
      end
   ) sources ([],[]) in
   CalcRing.mk_prod (List.flatten [
      List.map (fun ((ref_name:string),(rel_name:string)) -> 
         let (_,sch,_,_) = Sql.find_table rel_name tables in
            C.mk_rel rel_name
               (List.map (fun (_,vn,vt) -> 
                              var_of_sql_var ((Some(ref_name)),vn,vt)) sch)
      ) rels;
      List.map (fun (ref_name, q) ->
         if Sql.is_agg_query q
         then 
            (* For aggregate queries, calc_of_query gives us a list of 
               target name, target_expression pairs, each of which computes the
               value of one of the aggregate targets.  
               
               Ideally, we would just lift the aggregate target into a variable
               with the appropriate name and be done with it.  
               
               However, unlike other situations where we would use lifts, here
               in order to obey SQL semantics, we need to perform a 
               domain-restricted lift.  This is a lift that shares the domain of
               the outer expression.  A utility function for generating such an
               expression is defined in CalculusDomains.
               *)
            CalcRing.mk_prod (List.map (fun (tgt_name,subq) ->
               CalculusDomains.mk_domain_restricted_lift
                  (var_of_sql_var ((Some(ref_name)),
                                   tgt_name,
                                   (C.type_of_expr subq)))
                  (subq)
            ) (rcr_q q ref_name))
         else
            (* For non-aggregate queries, calc_of_query instead produces a
               single target named [*_]COUNT, which specifies the count of the
               each tuple in the result set.  (the query is cast to a count).  
               For this, we should not lift the expression, but rather just
               include it inline. *)
            match (rcr_q q ref_name) with
               | [_,count_expr] -> count_expr
               | _ -> Sql.error (
                  "BUG: calc_of_query produced unexpected number "^
                  "of targets for a non-aggregate nested query"
               )
      ) subqs
   ])

(**
   [calc_of_condition tables cond]
   
   Translate the [WHERE] clause of a SQL query to its corresponding Calculus
   form.
   @param tables  The schema of the database on which the query is being run
   @param sources All members of the [FROM] clause of the SQL query in the 
                  context of which [cond] is being evaluated.
   @param cond    The [WHERE] clause of a SQL query
   @return        A Calculus expression implementing [cond].
*)
and calc_of_condition (tables:Sql.table_t list) 
                      (sources:Sql.labeled_source_t list)
                      (cond:Sql.cond_t):
                      C.expr_t =
   let rcr_et tgt_var e = calc_of_sql_expr ~tgt_var:tgt_var tables sources e in
   let rcr_e e = calc_of_sql_expr tables sources e in
   let rcr_c c = calc_of_condition tables sources c in
   let calc_of_like wrapper expr like = 
      let like_regexp = "^"^(Str.global_replace (Str.regexp "%") ".*"
                                                (Str.quote like) )^"$"
      in
      let (expr_val, expr_calc) = lift_if_necessary ~t:"like" (rcr_e expr) in
      if not ((Arithmetic.type_of_value expr_val) = TString) then
         failwith "Non-string value compared using the LIKE predicate"
      else
         CalcRing.mk_prod [
            expr_calc;
            CalcRing.mk_val (wrapper (
               Arithmetic.mk_fn "regexp_match" 
                                [Arithmetic.mk_string like_regexp; expr_val] 
                                TInt
            ))
         ]
   in
   let rcr_q q = calc_of_query tables q in
      begin match Sql.push_down_nots cond with
         | Sql.Comparison(e1,cmp,e2) -> 
            let e1_calc = rcr_e e1 in
            let e2_calc = rcr_e e2 in
            let field_ty = Type.escalate_type (C.type_of_expr e1_calc)
                                               (C.type_of_expr e2_calc) in
            let (e1_val, e1_calc) = lift_if_necessary ~vt:field_ty e1_calc in
            let (e2_val, e2_calc) = lift_if_necessary ~vt:field_ty e2_calc in
               C.mk_aggsum [] 
                  (CalcRing.mk_prod [
                      e1_calc; e2_calc;
                      C.mk_cmp cmp e1_val e2_val;
                   ])
         | Sql.And(c1,c2) -> CalcRing.mk_prod [rcr_c c1; rcr_c c2]
         | Sql.Or(c1,c2)  -> 
            let rec extract_ors = (function
               | Sql.Or(inner_c1, inner_c2) -> 
                  (extract_ors inner_c1)@(extract_ors inner_c2)
               | c -> [c]
            ) in
            let (or_val,or_calc) = lift_if_necessary ~t:"or" (
               CalcRing.mk_sum (
                  List.map rcr_c ((extract_ors c1) @ (extract_ors c2))
               )
            ) in
               (* A little hackery is needed here to handle the case where
                  both c1 and c2 are true; To deal with this, we explicitly 
                  check to see if c1 and c2 are both greater than 0 *)
               C.mk_aggsum [] 
                  (CalcRing.mk_prod [or_calc; 
                                     C.mk_cmp Gt 
                                              or_val 
                                              (Arithmetic.mk_int 0)])
         | Sql.Not(Sql.Exists(q)) ->
            (* The exists operator is only bound for elements in the domain.  
               Since we're explicitly looking for cases that are not in the 
               domain, we need to handle this case separately *)
            begin match rcr_q q with
            | [_,q_calc_unlifted] ->
               CalculusDomains.mk_not_exists 
                  (C.mk_aggsum [] q_calc_unlifted)
            | _ -> (failwith "Nested subqueries must have exactly 1 argument")
            end

         | Sql.Exists(q) ->
            begin match rcr_q q with
            | [_,q_calc_unlifted] ->
               CalculusDomains.mk_exists 
                  (C.mk_aggsum [] q_calc_unlifted)

            | _ -> (failwith "Nested subqueries must have exactly 1 argument")
            end
         
         | Sql.Not(Sql.Like(expr, like_str)) -> 
            calc_of_like (fun x -> Cmp(Eq,Arithmetic.mk_int 0,x)) expr like_str

         | Sql.Like(expr, like_str) ->
            calc_of_like (fun x -> Cmp(Neq,Arithmetic.mk_int 0,x)) expr like_str
         
         | Sql.Not(Sql.InList(expr, l)) ->
            let (expr_val, expr_calc) = 
               let t = Sql.expr_type expr tables sources in
               let v = Some(tmp_var "in" t) in
               lower_if_value (rcr_et v expr)
            in
               CalcRing.mk_prod (expr_calc::(List.map (fun x -> 
                  C.mk_cmp Neq (Arithmetic.mk_const x) expr_val)
                     (ListAsSet.uniq l)))

         | Sql.InList(expr, l) ->
            let (expr_val, expr_calc) = 
               let t = Sql.expr_type expr tables sources in
               let v = Some(tmp_var "in" t) in
               lower_if_value (rcr_et v expr)
            in
            (** Unlike the general OR, we can ensure that the condition is
                an exclusive OR by making the list of comparisons unique *)
               CalcRing.mk_prod [expr_calc; 
                  CalcRing.mk_sum (List.map (fun x -> 
                     C.mk_cmp Eq (Arithmetic.mk_const x) expr_val)
                        (ListAsSet.uniq l))
               ]
         
         | Sql.Not(c) -> (* This should never be reached... push_down nots 
                            should push everything away *)
            failwith "Non-pushed down NOT in Sql Condition"

         | Sql.ConstB(true)  -> CalcRing.one
         | Sql.ConstB(false) -> CalcRing.zero
      end

(**
   [calc_of_sql_expr ~materialize_query:(...) tables expr]
   
   Translate a SQL expression into a Calculus expression.  The behavior of this
   function differs from the expected in one noticeable way: Aggregate functions
   can not be materialized inline, as they require the full context of the
   query surrounding the aggregate function.
   
   Specifically, consider the query:

   {[SELECT (SUM(x) / (COUNT( * ))) FROM R WHERE y > 0]}

   This query needs two separate aggregates, as it would be translated to:

   {[AggSum(R(x) * [y>0] * x) / AggSum(R(x) * [y>0])]}

   Apart from the elipses, [AggSum(R(x) * [y>0] * ...)] comes entirely from 
   outside of the expression itself.
   
   Thus, [calc_of_sql_expr] takes an optional materialize_query operator, which
   will be invoked at any Aggregate node to materialize the query at that point.
   If the operator is not present when calc_of_sql_expr is called on an 
   expression with Aggregates, an exception will be raised.
   @param materialize_query (optional) An operator to materialize the query when
                            an aggregate function is encountered
   @param tables            The schema of the database on which the query is 
                            being run
   @param sources           The sources of the query in the context of which 
                            [expr] is being evaluated.
   @param expr              A SQL expression
   @return                  The Calculus form of [expr]
   @raise Failure If the SQL expression is invalid (e.g., it contains a nested
                  subquery with >1 target or if it contains an aggregate
                  function where none is expected).
*)
and calc_of_sql_expr ?(tgt_var = None)
                     ?(materialize_query = None)
                     (tables:Sql.table_t list) 
                     (sources:Sql.labeled_source_t list)
                     (expr:Sql.expr_t): C.expr_t =
   let rcr_e ?(is_agg=false) e =
      if is_agg then calc_of_sql_expr tables sources e
      else calc_of_sql_expr ~materialize_query:materialize_query
                            tables sources e
   in
   let rcr_q q = calc_of_query tables q in
   let rcr_c c = calc_of_condition tables sources c in
   
   (* In order to support expressions like A + Sum(B) where A is a group-by 
      variable, we need to extend the schema of the non-aggregate term to 
      include the grouping terms.  We use an Exists to bind the required 
      domain *)
   let extend_sum_with_agg calc_expr = 
      begin match materialize_query with
         | None -> failwith "Unexpected aggregation operator (1)"
         | Some(m) -> 
            let count_agg = m (Sql.CountAgg(None)) CalcRing.one in
            let (_,count_sch) = C.schema_of_expr count_agg in
            CalcRing.mk_prod [
               C.mk_aggsum count_sch
                  (C.CalcRing.mk_val (C.Exists(count_agg)));
               calc_expr
            ]
      end
   in
   let calc, contains_target = 
      begin match expr with
         | Sql.Const(c) -> 
            C.mk_value (Arithmetic.mk_const c), false
         | Sql.Var(v)   -> 
            C.mk_value (Arithmetic.mk_var (var_of_sql_var v)), false
         | Sql.SQLArith(e1, ((Sql.Sum | Sql.Sub) as op), e2) ->
            let e1_calc, e2_base_calc =
               begin match ((Sql.is_agg_expr e1), (Sql.is_agg_expr e2)) with
                  | (true, true) | (false,false) -> (rcr_e e1, rcr_e e2)
                  | (false,true) -> (extend_sum_with_agg (rcr_e e1), (rcr_e e2))
                  | (true,false) -> ((rcr_e e1), extend_sum_with_agg (rcr_e e2))
               end
            in
            let e2_calc = if op = Sql.Sub then CalcRing.mk_neg e2_base_calc
                                          else e2_base_calc 
            in
               CalcRing.mk_sum [e1_calc; e2_calc], false
         | Sql.SQLArith(e1, Sql.Prod, e2) ->
            CalcRing.mk_prod [rcr_e e1; rcr_e e2], false
         | Sql.SQLArith(e1, Sql.Div, e2) ->
            let ce1 = rcr_e e1 in
            let ce2 = rcr_e e2 in
            let (e2_val, e2_calc) = lift_if_necessary ce2 in

            (* The ordering of ce1/e1_calc and e2_calc needs to pay attention to
               which variables are bound where.  Specifically, if e2 is an 
               aggregate expression and e1 is not (i.e., the entire thing is an
               aggregate expression, but there are computations going on outside
               of the aggregate), then it's possible that e2 will bind group by
               variables that are used in e1.  In this case, we need to flip the
               order of the lift operations.  

               The equivalent operations are performed for the other arithmetic
               operators in [normalize_agg_target_expr] above, since the correct
               rewrite behavior for some of those can not be determined locally.
            *)
            let needs_order_flip = 
               (not (Sql.is_agg_expr e1)) && (Sql.is_agg_expr e2)
            in 
            
            let nested_schema = 
               (ListAsSet.union (snd (C.schema_of_expr ce1))
                                (snd (C.schema_of_expr ce2)))
            in
               C.mk_aggsum nested_schema 
                  (CalcRing.mk_prod [
                      ( if needs_order_flip 
                        then CalcRing.mk_prod [e2_calc; ce1]
                        else CalcRing.mk_prod [ce1; e2_calc]
                      );
                      C.mk_value (Arithmetic.mk_fn "/" [e2_val] TFloat)    
                  ]), false

         | Sql.Negation(e) -> CalcRing.mk_neg (rcr_e e), false
         | Sql.NestedQ(q)  -> 
            let (q_targets,q_sources,q_cond,q_gb_vars,_) = 
               match q with
               | Sql.Select(targets,sources,cond,gb_vars,
                            Sql.ConstB(true),opts) -> 
                  (targets,sources,cond,gb_vars,opts) 
               | Sql.Select(_,_,_,_,_,_) ->
                  failwith 
                     ("Bug: Expected SELECT statement without HAVING clause")
               | _ -> 
                  Sql.error 
                     ("Target-nested subqueries with UNION not supported")
            in
            if (q_gb_vars <> []) || (* Group-by not allowed *)
               (List.length q_targets <> 1) || (* Only one target *)
               ((not (Sql.is_agg_query q)) && (q_sources <> [])) 
                            (* And if it has any sources, it had better be an
                               aggregate query *)
            then
               (* This should get caught much earlier... but let's be safe *)
               Sql.error ("Target-nested subqueries must produce exactly 1 "^
                          "column, and either be aggregate queries or not have "^
                          "a FROM clause")
            else
               (* If it's an aggregate query, then we do the standard thing, but
                  only return the one (nameless) target. *)
               if Sql.is_agg_query q then snd (List.hd (rcr_q q)), false
               (* If it's not an aggregate query, then all we care about is the 
                  condition and the single target that we have. *)
               else 
                  CalcRing.mk_prod 
                     [rcr_c q_cond; rcr_e (snd (List.hd (q_targets)))], false

         | Sql.Aggregate(agg, expr) -> 
            begin match materialize_query with
               | Some(mq) -> (mq agg (rcr_e ~is_agg:true expr))
               | None -> failwith "Unexpected aggregation operator (2)"
            end, false
         | Sql.ExternalFn(fn, fargs) ->
            let (lifted_args_and_gb_vars, arg_calc) = 
               List.split (List.map (fun arg ->
                  let raw_arg_calc = (rcr_e arg) in
                  let (arg_val, arg_calc) = lift_if_necessary raw_arg_calc in
                     ((arg_val, snd (C.schema_of_expr raw_arg_calc)),
                      (Sql.is_agg_expr arg, arg_calc))
               ) fargs) in
            let lifted_args, gb_vars = List.split lifted_args_and_gb_vars in
            let (agg_args,non_agg_args) = 
               List.partition fst arg_calc
            in
            let { Functions.ret_type       = impl_type;
                  Functions.implementation = impl_name;
                } = (Functions.declaration fn
                        (List.map Arithmetic.type_of_value lifted_args))
            in
            let agg_res = 
               match tgt_var with
               | None -> []
               | Some(t) -> [t]
            in

            let args_list = 
              let args_expr = 
                CalcRing.mk_prod (List.map snd (agg_args @ non_agg_args)) 
              in 
                (* Allow external functions to return types than cannot be escalated later on; e.g., SUBSTRING(x,3,4) = '3' is valid, 
                but (1 * SUBSTRING(x,3,4)) = '3' is not valid. *)
                if (args_expr = CalcRing.one) then [] 
                else CalcRing.prod_list args_expr
            in
               C.mk_aggsum (agg_res @ (List.flatten gb_vars)) 
                  (CalcRing.mk_prod 
                     ( args_list @ 
                      [
                        let calc = C.mk_value (Arithmetic.mk_fn impl_name lifted_args impl_type) in
                        if agg_res = [] then calc else C.mk_lift (List.hd agg_res) calc 
                      ])), agg_res != []
         | Sql.Case(cases, else_branch) ->
            let (ret_calc, else_cond) = 
               List.fold_left (fun (ret_calc, prev_cond) (curr_cond, curr_term) -> 
                  let full_cond = (Sql.mk_and prev_cond curr_cond)
                  in (
                     CalcRing.mk_sum [ret_calc; 
                        CalcRing.mk_prod [rcr_c full_cond;
                                          rcr_e curr_term]],
                     (Sql.mk_and prev_cond (Sql.Not(curr_cond)))
                  )
               ) (CalcRing.zero, Sql.ConstB(true))cases
            in
               CalcRing.mk_sum [ret_calc; 
                  CalcRing.mk_prod [rcr_c else_cond; rcr_e else_branch]], false
         
      end
   in
      match tgt_var, contains_target with
      | Some(v), false -> C.mk_lift v calc
      | _, _           -> calc

(**
   [extract_sql_schema db tables]
   
   Extract a SQL database schema into a [Schema.t] schema.
   @param db      The schema to extract [tables] into.
   @param tables  A SQL schema
*)
let extract_sql_schema (db:Schema.t) (tables:Sql.table_t list) = 
   List.iter (fun (reln, relsch, reltype, (relsource, reladaptor)) ->
      Schema.add_rel db ~source:relsource ~adaptor:reladaptor 
                        (reln, List.map var_of_sql_var relsch, reltype)
   ) (List.rev tables)
