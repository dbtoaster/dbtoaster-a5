open Ring
open Types
open Arithmetic
open Calculus

module C = Calculus

let tmp_var_id = ref 0

let tmp_var (vn:string) (vt:type_t): var_t = 
   tmp_var_id := !tmp_var_id + 1;
   (vn^(string_of_int !tmp_var_id), vt)

(* This utility function performs conversion from arbitrary calculus expressions 
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
let lift_if_necessary ?(t="agg") (calc:C.expr_t):
                      (value_t * C.expr_t) = 
   match calc with
      | CalcRing.Val(Value(x)) -> (x, CalcRing.one)
      | _ -> 
         let tmp_var = tmp_var ("__sql_"^t^"_tmp_") 
                               (C.type_of_expr calc) in
            (mk_var tmp_var, CalcRing.mk_val (Lift(tmp_var, calc)))

let rec preprocess ((tables, queries):Sql.file_t): Sql.file_t =
   (tables, List.map (fun q -> Sql.bind_select_vars q tables) queries)

let var_of_sql_var ((rel,vn,vt):Sql.sql_var_t):var_t = 
   begin match rel with
      (* If preprocess has been called, all the vars should be bound to a
         relation instance in a query. *)
      | Some(reln) -> (reln^"_"^vn, vt)
      | None       -> failwith ("Unbound var "^vn)
   end

let rec calc_of_query (tables:Sql.table_t list) 
                        ((targets,sources,cond,gb_vars):Sql.select_t): 
                        (string * C.expr_t) list = 
   let gb_var_names = List.map Sql.string_of_var gb_vars in
   let source_calc = calc_of_sources tables sources in
   let cond_calc = calc_of_condition tables cond in
   let (agg_tgts, noagg_tgts) = 
      List.partition (fun (_,expr) -> Sql.is_agg_expr expr)
                     targets in
   let noagg_calc = CalcRing.mk_prod (List.map (fun (tgt_name, tgt_expr) ->
      if not (List.mem tgt_name gb_var_names) then
         failwith ("Non-aggregate term '"^tgt_name^"' not in group by clause")
      else match tgt_expr with
         | Sql.Var(v) when (Sql.string_of_var v) = tgt_name -> 
            (* If we're being asked to group by a variable (with no renaming)
               then we don't actually need to include the variable in any way
               shape and/or form *)
            CalcRing.one
         | _ -> 
            (* If we're being asked to group by something more complex than
               just a simple variable, then we need to lift the expression
               up into a variable of the appropriate name (this also covers
               simple renaming) *)
            let tgt_var = (tgt_name, (Sql.expr_type tgt_expr tables sources)) in
            CalcRing.mk_val (Lift(tgt_var, calc_of_sql_expr tables tgt_expr))
   ) noagg_tgts) in
   List.map (fun (tgt_name, tgt_expr) ->
      (tgt_name, 
         calc_of_sql_expr ~materialize_query:(Some(fun agg_type agg_calc ->
            begin match agg_type with
               | Sql.SumAgg -> 
                  CalcRing.mk_val 
                     (AggSum(List.map var_of_sql_var gb_vars, 
                             CalcRing.mk_prod 
                                [source_calc; cond_calc; agg_calc; noagg_calc]
                     ))
            end
         )) tables tgt_expr
      )
   ) agg_tgts

and calc_of_sources (tables:Sql.table_t list) 
                    (sources:Sql.labeled_source_t list):
                    C.expr_t =
   let rcr_q q = calc_of_query tables q in
   let (rels,subqs) = List.fold_right (fun (sname,source) (tables,subqs) ->
      begin match source with
         | Sql.Table(reln) -> ((sname,reln)::tables, subqs)
         | Sql.SubQ(q)     -> (tables, (sname,q)::subqs)
      end
   ) sources ([],[]) in
   CalcRing.mk_prod (List.flatten [
      List.map (fun ((ref_name:string),(rel_name:string)) -> 
         let (_,sch,_,_) = Sql.find_table rel_name tables in
            CalcRing.mk_val (Rel(
               rel_name,
               List.map (fun (_,vn,vt) -> 
                              var_of_sql_var ((Some(ref_name)),vn,vt)) sch, 
               TInt
            ))
      ) rels;
      List.map (fun (ref_name, q) ->
         CalcRing.mk_prod (List.map (fun (tgt_name,subq) ->
            CalcRing.mk_val (Lift(
               var_of_sql_var ((Some(ref_name)),
                               tgt_name,
                               (C.type_of_expr subq)),
               subq
            ))
         ) (rcr_q q))
      ) subqs
   ])

and calc_of_condition (tables:Sql.table_t list) (cond:Sql.cond_t):
                      C.expr_t =
   let rcr_e e = calc_of_sql_expr tables e in
   let rcr_c c = calc_of_condition tables c in
   let rcr_q q = calc_of_query tables q in
      begin match cond with
         | Sql.Comparison(e1,cmp,e2) -> 
            let (e1_val, e1_calc) = lift_if_necessary (rcr_e e1) in
            let (e2_val, e2_calc) = lift_if_necessary (rcr_e e2) in
               CalcRing.mk_val (AggSum([], 
                  CalcRing.mk_prod [
                     e1_calc; e2_calc;
                     CalcRing.mk_val (Cmp(cmp, e1_val, e2_val));
                  ]
               ))
         | Sql.And(c1,c2) -> CalcRing.mk_prod [rcr_c c1; rcr_c c2]
         | Sql.Or(c1,c2)  -> 
            let (or_val,or_calc) = lift_if_necessary ~t:"or" (
               CalcRing.mk_sum [rcr_c c1;rcr_c c2]
            ) in
               (* A little hackery is needed here to handle the case where
                  both c1 and c2 are true; To deal with this, we explicitly 
                  check to see if c1 and c2 are both greater than 0 *)
               CalcRing.mk_val (AggSum([], 
                  CalcRing.mk_prod [or_calc; 
                     CalcRing.mk_val (Cmp(Gt, or_val, mk_int 0))]
               ))
         | Sql.Not(Sql.Exists(q)) ->
            begin match rcr_q q with
            | [_,q_calc_unlifted] ->
            let (q_val,q_calc) = lift_if_necessary q_calc_unlifted in
               CalcRing.mk_val (AggSum([], 
                  CalcRing.mk_prod [q_calc; 
                     CalcRing.mk_val (Cmp(Eq, q_val, mk_int 0))]
               ))
            | _ -> (failwith "Nested subqueries must have exactly 1 argument")
            end
         | Sql.Exists(q) ->
            begin match rcr_q q with
            | [_,q_calc_unlifted] ->
            let (q_val,q_calc) = lift_if_necessary q_calc_unlifted in
               CalcRing.mk_val (AggSum([], 
                  CalcRing.mk_prod [q_calc; 
                     CalcRing.mk_val (Cmp(Gt, q_val, mk_int 0))]
               ))
            | _ -> (failwith "Nested subqueries must have exactly 1 argument")
            end
         | Sql.Not(c) ->
            let (not_val,not_calc) = lift_if_necessary ~t:"not" (rcr_c c) in
               (* Note that Calculus negation is NOT boolean negation.  We need
                  to swap 0 and not 0 with a comparison *)
               CalcRing.mk_val (AggSum([], 
                  CalcRing.mk_prod [not_calc; 
                     CalcRing.mk_val (Cmp(Eq, not_val, mk_int 0))]
               ))
         | Sql.ConstB(b) -> 
            CalcRing.mk_val (Value(mk_bool b))
      end

and calc_of_sql_expr ?(materialize_query = None)
                     (tables:Sql.table_t list) 
                     (expr:Sql.expr_t): C.expr_t =
   let rcr_e ?(is_agg=false) e =
      if is_agg then calc_of_sql_expr tables e
      else calc_of_sql_expr ~materialize_query:materialize_query
                            tables e
   in
   let rcr_q q = calc_of_query tables q in
   begin match expr with
      | Sql.Const(c) -> 
         CalcRing.mk_val (Value(mk_const c))
      | Sql.Var(v)   -> 
         CalcRing.mk_val (Value(mk_var (var_of_sql_var v)))
      | Sql.SQLArith(e1, Sql.Sum, e2) ->
         CalcRing.mk_sum [rcr_e e1; rcr_e e2]
      | Sql.SQLArith(e1, Sql.Prod, e2) ->
         CalcRing.mk_prod [rcr_e e1; rcr_e e2]
      | Sql.SQLArith(e1, Sql.Sub, e2) ->
         CalcRing.mk_sum [rcr_e e1; CalcRing.mk_neg (rcr_e e2)]
      | Sql.SQLArith(e1, Sql.Div, e2) ->
         let (e2_val, e2_calc) = lift_if_necessary (rcr_e e2) in
         CalcRing.mk_val (AggSum([], 
            CalcRing.mk_prod [rcr_e e1; e2_calc; 
               CalcRing.mk_val (Value(ValueRing.mk_val (
                  AFn("/", [e2_val], TFloat)
               )))]
         ))
      | Sql.Negation(e) -> CalcRing.mk_neg (rcr_e e)
      | Sql.NestedQ(q)  -> 
         begin match rcr_q q with
         | [_,q_calc] -> q_calc
         | _ -> (failwith "Nested subqueries must have exactly 1 argument")
         end
      | Sql.Aggregate(agg, expr) -> 
         begin match materialize_query with
            | Some(mq) -> (mq agg (rcr_e ~is_agg:true expr))
            | None -> failwith "Unexpected aggregation operator"
         end
   end

let extract_sql_schema (db:Schema.t) (tables:Sql.table_t list) = 
   List.iter (fun (reln, relsch, reltype, (relsource, reladaptor)) ->
      Schema.add_rel db ~source:relsource ~adaptor:reladaptor 
                        (reln, List.map var_of_sql_var relsch, reltype, TInt)
   ) tables
