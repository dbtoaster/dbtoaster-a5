open Ring
open GlobalTypes
open Arithmetic
open Calculus
open Calculus.BasicCalculus

let tmp_var_id = ref 0

let tmp_var (vn:string) (vt:type_t): var_t = 
   tmp_var_id := !tmp_var_id + 1;
   (vn^(string_of_int !tmp_var_id), vt)

let lifted_exp ?(t="agg") (calc:BasicCalculus.expr_t) = 
   let tmp_var = tmp_var ("__sql_"^t^"_tmp_") 
                         (BasicCalculus.type_of_expr calc) in
      (tmp_var, CalcRing.mk_val (Lift(tmp_var, calc)))

let rec preprocess (tables:Sql.table_t list) (query:Sql.select_t): 
                   Sql.select_t =
   Sql.bind_select_vars query tables

let var_of_sql_var ((rel,vn,vt):Sql.sql_var_t):var_t = 
   begin match rel with
      (* If preprocess has been called, all the vars should be bound to a
         relation instance in a query. *)
      | Some(reln) -> (reln^"_"^vn, vt)
      | None       -> failwith ("Unbound var "^vn)
   end

let rec calc_of_query (tables:Sql.table_t list) 
                        ((targets,sources,cond,gb_vars):Sql.select_t): 
                        (string * BasicCalculus.expr_t) list = 
   let source_calc = calc_of_sources tables sources in
   let cond_calc = calc_of_condition tables cond in
   List.map (fun (tgt_name, tgt_expr) ->
      (tgt_name, CalcRing.mk_prod [
         source_calc;
         cond_calc;
         calc_of_sql_expr ~gb_vars:(List.map var_of_sql_var gb_vars) 
                          tables tgt_expr
      ])
   ) targets

and calc_of_sources (tables:Sql.table_t list) 
                    (sources:Sql.labeled_source_t list):
                    BasicCalculus.expr_t =
   let rcr_q q = calc_of_query tables q in
   let (rels,subqs) = List.fold_right (fun (sname,source) (tables,subqs) ->
      begin match source with
         | Sql.Table(reln) -> ((sname,reln)::tables, subqs)
         | Sql.SubQ(q)     -> (tables, (sname,q)::subqs)
      end
   ) sources ([],[]) in
   CalcRing.mk_prod (List.flatten [
      List.map (fun ((ref_name:string),(rel_name:string)) -> 
         let (_,sch,_) = Sql.find_table rel_name tables in
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
                               (BasicCalculus.type_of_expr subq)),
               subq
            ))
         ) (rcr_q q))
      ) subqs
   ])

and calc_of_condition (tables:Sql.table_t list) (cond:Sql.cond_t):
                      BasicCalculus.expr_t =
   let rcr_e e = calc_of_sql_expr tables e in
   let rcr_c c = calc_of_condition tables c in
   let rcr_q q = calc_of_query tables q in
      begin match cond with
         | Sql.Comparison(e1,cmp,e2) -> 
            let (e1_var, e1_calc) = lifted_exp (rcr_e e1) in
            let (e2_var, e2_calc) = lifted_exp (rcr_e e2) in
               CalcRing.mk_prod [
                  e1_calc; e2_calc;
                  CalcRing.mk_val (Cmp(cmp, mk_var e1_var, mk_var e2_var));
               ]
         | Sql.And(c1,c2) -> CalcRing.mk_prod [rcr_c c1; rcr_c c2]
         | Sql.Or(c1,c2)  -> 
            let (or_var,or_calc) = lifted_exp ~t:"or" (
               CalcRing.mk_sum [rcr_c c1;rcr_c c2]
            ) in
               (* A little hackery is needed here to handle the case where
                  both c1 and c2 are true; To deal with this, we explicitly 
                  check to see if c1 and c2 are both greater than 0 *)
               CalcRing.mk_prod [or_calc; 
                  CalcRing.mk_val (Cmp(Gt, (mk_var or_var), mk_int 0))]
         | Sql.Not(Sql.Exists(q)) ->
            begin match rcr_q q with
            | [_,q_calc_unlifted] ->
            let (q_var,q_calc) = lifted_exp q_calc_unlifted in
               CalcRing.mk_prod [q_calc; 
                  CalcRing.mk_val (Cmp(Eq, (mk_var q_var), mk_int 0))]
            | _ -> (failwith "Nested subqueries must have exactly 1 argument")
            end
         | Sql.Exists(q) ->
            begin match rcr_q q with
            | [_,q_calc_unlifted] ->
            let (q_var,q_calc) = lifted_exp q_calc_unlifted in
               CalcRing.mk_prod [q_calc; 
                  CalcRing.mk_val (Cmp(Gt, (mk_var q_var), mk_int 0))]
            | _ -> (failwith "Nested subqueries must have exactly 1 argument")
            end
         | Sql.Not(c) ->
            let (not_var,not_calc) = lifted_exp ~t:"not" (rcr_c c) in
               (* Note that Calculus negation is NOT boolean negation.  We need
                  to swap 0 and not 0 with a comparison *)
               CalcRing.mk_prod [not_calc; 
                  CalcRing.mk_val (Cmp(Eq, (mk_var not_var), mk_int 0))]
         | Sql.ConstB(b) -> 
            CalcRing.mk_val (Value(mk_bool b))
      end

and calc_of_sql_expr ?(gb_vars = []) (tables:Sql.table_t list) (expr:Sql.expr_t):
                 BasicCalculus.expr_t =
   let rcr_e e = calc_of_sql_expr ~gb_vars:gb_vars tables e in
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
         let (e2_var, e2_calc) = lifted_exp (rcr_e e2) in
         CalcRing.mk_prod [rcr_e e1; e2_calc; 
            CalcRing.mk_val (Value(ValueRing.mk_val (
               AFn("/", [mk_var e2_var], TFloat)
            )))]
      | Sql.Negation(e) -> CalcRing.mk_neg (rcr_e e)
      | Sql.NestedQ(q)  -> 
         begin match rcr_q q with
         | [_,q_calc] -> q_calc
         | _ -> (failwith "Nested subqueries must have exactly 1 argument")
         end
      | Sql.Aggregate(Sql.SumAgg, expr) -> 
         CalcRing.mk_val (AggSum(gb_vars, rcr_e expr))
   end

