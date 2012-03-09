(**
   Internal representation of a SQL parse tree.  The first representation in the 
   compiler pipeline.

   CREATE TABLE name(col1 type1[, ...]) 
   FROM type 
   
   SELECT   gb_term1[, ...], SUM(agg1)[, ...]
   FROM     rel
   WHERE    condition
   GROUP BY 
   
*)

open Types

exception SqlException of string
exception Variable_binding_err of string * int

let error msg = raise (SqlException(msg))

type sql_var_t = string option * string * type_t

type schema_t = sql_var_t list

type table_t = string * schema_t * Schema.rel_type_t *
               (Schema.source_t * Schema.adaptor_t)

type arith_t = Sum | Prod | Sub | Div

type agg_t = SumAgg

type expr_t = 
 | Const      of const_t
 | Var        of sql_var_t
 | SQLArith   of expr_t * arith_t * expr_t
 | Negation   of expr_t
 | NestedQ    of select_t
 | Aggregate  of agg_t * expr_t

and target_t = string * expr_t

and cond_t   = 
 | Comparison of expr_t * cmp_t * expr_t
 | And        of cond_t * cond_t
 | Or         of cond_t * cond_t
 | Not        of cond_t
 | Exists     of select_t
 | ConstB     of bool

and source_t = 
 | Table of (*rel name*) string
 | SubQ  of select_t

and labeled_source_t = string * source_t

and select_t =
   (* SELECT   *) target_t list *
   (* FROM     *) labeled_source_t list *
   (* WHERE    *) cond_t *
   (* GROUP BY *) sql_var_t list

type t = 
 | Create_Table of table_t
 | Select       of select_t

type file_t = table_t list * select_t list

(* Construction Helpers *)
let mk_file (stmt:t): file_t =
   match stmt with
    | Create_Table(table) -> ([table], [])
    | Select(select)      -> ([], [select])

let merge_files (lhs:file_t) (rhs:file_t): file_t =
   ((fst lhs) @ (fst rhs), (snd lhs) @ (snd rhs))

let empty_file:file_t = ([],[])


let add_to_file (stmt:t) (file:file_t): file_t =
   merge_files file (mk_file stmt)

let mk_and (lhs:cond_t) (rhs:cond_t): cond_t =
   match lhs with
    | ConstB(true) -> rhs
    | ConstB(false) -> ConstB(false)
    | _ -> (
      match rhs with
       | ConstB(true) -> lhs
       | ConstB(false) -> ConstB(false)
       | _ -> And(lhs, rhs)
    )

let mk_or (lhs:cond_t) (rhs:cond_t): cond_t =
   match lhs with
    | ConstB(true) -> ConstB(true)
    | ConstB(false) -> rhs
    | _ -> (
      match rhs with
       | ConstB(true) -> ConstB(true)
       | ConstB(false) -> lhs
       | _ -> Or(lhs, rhs)
    )

(* Printing *)

let string_of_arith_op (op:arith_t): string =
   match op with
      | Sum -> "+" | Prod -> "*" | Sub -> "-" | Div -> "/"

let string_of_agg (agg:agg_t): string =
   match agg with SumAgg -> "SUM"

let string_of_var ((s,v,_):sql_var_t): string =
   match s with Some(source) -> source^"."^v | None -> v
    (*^":"^(string_of_type t)*)

let rec string_of_expr (expr:expr_t): string =
   match expr with
      | Const(c) -> string_of_const c
      | Var(v) -> string_of_var(v)
      | SQLArith(a,op,b) -> "("^(string_of_expr a)^")"^
                              (string_of_arith_op op)^
                              "("^(string_of_expr b)^")"
      | Negation(a) -> "-("^(string_of_expr a)^")"
      | NestedQ(q) -> "("^(string_of_select q)^")"
      | Aggregate(agg,a) -> (string_of_agg agg)^"("^
                            (string_of_expr a)^")"

and string_of_cond (cond:cond_t): string =
   match cond with 
      | Comparison(a,cmp,b) -> "("^(string_of_expr a)^")"^
                                   (string_of_cmp cmp)^
                               "("^(string_of_expr b)^")"
      | And(a,b)      -> "("^(string_of_cond a)^") AND ("^(string_of_cond b)^")"
      | Or(a,b)       -> "("^(string_of_cond a)^") OR ("^(string_of_cond b)^")"
      | Not(c)        -> "NOT"^(string_of_cond c)
      | Exists(s)     -> "EXISTS ("^(string_of_select s)^")"
      | ConstB(true)  -> "TRUE"
      | ConstB(false) -> "FALSE"

and string_of_select (stmt:select_t): string =
   let (target,from,where,gb) = stmt in
   "SELECT "^(ListExtras.string_of_list ~sep:", " 
                              (fun (n,e) -> (string_of_expr e)^" AS "^n)
                              target)^
   (if List.length from > 0 then
         " FROM "^(ListExtras.string_of_list ~sep:", " (fun (n,s) -> 
            (match s with 
               | Table(t) -> t
               | SubQ(s) -> "("^(string_of_select s)^")"
            )^" AS "^n
         ) from)
      else "")^
   (if where <> ConstB(true) then
         " WHERE "^(string_of_cond where)
      else "")^
   (if List.length gb > 0 then
         " GROUP BY "^(ListExtras.string_of_list ~sep:", " string_of_var gb)
      else "")^
   ";"

let string_of_table ((name, vars, reltype, (source, adaptor)):table_t): string =
   
   "CREATE "^(if reltype == Schema.TableRel then "TABLE" else "STREAM")^
   " "^name^"("^
      (ListExtras.string_of_list ~sep:", " (fun (_,v,t) ->
         v^" "^(string_of_type t)
      ) vars)^
      ");"

(* Misc Utility *)

let find_table (t:string) (tables:table_t list): table_t =
   try 
      List.find (fun (t2,_,_,_) -> t = t2) tables
   with Not_found -> 
      error ("Undefined relation: '"^t^"'")

;;

let rec expr_type (expr:expr_t) (tables:table_t list) 
                  (sources:labeled_source_t list): type_t =
   let tree_err msg = 
      error (msg^": "^(string_of_expr expr))
   in
   let return_if_numeric st msg = 
      begin match st with TString(_) -> tree_err (msg^"string") | _ -> st end
   in
   let rcr e = expr_type e tables sources in
   match expr with
      | Const(c)        -> type_of_const c
      | Var(v)          -> var_type v tables sources
      | SQLArith(a,_,b) -> (escalate_type (rcr a) (rcr b))
      | Negation(subexp) ->
         return_if_numeric (rcr subexp) "Negation of "
      | NestedQ(stmt) -> 
         let subsch = select_schema tables stmt in
            if (List.length subsch) != 1 then
               tree_err "Nested query must return a single value"
            else 
               let (_,_,t) = List.hd subsch in
                  t
      | Aggregate(agg,subexp) -> 
         begin match agg with
            | SumAgg -> return_if_numeric (rcr subexp) "Aggregate of "
         end
         
      
and select_schema (tables:table_t list) (stmt:select_t): schema_t =
   let (targets, sources, _, _) = stmt in
   List.map (fun (name, expr) -> 
      (None, string_of_expr expr, expr_type expr tables sources)
   ) targets
   
and source_for_var_name (v:string) (tables:table_t list) 
                        (sources:labeled_source_t list): labeled_source_t =
   let candidates =
      List.find_all (fun (_,sd) ->
         let sch = 
            match sd with
               | Table(t) -> 
                  let (_,sch,_,_) = find_table t tables in sch
               | SubQ(s) -> 
                  select_schema tables s
         in
            List.exists (fun (_,v2,_) -> v2 = v) sch
      ) sources
   in
      if List.length candidates = 0 then
         raise (Variable_binding_err(v,0))
      else if List.length candidates > 1 then
         raise (Variable_binding_err(v,List.length candidates))
      else
         List.hd candidates
      
and source_for_var ((s,v,_):sql_var_t) (tables:table_t list) 
                   (sources:labeled_source_t list): labeled_source_t =
   match s with 
      | Some(sn) -> 
         if List.mem_assoc sn sources then
            (sn,List.assoc sn sources)
         else
            raise (Variable_binding_err(
               "Unable to find source '"^sn^"' in list: "^
               (ListExtras.string_of_list ~sep:", " fst sources), 
               0
            ))
      | None -> source_for_var_name v tables sources
      

and var_type (v:sql_var_t) (tables:table_t list) 
             (sources:labeled_source_t list): type_t =
   let (_,vn,t) = v in
   match t with
      | TAny -> 
         (* Try to dereference the variable and figure out its type *)
         let (_,source) = source_for_var v tables sources in
         let sch = (
            match source with 
               | Table(table) -> 
                     let (_,sch,_,_) = find_table table tables in sch
               | SubQ(subq) -> select_schema tables subq 
         )
         in
         (
            try 
               let (_,_,t) = List.find (fun (_,vn2,_) -> vn2 = vn) sch in t
            with Not_found ->
               failwith ("Bug in Sql.var_type: Found schema for variable '"^
                         (string_of_var v)^"': "^
                         (ListExtras.string_of_list ~sep:", " string_of_var sch)^
                         "; but variable not found inside")
         )
      | _ -> t

;;

let rec bind_select_vars ?(parent_sources = [])
                         ((targets,inner_sources,conds,gb):select_t)
                         (tables:table_t list): select_t =
   let sources = parent_sources @ inner_sources in
   let rcr_c c = bind_cond_vars c tables sources in
   let rcr_e e = bind_expr_vars e tables sources in
   let rcr_q q = bind_select_vars q tables in
   (
      List.map (fun (tn,te) -> (tn,rcr_e te)) targets,
      List.map (fun (sn,s) -> 
         (sn, (match s with Table(_) -> s | SubQ(subq) -> SubQ(rcr_q subq)))
      ) inner_sources,
      rcr_c conds,
      gb
   )

and bind_cond_vars (cond:cond_t) (tables:table_t list)
                   (sources:labeled_source_t list): cond_t =
   let rcr_c c = bind_cond_vars c tables sources in
   let rcr_e e = bind_expr_vars e tables sources in
   let rcr_q q = bind_select_vars ~parent_sources:sources q tables in
   match cond with
      | Comparison(a,op,b) -> Comparison(rcr_e a,op,rcr_e b)
      | And(a,b) -> And(rcr_c a,rcr_c b)
      | Or(a,b)  -> Or(rcr_c a,rcr_c b)
      | Not(a)   -> Not(rcr_c a)
      | Exists(q) -> Exists(rcr_q q)
      | ConstB(_) -> cond
   

and bind_expr_vars (expr:expr_t) (tables:table_t list) 
                   (sources:labeled_source_t list): expr_t =
   let rcr_e e = bind_expr_vars e tables sources in
   let rcr_q q = bind_select_vars ~parent_sources:sources q tables in
   match expr with 
      | Const(_) -> expr
      | Var(s,v,t) -> 
            Var(
               (Some(fst (source_for_var (s,v,t) tables sources))),
               v,
               var_type (s,v,t) tables sources
            )
      | SQLArith(a,op,b) -> SQLArith(rcr_e a,op,rcr_e b)
      | Negation(a) -> Negation(rcr_e a)
      | NestedQ(q) -> NestedQ(rcr_q q)
      | Aggregate(agg,a) -> Aggregate(agg, rcr_e a)

let rec is_agg_expr (expr:expr_t): bool =
   match expr with 
      | Const(_) -> false
      | Var(_) -> false
      | SQLArith(a,_,b) -> (is_agg_expr a) || (is_agg_expr b)
      | Negation(e) -> is_agg_expr e
      | NestedQ(_) -> false
      | Aggregate(_,_) -> true

let is_agg_query ((targets,_,_,_):select_t): bool =
   List.exists is_agg_expr (List.map snd targets)
;;
let global_table_defs:(string * table_t) list ref = ref []
;;
let reset_table_defs () = 
   (*print_endline ("Resetting tables; Old Tables Are: "^(
      string_of_list0 ", " fst !global_table_defs));*)
   global_table_defs := []