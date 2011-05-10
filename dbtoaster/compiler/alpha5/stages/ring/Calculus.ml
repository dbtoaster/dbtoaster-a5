open Util
open Sql.Types

type type_t  = Sql.type_t
type const_t = Sql.const_t
type var_t = string * type_t
type value_t = 
    | Var   of var_t
    | Const of const_t
type cmp_t = Sql.cmp_t

type ('init_expr) external_t = 
  string *               (* name *)
  (var_t * bool) list *  (* variable * is_input_var *)
  type_t *               (* external's content type *)
  'init_expr             (* customizable external metadata *)

type ('init_expr) calc_t =
    | Sum        of 'init_expr calc_t list         (* c1+c2+... *)
    | Prod       of 'init_expr calc_t list         (* c1*c2*... *)
    | Neg        of 'init_expr calc_t              (* -c *)
    | Cmp        of value_t * cmp_t * value_t      (* v1 [cmp] v2 *)
    | AggSum     of var_t list * 'init_expr calc_t (* AggSum([v1,v2,...],c) *)
    | Value      of value_t                        (* Var(v) | Const(#) *)
    | Relation   of string * var_t list            (* R(v1,v2,...) *)
    | External   of 'init_expr external_t
    | Definition of var_t * 'init_expr calc_t      (* v <- c *)

exception TypecheckError of string * unit calc_t * unit calc_t option

(*** Constructors ***)

let sum_list (c:'i calc_t): 'i calc_t list =
   match c with Sum(sl) -> sl | _ -> [c]
;;
let prod_list (c:'i calc_t): 'i calc_t list =
   match c with Prod(pl) -> pl | _ -> [c]
;;
let mk_neg (term:'i calc_t): 'i calc_t =
   match term with
      | Neg(subt) -> subt
      | _         -> Neg(term)
;;
let mk_sum (terms:'i calc_t list): 'i calc_t =
   let terms = 
      List.flatten (
         List.map sum_list (
            List.filter (fun v -> 
               match v with 
                  | Value(Const(Integer(0))) -> false 
                  | Value(Const(Double(0.))) -> false 
                  | Neg(Value(Const(Integer(0)))) -> false 
                  | Neg(Value(Const(Double(0.)))) -> false 
                  | _ -> true
               ) terms
            )
         )
   in
   match terms with
      | [] -> Value(Const(Integer(0)))
      | [v] -> v
      | _ -> Sum(terms)
;;
let mk_prod (terms:'i calc_t list): 'i calc_t =
   if (List.exists (fun v -> 
      match v with 
         | Value(Const(Integer(0))) -> true
         | Value(Const(Double(0.))) -> true
         | _ -> false
   ) terms) then Value(Const(Integer(0)))
   else
      let terms = 
         List.flatten (
            List.map prod_list (
               List.filter (fun v -> 
                  match v with 
                     | Value(Const(Integer(1))) -> false 
                     | Value(Const(Double(1.))) -> false 
                     | _ -> true
                  ) terms
               ) 
            )
      in
      match terms with
         | [] -> Value(Const(Integer(1)))
         | [v] -> v
         | _ -> Prod(terms)
;;
(*** Basic Operations ***)    

let rec fold_calc (sum_op:('ret list -> 'ret)) (prod_op:('ret list -> 'ret))
                  (neg_op:('ret -> 'ret)) 
                  (cmp_op:(value_t * cmp_t * value_t -> 'ret))
                  (aggsum_op:(var_t list * 'ret -> 'ret))
                  (value_op:(value_t -> 'ret))
                  (rel_op:(string * var_t list -> 'ret))
                  (ext_op:('a external_t -> 'ret))
                  (def_op:(var_t * 'ret -> 'ret))
                  (expr:'a calc_t): 'ret =
   let rcr = fold_calc sum_op prod_op neg_op cmp_op aggsum_op value_op rel_op
                       ext_op def_op in
   match expr with 
    | Sum(elems)      -> sum_op (List.map rcr elems)
    | Prod(elems)     -> prod_op (List.map rcr elems)
    | Neg(child)      -> neg_op (rcr child)
    | Cmp(a,op,b)     -> cmp_op (a,op,b)
    | AggSum(v,e)     -> aggsum_op (v, rcr e)
    | Value(l)        -> value_op l
    | Relation(rn,rv) -> rel_op (rn,rv)
    | External(l)     -> ext_op l
    | Definition(v,d) -> def_op (v,rcr d)
;;

let rewrite_leaves (cmp_op:(value_t * cmp_t * value_t -> 'b calc_t))
                   (aggsum_op:(var_t list * 'b calc_t -> 'b calc_t))
                   (value_op:(value_t -> 'b calc_t))
                   (rel_op:(string * var_t list -> 'b calc_t))
                   (ext_op:('a external_t -> 'b calc_t))
                   (def_op:(var_t * 'b calc_t -> 'b calc_t))
                   (expr:'a calc_t): 'b calc_t =
   fold_calc mk_sum mk_prod mk_neg 
             cmp_op aggsum_op value_op rel_op ext_op def_op 
             expr
;;

let rec replace_vars (repl:(var_t * var_t) list)
                     (expr:'a calc_t): 'a calc_t =
   let sub:(var_t -> var_t) = (fun x -> if (List.mem_assoc x repl) 
                                        then (List.assoc x repl)
                                        else x)
   in
   rewrite_leaves 
      (fun (a,op,b) -> Cmp(a,op,b))
      (fun (v,e)   -> AggSum(List.map sub v, e))
      (fun v       -> Value(match v with Var(var) -> Var(sub var) 
                                       | Const(_) -> v))
      (fun (rn,rv) -> Relation(rn, List.map sub rv))
      (fun (en,ev,et,ei) 
                   -> External(en, List.map (fun (v,t) -> (sub v, t)) ev,et,ei))
      (fun (d,e)   -> Definition(sub d, e))
      expr
;;

let rec strip_init (expr:'a calc_t): (unit calc_t) = 
   rewrite_leaves 
      (fun (a,op,b) -> Cmp(a,op,b))
      (fun (gb,sub) -> AggSum(gb,sub))
      (fun x -> Value(x))
      (fun (rn,rv) -> Relation(rn,rv))
      (fun (en,ev,et,_) -> External(en,ev,et,()))
      (fun (dv,dd) -> Definition(dv,dd))
      expr
;;

(*** Printing ***)
let string_of_type = Sql.string_of_type
   
let string_of_const = Sql.string_of_const
    
let string_of_var ((vn, vt):var_t): string =
   "["^vn^":"^(string_of_type vt)^"]"
;;   
let string_of_value (v:value_t): string =
   match v with
    | Var(c) -> string_of_var c
    | Const(c) -> string_of_const c
   
let string_of_cmp = Sql.string_of_cmp_op

let rec string_of_any_calc (string_of_meta:(('a -> string) option)) 
                                (expr:'a calc_t): string =
   let rcr e = string_of_any_calc string_of_meta e in
   let rcrl subs sep = 
      string_of_list sep (List.map (fun x -> "("^(rcr x)^")") subs)
   in
   let string_of_var_list gbv = (list_to_string string_of_var gbv) in
   match expr with
    | Sum(subexps)  -> rcrl subexps " + "
    | Prod(subexps) -> rcrl subexps " * "
    | Neg(subexp)   -> "-("^(rcr subexp)^")"
    | Cmp(l,c,r) -> 
      (string_of_value l)^" "^(string_of_cmp c)^" "^(string_of_value r)
    | AggSum(gbv,exp) ->
      "AggSum("^(string_of_var_list gbv)^": "^(rcr exp)^")"
    | Value(v) -> (string_of_value v)
    | Relation(rn,rv) -> rn^(string_of_var_list rv)
    | External(en,ev,et,ei) -> 
      "{"^en^(list_to_string (fun ((vn,vt),i) ->
         "["^vn^(if i then " <-:" else " ->:")^(string_of_type vt)^"]"
      ) ev)^":"^(string_of_type et)^
      (match string_of_meta with
       | None -> ""
       | Some(som) -> " <- "^(som ei)
      )^"}"
    | Definition(dv,dd) -> "("^(string_of_var dv)^" <- ("^(rcr dd)^"))"

let string_of_enhanced_calc (string_of_meta:('a -> string)) (expr:'a calc_t):
                            string =
   string_of_any_calc (Some(string_of_meta)) expr
   
let string_of_calc (expr:'a calc_t): string =
   string_of_any_calc None expr


(*** Basic Queries ***)

(* Returns true if the specified variable is bound in the expression *)
let rec is_bound (expr:'a calc_t) (var:var_t): bool =
   fold_calc (List.exists (fun x->x))              (* Sum *)
             (List.exists (fun x->x))              (* Prod *)
             (fun x -> x)                          (* Neg *)
             (fun _ -> false)                      (* Cmp *)
             (fun (gb,_) -> List.mem var gb)       (* AggSum *)
             (fun v -> match v with Var(var2) -> var = var2 | _ -> false)
                                                   (* Value *)
             (fun (_,relv) -> List.mem var relv)   (* Relation *)
             (fun (_,extv,_,_) -> List.exists (fun (var2,b) -> b && var=var2) 
                                              extv)
                                                   (* External *)
             (fun (v,sub) -> sub || v == var)      (* Definition *)
             expr
;;

let is_not_bound expr var: bool = not (is_bound expr var)
;;

(* Returns all variables, bound and unbound involved in the expression *)
let rec get_schema (expr:'a calc_t): (var_t * bool) list =
   let value_set v = match v with Var(v) -> [v,false] | Const(_) -> [] in
   fold_calc 
      (* The sum merge is a bit of a hack... it's not quite typesafe *)
      (List.fold_left ListAsSet.union [])
      (fun terms -> List.fold_left (fun old_vars term_vars -> 
            ListAsSet.union (List.filter (fun (v,is_bound) -> 
               (* Product eliminates all input variables that appear as 
                  output variables on the LHS *)
               is_bound || (not (List.mem_assoc v old_vars))
            ) term_vars) old_vars
         ) [] terms)
      (fun x -> x)
      (fun (a,_,b) -> (value_set a) @ (value_set b))
      (fun (gb,sub) -> List.filter (fun (v, is_bound) -> 
         (* Keep input variables and variables in the group by list only *)
         if is_bound then List.mem v gb else true
      ) sub)
      value_set
      (fun (_,rv) -> List.map (fun v -> (v,true)) rv)
      (fun (_,ev,_,_) -> ev)
      (fun (v,d) -> (v,true) :: d)
      expr
;;

(* Returns Some() and a schema mapping that will make the two expressions 
   equivalent, or none if no such mapping can be found.  Note that false
   negatives may be returned *)
let rec get_mapping (expr1:'a calc_t) (expr2:'a calc_t):
                    (var_t * var_t) list option =
   let merge_translations l1_opt l2_opt =
      match l2_opt with
         | None -> None
         | Some(l2) ->
            List.fold_left (fun trn_opt (orig_v, sub_v) -> 
               match trn_opt with
                  | None -> None
                  | Some(trn) ->
                     if List.mem_assoc orig_v trn then 
                        if (List.assoc orig_v trn) = sub_v then
                           Some(trn)
                        else
                           None
                     else
                        Some( ((orig_v, sub_v) :: trn) )
            ) l1_opt l2
   in
   let merge_values val1 val2 =
      match (val1,val2) with
       | (Var(v1), Var(v2))     -> Some([v1,v2])
       | (Const(c1), Const(c2)) -> if c1 = c2 then Some([]) else None
       | (_, _)                 -> None
   in
   let rcr e1 e2 = get_mapping e1 e2 in
   let rcrl (el1,el2) = 
      if List.length el1 != List.length el2 then
         None
      else 
         List.fold_left2 (fun v e1 e2 -> 
            match v with 
             | None          -> None
             | Some(old_trn) -> merge_translations v (rcr e1 e2)
         ) (Some([])) el1 el2
   in
   match (expr1,expr2) with
    | (Sum(t1), Sum(t2))   -> rcrl (t1,t2)
    | (Prod(t1), Prod(t2)) -> rcrl (t1,t2)
    | (Neg(t1), Neg(t2))   -> rcr t1 t2
    | (Cmp(cl1,c1,cr1), Cmp(cl2,c2,cr2)) -> 
      if c1 = c2 then (
         merge_translations (merge_values cl1 cl2) (merge_values cr1 cr2)
      ) else None
    | (AggSum(vl1, t1), AggSum(vl2, t2)) -> (
         match (rcr t1 t2) with 
            | None -> None
            | Some(trn) ->
               if (ListAsSet.seteq 
                  (List.map (Function.apply_strict trn) vl1) vl2)
               then None (* The GroupBy terms are different *)
               else Some(trn)
      )
    | (Value(v1), Value(v2)) -> merge_values v1 v2
    | (Relation(rn1,rv1), Relation(rn2,rv2)) -> 
      if rn1 = rn2 then Some(List.combine rv1 rv2) else None
    | (External(en1,ev1,_,_), External(en2,ev2,_,_)) -> 
      if en1 = en2 then Some(List.combine (List.map fst ev1) 
                                          (List.map fst ev2)) else None
    | (Definition(dv1,dd1), Definition(dv2,dd2)) -> (
         match (rcr dd1 dd2) with 
          | Some(trn) -> merge_translations (Some[(dv1,dv2)]) (Some(trn))
          | None -> None
      )
    | (Cmp(_,_,_),_)          -> None
    | (AggSum(_,_),_)         -> None
    | (Value(v1),_)           -> None
    | (Relation(_,_),_)       -> None
    | (External(_,_,_,_),_)   -> None
    | (Definition(_,_),_)     -> None
    | (Sum(_), _)  -> None
    | (Prod(_), _) -> None
    | (Neg(_), _)  -> None

(* Returns a list of all Relations appearing in the expression *)
let rec get_relations (expr:('a calc_t)): string list =
   let merge_op = List.fold_left ListAsSet.union [] in
   fold_calc 
      merge_op 
      merge_op 
      (fun x -> x)
      (fun _ -> [])
      (fun (_,subexp) -> subexp)
      (fun _ -> [])
      (fun (rn,_) -> [rn])
      (fun _ -> [])
      (fun (_,subexp) -> subexp)
      expr

(* Returns a list of all Externals (and related metadata) appearing in the 
   expression *)
let rec get_externals (expr:('a calc_t)): ('a external_t) list =
   let merge_op = List.fold_left ListAsSet.union [] in
   fold_calc 
      merge_op 
      merge_op 
      (fun x -> x)
      (fun _ -> [])
      (fun (_,subexp) -> subexp)
      (fun _ -> [])
      (fun _ -> [])
      (fun e -> [e])
      (fun (_,subexp) -> subexp)
      expr

let rec calc_type ?(strict = false) (expr:('a calc_t)): type_t =
   let fail1 msg (e1:'a calc_t) = 
      raise (TypecheckError(msg, strip_init e1, None)) in
   let fail msg (e1:'a calc_t) (e2:'a calc_t) = 
      raise (TypecheckError(msg, strip_init e1, (Some(strip_init e2)))) in
   let rcr = calc_type ~strict:strict in
   let check_for_numeric_type opname t expr = 
      match t with 
       | IntegerT -> IntegerT
       | DoubleT  -> DoubleT
       | StringT  -> fail1 (opname^" operation on String") expr
       | AnyT     -> fail1 (opname^" operation on [Any] Type") expr
   in
   let rec arith_list_op exp_list name reconstruct =
      match exp_list with
       | [] -> IntegerT
       | t::[] -> check_for_numeric_type "Arithmetic" (rcr t) t
       | t::r  -> (
         match ((check_for_numeric_type "Arithmetic" (rcr t) t), 
                (arith_list_op r name reconstruct)) with
          | (IntegerT,IntegerT) -> IntegerT
          | (DoubleT,DoubleT)   -> DoubleT
          | (_,_)                       -> 
            if strict then
               fail (name^" of Integer and Double") t (reconstruct r)
            else DoubleT
          (* All strings should have been cleared by now *)
         )
   in
   match expr with
    | Sum(subexprs)  -> arith_list_op subexprs "Sum" (fun x -> Sum(x))
    | Prod(subexprs) -> arith_list_op subexprs "Product" (fun x -> Prod(x))
    | Neg(subexp)    -> check_for_numeric_type "Negation" (rcr subexp) subexp
    | Cmp(l,c,r) -> (* Always '1' or '0' *)
      let lht = rcr (Value(l)) in
      let rht = rcr (Value(r)) in
      if c = Eq || c = Neq then (
         match (lht,rht) with
          | (AnyT,_) -> 
            fail1 "Comparison between AnyT and something else" expr
          | (_,AnyT) ->
            fail1 "Comparison between something else and AnyT" expr
          | (StringT, StringT) -> IntegerT
          | (StringT, _) -> 
            fail1 "Comparison of String with Nonstring" expr
          | (_, StringT) ->
            fail1 "Comparison of String with Nonstring" expr
          | (IntegerT,IntegerT) -> IntegerT
          | (DoubleT,DoubleT)   -> IntegerT
          | (_,_) -> 
            if strict then
               fail1 "Comparison of Integer and Double" expr
            else
               IntegerT
      ) else 
         let _ = (check_for_numeric_type "Inequality" lht expr)
         and _ = (check_for_numeric_type "Inequality" rht expr) in
            if strict && (lht != rht) then
               fail1 "Comparison of Integer and Double" expr
            else IntegerT
    | AggSum(_,subexp) -> 
            check_for_numeric_type "AggSum" (rcr subexp) subexp
    | Value(Const(Integer(_))) -> IntegerT
    | Value(Const(Double(_)))  -> DoubleT
    | Value(Const(String(_)))  -> StringT
    | Value(Var(_,vt))         -> vt
    | Relation(_,_)            -> IntegerT (* Arity *)
    | External(_,_,et,_)      -> et
    | Definition((dvn,dvt),dd) -> 
      match (
         match (dvt,(rcr dd)) with
            | (AnyT,_) -> None
            | (lhs,AnyT) -> Some(lhs,AnyT)
            | (StringT, StringT) -> None
            | (StringT, rhs) -> Some(StringT, rhs)
            | (lhs, StringT) -> Some(lhs, StringT)
            | (IntegerT, IntegerT) -> None
            | (DoubleT, DoubleT) -> None
            | (lhs, rhs) -> if strict then Some(lhs, rhs) else None
      ) with
       | None -> IntegerT (* Always '1' *)
       | Some(lhs,rhs) -> 
         fail ("Definition type mismatch ("^(string_of_type lhs)^" := "^
               (string_of_type rhs)^")") (Value(Var(dvn,dvt))) dd
               
;;

(*** Translation Operations ***)

exception Translation_error of string * Sql.select_t

;;

let rec calc_of_sql ?(tmp_var_id=(ref (-1))) (tables:Sql.table_t list)
                     (stmt:Sql.select_t): ((string * unit calc_t) list) =
   let bail msg = raise (Translation_error(msg, stmt)) in
   let tmp_var () = 
      tmp_var_id := !tmp_var_id + 1;
      "TMP_"^(string_of_int !tmp_var_id)
   in
   let mk_cmp (lhs:unit calc_t) (op:cmp_t) (rhs:unit calc_t): unit calc_t =
      let extract_value expr =
         match expr with
            | Value(v) -> ([], v)
            | _              -> 
               let var = (tmp_var(), calc_type expr) in
                  ([Definition(var, expr)], Var(var))
      in
      let (lhs_term, lhs_value) = extract_value lhs in
      let (rhs_term, rhs_value) = extract_value rhs in
         mk_prod (lhs_term @ rhs_term @ [Cmp(lhs_value, op, rhs_value)])   
   in
   let var_to_c (s,v,t) = 
      let (_,sources,_,_) = stmt in
      (  (match s with None -> fst (Sql.source_for_var_name v tables sources) 
                     | Some(s) -> s)^
           "_"^v, 
         t)
   in
   let 
      rec rcr_e (expr:Sql.expr_t): unit calc_t = 
         match expr with
            | Sql.Const(c) -> Value(Const(c))
            | Sql.Var(v)   -> Value(Var(var_to_c v))
            | Sql.Arithmetic(a,Sql.Sum,b) -> mk_sum [rcr_e a; rcr_e b]
            | Sql.Arithmetic(a,Sql.Prod,b) -> 
               (* a little dancing here.  If b is an aggregate (and a isn't),
                  then a might need to access some of b's output variables.  
                  That is, because the actual AggSum is defined within the 
                  expression (b in this case), the output variables of the
                  relations (including the group by terms) come from inside it
                  and not from above, so we need to make sure the chaining order
                  is correct *)
               if (Sql.is_agg_expr b) && (not (Sql.is_agg_expr a)) then
                  mk_prod [rcr_e b; rcr_e a]
               else
                  mk_prod [rcr_e a; rcr_e b]
            | Sql.Arithmetic(a,Sql.Sub,b) -> mk_sum [rcr_e a; 
                                                     mk_neg (rcr_e b)]
            | Sql.Arithmetic(_,Sql.Div,_) -> bail "Division unsupported"
            | Sql.Negation(e) -> mk_neg (rcr_e e)
            | Sql.NestedQ(q) -> rcr_q q
            | Sql.Aggregate(agg,subexp) -> 
               if Sql.is_agg_expr subexp then
                  bail "Doubly Nested Aggregate"
               else
                  match agg with
                     | Sql.SumAgg ->
                        let (_,sources,condition,gb) = stmt in
                           AggSum(
                              List.map var_to_c gb, 
                              mk_prod ((List.map rcr_src sources) @
                                       [rcr_c condition; rcr_e subexp]))
      and rcr_c (cond:Sql.cond_t): unit calc_t =
         match cond with
            | Sql.ConstB(true) -> (Value(Const(Integer(1))))
            | Sql.ConstB(false) -> (Value(Const(Integer(0))))
            | Sql.Comparison(a,op,b) -> mk_cmp (rcr_e a) op (rcr_e b)
            | Sql.And(a,b) -> mk_prod [rcr_c a; rcr_c b]
            | Sql.Or(a,b) -> 
               (* Need to subtract out the union of the two to prevent double
                  counting *)
               let a_calc = rcr_c a and b_calc = rcr_c b in
                  mk_sum [a_calc; b_calc; mk_neg (mk_prod [a_calc; b_calc])]
            | Sql.Not(Sql.Exists(q)) -> mk_cmp (rcr_q q) Eq 
                                               (Value(Const(Integer(0))))
            | Sql.Not(c) -> mk_cmp (rcr_c c) Neq 
                                   (Value(Const(Integer(0))))
            | Sql.Exists(q) -> mk_cmp (rcr_q q) Neq 
                                      (Value(Const(Integer(0))))
      and rcr_q (q:Sql.select_t): unit calc_t =
         let qcalc = calc_of_sql ~tmp_var_id:tmp_var_id tables q in
            if List.length qcalc != 1 then
               bail "Nested queries must have exactly one target"
            else
               snd (List.hd qcalc)
      and rcr_src ((sn,sd):Sql.labeled_source_t): unit calc_t =
         match sd with 
            | Sql.Table(t) -> 
               let (_,sch,_) = Sql.find_table t tables in
                  Relation(t, List.map (fun (_,v,vt) -> 
                                                 (sn^"_"^v, vt)) sch)
            | Sql.SubQ(q) ->
               mk_prod (
                  List.map (fun (term,termcalc) ->
                     Definition((sn^"_"^term, calc_type termcalc),
                                     termcalc)
                  ) (calc_of_sql ~tmp_var_id:tmp_var_id tables q)
               ) 
   in
   let (targets,sources,condition,gb) = stmt in
      if Sql.is_agg_query stmt then
         List.flatten (List.map (fun (tname,target) ->
            (* We don't put an aggsum at the root because we already put it in
               wherever we encounter the Aggregate(SumAgg, ...) expression.  
               This goes for all the relations and the conditions as well. *)
            if Sql.is_agg_expr target then
               [tname, rcr_e target]
            else
               if List.exists (fun (_,v,_) -> v = tname) gb then []
               else bail ("Non Aggregate Target '"^
                          (Sql.string_of_expr target)^
                          " AS "^tname^"' is not in the GROUP BY clause")
         ) targets)
      else
         (* Rewrite non aggregate queries as a COUNT with targets as group-by
            terms *)
         let target_rewrite = 
            (List.map (fun (tname,target) -> (tname,rcr_e target)) targets) in
         [  tmp_var(), 
            (AggSum(
               List.map (fun (tname,t)->(tname, calc_type t)) target_rewrite,
               mk_prod (
                  (List.map rcr_src sources) @
                  [rcr_c condition] @
                  (List.map (fun (tname,target) -> 
                     Definition((tname,calc_type target),
                                     target)
                  ) target_rewrite)
               )
            ))
         ]               