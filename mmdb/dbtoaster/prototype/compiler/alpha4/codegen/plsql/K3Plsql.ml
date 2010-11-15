open Util
open SourceCode

open M3
open K3.SR
open Values
open Database

module CG (*: K3Codegen.CG*) =
struct
type op_t = source_code_op_t

type schema_t   = string list 
type relation_t = code_t * schema_t
and  where_t    = source_code_t
and  group_by_t = code_t
and  select_t = code_t list * relation_t list * where_t list * group_by_t list
and  update_t = code_t * where_t list
 
and code_t =
    | Const         of string
    | Var           of string
    | ValueRelation of string
    | Relation      of string * schema_t
    | Op            of op_t * code_t * code_t
    | Aggregate     of op_t * code_t
    | QueryV        of select_t
    | QueryT        of select_t
    | QueryC        of select_t list
    | Memo          of code_t list * code_t
    | Bind          of arg_t list * code_t
    | For           of schema_t * code_t * code_t
    | IfThenElse    of code_t * code_t * code_t
    | Assign        of string * code_t
    | Insert        of string * code_t
    | Update        of string * update_t
    | Trigger       of M3.pm_t * M3.rel_id_t * M3.var_t list * code_t list
    | Main          of source_code_t (* stringify in main cg *)

(* These are not used, but we still need valid types here *)    
type db_t = NamedK3Database.db_t
type source_impl_t = source_code_t

(* Operators *)
let add_op  = "+"
let mult_op = "*"
let eq_op   = "="
let neq_op  = "<>"
let lt_op   = "<"
let leq_op  = "<="

(* These are just symbols for operator comparison, and not to be
 * used in stringification *)  
let ifthenelse0_op = "ifte"


(* Source code stringification aliases *)
let inl x = Inline x
let inline_var_list sl = List.map (fun x -> inl x) sl
 
let ssc = string_of_source_code
let csc = concat_source_code
let esc = empty_source_code
let dsc = delim_source_code
let cdsc = concat_and_delim_source_code
let cscl = concat_and_delim_source_code_list
let isc = indent_source_code
let rec iscn ?(n=1) delim bc =
  if n = 0 then bc else iscn ~n:(n-1) delim (isc delim bc) 
let ivl = inline_var_list

let sequence ?(final=false) ?(delim=stmt_delimiter) cl =
  cscl ~final:final ~delim:delim (List.filter (fun x -> not(esc x)) cl)

(* Helpers *)

let pop_back l =
  let x,y = List.fold_left
    (fun (acc,rem) v -> match rem with 
      | [] -> (acc, [v]) | _ -> (acc@[v], List.tl rem)) 
    ([], List.tl l) l
  in x, List.hd y

let get_vars arg = match arg with
  | AVar(v,_) -> [v]
  | ATuple(vt_l) -> List.map fst vt_l


let rec concats delim sl = String.concat delim (List.map string_of_select_t sl)
and concatc delim cl = String.concat delim (List.map string_of_code_t cl)
and concatr rl = String.concat "," (List.map (fun (r,sch) ->
    (string_of_code_t r)^
    (if sch = [] then "" else " as ("^(String.concat "," sch)^")")) rl)
and concatw wl = String.concat " and " (List.map ssc wl)
and concata args = String.concat "," (List.flatten (List.map get_vars args))
and string_of_select_t (sl,r,c,g) =
    "select "^(if sl = [] then "*" else (concatc "," sl))^
    (if r = [] then "" else " from "^(concatr r))^
    (if c = [] then "" else " where "^(concatw c))^
    (if g = [] then "" else " group by "^(concatc "," g))
and string_of_code_t c =
    let rcr = string_of_code_t in
    match c with 
    | Const c -> "Const("^c^")"
    | Var v -> "Var("^v^")"
    | ValueRelation v -> "ValueRelation("^v^")"
    | Relation(id,sch) -> "Relation("^id^")"^(if sch = [] then ""
                            else " as ("^(String.concat "," sch)^")")
    | Op(op,l,r) -> "Op("^op^","^(rcr l)^","^(rcr r)^")"
    | Aggregate(op,c) -> "Aggregate("^op^","^(rcr c)^")"

    | QueryV s -> "QueryV("^(string_of_select_t s)^")"
    | QueryT s -> "QueryT("^(string_of_select_t s)^")"
    | QueryC sl -> "QueryC(["^(concats ";" sl)^"])"
    
    | Memo(bl,c) -> "Memo(["^(concatc ";" bl)^"],"^(rcr c)^")"
    | Bind(args,b) -> "Bind(["^(concata args)^"], "^(rcr b)^")"
    | For(sch,c,b) -> "For(["^(String.concat "," sch)^"],"^(rcr c)^","^(rcr b)^")"
    | IfThenElse(p,t,e) -> "IfThenElse("^(rcr p)^","^(rcr t)^","^(rcr e)^")"
    | Assign(v,c) -> "Assign("^v^","^(rcr c)^")"
    | Insert (id,c) -> "Insert("^id^","^(rcr c)^")"
    | Update (id,(c,w)) -> "Update("^id^","^(rcr c)^
                            (if w = [] then "" else ","^(concatw w))^")"
    | Trigger(evt,rel,args,cl) -> "Trigger(_,"^rel^",_,"^(String.concat ";\n" (List.map rcr cl))^")"
    | Main (sc) -> ssc sc

(* Recursively lifts nested stacks into a single one, while respecting 
 * dependencies.  *)
let rec linearize_code c =
  let rcr = linearize_code in
  let fixpoint new_c = if new_c = c then c else rcr new_c in
  let flatten_bl bl = List.fold_left (fun blacc b -> match b with
      | Memo(x,y) -> blacc@x@[y] | _ -> blacc@[b]) [] bl
  in match c with
  | Memo(bl, Memo(bl2,v))      -> rcr      (Memo(flatten_bl (bl@bl2),v))
  | Memo(bl,v)                 -> fixpoint (Memo(flatten_bl (List.map rcr bl), rcr v))  
  | Bind(args,b)               -> fixpoint (Bind(args, rcr b))   
  | For(sch,Memo(bl,c),b)      -> rcr      (Memo(bl, For(sch,c,b)))
  | For(sch,c,b)               -> fixpoint (For(sch,rcr c, rcr b))
  | IfThenElse(Memo(bl,p),t,e) -> rcr      (Memo(bl,IfThenElse(p,t,e)))
  | IfThenElse(p,t,e)          -> fixpoint (IfThenElse(rcr p, rcr t, rcr e))
  | Assign(v,Memo(bl,c))       -> rcr      (Memo(bl, Assign(v,c)))
  | Assign(v,c)                -> fixpoint (Assign(v,rcr c))
  | Insert(r, Memo(bl,c))      -> rcr      (Memo(bl,Insert(r,c)))
  | Insert(r,c)                -> fixpoint (Insert(r,rcr c))
  | Update(r,(Memo(bl,c),w))   -> rcr      (Memo(bl,Update(r,(c,w))))
  | Update(r,(c,w))            -> fixpoint (Update(r,(rcr c,w)))      
  | _ -> c
 
(* Source code generation, for pretty stringification during main function
 * construction. Note that unlike previous code generator backends, this one
 * keeps its internal form until the very end, to facilitiate internal late-stage 
 * optimizations such as linearize_code and memoize *)
let rec append_sch sch =
  if sch = [] then "" else " as ("^(String.concat "," sch)^")"
and untab s =
  if String.length s < indent_width then s
  else if (String.sub s 0 indent_width) = tab
    then (" "^(String.sub s indent_width ((String.length s)-indent_width)))
  else s
and isq sc = match sc with Inline _ -> sc | _ -> isc tab sc
and flatten_sc sc = match sc with
  | Lines(l) ->
    let linelen = List.fold_left (+) 0 (List.map String.length l) in
      if linelen < 100 then
        inl(String.concat "" ((List.hd l)::(List.map untab (List.tl l))))
      else sc
  | Inline _ -> sc
and source_code_of_select (sl,r,c,g) =
  let lines l sc = if l = [] then inl "" else csc (Lines[]) sc in
  let auxcl cl = List.map source_code_of_code cl in
  let subqueries rl =
    let auxsq c sch = match c,sch with
      | Relation _,[] | ValueRelation _,[] -> isq (source_code_of_code c)
      | _ ->  csc (csc (inl "(") (isq (source_code_of_code c)))
                  (inl (")"^(append_sch sch)))
    in cscl ~delim:"," (List.map (fun (c,sch) -> auxsq c sch) rl)
  in
  flatten_sc (cscl ~delim:" " 
    [cscl ~delim:" "
      [inl("select");
        (if sl = [] then inl("*") else (cscl ~delim:"," (auxcl sl)))];
     (lines r (cdsc " " (inl ("from")) (subqueries r)));
     (lines c (cdsc " " (inl ("where")) (cscl ~delim:" and " c)));
     (lines g (cdsc " " (inl ("group by")) (cscl ~delim:"," (auxcl g))))])
and source_code_of_update (id,(c,w)) =
  let vsc = source_code_of_code c in
  let wsc = csc (if w = [] then inl "" else inl "where ")
                (cscl ~delim:" and " w)
  in flatten_sc (cscl
       [Lines["update "^id^" set av ="]; isc tab vsc; isc tab wsc])
and source_code_of_code c =
  let rcr = source_code_of_code in
  begin match c with
    | Const c -> inl c
    | Var v -> inl v
    | ValueRelation v -> inl v
    | Relation(id,sch) -> inl (id^(append_sch sch))
    | Op(op,l,r) ->
        begin match op with
        | x when x = ifthenelse0_op ->
            inl ("(case when "^(ssc (rcr l))^" <> 0 "^
                  "then "^(ssc (rcr r))^" else 0 end)")
        | _ -> cdsc op (rcr l) (rcr r)
        end
    | Aggregate(op,c) -> inl (op^"("^(ssc (rcr c))^")")
    | QueryV s  | QueryT s -> source_code_of_select s
    | QueryC sl -> cscl ~delim:" union " (List.map source_code_of_select sl)
    | Memo(bl,c) -> cscl ~delim:";" ((List.map rcr bl)@[rcr c])
    | Bind(args,b) -> failwith "unevaluated function in code"
    | For(sch,c,b) ->
       cscl [cscl ~delim:" "
              [(inl("for "^(String.concat "," sch)^" in")); (rcr c); inl("loop")];
             isc tab (sequence ~final:true [rcr b]); Lines["end loop"]]
    | IfThenElse(p,t,e) ->
       sequence
         [csc (cscl ~delim:" " [inl("if"); (rcr p); inl("then")])
              (csc (Lines[]) (isc tab (rcr t)));
          (csc (Lines["else"]) (isc tab (rcr e)));
          Lines["end if"]]

    | Assign(v,c) -> flatten_sc (cdsc " " (inl(v^" :=")) (isq (rcr c)))
    | Insert(id,c) -> cdsc " " (inl ("insert into "^id)) (rcr c)
    | Update(id,u) -> source_code_of_update (id,u) 
    | Trigger (evt,rel,args,stmtl) ->
        let pm_const pm = match pm with | M3.Insert -> "insert" | M3.Delete -> "delete" in
        let pm_name pm  = match pm with | M3.Insert -> "ins" | M3.Delete -> "del" in 
        let trig_name = (pm_name evt)^"_"^rel in
        cscl
          [Lines["\ncreate function "^trig_name^" () returns trigger as $$"];
           isc tab (sequence ~final:true (List.map rcr stmtl));
           inl("$$ language plpgsql;");
           inl("create trigger on_"^trig_name^" after "^(pm_const evt)^
               " on "^rel^" for each row execute procedure "^trig_name)]
    | Main sc -> sc
  end 


let is_const c = match c with Const _ -> true | _ -> false
let is_var c = match c with Var _ -> true | _ -> false
let is_query_value c = match c with QueryV _ -> true | _ -> false
let is_relation c = match c with Relation _ -> true | _ -> false
let is_op c = match c with Op _ -> true | _ -> false

let is_value c = match c with
  | Const _ | Var _ | QueryV _ | ValueRelation _ | Op _ -> true
  | _ -> false

let is_tuple c = match c with QueryT _ -> true | _ -> false
let is_collection c = match c with Relation _ | QueryC _ -> true | _ -> false

let is_memo c = match c with Memo _ -> true | _ -> false
let is_memo_value c = match c with Memo(_,c) -> is_value c | _ -> false
let is_memo_tuple c = match c with Memo(_,c) -> is_tuple c | _ -> false
let is_memo_collection c = match c with Memo(_,c) -> is_collection c | _ -> false

let is_condition c = match c with IfThenElse _ -> true | _ -> false

let get_var c = match c with
  | Var(x) -> x
  | _ -> failwith "invalid variable"
  
let get_memo_value c = match c with
  | Memo(_,v) -> v
  | _ -> failwith "invalid memo" 
 
let get_memo_binder v = match v with
  | Memo(b,_) -> b
  | _ -> failwith "invalid memo"

(* Symbol generation *)
let counters = Hashtbl.create 0

let var_c = "vars"
let var_prefix = "var"

let agg_c = "aggs"
let agg_prefix = "agg"

let record_c = "records"
let record_prefix = "tup"

let rel_c = "relations"
let rel_prefix = "rel"

let init_counters =
  Hashtbl.add counters var_c 0;
  Hashtbl.add counters record_c 0;
  Hashtbl.add counters agg_c 0;
  Hashtbl.add counters rel_c 0

let incr_counter n =
  let v = try Hashtbl.find counters n with Not_found -> 0
  in Hashtbl.replace counters n (v+1)

let get_counter n = try Hashtbl.find counters n with Not_found -> 0

let init_test_decl = "has_key integer"
let gen_init_sym() = "has_key"

let gen_var_sym () =
  let r = get_counter var_c in incr_counter var_c;
  var_prefix^(string_of_int r)

let gen_record_sym () =
  let r = get_counter record_c in incr_counter record_c;
  record_prefix^(string_of_int r)

let gen_agg_sym() =
  let r = get_counter agg_c in incr_counter agg_c;
  agg_prefix^(string_of_int r)

let gen_rel_sym() =
  let r = get_counter rel_c in incr_counter rel_c;
  rel_prefix^(string_of_int r)

let reset_syms() = Hashtbl.clear counters

let get_tuple_fields t = match t with
  | QueryT(sl,_,_,_) -> sl
  | _ -> failwith "invalid tuple for retrieving fields" 

let vars_of_schema sch = List.map (fun v -> Var(v)) sch 

let build_fields prefix l =
  snd(List.fold_left (fun (cnt,acc) _ ->
        cnt+1, acc@[prefix^(string_of_int cnt)]) (1,[]) l)
    
let build_temporary_schema tuple = build_fields "a" (get_tuple_fields tuple)

let build_tuple_schema r tuple =
  vars_of_schema (build_fields (r^".column") (get_tuple_fields tuple))

let get_schema c =  match c with
  | Memo(_,QueryC(c)) | QueryC(c) ->
    let sl,_,_,_ = List.hd c in build_fields "column" sl
  | _ -> failwith "invalid collection for retrieving fields"


(* SQL helpers *)
let sql_string_of_float f =
  let r = string_of_float f in
  if r.[(String.length r)-1] = '.' then r^"0" else r

let select_star c = match c with
  | Relation _ -> QueryC([([], [c, []], [], [])])   
  | _ -> failwith "invalid relation for select *" 

let select_query_value fn_arg v q = QueryC([[v], [q, get_vars fn_arg], [], []])

let select_query_tuple fn_arg t q = match t with
  | QueryT(sl,r,[],[]) -> QueryC([sl, [q, get_vars fn_arg]@r, [], []])
  | _ -> failwith "invalid tuple in select" 

(* TODO: define operator symbols specifically for aggregates *)
let aggregate_query fn_arg agg_c c = match c with
  | QueryC(ql) -> QueryV([Aggregate("sum", agg_c)], [c, get_vars fn_arg], [], [])
  | _ -> failwith "invalid collection in aggregate query" 

let get_group_by_fields fn_arg c = match c with
  | Const _ -> [c]
  | Var _ -> [c]
  | QueryV (sl,[],[],[]) -> sl
  | QueryT (sl,[],[],[]) -> sl
  | QueryV (sl,_,_,_) -> [c]
  | QueryT (sl,_,_,_) -> [c]
  | _ -> failwith "invalid group by fields"     

let group_by_aggregate_query fn_arg agg_c gb_fields c = match c with
  | QueryC(ql) -> 
    let agg_v = Aggregate("sum", agg_c)
    in QueryC([[agg_v], [c, get_vars fn_arg], [], gb_fields])
  | _ -> failwith "invalid collection in group by aggregate query"  




(* Terminals *)
let const ?(expr = None) c = match c with
  | CFloat x -> Const(sql_string_of_float x)

let var ?(expr = None) id = Var(id)

(* Tuples *)
let tuple ?(expr=None) fields =
  let bl,vl = List.fold_left (fun (acc_bl,acc_vl) c ->
    if not(is_memo c) then acc_bl, acc_vl@[c]
    else acc_bl@(get_memo_binder c), acc_vl@[get_memo_value c])
    ([],[]) fields in
  let r = QueryT(vl,[],[],[])
  in if bl = [] then r else Memo(bl,r)

let project ?(expr=None) tuple idx = match tuple with
  | QueryT(fields,r,c,g) -> QueryT(List.map (List.nth fields) idx,r,c,g)
  | Memo(bl,QueryT(fields,b,c,g)) ->
    Memo(bl,QueryT(List.map (List.nth fields) idx,b,c,g))
  | _ -> failwith "invalid tuple"

(* Native collections *)
let singleton ?(expr = None) cel cel_t =
  begin match cel with
    | Const _ | Var _ -> QueryC([[cel],[],[],[]])
    | QueryV (sl,r,c,g) -> QueryC([(sl,r,c,g)])
    | QueryT(q) -> QueryC([q])
    | Memo(bl,QueryT(q)) -> Memo(bl,QueryC([q]))
    |  _ -> failwith "invalid singleton input"
  end

let rec combine ?(expr = None) c1 c2 =
  begin match c1,c2 with
  | Memo(bl1, x), Memo(bl2, y) when is_collection x && is_collection y ->
    (* TODO: what if memoizations for bl1 overlap with bl2? *)
    Memo(bl1@bl2, combine x y)

  | Memo(bl1, x), y | x, Memo(bl1, y)
    when is_collection x && is_collection y -> Memo(bl1, combine x y)
 
  | x, y when is_relation x -> combine (select_star x) y
  | x, y when is_relation y -> combine x (select_star y)
  | QueryC(qc1), QueryC(qc2) -> QueryC(qc1@qc2)
  | _,_ -> failwith "invalid collections in combine" 
  end



(* Arithmetic, comparision operators *)

(* Functional ops *) 
let apply_op op c1 c2 =
  begin match c1, c2 with
  | QueryV([x],_,_,_), QueryV([y],_,_,_) ->
    let gen_var v x =
      if is_var x then x,[get_var x] else Var(v),[v] in
    let (lv,lsch),(rv,rsch) = gen_var "v1" x, gen_var "v2" y
    in QueryV([Op(op,lv,rv)], [(c1,lsch); (c2,rsch)], [], [])
  
  | x,QueryV([y],r,c,g) when is_const x || is_var x || is_op x ->
    QueryV([Op(op,x,y)],r,c,g)
  
  | QueryV([x],r,c,g),y when is_const y || is_var y || is_op y ->
    QueryV([Op(op,x,y)],r,c,g)
  
  | x,y when (is_const x || is_var x || is_op x)
             && (is_const y || is_var y || is_op y)
    -> Op(op,c1,c2)
  | _,_ -> failwith ("invalid apply op to non-value operands "^
                     (string_of_code_t c1)^" / "^(string_of_code_t c2)) 
  end

(* Note in K3 our ops always apply to singletons, since they are always in map
 * functions whenever applied to slices *)
let rec nest_op_conditions expr op l r =
  let aux = op_expr expr op in
  begin match l with
  | IfThenElse(pc, tc, ec) ->
    begin match r with
    | IfThenElse(pcr, tcr, ecr) -> IfThenElse(pc,
        IfThenElse(pcr, aux tc tcr, aux tc ecr),
        IfThenElse(pcr, aux ec tcr, aux ec ecr))
    | _ -> IfThenElse(pc, aux tc r, aux ec r)
    end    
  | _ ->
    begin match r with
    | IfThenElse(pc, tc, ec) -> IfThenElse(pc, aux l tc, aux l ec)
    | _ -> failwith "invalid condition operation"
    end 
  end

and op_expr expr op l r =
  begin match l, r with
    | x,y when is_condition x || is_condition y ->
        nest_op_conditions expr op l r
    
    (* TODO: what if binders overlap? *)
    | Memo(bl1,v1), Memo(bl2,v2) when is_value v1 && is_value v2 ->
        Memo(bl1@bl2, apply_op op v1 v2)
    
    | Memo(bl, v1), v2 when is_value v1 && is_value v2 -> 
        Memo(bl, apply_op op v1 v2)  
    
    | v1, Memo(bl, v2) when is_value v1 && is_value v2 -> 
        Memo(bl, apply_op op v1 v2)
    
    | v1, v2 when is_value v1 && is_value v2 -> apply_op op v1 v2
    
    | _, _ -> failwith ("invalid operand code: "^
                        (string_of_code_t l)^" / "^(string_of_code_t r))
  end

(* op type, lhs, rhs -> op *)
let op ?(expr = None) = op_expr expr

(* predicate, then clause, else clause -> condition *) 
let ifthenelse ?(expr = None) p t e =
  begin match t,e with
  | x,y when (is_value x || is_memo_value x)
             && (is_value y || is_memo_value y)
    ->
    let new_v = gen_var_sym() in
    let new_t = Assign(new_v, x) in
    let new_e = Assign(new_v, y) in
    Memo([IfThenElse(p,new_t,new_e)],Var(new_v))

  | x,y when (is_tuple x || is_memo_tuple x)
             && (is_tuple y || is_memo_tuple y)
    ->
    let new_record, new_fields =
      let r = gen_record_sym() in
      r, (build_tuple_schema r
           (if is_memo_tuple x then get_memo_value x else x))
    in
    let new_t = Assign(new_record,t) in
    let new_e = Assign(new_record,e) in
    let new_tuple = QueryT(new_fields,[],[],[]) in
    Memo([IfThenElse(p,new_t,new_e)],new_tuple)
  
  | x,y when (is_collection x || is_memo_collection x)
             && (is_collection y || is_memo_collection y)
    ->
    let x_sch = get_schema x in
    let y_sch = get_schema y in
    let x_tuple = QueryT(vars_of_schema x_sch,[],[],[]) in
    let y_tuple = QueryT(vars_of_schema y_sch,[],[],[]) in
    let new_cid, new_c =
      let id = gen_rel_sym() in
      let fields = build_fields id x_sch in
      id, Relation(id,fields) in
    let new_t = For(x_sch, x, Insert(new_cid,x_tuple)) in
    let new_e = For(y_sch, y, Insert(new_cid,y_tuple))
    in Memo([IfThenElse(p,new_t,new_e)],new_c)
  
  | Bind _, Bind _ 
  | For _, For _ | IfThenElse _, IfThenElse _ 
  | Assign _, Assign _ | Insert _, Insert _    
  | Update _, Update _ -> IfThenElse(p,t,e)

  | _,_ -> failwith ("unsupported branch types: "^
                     (string_of_code_t t)^" / "^(string_of_code_t e))
  end

(* statements -> block *)
let block ?(expr = None) cl =
  let x,y = pop_back cl in
  begin match y with
  | Var _ | QueryV _ | QueryT _ | QueryC _ -> Memo(x,y)
  | Memo(bl,QueryV _) -> Memo(x@bl,y)
  | Memo(bl,QueryT _) -> Memo(x@bl,y)
  | Memo(bl,QueryC _) -> Memo(x@bl,y)
  | _ -> failwith ("invalid block return value: "^(string_of_code_t y)) 
  end

let iterate ?(expr = None) fn c =
  begin match fn,c with
  | Bind([x],b), y when is_collection y || is_memo_collection y ->
    For(get_vars x, c, b)
  | _,_ -> failwith "invalid iterate function/collection" 
  end

(* arg, body -> fn *)
let lambda ?(expr = None) arg b = Bind([arg],b)

(* arg1, type, arg2, type, body -> assoc fn *)
let assoc_lambda ?(expr = None) arg1 arg2 b = Bind([arg1;arg2],b)

(* Binding *)
let bind_query_value v vq = match vq with
  | Const c -> QueryV([Var(v)], [vq, [v]], [], [])
  | Var v2 -> QueryV([Var(v)], [vq, [v]], [], [])
  | QueryV (sl,r,c,g) -> QueryV([Var(v)], [vq, [v]], [], [])
  | Op _ -> QueryV([Var(v)], [vq, [v]], [], []) 
  | ValueRelation _ -> QueryV([Var(v)], [vq, [v]], [], [])
  | _ -> failwith ("invalid value for binding "^(string_of_code_t vq))

let bind_query_tuple vl tq = match tq with
  | QueryT _ -> QueryT(List.map (fun v -> Var(v)) vl, [tq,vl], [], [])
  | _ -> failwith ("invalid tuple for binding "^(string_of_code_t tq))  

let bind_query_args x y b =
  let q = match x,y with
    | AVar(v,_), z when is_value z -> bind_query_value v z
    | ATuple(vtl), z when is_tuple z -> bind_query_tuple (List.map fst vtl) z
    | _,_ -> failwith ("invalid arg binding as query "^(string_of_code_t y))
  in begin match b with
    | Const _ -> b
    | Var v -> QueryV([b], [q,[]], [], [])
    | Op _ -> QueryV([b], [q,[]], [], [])
    | ValueRelation _ -> QueryV([], [b,[]], [], [])
    | QueryV(sl,r,c,g) -> QueryV(sl,r@[q,[]], c, g)
    | QueryT(sl,r,c,g) -> QueryT(sl,r@[q,[]], c, g)
    | QueryC(ql) ->
        let rql = List.map (fun (sl,r,c,g) -> (sl,r@[q,[]],c,g)) ql
        in QueryC(rql)
    | _ -> failwith ("invalid function body for query arg binding: "^
                      (string_of_code_t b))
  end

(* TODO: tuple and collection variables *)
let bind_memo_args x y =
  let bl_y, v_y = if is_memo y
    then get_memo_binder y, get_memo_value y else [], y
  in begin match x,v_y with
  | AVar(av,_),z when is_value z -> bl_y, [Assign(av,v_y)] 
  | AVar(av,_),_ ->
    failwith ("plsql only binds variables to values "^(string_of_code_t v_y))
  | ATuple(avl), QueryT(sl,r,c,g) ->
    let r = gen_record_sym() in
    let sch = build_tuple_schema r v_y in
    (bl_y, ([Assign(r, v_y)]@
      (List.map2 (fun (v,_) vc -> Assign(v,vc)) avl sch)))
  | ATuple _, _ -> failwith ("invalid tuple binding "^(string_of_code_t v_y))
  end

let apply ?(expr = None) fn arg =
  let apply_aux fn_arg argc b =
    let bl, blargs = bind_memo_args fn_arg argc in
    begin match b with
    | a when is_memo a -> 
        Memo(bl@blargs@(get_memo_binder a), get_memo_value a)
    
    | a when (is_value a || is_tuple a || is_collection a) -> 
        if bl = [] then bind_query_args fn_arg argc a
        else Memo(bl@blargs, a)

    | _ -> Memo(bl@blargs,b)
    end
  in
  begin match fn, arg with
  | Bind([x],b), y when (is_value y || is_memo_value y ||
                        is_tuple y || is_memo_tuple y ||
                        is_collection y || is_memo_collection y)
    -> apply_aux x y b
  | _,_ -> failwith ("invalid function application "^
                     (string_of_code_t fn)^" / "^(string_of_code_t arg))  
  end

let map ?(expr = None) fn fn_rv_t c =
  begin match fn, c with
  | Bind([x], y), z when is_value y && is_collection z ->
    select_query_value x y z
  
  | Bind([x], y), z when is_value y && is_memo_collection z ->
    Memo(get_memo_binder z, select_query_value x y (get_memo_value z))
  
  | Bind([x], y), z when is_tuple y && is_collection z ->
    select_query_tuple x y z

  | Bind([x], y), z when is_tuple y && is_memo_collection z ->
    Memo(get_memo_binder z, select_query_tuple x y (get_memo_value z))

  | Bind([x], b), y when (is_collection y || is_memo_collection y)
                         && (is_memo_value b || is_memo_tuple b) ->
    let new_cid,new_c =
      let id = gen_rel_sym() in
      let fields = build_fields id (get_schema y)  
      in id, Relation(id,fields) in
    Memo([For(get_vars x, c, Insert(new_cid,b))], new_c)
  
  | _,_ -> failwith ("invalid map function/collection"^
                     (string_of_code_t fn)^" / "^(string_of_code_t c))
  end

let rec is_sum_aggregate agg_v c =
  begin match c with
    | Op(add_op, Var(x), _) when x = agg_v -> true
    | Op(add_op, _, Var(x)) when x = agg_v -> true
    | QueryV([x],_,_,_) -> is_sum_aggregate agg_v x
    | _ -> false 
  end

let rec get_aggregate_value agg_v c =  
  begin match c with
    | Op(add_op, Var(x), y)
    | Op(add_op, y, Var(x)) when x = agg_v -> y
    | QueryV([x],r,c,g) -> QueryV([get_aggregate_value agg_v x],r,c,g)
    | _ -> failwith ("invalid aggregate function "^(string_of_code_t c))
  end

let aggregate ?(expr = None) fn init c =
  if not(is_value init) then
    failwith ("aggregate initializer is not a value "^(string_of_code_t init))
  else begin match fn, c with
  | Bind([w;x], y), z when is_collection z
                           && is_sum_aggregate (List.hd (get_vars x)) y ->
    aggregate_query w (get_aggregate_value (List.hd (get_vars x)) y) z

  | Bind([w;x], y), z when is_memo_collection z 
                           && is_sum_aggregate (List.hd (get_vars x)) y ->
    Memo(get_memo_binder z,
      aggregate_query w
        (get_aggregate_value (List.hd (get_vars x)) y) (get_memo_value z))

  | Bind([w;x], y), z when (is_collection z || is_memo_collection z)
                           && (is_value y || is_memo_value y) ->
    let new_agg = gen_agg_sym() in
    let bl_y, v_y =
      if is_memo_value y then get_memo_binder y, get_memo_value y
      else [], y in
    let new_bl_y = [Assign ((List.hd (get_vars x)), (Var(new_agg)))]@bl_y in
    let app_y = Memo(new_bl_y, v_y) in
    let z_c = if is_memo_collection z then get_memo_value z else z in
    Memo([For(get_vars w, z_c, Assign(new_agg, app_y))], Var(new_agg))

  | _,_ -> failwith ("invalid aggregate function/collection "^
                     (string_of_code_t fn)^" / "^(string_of_code_t c)) 
  end


let singleton_join q1 q2 =
  begin match q1, q2 with
  | a,b when (is_query_value a || is_tuple a) 
             && (is_query_value b || is_tuple b) ->
    QueryT([], [a,[]; b,[]], [], [])
    
  | QueryV(sl,r,c,g), b | QueryT(sl,r,c,g), b when is_value b
    -> QueryT(sl@[b],r,c,g)
    
  | a, QueryV(sl,r,c,g) | a, QueryT(sl,r,c,g) when is_value a
    -> QueryT([a]@sl,r,c,g)
    
  | a,b when is_value a && is_value b -> QueryT([a;b],[],[],[])
  | _,_ -> failwith ("invalid singleton join "^
                     (string_of_code_t q1)^", "^(string_of_code_t q2))
  end

let group_by_aggregate ?(expr = None) fn init gbfn c =
  if not(is_value init) then
    failwith ("aggregate initializer is not a value "^(string_of_code_t init))
  else begin match fn, gbfn, c with
  | Bind([u;v],w), Bind([x],y), z when u=x && (is_collection z)
    && (is_value y || is_tuple y)
    && (is_sum_aggregate (List.hd (get_vars v)) w) ->
    group_by_aggregate_query u 
      (get_aggregate_value (List.hd (get_vars v)) w) 
      (get_group_by_fields x y) z

  | Bind([u;v],w), Bind([x],y), z when u=x && (is_memo_collection z)
    && (is_value y || is_tuple y)
    && (is_sum_aggregate (List.hd (get_vars v)) w) ->
    Memo(get_memo_binder z,
      group_by_aggregate_query u
        (get_aggregate_value (List.hd (get_vars v)) w)
        (get_group_by_fields x y) z)

  | Bind([u;v],w), Bind([x],y), z
      when u=x && (is_collection z || is_memo_collection z)
               && (is_value y || is_memo_value y || is_tuple y || is_memo_tuple y)
               && (is_sum_aggregate (List.hd (get_vars v)) w) ->
    let new_cid,new_c =
      let id = gen_rel_sym() in
      let fields = build_fields id (get_schema z)
      in id, Relation(id,fields) in
    let z_c = if is_memo_collection z then get_memo_value z else z in
    let aggc = get_aggregate_value (List.hd (get_vars v)) w in
    let gbt =
      match aggc,y with
      | a,b when is_value a && (is_value b || is_tuple b) -> singleton_join b a
      | a,b when is_value a && (is_memo_value b || is_memo_tuple b) ->
        Memo(get_memo_binder b, singleton_join (get_memo_value b) a)
      | _,_ -> failwith ("invalid group-by aggregate functions "^
                          (string_of_code_t y))
    in Memo([For(get_vars x, z_c, Insert(new_cid,gbt))], new_c)
  
  | _,_,_ -> failwith ("invalid group-by aggregate "^
                       (string_of_code_t fn)^" / "^
                       (string_of_code_t gbfn)^" / "^
                       (string_of_code_t c))
  end
    
let flatten ?(expr = None) c = failwith "flatten NYI"

let valid_keys key_l = List.for_all (fun c -> is_const c || is_var c) key_l

let build_key_condition key_l =
  snd (List.fold_left (fun (cnt,acc) c -> match c with
    | Const c -> cnt+1, acc@[inl("a"^(string_of_int cnt)^"="^c)]
    | Var v -> cnt+1, acc@[inl("a"^(string_of_int cnt)^"="^v)]
    | _ -> failwith "invalid key") (0,[]) key_l)

let build_slice_condition pk pat =
  List.map2 (fun c id -> match c with 
    | Const c -> inl("a"^(string_of_int id)^"="^c)
    | Var v -> inl("a"^(string_of_int id)^"="^v)
    | _ -> failwith "invalid partial key") pk pat

(* Note: exists should only be used inside ifthenelse *)
let exists ?(expr = None) map key_l =
  if not(valid_keys key_l) then failwith "invalid keys in exists"
  else begin match map with
    | Relation _ ->
        (* TODO: return val of Op *)
        let test_v = gen_init_sym() in
        let test_agg_v = Aggregate("count", Const("*")) in 
        let bl = [Assign(test_v,
          QueryV([test_agg_v], [map,[]], build_key_condition key_l, []))]
        in Memo(bl, Op(leq_op, Const("1"), Var(test_v)))
    | _ -> failwith ("invalid map in exists "^(string_of_code_t map))
  end

let lookup ?(expr = None) map key_l =
  if not(valid_keys key_l) then failwith "invalid keys in lookup"
  else begin match map with
    | Relation _ -> QueryV([Var("av")], [map,[]], build_key_condition key_l, [])
    | _ -> failwith ("invalid map in lookup "^(string_of_code_t map))
  end

let slice ?(expr = None) map pk pat =
  if not(valid_keys pk) then failwith "invalid keys in lookup"
  else begin match map with
    | Relation _ -> QueryC([([],[map,[]],build_slice_condition pk pat, [])])
    | _ -> failwith ("invalid map in slice "^(string_of_code_t map))
  end

(* M3 DB API *)

(* Database retrieval methods *)
let get_value   ?(expr = None) id = QueryV([Var("av")], [ValueRelation(id),[]],[],[])
let get_in_map  ?(expr = None) id = Relation(id,[])
let get_out_map ?(expr = None) id = Relation(id,[])
let get_map     ?(expr = None) id = Relation(id,[])


(* persistent collection id, value -> update *)
let update_value ?(expr = None) id vc =
  Update(id, (vc, []))

(* persistent collection id, in key, value -> update *)
let update_in_map_value ?(expr = None) id keys vc =
  if not(valid_keys keys) then failwith "invalid keys in update"
  else Update(id, (vc, build_key_condition keys))

(* persistent collection id, out key, value -> update *)
let update_out_map_value ?(expr = None) id keys vc =
  if not(valid_keys keys) then failwith "invalid keys in update"
  else Update(id, (vc, build_key_condition keys))

(* persistent collection id, in key, out key, value -> update *)
let update_map_value ?(expr = None) id in_keys out_keys vc =
  if not(valid_keys in_keys) || not(valid_keys out_keys) then
    failwith "invalid keys in update"
  else Update(id, (vc,
         List.flatten (List.map build_key_condition [in_keys; out_keys])))


(* These updates should not be needed
 * -- TODO: should we remove them from the codegen API?
 *)
(* persistent collection id, updated collection -> update *)
let update_in_map ?(expr = None) id cc = failwith "NYI"

let update_out_map ?(expr = None) id cc = failwith "NYI"

(* persistent collection id, key, updated collection -> update *)
let update_map ?(expr = None) id keys cc = failwith "NYI"

let trigger event rel trig_args stmt_block =
  reset_syms();
  Trigger(event,rel,trig_args, List.map linearize_code stmt_block)

(* Sources *)
let valid_adaptors =
  ["csv",","; "orderbook",","; 
   "lineitem", "|"; "orders", "|";
   "part", "|"; "partsupp", "|";
   "customer", "|"; "supplier", "|"]

let make_file_source filename rel_adaptors =
  let va = List.filter (fun (_,(aname,_)) ->
    List.mem_assoc aname valid_adaptors) rel_adaptors
  in
  let aux (r,(aname,aparams)) =
    let delim =
      if List.mem_assoc "fields" aparams then
        List.assoc "fields" aparams
      else List.assoc aname valid_adaptors
    in inl("copy "^r^" from '"^filename^"' with delimiter '"^delim^"'")
  in sequence ~final:true (List.map aux va)

(* source type, framing type, (relation * adaptor type) list 
 * -> source impl type,
 *    source and adaptor declaration code, (optional)
 *    source and adaptor initialization code (optional) *)
let source src fr rel_adaptors = 
  let source_stmts =
    match src,fr with
    | FileSource fn, Delimited "\n" -> make_file_source fn rel_adaptors
    | _ -> failwith "Unsupported data source" 
  in (source_stmts, None, None)


let main schema patterns sources triggers toplevel_queries =
  let nl = Lines[""] in
  let maps = sequence ~final:true (List.flatten (List.map
    (fun (id,ins,outs) ->
      let f (i,t) = id^"_a"^(string_of_int i)^" real" in
      let add_counter l = snd(
        List.fold_left (fun (i,acc) x -> (i+1,acc@[(i,x)])) (1,[]) l) in
      let ivl = String.concat ", " (List.map f (add_counter ins)) in
      let ovl = String.concat ", " (List.map f (add_counter outs)) in
      let v = id^"_val real" in
      let tbl_params = String.concat ", "
        (List.filter (fun x -> x <> "") [ivl; ovl; v])
      in [inl ("drop table if exists "^id);
          inl ("create table "^id^" ("^(tbl_params)^")")])
    schema)) in
  let body = sequence ~final:true (List.map source_code_of_code triggers) in
  let source_scl = List.map (fun (x,y,z) -> x) sources in
  let footer = sequence ~final:true
    (List.map (fun (id,_,_) -> inl("drop table "^id)) schema)
  in Main(cscl ([maps; nl; body; nl]@source_scl@[nl; footer]))

let output code out_chan = match code with
  | Main sc -> (output_string out_chan (ssc sc); flush out_chan)
  | _ -> failwith "invalid code"

end
