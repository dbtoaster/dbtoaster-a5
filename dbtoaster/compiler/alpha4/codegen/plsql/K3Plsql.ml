open Util
open SourceCode

open M3
open K3.SR
open Values
open Database

(* Postgres-specific settings:
 * #variable_conflict use_column, in trigger source code generation.
 * found, in source_code_of_update
 * other potential non-standard terms, based on changes needed for Oracle:
 * -- anonymous records, that are bound at query issue time by postgres. see
 *    http://www.postgresql.org/docs/9.0/static/plpgsql-declarations.html#PLPGSQL-DECLARATION-RECORDS
 *
 * -- select-only statements (i.e. no from clause), i.e. "select 1", which can
 *    be used as a subquery to define a singleton relation. Used for pure-query
 *    implementation of K3 function applications to constant/var/expression
 *    arguments.
 *)

(* Oracle notes:
 * -- select stmt must have a from clause, thus cannot create singleton tables
 *    as a select-only subquery (as done in query binding for applying functions
 *    to singleton arguments)
 *  ++ no values expression as in postgres
 *  ++ cannot use collection expressions w/o defining collection types
 *  ++ fix: options:
 *    i. use subsitution to lift subqueries into a query with a from-clause
 *       (this works since the subquery is known to be a singleton). This will
 *       cause duplicate expression evaluation at the expense of preserving a
 *       pure-query implementation. Oracle may have a cheaper context switch
 *       between plsql engine that postgres (we need to benchmark postgres as
 *       well here).
 *    ii. memoize maximal single-use singleton subqueries, and implement
 *        in procedural form
 *  ++ solved: use the "dual" table, i.e. 'select ... from dual'. see
 *    http://download.oracle.com/docs/cd/E11882_01/server.112/e17118/queries009.htm#i2054159
 *
 * -- all variable assignments from queries must be "select ... into ..."
 *    since singleton queries are not expressions in Oracle.
 *
 * -- found variable: SQL%FOUND / SQL%NOTFOUND / SQL%ROWCOUNT etc. see
 *    http://download.oracle.com/docs/cd/E11882_01/appdev.112/e17126/static.htm#i46192
 *
 * -- no anonymous record types as in postgres, i.e. record fields must be
 *    defined up front
 *  ++ fix: in the CG, record variables have their schemas computed at the
 *     same point as declaration. Change gen_record_sym to take the schema as
 *     an argument, and track this with the record variable symbol.
 *
 * -- Oracle resolves ambiguous identifiers for variables and columns with
 *    column precedence (which is exactly what we want). see:
 *    http://download.oracle.com/docs/cd/E11882_01/appdev.112/e17126/nameresolution.htm#CHDGABGF
 *
 * -- Oracle has different (simpler+cleaner) trigger declaration syntax that Postgres. See:
 *    http://download.oracle.com/docs/cd/E11882_01/appdev.112/e17126/triggers.htm#g1043102
 *
 * Potential optimizations:
 * -- exploit FORALL loops, which sends pl/sql statements in bulk to the QP. see:
 *    http://download.oracle.com/docs/cd/E11882_01/appdev.112/e17126/tuning.htm#BABFHGHI
 *
 * -- use native compilation of triggers. see:
 *    http://download.oracle.com/docs/cd/E11882_01/appdev.112/e17126/tuning.htm#i48528
 *
 * -- we could replace temporary tables with Oracle collections... we should
 *    develop a simple benchmark to differentiate temp table vs. collection
 *    performance prior to pursuing this path. 
 *)

module CG (*: K3Codegen.CG*) =
struct

let cg_oracle = true

type op_t = source_code_op_t

(* Declaration counters: vars, aggs, tuples, relations, record types *)
type record_type_t = (string * (string list)) list
type counters_t = int * int * int * int * record_type_t

type schema_t   = string list 
type relation_t = code_t * string * schema_t
and  where_t    = source_code_t
and  select_t = code_t list * relation_t list * where_t list * code_t list
and  update_t = code_t * where_t list * code_t list
 
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
                       * counters_t * M3.var_t list
    | Main          of source_code_t (* stringify in main cg *)

(* These are not used, but we still need valid types here *)    
type db_t = NamedK3Database.db_t
type source_impl_t = source_code_t

(* value assignments can be used for single attributes, and records *)
type assign_t = NoAssign | SkipSelect | Fields of schema_t | Value of string

(* Accessors *)
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
let is_query c = match c with QueryV _ | QueryT _ | QueryC _ -> true | _ -> false 

let get_var c = match c with
  | Var(x) -> x
  | _ -> failwith "invalid variable"
  
let get_memo_value c = match c with
  | Memo(_,v) -> v
  | _ -> failwith "invalid memo" 
 
let get_memo_binder v = match v with
  | Memo(b,_) -> b
  | _ -> failwith "invalid memo"


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
let pm_const pm = match pm with M3.Insert -> "insert" | M3.Delete -> "delete"
let pm_name pm  = match pm with M3.Insert -> "ins" | M3.Delete -> "del"
let pm_tuple pm = match pm with
    | M3.Insert -> (if cg_oracle then ":" else "")^"NEW"
    | M3.Delete -> (if cg_oracle then ":" else "")^"OLD" 

(* Symbol generation *)
let counters = Hashtbl.create 0
let record_types = Hashtbl.create 0

let var_c = "vars"
let var_prefix = "var"

let agg_c = "aggs"
let agg_prefix = "agg"

let record_c = "records"
let record_prefix = "tup"

let rel_c = "relations"
let rel_prefix = "rel"

let rel_alias = "ralias"
let rel_alias_prefix = "R"

let init_counters =
  Hashtbl.add counters var_c 0;
  Hashtbl.add counters record_c 0;
  Hashtbl.add counters agg_c 0;
  Hashtbl.add counters rel_c 0;
  Hashtbl.add counters rel_alias 0

let incr_counter n =
  let v = try Hashtbl.find counters n with Not_found -> 0
  in Hashtbl.replace counters n (v+1)

let get_counter n = try Hashtbl.find counters n with Not_found -> 0

let gen_init_sym() = "has_key"

let gen_var_sym () =
  let r = get_counter var_c in incr_counter var_c;
  var_prefix^(string_of_int r)

let gen_record_sym schema =
  let r = get_counter record_c in incr_counter record_c;
  let sym = record_prefix^(string_of_int r) in
    Hashtbl.replace record_types sym schema;
    sym

let gen_agg_sym() =
  let r = get_counter agg_c in incr_counter agg_c;
  agg_prefix^(string_of_int r)

let gen_rel_sym() =
  let r = get_counter rel_c in incr_counter rel_c;
  rel_prefix^(string_of_int r)

let gen_rel_alias() =
  let r = get_counter rel_alias in incr_counter rel_alias;
  rel_alias_prefix^(string_of_int r)

let reset_syms() = Hashtbl.clear counters; Hashtbl.clear record_types

(* Misc *)
let pop_back l =
  let x,y = List.fold_left
    (fun (acc,rem) v -> match rem with 
      | [] -> (acc, [v]) | _ -> (acc@[v], List.tl rem)) 
    ([], List.tl l) l
  in x, List.hd y

let get_vars arg = match arg with
  | AVar(v,_) -> [v]
  | ATuple(vt_l) -> List.map fst vt_l

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
    let sl,_,_,g = List.hd c in build_fields "column" (g@sl)
  | _ -> failwith "invalid collection for retrieving fields"

(* Stringification *)

let rec concats delim sl = String.concat delim (List.map string_of_select_t sl)
and concatc delim cl = String.concat delim (List.map string_of_code_t cl)
and concatr rl = String.concat " natural join " (List.map (fun (r,id,sch) ->
    (string_of_code_t r)^
    (if sch = [] then "" else " as "^id^"("^(String.concat "," sch)^")")) rl)
and concatw wl = String.concat " and " (List.map ssc wl)
and concata args = String.concat "," (List.flatten (List.map get_vars args))
and string_of_select_t (sl,r,c,g) =
    "select "^(if sl = [] then "*" else (concatc "," (g@sl)))^
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
    | Update (id,(c,w,_)) -> "Update("^id^","^(rcr c)^
                            (if w = [] then "" else ","^(concatw w))^")"
    | Trigger(evt,rel,args,cl,_,_) ->
        "Trigger(_,"^rel^",_,"^(String.concat ";\n" (List.map rcr cl))^")"
    | Main (sc) -> ssc sc

(* Recursively lifts nested stacks into a single one, while respecting 
 * dependencies.  *)
let rec linearize_code c =
  let rcr = linearize_code in
  let unique_memo c = match c with
    | Memo(bl,c) ->
        let uniq_bl = List.fold_left (fun acc b ->
          if List.mem b acc then acc else acc@[b]) [] bl
        in Memo(uniq_bl,c)
    | _ -> c
  in
  let fixpoint new_c =
    (* TODO: this duplication should be handled upstream, but do it here for now *)
    if new_c = c then unique_memo c else rcr new_c in
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
  | Update(r,(Memo(bl,c),w,k)) -> rcr      (Memo(bl,Update(r,(c,w,k))))
  | Update(r,(c,w,k))          -> fixpoint (Update(r,(rcr c,w,List.map rcr k)))      
  | _ -> c
 
(* Source code generation, for pretty stringification during main function
 * construction. Note that unlike previous code generator backends, this one
 * keeps its internal form until the very end, to facilitiate internal late-stage 
 * optimizations such as linearize_code and memoize *) 
let rec append_rel_id id = if id = "" then id else " as "^id 
and append_sch sch =
  if sch = [] then "" else "("^(String.concat "," sch)^")"
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

and source_code_of_assign_relation c = match c with
  | Relation(id,fields) ->
    (* TODO: types *)
    let f = String.concat "," (List.map (fun v -> v^" real") fields)
    in Lines["create temporary table "^id^"("^f^")"]
  | _ -> failwith ("invalid relation :"^(string_of_code_t c))

and source_code_of_select ?(assign = NoAssign) (sl,r,c,g) =
  let from_clause l sc =
    if l = [] then 
      if cg_oracle then inl "from dual" else inl ""
    else csc (Lines[]) sc
  in
  let clause l sc = if l = [] then inl "" else csc (Lines[]) sc in
  let rcrl cl = List.map (fun c -> 
    let r = source_code_of_code ~assign:NoAssign c in
    if is_query c then csc (csc (inl "(") r) (inl ")") else r) cl in
  let assignments sc = match assign with
    | Fields sch -> csc sc (inl (" into "^(String.concat "," sch)))
    | Value v -> csc sc (inl (" into "^v))
    | _ -> sc
  in
  let subqueries rl =
    let auxsq c id sch = match c,id,sch with
      | Relation _,x,[] | ValueRelation _,x,[] ->
          csc (isq (source_code_of_code c)) (inl (append_rel_id x))
      | _ -> let alias = (append_rel_id id)^(append_sch sch)
             in csc (csc (inl "(") (isq (source_code_of_code c)))
                         (inl (")"^(if alias = "" then " as R" else alias)))
    in cscl ~delim:" natural join " (List.map (fun (c,id,sch) -> auxsq c id sch) rl)
  in
  let r = flatten_sc (cscl ~delim:" " 
    [cscl ~delim:" "
      [(if assign = SkipSelect then inl "" else inl("select"));
         assignments
           (if sl = [] then inl("*") else (cscl ~delim:"," (rcrl (g@sl))))];
     (from_clause r (cdsc " " (inl ("from")) (subqueries r)));
     (clause c (cdsc " " (inl ("where")) (cscl ~delim:" and " c)));
     (clause g (cdsc " " (inl ("group by")) (cscl ~delim:"," (rcrl g))))])
  in 
  let has_sum_aggregate cl = List.exists (fun c -> match c with
    | Aggregate("sum",_) -> true | _ -> false) sl 
  in if not(has_sum_aggregate sl) then r
     else csc (inl ((if assign = SkipSelect then "" else "select ")^
                     "case when r is null then 0 else r end from ("))
              (csc (if assign = SkipSelect then csc (inl "select") r else r)
                   (inl ") as R(r)"))

and source_code_of_update (id,(c,w,k)) =
  let vsc =
    let assign_param =
      if (is_value c || is_memo_value c) then
        if cg_oracle then Value("update_v") else SkipSelect
      else failwith "invalid assignment"
    in source_code_of_code ~assign:assign_param c in
  let wsc = csc (if w = [] then inl "" else inl "where ")
                (cscl ~delim:" and " w) in
  let t = cscl ~delim:"," ((List.map source_code_of_code k)@[inl "update_v"]) in
  let update_memo =
    if cg_oracle then flatten_sc (dsc ";" vsc)
    else flatten_sc (cscl [Lines["update_v :="]; dsc ";" (isc tab vsc)])
  in
  let update =
    [dsc ";" (cscl [Lines["update "^id^" set av = update_v"]; isc tab wsc])] in
  let insert = flatten_sc (cscl
    ([Lines["insert into "^id^" values("];t;Lines[");"]])) in
  let insert_on_update_fail =
    let fail_pred = if cg_oracle then "SQL%NOTFOUND" else "not found"
    in [Lines["if "^fail_pred^" then"]]@[(isc tab insert)]@[Lines["end if" ]]
  in flatten_sc (cscl ([update_memo]@update@insert_on_update_fail))

and source_code_of_code ?(assign = NoAssign) c =
  let rcr = source_code_of_code in
  begin match c with
    | Const c -> inl c
    | Var v -> inl v
    | ValueRelation v -> inl v
    | Relation(id,sch) -> inl (id)
    | Op(op,l,r) ->
        begin match op with
        | x when x = ifthenelse0_op ->
            inl ("(case when ("^(ssc (rcr l))^") <> 0 "^
                  "then "^(ssc (rcr r))^" else 0 end)")
        | x when x = eq_op || x = neq_op || x = lt_op || x = leq_op ->
            inl ("(case when ("^(ssc (rcr l))^") "^op^" ("^(ssc (rcr r))^")"^
                   " then 1 else 0 end)")
        | _ -> cdsc op (rcr l) (rcr r)
        end

    | Aggregate(op,c) -> inl (op^"("^(ssc (rcr c))^")")

    | QueryV s  | QueryT s -> source_code_of_select ~assign:assign s
    | QueryC sl -> cscl ~delim:" union "
                     (List.map (source_code_of_select ~assign:assign) sl)
    | Memo(bl,c) -> cscl ~delim:";" ((List.map rcr bl)@[rcr c])
    | Bind(args,b) -> failwith "unevaluated function in code"
    | For(sch,c,b) -> 
       let decl = [inl("declare");
         (sequence ~final:true (List.map (fun x -> isc tab (inl(x^" real"))) sch));
         inl("begin")]
       in cscl (decl@(List.map (isc tab) [cscl [cscl ~delim:" "
              [(inl("for "^(String.concat "," sch)^" in"));
                       (rcr c); inl("loop")];
             isc tab (sequence ~final:true [rcr b]); Lines["end loop;"]]]@
       [inl("end")]))

    | IfThenElse(p,t,e) ->
       sequence
         [csc (cscl ~delim:" " [inl("if"); (rcr p); inl("then")])
              (csc (Lines[]) (isc tab (rcr t)));
          (csc (Lines["else"]) (isc tab (rcr e)));
          Lines["end if"]]

    | Assign(v,c) -> 
      if is_tuple c || is_memo_tuple c then rcr ~assign:(Value(v)) c
      else if is_relation c then source_code_of_assign_relation c
      else 
        if cg_oracle then rcr ~assign:(Value(v)) c
        else flatten_sc (cdsc " " (inl(v^" :=")) (isq (rcr ~assign:SkipSelect c)))

    | Insert(id,c) -> cdsc " " (inl ("insert into "^id)) (rcr c)
    | Update(id,u) -> source_code_of_update (id,u) 
    | Trigger (evt,rel,args,stmtl,counters,relargs) ->
        let trig_name = (pm_name evt)^"_"^rel in
        let trig_params = sequence (List.map2 (fun x y ->
            inl(x^" real := "^(pm_tuple evt)^"."^y)) args relargs)
        in
        let (vc,ac,tc,rc,tup_types) = counters in
        let var_decls =
          let record_type_decls =
            let rec aux n i acc =
              if i >= n then acc
              else
                let rn = record_prefix^(string_of_int i) in
                let type_name = rn^"_type" in
                let sch =
                  if List.mem_assoc rn tup_types then List.assoc rn tup_types
                  else failwith ("could not find record type for "^rn)
                in
                let sch_decl = String.concat "," (List.map (fun v -> v^" real") sch)
                in aux n (i+1) (acc@[Lines["type "^type_name^" is record ("^sch_decl^")"]]) 
            in if cg_oracle then aux tc 0 [] else []
          in
          let rec declare prefix sql_type_f init n i acc =
            let rcr = declare prefix sql_type_f init n in
            if i >= n then acc
            else let nacc = acc@[inl(prefix^(string_of_int i)^" "^(sql_type_f i)^
                                 (if init = "" then "" else " := "^init))]
                 in rcr (i+1) nacc
          in 
            trig_params::
            (declare var_prefix (fun _ -> "real") "0.0" vc 0 [])@
            (declare agg_prefix (fun _ -> "real") "0.0" ac 0 [])@
            record_type_decls@
            (declare record_prefix
              (fun i ->
                if cg_oracle then (record_prefix^(string_of_int i)^"_type")
                else "record")  "" tc 0 [])@
            [inl("has_key integer"); inl("current_v real"); inl("update_v real");
             inl("init_val real"); inl("accv real")]
        in
        let drop_tmp_collections =
          let rec aux acc i =
            if i < 0 then acc
            else aux (csc (Lines
                   ["drop table rel"^(string_of_int i)^";"]) acc) (i-1) 
          in aux (Lines[]) (rc-1)
        in
        let trigger_header =
          if cg_oracle then
          [Lines["\ncreate or replace trigger on_"^trig_name];
           isc tab (inl("after "^(pm_const evt)^" on "^rel^" for each row"))]
          else
          [Lines["\ncreate function "^trig_name^" () returns trigger as $$"];
           inl("#variable_conflict use_column")] 
        in
        let trigger_footer =
          if cg_oracle then []
          else
          [inl("$$ language plpgsql;");
           inl("create trigger on_"^trig_name^" after "^(pm_const evt)^
               " on "^rel^" for each row execute procedure "^trig_name^"()")]
        in
        cscl
          (trigger_header@
          [inl("declare");
           isc tab (sequence ~final:true var_decls);
           inl("begin");
           isc tab (sequence ~final:true (List.map rcr stmtl));
           isc tab drop_tmp_collections;
           isc tab (inl "return null;");
           (let r = inl("end") in if cg_oracle then r else dsc ";" r)]@
           trigger_footer)
    | Main sc -> sc
  end 



(* SQL helpers *)
let sql_string_of_float f =
  let r = string_of_float f in
  if r.[(String.length r)-1] = '.' then r^"0" else r

let select_star c = match c with
  | Relation _ -> QueryC([([], [c, "", []], [], [])])   
  | _ -> failwith "invalid relation for select *" 

let select_query_value fn_arg v q =
  QueryC([[v], [q, gen_rel_alias(), get_vars fn_arg], [], []])

let select_query_tuple fn_arg t q = match t with
  | QueryT(sl,r,[],[]) ->
    QueryC([sl, [q, gen_rel_alias(), get_vars fn_arg]@r, [], []])
  | _ -> failwith "invalid tuple in select" 

(* TODO: define operator symbols specifically for aggregates *)
let aggregate_query fn_arg agg_c c = match c with
  | QueryC(ql) ->
    QueryV([Aggregate("sum", agg_c)], [c, "R", get_vars fn_arg], [], [])
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
    in QueryC([[agg_v], [c, "R", get_vars fn_arg], [], gb_fields])
  | _ -> failwith "invalid collection in group by aggregate query"  




(* Terminals *)
let const ?(expr = None) c = match c with
  | CFloat x -> Const(sql_string_of_float x)

let var ?(expr = None) id _ = Var(id)

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
    let gen_var v x = Var(v),[v] in
    let (lv,lsch),(rv,rsch) = gen_var "__v1" x, gen_var "__v2" y in
    let subq = [(c1,gen_rel_alias(),lsch); (c2,gen_rel_alias(),rsch)]
    in QueryV([Op(op,lv,rv)], subq, [], [])
  
  | x,QueryV([y],r,c,g) when is_const x || is_var x || is_op x ->
    QueryV([Op(op,x,y)],r,c,g)
  
  | QueryV([x],r,c,g),y when is_const y || is_var y || is_op y ->
    QueryV([Op(op,x,y)],r,c,g)
  
  | x,y when (is_const x || is_var x || is_op x)
             && (is_const y || is_var y || is_op y) -> Op(op,c1,c2)

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
    let x_tuple = if is_memo_tuple x then get_memo_value x else x in
    let new_record, sch, vars =
      let s = build_temporary_schema x_tuple in
      let r = gen_record_sym s in
      let v = vars_of_schema (List.map (fun v -> r^"."^v) s)
      in r,s,v
    in
    let rename t = match t with
      | QueryT _ -> QueryT([],[t,gen_rel_alias(),sch],[],[])
      | Memo(bl,((QueryT _) as q)) ->
        Memo(bl, QueryT([],[q,gen_rel_alias(),sch],[],[]))
      | _ -> failwith ("invalid tuple: "^(string_of_code_t t)) 
    in
    let new_t = Assign(new_record, rename t) in
    let new_e = Assign(new_record, rename e) in
    let new_tuple = QueryT(vars,[],[],[]) in
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
    in Memo([Assign(new_cid,new_c);IfThenElse(p,new_t,new_e)],
             QueryC([[], [new_c, "", []], [], []]))
  
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
  | Const _ | Var _ | Op _ ->
    let subq = QueryV([vq], [], [], [])
    in QueryV([Var(v)], [subq, gen_rel_alias(), [v]], [], []) 
  | QueryV (sl,r,c,g) -> QueryV([Var(v)], [vq, gen_rel_alias(),[v]], [], [])
  | ValueRelation _ -> QueryV([Var(v)], [vq, gen_rel_alias(), [v]], [], [])
  | _ -> failwith ("invalid value for binding "^(string_of_code_t vq))

let bind_query_tuple vl tq = match tq with
  | QueryT _ ->
    let nvl = List.map (fun v -> Var(v)) vl
    in QueryT(nvl, [tq,gen_rel_alias(),vl], [], [])
  | _ -> failwith ("invalid tuple for binding "^(string_of_code_t tq))  

let bind_query_args x y b =
  let q = match x,y with
    | AVar(v,_), z when is_value z -> bind_query_value v z
    | ATuple(vtl), z when is_tuple z -> bind_query_tuple (List.map fst vtl) z
    | _,_ -> failwith ("invalid arg binding as query "^(string_of_code_t y))
  in begin match b with
    | Const _ -> b
    | Var v -> QueryV([b], [q,"",[]], [], [])
    | Op _ -> QueryV([b], [q,"",[]], [], [])
    | ValueRelation _ -> QueryV([], [b,"",[]], [], [])
    | QueryV(sl,r,c,g) -> QueryV(sl,r@[q,"",[]], c, g)
    | QueryT(sl,r,c,g) -> QueryT(sl,r@[q,"",[]], c, g)
    | QueryC(ql) ->
        let rql = List.map (fun (sl,r,c,g) -> (sl,r@[q,"",[]],c,g)) ql
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
    let r = gen_record_sym (build_fields "column" sl) in
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

let join fn_arg y z = begin match y,z with
  | QueryC _, QueryC _ -> QueryC([[], [y, "R", []; z, "S", []], [], []])
  | _,_ -> failwith "invalid join operation"
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

  | Bind([x], y), z when is_collection y && is_collection z ->
    join x y z

  | Bind([x], y), z when is_collection y && is_memo_collection z ->
    Memo(get_memo_binder z, join x y (get_memo_value z))

  | Bind([x], y), z when (is_collection z || is_memo_collection z)
                         && (is_memo_value y || is_memo_tuple y) ->
    let new_cid,new_c =
      let id = gen_rel_sym() in
      let fields = build_fields "a" (get_schema z)  
      in id, Relation(id,fields)
    in Memo([Assign(new_cid,new_c);
             For(get_vars x, c, Insert(new_cid,y))],
            QueryC([[], [new_c, "", []], [], []]))

  | Bind([x], y), z when (is_collection z || is_memo_collection z)
                         && is_memo_collection y ->
    let y_sch = get_schema y in
    let y_tuple = QueryT(vars_of_schema y_sch, [], [], []) in
    let new_cid,new_c =
      let id,fields = gen_rel_sym(), build_fields "a" y_sch
      in id, Relation(id,fields)
    in
    let bl_z,v_z = if is_memo z
      then get_memo_binder z, get_memo_value z else [], z in
    let nl_body = For(y_sch,y,Insert(new_cid,y_tuple)) 
    in Memo(bl_z@[Assign(new_cid,new_c); For(get_vars x, v_z, nl_body)],
            QueryC([[], [new_c, "", []], [], []]))
  
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
    QueryT([], [a,"",[]; b,"",[]], [], [])
    
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
    in Memo([Assign(new_cid,new_c);
             For(get_vars x, z_c, Insert(new_cid,gbt))],
            QueryC([[], [new_c, "", []], [], []]))
  
  | _,_,_ -> failwith ("invalid group-by aggregate "^
                       (string_of_code_t fn)^" / "^
                       (string_of_code_t gbfn)^" / "^
                       (string_of_code_t c))
  end

(* Flatten is a no-op providing we actually have a collection input *)
let flatten ?(expr = None) c =
  if is_collection c || is_memo_collection c then c
  else failwith "invalid flatten operation on non-collection"

(* Tuple accessors *)
let valid_keys key_l = List.for_all (fun c -> is_const c || is_var c) key_l

let build_key_condition key_l =
  snd (List.fold_left (fun (cnt,acc) c -> match c with
    | Const c -> cnt+1, acc@[inl("a"^(string_of_int cnt)^"="^c)]
    | Var v -> cnt+1, acc@[inl("a"^(string_of_int cnt)^"="^v)]
    | _ -> failwith "invalid key") (1,[]) key_l)

let build_slice_condition pk pat =
  List.map2 (fun c id -> match c with 
    | Const c -> inl("a"^(string_of_int (id+1))^"="^c)
    | Var v -> inl("a"^(string_of_int (id+1))^"="^v)
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
          QueryV([test_agg_v], [map,"",[]], build_key_condition key_l, []))]
        in Memo(bl, Op(leq_op, Const("1"), Var(test_v)))
    | _ -> failwith ("invalid map in exists "^(string_of_code_t map))
  end

let lookup ?(expr = None) map key_l =
  if not(valid_keys key_l) then failwith "invalid keys in lookup"
  else begin match map with
    | Relation _ -> QueryV([Var("av")], [map,"",[]], build_key_condition key_l, [])
    | _ -> failwith ("invalid map in lookup "^(string_of_code_t map))
  end

let slice ?(expr = None) map pk pat =
  if not(valid_keys pk) then failwith "invalid keys in lookup"
  else begin match map with
    | Relation _ -> QueryC([([],[map,"",[]],build_slice_condition pk pat, [])])
    | _ -> failwith ("invalid map in slice "^(string_of_code_t map))
  end

(* M3 DB API *)

(* Database retrieval methods *)
let get_value   ?(expr = None) _ id =
  QueryV([Const("av")], [ValueRelation(id),"",[]],[],[])
let get_in_map  ?(expr = None) _ _ id = Relation(id,[])
let get_out_map ?(expr = None) _ _ id = Relation(id,[])
let get_map     ?(expr = None) (_,_) _ id = Relation(id,[])


(* persistent collection id, value -> update *)
let update_value ?(expr = None) id vc =
  Update(id, (vc, [], []))

(* persistent collection id, in key, value -> update *)
let update_in_map_value ?(expr = None) id keys vc =
  if not(valid_keys keys) then failwith "invalid keys in update"
  else Update(id, (vc, build_key_condition keys, keys))

(* persistent collection id, out key, value -> update *)
let update_out_map_value ?(expr = None) id keys vc =
  if not(valid_keys keys) then failwith "invalid keys in update"
  else Update(id, (vc, build_key_condition keys, keys))

(* persistent collection id, in key, out key, value -> update *)
let update_map_value ?(expr = None) id in_keys out_keys vc =
  if not(valid_keys in_keys) || not(valid_keys out_keys) then
    failwith "invalid keys in update"
  else Update(id, (vc,
         List.flatten (List.map build_key_condition [in_keys; out_keys]),
         in_keys@out_keys))


(* These updates should not be needed
 * -- TODO: should we remove them from the codegen API?
 *)
(* persistent collection id, updated collection -> update *)
let update_in_map ?(expr = None) id cc = failwith "NYI"

let update_out_map ?(expr = None) id cc = failwith "NYI"

(* persistent collection id, key, updated collection -> update *)
let update_map ?(expr = None) id keys cc = failwith "NYI"

let trigger event rel trig_args stmt_block =
  let vc,ac,tc,rc = get_counter var_c, get_counter agg_c,
                    get_counter record_c, get_counter rel_c in
  let tup_types =
    Hashtbl.fold (fun sym sch acc -> acc@[sym, sch]) record_types []
  in reset_syms();
     Trigger(event,rel,trig_args,
             List.map linearize_code stmt_block,(vc,ac,tc,rc,tup_types),[])

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
    in inl("--copy "^r^" from '"^filename^"' with delimiter '"^delim^"'")
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


let sql_type_of_calc_type_t t = match t with
  | Calculus.TInt -> "integer"
  | Calculus.TLong -> "bigint"
  | Calculus.TDouble -> "double precision"
  | Calculus.TString -> "text"
 
let main dbschema schema patterns sources triggers toplevel_queries =
  let nl = Lines[""] in
  let base_rels = sequence ~final:true (List.flatten (List.map (fun (id, sch) ->
    let tbl_params = String.concat ","
      (List.map (fun (v,t) -> v^" "^(sql_type_of_calc_type_t t)) sch)
    in 
    (if cg_oracle then
        [inl ("begin execute immediate 'drop table "^id^"'; "^
              "exception when others then null; end")]
     else [inl ("drop table if exists "^id)])@
    [inl ("create table "^id^" ("^tbl_params^")")]) dbschema))
  in
  let maps = sequence ~final:true (List.flatten (List.map
    (fun (id,ins,outs) ->
      let csv = String.concat ", " in
      let field (i,t) = "a"^(string_of_int i) in
      let field_and_type (i,t) = "a"^(string_of_int i)^" real" in
      let add_counter st l = snd(
        List.fold_left (fun (i,acc) x -> (i+1,acc@[(i,x)])) (st,[]) l) in
      let csv_ins f = csv (List.map f (add_counter 1 ins)) in
      let csv_outs f =
        csv (List.map f (add_counter (1+(List.length ins)) outs)) in
      let ivl_f, ivl_ft = csv_ins field, csv_ins field_and_type in
      let ovl_f, ovl_ft = csv_outs field, csv_outs field_and_type in
      let v_ft = "av real" in
      let concat_ne l = String.concat "," (List.filter (fun x -> x <> "") l) in
      let tbl_params = concat_ne [ivl_ft; ovl_ft; v_ft] in
      let key_params = concat_ne [ivl_f; ovl_f] in
      let secondaries =
        let pats =
          if List.mem_assoc id patterns then List.assoc id patterns else []
        in let idxname l = "idx"^(String.concat "" (List.map string_of_int l)) 
        in let col off l = String.concat ","
          (List.map (fun i -> "a"^(string_of_int (i+off))) l)
        in let aux name columns =
          if columns = "" then inl "" else
          let idxtype = if cg_oracle then "" else " using hash "
          in inl("create index "^name^" on "^id^idxtype^"("^columns^")") 
        in List.map (fun p -> match p with 
            | M3Common.Patterns.In(x,y) -> aux (idxname y) (col 1 y) 
            | M3Common.Patterns.Out(x,y) ->
                aux (idxname y) (col (1+(List.length ins)) y)) pats
      in
      let singleton_init = if ins = [] && outs = []
        then [inl("insert into "^id^" values(0)")] else []
      in
        (if cg_oracle then
          [inl ("begin execute immediate 'drop table "^id^"'; "^
                "exception when others then null; end")]
         else [inl ("drop table if exists "^id)])@
        [inl ("create table "^id^" ("^tbl_params^
                (if key_params = "" then ""
                 else ", primary key ("^key_params^")")^")")]@
         secondaries@singleton_init)
    schema)) in
  let triggers_w_relargs = List.map (fun c -> match c with
    | Trigger(e,r,a,sl,cnt,_) ->
        let rel_args =
          if List.mem_assoc r dbschema then
            List.map fst (List.assoc r dbschema)
          else failwith ("relation "^r^" not found in db schema")
        in Trigger(e,r,a,sl,cnt,rel_args)
    | _ -> failwith "invalid trigger") triggers in
  let body = sequence ~final:true
    (List.map source_code_of_code triggers_w_relargs) in
  let source_scl = List.map (fun (x,y,z) -> x) sources in
  let footer = sequence ~final:true
    ((List.flatten (List.map (fun c -> match c with
        | Trigger(evt,rel,_,_,_,_) ->
            let trig_name = (pm_name evt)^"_"^rel in
            [inl("drop trigger on_"^trig_name^" on "^rel)]@
            (if cg_oracle then [] else [inl("drop function "^trig_name^"()")])
        | _ -> failwith "invalid trigger code") triggers))@
     (List.map (fun (id,_,_) -> inl("drop table "^id)) schema)@
     (List.map (fun (id,_) -> inl("drop table "^id)) dbschema))
  in Main(cscl ([base_rels; maps; nl; body; nl]@source_scl@[nl]@
                (if cg_oracle then [] else [footer])))

let output code out_chan = match code with
  | Main sc -> (output_string out_chan (ssc sc); flush out_chan)
  | _ -> failwith "invalid code"

let to_string code = 
  failwith "Can't stringify plsql for now"
let debug_string code = 
  failwith "Can't stringify plsql for now"

end
