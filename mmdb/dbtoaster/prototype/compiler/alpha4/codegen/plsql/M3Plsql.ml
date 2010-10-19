(*
 * A language independence layer, for functional and imperative languages
 *
 * Code can be one of:
 * 1. functions
 * 2. values
 * 3. imperative wrapper
 * 4. nested/post code block
 * 5. future binding request
 *
 * <More documentation...> 
 *)

(*
 * TODO: map lookup ivc condition
 * TODO: functional map op naming
 * TODO: prebind, inbind
 *
 * TODO: var declarations
 *
 * TODO: test map naming
 * TODO: statement naming (dv, init_v, current_v, etc.)
 * 
 * TODO: modularize stringification
 *
 * TODO: use post code semantics consistently
 * -- for unit code: nc; pc
 * -- for a wrapper w: w(nc); pc 
 * -- what if we have a chain of wrappers, w1,w2?
 *   ++ we invoke wrappers by passing down arg code,
 *      e.g apply(w1(w2), c) = w1(w2(c))
 *   ++ the ambiguity is whether:
 *         i)  w1(w2) c = w1(w2(nc); null); pc
 *      or ii) w1(w2) c = w1(w2(nc); pc); null
 *   ++ I am in favor of option i), and if we want to add code after w2,
 *      the application of the w2 wrapper must handle this, i.e.
 *      w1(w2) c = w1(w2(c')); pc = w1(w2(nc); pc'); pc
 *      where c' = nc,pc' and pc'= f(nc,pc)
 *   ++ by default f(nc,pc) = null, unless for some optimization reason we
 *      want to split up pc
 *   ++ alternatively, rather than having w1 compute c', we can have w2 do this:
 *      w1(w2) c = w1(w2(c)); pc
 *      suppose nc',pc'= w2(c), then: w1(w2(c)) = w1(w2(nc);pc'); pc
 *      by default pc' = null
 *   ++ the question is do we compute pc' bottom-up or top-down
 *   ++ to easily implement null semantics, I think we should use top-down
 *      until we have good examples of non-null pc'
 *
 * TODO: language-independent abstraction
 * TODO: do we need full aggregation around ivc to ensure it's not wrapped code?
 *       -- ivc uses bigsums, which we must make sure are fully aggregated
 *       -- check what the M3 compiler does, it may put invoke this aggregation
 *          around the ivc
 *
 * TODO: do we want to support bindings when unrolling remainders, given we
 *       already build a list of bindings when wrapping?
 *       e.g. see init_expr, lookup_and_init_expr
 *
 * TODO: support functional conditionals, enabling functional map lookups
 *)

open M3
open M3Common.Patterns
open Util

module M3P = M3.Prepared

module CG : M3Codegen.CG = 
struct

type op_t = string

type bindings = string list

type basic_code_t = Lines of string list | Inline of string

type code_t =
    I of (code_t -> code_t) (* nesting wrapper code *)
  | F of basic_code_t
  | U of (basic_code_t * basic_code_t) (* nested code, post code *)
  | V of basic_code_t * string (* code, return value *)
  | B of string * code_t (* var to be bound before code *)

type debug_code_t = code_t
type db_t = string
type source_impl_t = string
    
(* Basic code helpers *)
let indent_width = 4
let stmt_delimiter = ";"

let inl x = Inline x

let string_of_basic_code bc =
  begin match bc with
  | Lines l -> String.concat "\n" l
  | Inline i -> i
  end

let concat_basic_code a b =
  begin match a,b with
  | Lines l, Lines l2 -> Lines(l@l2)
  | Lines l, Inline i -> Lines(l@[i])
  | Inline i, Lines l -> Lines(i::l)
  | Inline i, Inline i2 -> Inline(i^i2)  
  end

let empty_basic_code bc = bc = Lines([]) || bc = Inline("")

let delim_basic_code delim bc =
  begin match bc with
    | Lines (l) ->
      let x = List.rev l in
      Lines(List.rev (((List.hd x)^delim)::(List.tl x)))
    | Inline (i) ->
      if delim = stmt_delimiter then Lines [i^delim]
      else Inline(i^delim)
  end

let concat_and_delim_basic_code delim a b =
  concat_basic_code (delim_basic_code delim a) b

let concat_and_delim_basic_code_list ?(final=false) ?(delim="") bcl =
  if bcl = [] then inl("") else
    let r = List.fold_left (concat_and_delim_basic_code delim)
                 (List.hd bcl) (List.tl bcl)
    in if final then delim_basic_code delim r else r

let indent_basic_code s bc = begin match bc with
  | Lines l -> Lines(List.map (fun a -> s^a) l)
  | Inline i -> inl(s^i)
  end

let inline_var_list sl = List.map (fun x -> inl x) sl
 
let sbc = string_of_basic_code
let cbc = concat_basic_code
let ebc = empty_basic_code
let dbc = delim_basic_code
let cdbc = concat_and_delim_basic_code
let cbcl = concat_and_delim_basic_code_list
let ibc = indent_basic_code
let ivl = inline_var_list

let rec string_of_code c = match c with
    | I(ic) -> "I(...)"
    | F(fc) -> "F("^(sbc fc)^")"
    | U(nc,pc) -> "U{"^(sbc nc)^"}{"^(sbc pc)^"}"
    | V(c,r) -> "V("^(sbc c)^","^r^")"
    | B(v,c) -> "B("^v^","^(string_of_code c)^")"

(* Code wrapping, invokes a wrapper around the given code *)
let rec apply_wrapper wrap_f =
  fun c bindings -> match c with
    | I(rcf) -> I(fun xc -> wrap_f (rcf xc) bindings)
    | U(_) -> wrap_f c bindings
    | F(_) -> wrap_f c bindings
    | V(_,_) -> wrap_f c bindings
    | B(b, c2) -> apply_wrapper wrap_f c2 (bindings@[b])

(* Basic code generation helpers *)
let counters = Hashtbl.create 0

let sym_c = "vars"
let agg_c = "aggs"
let slice_c = "slices"

let init_counters =
  Hashtbl.add counters sym_c 0;
  Hashtbl.add counters agg_c 0;
  Hashtbl.add counters slice_c 0

let incr_counter n =
  let v = try Hashtbl.find counters n with Not_found -> 0
  in Hashtbl.replace counters n (v+1)

let get_counter n = try Hashtbl.find counters n with Not_found -> 0

let gen_sym () =
  let r = get_counter sym_c in incr_counter sym_c;
  "var"^(string_of_int r)

let gen_agg_sym() =
  let r = get_counter agg_c in incr_counter agg_c;
  "agg"^(string_of_int r)

let gen_slice_sym() =
  let r = get_counter slice_c in incr_counter slice_c;
  "slice"^(string_of_int r)

(* Debugging *)  
let debug_sequence cdebug cresdebug ccalc = ccalc
let debug_expr incr_calc = U(inl "", inl "")
let debug_expr_result incr_calc ccalc = U(inl "", inl "")
let debug_singleton_rhs_expr lhs_outv = U(inl "", inl "")
let debug_slice_rhs_expr rhs_outv = U(inl "", inl "")
let debug_rhs_init () = U(inl "", inl "")
let debug_stmt lhs_mapn lhs_inv lhs_outv = U(inl "", inl "")

(* Language-specific stringification *)

(* Primitives *)
let const_code c = match c with
  | CFloat(f) -> F(inl("select "^(string_of_float f)^" as v"))

let const = const_code

(* TODO *)
let singleton_var v = F(inl v)
let slice_var v = F(inl v)

let add_op  = "+"
let mult_op = "*"
let eq_op   = "="
let lt_op   = "<"
let leq_op  = "<="
  
(* TODO *)
let ifthenelse0_op = "ifte"
let ifthenelse0_bigsum_op = "ifte_bigsum"

(* returns whether the given op can be evaluated functionally *)
let functional op =
  not(op = ifthenelse0_op || op = ifthenelse0_bigsum_op)

(* Additional operators for stringification *)
let and_op = "and"
let or_op  = "or"
let neq_op = "<>"

let ws_start_re = Str.regexp ("^"^(Str.quote " ")^"*")

let rec indent ?(n=1) s = match n with | 0 -> s
  | _ -> indent ~n:(n-1) (ibc (String.make indent_width ' ') s)

let single_comment c = "-- "^c
let multi_comment c = "/*\n"^c^"*/"

let sequence ?(delim=stmt_delimiter) cl =
  cbcl ~delim:delim (List.filter (fun x -> not(ebc x)) cl)

let assign lv rv = inl(lv^" := "^(sbc rv))

let cond p t e : basic_code_t = cbcl
  ([Lines ["if "^(sbc p)^" then {"]; indent t; Lines["}"];]@
  (if ebc e then [e] else [Lines ["else {"]; indent e; Lines["}"]]))

let iter_fields l = String.concat "," l

let iterate_cl fields collection body = cbcl
  [Lines ["foreach "^fields;"    in ("^(sbc collection)^") {"];
   indent body; Lines ["}"]] 
    
let bind l v = (sequence (List.map (fun x -> assign x v) l))

let rebind l c = List.fold_left (fun rbc b -> B(b,rbc)) c (List.rev l)

let apply_op op l r = inl((sbc l)^" "^op^" "^(sbc r))

let apply_procedural_op op rv l r =
  if functional op then
    failwith ("op ("^op^") should be evaluated functionally")
  else if op = ifthenelse0_op then
    cond (apply_op neq_op l (inl "0")) (assign rv r) (assign rv (inl "0"))
  else cond (apply_op neq_op r (inl "0")) (assign rv l) (assign rv (inl "0"))

(* Code access helpers *)
let get_unit_code c = match c with
    | U(uc) -> sequence [fst uc; snd uc]
    | V(vc,rv) -> vc
    | _ -> failwith "invalid unit code"

let get_nested_code c = match c with
    | U(uc) -> fst uc
    | V(vc,rv) -> vc
    | _ -> failwith "invalid nested code"

(* TODO: handle sequencing if necessary *)
let add_post_code c pc =
  if ebc pc then (c,inl "") else (cbc c pc, inl "")

let add_nested_code bindings v c nc =
  if ebc nc then c
  else sequence [c; bind bindings (inl v); nc]

let add_binary_post_code c pc = match c with
  | U(c2,p2) -> let x = add_post_code c2 pc in U(fst x, p2)
  | _ -> failwith "invalid input for appending code."

let strip_post_code c = match c with | U(x,y) -> U(x,inl "") | _ -> c

(* Language-specific map primitives *)
(* Positional naming helpers *)
let map_dimensions = Hashtbl.create 10
let add_map_dim mapn num_in num_out =
  if Hashtbl.mem map_dimensions mapn then
    let x,y = Hashtbl.find map_dimensions mapn in
    if not(x = num_in && y = num_out) then
      failwith ("inconsistent dimensions found for map: "^mapn)
  else Hashtbl.replace map_dimensions mapn (num_in,num_out)

let get_map_dim mapn =
  try Hashtbl.find map_dimensions mapn
  with Not_found -> failwith ("no dimensions found for map: "^mapn)

let make_fields mapn n =
  if n = 0 then [] else
  let f i = mapn^"_a"^(string_of_int i) in
  let rec aux acc i =
    if i = n then acc@[f i] else aux (acc@[f i]) (i+1)
  in aux [] 1

let get_map_fields_l mapn num_in num_out =
  let num_fields = num_in + num_out in
  add_map_dim mapn num_in num_out;
  (make_fields mapn num_fields)

let get_map_fields mapn num_in num_out =
  String.concat "," (get_map_fields_l mapn num_in num_out)
  
let get_num_outv outv out_patterns =
  if outv = [] && out_patterns = [] then 0
  else if outv = [] then List.length (List.hd out_patterns)
  else List.length outv

(* Map access stringification *)

(* SQL map predicate helpers *)
let bind_map_vars map_fields vars =
  if map_fields = [] || vars = [] then inl "" else
  let constraints = List.map
    (fun (mf,v) -> apply_op eq_op (inl mf) (inl v))
    (List.combine map_fields vars)
  in List.fold_left (apply_op and_op)
      (List.hd constraints) (List.tl constraints)

let bind_map_fields mapn inv outv =
  let mf = get_map_fields_l mapn (List.length inv) (get_num_outv outv [])
  in bind_map_vars mf (inv@outv)

(* Returns a list of renames for map vars and a lookup predicate
 * -- pat, patv specify a partial lookup of inv if ins is true *)
let bind_map_pattern ins mapn inv outv pat patv =
  let num_inv = List.length inv in
  let offset = if ins then 0 else num_inv in
  let mf = get_map_fields_l mapn num_inv (get_num_outv outv []) in
  let mf_pat = List.map (fun i -> List.nth mf (i+offset)) pat in
  let (mf_inv,mf_outv) = 
    snd(List.fold_left (fun (i,(lacc,racc)) x ->
    if i < num_inv then (i+1,(lacc@[x],racc))
    else (i+1,(lacc,racc@[x]))) (0,([],[])) mf) in
  let cmv, cv, rmv, rv =
    if ins then mf_pat,patv,mf_inv,inv
    else (mf_inv@mf_pat,inv@patv,mf_outv,outv) in
  let cond = bind_map_vars cmv cv in
  let renames = List.map (fun (mf,v) -> mf^" as "^v) (List.combine rmv rv)
  in renames,cond

(* Map primitives *)  
let create_map n f = inl("create temporary table "^n^" as "^(sbc f))
let copy_map o n =
  inl("create temporary table "^n^" as select * from "^(sbc o))

(* Getters *)
let get_map_sing v mapn inv outv =
  let where_c =
    let x = bind_map_fields mapn inv outv in
    if ebc x then x else inl(" where "^(sbc x))
  in cbc (inl("select "^mapn^"_val as "^v^" from "^mapn)) where_c

(* Returns a query to access an in or out tier.
 * The query includes only those attrs in the tier (and the value if an out
 * tier is requested).
 * Empty inv/outv/pat/patv semantics:
 * -- if the map has no in vars, an in tier request yields empty code.
 * -- an out tier access can never be empty, since we add the map value.
 * -- if map is given an empty partial key, but has in/out vars this is
 *    a full scan. *)
let get_map_slice_aux ins mapn inv outv pat patv =
  let rename,cond = bind_map_pattern ins mapn inv outv pat patv in
  let fields = rename@(if ins then [] else [mapn^"_val"]) in
  let where_c = if ebc cond then cond else inl(" where "^(sbc cond)) in
  let rc =
    if fields = [] then inl "" else
    let select_list = String.concat "," fields in
    cbc (inl("select "^(if ins then "distinct " else "")^
             select_list^" from "^mapn)) where_c
  in fields, rc
    

let get_map_out_slice = get_map_slice_aux false 
let get_map_in_slice = get_map_slice_aux true 

(* This query must return a single map entry, otherwise a runtime exception
 * will be thrown. The code must assign the entry's value to <rv> and
 * set <rv>_valid if successful. *)
(* TODO: make FOUND independent *)
let get_map_sing_val mapn inv outv rv =
  Lines(["select "^mapn^"_val into strict "^rv^" from "^mapn^
         " where "^(sbc (bind_map_fields mapn inv outv));
        rv^"_valid := FOUND"])

(* Accesses a memoized slice value *)
let get_slice_val x = inl("select * from "^x)

(* Adders *)
let add_map n k v =
  inl("insert into "^n^" values ("^k^(if k = "" then "" else ", ")^v^")")

(* Updates *)
let update_map_sing mapn inv outv v =
  let where_c =
    let cond = bind_map_fields mapn inv outv in
    if ebc cond then cond else inl("where "^(sbc cond))
  in cbc (inl("update "^mapn^" set "^mapn^"_val = "^(sbc v))) where_c

let aggregate agg_v v = assign agg_v (apply_op add_op (inl agg_v) v)

(* TODO: how do we use naming for functional versions elsewhere? *)
let full_aggregate_f fc =
  inl("select sum(v) as agg from ("^(sbc fc)^") as R")

let full_aggregate_v v rv =
  inl("select sum(v) into "^v^" from "^(sbc rv))

let gb_aggregate_f fc gb =
  inl("select "^gb^",sum(v) as v from ("^(sbc fc)^") as R group by "^gb)

let gb_aggregate_v in_v out_v gb =
  inl("create temporary table "^(sbc out_v)^
      " as select "^gb^",sum(v) as v"^
      "from "^(sbc in_v)^" group by "^gb)

(* For procedural ops, memoize and eval *) 
let op_sing_func op c1 c2 =
  if not(functional op) then
    let l,r,rv = gen_sym(),gen_sym(),gen_sym() in
    let lc,rc = assign l c1, assign r c2 in
    sequence [lc; rc; apply_procedural_op op rv (inl l) (inl r)]
  else apply_op op c1 c2

(* TODO: handle naming *)
let op_slice_func op c1 c2 =
  begin match op with
  | "*" | "+" ->
    inl("select R.*,S.*,R.v"^op^"S.v as v "^
           "from ("^(sbc c1)^") as R natjoin ("^(sbc c2)^") as S")
  
  | "<" | "<=" | "=" ->
    inl("select R.*,S.*,(case when R.v"^op^"S.v then 1 else 0 end) as v"^
           "from ("^(sbc c1)^") as R natjoin ("^(sbc c2)^") as S")
  
  | x when x = ifthenelse0_op ->
    inl("select R.*,S.*, (case when R.v <> 0 then S.v else 0 end) as v "^
       "from ("^(sbc c1)^") as R natjoin ("^(sbc c2)^") as S")

  | x when x = ifthenelse0_bigsum_op ->
    inl("select R.*,S.*,(case when S.v <> 0 then R.v else 0 end) as v "^
           "from ("^(sbc c1)^") as R natjoin ("^(sbc c2)^") as S")

  | _ -> failwith "invalid slice op"
  end

let op_sing_val op c1 c2 rv1 rv2 =
  let r = gen_sym() in
  let op_c =
    if functional op then assign r (apply_op op (inl rv1) (inl rv2))
    else apply_procedural_op op r (inl rv1) (inl rv2)
  in (sequence [c1; c2; op_c], r)

let op_slice_val op c1 c2 rv1 rv2 =
  let sym = gen_sym() in
  let opc = create_map sym (op_slice_func op (inl rv1) (inl rv2))
  in (sequence [c1; c2; opc], sym)


(* From here on use strings for names only. Any language operations should be
 * explicitly defined above
 *)

(* Wrapper helpers
 * TODO: use these generally throughout CG *)
let build_wrapper wrap_f = I(fun c -> apply_wrapper wrap_f c [])

let sequence_wrapper pre post c bindings =
  match (c, bindings) with
  | U(uc),[] -> U(sequence [pre; fst uc; snd uc; post], inl "")
  | U(_), b -> failwith "unable to bind from unit return type"
  | _,x -> failwith "invalid code for imperative sequence"

let iterate_wrapper (fields,bodyb) collection_c collection c bindings =
  let aux body = 
    let l = iterate_cl fields collection (sequence [bodyb; body])
    in sequence [collection_c; l] in
  match c with
  | U(uc) -> U(sequence [aux (fst uc); snd uc], inl "")  
  | _ -> failwith "invalid body for iteration"


(* Code-style transformation helpers *)

let use_vc_as_nc f vc rv = let x,y = f (vc, inl "") in sequence [x;y], rv

(* Binding and memoization *)

(* Inner bindings, bind/memoize a fun/val expression *)
let bind_aux var c bindings (nested_code, post_code) =
  (add_post_code (sequence [c; bind bindings (inl var); nested_code]) post_code)

let bind_fun var fc = bind_aux var (assign var fc)
let bind_val var vc rv = bind_aux var (sequence [vc; assign var (inl rv)])    

let bind_fun_val var fc bindings = use_vc_as_nc (bind_fun var fc bindings)

let bind_val_val var vc1 rv1 bindings =
  use_vc_as_nc (bind_val var vc1 rv1 bindings)

(* Outer bindings, bind values in code to a given set of bindings *)

(* Memoize singleton code to the given bindings
 * -- passes through unit code if no bindings are requested *)
let bind_sing_code c bindings =
  let aux v c = match bindings with
    | [] -> failwith "no bindings specified for value"
    | [x] -> (assign x (if v = "" then c else inl v), x)
    | _ -> if v = "" then let memo = gen_sym() in
             (sequence [assign memo c; bind bindings (inl memo)], memo)
           else (sequence [c; bind bindings (inl v)], v)
  in
  match c with
  | U(uc) ->
    if bindings = [] then U(sequence [fst uc;snd uc], inl "")
    else failwith "cannot add bindings for unit code" 
  | F(fc) -> let x,y = aux "" fc in V(x,y)
  | V(vc,rv) -> let x,y = aux rv vc in V(x,y)
  | I _ -> failwith "unable to bind to unwrapped code"
  | B (_,_) -> failwith "invalid code for binding, expects binding from wrapper"


(* Memoize slice code to the given bindings
 * -- passes through unit code if no bindings are requested *)
let bind_all_loop_fields bindings = (String.concat "," bindings, "")
    
let bind_value_loop_field fields bindings =
  match bindings with 
  | [] -> failwith "no bindings specified for map values"
  | [v] -> (iter_fields (fields@[v]), inl "")
  | h::t -> (h, bind t (inl h))

let bind_slice_code fields c bindings =
  match c with
  | F(fc) -> 
    let fbb = bind_value_loop_field fields bindings
    in build_wrapper (iterate_wrapper fbb (inl "") fc)
  | V(vc,rv) ->
    let fbb = bind_value_loop_field fields bindings
    in build_wrapper (iterate_wrapper fbb vc (inl rv)) 
  | U(uc) ->
    if bindings = [] then
      build_wrapper (sequence_wrapper (fst uc) (snd uc))
    else failwith "unable to bind a slice from unit code"
  | I _ -> failwith "unable to bind to unwrapped code"
  | B (_,_) -> failwith "invalid code for binding, expects binding from wrapper"


(* Binding code transformations *)

(* Transform singletons to a bound var in unit code *)
let bind_sing bind_u bind_v =
  let aux c bindings = match c with
    | U(uc) -> U(bind_u bindings uc)
    | V(vc,rv) -> let x,y = bind_v bindings vc rv in V(x,y)
    | _ -> failwith "invalid remainder for binding"
  in (fun restc -> apply_wrapper aux restc [])

let sing_func fc =
  let s = gen_sym() in bind_sing (bind_fun s fc) (bind_fun_val s fc)

let sing_val vc rv =
  let s = gen_sym() in bind_sing (bind_val s vc rv) (bind_val_val s vc rv)

(* Iterate over a fun/val expression *)
let loop_code fields prec fc bindings (nested_code, post_code) =
  let v =
    if fields = [] then failwith "invalid loop fields for iterator"
    else List.nth fields ((List.length fields)-1)
  in
  let lc = iterate_cl (iter_fields fields) fc
    (sequence [bind bindings (inl v); nested_code]) in 
  let sc = if ebc prec then [lc] else [prec; lc]
  in (add_post_code (sequence sc) post_code)

(* Transform a slice function to an iterator *)
let loop_slice loop_f =
  let aux c bindings = match c with
    | U(uc) -> U(loop_f bindings uc)
    | _ -> failwith "invalid unit value"
  in (fun restc -> apply_wrapper aux restc [])

let slice_func fields fc = loop_slice (loop_code fields (inl "") fc)
let slice_val fields vc rv = loop_slice (loop_code fields vc (get_slice_val rv))

(* Bind/memoize the function into a value *)
let sing_func_val f = let sym = gen_sym() in (assign sym f, sym)
let slice_func_val f = let sym = gen_sym() in (create_map sym f, sym)



(* DBToaster-specific code generators that distinguish singletons/slices *)

(* Operator helpers *)
(* TODO: check post code *)
(* Nesting wrapper semantics:
 * we create a single nesting wrapper from binary trees, based on linearizing
 * in a postfix order. Thus leftmost leaves are first in the nesting chain, and
 * the tree root is the last node in the chain.  
 *)
let wrap_op op lic ric =
  let aux_op c bindings =
    let r, ls, rs = gen_sym(), gen_sym(), gen_sym() in
    let (op_restc, op_postc) =
      let op_c = 
        if functional op then
            assign r (apply_op op (inl ls) (inl rs))
        else apply_procedural_op op r (inl ls) (inl rs)
      in
      match c with
      | U(uc) -> U(add_nested_code bindings r op_c (fst uc), inl ""), snd uc
      | V(vc,rv) -> V(add_nested_code bindings r op_c vc, rv), inl ""
      | _ -> failwith "invalid remainder for op"
    in
    let rc = lic (B(ls, (ric (B(rs, op_restc)))))
    in add_binary_post_code rc op_postc 
  in build_wrapper aux_op


(* Map access CG auxiliaries *)

(* TODO: fix map keys in insert for sing/slice map init *)  
let sing_init_code mapn inv out_patterns outv ivbc bindings (nc, pc) =
  let v = mapn^"_val" in
  let fields = get_map_fields mapn (List.length inv) (get_num_outv outv out_patterns) in
  let addc = add_map mapn fields v in
  let ic = try get_unit_code ivbc
           with Failure(s) -> failwith ("invalid map singleton ivbc: "^s)
  in add_post_code (add_nested_code bindings v (sequence [ic; addc]) nc) pc 

let sing_val_init_code mapn inv out_patterns outv ivbc bindings =
  use_vc_as_nc (sing_init_code mapn inv out_patterns outv ivbc bindings)

let slice_init_code mapn inv out_patterns ivbc bindings (nc, pc) =
  let v = mapn^"_val" in
  let fields = get_map_fields mapn (List.length inv) (get_num_outv [] out_patterns) in
  let addc = add_map mapn fields v in
  begin match ivbc with
    | U _ | V _ ->
      let c = sequence [get_unit_code ivbc; addc]
      in add_post_code (add_nested_code bindings v c nc) pc       
    | I(ic) ->
      let c = add_nested_code bindings v addc nc
      in add_post_code (get_unit_code (ic (U(c,inl "")))) pc
    | _ -> failwith "invalid slice ivbc"
  end

let slice_val_init_code mapn inv out_patterns ivbc bindings =
  use_vc_as_nc (slice_init_code mapn inv out_patterns ivbc bindings)

(* TODO: needs_init condition *)
let sing_lookup_code mapn inv outv bindings init_code (nc, pc) =
  let v = mapn^"_val" in
  let lookup_val = get_map_sing v mapn inv outv in
  let lookupc = cond (inl "needs_init") init_code
    (add_nested_code bindings v lookup_val nc)
  in (add_post_code lookupc pc)

let sing_val_lookup_code mapn inv outv bindings init_code =
  use_vc_as_nc (sing_lookup_code mapn inv outv bindings init_code)

let slice_lookup_code mapn inv outv pat patv bindings init_code (nc, pc) =
  let out_fields, lookup_slice = get_map_out_slice mapn inv outv pat patv in
  let v = mapn^"_val" in
  let lookupc = cond (inl "needs_init") init_code
    (iterate_cl (iter_fields (out_fields@[v])) lookup_slice
      (sequence [bind bindings (inl v); nc]))
  in (add_post_code lookupc pc)

let slice_val_lookup_code mapn inv outv pat patv bindings init_code =
  use_vc_as_nc (slice_lookup_code mapn inv outv pat patv bindings init_code)

(* Statement initializer, increment and update helpers
 * TODO: naming, perhaps make this independent of d<outv>, dv, init_v?
 *)

(* Statement IVC expects "d<lhs_outv>", "dv", "init_v" to be bound. *)
let stmt_sing_init_code ivbc bindings (nc, pc) =
  let v = gen_sym() in
  let c = assign v (apply_op add_op (inl "dv") (inl "init_v")) in
  let ic = try get_unit_code ivbc
           with Failure(s) -> failwith ("invalid stmt singleton ivbc: "^s)
  in add_post_code (add_nested_code bindings v (sequence [ic; c]) nc) pc

let stmt_sing_val_init_code ivbc bindings =
  use_vc_as_nc (stmt_sing_init_code ivbc bindings)

let stmt_slice_init_code lhs_outv ivbc bindings (nc,pc) =
  let v = gen_sym() in
  let slice_cond =
    bind_map_vars (List.map (fun x -> "d"^x) lhs_outv) lhs_outv in
  let ic = assign v (apply_op add_op (inl "dv") (inl "init_v")) in
  begin match ivbc with
    | U _ | V _ -> 
      let lc =
        add_nested_code bindings v (sequence [get_unit_code ivbc; ic]) nc in
      let c = cond slice_cond lc (inl "") in add_post_code c pc
    | I(ivc) ->
      let lc = (add_nested_code bindings v ic nc) in 
      let c = cond slice_cond lc (inl "")
      in add_post_code (get_unit_code (ivc (U(c, inl "")))) pc
    | _ -> failwith "invalid stmt slice ivbc"
  end

let stmt_slice_val_init_code lhs_outv ivbc bindings =
  use_vc_as_nc (stmt_slice_init_code lhs_outv ivbc bindings)


(* Statement increment helpers
 * TODO: naming, perhaps make this independent of d<outv>, dv,
 *       current_v, current_v_valid?
 *)

(* Singleton stmt increments expect "current_v", "current_v_valid" to be bound *)
let incr_code_aux v initb bindings nc =
  let sum_c = assign v (apply_op add_op (inl "current_v") (inl "dv")) in
  let initc = match initb with
    | U(uc) -> add_nested_code bindings v (get_unit_code initb) nc
    | _ -> failwith ("invalid singleton incr stmt init code: "^(string_of_code initb))
  in
  let upd_cond = inl "current_v_valid"
  in cond upd_cond (add_nested_code bindings v sum_c nc) initc

let sing_incr_code v initb incrb bindings (nc, pc) =
  let upd_c = incr_code_aux v initb bindings nc in
  let incrc = match incrb with
    | U(uc) -> get_unit_code incrb
    | V(vc,rv) ->
        sequence [vc; if rv <> "dv" then assign "dv" (inl v) else inl ""] 
    | _ -> failwith "invalid singleton incr stmt incr code"
  in add_post_code (sequence [incrc; upd_c]) pc

let sing_val_incr_code v initb incrb bindings =
  use_vc_as_nc (sing_incr_code v initb incrb bindings)

(* Slice stmt increments perform a merge that includes map lookups to bind
 * "current_v", "current_v_valid". This is currently inefficient, since the
 * lookup is performed on the entire map, rather than an out tier.
 * Instead, we could implement a two-tiered relational map, or create a
 * temporary table for the out tier.
 * The merge iterates over the delta slice since it is usually smaller than the
 * existing map.
 *)
let slice_incr_code mapn inv outv v initb incrb bindings (nc, pc) =
  let entry_c = incr_code_aux v initb bindings nc in
  let rvf = iter_fields ((List.map (fun x -> "d"^x) outv)@["dv"]) in
  let current_c = get_map_sing_val mapn inv outv "current_v" in
  let sc = sequence [current_c; entry_c] in
  begin match incrb with
    | U(_) -> failwith "cannot handle unit code for delta slice" 
    | V(vc,rv) -> 
        let merge_c = iterate_cl rvf (get_slice_val rv) sc in
        let c = sequence [get_unit_code incrb; merge_c]
        in add_post_code c pc
    | I(ic) -> add_post_code (get_unit_code (ic (U(sc, inl "")))) pc
    | _ -> failwith "invalid slice incr stmt incr code"
  end

let slice_val_incr_code mapn inv outv v initb incrb bindings =
  use_vc_as_nc (slice_incr_code mapn inv outv v initb incrb bindings)

(* Statement update helpers *)

(* TODO: enforce common use of "current_v", "updated_v" with above
 * statement update code *)
let sing_update_code mapn inv outv pat patv direct update_code =
  let v = "current_v" in
  let lc = get_map_sing_val mapn inv outv v in
  let uc = sequence [lc; update_code] in
  if direct then (uc, inl "") else
  let ivf, ivlq = get_map_in_slice mapn inv outv pat patv in
  if ebc ivlq then uc, inl ""
  else (iterate_cl (iter_fields ivf) ivlq uc, inl "")

(* If pat/patv is the empty list, and inv is not,
 * we loop over the entire in tier *)
let slice_update_code mapn inv outv pat patv direct update_code =
  if direct then update_code, inl "" else
  let ivf, ivlq = get_map_in_slice mapn inv outv pat patv in
  if ebc ivlq then update_code, inl ""
  else (iterate_cl (iter_fields ivf) ivlq update_code, inl "")




(* Code generator methods *)

(* Operators
 * -- nesting wrapper semantics: operators combine branches, thus
 *    they yield a single nesting wrapper
 * TODO: prebind, inbind
 *)
let op_expr lf_i rf_i lv_i rv_i lf_v rf_v compose_f compose_v
            prebind inbind op outv1 outv2 schema theta_ext schema_ext ce1 ce2
=
  let aux_val c1 c2 v1 v2 =
    let x,y = compose_v op c1 c2 v1 v2 in V(x,y) 
  in
  match (ce1,ce2) with
  | I(lic),   I(ric)   -> wrap_op op lic ric
  | I(lic),   F(fc)    -> wrap_op op lic (rf_i fc)
  | F(fc),    I(ric)   -> wrap_op op (lf_i fc) ric
  | I(lic),   V(vc,rv) -> wrap_op op lic (rv_i vc rv)
  | V(vc,rv), I(ric)   -> wrap_op op (lv_i vc rv) ric

  | F(fc),    V(vc,rv) -> 
    if functional op then
    let vcf,rvf = lf_v fc in aux_val vcf vc rvf rv
    else wrap_op op (lf_i fc) (rv_i vc rv) 

  | V(vc,rv), F(fc)    ->
    if functional op then
    let vcf,rvf = rf_v fc in aux_val vc vcf rv rvf
    else wrap_op op (lv_i vc rv) (lf_i fc)

  | V(c1,r1), V(c2,r2) ->
    if functional op then aux_val c1 c2 r1 r2
    else wrap_op op (lv_i c1 r1) (rv_i c2 r2)

  | F(c1),    F(c2)    ->
    if functional op then F(compose_f op c1 c2)
    else wrap_op op (lf_i c1) (lf_i c2)

  | U(_), _  | _, U(_) -> failwith "invalid unit operands"
  | B(_,_),_ | _,B(_,_) -> failwith "Op binding NYI" 

let op_singleton_expr prebind op ce1 ce2 = op_expr
    sing_func sing_func sing_val sing_val
    sing_func_val sing_func_val op_sing_func op_sing_val
    prebind [] op [] [] [] [] [] ce1 ce2 

let op_slice_expr prebind inbind op outv1 outv2
                  schema theta_ext schema_ext ce1 ce2 =
  op_expr
    (slice_func (outv1@["v1"])) (slice_func (outv2@["v2"]))
    (slice_val (outv1@["v1"])) (slice_val (outv2@["v2"]))
    slice_func_val slice_func_val op_slice_func op_slice_val
    prebind inbind op outv1 outv2 schema theta_ext schema_ext ce1 ce2

let op_slice_product_expr prebind op outv1 outv2 ce1 ce2 =
  op_expr
    (slice_func (outv1@["v1"])) (slice_func (outv2@["v2"]))
    (slice_val (outv1@["v1"])) (slice_val (outv2@["v2"]))
    slice_func_val slice_func_val op_slice_func op_slice_val
    prebind [] op  [] [] [] [] [] ce1 ce2

let op_lslice_expr prebind inbind op outv1 outv2
                   schema theta_ext schema_ext ce1 ce2 =
  op_expr
    (slice_func (outv1@["v1"])) sing_func (slice_val (outv1@["v1"])) sing_val
    slice_func_val sing_func_val op_slice_func op_slice_val
    prebind inbind op outv1 outv2 schema theta_ext schema_ext ce1 ce2

let op_lslice_product_expr prebind op outv1 outv2 ce1 ce2 =
  op_expr
    (slice_func (outv1@["v1"])) sing_func (slice_val (outv1@["v1"])) sing_val
    slice_func_val sing_func_val op_slice_func op_slice_val
    prebind [] op [] outv2 [] [] [] ce1 ce2

let op_rslice_expr prebind op outv2 schema schema_ext ce1 ce2 =
  op_expr
    sing_func (slice_func (outv2@["v2"])) sing_val (slice_val (outv2@["v2"]))
    sing_func_val slice_func_val op_slice_func op_slice_val
    prebind [] op [] outv2 schema [] schema_ext ce1 ce2


(* Map initializers
 * -- return wrappers since the init expr will be used as part of a lookup,
 *    which may be wrapped itself, and should apply the same remainder to
 *    this init expr
 * -- ivc expression is bound to "<map name>_val"
 * -- note ivc expression may be a wrapper since ivc may use other maps
 * -- if ivc code is a function or a value, we use outer binding for CG 
 * -- if ivc is a wrapper, we use inner binding for CG
 *)
let init_expr bind_f init_u init_v mapn cinit =
  let v = mapn^"_val" in
  let aux ivbc c bindings = match c with
    | U(uc) -> U(init_u ivbc bindings uc)
    | V(vc,rv) -> let x,y = init_v ivbc bindings vc rv in V(x,y)
    | _ -> failwith "invalid map initialization code"
  in 
  let ivbc =
    match cinit with
    | I(ic) -> ic (B(v, U(inl (single_comment "end of stmt init"), inl "")))
    | _ -> bind_f cinit [v]
  in build_wrapper (aux ivbc)

let singleton_init_lookup mapn inv out_patterns outv cinit =
  init_expr bind_sing_code
  (sing_init_code mapn inv out_patterns outv)
  (sing_val_init_code mapn inv out_patterns outv)
  mapn cinit

let slice_init_lookup mapn inv out_patterns cinit =
  let fields =
    get_map_fields_l mapn (List.length inv) (get_num_outv [] out_patterns)
  in init_expr (bind_slice_code fields) (slice_init_code mapn inv out_patterns)
    (slice_val_init_code mapn inv out_patterns) mapn cinit 


(* Map lookups *)
let lookup_and_init_expr init_f init_v lookup_u lookup_v
                         mapn inv outv pat patv cinit =
  let aux_lookup ic c bindings =
    let init_restc = rebind bindings (strip_post_code c) in
    match c with
    | U(uc) ->
        let initc = get_unit_code (ic init_restc)
        in U(lookup_u bindings initc uc)
    | V(vc,rv) ->
        let initc = get_unit_code (ic init_restc) in
        let x,y = lookup_v bindings initc vc rv in V(x,y)
    | _ -> failwith "invalid unit value"
  in 
  match cinit with
  | I(ic) -> build_wrapper (aux_lookup ic)
  | F(iv) -> build_wrapper (aux_lookup (init_f iv))
  | V(ivc,irv) -> build_wrapper (aux_lookup (init_v ivc irv))
  | B(_) -> failwith "invalid binding"
  | U(_) -> failwith "invalid unit initializer"

let singleton_lookup_and_init mapn inv outv cinit =
  let aux f = f mapn inv outv in 
  lookup_and_init_expr sing_func sing_val
    (aux sing_lookup_code) (aux sing_val_lookup_code)
    mapn inv outv [] [] cinit

let singleton_lookup mapn inv outv cinit =
  let aux f = f mapn inv outv in
  let ofs = outv@[mapn^"_val"] in
  lookup_and_init_expr (slice_func ofs) (slice_val ofs)
  	(aux sing_lookup_code) (aux sing_val_lookup_code)
    mapn inv outv [] [] cinit

let slice_lookup_sing_init mapn inv outv pat patv cinit =
  let aux f = f mapn inv outv pat patv in
  lookup_and_init_expr sing_func sing_val
    (aux slice_lookup_code) (aux slice_val_lookup_code)
    mapn inv outv pat patv cinit

let slice_lookup mapn inv outv pat patv cinit =
  let aux f = f mapn inv outv pat patv in
  let ofs = outv@[mapn^"_val"] in
  lookup_and_init_expr (slice_func ofs) (slice_val ofs)
    (aux slice_lookup_code) (aux slice_val_lookup_code)
    mapn inv outv pat patv cinit


(* RHS expressions, bigsums, etc *)
    
(* TODO: create f/v for unit code, just as for below *)
let singleton_expr ccalc cdebug = ccalc
let direct_slice_expr ccalc cdebug = ccalc

let full_agg_slice_expr ccalc cdebug =
  match ccalc with
  | I(ic) ->
    let agg_v,v = gen_agg_sym(), gen_sym() in 
    let aggc = dbc stmt_delimiter (aggregate agg_v (inl v)) in
    V(get_unit_code (ic (B(v,(U(aggc, inl ""))))), agg_v)

  | F(fc) -> F(full_aggregate_f fc)
  | V(vc,rv) ->
    let agg_v = gen_agg_sym() in
    V(sequence [vc; full_aggregate_v agg_v (inl rv)], agg_v)

  (* Can't handle unit since we don't know what to aggregate *)
  | _ -> failwith "invalid full agg slice rhs"

let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
  let gb = String.concat "," lhs_outv in
  match ccalc with
  | I(ic) ->
    let v, agg_v = gen_sym(), gen_agg_sym() in
    let slice_v = gen_slice_sym() in
    let addc = dbc stmt_delimiter (add_map slice_v gb v) in
    let gbc = gb_aggregate_v (inl slice_v) (inl agg_v) gb
    in V(get_unit_code (ic (B(v,(U(addc, gbc))))), agg_v)

  | F(fc) -> F(gb_aggregate_f fc gb)
  | V(vc,rv) ->
    let agg_v = gen_agg_sym()
    in V(sequence [vc; gb_aggregate_v (inl rv) (inl agg_v) gb], agg_v)
  
  (* Can't handle unit since we don't know what to aggregate *)
  | _ -> failwith "invalid slice rhs"


(*
 * Statements
 *
 * A statement's RHS expression is computed imperatively, that is inside
 * a loop for slice RHS or sequentially for singletons.
 *
 * In addition to dealing with deltas, we may have to compute initial values
 * for a LHS map. We do this if necessary inside any delta evaluation, that is
 * map initialization is nested inside delta evaluation.
 *
 * We have a choice of doing the database update per statement in
 * nested or post computation fashion, that is doing map inserts or updates
 * inside every branch we generate, or by assigning to identical variables
 * across branches, and doing the map insert or update using these common
 * variables afterwards.
 *
 * We do the database update post updated value and initial value computation,
 * but nested inside the delta evaluation.  
 *)

(* IVC for statements:
 * -- assumes any code returned will be used in a scope where the
 *    delta value for the statement is bound.
 * -- yields code that computes the new initial value added to the stmt delta
 * -- ivc expression is bound to "init_v"
 * -- note ivc expression may be a wrapper since ivc may use other maps
 * -- if ivc code is a function or a value, we use outer binding for CG 
 * -- if ivc is a wrapper, we use inner binding for CG
 *)
let stmt_init_expr bind_f stmt_u stmt_v cinit cdebug =
  let aux ivbc c bindings = match c with
    | U(uc) -> U(stmt_u ivbc bindings uc)
    | V(vc,rv) -> let x,y = stmt_v ivbc bindings vc rv in V(x,y)
    | _ -> failwith "invalid map initialization code"
  in
  let ivbc = match cinit with
    | I(ic) ->
        ic (B("init_v", U(inl (single_comment "end of stmt init"), inl "")))
    | _ -> bind_f cinit ["init_v"]
  in build_wrapper (aux ivbc)

let singleton_init = stmt_init_expr bind_sing_code
  stmt_sing_init_code stmt_sing_val_init_code 

(* bind_slice_code binds the keys of the ivc slice here *)
let slice_init lhs_inv lhs_outv init_ext =
  stmt_init_expr (bind_slice_code lhs_outv)
    (stmt_slice_init_code lhs_outv) (stmt_slice_val_init_code lhs_outv)


(* Statement increments
 * -- ensures both branches populate "newv" variable with the incremented value
 * -- computes delta sing/slice first
 * -- branches on ivc if needed
 * -- performs merging for slices
 *)
let stmt_incr_expr bindt_f bindr_f stmt_u stmt_v
                   lhs_inv lhs_outv cincr cinit cdebug =
  let v = "newv" in
  let aux initb incrb c bindings =
    match c with
    | U(uc) -> U(stmt_u v initb incrb bindings uc)
    | V(vc,rv) -> let x,y = stmt_v v initb incrb bindings vc rv in V(x,y) 
    | _ -> failwith "invalid unit code"
  in
  let initb = match cinit with
    | I(initc) ->
        initc (B(v, U(inl (single_comment "end of incr stmt init"), inl "")))
    | _ -> bindt_f cinit [v]
  in
  let incrb = match cincr with
    | I(incrc) ->
        incrc (B("dv", U(inl (single_comment "end of incr stmt"), inl "")))
    | _ -> bindr_f cincr ["dv"]
  in build_wrapper (aux initb incrb)

let singleton_update lhs_outv cincr cinit cdebug = stmt_incr_expr
  bind_sing_code bind_sing_code sing_incr_code sing_val_incr_code
  [] lhs_outv cincr cinit cdebug

(* bind_slice_code binds the keys of the delta slice here *)
let slice_update lhs_mapn lhs_inv lhs_outv cincr cinit cdebug = stmt_incr_expr
  bind_sing_code (bind_slice_code (List.map (fun v -> "d"^v) lhs_outv))
  (slice_incr_code lhs_mapn lhs_inv lhs_outv)
  (slice_val_incr_code lhs_mapn lhs_inv lhs_outv)
  lhs_inv lhs_outv cincr cinit cdebug


(* Statement updates
 * -- persists map increments, yielding a side-effecting statement
 * -- map update assumes d<outv> is bound in the update code (and the result
 *    it yields), so that we only persist delta entries
 * -- map update assumes <inv> is bound in the update code, which is safe
 *    since we loop over all in vars 
 *)
let stmt_update_expr update_f update_v stmt_f
                     lhs_mapn lhs_inv lhs_outv cupdate =
  let v = "updated_v" in
  let outv = List.map (fun v -> "d"^v) lhs_outv in
  let wq = dbc stmt_delimiter (update_map_sing lhs_mapn lhs_inv outv (inl v)) in 
  let aux_update uc = get_unit_code (uc (B(v, (U(wq, inl ""))))) in
  let updc =
    match cupdate with
    | I(ic) -> ic
    | F(fc) -> update_f fc
    | V(vc,rv) -> update_v vc rv
    | _ -> failwith "invalid statement"
  in U(stmt_f (aux_update (updc)))

let singleton_statement lhs_mapn lhs_inv lhs_outv map_out_patterns
                        lhs_ext patv pat direct cupdate =
  begin match lhs_inv, lhs_outv with
  | _,_ -> stmt_update_expr sing_func sing_val
             (sing_update_code lhs_mapn lhs_inv lhs_outv pat patv direct)
             lhs_mapn lhs_inv lhs_outv cupdate 
  end
  

let statement lhs_mapn lhs_inv lhs_outv lhs_ext patv pat direct cupdate =
  let ofs = lhs_outv@["v"] in
  let aux f = f lhs_mapn lhs_inv lhs_outv cupdate in
  begin match lhs_inv, lhs_outv with
  | _,_ -> aux (stmt_update_expr
             (slice_func ofs) (slice_val ofs)
             (slice_update_code lhs_mapn lhs_inv lhs_outv pat patv direct))
  end
    

(* Triggers *)
let pm_const pm = match pm with | Insert -> "Insert" | Delete -> "Delete" 
let pm_name pm  = match pm with | Insert -> "ins" | Delete -> "del" 

let trigger event rel trig_args stmt_block =
  let trig_name = "on_"^(pm_name event)^"_"^rel in
  let tc =
    (Lines ["create trigger "^trig_name^"{"])::
    (List.map (fun x -> match x with
      | U(uc) -> ibc "    " (get_unit_code x)  
      | _ -> failwith "invalid statement code") stmt_block)@
    [(Lines ["}"])]
  in U(List.fold_left cbc (List.hd tc) (List.tl tc), inl "")

let source src fr rel_adaptors = ("<sources>", None, None)

let main schema patterns sources triggers toplevel_queries =
  let maps = Lines (List.map (fun (id,ins,outs) ->
      let f (i,t) = id^"_a"^(string_of_int i)^" int" in
      let add_counter l = snd(
        List.fold_left (fun (i,acc) x -> (i+1,acc@[(i,x)])) (0,[]) l) in
      let ivl = String.concat "," (List.map f (add_counter ins)) in
      let ovl = String.concat "," (List.map f (add_counter outs)) in
      "create table "^id^" ("^ivl^","^ovl^");")
    schema)
  in
  let script = cbcl (maps::(List.map (fun x -> match x with
    | U(uc) -> fst uc
    | _ -> failwith "invalid trigger") triggers))
  in U(script, inl "")

let output code out_chan =
  output_string out_chan (sbc (get_unit_code code));
  flush out_chan

let eval_trigger trigger tuple db =
  failwith "Cannot directly evaluate OCaml source"

let event_evaluator triggers db evt =
  failwith "Cannot directly evaluate OCaml source"

(* Unit tests *)
(*
let print_code c = match c with
  | U(c,pc) -> print_endline (sbc (fst (add_post_code c pc)))
  | F(fc) -> print_endline (sbc fc)
  | V(vc,v) -> print_endline (v^" ::= "); print_endline (sbc vc)
  | I(_) -> failwith "cannot print, unfinished code generation."
  | _ -> failwith "invalid unit code"
;;


Debug.log_unit_test "stringify sequence" (fun x -> x)
  (sbc (sequence [inl "x"; inl ""; inl "y"])) "x;\ny";;

Debug.log_unit_test "stringify 2-nested seq" (fun x -> x)
  (sbc (sequence [sequence [inl "a"; inl "b"]; inl "c"]))
  "a;\nb;\nc";;

Debug.log_unit_test "stringify 3-nested seq" (fun x -> x)
  (sbc (sequence
    [sequence [sequence [inl "a"; inl "b"]; inl "c"; inl "d"]; inl "e"]))
  "a;\nb;\nc;\nd;\ne";;

Debug.log_unit_test "stringify add_nested_code" (fun x -> x)
  (sbc (add_nested_code [] "v" (inl "x") (inl "y")))
  (sbc (sequence [inl "x"; inl ""; inl "y"]));;

Debug.log_unit_test "stringify add_nested_code w/ binding" (fun x -> x)
  (sbc (add_nested_code ["x"] "v" (sequence [inl "y"]) (inl "z")))
  (sbc (sequence [inl "y"; assign "x" (inl "v"); inl "z"]));;  

let tab = String.make indent_width ' ' in
let x = cbc (inl "foreach x\n    in (coll) {")
  (cbc (sequence [(ibc tab (inl "s1")); (ibc tab (inl "s2"))]) (inl "}")) in
Debug.log_unit_test "stringify iterate_cl" (fun x -> x)
   (sbc (iterate_cl "x" (inl "coll") (sequence [inl "s1"; inl "s2"]))) (sbc x)
;;
*)

(*
let q = singleton_init_lookup "m1" ["x"; "y"] [] ["a"; "b"] (const (CFloat 0.0)) in 
let c = match q with
  | I(qf) -> qf (U(inl "<update/insert code>;", inl "<agg, merge code>;"))
  | _ -> failwith "invalid query"
in print_code c
*)

(*
let q = slice_lookup "m1" ["x"; "y"] ["a";"b";"c"] [1] ["b"]
  (singleton_init_lookup "m1" ["x";"y"] [] ["a";"b";"c"] (const (CFloat 0.0)))
in 
  match q with
  | I(qf) ->
    print_code (qf (U(inl "<update/insert code>;", inl "<agg, merge code>;")))
  | _ -> failwith "invalid query"
*)

(* Product, singleton initializers *)
(*
let q = op_slice_expr "*"
          (slice_lookup "m1" (singleton_init_lookup "m1" (const (CFloat 0.0))))
          (slice_lookup "m2" (singleton_init_lookup "m2" (const (CFloat 0.0))))
in
  match q with
  | I(qf) ->
    print_code (qf (U(inl "<update/insert code>;", inl "<agg, merge code>;")))
  | _ -> failwith "invalid query"
*)

(* Product, bigsum initializers *)
(*
let q = 
  let init_m1 = full_agg_slice_expr (slice_lookup "b1" (const (CFloat(0.0)))) in
  let init_m2 = full_agg_slice_expr (slice_lookup "b2" (const (CFloat(0.0)))) in
    op_slice_expr "*"
  	  (slice_lookup "m1" (slice_init_lookup "m1" init_m1))
      (slice_lookup "m2" (slice_init_lookup "m2" init_m2))
in print_code (slice_expr q) 
*)

(* Statement, single map update *)
(*
let q = (slice_lookup_sing_init "m1"
            (singleton_init_lookup "m1" (const (CFloat(0.0)))))
in print_code
    (statement "m2" "partials"
        (slice_update
            (singleton_init (const (CFloat(0.0))))
            (slice_expr q)))
*)

(* Statement, update with product of two maps *)
(*
let q =
  let init_m1 = full_agg_slice_expr (slice_lookup "b1" (const (CFloat(0.0)))) in
  let init_m2 = full_agg_slice_expr (slice_lookup "b2" (const (CFloat(0.0)))) in
    op_slice_expr "*"
      (slice_lookup "m1" (slice_init_lookup "m1" init_m1))
      (slice_lookup "m2" (slice_init_lookup "m2" init_m2))
in print_code
    (statement "m3" "partials"
        (slice_update
            (singleton_init (const (CFloat(0.0))))
            (slice_expr q)))
*)
end
;;