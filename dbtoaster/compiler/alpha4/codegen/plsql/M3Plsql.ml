(*
 * PL/PGSQL code generator.
 * -- attempted to be a language independence layer, for functional and
 *    imperative languages, but insufficient to express global backtracking in
 *    cg to one mode or the other, thus unable to generate arbitrary
 *    combinations of fp/imp code.
 * -- while carrying around alternatives are one way of implementing
 *    backtracking, the code generation I want for plpgsql still requires
 *    knowledge of conditionals, and potentially loops, leading to a cg 
 *    representation similar to that of k3, thus I'm deprecating this in 
 *    favour of pursuing k3, while applying lessons learned for general
 *    merges and statement increments from here within.
 *    
 *
 * Code can be one of:
 * 1. unnamed values: purely functional values (similar to r-values)
 * 2. values: a block that binds values and a purely functional return value
 * 3. a code wrapper
 * 4. nested/post code block
 * 5. future binding request
 *
 * <More documentation...> 
 *)

(*
 * Current status: code and method types are set up to handle alternatives,
 * which would facilitate global backtracking, but handlers are yet to be written.
 * 
 * TODO: functional map op naming
 * TODO: prebind, inbind
 * -- needs more general model for B, i.e. decl, val rather than just decl
 *
 * TODO: unify map and statement initialization return types
 * (currently nc/pc pair and code respectively)
 *
 * TODO: parallel/alternatives cg, with duplicate elimination 
 *
 * TODO: modularize stringification
 *
 * TODO: typing
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
 * TODO: support functional conditionals, enabling functional map lookups
 *)

open Util
open SourceCode

open M3
open M3Common.Patterns

(* Use sources to get standard adaptor parameters *)
open Sources

module M3P = M3.Prepared

module CG : M3Codegen.CG = 
struct

type op_t = string

type bindings = string list

type code_t =
    W of (code_t list -> code_t list) (* nesting wrapper code *)
  | F of source_code_t
  | U of (source_code_t * source_code_t) (* nested code, post code *)
  | V of source_code_t * string (* code, return value *)
  | B of string * code_t list (* var to be bound before code *)

type source_impl_t = source_code_t

type debug_code_t = code_t
type db_t = string
    
(* Source code stringification aliases *)
let inl x = Inline x
let inline_var_list sl = List.map (fun x -> inl x) sl
 
let sbc = string_of_source_code
let cbc = concat_source_code
let ebc = empty_source_code
let dbc = delim_source_code
let cdbc = concat_and_delim_source_code
let cbcl = concat_and_delim_source_code_list
let ibc = indent_source_code
let ivl = inline_var_list

let rec string_of_code c = match c with
    | W(ic) -> "W(...)"
    | F(fc) -> "F("^(sbc fc)^")"
    | U(nc,pc) -> "U{"^(sbc nc)^"}{"^(sbc pc)^"}"
    | V(c,r) -> "V("^(sbc c)^","^r^")"
    | B(v,c) -> "B("^v^",["^(String.concat "/" (List.map string_of_code c))^"])"


(* Basic code generation helpers *)
let counters = Hashtbl.create 0

let var_c = "vars"
let var_prefix = "var"

let agg_c = "aggs"
let agg_prefix = "agg"

let slice_c = "slices"
let slice_prefix = "slice"

let init_counters =
  Hashtbl.add counters var_c 0;
  Hashtbl.add counters agg_c 0;
  Hashtbl.add counters slice_c 0

let incr_counter n =
  let v = try Hashtbl.find counters n with Not_found -> 0
  in Hashtbl.replace counters n (v+1)

let get_counter n = try Hashtbl.find counters n with Not_found -> 0

let gen_sym () =
  let r = get_counter var_c in incr_counter var_c;
  var_prefix^(string_of_int r)

let gen_agg_sym() =
  let r = get_counter agg_c in incr_counter agg_c;
  agg_prefix^(string_of_int r)

let gen_slice_sym() =
  let r = get_counter slice_c in incr_counter slice_c;
  slice_prefix^(string_of_int r)

let reset_syms() = Hashtbl.clear counters

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
let sql_string_of_float f =
  let r = string_of_float f in
  if r.[(String.length r)-1] = '.' then r^"0" else r

let const_code c = match c with
  | CFloat(f) -> V(inl "", sql_string_of_float f)

let const = const_code

let singleton_var v = V(inl "", v)
let slice_var v = V(inl "", v)

let add_op  = "+"
let mult_op = "*"
(* TODO: make comparison operators integers *)
let eq_op   = "="
let lt_op   = "<"
let leq_op  = "<="

(* These are just symbols for operator comparison, and not to be
 * used in stringification *)  
let ifthenelse0_op = "ifte"
let ifthenelse0_bigsum_op = "ifte_bigsum"

(* returns whether the given op can be evaluated functionally *)
let functional op =
  not(op = eq_op || op = lt_op || op = leq_op ||
      op = ifthenelse0_op || op = ifthenelse0_bigsum_op)

(* Additional operators for stringification *)
let and_op = "and"
let or_op  = "or"
let neg_op = "not"
let neq_op = "<>"

let ws_start_re = Str.regexp ("^"^(Str.quote " ")^"*")

let rec indent ?(n=1) s = match n with | 0 -> s
  | _ -> indent ~n:(n-1) (ibc (String.make indent_width ' ') s)

let single_comment c = "-- "^c
let multi_comment c = "/*\n"^c^"*/"

let delimit c = dbc stmt_delimiter c

let sequence ?(final=false) ?(delim=stmt_delimiter) cl =
  cbcl ~final:final ~delim:delim (List.filter (fun x -> not(ebc x)) cl)

let assign lv rv = inl(lv^" := "^(sbc rv))

let ucond p t : source_code_t = cbcl
  ([Lines ["if "^(sbc p)^" then"]; indent t; Lines["end if"]])

let scond p t e : source_code_t =
  inl ("if "^(sbc p)^" then "^(sbc t)^
      (if ebc e then " end if" else " else "^(sbc e)^" end if"))

let cond p t e : source_code_t = cbcl
  ([Lines ["if "^(sbc p)^" then"]; indent t;]@
  (if ebc e then [Lines ["end if"]]
   else [Lines ["else "]; indent e; Lines["end if"]]))

let iter_fields l = String.concat "," l

let iterate_cl fields collection body = cbcl
  [Lines ["for "^fields^" in "^(sbc collection)^" loop"];
   indent body; Lines ["end loop"]] 
    
let bind l v = (sequence (List.map (fun x -> assign x v) l))

let apply_uop op c = inl(op^" "^(sbc c))

let apply_op op l r = inl((sbc l)^" "^op^" "^(sbc r))

let apply_procedural_op op rv l r =
  if functional op then
    failwith ("op ("^op^") should be evaluated functionally")
  else if op = eq_op || op = lt_op || op = leq_op then
    scond (apply_op op l r)
      (delimit (assign rv (inl "1"))) (delimit (assign rv (inl "0")))
  else if op = ifthenelse0_op then
    scond (apply_op neq_op l (inl "0"))
      (delimit (assign rv r)) (delimit (assign rv (inl "0")))
  else scond (apply_op neq_op r (inl "0"))
         (delimit (assign rv l)) (delimit (assign rv (inl "0")))

(* Code access helpers *)
let get_procedural_code c = match c with
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

let substitute sub by c =
  let sub_re = List.flatten (List.map (fun x ->
    [Str.regexp_string (x^" "), by^" ";
     Str.regexp_string (x^";"), by^";"]) sub)
  in
  let aux s =
    List.fold_left (fun acc (x,y) -> Str.global_replace x y acc) s sub_re
  in begin match c with
  | Lines(sl) -> Lines(List.map aux sl)
  | Inline(s) -> Inline(aux s)
  end

let add_nested_code bindings v c nc =
  if ebc nc then c
  else (*sequence [c; bind bindings (inl v); nc]*)
  sequence [c; substitute bindings v nc]

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

let get_map_key = String.concat ","

let get_map_fields_l mapn num_in num_out =
  let num_fields = num_in + num_out in
  add_map_dim mapn num_in num_out;
  (make_fields mapn num_fields)

let get_map_fields mapn num_in num_out =
  get_map_key (get_map_fields_l mapn num_in num_out)
  
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
  in renames,rv,cond

(* Map primitives *)
let declare_map n f = inl("create temporary table "^n^" ("^
  (String.concat "," (List.map (fun x -> x^" real") (f@["v"])))^")")
  
let create_map n f = inl("create temporary table "^n^" as "^(sbc f))
let copy_map o n =
  inl("create temporary table "^n^" as select * from "^(sbc o))

(* Getters *)
(* This query must return a single map entry, otherwise a runtime exception
 * will be thrown. The code must assign the entry's value to <rv> and
 * set <rv>_valid if successful. *)
(* TODO: make FOUND independent *)
let get_map_sing_val mapn inv outv rv =
  let pred = bind_map_fields mapn inv outv in
  let where_c = if ebc pred then pred else inl (" where "^(sbc pred)) in
  sequence [ inl("select "^mapn^"_val into strict "^rv^
                 " from "^mapn^(sbc where_c));
             assign (rv^"_valid") (inl "FOUND")]

let get_map_sing mapn inv outv v =
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
  let rename,rfields,cond = bind_map_pattern ins mapn inv outv pat patv in
  let fields = rename@(if ins then [] else [mapn^"_val"]) in
  let where_c = if ebc cond then cond else inl(" where "^(sbc cond)) in
  let rc =
    if fields = [] then inl "" else
    let select_list = String.concat "," fields in
    cbc (inl("select "^(if ins then "distinct " else "")^
             select_list^" from "^mapn)) where_c
  in rfields, rc
    

let get_map_out_slice = get_map_slice_aux false 
let get_map_in_slice = get_map_slice_aux true 

(* Accesses a memoized slice value *)
let get_slice_val x = inl("select * from ("^(sbc x)^") as R")

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

(* TODO: for agg, gb_agg, how do we use naming for functional
 * versions elsewhere? *)

(* Aggregation, for when all LHS map keys are bound, and the RHS
 * expression is a slice *)
let full_aggregate_f fc =
  inl("select sum(v) as agg from ("^(sbc fc)^") as R")

let full_aggregate_v v rv =
  inl("select sum(v) into "^v^" from "^(sbc rv))

(* Update statements project RHS expression keys to LHS map
 * keys during evaluation
 * -- this is essentially a gb aggregate. *)
let gb_aggregate_f fc gb =
  inl("select "^gb^(if gb = "" then "" else ",")^"sum(v) as v "^
      " from ("^(sbc fc)^") as R"^(if gb = "" then "" else " group by ")^gb)

let gb_aggregate_v in_v gb =
  inl("select "^gb^(if gb = "" then "" else ",")^"sum(v) as v"^
      " from "^(sbc in_v)^(if gb = "" then "" else " group by ")^gb)

(* Functional and value binary operation evaluation for singletons and slices
 * -- these should never be invoked on procedural ops, all handling of
 *    such should be done outside. *)

(* Functional singleton ops *) 
let op_sing_func op c1 c2 =
  if not(functional op) then failwith "cannot eval procedural ops functionally"
  else apply_op op c1 c2

(* TODO: handle naming *)
(* Functional slice ops *)
let op_slice_func op c1 c2 =
  if not(functional op) then
    failwith "cannot eval procedural ops functionally"
  else
  begin match op with
  | "*" | "+" ->
    inl("select R.*,S.*,R.v"^op^"S.v as v "^
           "from ("^(sbc c1)^") as R natjoin ("^(sbc c2)^") as S")
  
  | "<" | "<=" | "=" ->
    inl("select R.*,S.*,(case when R.v"^op^"S.v then 1 else 0 end) as v "^
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
  if not(functional op) then
    failwith "cannot eval procedural ops functionally"
  else sequence [c1; c2], sbc (apply_op op (inl rv1) (inl rv2))

let op_slice_val op c1 c2 rv1 rv2 =
  if not(functional op) then
    failwith "cannot eval procedural ops functionally"
  else sequence [c1; c2], sbc (op_slice_func op (inl rv1) (inl rv2))


(* From here on use strings for names only.
 * Any language operations should be explicitly defined above. *)

(* Wrapper helpers
 * TODO: use these generally throughout CG *)

(* Code wrapping, invokes a wrapper around the given code, supplying bindings *)
let rec apply_wrapper (wrap_f : code_t -> bindings -> code_t list) cl bindings =
  List.fold_left
  (fun alt c ->
    alt@(begin match c with
      | W(rcf) -> [W(fun xcl -> apply_wrapper wrap_f (rcf xcl) bindings)]
      | U(_) -> wrap_f c bindings
      | F(_) -> wrap_f c bindings
      | V(_,_) -> wrap_f c bindings
      | B(b, c2l) -> apply_wrapper wrap_f c2l (bindings@[b])
    end)) [] cl

let build_wrapper wrap_f = W(fun cl -> apply_wrapper wrap_f cl [])

let wrap_code u_f v_f f_f : (code_t list -> code_t list) =
  let aux c bindings = [match c with
    | U(uc) -> U(u_f bindings uc)
    | V(vc,rv) -> let x,y = v_f bindings vc rv in V(x,y)
    | F(fc) -> F(f_f bindings fc)
    | _ -> failwith "invalid remainder for binding"]
  in (fun restcl -> apply_wrapper aux restcl [])

let sequence_wrapper pre post c bindings =
  [match (c, bindings) with
  | U(uc),[] -> U(sequence [pre; fst uc; snd uc; post], inl "")
  | U(_), b -> failwith "unable to bind from unit return type"
  | _,x -> failwith "invalid code for imperative sequence"]

let iterate_wrapper (fields,bodyb) collection_c collection c bindings =
  let aux body = 
    let l = iterate_cl fields collection (sequence [bodyb; body])
    in sequence [collection_c; l] in
  [match c with
  | U(uc) -> U(sequence [aux (fst uc); snd uc], inl "")  
  | _ -> failwith "invalid body for iteration"]


(*************************************
 * Code-style transformation helpers
 *************************************)

(* Helper to treat value as a block *)
let use_vc_as_nc f vc rv = let x,y = f (vc, inl "") in sequence [x;y], rv 

let use_vc_as_nc_l f vc rv =
  let xyl = f (vc, inl "") in List.map (fun (x,y) -> sequence [x;y], rv) xyl 

(* returns a value to bind from a list of bindings *)
let get_var_to_bind bindings =
  if bindings = [] then gen_sym(),[]
  else List.hd bindings, List.tl bindings

(***************************
 * Binding and memoization
 ***************************)

(* Inner bindings, bind/memoize a fun/val expression
 * -- inner refers to the fact that the source of the binding comes from code
 *    that has already been generated (like a return-value in procedural code)
 * -- allows bottom-up binding, where we generate deeply nested code
 *    that requests bindings from wrapping code
 * -- note this is type-independent, i.e. does not depend on
 *    singleton/slices being passed around
 *)

(* returns a block evaluating a binding for a var (from some binding code),
 * and making it available to nested code. 
 * args: bound var, binding code, bindings for nested, nested+post code *)
let bind_aux var c bindings (nested_code, post_code) =
  (add_post_code (sequence [c; bind bindings (inl var); nested_code]) post_code)

(* inner bindings and memoization for functions and value
 * code to the given var *)
let bind_fun var fc = bind_aux var (assign var fc)
let bind_val var vc rv = bind_aux var (sequence [vc; assign var (inl rv)])    

(* binds a function for a value *)
let bind_fun_val var fc bindings vc rv =
  use_vc_as_nc (bind_fun var fc bindings) vc rv

let bind_val_val var vc1 rv1 bindings =
  use_vc_as_nc (bind_val var vc1 rv1 bindings)

(* Outer bindings, bind values from code to a given set of bindings
 * -- outer refers to the fact that this code explicitly yields a value 
 * -- this facilitates bindings in top-down code generation *)

(* Memoize singleton code to the given bindings
 * -- passes through unit code if no bindings are requested
 * -- only memoizes to a new var if no bindings are requested, returning
 *    the new var
 * -- otherwise returns the first bound var requested *)
let bind_sing_code c bindings =
  let aux v c = match bindings with
    | [] -> failwith "no bindings specified for value"
    | [x] ->
      if v = "" then (assign x c,x)
      else (sequence [c; assign x (inl v)], x)
    | _ ->
      let bv,rb = get_var_to_bind bindings in
      if v = "" then sequence [assign bv c; bind rb (inl bv)], bv
      else sequence [c; assign bv (inl v); bind rb (inl bv)], bv
  in
  match c with
  | U(uc) ->
    if bindings = [] then U(sequence [fst uc;snd uc], inl "")
    else failwith "cannot add bindings for unit code" 
  | F(fc) -> let x,y = aux "" fc in V(x,y)
  | V(vc,rv) -> let x,y = aux rv vc in V(x,y)
  | W _ -> failwith "unable to bind to unwrapped code"
  | B (_,_) -> failwith "invalid code for binding, expects binding from wrapper"

(* Auxiliaries for collection binding, defining how inner bindings are to be 
 * interpreted w.r.t to collection fields *)

(* Binds fields to the given bindings. We assume the same number of fields and
 * bindings. If not there will be a runtime exception.
 * TODO: can we statically check this?
 *)
let bind_all_loop_fields bindings = (iter_fields bindings, "")
    
(* Binds given bindings to the last field from the collection. This is useful
 * for DBToaster maps, where we want to bind to the map value *)
let bind_value_loop_field fields bindings =
  match bindings with 
  | [] -> failwith "no bindings specified for map values"
  | [v] -> (iter_fields (fields@[v]), inl "")
  | h::t -> (h, bind t (inl h))

(* Memoize slice code to the given bindings
 * -- passes through unit code if no bindings are requested *)
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
  | W _ -> failwith "unable to bind to unwrapped code"
  | B (_,_) -> failwith "invalid code for binding, expects binding from wrapper"


(* Binding code transformations, performing memoization/binding necessary
 * to evaluate functional code procedurally in impure languages.
 * TODO: these helpers return pseudo-wrappers, do we want to change?
 *        i.e. f : code -> code, rather than W(f)
 *)

(* Binds singleton functions to a fresh var, wrapping for use in other code *)
let sing_func fc =
  let s = gen_sym() in wrap_code (bind_fun s fc) (bind_fun_val s fc)
    (fun _ -> failwith "cannot bind for functions")

(* Binds singleton return values, wrapping for use in other code.
 * -- does not produce a fresh var, rather uses the existing return value
 *    which has already been allocated
 * -- if rv is functional, not memoizing to a fresh var will cause multiple
 *    evaluations on reuse. We assume this is ok since we're dealing with a
 *    singleton *)
let sing_val vc rv =
  wrap_code
    (bind_aux rv vc)
    (fun bindings -> use_vc_as_nc (bind_aux rv vc bindings))
    (fun _ -> failwith "cannot bind for functions")

(* Iterate over a fun/val expression, binding to the last field of a collection
 * TODO: generalize over binding semantics as with bind_slice_code above *)
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
let loop_slice loop_f : (code_t list -> code_t list) =
  let aux c bindings = [match c with
    | U(uc) -> U(loop_f bindings uc)
    | _ -> failwith "invalid unit value"]
  in (fun restcl -> apply_wrapper aux restcl [])


(* Wrapped slice binding code transformation methods.   *)
let slice_func fields fc = loop_slice (loop_code fields (inl "") fc)

let slice_val fields vc rv =
  loop_slice (loop_code fields vc (inl rv))

(* Use a function as value binding code *)
let sing_func_val f = let sym = gen_sym() in (assign sym f, sym)
let slice_func_val f = let sym = gen_slice_sym() in (create_map sym f, sym)

(* Bind conditionals: attempt to bind, and branch based on success
 * This is useful for map lookups. *)
(* TODO: make FOUND independent *)
let bind_sing_cond_f f v t e =
  let v_valid = v^"_valid" in
  let condc = cond (inl v_valid) t e in
  sequence [assign v f; assign v_valid (inl "FOUND"); condc]

let bind_slice_cond_f fields f v body e =
  let v_valid = v^"_valid" in
  let condc = ucond (apply_uop neg_op (inl v_valid)) e in
  sequence [iterate_cl fields f body; assign v_valid (inl "FOUND"); condc]
    

(************************************************************************
 * DBToaster-specific code generators that distinguish singletons/slices
 *************************************************************************)

(* Operator helpers *)
(* TODO: check post code *)
(* Nesting wrapper semantics:
 * we create a single nesting wrapper from binary trees, based on linearizing
 * in a postfix order. Thus leftmost leaves are first in the nesting chain, and
 * the tree root is the last node in the chain.  
 *)
(* lic,ric : code_t list -> code_t list *)
let wrap_op op (lic,lb,ls) (ric,rb,rs) =
  let aux_op c bindings =
    let r, remb = get_var_to_bind bindings in
    let (op_restc, op_postc) =
      let op_c = 
        if functional op then
            assign r (apply_op op (inl ls) (inl rs))
        else apply_procedural_op op r (inl ls) (inl rs)
      in begin match c with
      | U(uc) -> U(add_nested_code remb r op_c (fst uc), inl ""), snd uc
      | V(vc,rv) -> V(add_nested_code remb r op_c vc, rv), inl ""
      | _ -> failwith "invalid remainder for op"
      end in
    let ric_in = if rb then [B(rs, [op_restc])] else [op_restc] in
    let lic_in = if lb then [B(ls, ric ric_in)] else ric ric_in in
    List.map (fun lic_out -> add_binary_post_code lic_out op_postc) (lic lic_in) 
  in build_wrapper aux_op


(* Map access CG auxiliaries *)

(* Singleton/slice map initialization CG.
 * -- assumes ivbc binds its result to "<mapn>_val" 
 * -- ignores return values for initializers defined as value blocks
 * -- initializers will never be pure functions, they are
 *    memoized in init_expr
 * TODO: initialization currently returns a nested/post code pair, should
 * we change this to a block of type U?
 *)

let sing_init_code mapn inv out_patterns outv ivbc bindings (nc, pc) =
  let v = mapn^"_val" in
  let fields = get_map_key (inv@outv) in
  let addc = add_map mapn fields v in
  List.map (fun ivbc ->
  let ic = try get_procedural_code ivbc
           with Failure(s) -> failwith ("invalid map singleton ivbc: "^s)
  in add_post_code (add_nested_code bindings v (sequence [ic; addc]) nc) pc) ivbc 

let sing_val_init_code mapn inv out_patterns outv ivbc bindings =
  use_vc_as_nc_l (sing_init_code mapn inv out_patterns outv ivbc bindings)

(* TODO: this should really add a slice to the database in the case
 * where ivbc is not a wrapper. In general we need a fixed semantics to what
 * the ivbc binds, i.e. singletons for wrappers, and slices for U/V *)
let slice_init_code mapn inv out_patterns outv ivbc bindings (nc, pc) =
  let v = mapn^"_val" in
  let fields = get_map_key (inv@outv) in
  let addc = add_map mapn fields v in
  List.flatten (List.map (fun ivbc -> begin match ivbc with
    | U _ | V _ ->
      let c = sequence [get_procedural_code ivbc; addc]
      in [add_post_code (add_nested_code bindings v c nc) pc]       
    | W(ic) ->
      let c = add_nested_code bindings v addc nc in
      List.map (fun ic -> add_post_code (get_procedural_code ic) pc)
        (ic [U(c,inl "")])
    | _ -> failwith "invalid slice ivbc"
  end) ivbc)

let slice_val_init_code mapn inv out_patterns outv ivbc bindings =
  use_vc_as_nc_l (slice_init_code mapn inv out_patterns outv ivbc bindings)

(* Singleton/slice map lookup CG.
 * -- uses binding conditionals, i.e. attempt to bind to existing value,
 *    and on failure evaluate IVC.
 * -- both init and lookup branches bind to "<mapn>_val"
 * TODO: generalize based on type of conditional available (i.e. binding,
 * procedural, functional)
 *)

let lookup_code_init_aux v ivbc bindings nc =
  List.flatten (List.map (fun ivbc -> begin match ivbc with
    | U _ | V _ -> [add_nested_code bindings v (get_procedural_code ivbc) nc]
    | W(ic) ->
        let initc = (*sequence ~final:true [bind bindings (inl v); nc]*)
          substitute bindings v nc
        in
        List.map (fun ic -> get_procedural_code ic) (ic [U(initc, inl "")])
    | _ -> failwith "invalid init binding code during lookup"
  end) ivbc)

let sing_lookup_code mapn inv outv bindings ivbc (nc, pc) =
  let v = mapn^"_val" in
  let lookupf = get_map_sing mapn inv outv v in
  let init_code_l = lookup_code_init_aux v ivbc bindings nc in
  List.map (fun init_code ->
    let lookup_and_init_c = bind_sing_cond_f lookupf v
      (*(sequence ~final:true [bind bindings (inl v); nc])*)
      (substitute bindings v nc)
      init_code
    in (add_post_code lookup_and_init_c pc)) init_code_l

let sing_val_lookup_code mapn inv outv bindings init_code =
  use_vc_as_nc_l (sing_lookup_code mapn inv outv bindings init_code)

let slice_lookup_code mapn inv outv pat patv bindings ivbc (nc, pc) =
  let out_fields, lookup_slice = get_map_out_slice mapn inv outv pat patv in
  let v = mapn^"_val" in
  let init_code_l = lookup_code_init_aux v ivbc bindings nc in
  List.map (fun init_code ->
    let lfields = iter_fields (out_fields@[v]) in
    let lbody =
      (*(sequence ~final:true [bind bindings (inl v); nc])*)
      (substitute bindings v nc)
    in
    let lookup_c = 
      if inv = [] then iterate_cl lfields lookup_slice lbody
      else bind_slice_cond_f lfields lookup_slice v lbody init_code
    in (add_post_code lookup_c pc)) init_code_l

let slice_val_lookup_code mapn inv outv pat patv bindings ivbc =
  use_vc_as_nc_l (slice_lookup_code mapn inv outv pat patv bindings ivbc)

(* Statement initializer, increment and update helpers.
 * Much like map initialization:
 * -- assumes ivbc binds its result to "init_v" 
 * -- ignores return values for initializers defined as value blocks
 * -- initializers will never be pure functions, they are
 *    memoized in stmt_init_expr 
 * TODO: naming, perhaps make this independent of d<outv>, dv, init_v?
 *)

(* Statement IVC expects "d<lhs_outv>", "dv", "init_v" to be bound. *)
let stmt_sing_init_code ivbc bindings (nc, pc) =
  let v, rb = get_var_to_bind bindings in
  let c = assign v (apply_op add_op (inl "dv") (inl "init_v")) in
  List.map (fun ivbc ->
    let ic = try get_procedural_code ivbc
             with Failure(s) -> failwith ("invalid stmt singleton ivbc: "^s)
    in add_post_code (add_nested_code rb v (sequence [ic; c]) nc) pc) ivbc

let stmt_sing_val_init_code ivbc bindings =
  use_vc_as_nc_l (stmt_sing_init_code ivbc bindings)

let stmt_slice_init_code lhs_outv ivbc bindings (nc,pc) =
  let v, rb = get_var_to_bind bindings in
  let slice_cond =
    bind_map_vars (List.map (fun x -> "d"^x) lhs_outv) lhs_outv in
  let ic = assign v (apply_op add_op (inl "dv") (inl "init_v")) in
  List.flatten (List.map (fun ivbc -> begin match ivbc with
    | U _ | V _ -> 
      let lc = add_nested_code rb v
        (sequence [get_procedural_code ivbc; ic]) nc in
      let c = cond slice_cond lc (inl "") in [add_post_code c pc]
    | W(ivc) ->
      let lc = (add_nested_code rb v ic nc) in 
      let c = cond slice_cond lc (inl "") in 
      List.map (fun ic -> add_post_code (get_procedural_code ic) pc)
        (ivc [U(c, inl "")])
    | _ -> failwith "invalid stmt slice ivbc"
  end) ivbc)

let stmt_slice_val_init_code lhs_outv ivbc bindings =
  use_vc_as_nc_l (stmt_slice_init_code lhs_outv ivbc bindings)


(* Statement increment helpers
 * TODO: naming, perhaps make this independent of d<outv>, dv,
 *       current_v, current_v_valid?
 *)

(* Singleton stmt increments expect "current_v","current_v_valid" to be bound *)
let incr_code_aux v initb bindings nc =
  let sum_c = assign v (apply_op add_op (inl "current_v") (inl "dv")) in
  let initc_l = List.map (fun ivbc ->
    add_nested_code bindings v (get_procedural_code ivbc) nc) initb
  in
  let upd_cond = inl "current_v_valid" in
  List.map (fun initc ->
    cond upd_cond (add_nested_code bindings v sum_c nc) initc) initc_l

let sing_incr_code v initb incrb bindings (nc, pc) =
  let upd_cl = incr_code_aux v initb bindings nc in
  let incrc_l = List.map (fun incrb -> get_procedural_code incrb) incrb
  in List.flatten (List.map (fun incrc ->
    List.map (fun upd_c ->
      add_post_code (sequence [incrc; upd_c]) pc) upd_cl) incrc_l)

let sing_val_incr_code v initb incrb bindings =
  use_vc_as_nc_l (sing_incr_code v initb incrb bindings)

(* Slice stmt increments perform a merge that includes map lookups to bind
 * "current_v", "current_v_valid". This is currently inefficient, since the
 * lookup is performed on the entire map, rather than an out tier.
 * Instead, we could implement a two-tiered relational map, or create a
 * temporary table for the out tier.
 * The merge iterates over the delta slice since it is usually smaller than the
 * existing map.
 *)
let slice_incr_code mapn inv outv v initb incrb bindings (nc, pc) =
  let entry_cl = incr_code_aux v initb bindings nc in
  let rvf = iter_fields ((List.map (fun x -> "d"^x) outv)@["dv"]) in
  let current_c = get_map_sing_val mapn inv outv "current_v" in
  List.flatten (List.map (fun incrb -> begin match incrb with
    | U(_) -> failwith "cannot handle unit code for delta slice" 
    | V(vc,rv) ->
      let scl = List.map (fun entry_c ->
        sequence ~final:true [current_c; entry_c]) entry_cl
      in
      List.map (fun sc -> 
        let merge_c = iterate_cl rvf (inl rv) sc in
        let c = sequence [get_procedural_code incrb; merge_c]
        in add_post_code c pc) scl
    | W(ic) ->
      let scl = List.map (fun entry_c -> sequence [current_c; entry_c]) entry_cl in
      List.map (fun ic -> add_post_code (get_procedural_code ic) pc)
        (List.flatten (List.map (fun sc -> ic [U(sc, inl "")]) scl))
    | _ -> failwith "invalid slice incr stmt incr code"
  end) incrb)

let slice_val_incr_code mapn inv outv v initb incrb bindings =
  use_vc_as_nc_l (slice_incr_code mapn inv outv v initb incrb bindings)

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




(*************************************************************************
 * High-level dispatchers to low-level code generator methods.
 * These are based on DBToaster's incremental statements, and are language
 * independent, thus do not need to be reimplemented for different backends.
 *************************************************************************)

(* Operators
 * -- nesting wrapper semantics: operators combine branches, thus
 *    they yield a single nesting wrapper
 * TODO: prebind, inbind
 * -- needs more general model for B, i.e. decl, val rather than just decl
 * -- inbinds can be added before eval of lhs op
 * -- prebinds are bindings around result, ensuring binding before op eval
 *)
let op_expr lf_i rf_i lv_i rv_i lf_v rf_v compose_f compose_v
            prebind inbind op outv1 outv2 schema theta_ext schema_ext ce1 ce2
=
  let auxv c1 c2 v1 v2 =
    let x,y = compose_v op c1 c2 v1 v2 in V(x,y) in
  let prep_op f v c = match c with
    | W(ic) -> ic, true, gen_sym()
    | V(vc,rv) -> (v vc rv), false, rv
    | F(fc) -> (f fc), true, gen_sym()
    | _ -> failwith "invalid argument for binary operator"
  in
  let lc,rc = (prep_op lf_i lv_i ce1), (prep_op rf_i rv_i ce2) in
  match (prebind, inbind, functional op, ce1,ce2) with
  | _,_,false,_,_
  | _,_,_,W _,W _ | _,_,_,W _,F _ | _,_,_,F _,W _
  | _,_,_,W _,V _ | _,_,_,V _,W _ -> wrap_op op lc rc

  | _, _, true, F(fc), V(vc,rv) -> let vcf,rvf = lf_v fc in auxv vcf vc rvf rv
  | _, _, true, V(vc,rv), F(fc) -> let vcf,rvf = rf_v fc in auxv vc vcf rv rvf
  | _, _, true, V(c1,r1), V(c2,r2) -> auxv c1 c2 r1 r2
  | [], [], true, F(c1), F(c2) -> F(compose_f op c1 c2)

  | _, _, true, F(c1), F(c2) ->
    failwith "sequential binding not supported for purely functional code"
  | _,_,_,U(_),_  | _,_,_,_,U(_) -> failwith "invalid unit operands"
  | _,_,_,B(_,_),_ | _,_,_,_,B(_,_) -> failwith "Op binding NYI" 

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
    | U(uc) -> List.map (fun (x,y) -> U(x,y)) (init_u ivbc bindings uc)
    | V(vc,rv) -> List.map (fun (x,y) -> V(x,y)) (init_v ivbc bindings vc rv)
    | _ -> failwith "invalid map initialization code"
  in 
  let ivbc =
    match cinit with
    | W(ic) -> ic [B(v, [U(inl (single_comment "end of stmt init"), inl "")])]
    | _ -> [bind_f cinit [v]]
  in build_wrapper (aux ivbc)

let singleton_init_lookup mapn inv out_patterns outv cinit =
  init_expr bind_sing_code
  (sing_init_code mapn inv out_patterns outv)
  (sing_val_init_code mapn inv out_patterns outv)
  mapn cinit

let slice_init_lookup mapn inv out_patterns outv cinit =
  let fields =
    get_map_fields_l mapn (List.length inv) (get_num_outv [] out_patterns)
  in init_expr (bind_slice_code fields)
    (slice_init_code mapn inv out_patterns outv)
    (slice_val_init_code mapn inv out_patterns outv) mapn cinit 


(* Map lookups *)
let lookup_and_init_expr lookup_u lookup_v cinit =
  let aux_lookup ic c bindings =
    match c with
    | U(uc) -> List.map (fun (x,y) -> U(x,y)) (lookup_u bindings ic uc)
    | V(vc,rv) -> List.map (fun (x,y) -> V(x,y)) (lookup_v bindings ic vc rv)
    | _ -> failwith "invalid unit value"
  in build_wrapper (aux_lookup [cinit])

let singleton_lookup_and_init mapn inv outv =
  let aux f = f mapn inv outv in 
  lookup_and_init_expr (aux sing_lookup_code) (aux sing_val_lookup_code)

let singleton_lookup mapn inv outv =
  let aux f = f mapn inv outv in
  lookup_and_init_expr (aux sing_lookup_code) (aux sing_val_lookup_code)

let slice_lookup_sing_init mapn inv outv pat patv =
  let aux f = f mapn inv outv pat patv in
  lookup_and_init_expr (aux slice_lookup_code) (aux slice_val_lookup_code)

let slice_lookup mapn inv outv pat patv =
  let aux f = f mapn inv outv pat patv in
  lookup_and_init_expr (aux slice_lookup_code) (aux slice_val_lookup_code)


(* RHS expressions, bigsums, etc *)
    
(* TODO: create f/v for unit code, just as for below *)
let singleton_expr ccalc cdebug = ccalc
let direct_slice_expr ccalc cdebug = ccalc

let full_agg_slice_expr ccalc cdebug =
  match ccalc with
  | W(ic) ->
    (* TODO: pick best code *)
    let agg_v,v = gen_agg_sym(), gen_sym() in 
    let aggc = aggregate agg_v (inl v) in
    V(get_procedural_code (List.hd (ic [B(v,[U(aggc, inl "")])])), agg_v)

  | F(fc) -> F(full_aggregate_f fc)
  | V(vc,rv) ->
    let agg_v = gen_agg_sym() in
    V(sequence [vc; full_aggregate_v agg_v (inl rv)], agg_v)

  (* Can't handle unit since we don't know what to aggregate *)
  | _ -> failwith "invalid full agg slice rhs"

let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
  let gb = String.concat "," lhs_outv in
  match ccalc with
  | W(ic) ->
    (* TODO: pick best code *)
    let v, slice_v = gen_sym(), gen_slice_sym() in
    let slice_decl_c = declare_map slice_v lhs_outv in
    let addc = add_map slice_v gb v in
    let gbc = gb_aggregate_v (inl slice_v) gb in
    let rc = get_procedural_code (List.hd (ic [B(v,[U(addc, inl "")])])) in 
    V(sequence [slice_decl_c; rc], sbc gbc)

  | F(fc) -> F(gb_aggregate_f fc gb)
  | V(vc,rv) -> V(vc, sbc (gb_aggregate_v (inl rv) gb))
  
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
    | U(uc) -> List.map (fun (x,y) -> U(x,y)) (stmt_u ivbc bindings uc)
    | V(vc,rv) -> List.map (fun (x,y) -> V(x,y)) (stmt_v ivbc bindings vc rv)
    | _ -> failwith "invalid map initialization code"
  in
  let ivbc = match cinit with
    | W(ic) ->
        ic [B("init_v", [U(inl (single_comment "end of stmt init"), inl "")])]
    | _ -> [bind_f cinit ["init_v"]]
  in build_wrapper (aux ivbc)

let singleton_init = stmt_init_expr bind_sing_code
  stmt_sing_init_code stmt_sing_val_init_code 

(* bind_slice_code binds the keys of the ivc slice here *)
let slice_init lhs_inv lhs_outv init_ext =
  stmt_init_expr (bind_slice_code lhs_outv)
    (stmt_slice_init_code lhs_outv) (stmt_slice_val_init_code lhs_outv)


(* Statement increments
 * -- ensures both branches populate "newv" or variable from bindings with
 *    the incremented value
 * -- computes delta sing/slice first
 * -- branches on ivc if needed
 * -- performs merging for slices
 *)
let stmt_incr_expr bindt_f bindr_f stmt_u stmt_v
                   lhs_inv lhs_outv cincr cinit cdebug =
  let aux initb incrb c bindings =
    let v, rb = if bindings = [] then "newv", []
                else List.hd bindings, List.tl bindings in
    match c with
    | U(uc) -> List.map (fun (x,y) -> U(x,y)) (stmt_u v (initb v) incrb rb uc)
    | V(vc,rv) -> List.map (fun (x,y) -> V(x,y)) (stmt_v v (initb v) incrb rb vc rv) 
    | _ -> failwith "invalid unit code"
  in
  let initb v = match cinit with
    | W(initc) ->
        initc [B(v, [U(inl (single_comment "end of incr stmt init"), inl "")])]
    | _ -> [bindt_f cinit [v]]
  in
  let incrb = match cincr with
    | W(incrc) ->
        incrc [B("dv", [U(inl (single_comment "end of incr stmt"), inl "")])]
    | _ -> [bindr_f cincr ["dv"]]
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
  let updc =
    match cupdate with
    | W(ic) -> ic
    | F(fc) -> update_f fc
    | V(vc,rv) -> update_v vc rv
    | _ -> failwith "invalid statement"
  in
  (* TODO: pick the best code *)
  List.hd (List.map (fun uc ->
    U(stmt_f (get_procedural_code uc))) (updc [B(v, [U(wq, inl "")])]))

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
let pm_const pm = match pm with | Insert -> "insert" | Delete -> "delete" 
let pm_name pm  = match pm with | Insert -> "ins" | Delete -> "del" 

let trigger event rel trig_args stmt_block =
  let trig_name = (pm_name event)^"_"^rel in
  let trig_aux_def =
    "create trigger on_"^trig_name^
    " after "^(pm_const event)^" on "^rel^
    " for each row execute procedure "^trig_name^";"
  in
  let trig_params = ibc tab (sequence ~final:true
    (List.map (fun x -> assign x (inl ("NEW."^x))) trig_args))
  in
  let tc_stmts = List.map (fun x -> match x with
    | U(uc) -> ibc tab (get_procedural_code x)
    | _ -> failwith "invalid statement code") stmt_block
  in
  let var_decls =
    let num_vars = get_counter var_c in
    let rec decl_var i acc =
        if i > num_vars then acc
        else
          let x =
            let var_name = "var"^(string_of_int i) in
            let var_used = List.exists (fun stmt_c ->
                let sc = sbc stmt_c in
                List.exists (fun re ->
                    try (Str.search_forward re sc 0) >= 0
                    with Not_found -> false)
                  [Str.regexp_string (var_name^" ");
                   Str.regexp_string (var_name^";")])
              tc_stmts in
            if var_used then acc@[inl(var_name^" real")] else acc
          in decl_var (i+1) x
    in
    let vl = (decl_var 0 [])@(List.map (fun v -> inl(v^" real"))
      ["current_v"; "dv"; "init_v"; "newv"; "updated_v"])
    in ibc tab (cbc (cbc (inl "declare")
         (ibc tab (sequence ~final:true vl))) (inl "begin"))
  in
  let tc =
    (Lines ["create function "^trig_name^" () returns trigger as $$"])::
    var_decls::
    trig_params::
    (sequence ~final:true (tc_stmts@[ibc tab (inl "end")]))::
    [(Lines ["$$ language plpgsql;"; ""; trig_aux_def; ""])]
  in
  (* Reset symbol generation for the next trigger *)
  reset_syms();
  U(List.fold_left cbc (List.hd tc) (List.tl tc), inl "")

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


(* We only support line delimited csv file sources for now *)
let source src fr rel_adaptors = 
  let source_stmts =
    match src,fr with
    | FileSource fn, Delimited "\n" -> make_file_source fn rel_adaptors
    | _ -> failwith "Unsupported data source" 
  in (source_stmts, None, None)

let main schema patterns sources triggers toplevel_queries =
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
  let body = List.map (fun x -> match x with
    | U(uc) -> fst uc
    | _ -> failwith "invalid trigger") triggers
  in
  let footer = sequence ~final:true
    (List.map (fun (id,_,_) -> inl("drop table "^id)) schema) in
  let script = cbcl
    ([maps; inl ""]@body@
    (List.map (fun (x,y,z) -> x) sources)@
    [inl ""; footer])
  in U(script, inl "")

let output code out_chan =
  output_string out_chan (sbc (get_procedural_code code));
  flush out_chan

let eval_trigger trigger tuple db =
  failwith "Cannot directly evaluate PLSQL source"


(* Unit tests *)
(*
let print_code c = match c with
  | U(c,pc) -> print_endline (sbc (fst (add_post_code c pc)))
  | F(fc) -> print_endline (sbc fc)
  | V(vc,v) -> print_endline (v^" ::= "); print_endline (sbc vc)
  | W(_) -> failwith "cannot print, unfinished code generation."
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
  | W(qf) -> qf (U(inl "<update/insert code>;", inl "<agg, merge code>;"))
  | _ -> failwith "invalid query"
in print_code c
*)

(*
let q = slice_lookup "m1" ["x"; "y"] ["a";"b";"c"] [1] ["b"]
  (singleton_init_lookup "m1" ["x";"y"] [] ["a";"b";"c"] (const (CFloat 0.0)))
in 
  match q with
  | W(qf) ->
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
  | W(qf) ->
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