(* m3 interface module *)
open Util

type const_t = (* CInt of int | *) CFloat of float (* | CBool of bool *)
type var_id_t = string
type var_type_t = VT_String | VT_Int | VT_Float
type var_t = var_id_t (*  * var_type_t *)
type map_id_t = string
type pm_t = Insert | Delete
type rel_id_t = string
type map_type_t = map_id_t * (var_type_t list) * (var_type_t list)


(* Printing basic types *)
let vars_to_string vs = Util.list_to_string (fun x->x) vs

let string_of_const v =
  match v with
      CFloat(f) -> string_of_float f
 (* | CInt(i)   -> string_of_int i *)
 (* | CBool(b)  -> if (b) then "false" else "true" *)

let string_of_var_type v = 
  match v with
      VT_String -> "STRING"
    | VT_Int    -> "INT"
    | VT_Float  -> "FLOAT"

let string_of_type_list (l:var_type_t list) : string = 
  (list_to_string string_of_var_type l)

type ('calcmeta_t, 'aggmeta_t) generic_aggcalc_t = 
  ('calcmeta_t, 'aggmeta_t) generic_calc_t * 'aggmeta_t
and ('c, 'a) generic_calc_contents_t = 
    Add         of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
  | Mult        of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t 
             (* if (cond != 0) then calc else 0 *)
  | IfThenElse0 of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t 
  | MapAccess   of ('c,'a) generic_mapacc_t
  | Const of const_t
  | Var   of var_t
             (* conditions: 1 if calc OP calc else 0 *)
  | Leq of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
  | Eq  of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
  | Lt  of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
and ('c, 'a) generic_calc_t = ('c,'a) generic_calc_contents_t * 'c

and ('c, 'a) generic_mapacc_t = 
  map_id_t * (var_t list) * (var_t list) * ('c,'a) generic_aggcalc_t
type ('c, 'a, 'sm) generic_stmt_t = 
  ('c, 'a) generic_mapacc_t * ('c, 'a) generic_aggcalc_t * 'sm
type ('c, 'a, 'sm) generic_trig_t = 
  pm_t * rel_id_t * (var_t list) * (('c, 'a, 'sm) generic_stmt_t list)
type ('c,'a,'sm) generic_prog_t = 
  (map_type_t list) * (('c,'a,'sm) generic_trig_t list)

(* M3 Basic - Types *)
type calc_t = (unit, unit) generic_calc_t;;
type mapacc_t = (unit, unit) generic_mapacc_t;;
type stmt_t = (unit, unit, unit) generic_stmt_t;;
type trig_t = (unit, unit, unit) generic_trig_t;;
type prog_t = (unit, unit, unit) generic_prog_t;;

(******************* Type Constructors *******************)
let mk_op (meta:'calcmeta_t) (op:string) 
          (l:('c,'a) generic_calc_t) (r:('c,'a) generic_calc_t) = 
  match op with 
    | "+"  -> ( Add(l,r), meta)
    | "*"  -> (Mult(l,r), meta)
    | "<"  -> (  Lt(l,r), meta)
    | "<=" -> ( Leq(l,r), meta)
    | "="  -> (  Eq(l,r), meta)
    | ">"  -> (  Lt(r,l), meta)
    | ">=" -> ( Leq(r,l), meta)
    | _    -> failwith ("mk_op: Unknown Op '"^op^"'")

let mk_sum l r = (Add(l,r), ())
let mk_prod l r = (Mult(l,r), ())
let mk_lt l r = (Lt(l,r), ())
let mk_leq l r = (Leq(l,r), ())
let mk_eq l r = (Eq(l,r), ())
let mk_if l r = (IfThenElse0(l,r), ())
let mk_ma ma = (MapAccess(ma), ())
let mk_c c = (Const(CFloat(c)), ())
let mk_v v = (Var(v), ())

(******************* Utility Operations *******************)
let rec recurse_calc_with_meta 
    (op_f: 'c -> string -> 'b -> 'b -> 'b)
    (if_f: 'c -> 'b -> 'b -> 'b)
    (ma_f: 'c -> ('c,'a) generic_mapacc_t -> 'b)
    (const_f: 'c -> const_t -> 'b)
    (var_f: 'c -> var_t -> 'b)
    (calc:('c,'a) generic_calc_t) : 'b = 
  let recurse subexp = 
    (recurse_calc_with_meta op_f if_f ma_f const_f var_f subexp) 
  in
  match (fst calc) with
    | MapAccess(ma) -> ma_f (snd calc) ma
    | Add (c1, c2) -> op_f (snd calc) "+" (recurse c1) (recurse c2)
    | Mult(c1, c2) -> op_f (snd calc) "*" (recurse c1) (recurse c2)
    | Lt  (c1, c2) -> op_f (snd calc) "<" (recurse c1) (recurse c2)
    | Leq (c1, c2) -> op_f (snd calc) "<=" (recurse c1) (recurse c2)
    | Eq  (c1, c2) -> op_f (snd calc) "=" (recurse c1) (recurse c2)
    | IfThenElse0(c1, c2) -> if_f (snd calc) (recurse c1) (recurse c2)
    | Const(c)     -> const_f (snd calc) c
    | Var(v)       -> var_f (snd calc) v
;;

let recurse_calc op_f if_f ma_f c_f var_f : (('c,'a) generic_calc_t -> 'b) =
  let uncurry f = (fun _ -> f) in
  recurse_calc_with_meta
    (uncurry op_f) (uncurry if_f) (uncurry ma_f) (uncurry c_f) (uncurry var_f)

let name_of_op (op:string) =
  match op with 
    | "+"  -> "Add"
    | "*"  -> "Mult"
    | "<"  -> "Lt"
    | "<=" -> "Leq"
    | "="  -> "Eq"
    | _    -> failwith ("name_of_op: Unknown Op '"^op^"'")

let recurse_calc_lf 
      (join_f: 'b -> 'b -> 'b) 
      (ma_f: ('c,'a) generic_mapacc_t -> 'b)
      (const_f: const_t -> 'b)
      (var_f: var_t -> 'b)
      (calc:('c,'a) generic_calc_t) : 'b = 
  recurse_calc (fun _ a b -> join_f a b) join_f ma_f const_f var_f calc;;

let replace_calc_lf_with_meta 
      (ma_f: 'c -> ('c,'a) generic_mapacc_t -> ('c,'a) generic_calc_t)
      (const_f: 'c -> const_t -> ('c,'a) generic_calc_t)
      (var_f: 'c -> var_t -> ('c,'a) generic_calc_t)
      (calc:('c,'a) generic_calc_t) : ('c,'a) generic_calc_t = 
  recurse_calc_with_meta mk_op (fun meta a b -> (IfThenElse0(a, b), meta))
                         ma_f const_f var_f calc;;

let replace_calc_lf 
      (ma_f: ('c,'a) generic_mapacc_t -> ('c,'a) generic_calc_contents_t)
      (const_f: const_t -> ('c,'a) generic_calc_contents_t)
      (var_f: var_t -> ('c,'a) generic_calc_contents_t)
      (calc:('c,'a) generic_calc_t) : ('c,'a) generic_calc_t = 
  replace_calc_lf_with_meta 
    (fun meta p -> (ma_f p   , meta))
    (fun meta p -> (const_f p, meta))
    (fun meta p -> (var_f p  , meta))
    calc;;

(******************* Generic Type Math Operations *******************)
let c_sum (a : const_t) (b : const_t) : const_t = 
  match a with
 (*   CInt(i1) -> (
        match b with
            CInt(i2) -> CInt(i1 + i2)
          | CFloat(f2) -> CFloat((float_of_int i1) +. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
 *)
    | CFloat(f1) -> (
        match b with
 (*         CInt(i2) -> CFloat(f1 +. (float_of_int i2)) *)
          | CFloat(f2) -> CFloat(f1 +. f2)
 (*       | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)
      )
 (* | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)
;; 

let c_prod (a : const_t) (b : const_t) : const_t =
  match a with
 (*   CInt(i1) -> (
        match b with
            CInt(i2) -> CInt(i1 * i2)
          | CFloat(f2) -> CFloat((float_of_int i1) *. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
 *)
    | CFloat(f1) -> (
        match b with
 (*         CInt(i2) -> CFloat(f1 *. (float_of_int i2)) *)
          | CFloat(f2) -> CFloat(f1 *. f2)
 (*       | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)
      )
 (* | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)

(******************* Querying *******************)
let calc_vars_aux f : ((('c,'a) generic_calc_t) -> var_t list)
  = recurse_calc_lf Util.ListAsSet.union
(*ma*)       (fun (_, inv, outv, _) -> f inv outv)
(*const*)    (fun _ -> [])
(*var*)      (fun x -> [x])

let calc_schema (c:(('c,'a) generic_calc_t)) : var_t list
    = calc_vars_aux (fun inv outv -> outv) c
let calc_vars (c:(('c,'a) generic_calc_t)) : var_t list
    = calc_vars_aux (fun inv outv -> inv@outv) c

(******************* Batch Modification *******************)
let rename_maps (mapping:(map_id_t) StringMap.t)
                ((stmt_defn, stmt_calc, stmt_meta):('c,'a,'s) generic_stmt_t): 
                (('c,'a,'s) generic_stmt_t) =
  let rec aux_ma ((mn,ivars,ovars,init):('c,'a) generic_mapacc_t) : 
                 ('c,'a) generic_mapacc_t =
    (
      (if StringMap.mem mn mapping then StringMap.find mn mapping else mn),
      ivars, ovars, (aux_calc init)
    )
  and aux_calc ((e,meta):('c,'a) generic_aggcalc_t): 
               (('c,'a) generic_aggcalc_t) = 
    ((replace_calc_lf 
      (fun ma -> MapAccess(aux_ma ma)) (fun x->Const(x)) (fun x->Var(x)) e),
     meta)
  in
    (aux_ma stmt_defn, aux_calc stmt_calc, stmt_meta)
;;

let rename_vars (src_vars:var_t list) (dst_vars:var_t list) 
                ((stmt_defn, stmt_calc, stmt_meta):('c,'a,'s) generic_stmt_t): 
                ('c,'a,'s) generic_stmt_t =
  let rec var_unused_in_calc (var:var_t) : (('c,'a) generic_calc_t -> bool) =
    recurse_calc_lf (&&) (var_unused_in_mapacc var)
                    (fun _ -> true) (fun x -> x <> var)
  and var_unused_in_mapacc var (_,ivars,ovars,icalc) =
    if List.exists (fun x -> x == var) ivars ||
       List.exists (fun x -> x == var) ovars
    then false
    else (var_unused_in_calc var (fst icalc))
  in
  let rec find_safe_mapping vars subs mapping_accum =
    let (todos, mapping) =
      List.fold_left2 (fun (todos,mapping) var sub ->
        (
          if var == sub then (todos,mapping)
          else
            (*
              We need to guarantee that var is replaced with sub everywhere
              it occurs in the expression.  However it's possible that sub
              already occurs in the expression.  If that's the case, then we
              need to replace all occurrences of sub with sub^something.
            *)
            (
              (if (var_unused_in_mapacc sub stmt_defn) &&
                  (var_unused_in_calc sub (fst stmt_calc))
                then todos
                else sub::todos),
              StringMap.add var sub mapping
            )
        )
      ) ([],mapping_accum) vars subs
    in
      if (List.length todos) > 1 then
        find_safe_mapping todos (List.map (fun x -> x^"_p") todos) mapping
      else
        mapping
  in
  let mapping = find_safe_mapping src_vars dst_vars StringMap.empty in
  let map_var var = 
    if StringMap.mem var mapping then StringMap.find var mapping
                                 else var
  in
  let rec sub_exp ((e,meta):('c,'a) generic_aggcalc_t) = 
    ( replace_calc_lf (fun ma -> MapAccess(sub_ma ma))
                      (fun x->Const(x)) (fun x->Var(x)) e,
      meta
    )
  and sub_ma ((name, ivars, ovars, icalc):('c,'a) generic_mapacc_t) : 
             ('c,'a) generic_mapacc_t = 
    (name, List.map map_var ivars, List.map map_var ovars, 
      sub_exp icalc)
  in 
    (sub_ma stmt_defn, sub_exp stmt_calc, stmt_meta)

(******************* String Output *******************)
let rec indented_map_access (mapn, inv, outv, init_calc): IndentedPrinting.t =
  IndentedPrinting.Node(("{ "," }"),(" := ",""),
    IndentedPrinting.Leaf(mapn^(vars_to_string inv)^(vars_to_string outv)),
    indented_calc (fst init_calc)
  )
and indented_const c = IndentedPrinting.Leaf(string_of_const c)
and indented_vtype vt = IndentedPrinting.Leaf(string_of_var_type vt)
and indented_calc (calc:('c,'a) generic_calc_t) : IndentedPrinting.t = 
  recurse_calc
    (fun op c1 c2 -> 
      IndentedPrinting.Node(("( "," )"),(" "^op^" ",""), c1, c2)
    )
    (fun c1 c2 -> 
      IndentedPrinting.Node(
        ("{ "," )}"),(" ) ","THEN ( "),
        IndentedPrinting.Parens(("IF ( ",""), c1), c2
      )
    )
    indented_map_access
    indented_const
    (fun x -> IndentedPrinting.Leaf(x))
    calc
and indented_stmt (mapacc, delta_term, meta) =
  IndentedPrinting.Node(
    ("",""),(" +="," "),
    (indented_map_access mapacc), (indented_calc (fst delta_term))
  )
and indented_trig (pm, rel_id, var_id, statements) = 
  IndentedPrinting.Node(
    ("O",""),(": ",""),
    IndentedPrinting.Leaf(
      "N "^(match pm with Insert -> "+" | Delete -> "-")^
      rel_id^(vars_to_string var_id)
    ),
    IndentedPrinting.Lines(List.map indented_stmt statements)
  )
and indented_map_defn (mapn, input_var_types, output_var_types) = 
  IndentedPrinting.Leaf(mapn ^ " " ^ (string_of_type_list input_var_types) ^
                                     (string_of_type_list output_var_types))
and indented_prog ((maps, trigs):('c,'a,'s) generic_prog_t) =
  IndentedPrinting.Lines(
    (List.map indented_map_defn maps)@
    (List.map indented_trig trigs)
  )
      
let indent_to_string = IndentedPrinting.to_string 120;;(* set width here *)

let pretty_print_map_access (ma:('c,'a) generic_mapacc_t): string = 
  indent_to_string (indented_map_access ma)
let pretty_print_calc calc : string =
  indent_to_string (indented_calc calc)
let pretty_print_stmt stmt : string =
  indent_to_string (indented_stmt stmt)
let pretty_print_trig trig : string =
  indent_to_string (indented_trig trig)  
let pretty_print_map map : string =
  indent_to_string (indented_map_defn map)
let pretty_print_prog prog = 
  indent_to_string (indented_prog prog)

let rec code_of_map_access 
      ((mapn,inv,outv,init):('c,'a) generic_mapacc_t): string = 
  "("^mapn^", "^(vars_to_string inv)^", "^(vars_to_string outv)^
  (code_of_calc (fst init))^")"
and code_of_calc (calc:('c,'a) generic_calc_t): string =
  recurse_calc 
    (fun op l r -> "("^(name_of_op op)^"("^l^", "^r^"))")
    (fun l r -> "(IfThenElse0("^l^", "^r^"))")
    (fun ma -> "(MapAccess"^(code_of_map_access ma)^")")
    (fun c -> "(Const(CFloat("^(string_of_const c)^")))")
    (fun v -> "(Var("^v^"))")
    calc
let code_of_stmt ((mapacc,(calc,_),_):('c,'a,'sm) generic_stmt_t) : string =
  "("^(code_of_map_access mapacc)^", "^(code_of_calc calc)^")"

(******************* M3 Prepared *******************)
module Prepared = struct

  type pextension_t   = var_t list
   
  (* id, theta extension, singleton, cross product *)
  type pcalcmeta_t    = int * pextension_t * bool * bool
  
  (* name, full aggregation *)   
  type paggmeta_t     = string * bool
  
  (* loop in vars extension *)
  type pstmtmeta_t    = pextension_t
  
  type pcalc_t = (pcalcmeta_t, paggmeta_t) generic_calc_contents_t
  type ecalc_t = (pcalcmeta_t, paggmeta_t) generic_calc_t
  type aggecalc_t = (pcalcmeta_t, paggmeta_t) generic_aggcalc_t
  
  type pmapacc_t = (pcalcmeta_t, paggmeta_t) generic_mapacc_t
  type pstmt_t = (pcalcmeta_t, paggmeta_t, pstmtmeta_t) generic_stmt_t
  type ptrig_t = (pcalcmeta_t, paggmeta_t, pstmtmeta_t) generic_trig_t
  type pprog_t = (pcalcmeta_t, paggmeta_t, pstmtmeta_t) generic_prog_t
  
  (* accessors *)
  let get_calc ecalc = fst ecalc
  let get_meta ecalc = snd ecalc
  let get_extensions ecalc = let (_,x,_,_) = get_meta ecalc in x
  let get_id ecalc = let (x,_,_,_) = get_meta ecalc in x
  let get_singleton ecalc = let (_,_,x,_) = get_meta ecalc in x
  let get_product ecalc = let (_,_,_,x) = get_meta ecalc in x
  
  let get_ecalc aggecalc = fst aggecalc
  let get_agg_meta aggecalc = snd aggecalc
  let get_agg_name aggmeta = fst aggmeta
  let get_full_agg aggmeta = snd aggmeta
  
  let get_inv_extensions stmtmeta = stmtmeta
end

(******************* Patterns *******************)
module Patterns =
struct
  type pattern =
       In of (var_t list * int list)
     | Out of (var_t list * int list)
  
  type pattern_map = (string * pattern list) list
  
  let index l x =
     let pos = fst (List.fold_left (fun (run, cur) y ->
        if run >= 0 then (run, cur) else ((if x=y then cur else run), cur+1))
        (-1, 0) l)
     in if pos = -1 then raise Not_found else pos
  
  let make_in_pattern dimensions accesses =
     In(accesses, List.map (index dimensions) accesses)
  
  let make_out_pattern dimensions accesses =
     Out(accesses, List.map (index dimensions) accesses)
  
  let get_pattern = function | In(x,y) | Out(x,y) -> y
  
  let get_pattern_vars = function | In(x,y) | Out(x,y) -> x
  
  let empty_pattern_map() = []
  
  let get_filtered_patterns filter_f pm mapn =
     let map_patterns = if List.mem_assoc mapn pm
                        then List.assoc mapn pm else []
     in List.map get_pattern (List.filter filter_f map_patterns)
  
  let get_in_patterns (pm:pattern_map) mapn = get_filtered_patterns
     (function | In _ -> true | _ -> false) pm mapn
  
  let get_out_patterns (pm:pattern_map) mapn = get_filtered_patterns
     (function | Out _ -> true | _ -> false) pm mapn
  
  let get_out_pattern_by_vars (pm:pattern_map) mapn vars = 
    List.hd (get_filtered_patterns
     (function Out(x,y) -> x = vars | _ -> false) pm mapn)
  
  let add_pattern pm (mapn,pat) =
     let existing = if List.mem_assoc mapn pm then List.assoc mapn pm else [] in
     let new_pats = pat::(List.filter (fun x -> x <> pat) existing) in
        (mapn, new_pats)::(List.remove_assoc mapn pm)
  
  let merge_pattern_maps p1 p2 =
     let aux pm (mapn, pats) =
        if List.mem_assoc mapn pm then
           List.fold_left (fun acc p -> add_pattern acc (mapn, p)) pm pats
        else (mapn, pats)::pm
     in List.fold_left aux p1 p2
  
  let singleton_pattern_map (mapn,pat) = [(mapn, [pat])]
  
  let patterns_to_string pm =
     let patlist_to_string pl = List.fold_left (fun acc pat ->
        let pat_str = String.concat "," (
           match pat with | In(x,y) | Out(x,y) ->
              List.map (fun (a,b) -> a^":"^b)
                 (List.combine (List.map string_of_int y) x))
        in
        acc^(if acc = "" then acc else " / ")^pat_str) "" pl
     in
     List.fold_left (fun acc (mapn, pats) ->
        acc^"\n"^mapn^": "^(patlist_to_string pats)) "" pm
end

(******************* M3 Data Sources *******************)

type event = pm_t * const_t list
type stream_event = pm_t * rel_id_t * const_t list
type framing_t = 
    FixedSize of int
  | Delimited of string
  | VarSize of int * int
type source_t =
   FileSource of string
 | PipeSource of string
 | SocketSource of Unix.inet_addr * int
type adaptor_t = string * (string * string) list
type relation_input_t = source_t * framing_t * string * adaptor_t
