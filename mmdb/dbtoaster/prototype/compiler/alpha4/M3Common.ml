include M3

module StringMap = Map.Make(String)

let vars_to_string vs = Util.list_to_string (fun x->x) vs

let rec calc_vars_aux f calc =
   let recur = calc_vars_aux f in
   let op c1 c2 = Util.ListAsSet.union (recur c1) (recur c2) in
   match calc with
      MapAccess(mapn, inv, outv, init_calc) -> f inv outv
    | Add (c1, c2)        -> op c1 c2
    | Mult(c1, c2)        -> op c1 c2
    | Lt  (c1, c2)        -> op c1 c2
    | Leq (c1, c2)        -> op c1 c2
    | Eq  (c1, c2)        -> op c1 c2
(*    | And (c1, c2)        -> op c1 c2 *)
    | IfThenElse0(c1, c2) -> op c2 c1
    | Const(i)            -> []
    | Var(x)              -> [x]

let calc_schema = calc_vars_aux (fun inv outv -> outv)
let calc_vars = calc_vars_aux (fun inv outv -> inv@outv)

let rec pretty_print_map_access (mapn, inv, outv, init_calc): string =
  "{ "^mapn^(vars_to_string inv)^(vars_to_string outv)^" := ("^(pretty_print_calc init_calc)^") }"

and string_of_const v =
  match v with
      CFloat(f) -> string_of_float f
 (* | CInt(i)   -> string_of_int i *)
 (* | CBool(b)  -> if (b) then "false" else "true" *)

and string_of_var_type v = 
  match v with
      VT_String -> "STRING"
    | VT_Int    -> "INT"

and pretty_print_calc calc : string =
  let ots op c1 c2 = "("^(pretty_print_calc c1)^" "^op^" "^(pretty_print_calc c2)^")" in
  match calc with
      MapAccess(mapacc)  -> pretty_print_map_access(mapacc)
    | Add(c1, c2)        -> ots "+" c1 c2
    | Mult(c1, c2)       -> ots "*" c1 c2
    | Lt(c1, c2)         -> ots "<" c1 c2
    | Leq(c1, c2)        -> ots "<=" c1 c2
    | Eq(c1, c2)         -> ots "==" c1 c2
 (* | And(c1, c2)        -> ots "AND" c1 c2 *)
    | IfThenElse0(c1,c2) -> "{ IF "^(pretty_print_calc c1)^" THEN "^(pretty_print_calc c2)^" }"
    | Const(c)           -> string_of_const(c)
    | Var(x)             -> x

and pretty_print_stmt (mapacc, delta_term) : string =
  (pretty_print_map_access mapacc)^" += "^(pretty_print_calc delta_term)

and pretty_print_trig (pm, rel_id, var_id, statements) : string =
  "ON " ^
  (match pm with Insert -> "+" | Delete -> "-") ^
  rel_id ^(vars_to_string var_id)^ ": " ^
  (List.fold_left (fun oldstr stmt ->
    oldstr^"\n"^(pretty_print_stmt stmt)
  ) " " statements)^"\n"
  
and pretty_print_type_list (l:var_type_t list) : string = 
  (Util.list_to_string string_of_var_type l)

and pretty_print_map (mapn, input_var_types, output_var_types) : string =
  mapn ^ " " ^ (pretty_print_type_list input_var_types) ^(pretty_print_type_list output_var_types)^"\n"

and pretty_print_prog ((maps, trigs):prog_t) = 
  (List.fold_left (fun oldstr map -> oldstr^(pretty_print_map map)) "" maps)^
  (List.fold_left (fun oldstr trig -> oldstr^(pretty_print_trig trig)) "" trigs);;

let rename_maps (mapping:(M3.map_id_t) StringMap.t)
                ((stmt_defn, stmt_calc):M3.stmt_t): M3.stmt_t =
  let rec sub_calc calc : M3.calc_t =
    match calc with
    | MapAccess(ma)      -> MapAccess(sub_ma ma)
    | Add(c1, c2)        ->         Add((sub_calc c1), (sub_calc c2))
    | Mult(c1, c2)       ->        Mult((sub_calc c1), (sub_calc c2))
    | Lt(c1, c2)         ->          Lt((sub_calc c1), (sub_calc c2))
    | Leq(c1, c2)        ->         Leq((sub_calc c1), (sub_calc c2))
    | Eq(c1, c2)         ->          Eq((sub_calc c1), (sub_calc c2))
    | IfThenElse0(c1,c2) -> IfThenElse0((sub_calc c1), (sub_calc c2))
    | Const(c)           -> Const(c)
    | Var(x)             -> Var(x)
  and sub_ma (mapacc_name, ma_ivars, ma_ovars, ma_icalc) : M3.mapacc_t =
    if StringMap.mem mapacc_name mapping then
      (StringMap.find mapacc_name mapping, 
       ma_ivars, ma_ovars, sub_calc ma_icalc)
    else
      (mapacc_name, ma_ivars, ma_ovars, sub_calc ma_icalc)
  in
    (sub_ma stmt_defn, sub_calc stmt_calc)

let rename_vars (src_vars:M3.var_t list) (dst_vars:M3.var_t list) 
                ((stmt_defn, stmt_calc):M3.stmt_t): M3.stmt_t =
  (* make sure the dst_vars are safe *)
  let rec check_safe_var var calc =
    match calc with 
    | MapAccess(ma)      -> check_mapacc var ma
    | Add(c1, c2)        -> (check_safe_var var c1) && (check_safe_var var c2)
    | Mult(c1, c2)       -> (check_safe_var var c1) && (check_safe_var var c2)
    | Lt(c1, c2)         -> (check_safe_var var c1) && (check_safe_var var c2)
    | Leq(c1, c2)        -> (check_safe_var var c1) && (check_safe_var var c2)
    | Eq(c1, c2)         -> (check_safe_var var c1) && (check_safe_var var c2)
    | IfThenElse0(c1,c2) -> (check_safe_var var c1) && (check_safe_var var c2)
    | Const(c)           -> true
    | Var(x)             -> x <> var
  and check_mapacc var (_,ivars,ovars,icalc) = 
    if
      (List.fold_right (fun x old -> (x == var)||(old)) ivars false) ||
      (List.fold_right (fun x old -> (x == var)||(old)) ovars false) then
      false
    else
      check_safe_var var icalc
  in
  let rec check_var_list list replacements in_mapping =
    let (todos, mapping) = 
      List.fold_left2 (fun (todos,mapping) var replacement -> 
        (
          if var == replacement then (todos, mapping)
          else
            (if (check_mapacc replacement stmt_defn) &&
               (check_safe_var replacement stmt_calc) then todos
                                                      else replacement::todos),
            StringMap.add var replacement mapping
        )
      ) ([], in_mapping) list replacements
    in
      if (List.length todos) > 1 then
        check_var_list todos (List.map (fun x -> x^"_p") todos) in_mapping
      else
        mapping
  in
  let mapping = check_var_list src_vars dst_vars StringMap.empty in
  let map_var var = 
    if StringMap.mem var mapping then StringMap.find var mapping
                                 else var
  in
  let rec sub_stmt calc : M3.calc_t = 
    match calc with 
    | MapAccess(ma)      -> MapAccess(sub_ma ma)
    | Add(c1, c2)        ->         Add((sub_stmt c1), (sub_stmt c2))
    | Mult(c1, c2)       ->        Mult((sub_stmt c1), (sub_stmt c2))
    | Lt(c1, c2)         ->          Lt((sub_stmt c1), (sub_stmt c2))
    | Leq(c1, c2)        ->         Leq((sub_stmt c1), (sub_stmt c2))
    | Eq(c1, c2)         ->          Eq((sub_stmt c1), (sub_stmt c2))
    | IfThenElse0(c1,c2) -> IfThenElse0((sub_stmt c1), (sub_stmt c2))
    | Const(c)           -> Const(c)
    | Var(x)             -> Var(map_var x)
  and sub_ma (name, ivars, ovars, icalc) : M3.mapacc_t = 
    (name, List.map map_var ivars, List.map map_var ovars, sub_stmt icalc)
  in
    (sub_ma stmt_defn, sub_stmt stmt_calc)
    

let rec c_sum (a : const_t) (b : const_t) : const_t = 
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

let rec c_prod (a : const_t) (b : const_t) : const_t =
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
    

(* Prepared statement helpers *)
module M3P = M3.Prepared

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

let rec pcalc_to_string calc =
   let ots op e1 e2 =
      op^"("^(pcalc_to_string (get_calc e1))^
      ", "^(pcalc_to_string (get_calc e2))^")" in
   match calc with
      M3P.MapAccess(mapn, inv, outv, init_aggecalc) ->
         "MapAccess("^mapn^", "^(vars_to_string inv)^", "^(vars_to_string outv)^
         ", "^(pcalc_to_string (get_calc (get_ecalc init_aggecalc)))^")"
    | M3P.Add (e1, e2)        -> ots "Add"  e1 e2
    | M3P.Mult(e1, e2)        -> ots "Mult" e1 e2
    | M3P.Lt  (e1, e2)        -> ots "Lt"   e1 e2
    | M3P.Leq (e1, e2)        -> ots "Leq"  e1 e2
    | M3P.Eq  (e1, e2)        -> ots "Eq"   e1 e2
(*    | M3P.And (e1, e2)        -> ots "And"  e1 e2 *)
    | M3P.IfThenElse0(e1, e2) -> ots "IfThenElse0" e1 e2
    | M3P.Const(i)            -> string_of_const i
    | M3P.Var(x)              -> x

let rec pcalc_schema (calc : M3P.pcalc_t) =
   let op c1 c2 = Util.ListAsSet.union
      (pcalc_schema (get_calc c1)) (pcalc_schema (get_calc c2)) in
   match calc with
      M3P.MapAccess(mapn, inv, outv, init_ecalc) -> outv
    | M3P.Add (c1, c2)        -> op c1 c2
    | M3P.Mult(c1, c2)        -> op c1 c2
    | M3P.Lt  (c1, c2)        -> op c1 c2
    | M3P.Leq (c1, c2)        -> op c1 c2
    | M3P.Eq  (c1, c2)        -> op c1 c2
(*    | M3P.And (c1, c2)        -> op c1 c2 *)
    | M3P.IfThenElse0(c1, c2) -> op c2 c1
    | M3P.Const(i)            -> []
    | M3P.Var(x)              -> [x]


(* Patterns *)
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
