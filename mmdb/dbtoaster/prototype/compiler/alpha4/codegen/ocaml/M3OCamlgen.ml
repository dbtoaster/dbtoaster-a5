(* M3 OCaml Generator
 * A backend that produces OCaml source code for M3 programs.
 * -- no eval_trigger method to interpret resulting source code.
 * -- uses M3OCaml, an OCaml library with standard data structures for maps,
 *    valuations (i.e. environments/scopes), sources and multiplexers,
 *    and databases.
 *
 * TODO: architectural overview of source code generated. This is similar to
 * the structure of the code used for interpreting.
 *)

open M3
open M3Common.Patterns
open M3OCaml

module M3P = M3.Prepared

module CG : M3Codegen.CG =
struct
   type op_t         = string
   type code_t       = Lines of string list | Inline of string
   type debug_code_t = string list

   let get_lines c = match c with Lines(l) -> l | Inline(c) -> [c]
   let inline s = "(List.hd "^s^")"

   (* Code prettification *)
   (*
   let level     = ref 0
   let nest ()   = incr level
   let unnest () = decr level
   let tab s     = "    "^s
   let indent s  =
      let rec aux n s = if n = 0 then s else aux (n-1) (tab s)
      in aux (!level) s
   let block l   = String.concat "\n" (List.map indent l)
   let tabify l = nest(); let r = block l; unnest() in r
   *)

   let tab s           = "   "^s
   let rec indent n s  = if n = 0 then s else indent (n-1) (List.map tab s)
   let tabify l        = Lines(List.map tab l)

   (* Static code generation *)
   let gen_var v = "var_"^v

   let list_to_string f l = "["^(String.concat ";" (List.map f l))^"]"

   let pm_const pm = match pm with | Insert -> "Insert" | Delete -> "Delete" 
   let pm_name pm  = match pm with | Insert -> "ins" | Delete -> "del" 

   let string_const x = 
        "\""^(Str.global_replace (Str.regexp "\"") "\\\"" x)^"\""
   let string_pair_const (x,y) = "("^(string_const x)^","^(string_const y)^")"
   let pattern_const p = list_to_string string_of_int p
   let patterns_const ps = list_to_string pattern_const ps

   let schema_const schema =
      let string_of_var_type_t vt =
         match vt with | VT_Int -> "VT_Int" | VT_String -> "VT_String" 
                       | VT_Float -> "VT_Float"
      in list_to_string (fun (mapn, inv_t, outv_t) ->
         "("^(string_const mapn)^", "^
            (list_to_string string_of_var_type_t inv_t)^", "^
            (list_to_string string_of_var_type_t outv_t)^")") schema

   let pattern_map_const pm =
      let aux (x,y) =
         (list_to_string string_const x)^","^(list_to_string string_of_int y)
      in let f p = match p with
       | In(x,y) -> "In("^aux(x,y)^")"
       | Out(x,y) -> "Out("^aux(x,y)^")"
      in
      list_to_string (fun (mapn, pats) ->
         "("^(string_const mapn)^", "^(list_to_string f pats)^")") pm

   let index l el =
      snd (List.fold_left (fun (f,c) x -> if not(f) then
         (if (x = el) then (true, c) else (f,c+1)) else (f,c))
         (false, 0) l)

   let indices n =
      let rec indices_aux n acc =
         if n = 0 then acc else indices_aux (n-1) ((n-1)::acc)
      in indices_aux n []

   let bind_vars_from_list_indices vi consts_list =
      List.map (fun (v,i) ->
         "let "^(gen_var v)^" = List.nth "^
         consts_list^" "^(string_of_int i)^" in ") vi

   let bind_vars_from_list vars consts_list =
      bind_vars_from_list_indices
         (List.combine vars (indices (List.length vars))) consts_list

   (* Binds vars given in an extension from "consts_list", according to positions
    * specified by "vars".
    * Note, this ignores any extensions that are not present in "vars", we
    * assume such extensions are already bound above. *)
   let bind_vars_from_extension vars consts_list ext =
      let ext_vars = Util.ListAsSet.inter vars ext in
      let ext_idx = List.map (index vars) ext_vars in
         bind_vars_from_list_indices (List.combine ext_vars ext_idx) consts_list

   let gen_list el = "["^(String.concat ";" el)^"]"
   let vars_list vars = gen_list (List.map gen_var vars)

   (* bindings: vars * const list  *)
   let inline_vars_list vars bindings =
      (* The last binding overrides others. *)
      let rbindings = List.rev bindings in
      let aux v =
         let valid = List.filter (fun (vl,cl_v) -> List.mem v vl) rbindings in
         if valid = [] then (gen_var v) else
            let (vl, cl_v) = List.hd valid in
               ("List.nth "^cl_v^" "^(string_of_int (index vl v)))
      in gen_list (List.map aux vars)

   (* Debugging helpers *)
   let annotate_code_schema msg schema =
     ["(* "^msg^": "^(M3Common.vars_to_string schema)^" *)"]

   (*
   let debug_sequence cdebug cresdebug ccalc =
      ["(*"]@cdebug@[";*)"]@ccalc@["(*]@cresdebug@[*)"]

   let debug_expr incr_calc = 
      ["print_string(\"\\neval_pcalc "^(pcalc_to_string incr_calc)^" \"^(Database.db_to_string db)^\"   \")"]

   (* TODO *)
   let debug_expr_result incr_calc ccalc = []

   let debug_singleton_rhs_expr lhs_outv =
      ["(fun k v ->";
       "print_endline (\"End of PCALC_S; outv=\"^\""^(vars_list lhs_outv)^"\"^";
       "               \" k=\"^(Util.list_to_string string_of_const k)^";
       "               \" v=\"^(string_of_const v)^";
       "              (\" db=\"^(Database.db_to_string db))))"]

   let debug_slice_rhs_expr rhs_outv =
      ["(fun slice0 ->";
       "print_endline (\"End of PCALC; outv=\"^\""^(vars_list rhs_outv)^"\"^";
       "               \" slice=\"^(Database.slice_to_string slice0)^";
       "              (\" db=\"^(Database.db_to_string db))))"]

   let debug_rhs_init () =
      ["(fun k v ->";
       "print_string (\"@PSTMT.init{\"";
       "             ^\"key=\"^(Util.list_to_string string_of_const k)";
       "             ^\", db=\"^(Database.db_to_string db)))"]

   let debug_stmt lhs_mapn lhs_inv lhs_outv =
      ["print_string(\"\\nPSTMT (db=_\"^";
       "            \", stmt=(("^(string_const lhs_mapn)^"\"^";
       "            \" "^(vars_list lhs_inv)^" \"^";
       "            \""^(vars_list lhs_outv)^" _), _))\\n\"))"]
   *)
   
   let debug_sequence cdebug cresdebug ccalc = ccalc
   let debug_expr incr_calc = []
   let debug_expr_result incr_calc ccalc = []
   let debug_singleton_rhs_expr lhs_outv = []
   let debug_slice_rhs_expr rhs_outv = []
   let debug_rhs_init () = []
   let debug_stmt lhs_mapn lhs_inv lhs_outv = []


   (* Code generation for M3 AST elements *)

   (* TODO: potential loss of precision with OCaml's string_of_float
    * without formatting. *)
   let const c = match c with | CFloat(f) ->
      Inline("[CFloat("^(string_of_float f)^")]")

   let singleton_var v = Inline("["^(gen_var v)^"]")
   let slice_var v = Lines(["ValuationMap.from_list [(["^
                              (gen_var v)^"], "^(gen_var v)^")] []"])
   
   let int_op op      = "(fun a b -> if ("^op^" a b) then CFloat(1.0) else CFloat(0.0))"
   let add_op         = "c_sum"
   (* Note: can't use prefix form due to ambiguity with comments. *)
   let mult_op        = "c_prod"
   let eq_op          = int_op "(=)"
   let lt_op          = int_op "(<)"
   let leq_op         = int_op "(<=)"
   let ifthenelse0_op =
      "(fun v cond -> match cond with"^
      " | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0))"

   let op_singleton_expr op ce1 ce2 =
      let aux s in_s = tabify (
         ["let r = ";]@s@
         ["in match r with";
          " | [] -> []";
          " | [v] -> [("^op^" v "^(inline in_s)^")]";
          " | _ -> failwith \"op_singleton_expr: invalid singleton\"" ])
      in
      match (ce1, ce2) with
       | (Lines(ce1_l), Lines(ce2_l)) -> tabify (
         ["let (r1,r2) = ((";]@ce1_l@["),("]@ce2_l@[")) in";
          "match (r1,r2) with";
          " | ([], _) | (_,[]) -> []";
          " | ([v1], [v2]) -> [("^op^" v1 v2)]";
          " | _ -> failwith \"op_singleton_expr: invalid singleton\"" ])

       | (Inline(ce1_i), Lines(ce2_l)) -> aux ce2_l ce1_i
       | (Lines(ce1_l), Inline(ce2_i)) -> aux ce1_l ce2_i
       | (Inline(ce1_i), Inline(ce2_i)) ->
          Inline("[("^op^" "^(inline ce1_i)^" "^(inline ce2_i)^")]")

   let op_slice_expr op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
      tabify (
      (annotate_code_schema "outv1" outv1)@
      (annotate_code_schema "theta_ext" theta_ext)@
      ["let res1 = "]@(get_lines ce1)@
      ["in";
       "let f k v1 r ="]@
       (indent 1 (annotate_code_schema "outv2" outv2))@
      ["   let r2 = "]@ 
      (indent 2 (bind_vars_from_extension outv1 "k" theta_ext))@
      (indent 1 (get_lines ce2))@
      ["   in";
       "   let r3 = ";
              (* Inlined AggregateMap.concat_keys *) 
       "      ValuationMap.mapi (fun k2 v2 -> "]@
      (* Bind outv2, and schema_ext. Note schema_ext corresponds to
       * vars in outv1 which can be accessed through const list "k" *)
      (let new_k = inline_vars_list schema [(outv2, "k2"); (schema_ext, "k")] in
      (indent 2 (annotate_code_schema "schema" schema))@
      (indent 2 (annotate_code_schema "outv2" outv2))@
      (indent 2 (annotate_code_schema "schema_ext" schema_ext))@
      ["         ("^new_k^", ("^op^" v1 v2))"])@
      ["      ) r2";
       "   in ValuationMap.union r r3";
       "in ValuationMap.fold f (ValuationMap.empty_map()) res1"])
       
   let op_slice_product_expr op ce1 ce2 = tabify (
      ["let res1 = "]@(get_lines ce1)@
      ["in";
       "let res2 = "]@(get_lines ce2)@
      ["in ValuationMap.product "^op^" res1 res2"])

   (* Note: no need to bind outv2 anywhere in this code, since in this case
    * outv2 is bound from above *)
   let op_lslice_expr op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
      tabify (
      let f_body =
         match ce2 with
          | Lines(ce2_l) ->
            ["   let res2 = "]@
             (indent 2 (bind_vars_from_extension outv1 "k" theta_ext))@
             (indent 1 ce2_l)@
            ["   in begin match res2 with";
             "    | [] -> r";
             "    | [v2] ->";
             "       let nv = ("^op^" v v2) in"]@
            (let nk = inline_vars_list schema [(schema_ext, "k")] in
            ["       let nk = "^nk])@
            ["       in ValuationMap.add nk nv r";
             "    | _ -> failwith \"op_lslice_expr: invalid singleton\"";
             "   end"]
          | Inline(ce2_i) ->
            (let nk = inline_vars_list schema [(schema_ext, "k")] in
            ["   let nk = "^nk])@
             (* schema_ext will contain superset of theta_ext so we dont
              * need to extend the scope with both.
              * TODO: check this *)
(*              
             ***(indent 1 (bind_vars_from_extension outv1 "k" theta_ext))@***
             (indent 1 (bind_vars_from_list schema_ext "k"))@
            ["    let nk = "^(vars_list schema)]@
*)
            ["    in ValuationMap.add nk ("^op^" v "^(inline ce2_i)^") r"]
      in
         ["let res1 = "]@(get_lines ce1)@
         ["in";
          "let f k v r = ";]@f_body@
         ["in ValuationMap.fold f (ValuationMap.empty_map()) res1"])
   
   (* Note: no need to bind any vars from outv2 since these should be bound
    * from above *)
   let op_lslice_product_expr op outv2 ce1 ce2 = tabify (
      let body = match ce2 with
         | Lines(ce2_l) ->
            ["let res2 = "]@ce2_l@
            ["in begin match res2 with";
             "    | [] ->  ValuationMap.empty_map()";
             "    | [v2] -> ValuationMap.mapi (fun k v -> (k@k2, ("^op^" v v2))) res1";
             "    | _ -> failwith \"op_lslice_product_expr: invalid singleton\"";
             "   end"]
         | Inline(ce2_i) ->
            ["ValuationMap.mapi (fun k v -> (k@k2, ("^op^" v "^(inline ce2_i)^"))) res1"]
      in
      ["let res1 = "]@(get_lines ce1)@
      ["in";
       "let k2 = "^(vars_list outv2)^" in"]@body)

   (* Note: no need to bind vars schema_ext since these should be bound from
    * above. *)
   let op_rslice_expr op outv2 schema schema_ext ce1 ce2 = tabify (
      let body ce1_v =
         ["let r = "]@
          (get_lines ce2)@
         ["in ValuationMap.mapi (fun k2 v2 ->"]@
         (let nk = inline_vars_list schema [(outv2, "k2")] in
         ["   ("^nk^", ("^op^" "^ce1_v^" v2))) r"])
      in 
      match ce1 with
       | Lines(ce1_l) ->
         ["let res1 = "]@ce1_l@
         ["in";
          "   match res1 with";
          "    | [] -> ValuationMap.empty_map()";
          "    | [v] ->"]@
         (indent 2 (body "v"))@
         ["   | _ -> failwith \"op_rslice_expr: invalid singleton\""]
       | Inline(ce1_i) -> (body (inline ce1_i)))

   let singleton_init_lookup mapn inv out_patterns outv cinit =
      let cmapn = string_const mapn in
      let cout_patterns = patterns_const out_patterns in
      if inv = [] then
         let error =
            "singleton_init_lookup: invalid init value for a "^
            "lookup with no in vars."
         in tabify(["failwith \""^error^"\""])
      else
      let body v = 
         ["(Database.update_value "^cmapn^" "^cout_patterns^" "^(vars_list inv)^" "^(vars_list outv)^" "^v^" db;";
          "   ValuationMap.from_list [("^(vars_list outv)^", "^v^")] "^cout_patterns^")"]
      in
      tabify (match cinit with
         | Lines(cinit_l) ->
            ["let init_val = "]@cinit_l@
            ["in begin match init_val with";
             " | [] -> ValuationMap.empty_map()";
             " | [v] -> "]@
             (indent 2 (body "v"))@
            [" | _ -> failwith \"MapAccess: invalid singleton\""; "end"]
         | Inline(cinit_i) -> (body (inline cinit_i)))
       
   let slice_init_lookup mapn inv out_patterns cinit =
      let cmapn = string_const mapn in
      let cout_patterns = patterns_const out_patterns in
      if inv = [] then
         let error =
            "singleton_init_lookup: invalid init value for a lookup with no in vars."
         in tabify(["failwith \""^error^"\""])
      else
      tabify (
      ["let init_slice = "]@(get_lines cinit)@
      ["in";
       "let init_slice_w_indexes = List.fold_left";
       "   ValuationMap.add_secondary_index init_slice "^cout_patterns;
       "in (Database.update "^cmapn^" "^(vars_list inv)^" init_slice_w_indexes db;";
       "    init_slice_w_indexes)"])

   let lookup_aux mapn inv init_val_code =
      let cmapn = string_const mapn in
      ["let slice =";
       "   let m = Database.get_map "^cmapn^" db in";
       "   let inv_img = "^(vars_list inv)^" in";
       "      if ValuationMap.mem inv_img m then ValuationMap.find inv_img m";
       "      else "]@
       (indent 2 (get_lines init_val_code))

   let singleton_lookup mapn inv outv init_val_code = tabify (
      (lookup_aux mapn inv init_val_code)@
      ["in";
       "let outv_img = "^(vars_list outv)^" in";
       "let lookup_slice =";
       "   if ValuationMap.mem outv_img slice then slice";
       "   else"]@
       (indent 2 (get_lines init_val_code))@
      ["in [ValuationMap.find outv_img lookup_slice]"])

   let slice_lookup mapn inv pat patv init_val_code = tabify (
      (lookup_aux mapn inv init_val_code)@
      (if patv = [] then
      ["in ValuationMap.strip_indexes slice"]
      else
      ["in";
       "let pkey = "^(vars_list patv)^" in";
       "let lookup_slice = ValuationMap.slice "^(pattern_const pat)^" pkey slice in";
       "   (ValuationMap.strip_indexes lookup_slice)"]))

   (* TODO: add debugging to inlined code *)
   let singleton_expr ccalc cdebug =
      match ccalc with
       | Lines(ccalc_l) -> tabify (
         ["let r = "]@ccalc_l@
         ["in";
          "match r with";
          " | [] -> []";
          " | [v] -> (* "]@(indent 2 cdebug)@[" [] v *) [v]";
          " | _ -> failwith \"singleton_expr: invalid singleton\""])
       | Inline(ccalc_i) -> Inline(ccalc_i)

   let direct_slice_expr ccalc cdebug = tabify (
      ["let r = "]@(get_lines ccalc)@
      ["in"; "(* "]@cdebug@[" r; *)"; "r"])

   let full_agg_slice_expr ccalc cdebug = tabify (
      ["let slice0 = "]@(get_lines ccalc)@
      ["in"; "(* "]@cdebug@[" slice0; *)";
       "[ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]"])

   (* Note: no need to bind vars in rhs_ext, these come from above. *)
   let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
      let cpat = pattern_const rhs_pattern in
      tabify (
      ["let slice0 = "]@(get_lines ccalc)@
      ["in"; "(* "]@cdebug@[" slice0; *)";
       "let slice1 = ValuationMap.add_secondary_index slice0 "^cpat^" in";
          (* Inlined version of AggregateMap.project_keys *)
       "   ValuationMap.fold_index (fun pk kv nm ->";
       "      let (nk,nv) ="; 
       "         let aggv = List.fold_left (fun x (y,z) -> c_sum x z) (CFloat(0.0)) kv in "]@
      (indent 3 (bind_vars_from_list rhs_projection "pk"))@
      ["         let new_k = "^(vars_list lhs_outv)^" in (new_k, aggv)";
       "      in ValuationMap.add nk nv nm)"; 
       "   "^cpat^" slice1 (ValuationMap.empty_map())"])

   (* TODO: add debugging to inlined code *)
   (* returns a function with sig:
    *   AggregateMap.agg_t list -> AggregateMap.agg_t *)
   let singleton_init cinit cdebug =
      match cinit with
       | Lines(cinit_l) -> tabify (
         ["(fun _ v ->";
          "(* "]@cdebug@[" [] v; *)";
          "let iv = "]@cinit_l@
         ["in";
          "match iv with";
          " | [] -> v";
          " | [init_v] -> c_sum v init_v";
          " | _ -> failwith \"singleton_init: invalid singleton\")"])
 
       | Inline(cinit_i) -> Inline("(fun _ v -> c_sum v "^(inline cinit_i)^")")

   (* returns a function with sig:
    *   AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t *)
   let slice_init lhs_outv init_ext cinit cdebug = tabify (
      ["(fun k v ->";
       "(* "]@cdebug@[" k v; *)";
       "let init_slice = "]@
       (indent 1 (bind_vars_from_list lhs_outv "k"))@
       (get_lines cinit)@
      ["in";
       "let init_v =";
       "   if ValuationMap.mem k init_slice";
       "   then (ValuationMap.find k init_slice) else CFloat(0.0)";
       "in c_sum v init_v)"])

   (* assume cinit is a function with sig:
    *   val cinit: AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t *)
   let singleton_update lhs_outv cincr cinit cdebug =
      let iv_code v = match cinit with
         | Lines(cinit_l) -> 
            ["let delta_k = "^(vars_list lhs_outv)^" in";
             "let iv = "]@
             (indent 1 cinit_l)@
            ["  delta_k "^v;
             "in [iv]"]
         | Inline(cinit_i) ->
            (* Note no need to call inline function, since cinit_i is a function,
             * not a list *)
            ["["^cinit_i^" "^(vars_list lhs_outv)^" "^v^"]"]
      in
      tabify (match cincr with 
       | Lines(cincr_l) ->
         ["(fun current_singleton ->";
          "(* "]@cdebug@["; *)";
          "let delta_slice = "]@cincr_l@
         ["in match (current_singleton, delta_slice) with";
          "   | (s, []) -> s";
          "   | ([], [delta_v]) ->"]@(indent 2 (iv_code "delta_v"))@
         ["   | ([current_v], [delta_v]) -> [c_sum current_v delta_v]";
          "   | _ -> failwith \"singleton_update: invalid singleton\")"]
       
       | Inline(cincr_i) ->
         ["(fun current_singleton ->";
          "(* "]@cdebug@["; *)";
          "match current_singleton with";
          " | [] -> "]@(indent 2 (iv_code (inline cincr_i)))@
         [" | [current_v] -> [c_sum current_v "^(inline cincr_i)^"]";
          " | _ -> failwith \"singleton_update: invalid singleton\")"])

   (* assume cinit is a function with sig:
    *   val cinit: AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t *)
   let slice_update cincr cinit cdebug = tabify (
      match cinit with
       | Lines(cinit_l) ->
         ["(fun current_slice ->";
          "(* "]@cdebug@["; *)";
          "let delta_slice = "]@(get_lines cincr)@[" in";
          "   ValuationMap.merge_rk (fun k v -> v)"]@
          (indent 1 cinit_l)@
         ["      (fun k v1 v2 -> c_sum v1 v2) current_slice delta_slice)"]
       | Inline(cinit_i) ->
         ["(fun current_slice ->";
          "(* "]@cdebug@["; *)";
          "let delta_slice = "]@(get_lines cincr)@[" in";
          "   ValuationMap.merge_rk (fun k v -> v)"]@
          (indent 1 [cinit_i])@
         ["      (fun k v1 v2 -> c_sum v1 v2) current_slice delta_slice)"])

   (* assume cstmt is a function with sig:
    *   val cstmt: AggregateMap.agg_t list -> AggregateMap.agg_t list *)
   let db_singleton_update lhs_mapn lhs_outv map_out_patterns cstmt = 
      let cmapn = string_const lhs_mapn in
      let cpatterns = patterns_const map_out_patterns in
      tabify (
      ["(fun inv_img in_slice ->";
       "let outv_img = "^(vars_list lhs_outv)^" in";
       "let singleton =";
       "   if ValuationMap.mem outv_img in_slice";
       "   then [ValuationMap.find outv_img in_slice] else [] in";
       "let new_value = "]@(get_lines cstmt)@
      ["   singleton";
       "in";
       "match new_value with";
       " | [] -> ()";
       " | [v] -> Database.update_value "^cmapn^" "^cpatterns^" inv_img outv_img v db";
       " | _ -> failwith \"db_singleton_update: invalid singleton\")"])

   (* assume cstmt is a function with sig:
    *    val cstmt: AggregateMap.t -> AggregateMap.t *) 
   let db_slice_update lhs_mapn cstmt = 
      let cmapn = string_const lhs_mapn in tabify (
      ["(fun inv_img in_slice ->";
       "let new_slice = "]@(get_lines cstmt)@
      ["   in_slice";
       "in";
       "   Database.update "^cmapn^" inv_img new_slice db)"])

   let statement lhs_mapn lhs_inv lhs_ext patv pat direct db_update_code = 
      let cmapn = string_const lhs_mapn in
      let cpat = pattern_const pat in tabify (
      ["(*** Update "^lhs_mapn^" ***)";
       "let lhs_map = Database.get_map "^cmapn^" db in";
       "let iifilter inv_img = "]@
       (indent 1 (bind_vars_from_extension lhs_inv "inv_img" lhs_ext))@
      ["   let slice = ValuationMap.find inv_img lhs_map in";
       "   let db_f = "]@
       (indent 1 (get_lines db_update_code))@
      ["   in db_f inv_img slice"]@
      (* Note slice_keys does a full scan for an empty pattern, is the map is
       * non-empty *)
      ["in";
       "let pkey = "^(vars_list patv)^" in";
       "let inv_imgs = "^
          (if direct then "[pkey]"
           else ("ValuationMap.slice_keys "^cpat^" pkey lhs_map"));
       "in List.iter iifilter inv_imgs;"])
   
   let trigger event rel trig_args stmt_block =
      let trigger_name = "on_"^(pm_name event)^"_"^rel in Lines (
      ["let "^trigger_name^" = (fun tuple -> "]@
       (indent 1 (bind_vars_from_list trig_args "tuple"))@
       (List.flatten (List.map get_lines stmt_block))@
      [");;"])

   (* Sources *)
   (* TODO: naming conventions for:
    * -- FileSource names in declaration from source_impl_t
    *    ++ val get_source_instance: source_impl_t -> string
    * -- source_impl_t = string * (rel_id_t list)
    *                    source inst name, adaptor rel list *)
   type source_impl_t = string * (string * string) list
   
   let addr_const addr port = 
     (string_const (Unix.string_of_inet_addr addr))^", "^(string_of_int port)
   
   let source_const src = match src with
      | FileSource(fn) -> "(FileSource("^(string_const fn)^"))"
      | SocketSource(addr,port) -> "(SocketSource("^(addr_const addr port)^"))"
      | PipeSource(p) -> "(PipeSource("^(string_const p)^"))"

   let framing_const fr = match fr with
      | FixedSize(l)    -> "(FixedSize("^(string_of_int l)^"))"
      | Delimited("\n") ->  "(Delimited(\"\\n\"))"
      | Delimited(s)    -> "(Delimited("^(string_const s)^"))"
      | VarSize(s,e)    -> "(VarSize("^(string_of_int s)^","^(string_of_int e)^"))"

   let src_counter = ref 0
   let adaptor_counter = ref 0

   let gen_source_name() =
      let r = "src"^(string_of_int !src_counter) in incr src_counter; r
   
   let gen_adaptor_name() =
      let r = "adaptor"^(string_of_int !adaptor_counter)
      in incr adaptor_counter; r

   let get_source_instance impl = fst impl
   let get_source_adaptor_instances impl = List.map fst (snd impl)

   (* Source geneation:
    * -- code to declare sources:
    *    ++ adaptor creation, naming adaptors according to relations
    *       adaptors will be embedded into the main method
    *    ++ FileSource creation with framing_t and adaptor instances
    * -- code to initialize sources
    *    ++ none for now w/ file sources
    *)
   (* TODO:
    * -- random sources *)
   let source src fr rel_adaptors =
      let source_name = gen_source_name () in
      let adaptor_meta = List.map
         (fun (r,a) -> ((gen_adaptor_name(), r), a)) rel_adaptors in
      let s = (source_name, List.map fst adaptor_meta) in
      let framing_spec = framing_const fr in
      let adaptor_aux (name,params) =
         "   ("^(string_const name)^","^
         (list_to_string string_pair_const params)^")" in
      let adaptor_nl = List.map (fun ((n,r),a) -> 
         ((r,n), ["let "^n^" = Adaptors.create_adaptor "; (adaptor_aux a)^" in"]))
         adaptor_meta in
      let (adaptor_rels_names, adaptor_lines_l) = List.split adaptor_nl in
      let adaptor_lines = List.flatten adaptor_lines_l in
      let adaptor_vars = list_to_string
         (fun (x,y)->"("^(string_const x)^","^y^")") adaptor_rels_names in
      let make_file_source () = 
          let source_spec = source_const src in
          let decl_lines =
             ["let "^source_name^" = "^
               "FileSource.create "^source_spec^" "^framing_spec^" "^
                  adaptor_vars^" "^
                 (string_const 
                    (Util.list_to_string (fun (r,_) -> r) rel_adaptors))^
                  " in"]
          in (s, Some(Lines(adaptor_lines@decl_lines)), None)
      in
      match src with
       | FileSource _ -> make_file_source ()
       | PipeSource _ -> make_file_source ()
       | _ -> failwith "Unsupported data source"

   (* TODO:
    * -- use integers rather than strings for stream dispatching for efficiency
    *    (requires changing stream_event type, and source creation)
    * -- generate a dummy benchmarker: random source, multiplexed, each w/ fixed # of tuples *)
   let main schema patterns sources triggers toplevel_queries = Lines (
      let sources_lines = List.flatten (List.map (fun (impl,decl,init) ->
         let aux c = match c with
            | None -> [] | Some(Lines(x)) -> x | Some(Inline(x)) -> [x]
         in List.flatten (List.map aux [decl; init])) sources) in
      let multiplexer_lines =
         ["let mux = "]@
         ["  let mux = FileMultiplexer.create() in" ]@
         (List.map (fun (impl,_,_) ->
            let inst_name = get_source_instance impl in
              "    let mux = FileMultiplexer.add_stream mux "^inst_name^" in"
         ) sources)@["  ref mux";"in"] in
      let dispatch_aux evt r =
         "| Some("^(pm_const evt)^","^(string_const r)^", t) -> "^
            "on_"^(pm_name evt)^"_"^r^" t" in
      let dispatch_lines = List.flatten (List.map (fun (impl,_,_) ->
         List.flatten (List.map (fun (_,r) ->
            [dispatch_aux Insert r(*; dispatch_aux Delete r*)]) (snd impl))) sources)
      in
      let query_aux (f:(string -> string list)) = 
        List.flatten (List.map f toplevel_queries)
      in
      let main_lines =
      ["let main() = "]@
      ["let arguments = (ParseArgs.parse (ParseArgs.compile";
       "  [([\"-v\"],(\"VERBOSE\",ParseArgs.NO_ARG),\"\",\"Show all updates\")";
       "  ])) in";
       "let log_evt = if ParseArgs.flag_bool arguments \"VERBOSE\" then";
       "    (fun evt -> match evt with None -> () | Some(pm,rel,t) -> ";
       "      print_endline (M3OCaml.string_of_evt pm rel t))";
       "  else (fun evt -> ()) in";
       "let log_results = if ParseArgs.flag_bool arguments \"VERBOSE\" then";
       "    (fun () -> "]@
       (query_aux (fun q -> [
       "      print_endline (\""^q^": \"^(";
       "        Database.dbmap_to_string (";
       "          Database.get_map \""^q^"\" db";
       "      )))"
       ]))@[
       "    )";
       "  else (fun () -> ()) in"]@
       (indent 1 sources_lines)@(indent 1 multiplexer_lines)@
      ["   let start = Unix.gettimeofday() in";
       "   while FileMultiplexer.has_next !mux do";
       "   let (new_mux,evt) = FileMultiplexer.next !mux in";
       "    ( log_evt evt;";
       "      mux := new_mux;";
       "      (match evt with "]@
       (indent 2 dispatch_lines)@
      ["      | None -> ()";
       "      | Some(Insert,rel,t) -> (print_string (\"Unhandled Insert: \"^rel^\"\\n\"))";
       "      | Some(Delete,rel,t) -> (print_string (\"Unhandled Delete: \"^rel^\"\\n\"))";
       "      );";
       "      log_results ()";
       "    )";
       "   done;";
       "   let finish = Unix.gettimeofday() in";
       "   print_endline (\"Tuples: \"^(string_of_float (finish -. start)));"]@
       (query_aux (fun q -> [
       "   print_endline (\""^q^": \"^(";
       "     Database.dbmap_to_string (";
       "       Database.get_map \""^q^"\" db";
       "   )))"
       ]))@[
       "in main();;"]
      in
      ["open M3;;";
       "open M3Common;;";
       "open M3Common.Patterns;;";
       "open M3OCaml;;\n";
       "open StandardAdaptors;;";
       "open Util;;";
       "StandardAdaptors.initialize();;";
       "let db = Database.make_empty_db ";
       "   "^(schema_const schema);
       "   "^(pattern_map_const patterns)^";;\n\n"]@
       (List.flatten (List.map get_lines triggers))@
       main_lines)

   let output code out_chan =
      output_string out_chan (String.concat "\n" (get_lines code));
      flush out_chan

   (* No direct evaluation of OCaml source code. *)
   type db_t = Database.db_t
   let eval_trigger trigger tuple db =
      failwith "Cannot directly evaluate OCaml source"

end