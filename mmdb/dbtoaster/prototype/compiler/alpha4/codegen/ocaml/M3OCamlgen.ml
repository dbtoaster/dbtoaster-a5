open M3Common
open M3Common.Patterns
open M3OCaml

module CG : M3Codegen.CG =
struct
   type op_t         = string
   type code_t       = string list
   type debug_code_t = string list

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
   let tabify l        = List.map tab l

   (* Static code generation *)
   let list_to_string f l = "["^(String.concat ";" (List.map f l))^"]"

   let string_const x = "\""^x^"\"" 
   let pattern_const p = list_to_string string_of_int p
   let patterns_const ps = list_to_string pattern_const ps

   let schema_const schema =
      let string_of_var_type_t vt =
         match vt with | VT_Int -> "VT_Int" | VT_String -> "VT_String"
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
         "let "^v^" = List.nth "^consts_list^" "^(string_of_int i)^" in ") vi

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

   let vars_list vars = "["^(String.concat "," vars)^"]"

   (* Debugging helpers *)
   let debug_sequence cdebug ccalc = ["(*"]@cdebug@[";*)"]@ccalc
   let debug_expr incr_calc = 
      ["print_string(\"\\neval_pcalc \""^(pcalc_to_string incr_calc)^
                   "\" \"^(Database.db_to_string db)^\"   \")"]

   (* lhs_outv *)
   let debug_singleton_rhs_expr lhs_outv =
      ["(fun k v ->";
       "print_endline (\"End of PCALC_S; outv=\"^\""^(vars_list lhs_outv)^"\"^";
       "               \" k=\"^(Util.list_to_string string_of_const k)^";
       "               \" v=\"^(string_of_const v)^";
       "              (\" db=\"^(Database.db_to_string db))))"]

   (* rhs_outv *)
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

   (* lhs_mapn, lhs_inv, lhs_outv *)
   let debug_stmt lhs_mapn lhs_inv lhs_outv =
      ["print_string(\"\\nPSTMT (db=_\"^";
       "            \", stmt=(("^(string_const lhs_mapn)^"\"^";
       "            \" "^(vars_list lhs_inv)^" \"^";
       "            \""^(vars_list lhs_outv)^" _), _))\\n\"))"]


   (* Code generation for M3 AST elements *)

   (* TODO: potential loss of precision with OCaml's string_of_float
    * without formatting. *)
   let const c = match c with | CFloat(f) -> ["[CFloat("^(string_of_float f)^")]"]

   let singleton_var v = ["["^v^"]"]
   let slice_var v = ["Valuation.from_list [(["^v^"], "^v^")] []"]
   
   let null() = ["[]"]
   
   let int_op op      = "(fun a b -> if ("^op^" a b) then 1.0 else 0.0)"
   let add_op         = "c_sum"
   (* Note: can't use prefix form due to ambiguity with comments. *)
   let mult_op        = "c_mult"
   let eq_op          = int_op "(=)"
   let lt_op          = int_op "(<)"
   let leq_op         = int_op "(<=)"
   let ifthenelse0_op = "(fun v cond -> if cond <> 0 then v else 0.0)"

   let op_singleton_expr op ce1 ce2 = tabify (
      ["let (r1,r2) = (";]@ce1@[","]@ce2@[") in";
       "match (r1,r2) with";
       " | ([], _) | (_,[]) -> []";
       " | ([v1], [v2]) -> ("^op^" v1 v2)";
       " | _ -> failwith \"op_singleton_expr: invalid singleton\"" ])

   let op_slice_expr op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
      tabify (
      ["let res1 = "]@ce1@
      ["in";
       "let f k v1 r =";
       "   let r2 = "]@ 
      (indent 2 (bind_vars_from_extension outv1 "k" theta_ext))@
      (indent 1 ce2)@
      ["in";
       "   let r3 = ";
              (* Inlined AggregateMap.concat_keys *) 
       "      ValuationMap.mapi (fun k2 v2 -> "]@
      (* Bind outv2, and schema_ext. Note schema_ext corresponds to
       * vars in outv1 which can be accessed through const list "k" *)
      (indent 2 (bind_vars_from_list outv2 "k2"))@
      (indent 2 (bind_vars_from_list schema_ext "k"))@
      ["      let new_k = "^(vars_list schema)^" in";
       "         (new_k, ("^op^" v1 v2))) r2";
       "   in ValuationMap.union r r3";
       "in ValuationMap.fold f (ValuationMap.empty_map()) res1"])
       
   let op_slice_product_expr op ce1 ce2 = tabify (
      ["let res1 = "]@ce1@
      ["in";
       "let res2 = "]@ce2@
      ["in ValuationMap.product "^op^" res1 res2"])

   (* Note: no need to bind outv2 anywhere in this code, since in this case
    * outv2 is bound from above *)
   let op_lslice_expr op outv1 outv2 schema theta_ext schema_ext ce1 ce2 =
      tabify (
      ["let res1 = "]@ce1@
      ["in";
       "let f k v r = ";
       "   let res2 = "]@
       (indent 2 (bind_vars_from_extension outv1 "k" theta_ext))@
       (indent 1 ce2)@
      ["in";
       "   begin match res2 with";
       "    | [] -> r";
       "    | [v2] ->";
       "       let nv = ("^op^" v v2) in";
       "       let nk = "]@
       (indent 3 (bind_vars_from_list schema_ext "k"))@
      ["          "^(vars_list schema);
       "       in ValuationMap.add nk nv r";
       "    | _ -> failwith \"op_lslice_expr: invalid singleton\"";
       "   end";
       "in ValuationMap.fold f (ValuationMap.empty_map()) res1"])
   
   (* Note: no need to bind any vars from outv2 since these should be bound
    * from above *)
   let op_lslice_product_expr op outv2 ce1 ce2 = tabify (
      ["let res1 = "]@ce1@
      ["in";
       "let k2 = "^(vars_list outv2)^" in";
       "let res2 = "]@ce2@
      ["in";
       "   begin match res2 with";
       "    | [] ->  ValuationMap.empty_map()";
       "    | [v2] -> ValuationMap.mapi (fun k v -> (k@k2, ("^op^" v v2))) res1";
       "    | _ -> failwith \"op_lslice_product_expr: invalid singleton\"";
       "   end"])

   (* Note: no need to bind vars schema_ext since these should be bound from
    * above. *)
   let op_rslice_expr op outv2 schema schema_ext ce1 ce2 = tabify (
      ["let res1 = "]@ce1@
      ["in";
       "  match res1 with";
       "   | [] -> ValuationMap.empty_map()";
       "   | [v] ->";
       "      let r = "]@
       (indent 2 ce2)@
      ["      in ValuationMap.mapi (fun k2 v2 ->";
       "         let new_k = "]@
       (indent 3 (bind_vars_from_list outv2 "k2"))@
      ["         "^(vars_list schema);
       "         in (new_k, ("^op^" v v2))) r";
       "   | _ -> failwith \"op_rslice_expr: invalid singleton\""])

   let singleton_init_lookup mapn inv out_patterns outv cinit =
      let cmapn = string_const mapn in
      let cout_patterns = patterns_const out_patterns in
      tabify (
      ["let inv_img = "^(vars_list inv)^" in";
       "let init_val = "]@cinit@
      ["in";
       "let outv_img = "^(vars_list outv)^" in";
       "begin match init_val with";
       " | [] -> ValuationMap.empty_map()";
       " | [v] -> (Database.update_value "^cmapn;
       "            "^cout_patterns;
       "            inv_img outv_img v db;";
       "           ValuationMap.from_list [(outv_img, v)] "^cout_patterns^")";
       " | _ -> failwith \"MapAccess: invalid singleton\"";
       "end"])
       
   let slice_init_lookup mapn inv out_patterns cinit =
      let cmapn = string_const mapn in
      let cout_patterns = patterns_const out_patterns in
      tabify (
      ["let inv_img = "^(vars_list inv)^" in";
       "let init_slice = "]@cinit@
      ["in";
       "let init_slice_w_indexes = List.fold_left";
       "   ValuationMap.add_secondary_index init_slice "^cout_patterns;
       "in (Database.update "^cmapn^" inv_img init_slices_w_indexes db;";
       "    init_slice_w_indexes)"])

   let lookup_aux mapn inv init_val_code =
      let cmapn = string_const mapn in
      ["let slice =";
       "   let m = Database.get_map "^cmapn^" db in";
       "   let inv_img = "^(vars_list inv)^" in";
       "      if ValuationMap.mem inv_img m then";
       "         ValuationMap.find inv_img m";
       "      else "]@
       (indent 2 init_val_code)@
      ["in"]

   let singleton_lookup mapn inv outv init_val_code = tabify (
      (lookup_aux mapn inv init_val_code)@
      ["let outv_img = "^(vars_list outv)^" in";
       "   if ValuationMap.mem outv_img slice";
       "   then [ValuationMap.find outv_img slice] else []"])

   let slice_lookup mapn inv pat patv init_val_code = tabify (
      (lookup_aux mapn inv init_val_code)@
      ["let pkey = "^(vars_list patv)^" in";
       "let lookup_slice = ValuationMap.slice "^(pattern_const pat)^" pkey slice in";
       "   (ValuationMap.strip_indexes lookup_slice)"])

   let singleton_expr ccalc cdebug = tabify (
      ["let r = "]@ccalc@
      ["in";
       "match r with";
       " | [] -> []";
       " | [v] -> (* "]@(indent 2 cdebug)@[" [] v *) [v]";
       " | _ -> failwith \"singleton_expr: invalid singleton\""])

   let direct_slice_expr ccalc cdebug = tabify (
      ["let r = "]@ccalc@
      ["in"; "(* "]@cdebug@[" r; *)"; "r"])

   let full_agg_slice_expr ccalc cdebug = tabify (
      ["let slice0 = "]@ccalc@
      ["in"; "(* "]@cdebug@[" slice0; *)";
       "[ValuationMap.fold (fun k v acc -> c_sum v acc) 0.0 slice0]"])

   (* Note: no need to bind vars in rhs_ext, these come from above. *)
   let slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug =
      let cpat = pattern_const rhs_pattern in
      tabify (
      ["let slice0 = "]@ccalc@
      ["in"; "(* "]@cdebug@[" slice0; *)";
       "let slice1 = ValuationMap.add_secondary_index slice0 "^cpat^" in";
          (* Inlined version of AggregateMap.project_keys *)
       "   VM.fold_index (fun pk kv nm ->";
       "      let (nk,nv) ="; 
       "         let aggv = List.fold_left (fun x (y,z) -> c_sum x z) 0.0 kv in "]@
      (indent 3 (bind_vars_from_list rhs_projection "pk"))@
      ["         let new_k = "^(vars_list lhs_outv)^" in (new_k, aggv)";
       "      in VM.add nk nv nm)"; 
       "   "^cpat^" slice1 (VM.empty_map())"])

   (* returns a function with sig:
    *   AggregateMap.agg_t list -> AggregateMap.agg_t *)
   let singleton_init cinit cdebug = tabify (
      ["(fun _ v ->";
       "(* "]@cdebug@[" [] v; *)";
       "let iv = "]@cinit@
      ["in";
       "match iv with";
       " | [] -> v";
       " | [init_v] -> c_sum v init_v";
       " | _ -> failwith \"singleton_init: invalid singleton\")"])

   (* returns a function with sig:
    *   AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t *)
   let slice_init lhs_outv init_ext cinit cdebug = tabify (
      ["(fun k v ->";
       "(* "]@cdebug@[" k v; *)";
       "let init_slice = "]@cinit@
      ["in";
       "let init_v =";
       "   if ValuationMap.mem k init_slice";
       "   then (ValuationMap.find k init_slice) else 0.0";
       "in c_sum v init_v)"])

   (* assume cinit is a function with sig:
    *   val cinit: AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t *)
   let singleton_update lhs_outv cincr cinit cdebug = tabify (
      ["(fun current_singleton ->";
       "(* "]@cdebug@["; *)";
       "let delta_slice = "]@cincr@
      ["in match (current_singleton, delta_slice) with";
       "   | (s, []) -> s";
       "   | ([], [delta_v]) ->";
       "      let delta_k = "^(vars_list lhs_outv)^" in";
       "      let iv = "]@
       (indent 2 cinit)@
       ["        delta_k delta_v";
       "      in [iv]";
       "   | ([current_v], [delta_v]) -> [c_sum current_v delta_v]";
       "   | _ -> failwith \"singleton_update: invalid singleton\")"])

   (* assume cinit is a function with sig:
    *   val cinit: AggregateMap.key -> AggregateMap.agg_t -> AggregateMap.agg_t *)
   let slice_update cincr cinit cdebug = tabify (
      ["(fun current_slice ->";
       "(* "]@cdebug@["; *)";
       "let delta_slice = "]@cincr@[" in";
       "   ValuationMap.merge_rk (fun k v -> v)"]@
       (indent 1 cinit)@
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
       "let new_value = "]@cstmt@
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
       "let new_slice = "]@cstmt@
      ["   in_slice";
       "in";
       "   Database.update "^cmapn^" inv_img new_slice)"])

   let statement lhs_mapn lhs_inv lhs_ext patv pat direct db_update_code = 
      let cmapn = string_const lhs_mapn in
      let cpat = pattern_const pat in tabify (
      ["let lhs_map = Database.get_map "^cmapn^" db in";
       "let iifilter db inv_img = "]@
       (indent 1 (bind_vars_from_extension lhs_inv "inv_img" lhs_ext))@
      ["   let slice = ValuationMap.find inv_img lhs_map in";
       "   let db_f = "]@
       (indent 1 db_update_code)@
      ["   in";
       "      db_f inv_img slice";
       "in";
       "let pkey = "^(vars_list patv)^" in";
       "let inv_imgs = "^
          (if direct then "[pkey]"
           else ("ValuationMap.slice_keys "^cpat^" pkey lhs_map"));
       "in List.iter (iifilter db) inv_imgs"])
   
   let trigger event rel trig_args stmt_block =
      let trigger_name = match event with
         | Insert -> "on_insert_"^rel
         | Delete -> "on_delete_"^rel
      in
      ["let "^trigger_name^" = (fun tuple -> "]@
       (indent 1 (bind_vars_from_list trig_args "tuple"))@
       (List.flatten stmt_block)@
      [");;"]
   
   (* TODO: generate data source initialization etc. *)
   (* TODO: generate a dummy benchmarker *)
   let main schema patterns triggers =
      ["open M3Common";
       "open M3OCaml\n";
       "let db = Database.make_empty_db ";
       "   "^(schema_const schema);
       "   "^(pattern_map_const patterns)^";;\n\n"]@
       (List.flatten triggers)

   let output code out_chan =
      output_string out_chan (String.concat "\n" code);
      flush out_chan

   (* No direct evaluation of OCaml source code. *)
   type db_t = Database.db_t
   let eval_trigger trigger tuple db =
      failwith "Cannot directly evaluate OCaml source"

end