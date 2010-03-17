
module StringMap = Map.Make(String)
module StringSet = Set.Make(String)

(* making and breaking externals, i.e., map accesses. *)

let mk_external (n: string) (vs: Calculus.var_t list) =
   Calculus.make_term(Calculus.RVal(Calculus.External(n, vs)))

(* compilation functions *)

(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: string list)
                          (delete: bool)
                          (map_term: Calculus.term_t)
                          (external_bound_vars: string list)
                          (externals_mapping: Calculus.term_mapping_t)
                          (term: Calculus.term_t)
   : ((bool * string * Calculus.var_t list *
                Calculus.var_t list * Calculus.term_t) list *
      Calculus.term_mapping_t) =
(*
   print_endline ("compile_delta_for_rel: reln="^reln
      ^"  relsch=["^(Util.string_of_list ", " relsch)^"]"
      ^"  map_term="^(Calculus.term_as_string map_term)
      ^"  external_bound_vars=["
              ^(Util.string_of_list ", " external_bound_vars)^"]"
      ^"  externals_mapping=["^(Util.string_of_list "; "
                (List.map (fun (x,y) -> ("<"^(Calculus.term_as_string x)^", "
                                            ^(Calculus.term_as_string y)^">"))
                          externals_mapping))^"]"
      ^"  term="^(Calculus.term_as_string term));
*)
   let (mapn, params) = Calculus.decode_map_term map_term
   in
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      x_mR_A1, ..., x_mR_Ak.
      This is the list of parameters to the trigger, while
      params is the list of parameters to the map.
   *)
   let tuple      = relsch in
   let bound_vars = tuple @ external_bound_vars (* there must be no overlap *)
   in
   (* compute the delta and simplify.
      The result is a list of pairs (new_params, new_term).
   *)
   let s = Calculus.simplify
          (Calculus.term_delta externals_mapping delete reln tuple term)
          bound_vars params
   in
   (* create the child maps still to be compiled, i.e., the subterms
      that are aggregates with a relational algebra part that is not
      constraints_only.  Also substitute extracted terms by externals.

      Note: given that aggs are now also extracted in bigsum_rewriting,
      this is rarely used: actually, it is only used if the terms reintroduced
      as deltas of maps that have been extracted before have aggregate
      subterms to be extracted. This will only happen in very complicated
      queries.
   *)
   let (terms_after_substitution, todos) =
      Calculus.extract_named_aggregates (mapn^reln) bound_vars s
   in
   ((List.map (fun (p, t) -> (delete, reln, tuple, p, t))
              terms_after_substitution),
    todos)


let extract_output_vars 
  ((t: Calculus.term_t), (mt: Calculus.term_t)): 
  CalcToM3.map_ref_t =
  (t, mt, (
    let outer_term = (Calculus.readable_term t) in 
    match (Calculus.readable_term mt) with
        Calculus.RVal(Calculus.External(n, vs)) -> 
              List.map 
                (fun var -> 
                    if (CalcToM3.find_binding_term var outer_term) 
                    then CalcToM3.Binding_Present(var)
                    else CalcToM3.Binding_Not_Present
                ) vs
      | _ -> failwith "LHS of a map definition is not an External()"
  ))
;;

(* auxiliary for compile.
   Writes (= generates string for) one increment statement. For example,

   generate_code "R" ["x_R_A"; "x_R_B"] ["x_C"] "m" ["x_C"] "+=" []
      (Prod[Val(Var(x_R_A)); Val(External("mR1", ["x_R_B"; "x_C"]))]) =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)
let generate_classic (delete:bool)
                  reln tuple loop_vars mapn params oper bigsum_vars new_term ovars inner_ovars =
   (if delete then "-" else "+")
   ^reln^"("^(Util.string_of_list ", " tuple)^ "): "^
   (if (loop_vars = []) then ""
    else "foreach "^(Util.string_of_list ", " loop_vars)^" do ")
   ^mapn^"["^(Util.string_of_list ", " params)^"] "^oper^" "^
   (if (bigsum_vars = []) then ""
    else "bigsum_{"^(Util.string_of_list ", " bigsum_vars)^"} ")
   ^(Calculus.term_as_string new_term)



(* the main compile function. call this one, not the others. *)
let rec compile (bs_rewrite_mode: Calculus.bs_rewrite_mode_t)
                (db_schema: (string * (string list)) list)
                (map_term: Calculus.term_t)
                (term: Calculus.term_t) 
                (output_vars: CalcToM3.bindings_list_t) 
                generate_code =
(*
   print_endline ("compile: t="^(Calculus.term_as_string term)^
                       ";  mt="^(Calculus.term_as_string map_term));
*)
   let (mapn, map_params) = Calculus.decode_map_term map_term
   in
   let (bigsum_vars, bsrw_theta, bsrw_term) = Calculus.bigsum_rewriting
      bs_rewrite_mode (Calculus.roly_poly term) [] (mapn^"__")
   in
   let (completed_code, todos) =
         (* We can compute a delta for bsrw_term. *)
      (
         let cdfr (delete, reln, relsch) =
            compile_delta_for_rel reln relsch delete map_term bigsum_vars
                                  bsrw_theta bsrw_term
         in
         let insdel (reln, relsch) =
            [(false, reln, relsch)]
(*
            [(false, reln, relsch); (true, reln, relsch)]
            (* this makes too many copies of the rules maintaining the
               auxiliary maps. *)
*)
         in
         let triggers = List.flatten (List.map insdel db_schema) in
         let (l1, l2) = (List.split (List.map cdfr triggers)) in
         let inner_output_vars = (List.map extract_output_vars ((List.flatten l2) @ bsrw_theta)) in
         let completed_code = List.map
            (fun (delete, reln, tuple, p, t) ->
(*
               if(bigsum_vars <> (Util.ListAsSet.diff (free t)) p) then
                  failwith "I thought that should hold."
               else
*)
               generate_code delete reln tuple (term, map_term, output_vars) p t inner_output_vars
            )
            (List.flatten l1)
         in
         (completed_code, inner_output_vars)
      )
   in
   completed_code @
   (List.flatten (List.map
      (fun (t, mt, inner_output_vars) -> compile bs_rewrite_mode db_schema mt t inner_output_vars generate_code) todos));;


let generate_m3 (delete: bool) reln tuple map_descriptor params term inner_descriptors : M3.trig_t * CalcToM3.relation_set_t =
  let (map_target_access, ra_map1) = (CalcToM3.to_m3_map_access map_descriptor) in
  let (map_update_trig, ra_map2) = (CalcToM3.to_m3 (Calculus.readable_term term) 
      (List.fold_left (fun descriptor_map descriptor -> 
          let (_, in_map, _) = descriptor in
            match (Calculus.readable_term in_map) with
                Calculus.RVal(Calculus.External(match_map, match_vars)) -> 
                  (StringMap.add match_map descriptor descriptor_map)
              | _ -> failwith "Compiler.ml: Improperly typed map LHS"
        ) StringMap.empty inner_descriptors
      )
    ) in
   (  ( (if delete then M3.Delete else M3.Insert),
        reln, tuple,
        [(map_target_access, map_update_trig)]
      ),
      ra_map2
   );;

module StatementMap = Map.Make(struct
   type t = M3.pm_t * M3.rel_id_t
   let compare = compare
end)
module MapMap = Map.Make(String)

let rec compile_m3 (db_schema: (string * (string list)) list)
                   (map_term: Calculus.term_t)
                   (term: Calculus.term_t) : M3.prog_t =
  let compiled_with_ra_map = 
    (compile Calculus.ModeOpenDomain db_schema map_term term [] generate_m3) 
  in
  let (compiled_code, ra_map) = (List.fold_right
    (fun (new_compiled, new_ra_map) (curr_compiled, curr_ra_map) -> 
      (
        new_compiled :: curr_compiled, 
        (StringSet.union new_ra_map curr_ra_map)
      )
    ) compiled_with_ra_map ([], StringSet.empty)
  ) in
  let triggers_by_relation = 
    (List.fold_right (
      fun (pm, rel_id, var, stmt) collection -> 
        StatementMap.add (pm, rel_id) (
          let (old_var, old_list) = 
            ( if (StatementMap.mem (pm, rel_id) collection) 
              then StatementMap.find (pm, rel_id) collection 
              else (var, [])
            ) in
            if ((compare old_var var) <> 0) 
            then failwith "Compiler.ml: compile_m3: Relation variables don't match between different triggers"
            else (old_var, stmt @ old_list)
        ) collection
      ) compiled_code StatementMap.empty
    ) in
  let mapvars_by_map =
    (List.fold_right (
      fun (pm, rel_id, var, stmt_list) collection ->
        (List.fold_right (
          fun ((map_id, in_map_vars, out_map_vars, init_calc), update_calc) inner_collection ->
            if (MapMap.mem map_id inner_collection)
            then inner_collection
            else (MapMap.add map_id (in_map_vars, out_map_vars) inner_collection)
          )
        ) stmt_list collection
      ) compiled_code MapMap.empty
    ) in
  let var_type vars = (List.map (fun v -> M3.VT_Int) vars) in
  (
    (* MAP LIST *)
    (MapMap.fold (* the maps as defined by the query *)
      (fun map_id (in_map_vars, out_map_vars) map_list -> 
        (map_id, (var_type in_map_vars), (var_type out_map_vars)) :: map_list
      ) mapvars_by_map []) @
    (StatementMap.fold (* additional maps as needed to store data for initial value computations *)
      (fun (pm, rel_id) (var, stmt) rel_map_list -> 
        if ((pm = M3.Insert) && (StringSet.mem rel_id ra_map))
        then ("INPUT_MAP_"^rel_id, [], (List.map (fun v-> M3.VT_Int) var)) :: rel_map_list
        else rel_map_list
      ) triggers_by_relation []),
    
    (* STMT LIST *)
    (StatementMap.fold 
      (fun (pm, rel_id) (var, stmt) trigger_list -> 
        (pm, rel_id, var, 
          ( 
            if (StringSet.mem rel_id ra_map) 
            then (("INPUT_MAP_"^rel_id, [], var, M3.Const(M3.CFloat(0.0))), M3.Const(M3.CFloat(1.0))) :: stmt
            else stmt
          )
        ) ::
        trigger_list
      ) triggers_by_relation [])
  );;
  
