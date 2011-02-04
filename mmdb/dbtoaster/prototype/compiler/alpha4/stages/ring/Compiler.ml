open Util;;

(*               map_definition,   map_term *)
type map_ref_t = (Calculus.term_t * Calculus.term_t)

type bound_vars_t = 
(* params,               bigsum_vars *)
  (Calculus.var_t list * Calculus.var_t list)

type trigger_definition_t =
(* delete, rel,    relvars,              var types,     trigger expr *)
  (bool * string * Calculus.var_t list * bound_vars_t * Calculus.term_t)

type 'a output_translator_t = 
  (string * (Calculus.var_t list)) list -> map_ref_t -> 
  trigger_definition_t list -> 'a -> 'a

(* making and breaking externals, i.e., map accesses. *)

let mk_external (n: string) (vs: Calculus.var_t list) =
   Calculus.make_term(Calculus.RVal(Calculus.External(n, vs)))

(* Unit test code generation
   Writes (= generates string for) one increment statement. For example,

   generate_unit_test_code
      _ (_, External("m",[("x_C", TInt)])
      false "R" ["x_R_A"; "x_R_B"] (["x_C"],[])
      (Prod[Val(Var(x_R_A)); Val(External("mR1", ["x_R_B"; "x_C"]))])
   =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)

let generate_unit_test_code
      (db_schema : (string * (Calculus.var_t list)) list)
      ((map_definition, map_term) : map_ref_t)
      (trigger_defs : trigger_definition_t list)
      (accum: string list) : string list 
=
   let aux acc (delete, reln, relvars, (params, bigsum_vars), new_term) =
   match (Calculus.readable_term map_term) with
    | Calculus.RVal(Calculus.External(mapn,_)) ->
      let vars = List.map fst in
      let strlist = String.concat in
      let loop_vars = Util.ListAsSet.diff (vars params) (vars relvars) in
      let prop_vars = Util.ListAsSet.inter (vars bigsum_vars) loop_vars in
      let bs_vars = Util.ListAsSet.diff (vars bigsum_vars) loop_vars in
      let code =
         (if delete then "-" else "+")^
         reln^"("^(strlist ", " (vars relvars))^ "): "^
         (if (loop_vars = []) then ""
          else "foreach "^(strlist ", " loop_vars)^" do ")^
         mapn^"["^(strlist ", " (vars params))^"] += "^
         (if prop_vars = [] then ""
          else "up_and_left_{"^(strlist ", " prop_vars)^"} ")^
         (if (bs_vars = []) then ""
          else "bigsum_{"^(strlist ", " bs_vars)^"} ")^
         (Calculus.term_as_string new_term)
      in [code]@acc

    | _ -> failwith "Invalid map term for unit test code generation."
   in List.fold_left aux accum (List.rev trigger_defs)


(* compilation functions *)

(* auxiliary for compile. *)
let compile_delta_for_rel (produce_child_maps: bool)
                          (reln:   string)
                          (relsch: Calculus.var_t list)
                          (delete: bool)
                          (map_term: Calculus.term_t)
                          (bigsum_vars: Calculus.var_t list)
                          (externals_mapping: Calculus.term_mapping_t)
                          (db_schema: (string * (Calculus.var_t list)) list)
                          (term: Calculus.term_t)
   : ((bool * string * Calculus.var_t list *
                bound_vars_t * Calculus.term_t) list *
      Calculus.term_mapping_t) =

   let (mapn, params) = Calculus.decode_map_term map_term
   in
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      mR_A1, ..., mR_Ak.
      This is the list of parameters to the trigger, while
      params is the list of parameters to the map.
   *)
   let tuple = (List.map (fun (v,t) -> mapn^reln^"_"^v,t) relsch) in

   Debug.print "LOG-COMPILE-STEPS" (fun () -> "Compiling " ^
                  (if delete then "-" else "+")^reln^
                  (Util.list_to_string fst tuple)^" of "^ 
                  (Calculus.term_as_string map_term)^" := "^
                  (Calculus.term_as_string term)^"\n  with "^
                  (Util.list_to_string fst bigsum_vars)^"\n"^
                  (Util.string_of_list "" 
                     (List.map (fun (x,y)-> "    "^
                        (Calculus.term_as_string y)^" := "^
                        (Calculus.term_as_string x)^"\n")
                        externals_mapping)));
   
   Debug.print "LOG-COMPLEX-DELTA" (fun () ->
     "Unsimplified delta: "^(Calculus.term_as_string
       (Calculus.term_delta externals_mapping delete reln tuple term))^"\n");
   
   (* compute the delta and simplify.
      The result is a list of pairs (new_params, new_term). *)
   let (s,bigsum_after_subst) = 
     List.split (
       Calculus.simplify
          (Calculus.term_delta externals_mapping delete reln tuple term)
          tuple bigsum_vars params)
   in

   Debug.print "LOG-COMPILE-TRIGGERS" (fun () ->
     String.concat "\n" (List.map (fun (p, t) ->
         "Trigger params: "^(Util.list_to_string fst p)^"\n"^
         "Trigger delta: "^(Calculus.term_as_string t)^"\n")
       s));

   (* create the child maps still to be compiled, i.e., the subterms
      that are aggregates with a relational algebra part that is not
      constraints_only.  Also substitute extracted terms by externals.

      Note: given that aggs are now also extracted in bigsum_rewriting,
      this is rarely used: actually, it is only used if the terms reintroduced
      as deltas of maps that have been extracted before have aggregate
      subterms to be extracted. This will only happen in very complicated
      queries.
   *)
   if produce_child_maps then
     let (terms_after_substitution, todos) =
       (* pass down bigsum vars as well so they are preserved as
        * output vars of maps for deltas that do not involve the bigsum var,
        * e.g. in the case of joins in the flat part of the bigsum rewrite. *)
       let extract_vars =
         Util.ListAsSet.union tuple (List.flatten bigsum_after_subst)
       in Calculus.extract_named_aggregates 
            (mapn^reln) extract_vars s (*(if delete then "_m" else "_p")*)
     in
     Debug.print "LOG-COMPILE-TRIGGERS" (fun () ->
        String.concat "\n" (List.map (fun (p,t) ->
            "Trigger simplified params: "^(Util.list_to_string fst p)^"\n"^
            "Trigger delta w/ maps: "^(Calculus.term_as_string t)^"\n") 
          terms_after_substitution));

     ((List.map (fun ((p, t), bs) -> (delete, reln, tuple, (p, bs), t))
                (List.combine terms_after_substitution bigsum_after_subst)),
      todos)
   else
     let (base_relations, terms_after_substitution) = 
       Calculus.extract_base_relations s
     in
       (List.map (fun (p,t) -> (delete, reln, tuple, (p, []), t)) 
          terms_after_substitution,
        List.map (fun (reln) -> 
          let rel_schema = List.assoc reln db_schema in
            (Calculus.base_relation_expr (reln, rel_schema),
             Calculus.map_term reln rel_schema))
          base_relations)


(* the main compile function. call this one, not the others. *)
let rec compile ?(dup_elim = ref StringMap.empty)
                ?(top_down_depth = None)
                (bs_rewrite_mode: Calculus.bs_rewrite_mode_t)
                (db_schema: (string * (Calculus.var_t list)) list)
                ((map_definition, map_term): map_ref_t)
                (generate_code:'a output_translator_t)
                (accum:'a): 'a =

  Debug.print "LOG-COMPILES" (fun () -> "Compiling: " ^ 
                  (Calculus.term_as_string map_term)^" := "^
                  (Calculus.term_as_string map_definition));
  
  let (mapn, map_params) = Calculus.decode_map_term map_term in
  if StringMap.mem mapn !dup_elim then 
    (try
      let _ = Calculus.equate_terms (StringMap.find mapn !dup_elim) map_definition 
      in accum (* If we've compiled this map already, just return accum as is *)
    with Calculus.TermsNotEquivalent(_) ->
      print_endline ("Bug: Compiling two distinct maps with the same name: "^mapn^
                "\n"^(Calculus.term_as_string map_definition)^
                "\n"^(Calculus.term_as_string (StringMap.find mapn !dup_elim)));
      failwith ("Bug: Compiling two distinct maps with the same name: "^mapn^
                "\n"^(Calculus.term_as_string map_definition)^
                "\n"^(Calculus.term_as_string (StringMap.find mapn !dup_elim))))
  else
  let (preagg_vars, preagg_theta, preagg_term) =
    Calculus.preaggregate
      bs_rewrite_mode (Calculus.roly_poly map_definition) map_params (mapn^"_")
  in

  let (bigsum_vars, bsrw_theta, bsrw_term) =
    (* 
    Calculus.bigsum_rewriting
      bs_rewrite_mode (Calculus.roly_poly map_definition) map_params (mapn^"_")
    *)
    Calculus.preaggregate
      bs_rewrite_mode (Calculus.roly_poly map_definition) map_params (mapn^"_")
  in

  Debug.print "LOG-PREAGG" (fun () ->
    "Preaggregation:\n"^
    (Calculus.term_as_string (Calculus.roly_poly map_definition))^
    "\n    =>\n    "^(Calculus.term_as_string preagg_term)^
    "\n   with:\n"^
    (String.concat "\n    "
      (List.map (fun (defn,t) ->
        (Calculus.term_as_string t)^" := "^(Calculus.term_as_string defn))
        preagg_theta))^
    "\nPropagated vars:\n    "^(String.concat "," (List.map fst preagg_vars)));

  Debug.print "LOG-PREAGG" (fun () ->
    "Bigsum rewrite:\n"^
    (Calculus.term_as_string (Calculus.roly_poly map_definition))^
    "\n    =>\n    "^(Calculus.term_as_string bsrw_term));

  let (next_top_down_depth, go_deeper) = 
    match top_down_depth with 
    | None -> (None, true)
    | Some(i) -> if i <= 1 then (None, false) else ((Some(i-1)),true)
  in
  let cdfr (delete, reln, relsch) =
     compile_delta_for_rel go_deeper
                           reln relsch delete map_term bigsum_vars
                           bsrw_theta db_schema bsrw_term
  in
  let insdel (reln, relsch) = 
    (false, reln, relsch) :: 
    ( if Debug.active "DISABLE-DELETES" then []
      else [(true, reln, relsch)] )
  in
  (* Create a list of triggers by crossing [Relations] x [insert | delete]  *)
  let triggers = List.flatten (List.map insdel db_schema) in
  (* CDFR returns 2 lists (for a specific RELATION & insert|delete trigger): 
      l1: A list of compiled update triggers for immediate insertion
      l2: A list of maps (subexpressions) that need to be compiled (todos) 
          (note that Calculus.bigsum_rewriting also returns a list of todos)
  *)
  let (l1, l2) = (List.split (List.map cdfr triggers)) in
  (if not (Debug.active "DISABLE-COMPILER-DUPS") then
    dup_elim := 
      StringMap.add (fst (Calculus.decode_map_term map_term)) 
                    map_definition
                    !dup_elim);
  let todos = ((List.flatten l2) @ bsrw_theta) 
  in
    (generate_code (* We need to do this traversal DEPTH FIRST *)
      db_schema
      (map_definition, map_term)
      (List.flatten l1)
        (List.fold_left (fun curr_accum curr_map -> 
             compile ~dup_elim:dup_elim
                     ~top_down_depth:next_top_down_depth
                     bs_rewrite_mode
                     db_schema
                     curr_map
                     generate_code
                     curr_accum)
           accum todos))
