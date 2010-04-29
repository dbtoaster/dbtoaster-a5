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
      let code =
         (if delete then "-" else "+")^
         reln^"("^(strlist ", " (vars relvars))^ "): "^
         (if (loop_vars = []) then ""
          else "foreach "^(strlist ", " loop_vars)^" do ")^
         mapn^"["^(strlist ", " (vars params))^"] += "^
         (if (bigsum_vars = []) then ""
          else "bigsum_{"^(strlist ", " (vars bigsum_vars))^"} ")^
         (Calculus.term_as_string new_term)
      in [code]@acc

    | _ -> failwith "Invalid map term for unit test code generation."
   in List.fold_left aux accum (List.rev trigger_defs)


(* compilation functions *)

(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: Calculus.var_t list)
                          (delete: bool)
                          (map_term: Calculus.term_t)
                          (bigsum_vars: Calculus.var_t list)
                          (externals_mapping: Calculus.term_mapping_t)
                          (term: Calculus.term_t)
   : ((bool * string * Calculus.var_t list *
                bound_vars_t * Calculus.term_t) list *
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
      mR_A1, ..., mR_Ak.
      This is the list of parameters to the trigger, while
      params is the list of parameters to the map.
   *)
   let tuple      = 
     (List.map (fun (v,t) -> 
       mapn^reln^"_"^v,t)
     relsch) in
   (* compute the delta and simplify.
      The result is a list of pairs (new_params, new_term).
   *)
   let (s,bigsum_after_subst) = 
     List.split (
       Calculus.simplify
          (Calculus.term_delta externals_mapping delete reln tuple term)
          tuple bigsum_vars params
     )
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
      Calculus.extract_named_aggregates 
          (mapn^reln) tuple s (*(if delete then "_m" else "_p")*)
   in
   ((List.map (fun ((p, t), bs) -> (delete, reln, tuple, (p, bs), t))
              (List.combine terms_after_substitution bigsum_after_subst)),
    todos)

(* the main compile function. call this one, not the others. *)
let rec compile ?(dup_elim = ref StringMap.empty)
                (bs_rewrite_mode: Calculus.bs_rewrite_mode_t)
                (db_schema: (string * (Calculus.var_t list)) list)
                ((map_definition, map_term): map_ref_t)
                (generate_code:'a output_translator_t)
                (accum:'a): 'a =
  (*print_string ("Compiling: " ^ 
                  (Calculus.term_as_string map_term)^" := "^
                  (Calculus.term_as_string map_definition)^"\n")*)
  let (mapn, map_params) = Calculus.decode_map_term map_term in
  if StringMap.mem mapn !dup_elim then 
    (try
      let _ = Calculus.equate_terms (StringMap.find mapn !dup_elim) 
                                    map_definition 
      in accum (* If we've compiled this map already, just return accum as is *)
    with Calculus.TermsNotEquivalent(_) ->
      failwith ("Bug: Compiling two distinct maps with the same name: "^mapn^
                "\n"^(Calculus.term_as_string map_definition)^
                "\n"^(Calculus.term_as_string (StringMap.find mapn !dup_elim))))
  else
  let (bigsum_vars, bsrw_theta, bsrw_term) = 
    Calculus.bigsum_rewriting
      bs_rewrite_mode (Calculus.roly_poly map_definition) [] (mapn^"__")
  in
  let cdfr (delete, reln, relsch) =
     compile_delta_for_rel reln relsch delete map_term bigsum_vars
                           bsrw_theta bsrw_term
  in
  let insdel (reln, relsch) = 
    (false, reln, relsch) :: 
    ( if Debug.active "DISABLE-DELETES" then []
      else [(true, reln, relsch)] )
  in
  let triggers = List.flatten (List.map insdel db_schema) in
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
      ( List.fold_left (fun curr_accum curr_map -> 
          compile ~dup_elim:dup_elim
                  bs_rewrite_mode
                  db_schema
                  curr_map
                  generate_code
                  curr_accum
        ) accum todos)
    )
;;
