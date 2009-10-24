(* a library of delta functions for externals *)

exception Assert0Exception

(* external functions that only depend on their arguments, but not on the
   database, such as division, string manipulations, etc. *)
let delta_for_true_external name vars = Calculus.term_zero

let delta_for_named_map
      (mapping: (string * (Calculus.var_t list * Calculus.term_t)) list)
      (name: string) (vars: Calculus.var_t list): Calculus.term_t =
   let (vars2, term) = Util.Function.apply_strict mapping name
                       (* if the delta for external function name is not
                          given, this raises a NonFunctionalMappingException. *)
   in
   (* TODO: make sure that the variables vars either do not yet occur in
      term or are equal to vars2. Then substitute. For now, we assume that
      vars and vars2 are the same. *)
   if (vars=vars2) then term
   else raise Assert0Exception

let externals_forbidden name vars = raise Assert0Exception



(* making and breaking externals, i.e., map accesses. *)

let mk_external (map_name: string) (params: Calculus.var_t list):
                Calculus.term_t =
   Calculus.make_term(Calculus.RVal(Calculus.External(map_name, params)))

let decode_map_term (map_term: Calculus.term_t):
                    (string * (Calculus.var_t list)) =
   match (Calculus.readable_term map_term) with
      Calculus.RVal(Calculus.External(n, vs)) -> (n, vs)
    | _ -> raise Assert0Exception 




(* compilation functions *)

(* given a list of pairs of terms and their parameters, this function
   extracts all the nested aggregate sum terms, eliminates duplicates,
   and then names the aggregates.

   We do it in this complicated fashion to avoid creating the same
   child terms redundantly.
*)
let extract_named_aggregates (name_prefix: string) bound_vars
                  (workload: ((Calculus.var_t list) * Calculus.term_t) list):
                  (((Calculus.var_t list) * Calculus.term_t) list *
                   ((Calculus.term_t * Calculus.term_t) list)) =
   let extract_from_one_term (params, term) =
      let prepend_params t =
         let p = (Util.ListAsSet.inter (Calculus.term_vars t)
                    (Util.ListAsSet.union params bound_vars))
         in (p, t)
      in
      List.map prepend_params (Calculus.extract_aggregates_from_term term)
   in
   (* the central duplicate elimination step. *)
   let extracted_terms = Util.ListAsSet.no_duplicates
                (List.flatten (List.map extract_from_one_term workload))
   in
   (* give names to the extracted terms *)
   let add_name ((p, t), i) = (name_prefix^(string_of_int i), p, t)
   in
   let named_terms = List.map add_name (Util.add_positions (extracted_terms) 1)
   in
   (* apply substitutions to input terms. *)
   let theta = List.map (fun (n, vs, t) -> (t, mk_external n vs)) named_terms 
   in
   let terms_after_substition =
      List.map (fun (p, t) ->  (p, (Calculus.substitute_in_term theta t)))
               workload
   in
   (terms_after_substition, theta)



(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: string list)
                          (map_term: Calculus.term_t)
                          (external_bound_vars: string list)
                          (term: Calculus.term_t) =
   let (mapn, params) = decode_map_term map_term
   in
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      x_mR_A1, ..., x_mR_Ak.
      This is the list of parameters to the trigger, while
      params is the list of parameters to the map.
   *)
   let tuple = (List.map (fun x -> "x_"^mapn^reln^"_"^x) relsch)
   in
   let bound_vars = tuple @ external_bound_vars (* there must be no overlap *)
   in
   (* compute the delta and simplify.
      The result is a list of pairs (new_params, new_term).
   *)
   let s = List.filter (fun (_, t) -> t <> Calculus.term_zero)
       (Calculus.simplify
          (Calculus.term_delta externals_forbidden false reln tuple term)
          bound_vars params)
   in
   (* create the child maps still to be compiled, i.e., the subterms
      that are aggregates with a relational algebra part that is not
      constraints_only.  Also substitute extracted terms by externals. *)
   let (terms_after_substitution, todos) =
      extract_named_aggregates (mapn^reln) bound_vars s
   in
   ((List.map (fun (p, t) -> (reln, tuple, p, t)) terms_after_substitution),
    todos)


(* auxiliary for compile.
   Writes (= generates string for) one increment statement. For example,

   generate_code "R" ["x_R_A"; "x_R_B"] ["x_C"] "m" ["x_C"]
      Prod[Val(Var(x_R_A)); Val(External("mR1", ["x_R_B"; "x_C"]))]) =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)
let generate_code reln tuple loop_vars mapn params bigsum_vars new_term =
   "+"^reln^"("^(Util.string_of_list ", " tuple)^ "): "^
   (if (loop_vars = []) then ""
    else "foreach "^(Util.string_of_list ", " loop_vars)^" do ")
   ^mapn^"["^(Util.string_of_list ", " params)^"] += "^
   (if (bigsum_vars = []) then ""
    else "bigsum_{"^(Util.string_of_list ", " bigsum_vars)^"} ")
   ^(Calculus.term_as_string new_term)


(* the main compile function. call this one, not the others. *)
let rec compile (db_schema: (string * (string list)) list)
                (map_term: Calculus.term_t)
                (bigsum_vars: string list)  (* external bound variables *)
                (term: Calculus.term_t): (string list) =
   let cdfr (reln, relsch) =
      compile_delta_for_rel reln relsch map_term bigsum_vars term
   in
   let (l1, l2) = (List.split (List.map cdfr db_schema))
   in
   let completed_code = List.map
      (fun (reln, tuple, p, t) ->
         let mapn = fst (decode_map_term map_term) in
         let loop_vars = Util.ListAsSet.diff p tuple in
         generate_code reln tuple loop_vars mapn p bigsum_vars t)
      (List.flatten l1)
   in
   let todos = List.flatten l2
   in
   completed_code @
   (List.flatten (List.map (fun (t, mt) -> compile db_schema mt [] t) todos))


