exception Assert0Exception of string

(* making and breaking externals, i.e., map accesses. *)

let mk_external (n: string) (vs: Calculus.var_t list) =
   Calculus.make_term(Calculus.RVal(Calculus.External(n, vs)))

let decode_map_term (map_term: Calculus.term_t):
                    (string * (Calculus.var_t list)) =
   match (Calculus.readable_term map_term) with
      Calculus.RVal(Calculus.External(n, vs)) -> (n, vs)
    | _ -> raise (Assert0Exception "Compiler.decode_map_term")




(* compilation functions *)

(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: string list)
                          (map_term: Calculus.term_t)
                          (external_bound_vars: string list)
                          (externals_mapping: Calculus.term_mapping_t)
                          (term: Calculus.term_t) =
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
   let (mapn, params) = decode_map_term map_term
   in
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      x_mR_A1, ..., x_mR_Ak.
      This is the list of parameters to the trigger, while
      params is the list of parameters to the map.
   *)
   let tuple      = (List.map (fun x -> "x_"^mapn^reln^"_"^x) relsch) in
   let bound_vars = tuple @ external_bound_vars (* there must be no overlap *)
   in
   (* compute the delta and simplify.
      The result is a list of pairs (new_params, new_term).
   *)
   let s = List.filter (fun (_, t) -> t <> Calculus.term_zero)
       (Calculus.simplify
          (Calculus.term_delta externals_mapping false reln tuple term)
          bound_vars params)    (* FIXME: we only create insertion triggers. *)
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
   ((List.map (fun (p, t) -> (reln, tuple, p, t))
              terms_after_substitution), todos)


(* auxiliary for compile.
   Writes (= generates string for) one increment statement. For example,

   generate_code "R" ["x_R_A"; "x_R_B"] ["x_C"] "m" ["x_C"] "+=" []
      (Prod[Val(Var(x_R_A)); Val(External("mR1", ["x_R_B"; "x_C"]))]) =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)
let generate_code reln tuple loop_vars mapn params oper bigsum_vars new_term =
   "+"^reln^"("^(Util.string_of_list ", " tuple)^ "): "^
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
                (term: Calculus.term_t): (string list) =
(*
   print_endline ("compile: t="^(Calculus.term_as_string term)^
                       ";  mt="^(Calculus.term_as_string map_term));
*)
   let (mapn, map_params) = decode_map_term map_term
   in
   let (bigsum_vars, bsrw_theta, bsrw_term) = Calculus.bigsum_rewriting
      bs_rewrite_mode (Calculus.roly_poly term) [] (mapn^"__")
   in
   let (completed_code, todos) =
      if ((bsrw_theta = []) ||
          (bs_rewrite_mode = Calculus.ModeOpenDomain)) then
         (* We can compute a delta for bsrw_term. *)
      (
         let cdfr (reln, relsch) =
            compile_delta_for_rel reln relsch map_term bigsum_vars
                                  bsrw_theta bsrw_term
         in
         let (l1, l2) = (List.split (List.map cdfr db_schema))
         in
         let completed_code = List.map
            (fun (reln, tuple, p, t) ->
               let loop_vars = Util.ListAsSet.diff p tuple in
               generate_code reln tuple loop_vars mapn p "+=" bigsum_vars t)
            (List.flatten l1)
         in
         (completed_code, (List.flatten l2) @ bsrw_theta)
      )
      else (* We cannot compute a delta for bsrw_term, so we have to leave
              it as is and hope the extracted subterms will allow us to do
              something smart. *)
         let (_, bsrw_term_simple) =
            Calculus.simplify_roly true bsrw_term map_params
            (* we didn't call compile_delta_for_rel, so the term needs
               to be simplified. *)
         in
         (* an On Lookup statement is not an update trigger: it is called
            when we want to access the query result. In principle, we
            could drop the for loops. *)
(*
         ([generate_code "On Lookup" [] map_params
                           mapn map_params ":=" [] bsrw_term_simple],
          bsrw_theta)
*)
         (* Note: no creation of for loop over the map_params -- we look
            up one value only, for the given params. *)
         ([("+On Lookup(): "^mapn^"["^(Util.string_of_list ", " map_params)
           ^"] := "
           ^(Calculus.term_as_string bsrw_term_simple))], bsrw_theta)
   in
   completed_code @
   (List.flatten (List.map
      (fun (t, mt) -> compile bs_rewrite_mode db_schema mt t) todos))


