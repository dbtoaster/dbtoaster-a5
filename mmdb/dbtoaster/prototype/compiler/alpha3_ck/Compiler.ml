

(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: string list)
                          (mapn:   string)         (* map name *)
                          (params: string list)    (* params of the map *)
                          (bigsum_vars: string list)
                          (term: Calculus.term_t) =
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      x_mR_A1, ..., x_mR_Ak. *)
   let bound_vars = (List.map (fun x -> "x_"^mapn^reln^"_"^x) relsch)
   (* so bound_vars is the list of parameters to the trigger, while
      params is the list of parameters to the map. *)
   in
   (* compute the delta and simplify. *)
   let s = List.filter (fun (_, t) -> t <> Calculus.term_zero)
              (Calculus.simplify (Calculus.term_delta reln bound_vars term)
                                 (bound_vars @ bigsum_vars) params)
   (* the result is a list of pairs (new_params, new_term). *)
   in
   (* creating the child maps still to be compiled, i.e., the subterms
      that are aggregates with a relational algebra part that is not
      constraints_only. *)
   let todos =
      let f (new_params, new_term) =
         let mk x =
            let t_params = (Util.ListAsSet.inter (Calculus.term_vars x)
                              (Util.ListAsSet.union new_params
                                 (Util.ListAsSet.union bound_vars bigsum_vars)))
            in (t_params, x)
         in
         List.map mk (Calculus.extract_aggregates new_term)
      in
      let add_name ((p, x), i) = (mapn^reln^(string_of_int i), p, x)
      in
      List.map add_name (Util.add_positions (Util.ListAsSet.no_duplicates
         (List.flatten (List.map f s))) 1)
   in
   let g (new_params, new_term) =
      (reln, bound_vars, new_params, bigsum_vars, mapn, new_term)
   in
   ((List.map g s), todos)

   


(* auxiliary for compile.
   Writes (= generates string for) one increment statement. For example,

   generate_code ("R", ["x_R_A"; "x_R_B"], ["x_C"], "m",
                  Prod[Val(Var(x_R_A)); <mR1>],
                  [("mR1", ["x_R_B"; "x_C"], <mR1>)]) =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)
let generate_code (reln, bound_vars, params, bigsum_vars,
                   mapn, new_term) new_term_aggs =
   let loop_vars = Util.ListAsSet.diff params bound_vars
   in
   let fn (mapname, params, mapstructure) =
          (mapstructure, mapname^"["^(Util.string_of_list ", " params)^"]")
   in
      "+"^reln^"("^(Util.string_of_list ", " bound_vars)^ "): "^
        (if (loop_vars = []) then ""
         else "foreach "^(Util.string_of_list ", " loop_vars)^" do ")
        ^mapn^"["^(Util.string_of_list ", " params)^"] += "^
        (if (bigsum_vars = []) then ""
         else "bigsum_{"^(Util.string_of_list ", " bigsum_vars)^"} ")
        ^(Calculus.term_as_string new_term (List.map fn new_term_aggs))


(* the main compile function. call this one, not the others. *)
let rec compile (db_schema: (string * (string list)) list)
                (mapn: string)
                (params: string list)
                (bigsum_vars: string list)
                (term: Calculus.term_t): (string list) =
   let cdfr (reln, relsch) =
      compile_delta_for_rel reln relsch mapn params bigsum_vars term
   in
   let (l1, l2) = (List.split (List.map cdfr db_schema))
   in
   let todo = List.flatten l2
   in
   let ready = List.map (fun x -> generate_code x todo) (List.flatten l1)
   in
   ready @
   (List.flatten (List.map (fun (x,y,z) -> compile db_schema x y [] z) todo))


