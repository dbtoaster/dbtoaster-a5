

(* auxiliary for compile. *)
let compile_delta_for_rel (reln:   string)
                          (relsch: string list)
                          (mapn:   string)         (* map name *)
                          (params: string list)
                          (term: Algebra.term_t) =
   (* on insert into a relation R with schema A1, ..., Ak, to update map m,
      we assume that the input tuple is given by variables
      x_mR_A1, ..., x_mR_Ak. *)
   let bound_vars = (List.map (fun x -> "x_"^mapn^reln^"_"^x) relsch)
   in
   (* compute the delta and simplify. *)
   let s = Algebra.simplify (Algebra.term_delta reln bound_vars term)
                            bound_vars params
   in
   let f ((new_params, new_ma), j) =
      (* extract nested aggregates, name them, and prepare them to be
         processed next *)
      let todos =
         let mk_triple (x, i) =
            let t_params = (Util.ListAsSet.inter (Algebra.term_vars x)
                              (Util.ListAsSet.union new_params bound_vars))
            in
            (mapn^reln^(string_of_int j)^"_"^(string_of_int i), t_params, x)
         in
         (List.map mk_triple (Util.add_positions
            (Algebra.extract_aggregates new_ma) 1))
      in
      (reln, bound_vars, new_params, mapn, new_ma, todos)
   in
   List.map f (Util.add_positions s 1)


(* auxiliary for compile.
   Writes (= generates string for) one increment statement. For example,

   generate_code ("R", ["x_R_A"; "x_R_B"], ["x_C"], "m",
                  Prod[Val(Var(x_R_A)); <mR1>],
                  [("mR1", ["x_R_B"; "x_C"], <mR1>)]) =
   "+R(x_R_A, x_R_B): foreach x_C do m[x_C] += (x_R_A*mR1[x_R_B, x_C])"
*)
let generate_code (reln, bound_vars, params, mapn, new_ma, new_ma_aggs) =
   let loop_vars = Util.ListAsSet.diff params bound_vars
   in
   let fn (mapname, params, mapstructure) =
          (mapstructure, mapname^"["^(Util.string_of_list ", " params)^"]")
   in
      "+"^reln^"("^(Util.string_of_list ", " bound_vars)^ "): "^
        (if (loop_vars = []) then ""
         else "foreach "^(Util.string_of_list ", " loop_vars)^" do ")
        ^mapn^"["^(Util.string_of_list ", " params)^"] += "^
        (Algebra.term_as_string new_ma (List.map fn new_ma_aggs))


(* the main compile function. call this one, not the others. *)
let rec compile (db_schema: (string * (string list)) list)
                (mapn: string)
                (params: string list)
                (term: Algebra.term_t): (string list) =
   let cdfr (reln, relsch) =
      compile_delta_for_rel reln relsch mapn params term
   in
   (* remove deltas that are zero from list. *)
   let l = List.filter (fun (_, _, _, _, new_ma, _) ->
                            (new_ma <> Algebra.term_zero))
                       (List.flatten (List.map cdfr db_schema))
   in
   let ready = List.map generate_code l
   in
   let todo = List.flatten (List.map (fun (x,y,z,u,v,w) -> w) l)
   in
   ready @
   (List.flatten (List.map (fun (x,y,z) -> compile db_schema x y z) todo))


