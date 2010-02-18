
exception Assert0Exception of string

type binding_t = bool Map.Make(String).t
type binding_set_element = 
    Binding_Present of Calculus.var_t
  | Binding_Not_Present
type bindings_set_t = binding_set_element list
type inner_bindings_set_t = (Calculus.term_t * Calculus.term_t * bindings_set_t) list
module BindingMap = Map.Make(String)
type binding_map_t = bindings_set_t BindingMap.t

(* making and breaking externals, i.e., map accesses. *)

let mk_external (n: string) (vs: Calculus.var_t list) =
   Calculus.make_term(Calculus.RVal(Calculus.External(n, vs)))

let decode_map_term (map_term: Calculus.term_t):
                    (string * (Calculus.var_t list)) =
   match (Calculus.readable_term map_term) with
      Calculus.RVal(Calculus.External(n, vs)) -> (n, vs)
    | _ -> raise (Assert0Exception "Compiler.decode_map_term")

let rec to_m3_map_access (mapn:M3.map_id_t) (mapvars:M3.var_t list) (bindings:bindings_set_t) : M3.mapacc_t =
  (
    mapn,
    (List.fold_right2
      (fun var binding input_vars -> 
        match binding with
            Binding_Present(b) -> input_vars
          | Binding_Not_Present -> var :: input_vars
      ) mapvars bindings []
    ),
    (List.fold_right2
      (fun var binding output_vars -> 
        match binding with
            Binding_Present(b) -> var :: output_vars
          | Binding_Not_Present -> output_vars
      ) mapvars bindings []
    ),
    (M3.Null[])
  )


(* conversion of calculus expressions to M3 expressions *)
let rec to_m3 (t: Calculus.readable_term_t) (my_bindings:bindings_set_t) (inner_bindings:binding_map_t) =
   let calc_lf_to_m3 lf =
      match lf with
         Calculus.AtomicConstraint(Calculus.Eq,  t1, t2) ->
                                          M3.Eq ((to_m3 t1 my_bindings inner_bindings), (to_m3 t2 my_bindings inner_bindings))
       | Calculus.AtomicConstraint(Calculus.Le,  t1, t2) ->
                                          M3.Leq((to_m3 t1 my_bindings inner_bindings), (to_m3 t2 my_bindings inner_bindings))
       | Calculus.AtomicConstraint(Calculus.Lt,  t1, t2) ->
                                          M3.Lt ((to_m3 t1 my_bindings inner_bindings), (to_m3 t2 my_bindings inner_bindings))
         (* AtomicConstraint(Neq, t1, t2) -> ... *)
       | _ -> failwith "Compiler.to_m3: TODO cond"
   in
   let calc_to_m3 calc =
      match calc with
         Calculus.RA_Leaf(lf) -> calc_lf_to_m3 lf
       | _                    -> failwith "Compiler.to_m3: TODO calc"
   in
   let lf_to_m3 (lf: Calculus.readable_term_lf_t) =
      match lf with
         Calculus.AggSum(t,phi)             -> M3.IfThenElse0((calc_to_m3 phi),
                                                          (to_m3 t my_bindings inner_bindings))
       | Calculus.External(mapn, map_vars)      -> 
          (
            try
              M3.MapAccess(to_m3_map_access mapn map_vars (BindingMap.find mapn inner_bindings))
            with Not_found -> 
              failwith ("Unable to find map '"^mapn^"' in {"^
                (BindingMap.fold (fun k v accum -> accum^", "^k) inner_bindings "")^"}\n")
          )
       | Calculus.Var(v)                    -> M3.Var(v)
       | Calculus.Const(Calculus.Int c)     -> M3.Const (M3.CInt c)
       | Calculus.Const(Calculus.Double c)  -> M3.Const (M3.CFloat c)
       | Calculus.Const(_)                  ->
                                          failwith "Compiler.to_m3: TODO String,Long"

   in
   match t with
      Calculus.RVal(lf)      -> lf_to_m3 lf
    | Calculus.RNeg(t1)      -> M3.Mult((M3.Const (M3.CInt (-1))), (to_m3 t1 my_bindings inner_bindings))
    | Calculus.RProd(t1::[]) -> (to_m3 t1 my_bindings inner_bindings)
    | Calculus.RProd(t1::l)  -> M3.Mult((to_m3 t1 my_bindings inner_bindings), (to_m3 (Calculus.RProd l) my_bindings inner_bindings))
    | Calculus.RProd([])     -> M3.Const (M3.CInt 1)  (* impossible case, though *)
    | Calculus.RSum(t1::[])  -> (to_m3 t1 my_bindings inner_bindings)
    | Calculus.RSum(t1::l)   -> M3.Add ((to_m3 t1 my_bindings inner_bindings), (to_m3 (Calculus.RSum  l) my_bindings inner_bindings))
    | Calculus.RSum([])      -> M3.Const (M3.CInt 0)  (* impossible case, though *)
    

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
   let (mapn, params) = decode_map_term map_term
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

let rec find_binding_calc_lf (var: Calculus.var_t) (calc: Calculus.readable_relcalc_lf_t) =
  (match calc with
      Calculus.False                                      -> false
    | Calculus.True                                       -> false
    | Calculus.AtomicConstraint(comp, inner_t1, inner_t2) -> 
          (find_binding_term var inner_t1) || (find_binding_term var inner_t2)
    | Calculus.Rel(relname, relvars)                      -> 
          List.fold_left (fun found curr_var -> (found || (curr_var = var))) false relvars
  )
and find_binding_calc (var: Calculus.var_t) (calc: Calculus.readable_relcalc_t) =
  (match calc with
      Calculus.RA_Leaf(leaf)       -> find_binding_calc_lf var leaf
    | Calculus.RA_Neg(inner_calc)  -> find_binding_calc var inner_calc
    | Calculus.RA_MultiUnion(u)    -> 
          failwith "Compiler.ml: finding output vars in MultiUnion not implemented yet"
    | Calculus.RA_MultiNatJoin(j)  -> 
          List.fold_left (fun found curr_calc -> (found || (find_binding_calc var curr_calc))) false j
  )
and find_binding_term (var: Calculus.var_t) (term: Calculus.readable_term_t) = 
  (match term with 
      Calculus.RVal(Calculus.AggSum(inner_t,phi))  -> 
          (find_binding_calc var phi) || (find_binding_term var inner_t)
    | Calculus.RVal(Calculus.External(mapn, vars)) ->
          failwith "Compiler.ml: (extract_output_vars) A portion of an uncompiled term is already compiled!"
    | Calculus.RVal(Calculus.Var(v))               -> false
    | Calculus.RVal(Calculus.Const(c))             -> false
    | Calculus.RNeg(inner_t)                       -> (find_binding_term var inner_t)
    | Calculus.RProd(inner_ts)                     -> 
          List.fold_left (fun found curr_t -> (found || (find_binding_term var curr_t))) false inner_ts
    | Calculus.RSum(inner_ts)                      ->
          List.fold_left (fun found curr_t -> (found || (find_binding_term var curr_t))) false inner_ts
  );;

let extract_output_vars ((t: Calculus.term_t), (mt: Calculus.term_t)): (Calculus.term_t * Calculus.term_t * bindings_set_t) =
  (t, mt, (
    let outer_term = (Calculus.readable_term t) in 
    match (Calculus.readable_term mt) with
        Calculus.RVal(Calculus.External(n, vs)) -> 
              List.map 
                (fun var -> 
                    if (find_binding_term var outer_term) 
                    then Binding_Present(var)
                    else Binding_Not_Present
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

let generate_m3 (delete: bool)
                reln tuple loop_vars mapn params oper bigsum_vars new_term ovars inner_ovars =
   ((if delete then M3.Delete else M3.Insert),
    reln, tuple,
    [((to_m3_map_access mapn params ovars), 
      (to_m3 (Calculus.readable_term new_term) ovars 
      (List.fold_left (fun in_ovar_map (in_rhs, in_map, in_ovars) -> 
          match (Calculus.readable_term in_map) with
              Calculus.RVal(Calculus.External(match_map, match_vars)) -> (BindingMap.add match_map in_ovars in_ovar_map)
            | _ -> failwith "Compiler.ml: Improperly typed map LHS"
        ) BindingMap.empty inner_ovars
      )
    ))]);;


(* the main compile function. call this one, not the others. *)
let rec compile (bs_rewrite_mode: Calculus.bs_rewrite_mode_t)
                (db_schema: (string * (string list)) list)
                (map_term: Calculus.term_t)
                (term: Calculus.term_t) 
                (output_vars: bindings_set_t) 
                generate_code =
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
               let loop_vars = Util.ListAsSet.diff p tuple
               in
               generate_code delete
                             reln tuple loop_vars mapn p "+=" bigsum_vars t output_vars inner_output_vars)
            (List.flatten l1)
         in
         (completed_code, inner_output_vars)
      )
   in
   completed_code @
   (List.flatten (List.map
      (fun (t, mt, inner_output_vars) -> compile bs_rewrite_mode db_schema mt t inner_output_vars generate_code) todos));;

module StatementMap = Map.Make(struct
   type t = M3.pm_t * M3.rel_id_t
   let compare = compare
end)
module MapMap = Map.Make(String)

let rec compile_m3 (db_schema: (string * (string list)) list)
                   (map_term: Calculus.term_t)
                   (term: Calculus.term_t) : M3.prog_t =
  let compiled_code = (compile Calculus.ModeOpenDomain db_schema map_term term [] generate_m3) in
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
    (MapMap.fold
      (fun map_id (in_map_vars, out_map_vars) map_list -> 
        (map_id, (var_type in_map_vars), (var_type out_map_vars)) :: map_list
      ) mapvars_by_map []),
    (StatementMap.fold 
      (fun (pm, rel_id) (var, stmt) trigger_list -> 
        (pm, rel_id, var, stmt) :: trigger_list
      ) triggers_by_relation [])
  );;
  
