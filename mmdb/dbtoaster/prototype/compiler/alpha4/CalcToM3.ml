exception Assert0Exception of string

open Util

type todo_list_t = Compiler.map_ref_t list

type map_key_binding_t = 
    Binding_Present of Calculus.var_t
  | Binding_Not_Present
type bindings_list_t = map_key_binding_t list
type map_ref_t = (Calculus.term_t * Calculus.term_t * bindings_list_t) 

type m3_condition_or_none =
    EmptyCondition
  | Condition of M3.calc_t


(********************************)
let rec find_binding_calc_lf 
  (var: Calculus.var_t) 
  (calc: Calculus.readable_relcalc_lf_t) =
  
  (match calc with
      Calculus.False                                      -> false
    | Calculus.True                                       -> false
    | Calculus.AtomicConstraint(comp, inner_t1, inner_t2) -> 
          (find_binding_term var inner_t1) || (find_binding_term var inner_t2)
    | Calculus.Rel(relname, relvars)                      -> 
          List.fold_left (
            fun found curr_var -> (found || (curr_var = var))
          ) false relvars
  )
(********************************)
and find_binding_calc 
  (var: Calculus.var_t) 
  (calc: Calculus.readable_relcalc_t) =
  
  (match calc with
      Calculus.RA_Leaf(leaf)       -> find_binding_calc_lf var leaf
    | Calculus.RA_Neg(inner_calc)  -> find_binding_calc var inner_calc
    | Calculus.RA_MultiUnion(u)    -> 
          failwith "Compiler.ml: finding output vars in MultiUnion not implemented yet"
    | Calculus.RA_MultiNatJoin(j)  -> 
          List.fold_left (fun found curr_calc -> (found || (find_binding_calc var curr_calc))) false j
  )
(********************************)

and find_binding_term 
  (var: Calculus.var_t) 
  (term: Calculus.readable_term_t) = 
  
  (match term with 
      Calculus.RVal(Calculus.AggSum(inner_t,phi))  -> 
          (find_binding_calc var phi) || (find_binding_term var inner_t)
    | Calculus.RVal(Calculus.External(mapn, vars)) ->
          failwith "Compiler.ml: (extract_output_vars) A portion of an uncompiled term is already compiled!"
    | Calculus.RVal(Calculus.Var(v))   -> false
    | Calculus.RVal(Calculus.Const(c)) -> false
    | Calculus.RNeg(inner_t)           -> (find_binding_term var inner_t)
    | Calculus.RProd(inner_ts)         -> 
          List.fold_left (
            fun found curr_t -> (found || (find_binding_term var curr_t))
          ) false inner_ts
    | Calculus.RSum(inner_ts)          ->
          List.fold_left (
            fun found curr_t -> (found || (find_binding_term var curr_t))
          ) false inner_ts
  );;
(********************************)

let translate_var (calc_var:Calculus.var_t): M3.var_t = (fst calc_var)
let translate_var_type (calc_var:Calculus.var_t): M3.var_type_t = 
  match (snd calc_var) with
  | Calculus.TInt    -> M3.VT_Int
  | Calculus.TDouble -> M3.VT_Float
  | Calculus.TLong   -> M3.VT_Int
  | Calculus.TString -> M3.VT_String
(********************************)
  

let translate_schema (calc_schema:Calculus.var_t list): M3.var_t list = 
  (List.map translate_var calc_schema)
(********************************)

let translate_map_ref ((t, mt): Compiler.map_ref_t): map_ref_t =
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
(********************************)

let rec to_m3_initializer (map_def: Calculus.term_t) (vars: Calculus.var_t list) 
                          (map_prefix: string): (M3.calc_t * todo_list_t) = 
  let rec extract_filters phi_and_psi = 
    match phi_and_psi with 
      | Calculus.RA_Leaf(Calculus.Rel(_)) -> ([phi_and_psi], [])
      | Calculus.RA_Leaf(_) -> ([], [phi_and_psi])
      | Calculus.RA_MultiNatJoin([]) -> ([], [])
      | Calculus.RA_MultiNatJoin(sub_exp) -> 
          let (new_phi, new_psi) = List.split (List.map extract_filters sub_exp) 
          in (List.flatten new_phi, List.flatten new_psi)
      | Calculus.RA_Neg(_) -> failwith ("TODO: to_m3_initializer: RA_Neg")
      | Calculus.RA_MultiUnion(_) -> failwith ("TODO: to_m3_initializer: RA_MultiUnion")
  in
  (* TODO: invoke simplify here *)
  match (Calculus.readable_term map_def) with 
    | Calculus.RVal(Calculus.AggSum(theta, phi_and_psi)) ->
      let (phi, psi) = extract_filters phi_and_psi in
      let inner_vars = 
        ListAsSet.union 
          (Calculus.term_vars (Calculus.make_term theta))
          (Calculus.relcalc_vars 
            (Calculus.make_relcalc 
              (Calculus.RA_MultiNatJoin(phi))))
      in
      let outer_vars =
        ListAsSet.union vars
          (Calculus.relcalc_vars 
            (Calculus.make_relcalc 
              (Calculus.RA_MultiNatJoin(psi))))
      in
      let init_map_name = map_prefix^"_init" in
      let init_map_term = 
        Calculus.RVal(Calculus.External(init_map_name, 
                      ListAsSet.inter inner_vars outer_vars))
      in
      let init_map_defn = 
        Calculus.RVal(Calculus.AggSum(
          theta,
          Calculus.RA_MultiNatJoin(phi)
        ))
      in
      let init_map_ref = (
        Calculus.make_term init_map_defn,
        Calculus.make_term init_map_term
      ) in
      let (translated_init,other_todos) = 
        (to_m3 
          (
            Calculus.RVal(
              Calculus.AggSum(
                init_map_term,
                Calculus.RA_MultiNatJoin(psi)))
          )
          (StringMap.add init_map_name 
                         (translate_map_ref init_map_ref) 
                         StringMap.empty)
        )
      in
        (
          translated_init,
          other_todos@[init_map_ref]
        )
    | _ -> failwith ("TODO: to_m3_initializer: "^
                      (Calculus.term_as_string map_def))

(********************************)
and split_vars (want_input_vars:bool) (vars:'a list) (bindings:bindings_list_t): 
               'a list = 
  List.fold_right2 (fun var binding ret_vars ->
    match binding with
    | Binding_Present(_)  -> if want_input_vars then ret_vars else var::ret_vars
    | Binding_Not_Present -> if want_input_vars then var::ret_vars else ret_vars
  ) vars bindings []
  
(********************************)

and to_m3_map_access 
    ((map_definition, map_term, bindings):map_ref_t)
    (vars:Calculus.var_t list option) :
      M3.mapacc_t * todo_list_t =
  let (mapn, basevars) = (Calculus.decode_map_term map_term) in
  let mapvars = 
    match vars with
      | Some(input_vars) -> 
          if List.length basevars = List.length input_vars then input_vars
          else failwith "BUG: CalcToM3: map access with incorrect arity"
      | None -> basevars
   in
  let input_var_list = (split_vars true mapvars bindings) in
  let output_var_list = (split_vars false mapvars bindings) in
  let (init_stmt, todos) = 
      (*  Doing something smarter with the initializers... in particular, it 
          might be a good idea to see if any of the initialzers can be further 
          decompiled or pruned away.  For now, a reasonable heuristic is to use
          the presence of input variables *)
    if (List.length input_var_list) > 0
      then (to_m3_initializer 
              (Calculus.apply_variable_substitution_to_term
                (List.combine basevars mapvars)
                map_definition)
              (input_var_list@output_var_list)
              mapn
            ) 
      else (M3.mk_c 0.0, [])
  in
    ((mapn, 
      (fst (List.split input_var_list)), 
      (fst (List.split output_var_list)), 
      (init_stmt, ())
    ), todos)

(********************************)

and to_m3 
  (t: Calculus.readable_term_t) 
  (inner_bindings:map_ref_t Map.Make(String).t) : 
  M3.calc_t * todo_list_t =
  
   let calc_lf_to_m3 lf =
      match lf with
         Calculus.AtomicConstraint(op,  t1, t2) ->
            let (lhs, lhs_todos) = (to_m3 t1 inner_bindings) in
            let (rhs, rhs_todos) = (to_m3 t2 inner_bindings) in
              ((match op with 
                  Calculus.Eq -> M3.mk_eq
                | Calculus.Le -> M3.mk_leq
                | Calculus.Lt -> M3.mk_lt
                | _ -> failwith ("CalcToM3.to_m3: TODO: cmp_op")
              ) lhs rhs, lhs_todos@rhs_todos)
       | _ -> failwith ("CalcToM3.to_m3: TODO constraint '"^
                   (Calculus.relcalc_as_string
                     (Calculus.make_relcalc (Calculus.RA_Leaf(lf))))^"' in '"^
                   (Calculus.term_as_string
                     (Calculus.make_term t))^"'")
   in
   let rec calc_to_m3 calc : M3.calc_t * todo_list_t =
      match calc with
         Calculus.RA_Leaf(lf) -> calc_lf_to_m3 lf
       | Calculus.RA_MultiNatJoin([lf]) ->  (calc_to_m3 lf) 
       | Calculus.RA_MultiNatJoin(lf::rest) ->
          let (lhs, lhs_todos) = (calc_to_m3 lf) in
          let (rhs, rhs_todos) = 
            (calc_to_m3 (Calculus.RA_MultiNatJoin(rest))) 
          in
            ((M3.mk_prod lhs rhs), lhs_todos@rhs_todos)
       | _                    -> 
          failwith ("Compiler.to_m3: TODO calc: "^
            (Calculus.relcalc_as_string (Calculus.make_relcalc calc)))
   in
   let lf_to_m3 (lf: Calculus.readable_term_lf_t) =
      match lf with
         Calculus.AggSum(t,phi)             -> 
            let (lhs, lhs_todos) = (calc_to_m3 phi) in
            let (rhs, rhs_todos) = (to_m3 t inner_bindings) in
              ((M3.mk_if lhs rhs), lhs_todos@rhs_todos)
       | Calculus.External(mapn, map_vars)      -> 
          (
            try
              let (access_term, todos) = 
                (to_m3_map_access 
                  (StringMap.find mapn inner_bindings)
                  (Some(map_vars)))
              in
                (M3.mk_ma access_term, todos)
            with Not_found -> 
              failwith ("Unable to find map '"^mapn^"' in {"^
                (StringMap.fold 
                  (fun k v accum -> accum^", "^k) 
                  inner_bindings "")^"}\n"
                )
          )
       | Calculus.Var(vn,vt)                -> 
            (M3.mk_v vn, [])
       | Calculus.Const(Calculus.Int c)     -> 
            (M3.mk_c (float_of_int c), [])
       | Calculus.Const(Calculus.Double c)  -> 
            (M3.mk_c c, [])
       | Calculus.Const(_)                  ->
            failwith "Compiler.to_m3: TODO String,Long"

   in
   match t with
      Calculus.RVal(lf)      -> lf_to_m3 lf
    | Calculus.RNeg(t1)      -> 
        let (rhs, todos) = (to_m3 t1 inner_bindings) in
        (M3.mk_prod (M3.mk_c (-1.0)) rhs, todos)
    | Calculus.RProd(t1::[]) -> (to_m3 t1 inner_bindings)
    | Calculus.RProd(t1::l)  -> 
        let (lhs, lhs_todos) = (to_m3 t1 inner_bindings) in
        let (rhs, rhs_todos) = (to_m3 (Calculus.RProd l) inner_bindings) in
        (M3.mk_prod lhs rhs, lhs_todos@rhs_todos)
    | Calculus.RProd([])     -> 
        (M3.mk_c 1.0, [])
        (* impossible case, though *)
    | Calculus.RSum(t1::[])  -> 
        (to_m3 t1 inner_bindings)
    | Calculus.RSum(t1::l)   -> 
        let (lhs, lhs_todos) = (to_m3 t1 inner_bindings) in
        let (rhs, rhs_todos) = (to_m3 (Calculus.RSum l) inner_bindings) in
        (M3.mk_sum lhs rhs, lhs_todos@rhs_todos)
    | Calculus.RSum([])      -> 
        (M3.mk_c 0.0, []) 
        (* impossible case, though *)

module M3InProgress = struct
  type mapping_params = (string * int list * int list)
  type map_mapping = mapping_params StringMap.t
  
  type possible_mapping = 
  | Mapping of mapping_params
  | NoMapping
  
  type trigger_map = (string * ((M3.var_t list) * (M3.stmt_t list))) list
  
  (* insertion triggers * deletion triggers * mappings * map definitions *)
  type t = trigger_map * trigger_map * map_mapping * 
           (string * map_ref_t) list
  
  type insertion = (map_ref_t * M3.trig_t list)
  
  let add_to_trigger_map rel vars stmts tmap =
    if List.mem_assoc rel tmap then
      let (tvars, tstmts) = List.assoc rel tmap in
      let joint_stmts = 
        (* We assume that we receive statements through build in the order that
           the statements should be executed in; This is rarely relevant but it
           is an atomicity guarantee made by M3.  The order with which we store
           relations is irrelevant, but within each relation's trigger, the
           statement order must be preserved *)
        (if tvars <> vars then
            List.map (M3Common.rename_vars vars tvars) stmts
          else
            stmts
        ) @ tstmts
          
      in
        (rel, (tvars, joint_stmts))::(List.remove_assoc rel tmap)
    else
       (rel, (vars, stmts))::tmap

  let init: t = ([], [], StringMap.empty, [])
    
  let build ((curr_ins, curr_del, curr_mapping, curr_refs):t)   
            ((descriptor, triggers):insertion): t =
    let (query_term, map_term, map_bindings) = descriptor in
    let (map_name, map_vars) = Calculus.decode_map_term map_term in
    let mapping = 
      List.fold_left (fun result (cmp_term, cmp_map_term, cmp_bindings) ->
        if result = NoMapping then
          try 
            let (cmp_name, cmp_vars) = Calculus.decode_map_term cmp_map_term in
              if (List.length map_vars) <> (List.length cmp_vars) then
                raise (Calculus.TermsNotEquivalent("Mismatched parameter list sizes"))
              else
            let var_mappings = Calculus.equate_terms query_term cmp_term in
            (* We get 2 different mappings; Check to see they're consistent *)
            if not (List.for_all2 (fun (a,_) (b,_) -> (a = b) ||
                  (* It shouldn't happen that a doesn't have a mapping in b, but
                     if it does, that just means the variable is unused in the
                     map definition *)
                    (StringMap.mem a var_mappings) || 
                    ((StringMap.find a var_mappings) = b)
                  ) map_vars cmp_vars)
              then raise (Calculus.TermsNotEquivalent("Mismatched parameter list s"))
              else
            let cmp_in_vars = split_vars true cmp_vars cmp_bindings in
            let cmp_out_vars = split_vars false cmp_vars cmp_bindings in
            let index_of (needle:Calculus.var_t)
                         (haystack:Calculus.var_t list) = 
              let (index, _) =
                (List.fold_left (fun (found, index) cmp -> 
                  ( (if cmp = needle then index else found),
                    index + 1
                  )
                ) (-1, 1) haystack)
              in
              if index = -1 then 
                raise (Calculus.TermsNotEquivalent("Variable Not Found"))
                                   (*shouldn't happen*)
              else index
            in
              Mapping(
                cmp_name,
                List.fold_right (fun in_var mapping -> 
                  (index_of in_var cmp_in_vars)::mapping
                ) (split_vars true map_vars map_bindings) [],
                List.fold_right (fun out_var mapping -> 
                  (index_of out_var cmp_out_vars)::mapping
                ) (split_vars false map_vars map_bindings) []
              )
          with Calculus.TermsNotEquivalent(a) -> (*print_string (a^"\n");*)NoMapping
        else result
      ) NoMapping (snd (List.split curr_refs))
    in
    let on_mapping_found (m:mapping_params) = 
      (
        Debug.print "MAP-CALC" (fun () -> 
          let (sub_name,_,_) = m in
            (Calculus.term_as_string map_term)^" (replaced by "^sub_name^") "^
            " := "^(Calculus.term_as_string query_term)
        );
        (
          curr_ins, (* an equivalent map exists. don't change anything *)
          curr_del,
          StringMap.add map_name m curr_mapping, (* just add a mapping *)
          curr_refs
        )
      )
    in
    let on_no_mapping () =
      (
        Debug.print "MAP-CALC" (fun () -> 
          (Calculus.term_as_string map_term)^
          " := "^(Calculus.term_as_string query_term)
        );
        let (insert_trigs, delete_trigs) =
          List.fold_left (fun (ins, del) (pm, rel, vars, stmts)  -> 
            match pm with 
            | M3.Insert -> ((add_to_trigger_map rel vars stmts ins), del)
            | M3.Delete -> (ins, (add_to_trigger_map rel vars stmts del))
          ) (curr_ins, curr_del) triggers 
        in
        (
          insert_trigs, delete_trigs, 
          curr_mapping, 
          (map_name,descriptor)::curr_refs
        )
      )
    in
      if Debug.active "IGNORE-DUP-MAPS" then
        on_no_mapping ()
      else
        match mapping with
      | Mapping(m) -> on_mapping_found m
      | NoMapping  -> on_no_mapping ()
        
  
  let get_rel_schema ((ins_trigs,del_trigs,_,_):t): 
      (M3.var_t list) StringMap.t =
    let gather_schema (rel_name, (rel_vars, _)) accum  =
      StringMap.add rel_name rel_vars accum
    in
      List.fold_right gather_schema ins_trigs (
        List.fold_right gather_schema del_trigs 
          StringMap.empty
      )
  
  let get_maps ((_, _, mapping, maps):t) : M3.map_type_t list =
    List.map (fun (map_name, (map_defn, map_term, map_bindings)) ->
      let (_, map_vars) = Calculus.decode_map_term map_term in
      (
        map_name, 
        List.map translate_var_type (split_vars true map_vars map_bindings),
        List.map translate_var_type (split_vars false map_vars map_bindings)
      )
    ) maps
  
  let get_triggers ((ins, del, mapping, _):t) : M3.trig_t list =
    let produce_triggers (pm:M3.pm_t) (tmap:trigger_map): M3.trig_t list =
      List.map (fun (rel_name, (rel_vars, triggers)) ->
        (pm, rel_name, rel_vars, 
          (List.map (fun trigger -> 
            (M3Common.rename_maps 
              (StringMap.map (fun (x,_,_)->x) mapping) trigger)
            ) triggers
          )
        )
      ) tmap
    in
      (produce_triggers M3.Insert ins) @ (produce_triggers M3.Delete del)
  
  let finalize (program:t) : M3.prog_t = 
    (get_maps program, get_triggers program)

  let get_map_refs ((_,_,mappings,refs):t) : map_ref_t StringMap.t =
    let mapped_map_terms: map_ref_t StringMap.t = 
      StringMap.map (fun (base_map,_,_) -> List.assoc base_map refs) mappings
    in
      List.fold_left (fun ref_map (map, ref) ->
        StringMap.add map ref ref_map
      ) mapped_map_terms refs
      
  let rec compile (schema:(string*(Calculus.var_t list)) list)
              (map_ref:Compiler.map_ref_t)
              (accum:t) : t =
    Compiler.compile Calculus.ModeOpenDomain
                     schema
                     map_ref
                     generate_m3
                     accum
    
  and generate_m3 (schema: (string*(Calculus.var_t list)) list)
                  (map_ref: Compiler.map_ref_t)
                  (triggers: Compiler.trigger_definition_t list)
                  (accum: t): t =
    let map_ref_as_m3 = translate_map_ref map_ref in
    let (triggers_as_m3, todos) = 
      List.fold_left (fun (tlist,old_todos) 
                          (delete, reln, relvars, (params,bsvars), expr) -> 
        let sub_map_params (map_definition, map_term, map_bindings) params =
          let (map_name, map_vars) = (Calculus.decode_map_term map_term) in
          (
            (Calculus.apply_variable_substitution_to_term (List.combine map_vars params) map_definition), 
            (Calculus.map_term map_name params), 
            map_bindings
          )
        in
        let (target_access, init_todos) = 
          (to_m3_map_access (sub_map_params map_ref_as_m3 params)) None in
        let (update, update_todos) = (to_m3 (Calculus.readable_term expr) 
                                         (get_map_refs accum)) in
        let todos = old_todos @ init_todos @ update_todos in
        let update_trigger = (
            (if delete then M3.Delete else M3.Insert),
            reln, (translate_schema relvars),
            [(target_access, (update,()), ())]
          )
        in
        (
          (Debug.print "MAP-DELTAS" (fun () -> 
              "ON "^(if delete then "-" else "+")^reln^
              (list_to_string (fun (a,_)->a) relvars)^
              ": "^(Calculus.term_as_string (snd map_ref))^
              (list_to_string (fun (a,_)->a) params)^" += "^
              (Calculus.term_as_string expr)^"\n   ->\n"^
              (M3Common.pretty_print_calc update)^"\n"
            ));
          (
            if delete then 
              (
                if Debug.active "DISABLE-DELETES" then
                  (tlist, old_todos)
                else
                  (tlist @ [update_trigger], todos)
              )
            else
              (tlist @ [ update_trigger ], todos)
          )
        )
      ) ([], []) triggers
    in
      List.fold_left 
        (fun old_accum todo -> 
          compile schema
                  todo
                  old_accum
        )
        (build accum (map_ref_as_m3, triggers_as_m3))
        todos
end

let compile = M3InProgress.compile;;
