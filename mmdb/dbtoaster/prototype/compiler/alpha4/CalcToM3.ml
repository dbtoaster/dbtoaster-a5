exception Assert0Exception of string

module StringMap = Map.Make(String)
module StringSet = Set.Make(String)
type relation_set_t = StringSet.t

type map_key_binding_t = 
    Binding_Present of Calculus.var_t
  | Binding_Not_Present
type bindings_list_t = map_key_binding_t list
type map_ref_t = (Calculus.term_t * Calculus.term_t * bindings_list_t) 

type m3_condition_or_none =
    EmptyCondition
  | Condition of M3.calc_t

let rec to_m3_initializer 
  (map_definition: Calculus.readable_term_t) : 
    (M3.calc_t * relation_set_t) = 
  let rec cond_to_init phi base_term : 
    (M3.calc_t * relation_set_t * m3_condition_or_none) = 
      match phi with
        Calculus.RA_Leaf(Calculus.False) ->
          (M3.Const(M3.CFloat(0.0)), StringSet.empty, EmptyCondition)
      | Calculus.RA_Leaf(Calculus.True) -> 
          (base_term, StringSet.empty, EmptyCondition)
      | Calculus.RA_Leaf(Calculus.AtomicConstraint(c, t1, t2)) -> 
          let (lh_term, ra_map1) = (to_m3_initializer t1) in
          let (rh_term, ra_map2) = (to_m3_initializer t2) in
          let ra_map = (StringSet.union ra_map1 ra_map2) in
            (match c with
                Calculus.Eq  -> 
                  (base_term, ra_map, Condition(M3.Eq(lh_term, rh_term)))
              | Calculus.Lt  -> 
                  (base_term, ra_map, Condition(M3.Lt(lh_term, rh_term)))
              | Calculus.Le  -> 
                  (base_term, ra_map, Condition(M3.Leq(lh_term, rh_term)))
              | Calculus.Neq -> 
                  failwith "TODO: Handle NEQ"
            )

      | Calculus.RA_Leaf(Calculus.Rel(mapn,map_vars)) -> 
          let (ma_term, ra_map) = (to_m3_map_access (
              (Calculus.make_term 
                (Calculus.RVal(
                  Calculus.Const(Calculus.Int(0))))), 
              (Calculus.make_term 
                (Calculus.RVal(
                  Calculus.External("INPUT_MAP_"^mapn, map_vars)))),
              (List.map (fun x -> Binding_Present(x)) map_vars)
            )) in
          (
            M3.Mult(M3.MapAccess(ma_term), base_term), 
            (StringSet.singleton mapn),
            EmptyCondition
          )
      
      | Calculus.RA_MultiNatJoin(t::[]) -> 
          cond_to_init t base_term

      | Calculus.RA_MultiNatJoin(t::l)  -> 
          let (term2, ra_map1, phi2) = 
            (cond_to_init (Calculus.RA_MultiNatJoin(l)) base_term) 
          in 
          let (term3, ra_map2, phi3) = 
            cond_to_init t term2
          in
          let ra_map = (StringSet.union ra_map1 ra_map2) in
            ( match phi2 with
                EmptyCondition -> (term3, ra_map, phi3)
              | Condition(phi2_as_m3) -> 
                ( match phi3 with
                    EmptyCondition -> (term3, ra_map, phi2)
                  | Condition(phi3_as_m3) ->
                    (term3, ra_map, Condition(M3.Add(phi3_as_m3, phi2_as_m3)))
                )
            )

      | Calculus.RA_MultiNatJoin([]) -> 
          (base_term, StringSet.empty, EmptyCondition)

      | Calculus.RA_Neg(t)        -> failwith "TODO:Handle RA_Neg"
      | Calculus.RA_MultiUnion(l) -> failwith "TODO:Handle RA_MultiUnion"
  in
  let lf_to_init lf =
    match lf with 
        Calculus.AggSum(t,phi)            -> 
          let (base_term, ra_map1) = (to_m3_initializer t) in
          let (term, ra_map2, result_phi) = 
            (cond_to_init phi base_term) in
            (
              match result_phi with
                EmptyCondition -> (term, (StringSet.union ra_map1 ra_map2))
              | Condition(result_phi_as_m3) -> 
                  (
                    M3.IfThenElse0(result_phi_as_m3, term), 
                    (StringSet.union ra_map1 ra_map2)
                  )
            )
            
      | Calculus.Const(Calculus.Int c) ->
          (M3.Const(M3.CFloat(float_of_int c)), StringSet.empty)
      | Calculus.Const(Calculus.Double d) -> 
          (M3.Const(M3.CFloat(d)), StringSet.empty)
      | Calculus.Var(vn,vt) -> 
          (M3.Var(vn), StringSet.empty)
      | Calculus.Const(_) -> 
          failwith "TODO: Handle String,Long in to_m3_initializer"
      | Calculus.External(s,vs) -> 
          failwith "Error: Uncompiled calculus expression shouldn't have a map reference"
  in
  match map_definition with
      Calculus.RVal(lf) -> 
          lf_to_init lf

    | Calculus.RNeg(t)       -> 
          let (rhs, ra_map) = 
            (to_m3_initializer t) 
          in
            ((M3.Mult((M3.Const (M3.CFloat (-1.0))), rhs)), ra_map)

    | Calculus.RProd(t1::[]) ->
          (to_m3_initializer t1)

    | Calculus.RProd(t1::l)  -> 
        let (rhs, ra_map1) = 
          (to_m3_initializer (Calculus.RProd(l))) 
        in
        let (lhs, ra_map2) = 
          (to_m3_initializer t1) 
        in
          ((M3.Mult(lhs, rhs)), (StringSet.union ra_map1 ra_map2))

    | Calculus.RProd([]) -> 
        (M3.Const(M3.CFloat(1.0)), StringSet.empty)

    | Calculus.RSum(t1::[]) ->
        (to_m3_initializer t1)

    | Calculus.RSum(t1::l) -> 
        let (rhs, ra_map1) = 
          (to_m3_initializer (Calculus.RProd(l))) 
        in
        let (lhs, ra_map2) = 
          (to_m3_initializer t1) 
        in
          ((M3.Add(lhs, rhs)), (StringSet.union ra_map1 ra_map2))

    | Calculus.RSum([]) -> 
        (M3.Const(M3.CFloat(0.0)), StringSet.empty)

(********************************)

and to_m3_map_access 
  ((map_definition, map_term, bindings):map_ref_t) : 
  M3.mapacc_t * relation_set_t =
  
  let (mapn, mapvars) = (Calculus.decode_map_term map_term) in
  let (init_stmt, ret_ra_map) = 
    (to_m3_initializer (Calculus.readable_term map_definition)) 
  in
  let input_var_list =
      (List.fold_right2
        (fun var binding input_vars -> 
          match binding with
              Binding_Present(b) -> input_vars
            | Binding_Not_Present -> var :: input_vars
        ) mapvars bindings []
      ) 
  in
  let output_var_list = 
      (List.fold_right2
        (fun var binding output_vars -> 
          match binding with
              Binding_Present(b) -> var :: output_vars
            | Binding_Not_Present -> output_vars
        ) mapvars bindings []
      ) 
  in
  ((mapn, 
    (fst (List.split input_var_list)), 
    (fst (List.split output_var_list)), 
    
      (*  Doing something smarter with the initializers... in particular, it 
          might be a good idea to see if any of the initialzers can be further 
          decompiled or pruned away.  For now, a reasonable heuristic is to use
          the presence of input variables *)
    (
      if (List.length input_var_list) > 0
      then init_stmt
      else M3.Const(M3.CFloat(0.0))
    )
  ), ret_ra_map);;

(********************************)

let rec to_m3 
  (t: Calculus.readable_term_t) 
  (inner_bindings:map_ref_t Map.Make(String).t) : 
  M3.calc_t * relation_set_t =
  
   let calc_lf_to_m3 lf =
      match lf with
         Calculus.AtomicConstraint(Calculus.Eq,  t1, t2) ->
            let (lhs, ra_map1) = (to_m3 t1 inner_bindings) in
            let (rhs, ra_map2) = (to_m3 t1 inner_bindings) in
            let ra_map = (StringSet.union ra_map1 ra_map2) in
              (M3.Eq (lhs, rhs), ra_map)
       | Calculus.AtomicConstraint(Calculus.Le,  t1, t2) ->
            let (lhs, ra_map1) = (to_m3 t1 inner_bindings) in
            let (rhs, ra_map2) = (to_m3 t1 inner_bindings) in
            let ra_map = (StringSet.union ra_map1 ra_map2) in
              (M3.Leq(lhs, rhs), ra_map)
       | Calculus.AtomicConstraint(Calculus.Lt,  t1, t2) ->
            let (lhs, ra_map1) = (to_m3 t1 inner_bindings) in
            let (rhs, ra_map2) = (to_m3 t1 inner_bindings) in
            let ra_map = (StringSet.union ra_map1 ra_map2) in
              (M3.Lt (lhs, rhs), ra_map)
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
         Calculus.AggSum(t,phi)             -> 
            let (lhs, ra_map1) = (calc_to_m3 phi) in
            let (rhs, ra_map2) = (to_m3 t inner_bindings) in
            let ra_map = (StringSet.union ra_map1 ra_map2) in
              ((M3.IfThenElse0(lhs,rhs)), ra_map)
       | Calculus.External(mapn, map_vars)      -> 
          (
            try
              let (access_term, ra_map) = 
                (to_m3_map_access 
                  (StringMap.find mapn inner_bindings)) 
              in
                (M3.MapAccess(access_term), ra_map)
            with Not_found -> 
              failwith ("Unable to find map '"^mapn^"' in {"^
                (StringMap.fold 
                  (fun k v accum -> accum^", "^k) 
                  inner_bindings "")^"}\n"
                )
          )
       | Calculus.Var(vn,vt)                -> 
            (M3.Var(vn), StringSet.empty)
       | Calculus.Const(Calculus.Int c)     -> 
            (M3.Const (M3.CFloat (float_of_int c)), StringSet.empty)
       | Calculus.Const(Calculus.Double c)  -> 
            (M3.Const (M3.CFloat c), StringSet.empty)
       | Calculus.Const(_)                  ->
            failwith "Compiler.to_m3: TODO String,Long"

   in
   match t with
      Calculus.RVal(lf)      -> lf_to_m3 lf
    | Calculus.RNeg(t1)      -> 
        let (rhs, ra_map) = (to_m3 t1 inner_bindings) in
        (M3.Mult((M3.Const (M3.CFloat (-1.0))), rhs), ra_map)
    | Calculus.RProd(t1::[]) -> (to_m3 t1 inner_bindings)
    | Calculus.RProd(t1::l)  -> 
        let (lhs, ra_map1) = (to_m3 t1 inner_bindings) in
        let (rhs, ra_map2) = (to_m3 (Calculus.RProd l) inner_bindings) in
        (M3.Mult(lhs, rhs), (StringSet.union ra_map1 ra_map2))
    | Calculus.RProd([])     -> 
        (M3.Const (M3.CFloat 1.0), StringSet.empty)
        (* impossible case, though *)
    | Calculus.RSum(t1::[])  -> 
        (to_m3 t1 inner_bindings)
    | Calculus.RSum(t1::l)   -> 
        let (lhs, ra_map1) = (to_m3 t1 inner_bindings) in
        let (rhs, ra_map2) = (to_m3 (Calculus.RSum l) inner_bindings) in
        (M3.Add(lhs, rhs), (StringSet.union ra_map1 ra_map2))
    | Calculus.RSum([])      -> 
        (M3.Const (M3.CFloat 0.0), StringSet.empty) 
        (* impossible case, though *)

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

let translate_var (calc_var:Calculus.var_t): M3.var_t = (fst calc_var)

let translate_schema (calc_schema:Calculus.var_t list): M3.var_t list = 
  (List.map translate_var calc_schema)
