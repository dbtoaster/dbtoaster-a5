open Util
open Calculus

type piped_term_data_t = 
   | Expression
   | BigSum of var_t list
;;
type piped_term_t = 
   string option *       (* Map Name *)
   var_t list ref *      (* All variables = Input + Output, ordered*)
   var_t list ref *      (* Input variables *)
   term_t ref *          (* Term defining the expression *)
   subterm_t ref *       (* Subterm definitions *)
   delta_term_t ref *    (* List of delta terms *)
   piped_term_data_t ref (* Internal metadata *)

and subterm_t = 
   |  Subterm_None
   |  Subterm_Some of piped_term_t list

and delta_term_t = 
   |  Delta_None 
   |  Delta_Some of delta_t list

and delta_t = ((bool * string) * (var_t list * piped_term_t list))

type map_term_t = (string * piped_term_t)

;;

let make_p_term (term:term_t) (defn:term_t) 
              (ivars:var_t list): piped_term_t =
   let (map_name,map_vars) = decode_map_term term in
      (  (Some(map_name)), 
         ref map_vars,
         ref ivars,
         ref defn,
         ref Subterm_None,
         ref Delta_None,
         ref Expression
      )

;;

let convert_term_mapping ?(bs_meta = None) (ivars:var_t list)
                         (terms:term_mapping_t):
                         piped_term_t list =
   (List.map (fun (defn,term) ->
      let bs_ivars = match bs_meta with
         | None                    -> []
         | Some(lhs_term, bs_vars) -> if term = lhs_term then [] else bs_vars
      in
         (make_p_term term defn 
                      (ListAsSet.inter
                         (snd (decode_map_term term))
                         (ListAsSet.union ivars bs_ivars))
         )
   ) terms)

;;

let convert_bigsum (term:term_t) (defn:term_t) 
                   (ivars:var_t list) 
                   (theta:term_mapping_t)
                   (bs_vars:var_t list): piped_term_t =
   let (map_name,map_vars) = decode_map_term term in
   let lhs_ext = (match (readable_term defn) with 
      | RVal(AggSum(lhs,_)) -> make_term lhs
      | _ -> failwith ("PipedCalculus: Invalid term in bigsum: "^ 
                       (term_as_string defn))
      )
   in
   let piped_theta = List.map (fun ((theta_defn:term_t),
                                    (theta_term:term_t)) -> 
      if (term = lhs_ext) then
         make_p_term theta_term theta_defn []
      else
         make_p_term theta_term theta_defn bs_vars 
   ) theta
   in
      (  (Some(map_name)),
         ref map_vars,
         ref ivars,
         ref defn,
         ref (Subterm_Some(piped_theta)),
         ref (Delta_None),
         ref (BigSum(bs_vars))
      )

;;

let get_definition ((_,_,_,term,_,_,_):piped_term_t):term_t = 
   !term
let get_vars ((_,map_vars,_,_,_,_,_):piped_term_t):var_t list =
   !map_vars
let get_ivars ((_,_,ivars,_,_,_,_):piped_term_t):var_t list = 
   !ivars
let get_ovars (pterm:piped_term_t):var_t list =  
   (ListAsSet.diff (get_vars pterm) (get_ivars pterm))
let get_map_name ((map_name,_,_,_,_,_,_):piped_term_t):string =
   match map_name with Some(s) -> s | None -> "UnnamedTerm"
let get_map_term (pterm:piped_term_t):term_t = 
   (map_term (get_map_name pterm) (get_vars pterm))
let get_deltas_opt ((_,_,_,_,_,deltas,_):piped_term_t): 
                   (delta_t list option) =
   match !deltas with Delta_None -> None | Delta_Some(s) -> Some(s)
let get_deltas (pterm:piped_term_t):(delta_t list) =
   match (get_deltas_opt pterm) with 
      | Some(dlist) -> dlist 
      | None -> failwith "Getting deltas from uncompiled piped_term_t"
let get_delta (pterm:piped_term_t) (rel:(bool*string)): delta_t =
   ((rel),(List.assoc (rel) (get_deltas pterm)))
let get_subterms ((_,_,_,_,subterms,_,_):piped_term_t):piped_term_t list =
   match !subterms with 
   |  Subterm_None    -> []
   |  Subterm_Some(s) -> s
let get_subterm (pterm:piped_term_t) (t:string):piped_term_t =
   List.find (fun pt -> (get_map_name pt) = t) 
             (get_subterms pterm)

;;

let get_internals ((_,_,_,_,_,_,idata):piped_term_t):piped_term_data_t =
   !idata
let get_subterm_mappings (pterm:piped_term_t): term_mapping_t =
   (List.map (fun t -> (get_definition t, get_map_term t)) 
             (get_subterms pterm))

;;

let replace_vars (new_vars:var_t list)
                 ((_,vars,_,_,_,_,_):piped_term_t):unit =
   vars := new_vars
let replace_ivars (new_ivars:var_t list)
                 ((_,ivars,_,_,_,_,_):piped_term_t):unit =
   ivars := new_ivars
let replace_definition (new_term:term_t)
                       ((_,_,_,term,_,_,_):piped_term_t):unit =
   term := new_term

let replace_subterms (new_subterms:piped_term_t list)
                     ((_,_,_,_,subterms,_,_):piped_term_t):unit =
   subterms := Subterm_Some(new_subterms)

let replace_deltas (new_deltas:delta_t list option)
                   ((_,_,_,_,_,deltas,_):piped_term_t):unit =
   deltas := match new_deltas with Some(s) -> Delta_Some(s) | None -> Delta_None

let replace_internals (new_idata:piped_term_data_t)
                      ((_,_,_,_,_,_,idata):piped_term_t):unit =
   idata := new_idata

;;

let rename_io_vars (new_p:var_t list) (pterm:piped_term_t): 
                   (var_t list * var_t list) =
   (  
      (List.map 
         (fun x -> (Function.apply (List.combine (get_vars pterm) new_p) x x))
         (get_ivars pterm)
      ),
      (List.map 
         (fun x -> (Function.apply (List.combine (get_vars pterm) new_p) x x))
         (get_ovars pterm)
      )
   )

;;

let bigsum_rewrite ?(rewrite_mode = Calculus.ModeOpenDomain) 
                   (pterm:piped_term_t): piped_term_t =
   (if (get_internals pterm) <> Expression then 
      failwith "Attempt to bigsum rewrite a bigsum");
   let (bigsum_vars, bsrw_theta, bsrw_term) = 
      Calculus.bigsum_rewriting 
         rewrite_mode 
         (Calculus.roly_poly (get_definition pterm))
         [] ((get_map_name pterm)^"_")
   in
      if bigsum_vars <> [] then
         (* The rewritten maps on the rhs have the bigsum_vars as input 
            variables.  The rewritten maps on the lhs does not.  Consequently
            we need to extract the LHS map *)
         let lhs_term = 
            match (readable_term bsrw_term) with 
               | RVal(AggSum(lhs,rhs)) -> make_term lhs
               | _ -> failwith "Bigsum of non-AggSum"
         in
         let converted_thetas = 
            convert_term_mapping ~bs_meta:(Some(lhs_term,bigsum_vars) )
                                 (get_ivars pterm)
                                 bsrw_theta
         in
         (  (replace_definition bsrw_term pterm);
            (replace_subterms   ((get_subterms pterm) @ (converted_thetas)) 
                                pterm);
            (replace_internals  (BigSum(bigsum_vars)) pterm);
            pterm)
      else 
         pterm

;;

let build_deltas ?(compile_deletes = true)
                 (schema:(string * var_t list) list)
                 (pterm:piped_term_t): piped_term_t = 
   let compile_one_delta (rel:string) (tuple:var_t list) (delete:bool):
                         delta_t =
      let (monomial_terms, subbed_bs_vars) =
         List.split (
            Calculus.simplify 
               (Calculus.term_delta
                  (List.map (fun t -> ((get_definition t),(get_map_term t)))
                            (get_subterms pterm))
                  delete
                  rel
                  tuple
                  (get_definition pterm))
               tuple
               (match (get_internals pterm) with 
                  |  BigSum(bs_vars) -> bs_vars
                  |  _ -> [])
               (get_vars pterm)
            )
      in
      let monomial_pterms = List.map (fun (vars,term) ->
            (term, (map_term (get_map_name pterm) vars))
         ) monomial_terms
      in
      let bs_varname_hacked_monomial_terms = 
         match (get_internals pterm) with
            |  BigSum(bs_vars) ->
                  (* the way we do bigsum rewriting is a little hacked...
                     we might end up with a bigsum term that appears in the
                     parameter list of the map.  This is unfortunate, but
                     it means we need to carry the variable names through the
                     simplification phase, since it acts differently on
                     terms involving bigsum vars.  It also means that we 
                     might need to do a postprocessing transformation on the
                     parameter list *)
                  List.map (fun (((p:var_t list),(t:term_t)),(bs:var_t list)) ->
                     (  t,
                        (map_term 
                           (get_map_name pterm)
                           (List.map (fun (x:var_t) -> 
                              (Function.apply (List.combine bs_vars bs) 
                                              x x))
                              p
                           )
                        )
                     ))
                     (List.combine monomial_terms subbed_bs_vars)
                  
            |  _ -> monomial_pterms
      in
         ((delete,rel),
          (tuple, (convert_term_mapping (get_ivars pterm)
                                        bs_varname_hacked_monomial_terms))
         )
   in
      (replace_deltas (Some(
         (List.flatten (
            List.map (fun (rel,tuple) ->
               List.map (compile_one_delta rel tuple) 
                        (if compile_deletes then [true;false] else [false])
            ) schema
         ))
      )) pterm);
      pterm

;;

let build_subterms_for_delta (delete:bool) (rel:string) (tuple:var_t list)
                             (pterm:piped_term_t): piped_term_t =
   let (terms_after_substitution, subterms) = 
      Calculus.extract_named_aggregates 
         ((get_map_name pterm)^(if delete then "_m" else "_p")^rel)
         tuple
         [(get_vars pterm), (get_definition pterm)]
   in
      (* There should only be one return value for each term passed to e_n_a *)
      (if (List.length terms_after_substitution) <> 1 then 
       failwith "Assertion failure: invalid extract_named_aggregates return");
   let (args,defn) = List.hd terms_after_substitution in
      (replace_vars       args pterm);
      (replace_definition defn pterm);
      (replace_subterms ((get_subterms pterm) @ 
                         (convert_term_mapping (get_ivars pterm) subterms))
                        pterm);
      pterm

;;

let build_product (op_prod:(var_t list -> 'a list -> 'a))
                  (ivars:var_t list) 
                  (rcr_fn:var_t list -> 'b -> var_t list * 'a) 
                  (tlist:'b list): (var_t list * 'a) =
   let (ovars,retlist) = 
      List.fold_left 
         (fun (curr_ivars,retlist) t ->
            let (new_vars,ret) = rcr_fn curr_ivars t in
               ((ListAsSet.union new_vars curr_ivars), 
                retlist@[ret])
               )
         (ivars,[])
         tlist
   in
      (ovars, (op_prod ivars retlist))
;;
let build_sum (op_tsum:(var_t list -> 'a list -> 'a))
              (ivars:var_t list) 
              (rcr_fn:var_t list -> 'b -> var_t list * 'a) 
              (tlist:'b list): (var_t list * 'a) =
   let (ovars,retlist) = 
      List.fold_left 
         (fun (ovars,retlist) t ->
            let (new_ovars,ret) = rcr_fn ivars t in
               ((ListAsSet.union ovars new_ovars), retlist@[ret])
         )
         ([],[])
         tlist
   in
      (ovars, (op_tsum ivars retlist))

;;

let fold (op_neg:(var_t list -> 'a -> 'a))
         (op_prod:(var_t list -> 'a list -> 'a))
         (op_tsum:(var_t list -> 'a list -> 'a))
         (op_asum:(var_t list -> 'a -> 'a))
         (op_const:(var_t list -> const_t -> 'a))
         (op_var:(var_t list -> var_t -> 'a))
         (op_ext:(var_t list -> piped_term_t * var_t list * var_t list -> 'a))
         (op_rel:(var_t list -> string * var_t list -> 'a))
         (op_cmp:(var_t list -> comp_t -> 'a -> 'a -> 'a))
         (pterm:piped_term_t): 'a =
   let 
   rec rcr_term (ivars:var_t list) (term:readable_term_t): 
                (var_t list * 'a) =
      let (new_vars,ret) = 
         match term with
         |  RVal(AggSum(as_term, as_calc)) ->
            let (calc_ovars,cret) = rcr_calc ivars      as_calc in
            let (term_ovars,tret) = rcr_term calc_ovars as_term in
               (  term_ovars, 
                  (op_asum ivars (op_prod ivars [cret;tret]))
               )
         |  RVal(External(ename,evars)) ->
            let subterm = (get_subterm pterm ename) in
            let (ext_ivars, ext_ovars) = 
               (rename_io_vars evars subterm) 
            in
               (ext_ovars,(op_ext ivars (subterm,ext_ivars,ext_ovars)))
         |  RVal(Const(c))    -> ([], (op_const ivars c))
         |  RVal(Var(v))      -> ([], (op_var   ivars v))
         |  RNeg(t)           -> 
            let (ret_ovars,ret) = (rcr_term ivars t) in
               (ret_ovars, (op_neg ivars ret))
         |  RProd(tlist)      -> (build_product op_prod ivars rcr_term tlist)
         |  RSum(tlist)       -> (build_sum     op_tsum ivars rcr_term tlist)
      in
      ((ListAsSet.union ivars new_vars), ret)
    and rcr_calc (ivars:var_t list) (term:readable_relcalc_t):
                 (var_t list * 'a) =
      let (new_vars,ret) = 
         match term with 
         |  RA_Leaf(AtomicConstraint(cmp,t1,t2)) ->
            let (left_ovars, left_ret ) = rcr_term ivars t1 in
            let (right_ovars,right_ret) = rcr_term ivars t2 in
               (  (ListAsSet.union left_ovars right_ovars),
                  (op_cmp ivars cmp left_ret right_ret)
               )
         |  RA_Leaf(Rel(relname,relvars)) ->
            (  relvars,
               (op_rel ivars (relname,relvars))
            )
         |  RA_Leaf(False) -> ([], (op_const ivars (Boolean(false))))
         |  RA_Leaf(True)  -> ([], (op_const ivars (Boolean(true))))
         |  RA_Neg(t)      -> 
            let (ret_ovars,ret) = (rcr_calc ivars t) in
               (ret_ovars, (op_neg ivars ret))
         |  RA_MultiNatJoin(tlist) -> 
               (build_product op_prod ivars rcr_calc tlist)
         |  RA_MultiUnion(tlist)  -> 
               (build_sum     op_tsum ivars rcr_calc tlist)
      in
      ((ListAsSet.union ivars new_vars), ret)
   in
   let term = Calculus.readable_term (get_definition pterm) in
   let (_, ret) = 
      match (get_internals pterm) with 
      |  Expression      -> rcr_term (get_ivars pterm) term
      |  BigSum(bs_vars) -> 
         (  match term with 
            |  RVal(AggSum(bs_term, bs_calc)) -> 
               (* Bigsums are represented as a top-level aggsum.  The only
                  difference in parsing is that the binding pattern in a 
                  bigsum is the reverse of an aggsum ->
                     Aggsum -> Sum(Calc * Term)
                     Bigsum -> Sum(Term * Calc)
               *)
               let (term_ovars,tret) = rcr_term (get_ivars pterm) bs_term in
               let (calc_ovars,cret) = rcr_calc term_ovars        bs_calc in
                  (  calc_ovars, 
                     (op_asum (get_ivars pterm)
                        (op_prod (get_ivars pterm) [tret;cret])))
            
            |  _ -> 
               failwith "AnnotatedCalculus: Bigsum over non-aggsum (fold)"
         )
   in 
      ret
