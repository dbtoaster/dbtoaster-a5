open Util
open Calculus

module PipedCalculus = struct
   type piped_term_data_t = 
      | Expression
      | BigSum of var_t list
   
   type piped_term_t = 
      var_t list *      (* All variables = Input + Output, ordered*)
      var_t list *      (* Input variables *)
      term_t *          (* Term definition; the expression defining the term *)
      piped_term list * (* Subterm definitions *)
      piped_term_data   (* Contents *)
   
   type map_term_t = (string * piped_term_t)
   
   ;;
   
   let make_term (term:term_t) (defn:term_t) 
                 (ivars:var_t list): piped_term_t =
      let (map_name,map_vars) = decode_map_term term in
         (  map_name, 
            map_vars,
            ivars,
            defn,
            [],
            Expression
         )
   
   ;;
   
   let convert_bigsum (term:term_t) (defn:term_t) 
                      (ivars:var_t list) 
                      (theta:term_mapping_t)
                      (bs_vars:var_t list): piped_term_t =
      let (map_name,map_vars) = decode_map_term term in
      if (match defn with AggSum(_,_) -> false | _ -> true) then
         failwith ("PipedCalculus: Invalid term in bigsum: "^
                   (term_to_string defn));
      else
         (  map_name,
            map_vars,
            ivars,
            defn,
            theta,
            (BigSum(bs_vars))
         )
   
   ;;
   
   let get_definition ((_,_,_,_,term,_,_):piped_term_t):term_t = 
      term
   let get_map_term ((map_name,map_vars,_,_,_,_):piped_term_t):term_t = 
      (map_term map_name map_vars)
   let get_vars ((_,map_vars,_,_,_,_):piped_term_t):var_t list =
      map_vars
   let get_ivars ((_,_,ivars,_,_,_):piped_term_t):var_t list = 
      ivars
   let get_ovars (pterm:piped_term_t):var_t list =  
      (ListAsSet.diff (get_vars pterm) (get_ivars pterm))
   let get_map_name ((map_name,_,_,_,_,_):piped_term_t):string =
      map_name
   let get_subterms ((_,_,_,_,subterms,_):piped_term_t):string =
      subterms
   let get_subterm (pterm:piped_term_t) (t:string):piped_term_t =
      List.find (fun (map_name,_,_,_,_,_) -> map_name = t) (get_subterms pterm)
   
   ;;
      
   let get_internals ((_,_,_,_,_,idata):piped_term_t):piped_term_data_t =
      idata
   let get_subterm_mappings (pterm:piped_term_t): term_mapping_t =
      (List.map (fun t -> (get_definition t, get_map_term t)) 
                (get_subterms pterm))
   
   ;;
   
   let rename_io_vars (new_names:var_t list) (pterm:piped_term_t): 
                      (var_t list * var_t list) =
      (
         (Function.apply (List.combine (get_vars pterm) new_names)
                         (get_ivars pterm)),
         (Function.apply (List.combine (get_vars pterm) new_names)
                         (get_ovars pterm))
      )
   
   ;;
   
   let fold (op_neg:(var_t list -> 'a -> 'a))
            (op_prod:(var_t list -> 'a list -> 'a))
            (op_tsum:(var_t list -> 'a list -> 'a))
            (op_asum:(var_t list -> 'a -> 'a))
            (op_const:(var_t list -> const_t -> 'a))
            (op_var:(var_t list -> var_t -> 'a))
            (op_ext:(var_t list -> string * var_t list * var_t list -> 'a))
            (op_rel:(var_t list -> string * var_t list -> 'a))
            (op_cmp:(var_t list -> comp_t -> 'a -> 'a -> 'a))
            (pterm:piped_term_t): 'a =
      let build_product ivars rcr_fn tlist =
         let (ovars,retlist) = 
            List.fold_left 
               (fun (curr_ivars,retlist) t ->
                  let (new_vars,ret) = rcr_fn curr_ivars t in
                     ((ListAsSet.union new_ivars curr_ivars), 
                      retlist@[ret])
                     )
               (ivars,[])
               tlist
         in
            (ovars, (op_prod ivars retlist))
      and let build_sum ivars rcr_fn tlist =
         let (ovars,retlist) = 
            List.fold_left 
               (fun (ovars,retlist) t ->
                  let (new_ovars,ret) = rcr_fn ivars t in
                     ((ListAsSet.union ovars new_ovars), ret)
               )
               ([],[])
               tlist
         in
            (ovars, (op_sum ivars retlist))
      in let 
      ret rcr_term (ivars:var_t list) (term:readable_term_t): 
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
               let (ext_ivars, ext_ovars) = 
                  (rename_io_vars evars (get_subterm pterm ename)) 
               in
                  (ext_ovars,(op_ext ivars (ename,ext_ivars,ext_ovars)))
            |  RVal(Const(c))    -> ([], (op_const ivars c))
            |  RVal(Var(v))      -> ([], (op_var   ivars v))
            |  RNeg(t)           -> 
               let (ret_ovars,ret) = (rcr_term ivars t) in
                  (ret_ovars, (op_neg ivars t))
            |  RProd(tlist)      -> (build_product ivars rcr_term tlist)
            |  RSum(tlist)       -> (build_sum     ivars rcr_term tlist)
         in
         (ListAsSet.union ivars new_vars)
       and rcr_calc (ivars:var_t list) (term:readable_relcalc_t) =
                    (var_t list * 'a) =
         let (new_vars,ret) = 
            match term with 
            |  RA_Leaf(AtomicConstraint(cmp,t1,t2)) ->
               let (left_ovars, left_ret ) = rcr ivars t1 in
               let (right_ovars,right_ret) = rcr ivars t2 in
                  (  (ListAsSet.union left_ovars right_ovars),
                     (op_cmp ivars left_ret right_ret)
                  )
            |  RA_Leaf(Rel(relname,relvars)) ->
               (  relvars,
                  (op_rel ivars (relname,relvars))
               )
            |  RA_Leaf(False) -> ([], (op_const ivars Bool(false)))
            |  RA_Leaf(True)  -> ([], (op_const ivars Bool(true)))
            |  RA_Neg(t)      -> 
               let (ret_ovars,ret) = (rcr_calc ivars t) in
                  (ret_ovars, (op_neg ivars t))
            |  RA_Prod(tlist) -> (build_product ivars rcr_calc tlist)
            |  RA_Sum(tlist)  -> (build_sum     ivars rcr_calc tlist)
         in
         (ListAsSet.union ivars new_vars)
   in
   let term = Calculus.readable_relcalc (get_definition pterm) in
      match (get_internals pterm) with 
      |  Expression      -> rcr_term (get_ivars pterm) term
      |  BigSum(bs_vars) -> 
         (  match term with 
            |  AggSum(bs_term, bs_calc) -> 
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

   ;;
   
end
(*
   let simplified_delta_terms (delete:bool) (relname:string) (tuple:var_t list)
                              (pterm:piped_term_t): (delta_term_t list) =
      let ((delta_calc:term_t list),(new_bs_vars:var_t list)) = 
         List.split 
            (Calculus.simplify 
               (Calculus.term_delta 
                  (get_subterm_mappings pterm)
                  delete
                  relname
                  tuple
                  (get_definition pterm)
               )
               tuple
               (match (get_internals pterm) with | External -> [] | Bigsum(v) -> v)
               (get_vars pterm)
            )
      in
         (List.map
*)