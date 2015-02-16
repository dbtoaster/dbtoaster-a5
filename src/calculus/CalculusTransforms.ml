(**
   Tools for optimizing/simplifying Calculus Expressions
*)

open Type
open Constants
open Ring
open Arithmetic
open Calculus

(**/**)
module C = Calculus

let next_temp_name = 
   FreshVariable.declare_class "calculus/CalculusTransforms"
                               "calc_transform_temp_var"
let mk_temp_var term = 
   (next_temp_name (), C.type_of_expr term)
(**/**)
;;
(**
 [combine_values expr]
 
   recursively merges together Value terms appearing in expr.  Ring operators 
   are pushed into the ValueRing, and expressions are evaluated to the fullest
   extent possible.  Additionally, negations are converted to multiplication
   by -1.
   @param expr The calculus expression to be processed
 *)
let rec combine_values ?(aggressive=false) ?(peer_groups=[])
                       (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-COMBINE-VALUES" (fun () ->
      "Combine Values (START): "^(CalculusPrinter.string_of_expr big_expr) 
   );  
   (* Peer group functions *)
   let pg_eq = fun x y -> ListAsSet.seteq x y in
   let pg_uniq = ListAsSet.uniq ~eq:pg_eq in
   let pg_union = ListAsSet.union ~eq:pg_eq in
   let pg_multiunion = ListAsSet.multiunion ~eq:pg_eq in
   (* let pg_validate grp = if List.length grp > 1 then [ grp ] else [] in  *)
   (* let pg_create vars = pg_validate (ListAsSet.uniq vars) in    *)
   let pg_create vars = [ ListAsSet.uniq vars ] in

   let merge merge_consts calc_op val_op peer_groups elem_list =

      let support_fn = (fun bucket ->
        let bucket_vars = ListAsSet.multiunion (
          List.map Arithmetic.vars_of_value 
                   (List.flatten (List.map snd bucket)))
        in 
          (* Filter out bucket if covered by multiple peer groups.
             Consequently, we won't merge A and B in expression
             R(A) * S(A,B) * {A} * {B}                            *)
          List.length 
            (List.filter (ListAsSet.subset bucket_vars) peer_groups) = 1
      ) in

      let rec apriori support_fn buckets =
        let supported_buckets = List.filter support_fn buckets in
        match supported_buckets with
          | [] -> []
          | head::tail -> 
            let candidates = ListAsSet.uniq 
              ~eq:(fun al bl ->  ListAsSet.seteq (List.map fst al) 
                                                 (List.map fst bl))
              (List.map (fun al -> ListAsSet.uniq (List.flatten al))
                 (ListExtras.subsets_of_size 2 supported_buckets))
            in

            Debug.print "LOG-COMBINE-VALUES-DETAIL" (fun () -> 
              "Candidates: "^ 
              ListExtras.ocaml_of_list (
                ListExtras.ocaml_of_list (fun (i, vals) -> 
                  "("^(string_of_int i)^","^
                    (ListExtras.ocaml_of_list Arithmetic.string_of_value 
                                              vals)^")")
              ) candidates
            );
            (* Note: We could additionally filter candidates by checking 
               for unsupported buckets; for simplicity, we don't do that *)  
            begin match (apriori support_fn candidates) with
              | [] -> head
              | bucket -> bucket
            end
      in

      let merge_components components = 
        let rec rcr tagged_components = 
          let unary_buckets = List.map (fun x -> [x]) tagged_components in
          let cmps_to_merge = apriori support_fn unary_buckets in
          if List.length cmps_to_merge < 2 then tagged_components
          else 
            (-1, List.flatten (List.map snd cmps_to_merge)) :: 
            rcr (ListAsSet.diff tagged_components cmps_to_merge)
        in
        let tagged_components = 
          List.mapi (fun i c -> (i,c)) components
        in
          List.map snd (rcr tagged_components)
      in
 
      Debug.print "LOG-COMBINE-VALUES-DETAIL" (fun () ->
        "Peer groups: "^(string_of_bool merge_consts)^" "^
        ListExtras.ocaml_of_list 
          (ListExtras.ocaml_of_list string_of_var) peer_groups^"\nExpr: "^
        CalculusPrinter.string_of_expr (calc_op elem_list)
      );

      let (val_list, calc_list) = List.fold_right (fun term (v, c) ->
        match term with
          | CalcRing.Val(Value(new_v)) -> (new_v :: v, c)

         (* If merge_consts is true (i.e., we deal with a sum list), we
            want to pull in constants into value terms of product lists,
             {const1} * {const2} * ... * {value} -> {const * value}
            as this would allow us to merge sum terms later on. 
             R(A) * ({1} + {-1} * {A}) -> 
             R(A) * ({1} + {-1 * A}) ->
             R(A) * {1 + (-1 * A)}
            If such terms are not merged, the constant is pulled out again. 
          *)
          | CalcRing.Prod(plist) when merge_consts ->            
            begin try
              let (const_terms, value_terms) = 
                List.fold_left (fun (cterms, vterms) term ->
                  match term with
                    | CalcRing.Val(Value(v)) when Arithmetic.value_is_const v -> 
                      (cterms @ [v], vterms)
                    | CalcRing.Val(Value(v)) when vterms = [] -> 
                      (cterms, [v])
                    | _ -> raise Not_found
                ) ([], []) plist
              in
                (Arithmetic.eval_partial
                   (ValueRing.mk_prod (const_terms @ value_terms)) :: v, c)
              with Not_found -> (v, term :: c)
            end 
          | _ -> (v, term :: c)
      ) elem_list ([],[]) in

      Debug.print "LOG-COMBINE-VALUES-DETAIL" (fun () -> 
        "Value list: "^
        (ListExtras.ocaml_of_list Arithmetic.string_of_value val_list)^
        "\nCalc list: "^
        (ListExtras.ocaml_of_list Calculus.string_of_expr calc_list)
      );

      if val_list = [] then calc_op calc_list
      else let val_term = 
         if aggressive then 
            C.mk_value (Arithmetic.eval_partial (val_op val_list))
         else
         (* If we let it run free, combine values would be a little too 
            aggressive.  Specifically consider the expression 
               R(A) * S(B) * (A + B)
            The (A+B) makes our lives harder, since now we can't materialize
            these two terms separately.  That is to say, using << >> as a
            materialization operator, we want to materialize this expression as
               << R(A) * A >> * << S(B) >> + << R(A) >> * << S(B) * B >>.
            Which is something that we'd get with the standard decomposition.
            However, once the values are combined, this is not possible.  Thus
            When we combine variables, we always respect the hypergraph, and 
            only merge connected components (terms without variables can be
            safely merged into one). 
            
            Thus, the default is to be nonaggressive, as follows:
            *)
         let (number_values, variable_values) = 
           List.partition Arithmetic.value_is_const val_list
         in
         let partitioned_variable_values = 
            HyperGraph.connected_components Arithmetic.vars_of_value
                                            variable_values
         in

         Debug.print "LOG-COMBINE-VALUES-DETAIL" (fun () ->
           "Partitioned values: "^
           ListExtras.ocaml_of_list 
             (fun vals -> Arithmetic.string_of_value (val_op vals))
             partitioned_variable_values 
         );

         (* Merge connected components if they are entirely covered by exactly 
            one peer group. The algorithm tries to recursively merge components 
            using a simplified version of the Apriori algorithm. *)
         let merged_variable_values =            
            merge_components partitioned_variable_values
         in

         let final_term_components = 
            if (List.length merged_variable_values > 0) && merge_consts
            then (number_values @ (List.hd merged_variable_values)) ::
                     (List.tl merged_variable_values)
            else number_values :: merged_variable_values
         in

         (* If merge_consts is true (i.e., we deal with a sum list),
            undo the effect of combining constants for unmerged terms *)
         let (fixed_value_list, fixed_calc_list) =         
           if not merge_consts then (final_term_components, []) else

           List.fold_left (fun (value_list, calc_list) component ->
             if List.mem component partitioned_variable_values then
               begin match component with
                 | [ ValueRing.Prod(ValueRing.Val(const_v) :: rest_v) ] 
                   when Arithmetic.value_is_const (ValueRing.mk_val const_v) ->
                   let new_calc = 
                     CalcRing.mk_prod [ 
                       C.mk_value (ValueRing.mk_val const_v);
                       C.mk_value (ValueRing.mk_prod rest_v) ]
                   in 
                     (value_list, calc_list @ [new_calc]) 
                 | _ -> (value_list @ [component], calc_list)
               end 
             else (value_list @ [component], calc_list)            
           ) ([], []) final_term_components 
         in
            calc_op (List.map (fun val_list -> C.mk_value (
               Arithmetic.eval_partial (val_op val_list)
            )) (fixed_value_list) @ fixed_calc_list)
      in      
      if calc_list = [] then val_term
      else calc_op (calc_list @ [val_term])
   in

   let rec rcr ?(peer_groups=[]) expr = begin match expr with

     | CalcRing.Sum(terms) -> 
       let (pg_bucket_list, new_terms) = 
         List.split (List.map (rcr ~peer_groups:peer_groups) terms) 
       in
       let pg_bucket = 
         List.map ListAsSet.multiinter (ListExtras.distribute pg_bucket_list)
       in 
       (* Enforce the representation invariant *)
       let sum_terms = (CalcRing.sum_list (CalcRing.mk_sum new_terms)) in       
         ( (* Compute new peer groups; keep only dominant, remove reduntant *)
            pg_uniq (List.filter ( fun pg -> match pg with
              | [] -> false
              | c -> not (List.exists (ListAsSet.proper_subset c) pg_bucket)
            ) pg_bucket),
           (* Compute new expression *)
           merge true CalcRing.mk_sum ValueRing.mk_sum peer_groups sum_terms
         )

     | CalcRing.Prod(terms) -> 
       let (pg_bucket_list, new_terms) = snd (
         List.fold_left (fun (acc_pg, (pgl, tl)) term -> 
           let (pg, new_term) = rcr ~peer_groups:acc_pg term in
           (pg_union acc_pg pg, (pgl @ [ pg ], tl @ [ new_term ]))
         ) (peer_groups, ([],[])) terms)
       in
       (* Enforce the representation invariant *)
       let prod_terms = (CalcRing.prod_list (CalcRing.mk_prod new_terms)) in
       let pg_bucket = pg_multiunion pg_bucket_list in
         ( (* Compute new peer groups *)
           pg_bucket,
           (* Compute new expression *)
           merge false CalcRing.mk_prod ValueRing.mk_prod 
             (pg_union peer_groups pg_bucket) prod_terms
         )
     | CalcRing.Neg(term) ->
       (peer_groups,
        CalcRing.mk_prod [ C.mk_value (Arithmetic.mk_int (-1)); 
                           snd (rcr ~peer_groups:peer_groups term) ])

     | CalcRing.Val(lf) -> 
       begin match lf with
         | Cmp((Neq|Lt|Gt ),x,y) when x = y  -> 
         (* Zero is contageous.  This particular case following case wreaks 
            havoc on the schema of the expression being optimized. Fortunately 
            though, the presence of Exists makes this point moot. Since lifts
            can't extend the schema, and since Exists(0) = 0, the zero should
            either only affect subexpressions where the variables in question 
            are already in-scope, or should be able to propagate further up 
            and kill anything that would have otherwise been able to bind the 
            variable.
            
            Even so, there's a flag to disable this.
         *)
           ([], 
            if Debug.active "CALC-DONT-CREATE-ZEROES" then expr 
            else CalcRing.zero)
         
         | Cmp((Eq|Gte|Lte),x,y) when x = y -> ([], CalcRing.one)
         | Cmp(op,x,y) ->
         (* Pre-Evaluate comparisons wherever and as much as possible *)
           ([], 
            begin match ((Arithmetic.eval_partial x),
                         (Arithmetic.eval_partial y)) with
              | (ValueRing.Val(AConst(x_const)), 
                 ValueRing.Val(AConst(y_const))) ->
                begin match (Constants.Math.cmp op x_const y_const) with
                  | CBool(true) -> CalcRing.one
                  | CBool(false) -> 
                    if Debug.active "CALC-DONT-CREATE-ZEROES" 
                    then C.mk_cmp op (Arithmetic.mk_const(x_const))
                                     (Arithmetic.mk_const(y_const))
                    else CalcRing.zero
                  | _ -> C.bail_out expr
                           "Unexpected return value of comparison op"
                end
              | (x_val, y_val) -> 
                let ret = C.mk_cmp op x_val y_val in
                  Debug.print "LOG-COMBINE-VALUES" (fun () ->
                     "Combining "^(C.string_of_leaf lf)^" into "^
                     (C.string_of_expr ret)
                  ); ret
            end)
         
         | Value(v) -> ([], C.mk_value (Arithmetic.eval_partial v))
         | Rel(_,_) 
         | DeltaRel(_,_) 
         | External(_)  -> (pg_create (snd (schema_of_expr expr)), expr)
         | AggSum(gb_vars, subexp) -> 
            let new_subexp = snd (rcr subexp) in
            if new_subexp = CalcRing.zero then ([], CalcRing.zero) 
            else ([], C.mk_aggsum gb_vars new_subexp)
         | Lift(lift_v, subexp)    -> 
            ([], C.mk_lift lift_v (snd (rcr subexp)))
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp)    -> 
            let new_subexp = snd (rcr subexp) in
            if new_subexp = CalcRing.zero then ([], CalcRing.zero)
            else ([], C.mk_exists new_subexp)
(***** END EXISTS HACK *****)
         | DomainDelta(subexp) -> 
            let new_subexp = snd (rcr subexp) in
            if new_subexp = CalcRing.zero then ([], CalcRing.zero) 
            else ([], C.mk_domain new_subexp)
       end      
     end
   in
   let rewritten_expr = snd (rcr big_expr) in
      Debug.print "LOG-COMBINE-VALUES" (fun () ->
         "Combine Values (END): "^
         (CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr
;;

(* A helper method for partially evaluating constant terms *) 
let eval_partial pl = 
   (* Extract constants *)
   let (consts, nonconsts) = 
      List.fold_left (fun (c, n) term -> match term with
         | CalcRing.Val(Value(v)) when Arithmetic.value_is_const v -> 
            (c @ [v], n)
         | _ -> (c, n @ [term])
      ) ([], []) pl 
   in
   let const_eval = 
      C.mk_value (Arithmetic.eval_partial (ValueRing.mk_prod consts))
   in
      CalcRing.mk_prod (const_eval :: nonconsts)
;;

(* Negate a given expression *)
let neg expr = eval_partial (C.mk_value (Arithmetic.mk_int (-1)) :: 
                             CalcRing.prod_list expr)
;;

(* Normalize a given expression by replacing all Negs with {-1} and 
   evaluating all constants in the product list. *)
let rec normalize expr = 
   C.rewrite 
      (fun _ -> CalcRing.mk_sum)
      (fun _ -> eval_partial)
      (fun _ -> neg)
      (fun _ -> CalcRing.mk_val)
      (fun _ _ -> true)
      expr
;;


(**/**)
(** Type used internally by lift_equalities to describe liftable equalities. *)
type lift_candidate_t = 
   (* A var=var equality that can be lifted in either direction *)
   | BidirectionalLift of var_t * var_t
   (* An equality that can only be lifted one way *)
   | UnidirectionalLift of var_t * value_t
(**/**)
(**
  [lift_equalities scope expr]
  
    Recursively commutes equality comparison terms as far left through the 
    expression as possible.  Once the term commutes all the way to the left, 
    if it is possible to commute it further by converting it to a lift, this
    function does so.
 
    Because it's as good a place to do it as anywhere, lift_equalities will
    also delete obviously irrelevant equalities (i.e., equalities of the form 
    X=X will be replaced by 1).
 
    @param scope  Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
    @param expr   The calculus expression to be processed
*)
let lift_equalities (global_scope:var_t list) (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
      "Lift Equalities (START): "^(CalculusPrinter.string_of_expr big_expr) 
   );
   let candidate_term (local_scope:var_t list) (candidate:lift_candidate_t) = 
      match candidate with
         | BidirectionalLift(x, y)  -> 
            if (List.mem x local_scope)
            then if (List.mem y local_scope)
                 then C.mk_cmp Eq (Arithmetic.mk_var x) 
                                         (Arithmetic.mk_var y)
                 else C.mk_lift y 
                         (C.mk_value (Arithmetic.mk_var x))
            else if (List.mem y local_scope)
                 then C.mk_lift x
                         (C.mk_value (Arithmetic.mk_var y))
                 else (Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                        "Scope of error is : " ^
                        (ListExtras.ocaml_of_list string_of_var local_scope)
                      );
                      C.bail_out big_expr 
                         "Error: lifted equality past scope of both vars")
         | UnidirectionalLift(x, y) -> 
            if not (List.for_all (fun y_var -> List.mem y_var local_scope)
                                 (Arithmetic.vars_of_value y))
            then (Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
                        "Scope of error is : " ^
                        (ListExtras.ocaml_of_list string_of_var local_scope)
                      );
                 failwith "Error: lifted equality past scope of value")
            else if (List.mem x local_scope)
                 then C.mk_cmp Eq (Arithmetic.mk_var x) y
                 else C.mk_lift x (C.mk_value y)
   in
   let merge (local_scope:var_t list)
             ((candidates:((lift_candidate_t) list list)),
              (terms:C.expr_t list)): C.expr_t = 
      CalcRing.mk_prod ((List.map (candidate_term local_scope)
                                  (List.flatten candidates)) @
                        terms)
   in
   let rec rcr (scope:var_t list) (expr:C.expr_t):
               ((lift_candidate_t) list * C.expr_t) = 
      Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () ->
         "Lift Equalities: "^
         (CalculusPrinter.string_of_expr expr)^
         "\n\t\tWith Scope: "^
         (ListExtras.ocaml_of_list string_of_var scope)
      );
      let rcr_merge x = merge scope (List.split [rcr scope x]) in
      begin match expr with
         | CalcRing.Sum(sl) -> 
            ([], (CalcRing.mk_sum (List.map rcr_merge sl)))
         | CalcRing.Prod([])    -> ([], CalcRing.one)
         | CalcRing.Prod(pt::pl) ->
            let pt_ovars = (snd (schema_of_expr pt)) in
            let entering_scope = (ListAsSet.diff pt_ovars scope) in
            let rhs_scope = (ListAsSet.union scope pt_ovars) in
            let (rhs_eqs, rhs_term) = rcr rhs_scope (CalcRing.mk_prod pl) in
            let (lhs_eqs, lhs_term) = rcr scope pt in
            (* There are three possibilities for each equality x = y in rhs_eqs:
               1 - (x=y) commutes wiith lhs_term.  In this case, we keep 
                   shifting it left by returning it as an equality
               2 - (x=y) doesn't commute with lhs_term because:
                     a - x would fall out of scope, but all variables in y 
                         remain in scope.
                     b - y is a single variable and would fall out of scope 
                         while x remains in scope.
                   If either of these is true, we can convert the equality to a
                   lift expression appearing to the left of lhs_term in the 
                   product
               3 - All other cases.  The equality can not be converted into a
                   lift expression and must be re-inserted into the product 
                   terms to the right of lhs_term.  TODO: It might be possible  
                   to rewrite (x=y) in such a way that it falls into case 2.
                   
               There is, of course a shortcut.  If nothing enters scope in this
               expression, then we can always commute anything past it
            *)
            if entering_scope = [] then (lhs_eqs @ rhs_eqs, 
                                         CalcRing.mk_prod [lhs_term; rhs_term])
            else
            let (commuting_eqs, updated_lhs) = 
               List.fold_left (fun (commuting_eqs, updated_lhs) candidate ->
                  begin match candidate with
                  | UnidirectionalLift(x, y) ->
                     let y_enters_scope = 
                        let y_vars = (vars_of_value y) in
                           (List.exists (fun v -> List.mem v entering_scope)
                                        y_vars) 
                           && (y_vars <> [])
                     in
                     if y_enters_scope then (
                        (* If y enters scope here, then it's not possible to
                           lift this expression (for now, this is true even if x
                           doesn't enter scope here) *)
                        Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () ->
                           "Aborting search for unidirectional lift "^
                           (string_of_var x)^" ^= "^
                              (CalculusPrinter.string_of_value y)^
                           " due to scope"
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod 
                              [updated_lhs; 
                               candidate_term rhs_scope candidate])
                     ) else if List.mem x entering_scope then (
                        (* x enters scope here, but y does not.  We can
                           lift this equality *)
                        Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () ->
                           "Found lift location for unidirectional lift "^
                           (string_of_var x)^" ^= "^
                              (CalculusPrinter.string_of_value y)^" in:\n"^
                              (CalculusPrinter.string_of_expr updated_lhs)
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              C.mk_lift x (C.mk_value y);
                              updated_lhs])
                     ) else
                        (* Neither x nor y enter scope here.  We commute the
                           equality further. *)
                        (  commuting_eqs @ [candidate], updated_lhs  )
                  | BidirectionalLift(x, y) ->
                     begin match (List.mem x entering_scope, 
                            List.mem y entering_scope) with
                     | (true, true) -> (* x and y enter scope: abort *)
                        Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () ->
                           "Aborting search for bidirectional lift "^
                           (string_of_var x)^" ^=^ "^(string_of_var y)^
                           " due to simultaneous scope entrance"
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod 
                              [updated_lhs; 
                               candidate_term rhs_scope candidate])
                     | (false, true) -> (* y enters scope, x does not: 
                                           lift into y *)
                        Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () ->
                           "Found lift location for bidirectional lift "^
                           (string_of_var y)^" ^= "^(string_of_var x)^
                              " in:\n"^
                              (CalculusPrinter.string_of_expr updated_lhs)
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              C.mk_lift y 
                                 (C.mk_value (Arithmetic.mk_var x));
                              updated_lhs])
                     | (true, false) -> (* x enters scope, y does not: 
                                           lift into x *)
                        Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () ->
                           "Found lift location for bidirectional lift "^
                           (string_of_var x)^" ^= "^(string_of_var y)^
                              " in:\n"^
                              (CalculusPrinter.string_of_expr updated_lhs)
                        );
                        (  commuting_eqs, 
                           CalcRing.mk_prod [
                              C.mk_lift x 
                                 (C.mk_value (Arithmetic.mk_var y));
                              updated_lhs])
                     | (false, false) -> (* neither enters scope: commute *)
                        (  commuting_eqs @ [candidate], updated_lhs  )
                     end
                  end
               ) ([],lhs_term) rhs_eqs
            in
               (lhs_eqs @ commuting_eqs, 
                CalcRing.mk_prod [updated_lhs; rhs_term])
         | CalcRing.Neg(nt) -> 
            let (eqs, term) = rcr scope nt in
               (eqs, CalcRing.mk_neg term)
         | CalcRing.Val(AggSum(gb_vars, term)) ->
            (** It's possible that we'll take advantage of a variable in-scope
                to unify some of the gb-vars away.  If this happens, we need
                to update the gb_vars *)
            let new_term = rcr_merge term in
            let new_gb_vars = ListAsSet.inter gb_vars
                                              (snd (C.schema_of_expr new_term))
            in
               ([], C.mk_aggsum new_gb_vars new_term)
         | CalcRing.Val(Lift(v, term)) ->
            ([], C.mk_lift v (rcr_merge term))
(***** BEGIN EXISTS HACK *****)
         | CalcRing.Val(Exists(term)) ->
            ([], C.mk_exists (rcr_merge term))
(***** END EXISTS HACK *****)
         | CalcRing.Val(Cmp(Eq, x, y)) when x = y ->
            (* X = X is a no-op *)
            ([], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, ValueRing.Val(AVar(x)), 
                                ValueRing.Val(AVar(y))))
               when (snd x) = (snd y) ->
            Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () -> 
               "Bidirectional lift candidate "^(string_of_var x)^" ^=^ "^
               (string_of_var y)
            );
            ([BidirectionalLift(x, y)], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, ValueRing.Val(AVar(x)), y)) 
               when (Type.can_escalate_type (type_of_value y) (snd x)) ->
            Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () -> 
               "Unidirectional lift candidate "^(string_of_var x)^" ^= "^
               (CalculusPrinter.string_of_value y)
            );
            ([UnidirectionalLift(x, y)], CalcRing.one)
         | CalcRing.Val(Cmp(Eq, x, ValueRing.Val(AVar(y))))
               when (Type.can_escalate_type (type_of_value x) (snd y)) ->
            Debug.print "LOG-LIFT-EQUALITIES-DETAIL" (fun () -> 
               "Unidirectional lift candidate "^(string_of_var y)^" ^= "^
               (CalculusPrinter.string_of_value x)
            );
            ([UnidirectionalLift(y, x)], CalcRing.one)
         | CalcRing.Val(Cmp _) 
         | CalcRing.Val(External _)
         | CalcRing.Val(Rel _)
         | CalcRing.Val(DeltaRel _)
         | CalcRing.Val(DomainDelta _)
         | CalcRing.Val(Value _) -> 
            ([], expr)
      end
   in
   let rewritten_expr = 
      merge global_scope (List.split [rcr global_scope big_expr])
   in
      Debug.print "LOG-LIFT-EQUALITIES" (fun () ->
         "Lift Equalities (END): "^
         (CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr
;;

(**/**)
(** Thrown when a potential unification would fail in order to back out of 
    unification and undo any changes that have been made.  The one field is the 
    reason for the failure. *)
exception CouldNotUnifyException of string;;
(**/**)

(**
  [unify_lifts scope schema expr]
  
   Where possible, variables defined by Lifts are replaced by the lifted 
   expression.  If possible, the Lift term is removed.  We do this by
   unfolding products, substituting the lifted expression throughout.  It's 
   possible that such a substitution will not be possible: e.g., if we're 
   lifting an aggregate expression and the lifted variable appears in a 
   comparison.  If so, then we do not remove the Lift term.

   The other thing that can prevent the removal of a lift is if the variable
   being lifted into is present in the output schema of the enclosing 
   expression.  In short, this step only unifies lifted variables in the 
   context of a single expression (product, etc...), and doesn't propagate
   the unified variable up through the AST.  

   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param schema  The expected output variables of the expression 
                  being unified.
                  These variables will never be unified away
   @param expr    The calculus expression being processed
*)
let unify_lifts (big_scope:var_t list) (big_schema:var_t list) 
                (big_expr:C.expr_t): C.expr_t =
   Debug.print "LOG-UNIFY-LIFTS" (fun () ->
      let (in_sch, out_sch) = C.schema_of_expr big_expr in
      "Unify Lifts (START): "^
      (ListExtras.ocaml_of_list string_of_var in_sch)^
      (ListExtras.ocaml_of_list string_of_var out_sch)^": \n"^
      (CalculusPrinter.string_of_expr big_expr) 
   );
   let unify (force:bool) (lift_v:var_t) (expr_sub:C.expr_t) (scope:var_t list) 
             (schema:var_t list) (expr:C.expr_t): C.expr_t =
      Debug.print "LOG-UNIFY-LIFTS-DETAIL" (fun () ->
         "Attempting to unify ("^(string_of_var lift_v)^" ^= "^
         (CalculusPrinter.string_of_expr expr_sub)^") with scope/schema "^
         (ListExtras.ocaml_of_list string_of_var scope)^
         (ListExtras.ocaml_of_list string_of_var schema)^"\nin: "^
         (CalculusPrinter.string_of_expr expr)
      );
      let var_sub msg=
         match expr_sub with 
         | CalcRing.Val(Value(ValueRing.Val(AVar(v)))) -> v
         | _ when force -> lift_v
         | _ -> raise (CouldNotUnifyException("Could not put '"^
                   (CalculusPrinter.string_of_expr expr_sub)^"' into a"^msg))
      in
      let val_sub ?(aggressive = false) msg =
         match (combine_values ~aggressive:aggressive expr_sub) with 
         | CalcRing.Val(Value(v)) -> v
         | _ when force -> Arithmetic.mk_var lift_v
         | _ -> raise (CouldNotUnifyException("Could not put '"^
                   (CalculusPrinter.string_of_expr expr_sub)^"' into a"^msg))
      in
      let map_vars msg = 
         List.map (fun x -> if x = lift_v then var_sub msg else x)
      in
      let make_cmp msg =
         let subexp_v = var_sub msg in
         C.mk_cmp Eq (Arithmetic.mk_var lift_v) (Arithmetic.mk_var subexp_v)
      in
      (* If the lift variable is in the schema, then we can't unify it --
         Otherwise, we'd shrink the schema.  In some cases, a unification of 
         this sort is actually possible, but we don't handle them here.  
          - If the schema is defined by an AggSum, nested_rewrites will pull
            the Lift out of the AggSum (and update the schema accordingly)
          - If the schema is based on the expression itself, we have to handle
            things on a case-by-case basis, because whatever is going to be 
            evaluating the expression has to be made aware of the schema 
            changes.  For example, in Compiler, this is handled by 
            extract_renamings. *)
      if (not force) && List.mem lift_v schema then 
         raise (CouldNotUnifyException("Lift var is in the schema"));
      
      (* If the lift variable is already in-scope, then we can't unify it, 
         because it's serving the role of an equality predicate.  We could try 
         to turn it into an equality predicate here, but that's unnecessary.
          - If the term that brought the lift variable into scope is itself a
            lift, then we could only have gotten to this point if that lift was
            not unifiable for other reasons.
          - Otherwise, the next pass through advance_lifts and/or 
            nesting_rewrites will move this lift up and left, and then we should
            be able to unify it normally without trouble. *)
      if (not force) && List.mem lift_v scope then
         raise (CouldNotUnifyException("Lift var is in the scope"));

      (* We unify by replacing expressions with CalcRing.one.  This is only 
         correct if the lifted expression is a singleton.  For example, we 
         could implement COUNT(UNIQUE A) as 
            AggSum([], (foo ^= R(A)) * {foo > 0})
         The nested expression cannot be safely replaced.  However, if we get:
            (A ^= ...) * AggSum([], (foo ^= R(A)) * {foo > 0})
         then the nested aggregate is always over a single element. *)
      if not (C.expr_is_singleton ~scope:scope expr_sub) then (
         if force then expr else
         raise (CouldNotUnifyException("Can only unify lifts of singletons"));
      ) else 

      let expr_scope = ListAsSet.union scope [lift_v] in
      let rewritten = 
      C.rewrite_leaves ~scope:expr_scope ~schema:schema 
        (fun _ x -> begin match x with
         (* No Arithmetic over lone values means they can take non-float/int
            values. *)
         | Value(ValueRing.Val(AVar(v))) 
            when v = lift_v && 
              (not force || rels_of_expr expr_sub = []) -> expr_sub
         | Value(v) when List.mem lift_v (Arithmetic.vars_of_value v) -> 
            C.mk_value(
               Arithmetic.eval_partial 
                  ~scope:[lift_v, val_sub " value expression"] v
            )
         | Value(_) -> CalcRing.mk_val x
         | AggSum(gb_vars, subexp) ->         
            let subexp_ovars = snd (C.schema_of_expr subexp) in         

            (*  Update gb_vars only if unification has removed 
                lift_v from the schema of subexp; counterexample:
                   S(C) * (A ^= C) * AggSum([A], R(B) * (A ^= B)) 
                Here, we don't update gb_vars ([A]).             *)
            if List.mem lift_v subexp_ovars then C.mk_aggsum gb_vars subexp 
            else
 
            (* subexp is rewritten.  We just need to update gb_vars properly.
               This can only change (in this unify function) if the lhs variable
               appears in the GB terms.  
                  - If lift_v is bound in the expression, then subst may not be
                    bound if lift_v is bound by a lift.  E.g.  
                       (A ^= X) * AggSum([A], (A ^= X) * ...) -> 
                          AggSum([], ...)
                  - If the variable is not bound in the expression, but the 
                    subst is (and is a variable), then the new subst had better 
                    already be in the group-by variables.  
            *)        
            let mapped_gb_vars = map_vars "n aggsum group-by var" gb_vars in
            let new_gb_vars = ListAsSet.inter mapped_gb_vars subexp_ovars in            
               C.mk_aggsum new_gb_vars subexp
         | Rel(rn, rv) ->           
            let new_rv = map_vars " relation var" rv in
            if ListAsSet.has_no_duplicates new_rv
            then C.mk_rel rn new_rv
            else 
               (* In order to prevent expressions of the form R(dA,dA), *)
               (* we transform (B^=dA)*R(dA,B) into R(dA,B)*(B=dA).     *)
               CalcRing.mk_prod [ C.mk_rel rn rv; 
                                  make_cmp " relation var" ]
         | DeltaRel(rn, rv) ->           
            let new_rv = map_vars " delta relation var" rv in
            if ListAsSet.has_no_duplicates new_rv
            then C.mk_deltarel rn new_rv
            else 
               (* In order to prevent expressions of the form R(dA,dA), *)
               (* we transform (B^=dA)*R(dA,B) into R(dA,B)*(B=dA).     *)
               CalcRing.mk_prod [ C.mk_deltarel rn rv; 
                                  make_cmp " delta relation var" ]
         | DomainDelta(subexp) -> C.mk_domain subexp
         | External(en, eiv, eov, et, em) ->
            (* The metadata is already rewritten *)
            let new_eiv = map_vars "n external input var" eiv in
            let new_eov = map_vars "n external output var" eov in
            if ListAsSet.has_no_duplicates new_eiv &&
               ListAsSet.has_no_duplicates new_eov
            then C.mk_external en new_eiv new_eov et em
            else
            
            (* In order to prevent expressions of the form M[][dA,dA], *)
            (* we transform (B^=dA)*M[][dA,B] into M[][dA,B]*(B=dA).   *)
            CalcRing.mk_prod [ 
               C.mk_external en (if ListAsSet.has_no_duplicates new_eiv 
                                 then new_eiv else eiv) 
                                (if ListAsSet.has_no_duplicates new_eov 
                                 then new_eov else eov) et em;
               make_cmp " n external var" ]
         | Cmp(op, lhs, rhs) when 
            (List.mem lift_v (Arithmetic.vars_of_value lhs)) ||
            (List.mem lift_v (Arithmetic.vars_of_value rhs)) ->
            let new_lhs = Arithmetic.eval_partial 
                     ~scope:[lift_v, val_sub " cmp expression"] lhs 
            in
            let new_rhs = Arithmetic.eval_partial 
                     ~scope:[lift_v, val_sub " cmp expression"] rhs 
            in
               C.mk_cmp op new_lhs new_rhs

         | Cmp(op, lhs, rhs) -> C.mk_cmp op lhs rhs


(***** BEGIN EXISTS HACK *****)
         | Exists(subexp) -> C.mk_exists subexp
            (* Subexp is rewritten.  No changes need to be made here *)
(***** END EXISTS HACK *****)

         | Lift(v, subexp) when (v = lift_v) && (subexp = expr_sub) &&
           (match subexp with 
              | CalcRing.Val(Value(ValueRing.Val(AVar(v)))) -> true 
              | _ -> false) ->
               (* We don't allow expressions like R(5,B) or R(4+C,B), thus,
                  when subexp is not a variable, unification could lose 
                  an output variable. For example (simplified TPCH Q8 and Q19): 
                     (X ^= 42) * AggSum([X], R(A,B) * (X ^= 42)) 
                  would get transformed into a wrong expression
                     (X ^= 42) * AggSum([], R(A,B)) *)
            CalcRing.one
         | Lift(v, subexp) when (v = lift_v) && (not force) ->
            (* If the subexpressions aren't equivalent, then we should turn this
               into an equality test on the two.  lift_equalities will do 
               something more intelligent later if possible.  Note that here, 
               for the sake of code simplicity we take a slight hit: we cannot
               do comparisons over expressions. *)
            begin match combine_values ~aggressive:true subexp with
               | CalcRing.Val(Value(subexp_v)) ->
                  C.mk_cmp 
                     Eq subexp_v (val_sub ~aggressive:true " lift comparison")
               | _ -> raise (CouldNotUnifyException("Conflicting Lift"))
            end
            (* the subexp is already rewritten *)
         | Lift(v, subexp) -> C.mk_lift v subexp
            
      end) 
      (fun (local_scope, _) _ -> List.mem lift_v local_scope)
      expr
      in 
         Debug.print "LOG-UNIFY-LIFTS-DETAIL" (fun () ->
            "Successfully unified ("^(string_of_var lift_v)^" ^= "^
            (CalculusPrinter.string_of_expr expr_sub)^") in: "^
            (CalculusPrinter.string_of_expr expr)^"\nto: "^
            (CalculusPrinter.string_of_expr rewritten)
         ); rewritten

   in
   let rec rcr (scope:var_t list) (schema:var_t list) (expr:C.expr_t):C.expr_t =
      Debug.print "LOG-UNIFY-LIFTS-DETAIL" (fun () ->
         "Attempting to unify lifts in: "^
         (ListExtras.ocaml_of_list string_of_var scope)^
         (ListExtras.ocaml_of_list string_of_var schema)^"\n"^
         (CalculusPrinter.string_of_expr expr)
      );
      begin match expr with 
      | CalcRing.Val(Value(_))
      | CalcRing.Val(Rel(_,_)) 
      | CalcRing.Val(DeltaRel(_,_))
      | CalcRing.Val(External(_)) 
      | CalcRing.Val(Cmp(_,_,_)) -> expr
      
      | CalcRing.Val(DomainDelta(subexp)) ->
         let (expr_ivars, expr_ovars) = C.schema_of_expr expr in
         let expr_vars = ListAsSet.union expr_ivars expr_ovars in
         let subexp_scope = ListAsSet.inter scope expr_vars in         
         let subexp_schema = ListAsSet.inter schema expr_ovars in
         C.mk_domain (rcr subexp_scope subexp_schema subexp)
(***** BEGIN EXISTS HACK *****)
      | CalcRing.Val(Exists(subexp)) -> 
         let (expr_ivars, expr_ovars) = C.schema_of_expr expr in
         let expr_vars = ListAsSet.union expr_ivars expr_ovars in
         let subexp_scope = ListAsSet.inter scope expr_vars in         
         let subexp_schema = ListAsSet.inter schema expr_ovars in         
         C.mk_exists (rcr subexp_scope subexp_schema subexp)
(***** END EXISTS HACK *****)
      
      | CalcRing.Val(AggSum(gb_vars, subexp)) ->
         let (expr_ivars, expr_ovars) = C.schema_of_expr expr in
         let expr_vars = ListAsSet.union expr_ivars expr_ovars in
         let subexp_scope = ListAsSet.inter scope expr_vars in         
         let subexp_schema = ListAsSet.inter schema expr_ovars in         

         let new_subexp = rcr subexp_scope subexp_schema subexp in
         let (_,new_ovars) = C.schema_of_expr new_subexp in
         (* Unifying lifts can do some wonky things to the schema of subexp.
            Among other things, previously unbound variables can become bound,
            and previously bound variables can become unbound.  A variable in
            the schema of an expression will never be unified (although the lift
            can be propagated up and out of the aggsum via nesting_rewrites) *)
         let new_gb_vars = 
            ListAsSet.union
               (ListAsSet.inter gb_vars new_ovars)
               (ListAsSet.inter expr_ivars new_ovars)
         in
            Debug.print "LOG-UNIFY-LIFTS-DETAIL" (fun () ->
               "Updating group-by variables from "^
               (ListExtras.ocaml_of_list string_of_var gb_vars)^" to "^
               (ListExtras.ocaml_of_list string_of_var new_gb_vars)^
               " in AggSum of: "^
               (CalculusPrinter.string_of_expr new_subexp)
            );
            C.mk_aggsum new_gb_vars new_subexp
      
      | CalcRing.Val(Lift(lift_v, subexp)) ->
         let (subexp_ivars, subexp_ovars) = C.schema_of_expr subexp in
         let subexp_vars = ListAsSet.union subexp_ivars subexp_ovars in
         let subexp_scope = ListAsSet.inter scope subexp_vars in
         let subexp_schema = 
            ListAsSet.inter subexp_ovars (ListAsSet.union scope schema) in

         let new_subexp = rcr subexp_scope subexp_schema subexp in (
            try 
               unify false lift_v new_subexp scope schema CalcRing.one;
            with CouldNotUnifyException(msg) ->
               Debug.print "LOG-UNIFY-LIFTS-DETAIL" (fun () ->
                  "Could not unify "^(string_of_var lift_v)^" because: "^msg^
                  " in terminal lift"
               );
               C.mk_lift lift_v new_subexp
         )

      | CalcRing.Sum(sum_terms) ->
         (* This is a bit of a hack for now.  The schema of every term in the
            sum needs to be kept identical.  Factorize will pull out any lifts 
            shared across terms in the sum, and poly decomposition will allow
            unification within a sum.  *)
         let (_,sum_schema) = C.schema_of_expr expr in
         let full_schema = ListAsSet.union sum_schema schema in
            CalcRing.mk_sum (List.map (rcr scope full_schema) sum_terms)
      
      | CalcRing.Neg(neg_term) ->
         CalcRing.mk_neg ((rcr scope schema) neg_term)
      
      | CalcRing.Prod(CalcRing.Val(Lift(lift_v, subexp))::rest) ->
         let (subexp_ivars, subexp_ovars) = C.schema_of_expr subexp in
         let subexp_vars = ListAsSet.union subexp_ivars subexp_ovars in
         let subexp_scope = ListAsSet.inter scope subexp_vars in
         let subexp_schema = 
            ListAsSet.inter subexp_ovars (ListAsSet.union scope schema) in

         let new_subexp = rcr subexp_scope subexp_schema subexp in 
         (
            let rest_expr = CalcRing.mk_prod rest in
            try
               let new_rexpr = 
                  unify false lift_v new_subexp scope schema rest_expr in
               let (new_rivars, new_rovars) = C.schema_of_expr new_rexpr in 
               let new_rvars = ListAsSet.union new_rivars new_rovars in
               let new_rscope = ListAsSet.inter scope new_rvars in
               let new_rschema = ListAsSet.inter new_rovars schema in
                  rcr new_rscope new_rschema new_rexpr

            with CouldNotUnifyException(msg) -> (
               Debug.print "LOG-UNIFY-LIFTS-DETAIL" (fun () ->
                  "Could not unify "^(string_of_var lift_v)^" because: "^msg^
                  " in: \n"^(CalculusPrinter.string_of_expr rest_expr)
               );
               let new_head = C.mk_lift lift_v new_subexp in
               let (_, nhead_ovars) = C.schema_of_expr new_head in

               let simplified_rest = (
                  if Debug.active "AGGRESSIVE-UNIFICATION" 
                  then unify true lift_v new_subexp scope schema rest_expr
                  else rest_expr
               ) in
               let (srest_ivars, srest_ovars) = 
                  C.schema_of_expr simplified_rest in
               let srest_vars = ListAsSet.union srest_ivars srest_ovars in
               let srest_scope = ListAsSet.inter srest_vars
                  (ListAsSet.union scope nhead_ovars) in
               let srest_schema = ListAsSet.inter srest_ovars
                  (ListAsSet.union schema nhead_ovars) in   
               let new_rest = rcr srest_scope srest_schema simplified_rest in   
                  CalcRing.mk_prod [ new_head; new_rest ]
            )
         )

      | CalcRing.Prod(head::rest) ->
         let rest_expr = (CalcRing.mk_prod rest)  in
         let (rest_ivars, rest_ovars) = C.schema_of_expr rest_expr in
         let head_schema = 
            (ListAsSet.multiunion [schema; rest_ivars; rest_ovars]) 
         in
         let new_head = rcr scope head_schema head in
         let (_, head_ovars) = C.schema_of_expr new_head in
         let new_scope = ListAsSet.union scope head_ovars in
         let new_schema = ListAsSet.inter rest_ovars 
            (ListAsSet.union schema head_ovars) 
         in
            CalcRing.mk_prod [ new_head; rcr new_scope new_schema rest_expr ]

      | CalcRing.Prod([]) -> CalcRing.one
   end 
 in
   let rewritten_expr = rcr big_scope big_schema big_expr in
   Debug.print "LOG-UNIFY-LIFTS" (fun () ->
      "Unify Lifts (END): "^
      (CalculusPrinter.string_of_expr rewritten_expr) 
   ); rewritten_expr


(**
  [advance_lifts scope expr]
  
    Move lifts as far to the left as possible -- The current implementation is 
    very heuristic, and can probably be replaced by something more effective.  
    For now though, something simple should be sufficient.

   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param expr    The calculus expression being processed
*)
let advance_lifts scope expr =
   Debug.print "LOG-ADVANCE-LIFTS" (fun () -> 
      "Advance Lifts (START): "^CalculusPrinter.string_of_expr expr^
      "\nScope: "^ListExtras.ocaml_of_list string_of_var scope
   );
   let rewritten_expr = C.rewrite ~scope:scope 
      (fun _ x -> CalcRing.mk_sum x)
      (fun (scope, _) pl ->
          Debug.print "LOG-ADVANCE-LIFTS-DETAIL" (fun () -> 
             "Advance Lifts: processing " ^         
             (CalculusPrinter.string_of_expr (CalcRing.mk_prod pl)) ^
             (ListExtras.ocaml_of_list string_of_var scope)
          );

          (* Extract DomainDelta and DeltaRel terms and pull them upfront.
             Note that DeltaRel terms don't have input variables. DomainDelta
             terms might have input variables -- however, when they do these
             are always bound from outside, so it's safe to push them upfront.*)
          let (domains, deltas, others) = 
             List.fold_left (fun (domains, deltas, others) term -> 
                match term with
                   | CalcRing.Val(DomainDelta _) ->
                      (domains @ [term], deltas, others)
                   | CalcRing.Val(DeltaRel _) -> 
                      (domains, deltas @ [term], others)
                   | _ -> (domains, deltas, others @ [term])
             ) ([], [], []) pl
          in

          CalcRing.mk_prod (List.fold_left (fun curr_ret curr_term -> 
             begin match curr_term with
                | CalcRing.Val(Lift(_,_)) -> 
                   begin match ( 
                      ListExtras.scan_fold (fun ret_term lhs rhs_hd rhs_tl ->
                         if ret_term <> None then ret_term else
                         let local_scope = 
                            ListAsSet.union scope
                               (snd (C.schema_of_expr (CalcRing.mk_prod lhs)))
                         in
                         let rhs = rhs_hd::rhs_tl in
                         if C.commutes ~scope:local_scope 
                               (CalcRing.mk_prod rhs) curr_term
                         then Some(lhs@[curr_term]@rhs)
                         else None
                      ) None curr_ret
                   ) with
                      |  Some(s) -> s
                      |  None    -> curr_ret @ [curr_term]
                   end
                | _ -> curr_ret @ [curr_term]
             end
          ) [] (domains @ deltas @ others))
      )
      (fun _ x -> CalcRing.mk_neg x)
      (fun _ x ->
            begin match x with
               | AggSum(gb_vars, subexp) ->
                  (* Advance lifts can turn output variables into input variables:
                     e.g., for R(A) * AggSum([A], S(A) * (B ^= A) * {B > 2}) *)
                  let new_gb_vars = 
                     ListAsSet.inter gb_vars (snd (C.schema_of_expr subexp))
                  in
                     C.mk_aggsum new_gb_vars subexp
               | _ -> CalcRing.mk_val x
            end)
      (fun _ _ -> true)
      expr 
   in
      Debug.print "LOG-ADVANCE-LIFTS" (fun () -> 
         "Advance Lifts (END): "^
         (CalculusPrinter.string_of_expr rewritten_expr)
      ); rewritten_expr
;;


(**
  [eliminate_duplicates expr]

    Simplification rules for terms with binary multiplicities (return 0 or 1).

    Recur through the expression tree and remove all expression that 
    have binary multiplicities when identical expressions are already defined. 
*)
let eliminate_duplicates (big_expr:C.expr_t) =
   Debug.print "LOG-ELIMINATE-DUPLICATES" (fun () ->
      "Eliminate duplicates (START): "^
      (CalculusPrinter.string_of_expr big_expr) 
   );
   let rec rcr scope context expr = begin match expr with
      | CalcRing.Sum(sl) -> 
         let (new_contexts, new_sl) = 
            List.split (List.map (rcr scope context) sl)
         in
            (ListAsSet.multiinter ~eq:C.exprs_are_identical new_contexts,
             CalcRing.mk_sum new_sl)

      | CalcRing.Prod(pl) -> snd (
         List.fold_left (fun (curr_scope, (curr_context, curr_prod)) term ->
            let (new_ctx, new_term) = rcr curr_scope curr_context term in
            (
               ListAsSet.union curr_scope (snd (C.schema_of_expr new_term)),
               (  ListAsSet.union ~eq:C.exprs_are_identical 
                     curr_context new_ctx, 
                  CalcRing.mk_prod [ curr_prod ; new_term]  )
            )
         ) (scope, (context, CalcRing.one)) pl)
         
      | CalcRing.Neg(subexp) -> 
           let (new_context, new_subexp) = rcr scope context subexp in
           (new_context, CalcRing.mk_neg new_subexp)
      
      | CalcRing.Val(lf) -> 
           (* Expressions returning 0 or 1 are candidates for elimination *) 
           let expr_is_eligible = 
              C.expr_has_binary_multiplicity ~scope:scope expr 
           in

           if expr_is_eligible && 
                 List.exists (C.exprs_are_identical expr) context 
           then ([], CalcRing.one) 
           else

           let new_context = if expr_is_eligible then [expr] else [] in

           let (expr_ivars, expr_ovars) = C.schema_of_expr expr in
           let expr_vars = ListAsSet.union expr_ivars expr_ovars in
           let filt_scope = ListAsSet.inter scope expr_vars in
           let filt_context = List.filter (fun ctx_term ->
              let (cterm_ivars, cterm_ovars) = C.schema_of_expr ctx_term in
              let cterm_vars = ListAsSet.union cterm_ivars cterm_ovars in
              ListAsSet.subset cterm_vars expr_vars
           ) context in 
              begin match lf with
                | AggSum(gb_vars,raw_subexp) -> 
                    let (_, subexp) = rcr filt_scope filt_context raw_subexp in
                    (new_context, C.mk_aggsum gb_vars subexp)
                | Lift(v,raw_subexp) -> 
                    let (_, subexp) = rcr filt_scope filt_context raw_subexp in
                    (new_context, C.mk_lift v subexp)
                | Exists(raw_subexp) -> 
                    let (_, subexp) = rcr filt_scope filt_context raw_subexp in
                    (new_context, C.mk_exists subexp)
                | External(en, ei, eo, et, Some(raw_subexp)) ->
                    let (_, subexp) = rcr ei filt_context raw_subexp in
                    (new_context, C.mk_external en ei eo et (Some(subexp)))
                | _ -> (new_context, expr)
              end            
      end
   in
      let scope = fst (C.schema_of_expr big_expr) in
      let rewritten_expr = snd (rcr scope [] big_expr) in
      Debug.print "LOG-ELIMINATE-DUPLICATES" (fun () ->
         "Eliminate duplicates (END): "^
         (CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr


type domain_cmp_t =
   | NotComparable
   | Identical
   | LessRestrictive    (* binds fewer variables *)
   | MoreRestrictive    (* binds more variables *)
   

let cmp_domain_exprs expr1 expr2 = 
   if C.exprs_are_identical expr1 expr2 then Identical else

   (* Check if term_list1 is a substring of term_list2 *)
   let rec term_list_subset term_list1 term_list2 =    
      match (term_list1, term_list2) with
         | ([], _) -> Some([])
         | (_, []) -> None
         | (hd1::tl1, hd2::tl2) ->
            begin match C.cmp_exprs hd1 hd2 with
               | Some(hd_mapping) -> 
                  begin match term_list_subset tl1 tl2 with
                     | Some(tl_mapping) -> 
                        ListAsFunction.merge hd_mapping tl_mapping
                     | None -> None
                  end
               | None -> term_list_subset term_list1 tl2
            end
   in
   let extract_list expr = match expr with
      | CalcRing.Val(AggSum(_, subexp)) -> CalcRing.prod_list subexp
      | _ -> CalcRing.prod_list expr
   in   
   let (ivars1,ovars1) = C.schema_of_expr expr1 in   
   let (ivars2,ovars2) = C.schema_of_expr expr2 in

   if ListAsSet.subset ovars1 ovars2 then 
      (* Check if expr1 is less restrictive than expr2 *)
      match term_list_subset (extract_list expr1) (extract_list expr2) with
         | Some(mapping) -> 
            let vars1 = ListAsSet.union ivars1 ovars1 in
            let relevant_mapping = 
               List.filter (fun (a,b) -> List.mem a vars1) mapping
            in
               if ListAsFunction.is_identity relevant_mapping 
               then LessRestrictive
               else NotComparable
         | None -> NotComparable

   else if ListAsSet.subset ovars2 ovars1 then 
      (* Check if expr1 is more restrictive than expr2 *)
      match term_list_subset (extract_list expr2) (extract_list expr1) with
         | Some(mapping) -> 
            let vars2 = ListAsSet.union ivars2 ovars2 in
            let relevant_mapping = 
               List.filter (fun (a,b) -> List.mem a vars2) mapping
            in
               if ListAsFunction.is_identity relevant_mapping 
               then MoreRestrictive
               else NotComparable
         | None -> NotComparable
   
   else NotComparable

(** For a given domain/exists subexpression eliminate duplicate terms 
    (relations, delta relations, and externals) while considering only 
    relevant variables appearing in the schema. For example, given a schema 
    [A], expression R(A,B) * R(A,C) * C is transformed into R(A,B) * B. *)
let unique_domains expr = 
   Debug.print "LOG-UNIQUE-DOMAINS" (fun () ->
      "Unique domains (START): "^ (CalculusPrinter.string_of_expr expr) 
   );
   let eligible_term = function
      | CalcRing.Val(Rel _) 
      | CalcRing.Val(DeltaRel _)
      | CalcRing.Val(External _) -> true
      | _ -> false
   in
   let merge_mappings mapping1 mapping2 = 
      match (ListAsFunction.merge mapping1 mapping2) with
         | Some(mapping) -> mapping
         | None -> failwith "Incompatible mappings"
   in
   let rec map_relations schema term_list = match term_list with 
      | [] -> []
      | [x] -> [x]
      | head::tail -> 
         let mapped_tail = map_relations schema tail in

         if not (eligible_term head) then head :: mapped_tail else

         let mapping = 
            List.fold_left (fun curr_mapping tail_term ->
               if not (eligible_term tail_term) then curr_mapping else

                match C.cmp_exprs head tail_term with 
                  | Some(mapping) ->
                     (* Map head onto tail_term *)
                     let (schema_mapping, rest_mapping) = 
                        List.partition (fun (a,b) -> List.mem a schema) 
                                       mapping
                     in
                     if ListAsFunction.is_identity schema_mapping then
                        merge_mappings curr_mapping rest_mapping
                     else

                     (* Map tail_term onto head *)
                     let (schema_mapping2, rest_mapping2) = 
                        List.partition (fun (a,b) -> List.mem a schema)
                           (List.map (fun (a,b) -> (b,a)) mapping)
                     in
                     if ListAsFunction.is_identity schema_mapping2 then
                        merge_mappings curr_mapping rest_mapping2
                     else curr_mapping

                  | None -> curr_mapping
            ) [] mapped_tail
         in
            CalcRing.prod_list (rename_vars mapping 
               (CalcRing.mk_prod (head::mapped_tail)))
   in   
   (* Remove redundant terms, for instance R(A,B) * R(A,B) -> R(A,B).
      This is safe to do only when the domain expression is a monomial
      since we don't care about the multiplicity. *)
  let remove_duplicates ivars ovars term_list =   
    ListAsSet.uniq ~eq:(fun e1 e2 -> 
      begin match C.cmp_exprs e1 e2 with
         | Some(mappings) -> 
            let vars = ListAsSet.union ivars ovars in
            let relevant_mappings = 
               List.filter (fun (a, b) -> List.mem a vars) mappings
            in
               ListAsFunction.is_identity relevant_mappings
         | None -> false
      end
    ) term_list  
   in 

   let rewritten_expr = 
      let sum_terms = CalcRing.sum_list expr in
      if (List.length sum_terms > 1) then expr else

      (* Removing AggSums can introduce new fresh variables,
         which might cause infinite optimization loops. E.g.:
           AggSum([A],(A^=5)*R(A,B)) * AggSum([A],(A^=5)*R(A,B))
           => AggSum([A],(A^=5)*R(A,B_XXX)), where XXX increases *)
      let unique_expr = CalcRing.mk_prod (
         ListAsSet.uniq ~eq:C.exprs_are_identical 
            (CalcRing.prod_list expr))
      in
      if (List.length (CalcRing.prod_list unique_expr) = 1) then unique_expr
      else

      let expr_noaggs = C.erase_aggsums unique_expr in 
      let sum_terms_noaggs = CalcRing.sum_list expr_noaggs in
      if (List.length sum_terms_noaggs > 1) then unique_expr else

      let (ivars, ovars) = C.schema_of_expr unique_expr in
      C.mk_aggsum ovars (
         let mapped_terms = 
            map_relations ovars (CalcRing.prod_list expr_noaggs)
         in
            (* Remove duplicates *)
            CalcRing.mk_prod (remove_duplicates ivars ovars mapped_terms)
      )
   in
      Debug.print "LOG-UNIQUE-DOMAINS" (fun () ->
         "Unique domains (END): "^
         (CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr

(**
  [extract_domains expr]

  Create DomainDelta terms and pull them out of AggSums. 
  
  DomainDelta terms serve only to restrict the domain of computation, one can 
  ignore them but at the price of iterating more. Domain terms are initially
  created out of Delta terms and lifts whose subexpressions are values 
  containing only input (trigger) variables. Later on, these domains are 
  expanded to include more restrictions and pushed outside AggSums.
*)
let extract_domains (big_scope:var_t list) (big_expr:C.expr_t) = 
   Debug.print "LOG-EXTRACT-DOMAINS" (fun () ->
      "Extract domains (START): "^(CalculusPrinter.string_of_expr big_expr) ^
      "\nEvent scope: "^(ListExtras.ocaml_of_list string_of_var big_scope)
   );
   let rec rcr expr = 
      let (scope, schema) = C.schema_of_expr expr in
      C.fold ~scope:scope ~schema:schema
         (fun _ sl ->  
            (* CalcRing.mk_sum sl *)

            (* Extract domain subexpressions *)
            let partition_domain term_list = 
               List.fold_left (fun (lhs,rhs) term -> match term with 
                  | CalcRing.Val(DomainDelta(subexp)) -> (lhs@[subexp], rhs)
                  | _ -> (lhs, rhs@[term])
               ) ([], []) term_list
            in           

            let rec common_domains dom_list1 dom_list2 = 
               if dom_list1 = [] || dom_list2 = [] then [] else

               let dom_term1 = List.hd dom_list1 in
               let domains_hd1 = 
                  List.fold_left (fun domains dom_term2 ->
                     match cmp_domain_exprs dom_term1 dom_term2 with
                        | Identical | LessRestrictive -> domains @ [dom_term1]
                        | MoreRestrictive -> domains @ [dom_term2]
                        | NotComparable -> domains
                  ) [] dom_list2
               in
               if domains_hd1 = [] then
                  common_domains (List.tl dom_list1) dom_list2
               else
                  let new_dom_list1 = 
                     ListAsSet.diff ~eq:C.exprs_are_identical 
                                    dom_list1 domains_hd1
                  in
                  let new_dom_list2 =
                     ListAsSet.diff ~eq:C.exprs_are_identical 
                                    dom_list2 domains_hd1 
                  in
                    domains_hd1 @ 
                    common_domains new_dom_list1 new_dom_list2
            in
            let rec extract_common_domain sum_terms = match sum_terms with 
               | [] -> ([], CalcRing.zero)
               | [term] -> 
                  let (domains, rest) = 
                     partition_domain (CalcRing.prod_list term) 
                  in
                     (domains, CalcRing.mk_prod rest)
               | hd::tl -> 
                  let (hd_domains, hd_rest) = 
                     partition_domain (CalcRing.prod_list hd) in
                  let hd_rhs = CalcRing.mk_prod hd_rest in
                  let (tl_domains, tl_rhs) = extract_common_domain tl in

                  (* Find common domains between hd_domains and tl_domains *)
                  let common_doms = common_domains hd_domains tl_domains in
                  let new_hd_lhs = ListAsSet.diff ~eq:C.exprs_are_identical 
                                                  hd_domains common_doms 
                  in
                  let new_tl_lhs = ListAsSet.diff ~eq:C.exprs_are_identical 
                                                  tl_domains common_doms 
                  in
                     (
                        common_doms,
                        CalcRing.mk_sum [
                           CalcRing.mk_prod (
                              List.map C.mk_domain new_hd_lhs @ [hd_rhs]);
                           CalcRing.mk_prod (
                              List.map C.mk_domain new_tl_lhs @ [tl_rhs])
                        ]
                     )
            in 
            let (lhs, rhs) = extract_common_domain sl in 
               Debug.print "LOG-EXTRACT-DOMAINS-DETAIL" (fun () ->
                  "Candidates: "^
                  (ListExtras.ocaml_of_list CalculusPrinter.string_of_expr sl)^
                  "\nExtracted common domain: "^
                  (C.string_of_expr (
                     CalcRing.mk_prod (List.map C.mk_domain lhs)))
               );
               CalcRing.mk_prod (List.map C.mk_domain lhs @ [rhs])
         )
         (fun (local_scope, schema) pl -> 
            (* Create DomainDelta terms that can be pulled out of AggSums.*)
            (* 1. Collect domain subexpressions. *)
            let (domain_terms, rest_terms) =
               List.fold_left (fun (dl, rl) term -> match term with
                  | CalcRing.Val(DomainDelta(subexp)) -> 
                     (dl @ CalcRing.prod_list subexp, rl)
                  | _ -> (dl, rl @ [term])
               ) ([],[]) (CalcRing.prod_list (CalcRing.mk_prod pl))
            in

            let (dom_ivars, dom_ovars) = 
               C.schema_of_expr (CalcRing.mk_prod domain_terms) in
            let (rest_ivars, rest_ovars) = 
               C.schema_of_expr (CalcRing.mk_prod rest_terms) in
            let rest_vars = ListAsSet.union rest_ivars rest_ovars in

            (* Sanity check - DeltaDomain terms might have input variables 
               that are bound only from the outside, not by any term in pl *)
            if not (ListAsSet.subset dom_ivars big_scope) && 
               not (ListAsSet.subset dom_ivars local_scope) then (
               print_endline ("Event big scope: " ^ 
                  ListExtras.ocaml_of_list string_of_var big_scope);
               print_endline ("Event local scope: " ^ 
                  ListExtras.ocaml_of_list string_of_var local_scope);
               bail_out (CalcRing.mk_prod pl) "Domain with unexpected inputvars"
            )
            else

            (* 2. Extend dom_terms with: 1) values covered by dom_vars and
                  2) (A ^= B) where B is value covered by dom_vars; *)
            let extended_domain_terms = snd (
               List.fold_left (fun (scope, dl) term -> 
                  match term with
                    | CalcRing.Val(Cmp _)
                      when ListAsSet.subset (fst (C.schema_of_expr term)) 
                                            scope -> 
                        (scope, dl @ [term])     
                    | CalcRing.Val(Lift(v1,CalcRing.Val(Value(v2)))) 
                      when ListAsSet.subset (Arithmetic.vars_of_value v2) scope ->
                         (ListAsSet.union scope [v1], dl @ [term]) 
                    | _ -> (scope, dl)
               ) (ListAsSet.union big_scope dom_ovars, domain_terms) 
                 rest_terms 
            ) in

            (* 3. Remove duplicate terms *)
            let unique_domain_expr = 
               unique_domains (CalcRing.mk_prod extended_domain_terms)
            in

            (* 4. Graph decomposition of unique_domain_terms *)
            let domain_components = snd (
               CalculusDecomposition.decompose_graph 
                  dom_ivars (schema, unique_domain_expr))
            in

            (* 5. Eliminate domains that do not intersect with schema 
                  (as they cannot be pulled out of AggSums) and also are 
                  irrelevant for the rest of the expression *)
            let filtered_domain_components = 
               List.filter (fun (local_schema, _) -> 
                  ListAsSet.inter local_schema 
                                  (ListAsSet.union schema big_scope) <> [] ||
                  ListAsSet.inter local_schema rest_vars <> []
               ) domain_components
            in            
               Debug.print "LOG-EXTRACT-DOMAINS-DETAIL" (fun () ->
                  "Unique extended domains: "^
                  (C.string_of_expr unique_domain_expr)^
                  "\nPartitioned domains: "^
                  (ListExtras.ocaml_of_list (fun (s, t) ->
                     "(" ^ ListExtras.ocaml_of_list string_of_var s ^
                     "," ^ C.string_of_expr t ^ ")") domain_components)^ 
                  "\nFiltered domains: "^
                  (ListExtras.ocaml_of_list (fun (s, t) ->
                     "(" ^ ListExtras.ocaml_of_list string_of_var s ^
                     "," ^ C.string_of_expr t ^ ")") filtered_domain_components) 
               );
               CalcRing.mk_prod (
                  List.map (fun (local_schema, local_term) -> 
                     C.mk_domain (C.mk_aggsum local_schema local_term)
                  ) filtered_domain_components @ rest_terms)
         )
         (fun _ e -> let rec unnest term_list = 
                        match term_list with
                           | CalcRing.Val(DomainDelta(subexp))::tail -> 
                              C.mk_domain subexp :: unnest tail
                           | rest -> [CalcRing.mk_neg (CalcRing.mk_prod rest)]
                     in
                        CalcRing.mk_prod (unnest (CalcRing.prod_list e))) 
         (fun _ lf -> begin match lf with
            | AggSum(gb_vars, raw_subexp) -> 
               let (raw_ivars, _) = C.schema_of_expr raw_subexp in
               let subexp = rcr raw_subexp in               
               (* Unnest DomainDelta terms *)
               let (unnested, nested) = 
                  List.fold_left (fun (lhs, rhs) term -> match term with
                     | CalcRing.Val(DomainDelta(subterm)) ->
                        (* Compute domain gb_vars *)
                        let dom_gb_vars = 
                           ListAsSet.inter (ListAsSet.union raw_ivars gb_vars)
                                           (snd (schema_of_expr subterm)) 
                        in                        
                        if (dom_gb_vars <> [] && 
                            ( ListAsSet.seteq dom_gb_vars gb_vars || 
                              C.expr_has_binary_multiplicity subterm )) then
                            (* Aggregating further over domain subterm is
                               allowed only for singletons *)
                          let extracted_domain = 
                             C.mk_domain (C.mk_aggsum dom_gb_vars subterm)
                          in
                             (CalcRing.mk_prod [ lhs; extracted_domain], rhs)  
                        else (lhs, CalcRing.mk_prod [rhs; term])
                     | _ -> (lhs, CalcRing.mk_prod [rhs; term])
                  ) (CalcRing.one, CalcRing.one) (CalcRing.prod_list subexp) 
               in
                  CalcRing.mk_prod [ unnested; C.mk_aggsum gb_vars nested ]

            | DeltaRel _ -> 
               let delta_term = CalcRing.mk_val lf in
               CalcRing.mk_prod [ C.mk_domain delta_term; delta_term ]
            | Lift(v1,CalcRing.Val(Value(v2)))
              when ListAsSet.subset (Arithmetic.vars_of_value v2) 
                                    big_scope -> 
                 let delta_term = CalcRing.mk_val lf in
                 CalcRing.mk_prod [ C.mk_domain delta_term; delta_term ]
            | Lift(v,raw_subexp) -> C.mk_lift v (rcr raw_subexp)
   (***** BEGIN EXISTS HACK *****)         
            | Exists(raw_subexp) -> C.mk_exists (rcr raw_subexp)
   (***** END EXISTS HACK *****)
            | _ -> CalcRing.mk_val lf
         end) 
         expr
   in
   let rewritten_expr = (* eliminate_duplicates *) (rcr big_expr) in
   Debug.print "LOG-EXTRACT-DOMAINS" (fun () ->
      "Extract domains (END): "^
      (CalculusPrinter.string_of_expr rewritten_expr) 
   ); rewritten_expr
;;


(**
  [simplify_domains expr]

    Simple rewrite rules focused on DomainDelta and Exists terms.
  
    [DomainDelta(E1) * R * E2 => 
      E2 * R  if E1 is less restrictive than E2 and R commutes with E2 or
      R * E2  if E1 is less restrictive than E2 and R commutes with E1 ]
    
    [Exists(E) * R * E => E * R ]

    [DomainDelta(A) => A
        IF A produces only tuples with multiplicity zero or one ]
  
    [Exists(A) => A
        IF A produces only tuples with multiplicity zero or one ]

    DISABLED!!!
    [DomainDelta(E1 * E2) -> DomainDelta(E1) * DomainDelta(E2)  ]

    DISABLED!!!
    [Exists(A * B) => Exists(A) * Exists(B) ]

*)
let simplify_domains (big_expr:C.expr_t) = 
   Debug.print "LOG-SIMPLIFY-DOMAINS" (fun () ->
      "Simplify domains (START): "^(CalculusPrinter.string_of_expr big_expr) 
   );
   (* Try to eliminate redundant domains, for example:
      DomainDelta(E1 * E2) * R1 * E4 * R2 * E3 -> E3 * E4 * R1 * R2 when 
      E1 is less restrictive than E3 and E2 is less restrictive than E4 *)
   let rec eliminate_domains term_list =

      let rec split_at_pivot cmp_fn term term_list = match term_list with
         | [] -> raise Not_found
         | head::tail ->
            if cmp_fn term head then ([], head, tail) else
            let (tail_lhs, pivot, tail_rhs) = 
               split_at_pivot cmp_fn term tail
            in
               (head::tail_lhs, pivot, tail_rhs)
      in
      (* Return new target list or throw Not_found *)
      let rec unify_domain dom_term_list target_list = 

         let less_restrictive_fn = (fun expr1 expr2 -> 
            match cmp_domain_exprs expr1 expr2 with
               | Identical | LessRestrictive -> true | _ -> false) 
         in
         let rec unify dom_term target_list =
            let (lhs, pivot, rhs) = 
               split_at_pivot less_restrictive_fn dom_term target_list 
            in
            let lhs_expr = CalcRing.mk_prod lhs 
            in
               if C.commutes lhs_expr pivot then (pivot::lhs) @ rhs
               else if C.commutes dom_term lhs_expr then lhs @ (pivot::rhs)
               else raise Not_found
         in
            match dom_term_list with 
               | [] -> target_list
               | head::tail -> unify head (unify_domain tail target_list)
      in
         match term_list with
            | [] -> []
            | CalcRing.Val(DomainDelta(subexp))::tail -> 
               begin 
                  try
                     eliminate_domains (
                        unify_domain (CalcRing.prod_list subexp) tail
                     )     
                  with Not_found -> 
                     C.mk_domain subexp :: (eliminate_domains tail)
               end
            | CalcRing.Val(Exists(subexp))::tail ->
               begin
                  try
                     eliminate_domains (
                        unify_domain (CalcRing.prod_list subexp) tail
                     ) 
                  with Not_found -> 
                     C.mk_exists subexp :: (eliminate_domains tail)
               end
            | head::tail -> head::(eliminate_domains tail)
   in
   let rec rcr expr = begin match expr with
      | CalcRing.Sum(sl) -> 
         CalcRing.mk_sum (List.map rcr sl)
      | CalcRing.Prod(pl) -> 
         CalcRing.mk_prod (List.map rcr (eliminate_domains pl))
      | CalcRing.Neg(e) -> 
         CalcRing.mk_neg (rcr e)
      | CalcRing.Val(lf) -> 
         begin match lf with
            | AggSum(gb_vars,raw_subexp) -> 
               C.mk_aggsum gb_vars (rcr raw_subexp)
            | Lift(v,raw_subexp) -> 
               C.mk_lift v (rcr raw_subexp)
(***** BEGIN EXISTS HACK *****)         
            | Exists(raw_subexp) ->
               let subexp = unique_domains (rcr raw_subexp) in
                  if C.expr_has_binary_multiplicity raw_subexp then subexp
                  else C.mk_exists subexp
(***** END EXISTS HACK *****)
            | DomainDelta(raw_subexp) ->
               let subexp = unique_domains (rcr raw_subexp) in
                  if C.expr_has_binary_multiplicity raw_subexp then subexp
                  else C.mk_domain subexp
            | _ -> CalcRing.mk_val lf
         end
      end
   in
      let rewritten_expr = eliminate_duplicates (rcr big_expr) in
      Debug.print "LOG-SIMPLIFY-DOMAINS" (fun () ->
         "Simplify domains (END): "^
         (CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr


(**
  [nesting_rewrites expr]

    A hodgepodge of simple rewrite rules for nested expressions (i.e., 
    expressions nested within a Lift or AggSum.  Many of these have to do with
    lifting expressions out of the nesting.
    
    [AggSum([...], 0) = 0]
    
    [AggSum([...], A) = A]
        IF all of the output variables of A are in the group-by variables of
        the AggSum
    
    [AggSum([...], A + B + ...) = 
        AggSum([...], A) + AggSum([...], B) + AggSum([...], ...)]
    
    [AggSum([...], A * B) = A * AggSum([...], B)]
        IF all of the output variables of A are in the group-by variables of
        the AggSum
    
    [AggSum([...], A * B) = B * AggSum([...], A)]
        IF all of the output variables of B are in the group-by variables of
        the AggSum AND B commutes with A.
  
    [AggSum(GB1, AggSum(GB2, A)) = AggSum(GB1, A)]
        IF all of the output variables of A are in the group-by variables of
        the AggSum.
    
    [AggSum([...], A) = A]
        IF A is a constant term (i.e., has no output variables)
    
    [Lift(A, A) = 1]
    
    [Lift(A, f(A)) => [A = f(A)]] (or equivalent)

   @param expr    The calculus expression being processed
*)
let rec nesting_rewrites (big_expr:C.expr_t) = 
   Debug.print "LOG-NESTING-REWRITES" (fun () ->
      "Nesting Rewrites (START): "^(CalculusPrinter.string_of_expr big_expr) 
   );
   let rec rcr expr = CalcRing.fold 
      (CalcRing.mk_sum) 
      (CalcRing.mk_prod) 
      (CalcRing.mk_neg)
      (fun e -> begin match e with
         | AggSum(gb_vars, x) when x = CalcRing.zero -> CalcRing.zero
         | AggSum(gb_vars, unprocessed_subterm) ->
            let subterm = rcr unprocessed_subterm in
            if (snd (C.schema_of_expr subterm)) = [] then subterm
            else
               begin match subterm with
                  | CalcRing.Sum(sl) ->
                     (* Input variables might be bound some but not all of the 
                        sum terms.  We need to update the aggsum accordingly *)
                     let (sum_ivars, _) = C.schema_of_expr subterm in
                     let rewritten = 
                        CalcRing.mk_sum 
                           (List.map (fun term -> 
                              let (_,term_ovars) = C.schema_of_expr term in
                              Debug.print "LOG-NESTINGREWRITES-DETAIL" (fun ()->
                                 "Rewriting aggsum term with ovars:"^
                                 (ListExtras.ocaml_of_list string_of_var 
                                                            term_ovars)^":\n"^
                                 (CalculusPrinter.string_of_expr term)
                              );
                              let term_gb_vars = 
                                 ListAsSet.union gb_vars
                                    (ListAsSet.inter sum_ivars term_ovars)
                              in
                                 C.mk_aggsum term_gb_vars term
                           ) sl)
                     in
                     Debug.print "LOG-NESTINGREWRITES-DETAIL" (fun () ->
                        "Rewrote : AggSum("^
                        (ListExtras.ocaml_of_list string_of_var gb_vars)^", \n"^
                        (CalculusPrinter.string_of_expr subterm)^
                        "\n) with ivars: "^
                        (ListExtras.ocaml_of_list string_of_var sum_ivars)^
                        "\nto: "^
                        (CalculusPrinter.string_of_expr rewritten)
                     );
                     rewritten
                  | CalcRing.Prod(pl) ->
                     let (unnested, nested) = 
                       List.fold_left (fun (unnested, nested) term->
                          if (C.commutes nested term) && 
                             (ListAsSet.subset (snd (C.schema_of_expr term))
                                                gb_vars)
                          then (CalcRing.mk_prod [unnested; term], nested)
                          else (unnested, CalcRing.mk_prod [nested; term])
                       ) (CalcRing.one, CalcRing.one) pl
                     in

                     (* Update group-by variables. Note that the schema can 
                        expand. For instance:
                          (A ^= 0) * AggSum([B], (B ^= {A + 5}) * R(A,B,C))
                        is transformed into:
                          (A ^= 0) * (B ^= {A + 5}) * AggSum([A,B], R(A,B,C)) *)                      
                     let unnested_ivars = fst(C.schema_of_expr unnested) in
                     let new_gb_vars = ListAsSet.inter 
                          (snd (C.schema_of_expr nested))
                          (ListAsSet.union gb_vars unnested_ivars)
                     in
                       Debug.print "LOG-NESTINGREWRITES-DETAIL" (fun () ->
                           "Nesting rewrites lifting out:\n"^
                           (string_of_expr unnested)^
                           "\n\tand keeping in:\n"^
                           (string_of_expr nested)^
                           "\n\twith output variables : "^
                           (ListExtras.ocaml_of_list fst 
                                 (snd (C.schema_of_expr nested)))^
                           "\n\tMaking the new group-by variables:\n"^
                           (ListExtras.ocaml_of_list fst new_gb_vars)
                       );
                       CalcRing.mk_prod [  unnested; 
                                           C.mk_aggsum new_gb_vars nested ]

                  | CalcRing.Val(AggSum(_, term)) ->
                     C.mk_aggsum gb_vars term
                  | _ -> 
                     if ListAsSet.subset (snd (C.schema_of_expr subterm))
                                         gb_vars 
                     then subterm
                     else C.mk_aggsum gb_vars subterm
               end
         
(***** BEGIN EXISTS HACK *****)
         | Exists(unprocessed_subexp) -> 
            begin match (rcr unprocessed_subexp) with 
               | CalcRing.Val(Value(ValueRing.Val(AConst(CInt(0))))) ->
                  CalcRing.zero
               | CalcRing.Val(Value(ValueRing.Val(AConst(CInt(_))))) ->
                  CalcRing.one
               | CalcRing.Val(Value(ValueRing.Val(AConst(_)))) ->
                  C.bail_out (CalcRing.mk_val e) 
                     "Exists with a non-integer value"
               | subexp -> C.mk_exists subexp
            end               

         | DomainDelta(unprocessed_subexp) -> 
            begin match (rcr unprocessed_subexp) with 
               | CalcRing.Val(Value(ValueRing.Val(AConst(CInt(0))))) ->
                  CalcRing.zero
               | CalcRing.Val(Value(ValueRing.Val(AConst(CInt(_))))) ->
                  CalcRing.one
               | CalcRing.Val(Value(ValueRing.Val(AConst(_)))) ->
                  C.bail_out (CalcRing.mk_val e) 
                     "Domain with a non-integer value"
               | subexp -> C.mk_domain subexp
            end             
            
(***** END EXISTS HACK *****)
         
         | Lift(v, unprocessed_subterm) ->
            let nested = rcr unprocessed_subterm in
            let (nested_ivars,nested_ovars) = C.schema_of_expr nested in
            (* Perform a few quick sanity checks.
               The variable being lifted should not occur in the nested 
               expression, especially not as an output variable (in which case, 
               the lift operation would overwrite that part of the schema).
               
               If the expression being lifted has the lifted variable as an 
               input, then what it's trying to express is an equality.  
               Technically this is an error, but there are certain cases 
               (unification in particular) where it can occur, so we simply 
               convert it into an equality.
            *)
            let lift_expr = 
               if List.mem v nested_ovars
               then failwith "Error: Overwriting schema of lifted expression"
               else if List.mem v nested_ivars
               then begin match nested with
                  | CalcRing.Val (Value(cmp_val)) ->
                     C.mk_cmp Eq (Arithmetic.mk_var v) cmp_val
                  | _ ->
                     let temp_var = mk_temp_var nested in
                        CalcRing.mk_prod [
                           C.mk_lift temp_var nested;
                           C.mk_cmp Eq (Arithmetic.mk_var v)
                                              (Arithmetic.mk_var temp_var)
                        ]
                  end
               else C.mk_lift v nested
            in lift_expr

         | _ -> CalcRing.mk_val e
      end)
      expr
    in
    let rewritten_expr = rcr big_expr in
       Debug.print "LOG-NESTING-REWRITES" (fun () ->
          "Nesting Rewrites (END): "^
          (CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr
   
;;

type selected_candidate_t = 
  | FactorizeLHS of C.expr_t
  | FactorizeRHS of C.expr_t
  | DoNotFactorize

(**
   [default_factorize_heuristic candidates scope term_list]
   
   where candidates is a 2-tuple of candidates that can be commuted to the lhs 
   of the terms they are extracted from, and the candidates that can be 
   commuted to the rhs.
   
   The default heuristic for factorization.  
      - Prefer non-value candidates
      - Prefer candidates that occur in the maximal number of terms
      - Prefer lhs candidates
      
*)
let default_factorize_heuristic (lhs_candidates,rhs_candidates)
                                scope term_list: (selected_candidate_t) =
   Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
      "Candidates for factorization:"^
      "\nLHS candidates: "^
      ListExtras.string_of_list ~sep:"\n" string_of_expr lhs_candidates^ 
      "\nRHS candidates: "^
      ListExtras.string_of_list ~sep:"\n" string_of_expr rhs_candidates
   );
   let increment_count term list = 
      let (pos_term, neg_term) = (term, neg term) in
      List.map (fun (cterm, count) -> 
        (cterm, if C.exprs_are_identical pos_term cterm ||
                   C.exprs_are_identical neg_term cterm  
                then count + 1 else count)
      ) list
   in
   let (lhs_counts,rhs_counts) =
      List.fold_left (fun old_counts product_term -> 
         List.fold_left (fun (lhs_counts,rhs_counts) term ->
               (  increment_count term lhs_counts, 
                  increment_count term rhs_counts)
         ) old_counts (CalcRing.prod_list product_term)
      ) (   List.map (fun x->(x,0)) lhs_candidates, 
            List.map (fun x->(x,0)) rhs_candidates
         ) term_list
   in
   let lhs_sorted = List.sort (fun x y -> compare (snd y) (snd x)) lhs_counts in
   let rhs_sorted = List.sort (fun x y -> compare (snd y) (snd x)) rhs_counts in

   Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
      "LHS sorted: \n"^
      ListExtras.string_of_list ~sep:"\n" (fun (expr, count) -> 
        string_of_expr expr^" ("^string_of_int count^")") lhs_sorted^
      "\nRHS sorted: \n"^
      ListExtras.string_of_list ~sep:"\n" (fun (expr, count) -> 
        string_of_expr expr^" ("^string_of_int count^")") lhs_sorted
   );   
   if ((List.length lhs_sorted < 1) || (snd (List.hd lhs_sorted) = 1)) &&
      ((List.length rhs_sorted < 1) || (snd (List.hd rhs_sorted) = 1))
   then
      (* If both candidate lists are empty, or all non-empty lists have each 
         candidate appearing exactly in one term, then we can't factorize 
         anything.  We're done here. *)
      DoNotFactorize
   else
      (* We should never get a candidate that doesn't appear at least once.  
         This is most definitely a bug if it happens. *)
   if ((List.length lhs_sorted >= 1) && (snd (List.hd lhs_sorted) < 1)) ||
      ((List.length rhs_sorted >= 1) && (snd (List.hd rhs_sorted) < 1))
   then failwith "BUG: Factorize got a vanishing candidate"
   else
      if (snd (List.hd rhs_sorted)) > (snd (List.hd lhs_sorted)) then
         FactorizeRHS(fst (List.hd rhs_sorted))
      else 
         FactorizeLHS(fst (List.hd lhs_sorted))

                                 

(**
  [factorize_one_polynomial scope term_list]
  
    Given an expression (A * B) + (A * C), factor out the A to get A * (B + C)
    Note that this works bi-directionally, we can factor terms off the front, 
    or off the back.  All that matters is whether we can commute the term 
    to wherever it needs to get factored.
  
    Most of the work happens in here in factorize_one_polynomial, which takes a 
    list of monomials representing a list of terms and identifies a Calculus 
    expression that is equivalent to their sum but which has terms factorized
    out as possible.
  
    This process happens in several stages. 
    - First, we identify a set of candidate subterms that may be factorized out 
      of the right or left hand side of each term.
    - For each candidate we count how many terms it can be factorized out of.
    - We pick the candidate that can be factorized out of the most terms and
      delete the term from those terms.
    - We recur twice, once on the set of terms that were factorized, and once
      on the set of terms that weren't.

   @param heuristic (optional) An optional factorization heuristic function
   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param term_list A list of terms to be treated as if they were part of a Sum
*)
let rec factorize_one_polynomial ?(heuristic = default_factorize_heuristic)
                                 (scope:var_t list) (term_list:C.expr_t list) =
   Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
      "Factorizing Expression: ("^
      (ListExtras.string_of_list ~sep:" + " 
          CalculusPrinter.string_of_expr term_list)^
      ")["^(ListExtras.string_of_list fst scope)^"]"
   );
   if List.length term_list < 2 then CalcRing.mk_sum term_list
   else

   let (one, minus_one) = (CalcRing.one, neg CalcRing.one) in
   let (lhs_candidates,rhs_candidates) = 
      List.fold_left (fun (lhs_candidates, rhs_candidates) term -> 
         let (new_lhs, new_rhs) = List.split (ListExtras.scan_map (fun p t n ->
            if (t = one || t = minus_one) then ([],[])
            else
               (  (if C.commutes ~scope:scope (CalcRing.mk_prod p) t
                   then [t] else []),
                  (if C.commutes ~scope:scope t (CalcRing.mk_prod n)
                   then [t] else [])  )
            ) (CalcRing.prod_list term))
         in
            (  lhs_candidates @ (List.flatten new_lhs),
               rhs_candidates @ (List.flatten new_rhs)  )
      ) ([],[]) term_list
   in
   if ((List.length lhs_candidates) < 1) && ((List.length rhs_candidates) < 1)
   then
      (* If there are no candidates, we're done here *)
      CalcRing.mk_sum term_list
   else 
   let factorize_and_split selected validate_fn =
      let (pos_selected, neg_selected) = (selected, neg selected) in
      List.fold_left (fun (factorized,unfactorized) term ->
         try 
            let ((lhs_of_selected,rhs_of_selected), correction) =
               try 
                  (* Try to find pos_selected *)
                  (ListExtras.split_at_pivot ~cmp_fn:C.exprs_are_identical  
                     pos_selected (CalcRing.prod_list term), one) 
               with Not_found ->
                  (* Try to find neg_selected *)
                  (ListExtras.split_at_pivot ~cmp_fn:C.exprs_are_identical  
                     neg_selected (CalcRing.prod_list term), minus_one)
            in

            let corr_lhs_of_selected = correction :: lhs_of_selected in
            Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
               "Split "^(CalculusPrinter.string_of_expr term)^" into "^
               (CalculusPrinter.string_of_expr 
                  (CalcRing.mk_prod corr_lhs_of_selected))^
               " * [...] * "^
               (CalculusPrinter.string_of_expr 
                  (CalcRing.mk_prod rhs_of_selected))         
            );
            if validate_fn corr_lhs_of_selected rhs_of_selected 
            then ( factorized@
                      [CalcRing.mk_prod 
                         (corr_lhs_of_selected@rhs_of_selected)],
                   unfactorized )
            else ( factorized, unfactorized@[term] )
         with Not_found ->
            (factorized, unfactorized@[term])
      ) ([],[]) term_list
   in
   let rcr ((factorized, unfactorized), merge_fn) =
      Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
         "Factorized: "^
            (ListExtras.string_of_list 
                CalculusPrinter.string_of_expr factorized)^"\n"^
         "Unfactorized: "^
            (ListExtras.string_of_list 
                CalculusPrinter.string_of_expr unfactorized)
      );
      (* Finally, recur. We might be able to pull additional terms out of the
         individual expressions.  Note that we're no longer able to pull out
         terms across the factorized/unfactorized boundary *)
      let ret =
         CalcRing.mk_sum [
            (merge_fn (factorize_one_polynomial scope factorized));
            (factorize_one_polynomial scope unfactorized)
         ]
      in Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
         "Final expression: \n"^(CalculusPrinter.string_of_expr ret)
      ); ret
   in
      match heuristic (lhs_candidates, rhs_candidates) scope term_list with
         | DoNotFactorize -> CalcRing.mk_sum term_list
         | FactorizeRHS(selected) ->
            Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
               "Factorizing "^
               (CalculusPrinter.string_of_expr selected)^
               " from the RHS"
            );
            rcr ((factorize_and_split selected (fun _ rhs ->
                  C.commutes ~scope:scope selected (CalcRing.mk_prod rhs))), 
                 (fun factorized -> CalcRing.mk_prod [factorized; selected])
                )
         | FactorizeLHS(selected) ->
            Debug.print "LOG-FACTORIZE-DETAIL" (fun () ->
               "Factorizing "^
               (CalculusPrinter.string_of_expr selected)^
               " from the LHS"
            );
            rcr ((factorize_and_split selected (fun lhs _ ->
                  C.commutes ~scope:scope (CalcRing.mk_prod lhs) selected)), 
                 (fun factorized -> CalcRing.mk_prod [selected; factorized])
                )


(**
   [factorize_polynomial scope expr]
   
   Invokes polynomial factorization throughout an arbitrary Calculus expression 
   wherever possible as described above in factorize_one_polynomial.
   
   @param scope   Any variables defined outside of the expression being 
                  evaluated.  This includes trigger variables, input variables 
                  from the map on the lhs of the statement being evaluated.
   @param expr    The calculus expression being processed
*)
let factorize_polynomial (scope:var_t list) (expr:C.expr_t): C.expr_t =
   Debug.print "LOG-FACTORIZE" (fun () ->
      "Factorize polynomial (START): \n"^
      CalculusPrinter.string_of_expr expr
   );
   let rewritten_expr = 
      C.rewrite ~scope:scope 
         (fun (scope,_) sum_terms -> factorize_one_polynomial scope sum_terms)
         (fun _ -> CalcRing.mk_prod)
         (fun _ -> CalcRing.mk_neg)
         (fun _ -> CalcRing.mk_val)
         (fun _ _ -> true)      
         (if Debug.active "AGGRESSIVE-FACTORIZE" 
          then combine_values expr
          else expr)
   in
      Debug.print "LOG-FACTORIZE" (fun () ->
         "Factorize polynomial (END): \n"^
         CalculusPrinter.string_of_expr rewritten_expr
      ); rewritten_expr
;;
 

type term_map_t = {
   definition     : C.expr_t;
   pos_definition : C.expr_t;
   neg_definition : C.expr_t;
   valid          : bool ref
}

(**
  Removes Negs from the expression. 

  To be on the safe side, we don't rely on combine_values here.
  Instead, we explicitly replace Negs with * {-1} in the expression.  
*)
(* let erase_negs = 
  Calculus.rewrite 
    (fun _ e -> CalcRing.mk_sum e)
    (fun _ e -> CalcRing.mk_prod e)
    (fun _ e -> CalcRing.mk_prod [ C.mk_value (Arithmetic.mk_int (-1)); e ])
    (fun _ e -> CalcRing.mk_val e)
    (fun _ _ -> true)
;;
 *) 
(* (** Removes Domains from the expression. *)
let erase_domain = 
  C.rewrite_leaves 
    (fun _ lf -> match lf with
      | DomainDelta _ -> CalcRing.one
      | _ -> CalcRing.mk_val lf)
    (fun _ _ -> true) 
;;
 *)

(**
   [cancel_terms expr]
   
   Removes opposite terms from the expression. 
   
   @param expr The processing expression
*)
let cancel_terms (big_expr: C.expr_t): C.expr_t =
   Debug.print "LOG-CANCEL-TERMS" (fun () ->
      "Cancel terms (START): "^(CalculusPrinter.string_of_expr big_expr) 
   );
   let cancel sum_list =
      let term_map = List.map (fun (schema, term) -> 
         let pos_term = normalize term in
         let neg_term = neg pos_term in
         { 
            definition     = C.mk_aggsum schema term;
            pos_definition = C.mk_aggsum schema pos_term;
            neg_definition = C.mk_aggsum schema neg_term;
            valid          = ref true 
         }
      ) (CalculusDecomposition.decompose_poly (CalcRing.mk_sum sum_list)) in 
      Debug.print "LOG-CANCEL-TERMS-DETAIL" (fun () ->
         "Attempting to cancel "^
         (ListExtras.ocaml_of_list (fun term -> 
             CalculusPrinter.string_of_expr term.definition) term_map)
      );    
      let (new_sum_list, changed) =
         ListExtras.scan_fold (fun (ret_terms, changed) lhs term rhs ->
            if not !(term.valid) then (ret_terms, changed) else
            try
               (* Try to find the matching term *)
               let cancel_term = List.find (fun cand_term -> 
                  Debug.print "LOG-CANCEL-TERMS-DEBUG" (fun () ->
                     "Comparing \n  "^
                     (CalculusPrinter.string_of_expr term.neg_definition)^
                     " with \n  "^
                     (CalculusPrinter.string_of_expr cand_term.pos_definition)
                  );
                  !(cand_term.valid) && 
                  (exprs_are_identical term.neg_definition 
                                       cand_term.pos_definition)
               ) rhs in
               term.valid := false;
               cancel_term.valid := false;
             
               Debug.print "LOG-CANCEL-TERMS-DETAIL" (fun () ->
                  "Canceling "^
                  (CalculusPrinter.string_of_expr term.definition)^
                  " with " ^
                  (CalculusPrinter.string_of_expr cancel_term.definition)
               );
               (ret_terms, true)
            with Not_found -> (ret_terms @ [ term.definition ], changed)
         ) ([], false) term_map
      in
      if not changed then sum_list else  new_sum_list 
   in
   let rewritten_expr = 
      C.rewrite 
         (fun _ sl -> CalcRing.mk_sum (cancel sl))
         (fun _ -> CalcRing.mk_prod)
         (fun _ -> CalcRing.mk_neg)
         (fun _ -> CalcRing.mk_val)
         (fun _ _ -> true)      
         big_expr
   in
      Debug.print "LOG-CANCEL-TERMS" (fun () ->
         "Cancel terms (END): "^(CalculusPrinter.string_of_expr rewritten_expr) 
      ); rewritten_expr
;;


type opt_t = 
   | OptNormalize
   | OptCombineValues
   | OptLiftEqualities
   | OptAdvanceLifts
   | OptUnifyLifts
   | OptNestingRewrites
   | OptFactorizePolynomial
   | OptCancelTerms
   | OptSimplifyDomains
   | OptExtractDomains
;;
let string_of_opt = (function
   | OptNormalize                -> "OptNormalize"
   | OptCombineValues            -> "OptCombineValues"
   | OptLiftEqualities           -> "OptLiftEqualities"
   | OptAdvanceLifts             -> "OptAdvanceLifts"
   | OptUnifyLifts               -> "OptUnifyLifts"
   | OptNestingRewrites          -> "OptNestingRewrites"
   | OptFactorizePolynomial      -> "OptFactorizePolynomial"
   | OptCancelTerms              -> "OptCancelTerms"
   | OptSimplifyDomains          -> "OptSimplifyDomain"
   | OptExtractDomains           -> "OptExtractDomain"
)
;;
let default_optimizations = 
   [  OptNormalize;
      OptLiftEqualities; 
      OptAdvanceLifts; 
      OptUnifyLifts; 
      OptNestingRewrites; 
      OptFactorizePolynomial; 
      OptCombineValues; 
      OptCancelTerms; 
      OptExtractDomains; 
      OptSimplifyDomains  ]
;;
let private_time_hash:(opt_t, float) Hashtbl.t = Hashtbl.create 10;;
let dump_timings () = 
   Hashtbl.iter (fun opt time ->
      print_endline ("[CalculusTransforms] TIME: "^(string_of_opt opt)^" -> "^
                     (string_of_float time))
   ) private_time_hash
;;

(**
  optimize_expr
    Given an expression apply the above optimizations to it until a fixed point
    is reached.
*)
let optimize_expr ?(optimizations = default_optimizations)                  
                  ((scope,schema):C.schema_t) (expr:C.expr_t): C.expr_t =

   Debug.print "LOG-CALCOPT-STEPS" (fun () ->
      "CalculusTransforms asked to optimize: "^
      (ListExtras.ocaml_of_list string_of_var scope)^
      (ListExtras.ocaml_of_list string_of_var schema)^"\n"^
      (CalculusPrinter.string_of_expr expr)
   );
   if Debug.active "LOG-CALCOPT-DETAIL" then 
   begin
      Debug.activate "LOG-COMBINE-VALUES";
      Debug.activate "LOG-FACTORIZE";      
      Debug.activate "LOG-CANCEL-TERMS";
      Debug.activate "LOG-EXTRACT-DOMAINS";
      Debug.activate "LOG-SIMPLIFY-DOMAINS";
      Debug.activate "LOG-NESTING-REWRITES";
      Debug.activate "LOG-LIFT-EQUALITIES";
      Debug.activate "LOG-UNIFY-LIFTS";
      Debug.activate "LOG-ADVANCE-LIFTS";
   end;
   if Debug.active "CALC-NO-OPTIMIZE" then expr else
 
   let fp_1 = ref (fun x -> sanity_check_variable_names x; x) in
   let include_opt_base o new_fn = 
     Fixpoint.build_if optimizations fp_1 o new_fn 
   in
   let include_opt o new_fn = 
      if not (Debug.active "TIME-CALCOPT") 
      then include_opt_base o new_fn 
      else
         let timed_fn x = 
            let start = Unix.time() in
            let result = new_fn x in
            let duration = Unix.time() -. start in 
            let tot = duration +. if (Hashtbl.mem private_time_hash o)
                                  then Hashtbl.find private_time_hash o
                                  else 0.0
            in Hashtbl.replace private_time_hash o tot; 
               result
         in include_opt_base o timed_fn
   in            
      include_opt OptAdvanceLifts            (advance_lifts scope);
      include_opt OptUnifyLifts              (unify_lifts scope schema);
      include_opt OptLiftEqualities          (lift_equalities scope);
      include_opt OptSimplifyDomains         (simplify_domains);
      include_opt OptNestingRewrites         (nesting_rewrites);
      include_opt OptExtractDomains          (extract_domains scope);
      include_opt OptNestingRewrites         (nesting_rewrites);
      include_opt OptFactorizePolynomial     (factorize_polynomial scope);
      include_opt OptCombineValues           (combine_values);
      include_opt OptCancelTerms             (cancel_terms);
      include_opt OptNormalize               (normalize);
      
   if Debug.active "LOG-CALCOPT-STEPS" then Fixpoint.build fp_1 
      (fun x -> 
         let (fail,(ivars,ovars)) = 
            try 
               (None, (C.schema_of_expr x))
            with Failure(msg) ->
               ((Some(msg)), (scope,schema))
         in
            print_endline ("OPTIMIZING: "^
               (ListExtras.ocaml_of_list fst ivars)^
               (ListExtras.ocaml_of_list fst ovars)^
               " ::>> \n"^
               (CalculusPrinter.string_of_expr x)); 
            begin match fail with
             | None      -> x
             | Some(msg) -> failwith ("Schema error: "^msg)
            end);
   Fixpoint.compute_with_history !fp_1 expr
 
