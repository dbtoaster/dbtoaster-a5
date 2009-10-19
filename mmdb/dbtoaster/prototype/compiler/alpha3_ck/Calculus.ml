
(* types *)

type var_t  = string                          (* type of variables *)
type comp_t  = Eq | Lt | Le | Neq             (* comparison operations *)
type const_t = Int    of int                  (* typed constant terms *)
             | Double of float
             | Long   of int64
             | String of string


type 'term_t generic_relcalc_lf_t =
            False
          | True
          | AtomicConstraint of comp_t * 'term_t * 'term_t
          | Rel of string * (var_t list)

type ('term_t, 'relcalc_t) generic_term_lf_t =
            AggSum of ('term_t * 'relcalc_t)
          | Const of const_t
          | Var of string


module rec CALC_BASE :
sig
   type t    = TermSemiRing.expr_t generic_relcalc_lf_t
   val  zero : t
   val  one  : t
end =
struct
   type t    = TermSemiRing.expr_t generic_relcalc_lf_t
   let  zero = False
   let  one  = True
end
and CalcSemiRing : SemiRing.SemiRing with type leaf_t = CALC_BASE.t
    = SemiRing.Make(CALC_BASE)
and TERM_BASE :
sig
   type t    = (TermSemiRing.expr_t, CalcSemiRing.expr_t) generic_term_lf_t
   val  zero : t
   val  one  : t
end =
struct
   type t    = (TermSemiRing.expr_t, CalcSemiRing.expr_t) generic_term_lf_t
   let  zero = Const(Int(0))  (* I think I mean this, even if we want to *)
   let  one  = Const(Int(1))  (* support floating point numbers.
                                 The consequence is that some optimizations
                                 will not apply for AggSum(1.0, ...). *)
end
and TermSemiRing : SemiRing.SemiRing with type leaf_t = TERM_BASE.t
    = SemiRing.Make(TERM_BASE)


type relcalc_lf_t = CalcSemiRing.leaf_t
type relcalc_t    = CalcSemiRing.expr_t
type term_lf_t    = TermSemiRing.leaf_t
type term_t       = TermSemiRing.expr_t





(* functions *)


(* accessing relational algebra expressions and terms once the above types
   are made abstract. *)


type readable_relcalc_lf_t = readable_term_t generic_relcalc_lf_t
and  readable_relcalc_t    = RA_Leaf         of readable_relcalc_lf_t
                           | RA_MultiUnion   of readable_relcalc_t list
                           | RA_MultiNatJoin of readable_relcalc_t list
and  readable_term_lf_t    =
         (readable_term_t, readable_relcalc_t) generic_term_lf_t
and  readable_term_t       = RVal    of readable_term_lf_t
                           | RProd   of readable_term_t list
                           | RSum    of readable_term_t list


let rec readable_relcalc (relcalc: relcalc_t): readable_relcalc_t =
   let lf_readable (lf: relcalc_lf_t): readable_relcalc_lf_t =
      match lf with
         AtomicConstraint(comp, x, y) ->
            AtomicConstraint(comp, (readable_term x), (readable_term y))
       | Rel(r, sch) -> Rel(r, sch)
       | False       -> False
       | True        -> True
   in
   CalcSemiRing.fold (fun x -> RA_MultiUnion x)
                     (fun y -> RA_MultiNatJoin y)
                     (fun z -> RA_Leaf(lf_readable z))
                     relcalc

and readable_term (term: term_t): readable_term_t =
   let lf_readable lf =
      match lf with
         Const(x)     -> Const(x)
       | Var(x)       -> Var(x)
       | AggSum(f, r) -> AggSum(readable_term f, readable_relcalc r)
   in
   TermSemiRing.fold (fun l -> RSum l) (fun l -> RProd l)
                     (fun x -> RVal (lf_readable x)) term



let rec make_relcalc readable_ra =
   let lf_make lf =
      match lf with
         AtomicConstraint(comp, x, y) ->
            AtomicConstraint(comp, (make_term x), (make_term y))
       | Rel(r, sch) -> Rel(r, sch)
       | False       -> False
       | True        -> True
   in
   match readable_ra with
      RA_Leaf(x)         -> CalcSemiRing.mk_val(lf_make x)
    | RA_MultiUnion(l)   -> CalcSemiRing.mk_sum  (List.map make_relcalc l)
    | RA_MultiNatJoin(l) -> CalcSemiRing.mk_prod (List.map make_relcalc l)

and make_term readable_term =
   let lf_make lf =
      match lf with
         Const(x)    -> Const(x)
       | Var(x)      -> Var(x)
       | AggSum(f,r) -> AggSum(make_term f, make_relcalc r)
   in
   match readable_term with
      RSum(x)  -> TermSemiRing.mk_sum( List.map make_term x)
    | RProd(x) -> TermSemiRing.mk_prod(List.map make_term x)
    | RVal(x)  -> TermSemiRing.mk_val(lf_make x)





(* Note: these are not the free variables, because the variables used in
   aggregate terms inside AtomicConstraints are not free. *)
let rec relcalc_vars relcalc: var_t list =
   let lf_vars lf =
      match lf with
         False    -> [] (* to check consistency of unions, ignore relational
                           algebra expressions that are equivalent to empty *)
       | True     -> []
       | Rel(n,s) -> Util.ListAsSet.no_duplicates s
       | AtomicConstraint (_, c1, c2) ->
            Util.ListAsSet.union (term_vars c1) (term_vars c2)
            (* to get the free variables, replace this by code that
               extracts variables from terms c1 and c2 unless they are nested
               inside AggSum terms. *)
   in
   CalcSemiRing.fold Util.ListAsSet.multiunion
                    Util.ListAsSet.multiunion lf_vars relcalc

and term_vars term: var_t list =
   let leaf_f x = match x with
      Var(y) -> [y]
    | AggSum(f, r) -> Util.ListAsSet.union (term_vars f) (relcalc_vars r)
    | _ -> []
   in
   TermSemiRing.fold Util.ListAsSet.multiunion
                     Util.ListAsSet.multiunion leaf_f term



type rr_ret_t = (var_t list) * ((var_t * var_t) list)

(* set of safe variables of a formula; a formula phi is range-restricted
   given a set of bound variables (which are treated like constants, i.e.,
   are safe) iff all free variables are in (safe_vars phi bound_vars).
   The result contains all the param_vars.
*)
let safe_vars (phi: relcalc_t) (param_vars: var_t list) : (var_t list) =
   let and_rr (l: rr_ret_t list) =
      let l1 = (Util.ListAsSet.multiunion (List.map (fun (x,y) -> x) l))
      in
      let l2 = (Util.ListAsSet.multiunion (List.map (fun (x,y) -> y) l))
      in
      ((Util.Vars.closure l2 (Util.ListAsSet.union l1 param_vars)), [])
   in
   let or_rr l = (Util.ListAsSet.multiinter (List.map (fun (x,y) -> x) l), [])
   in
   let rr_lf (psi: relcalc_lf_t) (param_vars: var_t list): rr_ret_t =
      match psi with
         False    -> ([], [])
       | True     -> ([], [])
       | Rel(n,s) -> (Util.ListAsSet.no_duplicates s, [])
       | AtomicConstraint(Eq, TermSemiRing.Val(Var c1),
                              TermSemiRing.Val(Var c2)) ->
            (Util.Vars.closure [(c1, c2)] param_vars, [(c1, c2)])
       | AtomicConstraint (_) -> ([], [])
   in
   let (x,y) = CalcSemiRing.fold or_rr and_rr (fun x -> rr_lf x param_vars) phi 
   in
   (Util.ListAsSet.union x param_vars)






(* TODO: enforce that the variables occurring in f are among the
   range-restricted variables of r. Otherwise throw exception. *)
let make_aggsum f r =
   if (r = CalcSemiRing.zero) then TermSemiRing.zero
   else if (f = TermSemiRing.zero) then TermSemiRing.zero
   else if (r = CalcSemiRing.one) then f
   else TermSemiRing.mk_val (AggSum (f, r))


let constraints_only (r: relcalc_t): bool =
   let leaves = CalcSemiRing.fold List.flatten List.flatten (fun x -> [x]) r
   in
   let bad x = match x with
         Rel(_) -> true
       | False  -> true
       | True   -> false
       | AtomicConstraint(_,_,_) -> false
   in
   (List.length (List.filter bad leaves) = 0)


exception Assert0Exception (* should never be reached *)

(* complement a constraint-only relcalc expression.
   This is an auxiliary function for term_delta. *)
let complement relcalc =
   let leaf_f lf =
      match lf with
         AtomicConstraint(comp, t1, t2) ->
            CalcSemiRing.mk_val(AtomicConstraint(
               (match comp with Eq -> Neq | Neq -> Eq
                              | Lt  -> Le | Le  -> Lt), t2, t1))
       | True  -> CalcSemiRing.mk_val(False)
       | False -> CalcSemiRing.mk_val(True)
       | _ -> raise Assert0Exception
   in
   (* switch prod and sum *)
   (CalcSemiRing.fold CalcSemiRing.mk_prod CalcSemiRing.mk_sum leaf_f relcalc)




let rec relcalc_delta relname (tuple: var_t list)
                              (relcalc: relcalc_t): relcalc_t =
   let delta_leaf lf =
      match lf with
         False -> CalcSemiRing.zero
       | Rel(r, l) when relname = r ->
            let f (x,y) = CalcSemiRing.mk_val(
                             AtomicConstraint(Eq, TermSemiRing.mk_val(Var(x)),
                                                  TermSemiRing.mk_val(Var(y))))
            in
            CalcSemiRing.mk_prod (List.map f (List.combine l tuple))
            (* We replace the relation atom R(vec{x}) by equalities
               vec{x} = tuple.
               For example, delta_leaf Rel("R", ["A"; "B"])
               with relname "R" and tuple ["x"; "y"] is (A=x and B=y). *)
       | Rel(x, l) -> CalcSemiRing.zero
       | True -> CalcSemiRing.zero
       | AtomicConstraint(comp, t1, t2) ->
            if(((term_delta relname tuple t1) = TermSemiRing.zero) &&
               ((term_delta relname tuple t2) = TermSemiRing.zero)) then
                  CalcSemiRing.zero
            else raise Assert0Exception
                 (* This case is not possible.
                    The terms with nonzero delta have been pulled
                    out of the constraint in term_delta. *)
   in
   CalcSemiRing.delta delta_leaf relcalc

and term_delta relname (tuple: var_t list) (term: term_t): term_t =
   let rec leaf_delta lf =
      match lf with
         Const(c)     -> TermSemiRing.zero
       | Var(x)       -> TermSemiRing.zero
       | AggSum(f, r) ->
            let d_f = (term_delta relname tuple f)
            in
            if (constraints_only r) then
               (* FIXME: this is overkill (but supposedly correct) in the
                  case that r does not contain AggSum terms, i.e., in the
                  case that all contained terms have zero delta. *)
               let new_r = (* replace each term t in r by (t + delta t) *)
                  let leaf_f lf =
                     (match lf with
                        AtomicConstraint(c, t1, t2) ->
                        CalcSemiRing.mk_val(AtomicConstraint(c,
                           TermSemiRing.mk_sum [t1;
                              (term_delta relname tuple t1)],
                           TermSemiRing.mk_sum [t2;
                              (term_delta relname tuple t2)]))
                      | True ->
                        CalcSemiRing.mk_val(True)
                      | _ -> raise Assert0Exception)
                  in
                  CalcSemiRing.apply_to_leaves leaf_f r
               in
               (*
                  (if (     new_r             ) then (delta f) else 0)
                + (if (     new_r  and (not r)) then        f  else 0)
                - (if ((not new_r) and      r ) then        f  else 0)
               *)
               TermSemiRing.mk_sum [
                  make_aggsum d_f new_r;
                  make_aggsum f (CalcSemiRing.mk_prod [new_r; (complement r)]);
                  make_aggsum (TermSemiRing.mk_prod[
                                  TermSemiRing.mk_val(Const(Int (-1))); f])
                              (CalcSemiRing.mk_prod [ (complement new_r); r ])
               ]
            else
               let d_r = (relcalc_delta relname tuple r)
               in
               TermSemiRing.mk_sum [ (make_aggsum d_f   r);
                                     (make_aggsum   f d_r);
                                     (make_aggsum d_f d_r) ]
   in
   TermSemiRing.delta leaf_delta term




type substitution_t = var_t Util.Vars.mapping_t  (* (var_t * var_t) list *)


let rec apply_variable_substitution_to_relcalc (theta: substitution_t)
                                               (alg: relcalc_t): relcalc_t =
   let substitute_leaf lf =
      match lf with
         Rel(n, vars) ->
            CalcSemiRing.mk_val
               (Rel(n, List.map (Util.Vars.apply_mapping theta) vars))
       | AtomicConstraint(comp, x, y) ->
            (CalcSemiRing.mk_val (AtomicConstraint(comp,
               (apply_variable_substitution_to_term theta x),
               (apply_variable_substitution_to_term theta y) )))
       | _ -> (CalcSemiRing.mk_val lf)
   in
   (CalcSemiRing.apply_to_leaves substitute_leaf alg)

and apply_variable_substitution_to_term (theta: substitution_t)
                                        (m: term_t): term_t =
   let leaf_f lf =
      match lf with
         Var(y) -> TermSemiRing.mk_val(Var(Util.Vars.apply_mapping theta y))
       | AggSum(f, r) ->
            TermSemiRing.mk_val(
               AggSum(apply_variable_substitution_to_term theta f,
                              apply_variable_substitution_to_relcalc theta r))
       | _ -> TermSemiRing.mk_val(lf)
   in 
   (TermSemiRing.apply_to_leaves leaf_f m)




let polynomial (q: relcalc_t): relcalc_t = CalcSemiRing.polynomial q

let monomials (q: relcalc_t): (relcalc_t list) =
   let p = CalcSemiRing.polynomial q in
   if (p = CalcSemiRing.zero) then []
   else CalcSemiRing.sum_list p

let relcalc_one  = CalcSemiRing.mk_val(CALC_BASE.one)
let relcalc_zero = CalcSemiRing.mk_val(CALC_BASE.zero)

let monomial_as_hypergraph (monomial: relcalc_t): (relcalc_t list) =
   CalcSemiRing.prod_list monomial

let hypergraph_as_monomial (hypergraph: relcalc_t list): relcalc_t =
   CalcSemiRing.mk_prod hypergraph



(* given a relcalc monomial, returns a pair (eqs, rest), where
   eqs is the list of equalities occurring in the input
   and rest is the input monomial minus the equalities.
   Auxiliary function used in extract_substitutions. *)
let split_off_equalities (monomial: relcalc_t) :
                         (((var_t * var_t) list) * relcalc_t) =
   let atoms = CalcSemiRing.prod_list monomial
   in
   let split0 lf =
      match lf with
         CalcSemiRing.Val(AtomicConstraint(Eq, TermSemiRing.Val(Var(x)),
            TermSemiRing.Val(Var(y)))) -> ([(x, y)], [])
         (* TODO: this can be generalized.
            We could replace variables by non-variable terms in some
            places. Question: do we want to do this in Rel-atoms too? *)
       | _ -> ([],     [lf])
   in
   let (eqs, rest) = (List.split (List.map split0 atoms))
   in
   (List.flatten eqs, CalcSemiRing.mk_prod(List.flatten rest))


let extract_substitutions (monomial: relcalc_t)
                          (bound_vars: var_t list) :
                          (substitution_t * relcalc_t) =
   let (eqs, rest) = split_off_equalities monomial
   in
   (* an equation will be in eqs_to_keep if it tries to set two bound vars
      equal, where we are not allowed to replace either. We have to keep
      these equalities. *)
   let (theta, eqs_to_keep) = Util.Vars.unifier eqs bound_vars
   in
   let f (x,y) = CalcSemiRing.mk_val (AtomicConstraint(Eq,
                    TermSemiRing.Val(Var(x)), TermSemiRing.Val(Var(y))))
   in
   (* add the inconsistent equations again as constraints. *)
   let rest2 = CalcSemiRing.mk_prod(
      [(apply_variable_substitution_to_relcalc theta rest)]
      @ (List.map f eqs_to_keep))
   in
   (theta, rest2)





let term_zero = TermSemiRing.mk_val (TERM_BASE.zero)
let term_one  = TermSemiRing.mk_val (TERM_BASE.one)


(* factorize an AggSum(f, r) where f and r are monomials *)
let factorize_aggsum_mm (f_monomial: term_t)
                        (r_monomial: relcalc_t) : term_t =
   if (r_monomial = relcalc_zero) then TermSemiRing.zero
   else
      let factors = (Util.MixedHyperGraph.connected_components
                       term_vars relcalc_vars
                       (Util.MixedHyperGraph.make
                           (TermSemiRing.prod_list f_monomial)
                           (monomial_as_hypergraph r_monomial)))
      in
      let mk_aggsum component =
         let (f, r) = Util.MixedHyperGraph.extract_atoms component in
         (make_aggsum (TermSemiRing.mk_prod f) (hypergraph_as_monomial r))
      in
      TermSemiRing.mk_prod (List.map mk_aggsum factors)


(* polynomials, recursively: in the end, +/union only occurs on the topmost
   level.
*)
let rec roly_poly (ma: term_t) : term_t =
   let r_leaf_f (lf: relcalc_lf_t) : relcalc_t =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            CalcSemiRing.mk_val(AtomicConstraint(c, roly_poly t1, roly_poly t2))
       | _ -> CalcSemiRing.mk_val(lf)
   in
   let t_leaf_f (lf: term_lf_t): term_t =
      match lf with
        Const(_)     -> TermSemiRing.mk_val(lf)
      | Var(_)       -> TermSemiRing.mk_val(lf)
      | AggSum(f, r) ->
         (
            (* recursively normalize contents of complex terms in
               atomic constraints. *)
            let r2 = CalcSemiRing.apply_to_leaves r_leaf_f r
            in
            let r_monomials = monomials r2 in
            let f_monomials = TermSemiRing.sum_list (roly_poly f)
            in
            TermSemiRing.mk_sum (List.flatten (List.map
               (fun y -> (List.map (fun x -> factorize_aggsum_mm x y)
                                    f_monomials))
               r_monomials))
         )
   in
   TermSemiRing.polynomial (TermSemiRing.apply_to_leaves t_leaf_f ma)




(* the input map algebra expression ma must be sum- and union-free.
   Call roly_poly first to ensure this.

   Auxiliary function not visible to the outside world.
*)
let rec simplify_roly (ma: term_t) (bound_vars: var_t list)
                        : (((var_t * var_t) list) * term_t) =
   let augment_bound_vars bound_vars b =
      let (_, b_img) = List.split b in
      Util.ListAsSet.union bound_vars b_img
   in
   let my_flatten combinator_fn l =
      let (b, f) = List.split l
      in ((List.flatten b), (combinator_fn f))
      (* FIXME: make sure that the concatenation of bindings does not
         lead to contradictions. *)
   in
   let r_leaf_f lf =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            let (l1, t1b) = simplify_roly t1 bound_vars in
            let (l2, t2b) = simplify_roly t2 bound_vars in
                            (* TODO: pass bindings from first to second *)
            (l1 @ l2, CalcSemiRing.mk_val(AtomicConstraint(c, t1b, t2b)))
       | _ -> ([], CalcSemiRing.mk_val lf)
   in
   let t_leaf_f lf =
      match lf with
        Const(_)     -> ([], TermSemiRing.mk_val(lf))
      | Var(_)       -> ([], TermSemiRing.mk_val(lf))
      | AggSum(f, r) ->
        (
           (* we test equality, not equivalence, to zero here. Sufficient
              if we first normalize using roly_poly. *)
           if (r = relcalc_zero) then raise Assert0Exception
           else if (f = TermSemiRing.zero) then ([], TermSemiRing.zero)
           else
              (* simplify terms in atomic constraints *)
              let (b1, r2) = CalcSemiRing.fold (my_flatten CalcSemiRing.mk_sum)
                                              (my_flatten CalcSemiRing.mk_prod)
                                              r_leaf_f r
                 (* TODO: use b1 *)
              in
              let (b, non_eq_cons) = extract_substitutions r2 bound_vars
              in
              let (b2, f2) = simplify_roly
                                (apply_variable_substitution_to_term b f)
                                (augment_bound_vars bound_vars b)
              in
              let b3 = Util.ListAsSet.union b b2
              in
              if (non_eq_cons = relcalc_one) then (b3, f2)
              else (b3, TermSemiRing.mk_val(AggSum(f2, non_eq_cons)))
                (* we represent the if-condition as a relational algebra
                   expression to use less syntax *)
        )
   in
   TermSemiRing.fold (my_flatten TermSemiRing.mk_sum)
                     (my_flatten TermSemiRing.mk_prod) t_leaf_f ma


(* apply roly_poly and simplify by unifying variables.
   returns a list of pairs (dimensions', monomial)
   where monomial is the simplified version of a nested monomial of ma
   and dimensions' is dimensions -- a set of variables occurring in term --
   after application of the substitution used to simplify monomial. *)
let simplify (ma: term_t)
             (bound_vars: var_t list)
             (dimensions: var_t list) :
             ((var_t list * term_t) list) =
   let simpl f =
      let (b, f2) = simplify_roly f bound_vars in
      ((List.map (Util.Vars.apply_mapping b) dimensions), f2)
   in
   List.map simpl (TermSemiRing.sum_list (roly_poly ma))


let rec extract_aggregates term =
   let r_aggs r =
      let r_leaf_f lf =
         match lf with
            AtomicConstraint(_, t1, t2) ->
               (extract_aggregates t1) @ (extract_aggregates t2)
          | _ -> []
      in
      CalcSemiRing.fold List.flatten List.flatten r_leaf_f r
   in
   let t_leaf_f x =
      match x with
         AggSum(f, r) ->
            (* if r is constraints_only, take the aggregates from the
               atoms of r; otherwise, return x monolithically. *)
            if (constraints_only r) then ((extract_aggregates f) @ (r_aggs r))
            else [TermSemiRing.Val x]
       | _ -> []
   in
   Util.ListAsSet.no_duplicates
      (TermSemiRing.fold List.flatten List.flatten t_leaf_f term)




(* pseudocode output of relcalc expressions and terms. *)
let rec relcalc_as_string (relcalc: relcalc_t)
                         (theta: (term_t * string) list): string =
   let sum_f  l = "(" ^ (Util.string_of_list " or " l) ^ ")" in
   let prod_f l = (Util.string_of_list " and " l) in
   let constraint_as_string c x y =
            (term_as_string x theta) ^ c ^ (term_as_string y theta) in
   let leaf_f lf =
      match lf with
         AtomicConstraint(Eq,  x, y) -> constraint_as_string "="  x y
       | AtomicConstraint(Lt,  x, y) -> constraint_as_string "<"  x y
       | AtomicConstraint(Le,  x, y) -> constraint_as_string "<=" x y
       | AtomicConstraint(Neq, x, y) -> constraint_as_string "<>" x y
       | False       -> "false"
       | True        -> "true"
       | Rel(r, sch) -> r^"("^(Util.string_of_list ", " sch)^")"
   in
   CalcSemiRing.fold sum_f prod_f leaf_f relcalc

and term_as_string (m: term_t)
                   (aggsum_theta :(term_t * string) list):
                   string =
   let leaf_f (lf: term_lf_t) =
   (match lf with
      Const(c)    ->
      (
         match c with
            Int(i)    -> string_of_int i
          | Double(d) -> string_of_float d
          | Long(l)   -> "(int64 output not implemented)" (* TODO *)
          | String(s) -> "'" ^ s ^ "'"
      )
    | Var(x)      -> x
    | AggSum(f,r) ->
      if (constraints_only r) then
         (* this is used even if the AtomicConstraints contain aggregate
            terms. *)
         "(if " ^ (relcalc_as_string r aggsum_theta) ^ " then " ^
                  (term_as_string   f aggsum_theta) ^ " else 0)"
      else
         (* replace aggregate term by map access *)
         Util.apply aggsum_theta
                    ("AggSum("^(term_as_string   f aggsum_theta)^", "
                              ^(relcalc_as_string r aggsum_theta)^")")
                    (TermSemiRing.mk_val(lf))
   )
   in (TermSemiRing.fold (fun l -> "("^(Util.string_of_list "+" l)^")")
                         (fun l -> "("^(Util.string_of_list "*" l)^")")
                         leaf_f m)


