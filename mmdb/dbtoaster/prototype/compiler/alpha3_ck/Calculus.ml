
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
          | Var of var_t 
          | External of (string * (var_t list))
                        (* name and variable list;
                           could be generalized to terms *)


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

type var_mapping_t = (var_t * var_t) list

(* the first is always the full term and the second is the external. *)
type term_mapping_t = (term_t * term_t) list


(* accessing relational algebra expressions and terms *)
type readable_relcalc_lf_t = readable_term_t generic_relcalc_lf_t
and  readable_relcalc_t    = RA_Leaf         of readable_relcalc_lf_t
                           | RA_MultiUnion   of readable_relcalc_t list
                           | RA_MultiNatJoin of readable_relcalc_t list
and  readable_term_lf_t    =
         (readable_term_t, readable_relcalc_t) generic_term_lf_t
and  readable_term_t       = RVal    of readable_term_lf_t
                           | RProd   of readable_term_t list
                           | RSum    of readable_term_t list



(* functions *)

let relcalc_one  = CalcSemiRing.mk_val(CALC_BASE.one)
let relcalc_zero = CalcSemiRing.mk_val(CALC_BASE.zero)
let term_zero    = TermSemiRing.mk_val (TERM_BASE.zero)
let term_one     = TermSemiRing.mk_val (TERM_BASE.one)


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
         Const(x)        -> Const(x)
       | Var(x)          -> Var(x)
       | AggSum(f, r)    -> AggSum(readable_term f, readable_relcalc r)
       | External(n, vs) -> External(n, vs)
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
         Const(x)       -> Const(x)
       | Var(x)         -> Var(x)
       | AggSum(f,r)    -> AggSum(make_term f, make_relcalc r)
       | External(n,vs) -> External(n,vs)
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




exception Assert0Exception of string (* should never be reached *)


type relalg_t =
   Alg_MultiProd of (relalg_t list)
 | Alg_Selection of (((comp_t * var_t * var_t) list) * relalg_t)
 | Alg_Rel of string * string
 | Alg_True


(* FIXME: this is only partially implemented.
   The code demonstrates how to deal with variables bound from the
   outside and how to extract implicit joins to create selection conditions.
   For example,

Calculus.relcalc_as_algebra
(make_relcalc(
  RA_MultiNatJoin
     [RA_Leaf (Rel ("C", ["c.custkey"]));
      RA_Leaf (Rel ("O", ["c.custkey"; "l1.orderkey"]));
      RA_Leaf (Rel ("L", ["l1.quantity"; "l1.orderkey"]));
      RA_Leaf (AtomicConstraint (Le, RVal (Const (Int 1)),
                                 RVal (External ("foo1", ["l1.orderkey"]))))]))
"X" ["c.custkey"] =
Alg_Selection
 ([(Eq, "X1.1", "X2.1"); (Eq, "X1.1", "c.custkey"); (Eq, "X2.2", "X3.2")],
  Alg_MultiProd
   [Alg_Rel ("C", "X1"); Alg_Rel ("O", "X2"); Alg_Rel ("L", "X3")])

   TODO: We ignore AtomicConstraints, and we do not yet create algebra for
         terms.
*)
let relcalc_as_algebra (r: relcalc_t) (rn_name_prefix: string)
                       (bound_vars: var_t list): relalg_t =
   let f lf =
      match lf with
         AtomicConstraint(c, t1, t2) -> ([], [(c, t1, t2)])
       | Rel(n, vs) -> ([(n, vs)], [])
       | _ -> raise (Assert0Exception "Calculus.relcalc_as_algebra")
   in
   let mono            = CalcSemiRing.cast_to_monomial r in
   let (rels0, cons0)  = List.split (List.map f mono) in
   let rels            = List.flatten rels0 in
       (* FIXME:  cons = List.flatten cons0 is currently ignored. *)
   let (vars, algrels) = List.split
                (List.map (fun (n, (r, vs)) -> ((n, vs), Alg_Rel(r, n)))
                          (Util.add_names rn_name_prefix rels))
   in
   let g (n, vs) = (Util.add_names (n^".") vs)
   in
   let vars2 = (List.flatten (List.map g vars)) @
               (List.map (fun x -> (x, x)) bound_vars)
   in
   let components: (var_t * var_t) list list =
      Util.HyperGraph.connected_components (fun (x,y) -> [x;y]) vars2
   in
   let h comp =
      List.map (fun x -> (Eq, fst(List.hd comp), fst x)) (List.tl comp)
   in
   let algconds = List.flatten (List.map h components)
   in
   Alg_Selection(algconds, Alg_MultiProd(algrels))





(* TODO: enforce that the variables occurring in f are among the
   range-restricted variables of r. Otherwise throw exception. *)
let mk_aggsum f r =
   if (r = CalcSemiRing.zero) then TermSemiRing.zero
   else if (f = TermSemiRing.zero) then TermSemiRing.zero
   else if (r = CalcSemiRing.one) then f
   else TermSemiRing.mk_val (AggSum (f, r))


let constraints_only (r: relcalc_t): bool =
   let leaves = CalcSemiRing.fold List.flatten List.flatten (fun x -> [x]) r
   in
   let bad x = match x with
       | True -> false
       | AtomicConstraint(_,_,_) -> false
       | _  -> true
   in
   (List.length (List.filter bad leaves) = 0)

(* complement a constraint-only relcalc expression.
   This is an auxiliary function for term_delta. *)
let complement relcalc =
   let leaf_f lf =
      match lf with
         AtomicConstraint(comp, t1, t2) ->
            CalcSemiRing.mk_val(AtomicConstraint(
               (match comp with Eq -> Neq | Neq -> Eq
                              | Lt -> Le  | Le  -> Lt), t2, t1))
       | True  -> CalcSemiRing.mk_val(False)
       | False -> CalcSemiRing.mk_val(True)
       | _ -> raise (Assert0Exception "Calculus.complement")
   in
   (* switch prod and sum *)
   (CalcSemiRing.fold CalcSemiRing.mk_prod CalcSemiRing.mk_sum leaf_f relcalc)


(* split a monomial into flat atoms and nested atoms, and provide the
   list of variables shared between the two monomials created.
   An atom is nested if it is an AtomicConstraint in which at least
   on term contains an AggSum term.
*)
let split_nested (monomial: relcalc_t) 
                 : relcalc_t * relcalc_t =
   let is_nesting_atom lf: bool =
      match lf with
         CalcSemiRing.Val(AtomicConstraint(c, t1, t2)) ->
            let is_nested_term t_lf =
               (match t_lf with
                  External(_,_) -> true
                | AggSum(_,_) -> true
                | _ -> false
               )
            in
            ([] != (List.filter is_nested_term
                     ((TermSemiRing.leaves t1) @ (TermSemiRing.leaves t2))))
       | _ -> false
   in
   let atoms           = List.map CalcSemiRing.mk_val
                                  (CalcSemiRing.cast_to_monomial monomial) in
   let nesting_atoms   = List.filter is_nesting_atom atoms in
   let flat_atoms      = Util.ListAsSet.diff atoms nesting_atoms in
   (CalcSemiRing.mk_prod flat_atoms,
    CalcSemiRing.mk_prod nesting_atoms)



let rec apply_variable_substitution_to_relcalc (theta: var_mapping_t)
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

and apply_variable_substitution_to_term (theta: var_mapping_t)
                                        (m: term_t): term_t =
   let leaf_f lf =
      match lf with
         Var(y) -> TermSemiRing.mk_val(Var(Util.Vars.apply_mapping theta y))
       | AggSum(f, r) ->
            TermSemiRing.mk_val(
               AggSum(apply_variable_substitution_to_term theta f,
                              apply_variable_substitution_to_relcalc theta r))
       | External(n, vs) ->
            TermSemiRing.mk_val(External(n, List.map
               (Util.Vars.apply_mapping theta) vs))
       | _ -> TermSemiRing.mk_val(lf)
   in 
   (TermSemiRing.apply_to_leaves leaf_f m)




let polynomial (q: relcalc_t): relcalc_t = CalcSemiRing.polynomial q

let monomials (q: relcalc_t): (relcalc_t list) =
   let p = CalcSemiRing.polynomial q in
   if (p = CalcSemiRing.zero) then []
   else CalcSemiRing.sum_list p

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
   let leaf_f lf =
      match lf with
         AtomicConstraint(Eq, TermSemiRing.Val(Var(x)),
            TermSemiRing.Val(Var(y))) -> ([(x, y)], CalcSemiRing.one)
         (* TODO: this can be generalized.
            We could replace variables by non-variable terms in some
            places. Question: do we want to do this in Rel-atoms too? *)
       | _ -> ([], CalcSemiRing.mk_val(lf))
   in
   CalcSemiRing.extract
      (fun x -> raise (Assert0Exception "Calculus.split_off_equalities"))
      List.flatten leaf_f monomial


let extract_substitutions (monomial: relcalc_t)
                          (bound_vars: var_t list) :
                          (var_mapping_t * relcalc_t) =
   let (eqs, rest) = split_off_equalities monomial
   in
   (* an equation will be in eqs_to_keep if it tries to set two bound vars
      equal, where we are not allowed to replace either. We have to keep
      these equalities. *)
   let (theta, eqs_to_keep) = Util.Vars.unifier eqs bound_vars
   in
   let f (x,y) = CalcSemiRing.mk_val (AtomicConstraint(Eq,
                    TermSemiRing.mk_val(Var(x)), TermSemiRing.mk_val(Var(y))))
   in
   (* add the inconsistent equations again as constraints. *)
   let rest2 = CalcSemiRing.mk_prod(
      [(apply_variable_substitution_to_relcalc theta rest)]
      @ (List.map f eqs_to_keep))
   in
   (theta, rest2)






(* factorize an AggSum(f, r) where f and r are monomials *)
let factorize_aggsum_mm (f_monomial: term_t)
                        (r_monomial: relcalc_t) : term_t =
   if (r_monomial = relcalc_zero) then TermSemiRing.zero
   else
      let factors = Util.MixedHyperGraph.connected_components
                       term_vars relcalc_vars
                       (Util.MixedHyperGraph.make
                           (TermSemiRing.prod_list f_monomial)
                           (monomial_as_hypergraph r_monomial))
      in
      let mk_aggsum2 component =
         let (f, r) = Util.MixedHyperGraph.extract_atoms component in
         (mk_aggsum (TermSemiRing.mk_prod f) (hypergraph_as_monomial r))
      in
      TermSemiRing.mk_prod (List.map mk_aggsum2 factors)




let rec apply_bottom_up (aggsum_f: term_t -> relcalc_t -> term_t)
                        (aconstraint_f: comp_t -> term_t -> term_t -> relcalc_t)
                        (term: term_t) : term_t =
   let r_leaf_f (lf: relcalc_lf_t): relcalc_t =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            aconstraint_f c (apply_bottom_up aggsum_f aconstraint_f t1)
                            (apply_bottom_up aggsum_f aconstraint_f t2)
       | _ -> CalcSemiRing.mk_val(lf)
   in
   let t_leaf_f (lf: term_lf_t): term_t =
      match lf with
         AggSum(f, r) -> aggsum_f (apply_bottom_up aggsum_f aconstraint_f f)
                                  (CalcSemiRing.apply_to_leaves r_leaf_f r)
       |  _ -> TermSemiRing.mk_val(lf)
   in
   TermSemiRing.apply_to_leaves t_leaf_f term


(* polynomials, recursively: in the end, +/union only occurs on the topmost
   level.
*)
let roly_poly (term: term_t) : term_t =
   let aconstraint_f c t1 t2 =
      CalcSemiRing.mk_val(AtomicConstraint(c,
         (TermSemiRing.polynomial t1), (TermSemiRing.polynomial t2)))
   in
   let aggsum_f f r =
      (* recursively normalize contents of complex terms in
         atomic constraints. *)
      let r_monomials = monomials r in
      let f_monomials = TermSemiRing.sum_list (TermSemiRing.polynomial f)
      in
      (* distribute the sums in r_monomials and f_monomials and factorize. *)
      TermSemiRing.mk_sum (List.flatten (List.map
         (fun y -> (List.map (fun x -> factorize_aggsum_mm x y)
                              f_monomials))
         r_monomials))
   in
   TermSemiRing.polynomial (apply_bottom_up aggsum_f aconstraint_f term)



(* the input formula must be or-free.  Call roly_poly first to ensure this.

   We simplify conditions and as a consequence formulae and terms
   by extracting equalities and using them to substitute variables.
   The variable substitutions have to be propagated and kept consistent
   across the expression tree (which is not fully done right now).
   The most important propagation of substitutions is, in AggSum(f, r)
   expressions, from r into f (but not the other way round), and obviously
   upwards.
*)
let rec simplify_calc_monomial (recurse: bool)
                               (relcalc: relcalc_t) (bound_vars: var_t list)
                               : (var_mapping_t * relcalc_t) =
   let leaf_f lf =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            let t1b = if recurse then (snd (simplify_roly true t1 bound_vars))
                           else t1 in
            let t2b = if recurse then (snd (simplify_roly true t2 bound_vars))
                           else t2 in
            CalcSemiRing.mk_val(AtomicConstraint(c, t1b, t2b))
       | _ -> CalcSemiRing.mk_val lf
   in
   extract_substitutions (CalcSemiRing.apply_to_leaves leaf_f relcalc)
                         bound_vars

and simplify_roly (recurse: bool) (term: term_t) (bound_vars: var_t list):
                  (var_mapping_t * term_t) =
   let leaf_f lf =
      match lf with
         AggSum(f, r) ->
            (* we test equality, not equivalence, to zero here. Sufficient
               if we first normalize using roly_poly. *)
            if (r = relcalc_zero) then
               raise (Assert0Exception "Calculus.simplify_roly.t_leaf_f")
            else if (f = TermSemiRing.zero) then ([], TermSemiRing.zero)
            else
               let (b, non_eq_cons) =
                  simplify_calc_monomial recurse r bound_vars
               in
               let f1 = apply_variable_substitution_to_term b f
               in
               (* the variables that are free in relcalc are bound in term;
                  that is, values are passed from relcalc to term. *)
               let (_, f2) = simplify_roly true f1
                  (Util.ListAsSet.multiunion [bound_vars; (relcalc_vars r);
                                              (Util.Function.img b)])
               in
               if (non_eq_cons = relcalc_one) then (b, f2)
               else (b, TermSemiRing.mk_val(AggSum(f2, non_eq_cons)))
                 (* we represent the if-condition as a calculus
                    expression to use less syntax *)
       | _            -> ([], TermSemiRing.mk_val(lf))
   in
   (* Note: in general, this can lead to an inconsistent substitution,
      which only hurts if a param variable occurs in more than
      one AggSum term. *)
   TermSemiRing.extract List.flatten List.flatten leaf_f term



(* apply roly_poly and simplify by unifying variables.
   returns a list of pairs (dimensions', monomial)
   where monomial is the simplified version of a nested monomial of term
   and dimensions' is dimensions -- a set of variables occurring in term --
   after application of the substitution used to simplify monomial. *)
let simplify (term: term_t)
             (bound_vars: var_t list)
             (params: var_t list) :
             ((var_t list * term_t) list) =
   let simpl f =
      (* we want to unify params if possible to eliminate for loops, but
         we do not want to use substitutions from aggregates nested in
         atomic constraints. The following strategy of calling simplify_roly
         twice with suitable arguments achieves that. *)
      let (_, t1) = simplify_roly true  f (bound_vars @ params) in
      let (b, t2) = simplify_roly false t1 bound_vars
      in
      ((List.map (Util.Vars.apply_mapping b) params), t2)
   in
   List.map simpl (TermSemiRing.sum_list (roly_poly term))


(* pseudocode output of relcalc expressions and terms. *)
let rec relcalc_as_string (relcalc: relcalc_t): string =
   let sum_f  l = "(" ^ (Util.string_of_list " or " l) ^ ")" in
   let prod_f l = (Util.string_of_list " and " l) in
   let constraint_as_string c x y =
            (term_as_string x) ^ c ^ (term_as_string y) in
   let leaf_f lf =
      match lf with
         AtomicConstraint(c,  x, y) ->
            let op = match c with
               Eq -> "=" | Lt -> "<" | Le -> "<=" | Neq -> "<>"
            in
            constraint_as_string op  x y
       | False       -> "false"
       | True        -> "true"
       | Rel(r, sch) -> r^"("^(Util.string_of_list ", " sch)^")"
   in
   CalcSemiRing.fold sum_f prod_f leaf_f relcalc

and term_as_string (m: term_t): string =
   let leaf_f (lf: term_lf_t) =
   (match lf with
      Const(c)           ->
         (match c with
            Int(i)    -> string_of_int i
          | Double(d) -> string_of_float d
          | Long(l)   -> "(int64 output not implemented)" (* TODO *)
          | String(s) -> "'" ^ s ^ "'"
         )
    | Var(x)             -> x
    | External(n,params) -> n^"["^(Util.string_of_list ", " params)^"]"
    | AggSum(f,r)        ->
      if (constraints_only r) then
         "(if " ^ (relcalc_as_string r) ^ " then " ^
                  (term_as_string    f) ^ " else 0)"
      else
         "AggSum("^(term_as_string f)^", " ^(relcalc_as_string r)^")"
   )
   in (TermSemiRing.fold (fun l -> "("^(Util.string_of_list "+" l)^")")
                         (fun l -> "("^(Util.string_of_list "*" l)^")")
                         leaf_f m)




let rec extract_aggregates_from_calc (aggressive: bool) (relcalc: relcalc_t) =
   let r_leaf_f lf =
      match lf with
         AtomicConstraint(_, t1, t2) ->
            (extract_aggregates_from_term aggressive t1) @
            (extract_aggregates_from_term aggressive t2)
       | _ -> []
   in
   Util.ListAsSet.no_duplicates
      (CalcSemiRing.fold List.flatten List.flatten r_leaf_f relcalc)

and extract_aggregates_from_term (aggressive: bool) (term: term_t) =
   let t_leaf_f x =
      match x with
         AggSum(f, r) ->
            (* if r is constraints_only, take the aggregates from the
               atoms of r; otherwise, return x monolithically. *)
            if ((constraints_only r) && (not aggressive)) then
               ((extract_aggregates_from_term aggressive f) @
                (extract_aggregates_from_calc aggressive r))
            else [TermSemiRing.mk_val x]
       | _ -> []
   in
   Util.ListAsSet.no_duplicates
      (TermSemiRing.fold List.flatten List.flatten t_leaf_f term)


(* Note: the substitution is bottom-up. This means we greedily replace
   smallest subterms, rather than largest ones. This has to be kept in
   mind if terms in aggsum_theta may mutually contain each other.

   FIXME: substitute_in_term currently can only replace AggSum terms,
   not general terms.
*)
let substitute_in_term (aggsum_theta: term_mapping_t)
                       (term: term_t): term_t =
   let aconstraint_f c t1 t2 =
      CalcSemiRing.mk_val(AtomicConstraint(c, t1, t2))
   in
   let aggsum_f f r =
      let this = TermSemiRing.mk_val(AggSum(f, r))
      in
      Util.Function.apply aggsum_theta this this
         (* FIXME: we have to unify the variables of the
            externals to be matched. See also apply_term_mapping. *)
   in
   apply_bottom_up aggsum_f aconstraint_f term

let mk_term_mapping (map_name_prefix: string)
                    (workload: (((var_t list) * term_t) list)):
                    term_mapping_t =
   List.map (fun (n, (vs, t)) -> (t, TermSemiRing.mk_val(External(n, vs))))
            (Util.add_names map_name_prefix workload)

let apply_term_mapping (mapping: term_mapping_t)
                       (map_term: term_t) : term_t =
   let decode_map_term mt =
      match mt with
         TermSemiRing.Val(External(n, vs)) -> (n, vs)
       | _ -> raise (Assert0Exception
                       "Calculus.apply_term_mapping.decode_map_term")
   in
   let (name, vars) = decode_map_term map_term in
   let (vars2, term) = Util.Function.apply_strict
      (List.map (fun (x,y) -> let (n,vs) = decode_map_term y in
                              (n, (vs, x))) mapping) name
      (* if the delta for external function name is not given, this raises
         a NonFunctionalMappingException. *)
   in
   (* TODO: make sure that the variables vars either do not yet occur in the
      term or are equal to vars2. Then substitute. *)
   if (vars=vars2) then term
   else if ((Util.ListAsSet.inter vars (term_vars term)) = []) then
      apply_variable_substitution_to_term (List.combine vars2 vars) term
   else raise (Assert0Exception "Calculus.apply_term_mapping")
      (* TODO: implement the renaming of the overlapping variables before
         the substitution. *)

(* given a list of pairs of terms and their parameters, this function
   extracts all the nested aggregate sum terms, eliminates duplicates,
   and then names the aggregates.

   We do it in this complicated fashion to avoid creating the same
   child terms redundantly.
*)
let extract_named_aggregates (name_prefix: string) (bound_vars: var_t list)
                  (workload: ((var_t list) * term_t) list):
                  (((var_t list) * term_t) list *
                   term_mapping_t) =
   let extract_from_one_term (params, term) =
      let prepend_params t =
         let p = (Util.ListAsSet.inter (term_vars t)
                    (Util.ListAsSet.union params bound_vars))
         in (p, t)
      in
      List.map prepend_params (extract_aggregates_from_term false term)
   in
   (* the central duplicate elimination step. *)
   let extracted_terms = Util.ListAsSet.no_duplicates
                (List.flatten (List.map extract_from_one_term workload))
   in
   (* create mapping *)
   let theta = mk_term_mapping name_prefix extracted_terms
   in
   (* apply substitutions to input terms. *)
   let terms_after_substition =
      List.map (fun (p, t) ->  (p, (substitute_in_term theta t)))
               workload
   in
   (terms_after_substition, theta)





type bs_rewrite_mode_t = ModeExtractFromCond
                       | ModeGroupCond
                       | ModeIntroduceDomain
                       | ModeOpenDomain

let bigsum_rewriting (mode: bs_rewrite_mode_t)
                     (term: term_t)
                     (bound_vars: var_t list)
                     (map_name_prefix: string):
                     ((var_t list) * term_mapping_t * term_t) =
   let leaf_f lf =
      match lf with
         AggSum(t, r) when ((not (constraints_only r)) &&
                           ((snd (split_nested r)) <> relcalc_one)) ->
            (* only do it if not constraints only and there are nested aggs. *)
            let (flat, nested) = split_nested r (* must be a monomial *)
            in
            let bs_vars = Util.ListAsSet.inter (relcalc_vars nested)
               (Util.ListAsSet.diff (safe_vars flat bound_vars) bound_vars)
            in
            let (bigsum_vars, new_term) =
               (match mode with
                  ModeExtractFromCond ->
                     (* just extract nested aggregates from conditions.
                        The extraction happens below, for all modes.
                        We must execute such a term as is: the delta
                        rewriting creates a term that is not simpler.
                        (The same is true for ModeGroupCond and
                         ModeIntroduceDomain.) *)
                     ([], (TermSemiRing.mk_val lf))
                | ModeGroupCond ->
                  (* extract all the conditions with a
                     nested aggregate, "nested", into a single condition
                     AggSum(1, nested) = 1. The advantage of this over
                     ModeExtractFromCond is that there is less work to do
                     at runtime iterating over all the tuples of flat. *)
                  let grouped_conds =
                     TermSemiRing.mk_val(AggSum(term_one, nested))
                  in
                  ([], (TermSemiRing.mk_val (
                     AggSum(t, CalcSemiRing.mk_prod([flat;
                       CalcSemiRing.mk_val(AtomicConstraint(Eq, term_one,
                          grouped_conds))])))))
                  (* TODO: the whole point of this mode is that grouped_conds
                     is made external, but this is not happening yet. *)
                | ModeIntroduceDomain ->
                  (* introduce explicit domain relations. List
                     ModeExtractFromCond but with duplicate elimination of
                     the values we iterate over, so there are fewer iteration
                     steps, but there is the additional cost of maintaining
                     the domain relation. *)
                  ([], (TermSemiRing.mk_val(AggSum(
                       TermSemiRing.mk_val(AggSum(t, flat)),
                       CalcSemiRing.mk_prod([CalcSemiRing.mk_val(Rel(
                          "Dom_{"^(Util.string_of_list ", " bs_vars)^"}",
                          bs_vars)); nested])))))
                          (* TODO: we should also collect the information
                             needed to maintain the domain relation here. *)
                | ModeOpenDomain ->
                  (* like ModeIntroduceDomain, but the domain is implicit.
                     There are free bigsum variables, and the loop to iterate
                     over the relevant valuations of these variables has to
                     be worried about elsewhere. The advantage is that
                     delta applied to this term is guaranteed to be simpler,
                     so we can do recursive delta computation. *)
                  (bs_vars, TermSemiRing.mk_val(AggSum(
                               TermSemiRing.mk_val(AggSum(t, flat)), nested)))
               )
            in
            (* now extract ALL nested aggregates *)
            let agg_plus_vars agg =
               (Util.ListAsSet.inter (term_vars agg) bs_vars, agg)
            in
            let extract_sub_aggregates_from_term t =
               match t with
                  TermSemiRing.Val(AggSum(t, r)) ->
                     (extract_aggregates_from_term false t) @
                     (extract_aggregates_from_calc false r)
                | _ -> raise (Assert0Exception
                  "Calculus.bigsum_rewriting.extract_sub_aggregates_from_term")
            in
            let aggs = (List.map agg_plus_vars
               (extract_sub_aggregates_from_term new_term))
            in
            let theta = mk_term_mapping map_name_prefix aggs
            in
            ([(bigsum_vars, theta)], substitute_in_term theta new_term)
       | _ -> ([([], [])], TermSemiRing.mk_val(lf))
   in
   let (a,  b)  = TermSemiRing.extract List.flatten List.flatten leaf_f term in
   let (a1, a2) = List.split a in
   (List.flatten a1, List.flatten a2, b)





(* Delta computation and simplification *)

let negate_term (do_it: bool) (x: term_t): term_t =  (* auxiliary *)
    if do_it then
       TermSemiRing.mk_prod([TermSemiRing.mk_val(Const(Int(-1))); x])
    else x


(* Note: for ((relcalc|term)_delta true relname tuple expr),
   the resulting deletion delta is already negative, so we *add* it to
   the old value, rather than subtracting it. *)
let rec relcalc_delta (theta: term_mapping_t)
                      (negate: bool) (relname: string)
                      (tuple: var_t list) (relcalc: relcalc_t) =
   let delta_leaf negate lf =
      match lf with
         False -> CalcSemiRing.zero
       | True  -> CalcSemiRing.zero
       | Rel(r, l) when relname = r ->
            let f (x,y) = CalcSemiRing.mk_val(
                             AtomicConstraint(Eq, TermSemiRing.mk_val(Var(x)),
                                                  TermSemiRing.mk_val(Var(y))))
            in
            CalcSemiRing.mk_prod (List.map f (List.combine l tuple))
       | Rel(x, l) -> CalcSemiRing.zero
       | AtomicConstraint(comp, t1, t2) ->
            let tda x = term_delta_aux theta negate relname tuple x
            in
            if(((tda t1) = TermSemiRing.zero) &&
               ((tda t2) = TermSemiRing.zero))
            then
                  CalcSemiRing.zero
            else raise (Assert0Exception "Calculus.relcalc_delta")
                 (* the terms with nonzero delta should have been pulled
                    out of the constraint elsewhere. *)
   in
   CalcSemiRing.delta (delta_leaf negate) relcalc

and term_delta_aux (theta: term_mapping_t)
                   (negate: bool) (relname: string)
                   (tuple: var_t list) (term: term_t)  =
   let rec leaf_delta negate lf =
      match lf with
         Const(_) -> TermSemiRing.zero
       | Var(_)   -> TermSemiRing.zero
       | External(name, vars) -> term_delta_aux theta negate relname tuple 
                           (apply_term_mapping theta (TermSemiRing.mk_val lf))
       | AggSum(f, r) ->
            let (flat, nested) = split_nested r
            in
            let new_nested = (* replace each term t in nested
                                   by (t + delta t) *)
               let leaf_f lf =
                  (match lf with
                     AtomicConstraint(c, t1, t2) ->
                     let t_pm_dt t =
                        TermSemiRing.mk_sum[t; negate_term negate
                           (term_delta_aux theta negate relname tuple t)]
                     in
                     CalcSemiRing.mk_val(
                         AtomicConstraint(c, t_pm_dt t1, t_pm_dt t2))
                   | True -> CalcSemiRing.mk_val(True)
                   | _ -> raise (Assert0Exception
                                   ("Calculus.term_delta_aux: "^
                               (relcalc_as_string (CalcSemiRing.mk_val(lf))))))
               in
               CalcSemiRing.apply_to_leaves leaf_f nested
               (* TODO: on could further optimize this by keeping only those
                  nested atomic constraints in new_nested that really have
                  a map with delta != zero. But that should anyway be mostly
                  the case in practice. *)
            in
            let d_f    = term_delta_aux theta negate relname tuple f in
            let d_flat = relcalc_delta  theta negate relname tuple flat
            in
            (* Delta +:
                 (if (    new+               ) then (delta +  (f, flat)) else 0)
               + (if (    new+  and (not old)) then           f  else 0)
               + (if (not(new+) and      old ) then          -f  else 0)

               Delta -:
                 (if (    new-               ) then (delta -  (f, flat)) else 0)
               + (if (    new-  and (not old)) then          -f  else 0)
               + (if (not(new-) and      old ) then           f  else 0)
            *)
            TermSemiRing.mk_sum [
               mk_aggsum d_f
                         (CalcSemiRing.mk_prod [  flat; new_nested]);
               mk_aggsum (negate_term negate f)
                         (CalcSemiRing.mk_prod [d_flat; new_nested]);
               mk_aggsum d_f
                         (CalcSemiRing.mk_prod [d_flat; new_nested]);
               mk_aggsum (negate_term negate f)
                         (CalcSemiRing.mk_prod [flat; new_nested;
                                                (complement nested)]);
               mk_aggsum (negate_term (not negate) f)
                         (CalcSemiRing.mk_prod [flat; (complement new_nested);
                                                nested])
            ]
   in
   TermSemiRing.delta (leaf_delta negate) term

and term_delta (theta: term_mapping_t)
               (negate: bool) (relname: string)
               (tuple: var_t list) (term: term_t)  =
   negate_term negate (term_delta_aux theta negate relname tuple term)



