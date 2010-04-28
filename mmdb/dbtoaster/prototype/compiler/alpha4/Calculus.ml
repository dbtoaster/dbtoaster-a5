open Util
(* types *)

type type_t = TInt | TLong | TDouble | TString
type var_t   = string * type_t                (* type of variables *)
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
   type t    = TermRing.expr_t generic_relcalc_lf_t
   val  zero : t
   val  one  : t
end =
struct
   type t    = TermRing.expr_t generic_relcalc_lf_t
   let  zero = False
   let  one  = True
end
and CalcRing : Ring.Ring with type leaf_t = CALC_BASE.t
    = Ring.Make(CALC_BASE)
and TERM_BASE :
sig
   type t    = (TermRing.expr_t, CalcRing.expr_t) generic_term_lf_t
   val  zero : t
   val  one  : t
end =
struct
   type t    = (TermRing.expr_t, CalcRing.expr_t) generic_term_lf_t
   let  zero = Const(Int(0))  (* I think I mean this, even if we want to *)
   let  one  = Const(Int(1))  (* support floating point numbers.
                                 The consequence is that some optimizations
                                 will not apply for AggSum(1.0, ...). *)
end
and TermRing : Ring.Ring with type leaf_t = TERM_BASE.t
    = Ring.Make(TERM_BASE)


type relcalc_lf_t = CalcRing.leaf_t
type relcalc_t    = CalcRing.expr_t
type term_lf_t    = TermRing.leaf_t
type term_t       = TermRing.expr_t

type var_mapping_t = (var_t * var_t) list

(* the first is always the full term and the second is the external. *)
type term_mapping_t = (term_t * term_t) list


(* accessing relational algebra expressions and terms *)
type readable_relcalc_lf_t = readable_term_t generic_relcalc_lf_t
and  readable_relcalc_t    = RA_Leaf         of readable_relcalc_lf_t
                           | RA_Neg          of readable_relcalc_t
                           | RA_MultiUnion   of readable_relcalc_t list
                           | RA_MultiNatJoin of readable_relcalc_t list
and  readable_term_lf_t    =
         (readable_term_t, readable_relcalc_t) generic_term_lf_t
and  readable_term_t       = RVal    of readable_term_lf_t
                           | RNeg    of readable_term_t
                           | RProd   of readable_term_t list
                           | RSum    of readable_term_t list



(* functions *)

let relcalc_one  = CalcRing.mk_val(CALC_BASE.one)
let relcalc_zero = CalcRing.mk_val(CALC_BASE.zero)
let term_zero    = TermRing.mk_val (TERM_BASE.zero)
let term_one     = TermRing.mk_val (TERM_BASE.one)


let rec readable_relcalc (relcalc: relcalc_t): readable_relcalc_t =
   let lf_readable (lf: relcalc_lf_t): readable_relcalc_lf_t =
      match lf with
         AtomicConstraint(comp, x, y) ->
            AtomicConstraint(comp, (readable_term x), (readable_term y))
       | Rel(r, sch) -> Rel(r, sch)
       | False       -> False
       | True        -> True
   in
   CalcRing.fold (fun x  -> RA_MultiUnion   x)
                 (fun y  -> RA_MultiNatJoin y)
                 (fun ng -> RA_Neg ng)
                 (fun lf -> RA_Leaf(lf_readable lf))
                 relcalc

and readable_term (term: term_t): readable_term_t =
   let lf_readable lf =
      match lf with
         Const(x)        -> Const(x)
       | Var(x)          -> Var(x)
       | AggSum(f, r)    -> AggSum(readable_term f, readable_relcalc r)
       | External(n, vs) -> External(n, vs)
   in
   TermRing.fold (fun l -> RSum l) (fun l -> RProd l)
                 (fun x -> RNeg x) (fun x -> RVal (lf_readable x)) term



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
      RA_Leaf(x)         -> CalcRing.mk_val  (lf_make x)
    | RA_MultiUnion(l)   -> CalcRing.mk_sum  (List.map make_relcalc l)
    | RA_MultiNatJoin(l) -> CalcRing.mk_prod (List.map make_relcalc l)
    | RA_Neg(x)          -> CalcRing.mk_neg  (make_relcalc x)

and make_term readable_term =
   let lf_make lf =
      match lf with
         Const(x)       -> Const(x)
       | Var(x)         -> Var(x)
       | AggSum(f,r)    -> AggSum(make_term f, make_relcalc r)
       | External(n,vs) -> External(n,vs)
   in
   match readable_term with
      RSum(x)  -> TermRing.mk_sum( List.map make_term x)
    | RProd(x) -> TermRing.mk_prod(List.map make_term x)
    | RVal(x)  -> TermRing.mk_val(lf_make x)
    | RNeg(x)  -> TermRing.mk_neg(make_term x)





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
   CalcRing.fold Util.ListAsSet.multiunion
                 Util.ListAsSet.multiunion
                 (fun x->x) lf_vars relcalc

and term_vars term: var_t list =
   let leaf_f x = match x with
      Var(y) -> [y]
    | AggSum(f, r) -> Util.ListAsSet.union (term_vars f) (relcalc_vars r)
    | External(_,v) -> Util.ListAsSet.no_duplicates v
    | _ -> []
   in
   TermRing.fold Util.ListAsSet.multiunion
                 Util.ListAsSet.multiunion
                 (fun x->x) leaf_f term



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
       | AtomicConstraint(Eq, TermRing.Val(Var c1),
                              TermRing.Val(Var c2)) ->
            (Util.Vars.closure [(c1, c2)] param_vars, [(c1, c2)])
       | AtomicConstraint (_) -> ([], [])
   in
   let (x,y) = CalcRing.fold or_rr and_rr (fun x->x)
                             (fun x -> rr_lf x param_vars) phi 
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
   let mono            = CalcRing.cast_to_monomial r in
   let (rels0, cons0)  = List.split (List.map f mono) in
   let rels            = List.flatten rels0 in
       (* FIXME:  cons = List.flatten cons0 is currently ignored. *)
   let (vars, algrels) = List.split
                (List.map (fun (n, (r, vs)) -> ((n, vs), Alg_Rel(r, n)))
                          (Util.add_names rn_name_prefix rels))
   in
   let g ((n, vs):string * (var_t list)): (var_t * var_t) list = 
      (List.combine 
          (Util.add_names (n^".") (snd (List.split vs))) 
          vs
      )
   in
   let vars2:(var_t*var_t) list = (List.flatten (List.map g vars)) @
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
   if (r = CalcRing.zero) then TermRing.zero
   else if (f = TermRing.zero) then TermRing.zero
   else if (r = CalcRing.one) then f
   else TermRing.mk_val (AggSum (f, r))


let constraints_only (r: relcalc_t): bool =
   let leaves = CalcRing.fold List.flatten List.flatten (fun x->x)
                              (fun x -> [x]) r
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
            CalcRing.mk_val(AtomicConstraint(
               (match comp with Eq -> Neq | Neq -> Eq
                              | Lt -> Le  | Le  -> Lt), t2, t1))
       | True  -> CalcRing.mk_val(False)
       | False -> CalcRing.mk_val(True)
       | _ -> raise (Assert0Exception "Calculus.complement")
   in
   (* switch prod and sum *)
   (CalcRing.fold CalcRing.mk_prod CalcRing.mk_sum
                  (fun _ -> failwith "complement TODO") leaf_f relcalc)


(* split a monomial into flat atoms and nested atoms, and provide the
   list of variables shared between the two monomials created.
   An atom is nested if it is an AtomicConstraint in which at least
   on term contains an AggSum term.
*)
let split_nested (monomial: relcalc_t) 
                 : relcalc_t * relcalc_t =
   let is_nesting_atom lf: bool =
      match lf with
         CalcRing.Val(AtomicConstraint(c, t1, t2)) ->
            let is_nested_term t_lf =
               (match t_lf with
                  External(_,_) -> true
                | AggSum(_,_) -> true
                | _ -> false
               )
            in
            ([] != (List.filter is_nested_term
                     ((TermRing.leaves t1) @ (TermRing.leaves t2))))
       | _ -> false
   in
   let atoms           = List.map CalcRing.mk_val
                                  (CalcRing.cast_to_monomial monomial) in
   let nesting_atoms   = List.filter is_nesting_atom atoms in
   let flat_atoms      = Util.ListAsSet.diff atoms nesting_atoms in
   (CalcRing.mk_prod flat_atoms,
    CalcRing.mk_prod nesting_atoms)



let rec apply_variable_substitution_to_relcalc (theta: var_mapping_t)
                                               (alg: relcalc_t): relcalc_t =
   let substitute_leaf lf =
      match lf with
         Rel(n, vars) ->
            CalcRing.mk_val
               (Rel(n, List.map (Util.Vars.apply_mapping theta) vars))
       | AtomicConstraint(comp, x, y) ->
            (CalcRing.mk_val (AtomicConstraint(comp,
               (apply_variable_substitution_to_term theta x),
               (apply_variable_substitution_to_term theta y) )))
       | _ -> (CalcRing.mk_val lf)
   in
   (CalcRing.apply_to_leaves substitute_leaf alg)

and apply_variable_substitution_to_term (theta: var_mapping_t)
                                        (m: term_t): term_t =
   let leaf_f lf =
      match lf with
         Var(y) -> TermRing.mk_val(Var(Util.Vars.apply_mapping theta y))
       | AggSum(f, r) ->
            TermRing.mk_val(
               AggSum(apply_variable_substitution_to_term theta f,
                              apply_variable_substitution_to_relcalc theta r))
       | External(n, vs) ->
            TermRing.mk_val(External(n, List.map
               (Util.Vars.apply_mapping theta) vs))
       | _ -> TermRing.mk_val(lf)
   in 
   (TermRing.apply_to_leaves leaf_f m)


type relcalc_mono_t = int * (relcalc_lf_t list)

let monomials (q: relcalc_t): (relcalc_t list) =
   List.map CalcRing.monomial_to_expr (CalcRing.polynomial q)

let polynomial (q: relcalc_t): relcalc_t = CalcRing.mk_sum (monomials q)



(* pseudocode output of relcalc expressions and terms. *)
let rec relcalc_as_string (relcalc: relcalc_t): string =
   let sum_f  l = "(" ^ (Util.string_of_list " or " l) ^ ")" in
   let prod_f l = (Util.string_of_list " and " l) in
   let neg_f x = "-("^x^")" in
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
       | Rel(r, sch) -> 
            r^"("^(Util.string_of_list ", " (fst (List.split sch)))^")"
   in
   CalcRing.fold sum_f prod_f neg_f leaf_f relcalc

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
    | Var(x)             -> (fst x)
    | External(n,params) -> 
          n^"["^(Util.string_of_list ", " (fst (List.split params)))^"]"
    | AggSum(f,r)        ->
      if (constraints_only r) then
         "(if " ^ (relcalc_as_string r) ^ " then " ^
                  (term_as_string    f) ^ " else 0)"
      else
         "AggSum("^(term_as_string f)^", " ^(relcalc_as_string r)^")"
   )
   in (TermRing.fold (fun l -> "("^(Util.string_of_list "+" l)^")")
                     (fun l -> "("^(Util.string_of_list "*" l)^")")
                     (fun x -> "-("^x^")")
                     leaf_f m)
;;




(* given a relcalc monomial, returns a pair (eqs, rest), where
   eqs is the list of equalities occurring in the input
   and rest is the input monomial minus the equalities.
   Auxiliary function used in extract_substitutions. *)
let split_off_equalities (monomial: relcalc_t) :
                         (((var_t * var_t) list) * relcalc_t) =
   let leaf_f lf =
      match lf with
         AtomicConstraint(Eq, TermRing.Val(Var(x)),
            TermRing.Val(Var(y))) -> ([(x, y)], CalcRing.one)
         (* TODO: this can be generalized.
            We could replace variables by non-variable terms in some
            places. Question: do we want to do this in Rel-atoms too? *)
       | _ -> ([], CalcRing.mk_val(lf))
   in
   CalcRing.extract
      (fun x -> raise (Assert0Exception "Calculus.split_off_equalities"))
      List.flatten (fun _ -> failwith "Calc.split_off_equalities TODO")
      leaf_f monomial


let extract_substitutions (monomial: relcalc_t)
                          (bound_vars: var_t list) :
                          (var_mapping_t * relcalc_t) =
   let (eqs, rest) = split_off_equalities monomial
   in
 (*print_string ("substitutions for: "^(relcalc_as_string monomial)^"\n")*)
 (*print_string ("eqs: "^(Util.Function.string_of_table_fn eqs fst fst)^"\n")*)
   (* an equation will be in eqs_to_keep if it tries to set two bound vars
      equal, where we are not allowed to replace either. We have to keep
      these equalities. *)
   let (theta, eqs_to_keep) = Util.Vars.unifier eqs bound_vars
   in
 (*print_string ("t: "^(Util.Function.string_of_table_fn theta fst fst)^"\n")*)
   let f (x,y) = CalcRing.mk_val (AtomicConstraint(Eq,
                    TermRing.mk_val(Var(x)), TermRing.mk_val(Var(y))))
   in
   (* add the inconsistent equations again as constraints. *)
   let rest2 = CalcRing.mk_prod(
      [(apply_variable_substitution_to_relcalc theta rest)]
      @ (List.map f eqs_to_keep))
   in
   (theta, rest2)






(* factorize an AggSum(f, r) where f and r are monomials *)
let factorize_aggsum_mm (f_monomial: term_t)
                        (r_monomial: relcalc_t) : term_t =
   if (r_monomial = relcalc_zero) then TermRing.zero
   else
      let factors = Util.MixedHyperGraph.connected_components
                       term_vars relcalc_vars
                       (Util.MixedHyperGraph.make
                           (TermRing.prod_list f_monomial)
                           (CalcRing.prod_list r_monomial))
      in
      let mk_aggsum2 component =
         let (f, r) = Util.MixedHyperGraph.extract_atoms component in
         (mk_aggsum (TermRing.mk_prod f) (CalcRing.mk_prod r))
      in
      TermRing.mk_prod (List.map mk_aggsum2 factors)




let rec apply_bottom_up (aggsum_f: term_t -> relcalc_t -> term_t)
                        (aconstraint_f: comp_t -> term_t -> term_t -> relcalc_t)
                        (term: term_t) : term_t =
   let r_leaf_f (lf: relcalc_lf_t): relcalc_t =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            aconstraint_f c (apply_bottom_up aggsum_f aconstraint_f t1)
                            (apply_bottom_up aggsum_f aconstraint_f t2)
       | _ -> CalcRing.mk_val(lf)
   in
   let t_leaf_f (lf: term_lf_t): term_t =
      match lf with
         AggSum(f, r) -> aggsum_f (apply_bottom_up aggsum_f aconstraint_f f)
                                  (CalcRing.apply_to_leaves r_leaf_f r)
       |  _ -> TermRing.mk_val(lf)
   in
   TermRing.apply_to_leaves t_leaf_f term


(* polynomials, recursively: in the end, +/union only occurs on the topmost
   level.
*)
let roly_poly (term: term_t) : term_t =
   let aconstraint_f c t1 t2 =
      CalcRing.mk_val(AtomicConstraint(c,
         (TermRing.polynomial_expr t1), (TermRing.polynomial_expr t2)))
   in
   let aggsum_f f r =
      (* recursively normalize contents of complex terms in
         atomic constraints. *)
      let r_monomials = CalcRing.polynomial r in
      let f_monomials = TermRing.sum_list (TermRing.polynomial_expr f)
      in
      (* move negations from relcalc to term *)
      let aux f2 ((rmult:int), rexpr) =
         let f3 = TermRing.mk_prod [f2; TermRing.mk_val(Const (Int rmult))]
         in
         factorize_aggsum_mm f3 (CalcRing.monomial_to_expr (1, rexpr))
      in
      (* distribute the sums in r_monomials and f_monomials and factorize. *)
      TermRing.mk_sum (List.flatten (List.map
         (fun y -> (List.map (fun x -> aux x y) f_monomials)) r_monomials))
   in
   TermRing.polynomial_expr (apply_bottom_up aggsum_f aconstraint_f term)



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
                               (bigsum_vars: var_t list)
                               : (var_mapping_t * relcalc_t) =
   let leaf_f lf =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            let t1b = if recurse 
                    then (snd (simplify_roly true t1 bound_vars bigsum_vars))
                    else t1 in
            let t2b = if recurse
                    then (snd (simplify_roly true t2 bound_vars bigsum_vars))
                    else t2 in
            CalcRing.mk_val(AtomicConstraint(c, t1b, t2b))
       | _ -> CalcRing.mk_val lf
   in
   extract_substitutions (CalcRing.apply_to_leaves leaf_f relcalc)
                         bound_vars

and simplify_roly (recurse: bool) (term: term_t) (bound_vars: var_t list)
                  (bigsum_vars: var_t list) : (var_mapping_t * term_t) =
   let leaf_f lf =
      match lf with
         AggSum(f, r) ->
            (* we test equality, not equivalence, to zero here. Sufficient
               if we first normalize using roly_poly. *)
            if (r = relcalc_zero) then
               raise (Assert0Exception "Calculus.simplify_roly.t_leaf_f")
            else if (f = TermRing.zero) then ([], TermRing.zero)
            else
               let ((b:(var_t * var_t) list), non_eq_cons) =
                  simplify_calc_monomial 
                    recurse r 
                    bound_vars
                    bigsum_vars
               in
               let f1 = apply_variable_substitution_to_term b f
               in
               (* loop variable bindings are passed from relcalc to term. *)
               (* bigsum variable bindings are passed from term to relcalc. *)
               let (f_b, f2) = simplify_roly true f1
                  (Util.ListAsSet.multiunion 
                    [bound_vars; 
                    (ListAsSet.diff (relcalc_vars r) bigsum_vars);
                    (ListAsSet.diff (Util.Function.img b) bigsum_vars)])
                  bigsum_vars
               in
               let non_eq_cons_subbed = 
                  apply_variable_substitution_to_relcalc f_b non_eq_cons
               in
               if (non_eq_cons_subbed = relcalc_one) then (b, f2)
               else (b, TermRing.mk_val(AggSum(f2, non_eq_cons_subbed)))
                 (* we represent the if-condition as a calculus
                    expression to use less syntax *)
       | _            -> ([], TermRing.mk_val(lf))
   in
   (* Note: in general, this can lead to an inconsistent substitution,
      which only hurts if a param variable occurs in more than
      one AggSum term. *)
   TermRing.extract List.flatten List.flatten
                    (fun _ -> failwith "simplify_roly TODO") leaf_f term


(* apply roly_poly and simplify by unifying variables.
   returns a list of pairs (dimensions', monomial)
   where monomial is the simplified version of a nested monomial of term
   and dimensions' is dimensions -- a set of variables occurring in term --
   after application of the substitution used to simplify monomial. 
   
   This is also where "if-lifting" happens, in a manner of speaking.  The call
   to roly_poly should leave us with a fully factorized expression that is the
   product of a list of aggregates.  Even if we can't factorize something up
   (does this ever happen?) we don't care about it, because it's either 
    - complex (eg, inequalities), in which case simplify can't do anything
      about it to begin with.
    - simple, in which case it either would have gotten factorized already, or 
      it should have been replaced by a common subexpression at this point.
   
   Effectively, the only thing we need to do after that is to unify variables
   and do some basic cleanup.  Unification is done in simplify_roly, by way of
   simplify_calc_monomial/extract_substitutions.  
   
   We guarantee that no unification happens on nested aggregates by "binding" 
   all variables we expect to see in the expression when doing recursive 
   processing, and only "bind" the bigsum and relation variables when we 
   simplify only the top-level terms.
   
   Also note: The top-level terms here, refers to the top level in the 
   comparison dimension.  Consider the following expression:
     AggSum(AggSum(b,c), a < 2) 
   The term 'AggSum(b,c)' is still considered top-level, but the term 'a' is 
   not.
   
*)
let simplify (term: term_t)
             (rel_vars: var_t list)
             (bsum_vars: var_t list)
             (loop_vars: var_t list) :
             (((var_t list * term_t) * var_t list) list) =
   let simpl f =
      (* we want to unify params if possible to eliminate for loops, but
         we do not want to use substitutions from aggregates nested in
         atomic constraints. The following strategy of calling simplify_roly
         twice with suitable arguments achieves that. *)
      let (_, t1) = simplify_roly true  f (rel_vars @ bsum_vars @ loop_vars) 
                                  bsum_vars in
        (* bsum_vars should technically not be bound from the outside; if we
           can eliminate a bigsum var, we should do so for the same reasons we 
           eliminate loop_vars.  However, simplify_roly handles each term in
           the monomial separately.  This means we need to do some extra work,
           making this a TODO *)
      let (b, t2) = simplify_roly false t1 rel_vars bsum_vars
      in
      ( ( (List.map (Util.Vars.apply_mapping b) loop_vars),
           t2
        ),
        (List.map (Util.Vars.apply_mapping b) bsum_vars)
      )
   in
   List.filter (fun ((_, t), _) -> t <> TermRing.zero)
      (List.map simpl (TermRing.sum_list (roly_poly term)))


let rec extract_aggregates_from_calc (aggressive: bool) (relcalc: relcalc_t) =
   let r_leaf_f lf =
      match lf with
         AtomicConstraint(_, t1, t2) ->
            (extract_aggregates_from_term aggressive t1) @
            (extract_aggregates_from_term aggressive t2)
       | _ -> []
   in
   Util.ListAsSet.no_duplicates
      (CalcRing.fold List.flatten List.flatten (fun x->x) r_leaf_f relcalc)

and extract_aggregates_from_term (aggressive: bool) (term: term_t) =
   let t_leaf_f x =
      match x with
         AggSum(f, r) ->
            (* if r is constraints_only, take the aggregates from the
               atoms of r; otherwise, return x monolithically. *)
            if ((constraints_only r) && (not aggressive)) then
               ((extract_aggregates_from_term aggressive f) @
                (extract_aggregates_from_calc aggressive r))
            else [TermRing.mk_val x]
       | _ -> []
   in
   Util.ListAsSet.no_duplicates
      (TermRing.fold List.flatten List.flatten (fun x->x) t_leaf_f term)


(* Note: the substitution is bottom-up. This means we greedily replace
   smallest subterms, rather than largest ones. This has to be kept in
   mind if terms in aggsum_theta may mutually contain each other.

   FIXME: substitute_in_term currently can only replace AggSum terms,
   not general terms.
*)
let substitute_in_term (aggsum_theta: term_mapping_t)
                       (term: term_t): term_t =
   let aconstraint_f c t1 t2 =
      CalcRing.mk_val(AtomicConstraint(c, t1, t2))
   in
   let aggsum_f f r =
      let this = TermRing.mk_val(AggSum(f, r))
      in
      Util.Function.apply aggsum_theta this this
         (* FIXME: we have to unify the variables of the
            externals to be matched. See also apply_term_mapping. *)
   in
   apply_bottom_up aggsum_f aconstraint_f term

let mk_term_mapping (map_name_prefix: string)
                    (workload: (((var_t list) * term_t) list)):
                    term_mapping_t =
   List.map (fun (n, (vs, t)) -> (t, TermRing.mk_val(External(n, vs))))
            (Util.add_names map_name_prefix workload)

let decode_map_term (map_term: term_t):
                    (string * (var_t list)) =
   match (readable_term map_term) with
      RVal(External(n, vs)) -> (n, vs)
    | _ -> failwith "Calculus.decode_map_term";;

let map_term (map_name:string) (map_vars:var_t list): term_t =
  make_term (RVal(External(map_name, map_vars)))

let apply_term_mapping (mapping: term_mapping_t)
                       (map_term: term_t) : term_t =
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
                    (Util.ListAsSet.union params bound_vars)
                 )
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
                     ([], (TermRing.mk_val lf))
                | ModeGroupCond ->
                  (* extract all the conditions with a
                     nested aggregate, "nested", into a single condition
                     AggSum(1, nested) = 1. The advantage of this over
                     ModeExtractFromCond is that there is less work to do
                     at runtime iterating over all the tuples of flat. *)
                  let grouped_conds =
                     TermRing.mk_val(AggSum(term_one, nested))
                  in
                  ([], (TermRing.mk_val (
                     AggSum(t, CalcRing.mk_prod([flat;
                       CalcRing.mk_val(AtomicConstraint(Eq, term_one,
                          grouped_conds))])))))
                  (* TODO: the whole point of this mode is that grouped_conds
                     is made external, but this is not happening yet. *)
                | ModeIntroduceDomain ->
                  (* introduce explicit domain relations. List
                     ModeExtractFromCond but with duplicate elimination of
                     the values we iterate over, so there are fewer iteration
                     steps, but there is the additional cost of maintaining
                     the domain relation. *)
                  ([], (TermRing.mk_val(AggSum(
                       TermRing.mk_val(AggSum(t, flat)),
                       CalcRing.mk_prod([CalcRing.mk_val(Rel(
                          "Dom_{"^(Util.string_of_list ", " 
                            (fst (List.split bs_vars))
                          )^"}",
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
                  (bs_vars, TermRing.mk_val(AggSum(
                               TermRing.mk_val(AggSum(t, flat)), nested)))
               )
            in
            (* now extract ALL nested aggregates *)
            let agg_plus_vars agg =
               (Util.ListAsSet.inter (term_vars agg) bs_vars, agg)
            in
            let extract_sub_aggregates_from_term t =
               match t with
                  TermRing.Val(AggSum(t, r)) ->
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
       | _ -> ([([], [])], TermRing.mk_val(lf))
   in
   let (a,  b)  = TermRing.extract List.flatten List.flatten (fun x->x)
                                   leaf_f term in
   let (a1, a2) = List.split a in
   (List.flatten a1, List.flatten a2, b)





(* Delta computation and simplification *)


let rec relcalc_delta (theta: term_mapping_t) (delete: bool)
                      (relname: string)
                      (tuple: var_t list) (relcalc: relcalc_t) =
   let delta_leaf delete lf =
      match lf with
         False -> CalcRing.zero
       | True  -> CalcRing.zero
       | Rel(r, l) when relname = r ->
            let f (x,y) = 
               CalcRing.mk_val(
                       AtomicConstraint(Eq, TermRing.mk_val(Var(x)),
                                            TermRing.mk_val(Var(y))))
            in
            let z = CalcRing.mk_prod (List.map f (List.combine l tuple)) in
             if delete then CalcRing.mk_neg z else z
       | Rel(x, l) -> CalcRing.zero
       | AtomicConstraint(comp, t1, t2) ->
            let td1 = term_delta theta delete relname tuple t1 in
            let td2 = term_delta theta delete relname tuple t2 in
            if((td1 = TermRing.zero) && (td2 = TermRing.zero))
            then
                  CalcRing.zero
            else
               let new1 = TermRing.mk_sum [t1; td1] in
               let new2 = TermRing.mk_sum [t2; td2] in
               let newcons = CalcRing.mk_val(
                                 AtomicConstraint(comp, new1, new2)) in
               let oldcons = CalcRing.mk_val lf
            in
            CalcRing.mk_sum [CalcRing.mk_prod [newcons; (complement oldcons)];
             CalcRing.mk_neg(CalcRing.mk_prod [oldcons; (complement newcons)])]
   in
   CalcRing.delta (delta_leaf delete) relcalc

and term_delta (theta: term_mapping_t) (delete: bool)
               (relname: string)
               (tuple: var_t list) (term: term_t)  =
   let rec leaf_delta delete lf =
      match lf with
         Const(_) -> TermRing.zero
       | Var(_)   -> TermRing.zero
       | External(name, vars) ->
            term_delta theta delete relname tuple 
               (apply_term_mapping theta (TermRing.mk_val lf))
       | AggSum(f, r) ->
            let d_f = term_delta    theta delete relname tuple f in
            let d_r = relcalc_delta theta delete relname tuple r
            in
            TermRing.mk_sum [ mk_aggsum d_f   r;
                              mk_aggsum   f d_r;
                              mk_aggsum d_f d_r ]
   in
   TermRing.delta (leaf_delta delete) term


exception TermsNotEquivalent of string

(* A variable type to keep track of variable name mappings.  Semi-bi-directional 
   mappings are tracked; If (A => B) exists in (fst equiv_state) then (B) exists
   in (snd equiv_state).  That is, (fst equiv_state) keeps track of name 
   equivalencies between the two maps, while (snd equiv_state) is used to ensure
   that we don't set a LHS variable equivalent to a RHS variable we've already
   seen
*)
type equiv_state_t = (string StringMap.t * StringSet.t) option

let equate_terms (term_a:term_t) (term_b:term_t): (string StringMap.t) =
  let map_var (equiv_state:equiv_state_t)
              ((a_name, a_type):var_t)
              ((b_name, b_type):var_t): equiv_state_t =
    match equiv_state with
    | None -> None
    | Some(l_r_map,r_l_set) ->
      if a_type <> b_type then None else
      if StringMap.mem a_name l_r_map then
        (* If there's a mapping, it'd better be the case that 
           (A => B) exists in l_r_map
           (B) exists in r_l_set
           The latter is really just a sanity check.
        *)
        if (StringMap.find a_name l_r_map) <> b_name then None
        else if not (StringSet.mem b_name r_l_set) then 
          failwith "Backwards mapping failed in Calculus.equate_terms:mem"
        else Some(l_r_map,r_l_set)
      else if StringSet.mem b_name r_l_set then None
      else Some(StringMap.add a_name b_name l_r_map,
                StringSet.add b_name r_l_set)
  in
  let rec cmp_term_lf (a:TermRing.leaf_t) 
                      (b:TermRing.leaf_t)
                      (var_map:equiv_state_t): 
                      equiv_state_t =
    match a with
    | AggSum(at,aphi)    -> 
      ( match b with 
        | AggSum(bt,bphi) -> (cmp_calc aphi bphi (cmp_term at bt var_map))
        | _               -> None
      )
    | Const(ac)          -> 
      ( if b = Const(ac) then var_map else None )
    | Var(av)            -> 
      ( match b with Var(bv) -> map_var var_map av bv | _ -> None )
    | External(an,avars) -> 
      ( None (* We don't support post-compiled comparisons (yet) *) )
  and cmp_calc_lf (a:CalcRing.leaf_t) 
                  (b:CalcRing.leaf_t)
                  (var_map:equiv_state_t): 
                  equiv_state_t =
    match a with
    | False -> if b = False then var_map else None
    | True  -> if b = True then var_map else None
    | AtomicConstraint(c,al,ar) -> 
      ( match b with 
        (* for now force ac == bc; TODO: comparator-specific equivalencies *)
        | AtomicConstraint(c,bl,br) -> 
          (cmp_term ar br (cmp_term al bl var_map))
        | _ -> None
      )
    | Rel(rel_name, a_vars) -> 
      ( match b with
        | Rel(rel_name, b_vars) -> 
            if (List.length a_vars) <> (List.length b_vars) then None
            else List.fold_left2 map_var var_map a_vars b_vars
        | _ -> None
      )
  and cmp_term a b var_map = TermRing.cmp_exprs cmp_term_lf a b var_map
  and cmp_calc a b var_map = CalcRing.cmp_exprs cmp_calc_lf a b var_map
  in
    match (cmp_term term_a term_b 
                         (Some(StringMap.empty,StringSet.empty))
          ) with
    | Some(a,_) -> a
    | None -> raise (TermsNotEquivalent("foo"))
;;
