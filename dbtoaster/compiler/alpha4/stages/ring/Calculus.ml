open Util
(* types *)

type type_t = TInt | TLong | TDouble | TString
type var_t   = string * type_t                (* type of variables *)
type comp_t  = Eq | Lt | Le | Neq             (* comparison operations *)
type const_t = Int    of int                  (* typed constant terms *)
             | Double of float
             | Long   of int64
             | String of string
             | Boolean of bool


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

;;

let rec relcalc_relations (calc:relcalc_t): string list =
   CalcRing.fold ListAsSet.multiunion ListAsSet.multiunion (fun x->x) 
                  (  fun x -> match x with 
                        | Rel(s,_) -> [s]
                        | AtomicConstraint(_,t1,t2) -> 
                           ListAsSet.union (term_relations t1)
                                           (term_relations t2)
                        | False -> [] | True -> []
                  )
                  calc

and term_relations (term:term_t): string list =
   TermRing.fold ListAsSet.multiunion ListAsSet.multiunion (fun x->x) 
                  (  fun x -> match x with
                        | AggSum(t,c) -> 
                           ListAsSet.union (term_relations t)
                                           (relcalc_relations c)
                        | External(_,_) -> []
                        | Const(_) -> [] | Var(_) -> []
                  )
                  term

;;

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

let rec bound_vars_of_term (t: term_t) : var_t list =
   TermRing.fold ListAsSet.multiinter ListAsSet.multiunion (fun x->x)
      (fun lf ->
         match lf with
            | Const(_)       -> []
            | Var(v)         -> [v]
            | External(_,ev) -> ev
            | AggSum(t,r)    -> 
               ListAsSet.union (bound_vars_of_term t) (bound_vars_of_relcalc r)
      ) t

and     bound_vars_of_relcalc (r: relcalc_t) : var_t list =
   CalcRing.fold ListAsSet.multiinter ListAsSet.multiunion (fun x->x)
      (fun lf ->
         match lf with
            | False -> [] | True -> []
             (* Constraints can propagate bindings, but this method returns
                only those variables that are explicitly bound *)
            | AtomicConstraint(_,_,_) -> []
            | Rel(_,rv) -> rv
      ) r

exception Assert0Exception of string (* should never be reached *)


type relalg_t =
   Alg_MultiProd of (relalg_t list)
 | Alg_Selection of (((comp_t * var_t * var_t) list) * relalg_t)
 | Alg_Rel of string * string
 | Alg_True



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
   let is_nested_term t_lf = match t_lf with
       External(_,_) | AggSum(_,_) -> true
     | _ -> false
   in
   let is_nesting_atom lf: bool =
     match lf with
       CalcRing.Val(AtomicConstraint(c, t1, t2)) ->
       ([] != (List.filter is_nested_term
                ((TermRing.leaves t1) @ (TermRing.leaves t2))))
     | _ -> false
   in
   let atoms           = List.map CalcRing.mk_val
                                  (CalcRing.cast_to_monomial monomial) in
   let nesting_atoms   = List.filter is_nesting_atom atoms in
   let flat_atoms      = Util.ListAsSet.diff atoms nesting_atoms in
   (CalcRing.mk_prod flat_atoms, CalcRing.mk_prod nesting_atoms)



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

let string_of_const c = 
   match c with
     Int(i)    -> string_of_int i
   | Double(d) -> string_of_float d
   | Long(l)   -> "(int64 output not implemented)" (* TODO *)
   | String(s) -> "'" ^ s ^ "'"
   | Boolean(true)  -> "true"
   | Boolean(false) -> "false"

let string_of_var = fst
;;
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
      Const(c)           -> string_of_const c
    | Var(x)             -> (string_of_var x)
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

let string_of_relcalc = relcalc_as_string
let string_of_term    = term_as_string

let code_of_var (vn,vt) = 
   "(\""^vn^"\", "^(match vt with 
      TInt -> "TInt" | TDouble -> "TDouble" | 
      TLong -> "TLong" | TString -> "TString"
   )^")"

let rec code_of_relcalc (relcalc:relcalc_t): string =
   CalcRing.fold
      (fun l    -> "(RA_MultiUnion["^(Util.string_of_list "; " l)^"])")
      (fun l    -> "(RA_MultiNatJoin["^(Util.string_of_list "; " l)^"])")
      (fun c    -> "(RA_Neg"^c^")")
      (fun leaf -> "(RA_Leaf("^
         (match leaf with
            | AtomicConstraint(c, l, r) -> "AtomicConstraint("^
               (match c with
                  Eq -> "Eq" | Lt -> "Lt" | Le -> "Le" | Neq -> "Neq"
               )^", "^(code_of_term l)^", "^(code_of_term r)^")"
            | False -> "False"
            | True -> "True"
            | Rel(rn, rv) -> 
               "Rel(\""^rn^"\", ["^
               (Util.string_of_list0 "; " code_of_var rv)^
               "])"
         )^"))"
      )
      relcalc

and code_of_term (m:term_t): string =
   TermRing.fold 
      (fun l      -> "(RSum["^(Util.string_of_list "; " l)^"])")
      (fun l      -> "(RProd["^(Util.string_of_list "; " l)^"])")
      (fun c      -> "(RNeg"^c^")")
      (fun leaf   -> "(RVal("^
         (match leaf with
            | AggSum(subt, subc) ->
               "AggSum("^(code_of_term subt)^", "^(code_of_relcalc subc)^")"
            | Const(Int(i))    -> "Const(Int("^(string_of_int i)^"))"
            | Const(Double(i)) -> "Const(Double("^(string_of_float i)^"))"
            | Const(Long(i))   -> "Const(Long("^(Int64.to_string i)^"))"
            | Const(String(i)) -> "Const(String(\""^i^"\"))"
            | Const(Boolean(true))  -> "Const(Boolean(true))"
            | Const(Boolean(false)) -> "Const(Boolean(false))"
            | Var(vn,vt) -> "Var("^(code_of_var (vn,vt))^")"
            | External(rn, rv) ->
               "External(\""^rn^"\", ["^
               (Util.string_of_list0 "; " code_of_var rv)^
               "])"
         )^"))"
      )
      m
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


(* returns a set of substitutions, as applied to the given monomial.
 * the substitutions are extracted from equalities in the input monomial,
 * except for those equalities comparing two bound vars. Bound vars are not
 * replaced in the resulting monomial, only free vars are replaced.
 *)
let extract_substitutions (monomial: relcalc_t)
                          (bound_vars: var_t list) :
                          (var_mapping_t * relcalc_t) =
   let (eqs, rest) = split_off_equalities monomial in
   
   Debug.print "EXTRACT-SUBS" (fun () ->
     ("substitutions for: "^(relcalc_as_string monomial)^"\n")^
     ("eqs: "^(Util.Function.string_of_table_fn eqs fst fst)^"\n"));
   
   (* an equation will be in eqs_to_keep if it tries to set two bound vars
      equal, where we are not allowed to replace either. We have to keep
      these equalities. *)
   let (theta, eqs_to_keep) = Util.Vars.unifier eqs bound_vars in
   
   Debug.print "EXTRACT-SUBS" (fun () ->
     ("theta: "^(Util.Function.string_of_table_fn theta fst fst)^"\n"));

   Debug.print "EXTRACT-SUBS" (fun () ->
     ("eqs_to_keep: "^(Util.Function.string_of_table_fn eqs_to_keep fst fst)^"\n"));
   
   let f (x,y) = CalcRing.mk_val (AtomicConstraint(Eq,
                    TermRing.mk_val(Var(x)), TermRing.mk_val(Var(y))))
   in
   (* add the inconsistent equations again as constraints. *)
   let rest2 = CalcRing.mk_prod(
      [(apply_variable_substitution_to_relcalc theta rest)]
      @ (List.map f eqs_to_keep))
   in (theta, rest2)


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
         let f3 = factorize_aggsum_mm f2 (CalcRing.monomial_to_expr (1, rexpr))
         in
         TermRing.mk_prod [f3; TermRing.mk_val(Const (Int rmult))]         
      in
      (* distribute the sums in r_monomials and f_monomials and factorize. *)
      TermRing.mk_sum (List.flatten (List.map
         (fun y -> (List.map (fun x -> aux x y) f_monomials)) r_monomials))
   in
   TermRing.polynomial_expr (apply_bottom_up aggsum_f aconstraint_f term)

let roly_poly_plural term = TermRing.sum_list (roly_poly term)

(* the input formula must be or-free.  Call roly_poly first to ensure this.

   We simplify conditions and as a consequence formulae and terms
   by extracting equalities and using them to substitute variables.
   The variable substitutions have to be propagated and kept consistent
   across the expression tree (which is not fully done right now).
   The most important propagation of substitutions is, in AggSum(f, r)
   expressions, from r into f (but not the other way round), and obviously
   upwards.
*)
let rec simplify_calc_monomial (recur: bool)
                               (relcalc: relcalc_t) (bound_vars: var_t list)
                               (bigsum_vars: var_t list)
                               : (var_mapping_t * relcalc_t) =
   let local_bindings = fst
      (CalcRing.extract 
         (fun _ -> failwith "Union in monomial")
         (List.flatten) (fun x -> x)
         (fun lf -> ([lf, safe_vars (CalcRing.mk_val lf) bound_vars], 
                     CalcRing.mk_val lf))
         relcalc
      )
   in
   let nonlocal_bindings lf =
      ListAsSet.multiunion
         (List.map snd 
            (List.remove_assoc lf local_bindings)
         )
   in
   let leaf_f lf =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            let aux t =
              if not recur then t else
              (* nested bindings are dropped here, thus cannot be used to 
                 eliminate upper-level loop variables. *)
              snd (simplify_roly_old 
                     true 
                     t
                     (ListAsSet.union
                        bound_vars 
                        (ListAsSet.diff (nonlocal_bindings lf) bigsum_vars))
                     bigsum_vars) in 
            let t1b, t2b = aux t1, aux t2 in
            CalcRing.mk_val(AtomicConstraint(c, t1b, t2b))
       | _ -> CalcRing.mk_val lf
   in
   extract_substitutions (CalcRing.apply_to_leaves leaf_f relcalc) bound_vars

and simplify_roly_old (recur: bool) (term: term_t) (bound_vars: var_t list)
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
                  simplify_calc_monomial recur r bound_vars bigsum_vars
               in
               let f1 = apply_variable_substitution_to_term b f
               in
               (* loop variable bindings are passed from relcalc to term. *)
               (* bigsum variable bindings are passed from term to relcalc. *)
               let (f_b, f2) = simplify_roly_old true f1
                  (Util.ListAsSet.multiunion 
                    [bound_vars; 
                    (ListAsSet.diff (relcalc_vars r) bigsum_vars);
                    (ListAsSet.diff (Util.Function.img b) bigsum_vars)])
                  bigsum_vars
               in

               Debug.print "LOG-SIMPLIFY-ROLY" (fun () ->
                 "BS VAR subst: "^(String.concat ","
                    (List.map (fun (x,y) -> (fst x)^"->"^(fst y)) f_b)));

               let (_, non_eq_cons_subbed) = 
                 simplify_calc_monomial recur
                   (apply_variable_substitution_to_relcalc f_b non_eq_cons)
                   bound_vars bigsum_vars
               in
               (* Pass propagated var bindings up, where propagated vars are
                * bigsums or group-bys appearing in nested expressions.
                * This assumes bigsums contain both true bigsums, and 
                * propagated group-bys (see notes in bigsum_rewriting) *)
               let b_and_bprop =
                 let bprop =
                   List.filter (fun (x,y) -> (List.mem x bigsum_vars)) f_b
                 in List.fold_left (fun acc (x,y) ->
                        if List.mem_assoc x acc then acc else (x,y)::acc)
                      b bprop
               in
               if (non_eq_cons_subbed = relcalc_one) then
                 (b_and_bprop, f2)
               else
                 begin
                 Debug.print "LOG-SIMPLIFY-ROLY" (fun() ->
                    "Simplified: "^(term_as_string (TermRing.mk_val lf))^
                    "\n    =>\n"^(term_as_string
                      (TermRing.mk_val(AggSum(f2, non_eq_cons_subbed)))));
                 (b_and_bprop,
                      TermRing.mk_val(AggSum(f2, non_eq_cons_subbed)))
                    (* we represent the if-condition as a calculus
                       expression to use less syntax *)
                 end
       
    | _            -> ([], TermRing.mk_val(lf))
   in
   (* Note: in general, this can lead to an inconsistent substitution,
      which only hurts if a param variable occurs in more than
      one AggSum term. *)
   TermRing.extract List.flatten List.flatten
                    (fun _ -> failwith "simplify_roly TODO") leaf_f term
;;
let rec simplify_roly  (term: term_t)
                       (input_subs: var_mapping_t)
                       (input_vars: var_t list)
                       (param_vars: var_t list): 
                       (var_mapping_t * term_t) =
   Debug.print "LOG-SIMPLIFY-ROLY" 
               (fun () -> "Simplify: "^(string_of_term term)^" <- ["^
                          (string_of_list0 "; " (fun ((a,_),(b,_)) -> a^"->"^b)
                                           input_subs)^"]");
   let rcr t subs iv = simplify_roly t subs iv param_vars in
   let unbound_vars = ListAsSet.diff
      (ListAsSet.diff (term_vars term) (bound_vars_of_term term)) input_vars in
   (* 
      Sums occur in a roly at the root level and at the root of each term 
      nested in an inequality.  Variable bindings never cross a sum, and 
      although variables might be bound outside of the term, we can never 
      unify two such terms, since doing so would require the unification to
      be lifted above the inequality.

      Products occur in two capacities -- 
       * At the root level (and the root of each nested term), product terms
         are the result of graph factorization performed in roly_poly.  As a
         consequence, variable bindings never need to be passed between these
         terms.
       * Elsewhere, products occur exclusively between non-aggsum leaf nodes.  
         Without aggsums, these terms will never introduce new unifications, so
         the bindings between these terms are irrelevant to simplify_roly.
      
      Relcalc expressions are always flattened and union-free (roly_poly 
      converts all unions into sums).  This means that a relcalc term can
       * Be a variable = variable term, which unifies (if possible) the two 
         variables.
       * Be an expression = expression term, to which we must apply our 
         mappings and process recursively, but which can not export new 
         mappings.
       * Be a constant leaf term, which we can ignore.
       * Be a relation leaf, to which we must apply our mappings, but which
         can not export new mappings.
      
      The simplify_roly algorithm takes the following steps
         1. Identify each individual aggsum in the expression.  Variable 
            mappings can be exported up through products, but not horizontally.
            Consequently, each aggsum is processed independently.  Mappings 
            received from the parent must also be applied to non-aggsum leaves.
         2. Identify all unifications in each aggsum's relcalc.  These must
            all be present at the root.
         3. Apply the unifications and identify any unifications in the term
            by recurring into the term.
         4. Apply the newly identified set of unifications to the remaining 
            relcalc expressions.
   *)
   let (mapping, result) = 
    TermRing.extract (fun _ -> []) (ListAsSet.multiunion)
      (fun _ -> failwith "Simplifying a nonroly")
      (fun lf -> 
         match lf with
         | AggSum(t, r) ->
            (* First we need to do some classification -- variables
               are classified into four categories : 
                  1- input variables -- variables bound outside of this 
                     expression.
                  2- loop variables -- variables bound in the relcalc.
                  3- bigsum variables -- variables defined in the term, but
                     not in the relcalc.  Note that some variables might
                     be defined in the term and simply not used in the relcalc.
                     Although, strictly speaking these are not bigsum variables,
                     it doesn't matter, since they don't actually appear in the
                     relcalc. 
                  
               Note that by this definition, it is possible for a variable
               to be both a bigsum or loop variable and an input variable.  
               Input takes precedence.
               *)
            let loop_vars   = bound_vars_of_relcalc r in
            let bigsum_vars = ListAsSet.diff (bound_vars_of_term t) 
                                             loop_vars in
            let classify v = 
               if List.mem v input_vars then `Input_Var
               else if List.mem v loop_vars then `Loop_Var
               else if List.mem v bigsum_vars then `Bigsum_Var
               else if List.mem v unbound_vars then `Unbound_Var
               else failwith "Simplify_Roly: BUG: Unclassifiable Variable"
            in
            let var_to_s v = (fst v)^":"^(match classify v with 
               |`Input_Var  -> "input"  | `Unbound_Var -> "unbound"
               |`Bigsum_Var -> "bigsum" | `Loop_Var    -> "loop") in
            (* Next, we extract all of the equality predicates.  r_eq contains
               all of the (a,b) terms where a = b is a term in the relcalc, and
               r_rest contains a list of all remaining terms.  Note that since
               we're operating on a roly, prod_list is guaranteed to return
               a list of *all* terms.  Otherwise, we'd have to use 
               CalcRing.extract, or an equivalent operation. *)
            let (r_eq,r_rest) = List.fold_left (fun (r_eq,r_rest) x -> 
               match x with 
               | (CalcRing.Val(AtomicConstraint(Eq,
                     TermRing.Val(Var(a)),TermRing.Val(Var(b))))) ->
                  (r_eq@[a,b], r_rest)
               | _ -> (r_eq, r_rest @ [x])) ([],[]) (CalcRing.prod_list r) in
            (* Now we use the classifications to figure out what we need to
               return.  The full matrix is enumerated on the simplify_roly
               wiki page.  The high level bits are:
                  1- Two parameters will never be unified -- This sort of 
                     unification could be used to simplify the parameter list,
                     but would end up necessitating an IF statement regardless.
                     We may as well include the constraint in the calculus and
                     keep the schema unchanged.
                  2- Note that parameters and input variables are not the same.
                     Parameters are externally bound variables (which we have 
                     only limited play with).  Input variables are variables 
                     who's original bindings come from outside of the 
                     expression. -- this means that a parameter will always
                     be an input variable, but not visa versa. *)
            let compute_substitution (subs,nonsubbed_eqs) (a, b) = 
               Debug.print "LOG-SIMPLIFY-ROLY" (fun () ->
                  "Potential Equivalence: "^(var_to_s a)^" = "^
                                            (var_to_s b));
               let return_none = 
                  if a = b then (subs, nonsubbed_eqs)
                  else let nonsub =
                           (CalcRing.Val(AtomicConstraint(Eq,
                              TermRing.Val(Var(a)),TermRing.Val(Var(b)))))
                       in (subs, nonsubbed_eqs @ [nonsub])
               in
               let return_sub sub = 
                  (subs @ [sub], nonsubbed_eqs) in
               let return_either = 
                  if List.mem a param_vars then 
                     if List.mem b param_vars  then return_none
                                               else return_sub (b,a)
                  else 
                     if String.compare (fst a) (fst b) >= 0 
                        then return_sub (a,b)
                        else return_sub (b,a)
               in match (classify a, classify b) with
                   | (`Input_Var, `Input_Var)     -> 
                     (* There are two possibilities here: Either the variables
                        have already been unified, or they haven't.  If they 
                        haven't, then we can't unify them -- we need to test
                        for equality explicitly.  If they have, then we can
                        simply drop this test. *)
                     if((Function.apply_if_present input_subs a) = 
                        (Function.apply_if_present input_subs b))
                     then (subs,nonsubbed_eqs)
                     else return_none
                   | (`Input_Var, _)              -> return_sub (b,a)
                   | (_, `Input_Var)              -> return_sub (a,b)
                   | (`Loop_Var, `Loop_Var)       -> return_either
                   | (`Loop_Var, _)               -> return_sub (b,a)
                   | (_, `Loop_Var)               -> return_sub (a,b)
                   | (`Bigsum_Var, `Bigsum_Var)   -> return_none
                   | (`Bigsum_Var, _)             -> return_sub (b,a)
                   | (_, `Bigsum_Var)             -> return_sub (a,b)
                   | (`Unbound_Var, `Unbound_Var) -> return_either
(* It should be safe to return a mapping of one unbound variable to another --
   there's nothing inherently invalid about this (as long as the overall 
   calculus expression is valid).  It just means that there's another binding
   elsewhere (and the subsequent transitive closure should find it) *)
(*                 failwith ("ERROR: Equijoin between two unsafe variables"^
                             ": "^(fst a)^" and "^(fst b)^" in "^
                             (string_of_term term)) *)
            in
            let (subs,nonsubbed_eqs) = List.fold_left compute_substitution
                                                      ([],[]) r_eq in
            (* Some of the substitutions may overlap.  We now need to compute
               their transitive closure to ensure that the substitutions don't 
               conflict.  There's a Util method that does this, but it doesn't
               preserve ordering, or some of the nice properties we want. 
               
               In addition to the transitive closure of the directed 
               substitution graph, we also want to do a little backtracking:
               The substitution [a -> b; a -> c] should be replaced by either 
               [ a -> c; b -> c ], [ a -> b; b -> c ], or possibly just 
               [ a -> b ], and the return of an equality predicate to the main
               expression
               *)
            let rec close_step (s_nonuniq:var_mapping_t): 
                               (var_mapping_t * relcalc_t list) = 
               let s = ListAsSet.uniq s_nonuniq in
               Debug.print "LOG-SIMPLIFY-ROLY" (fun () -> "Closing ["^
                  (string_of_list0 "; " (fun ((a,_),(b,_)) -> a^"->"^b) s)^
                  "]"); 
               let overlap = 
                  (ListAsSet.inter (Function.dom s) (Function.img s)) in
               if overlap = [] then
                  (* If all the directed closures have been exhausted, we can 
                     start handling undirected closure *)
                  try 
                     let (a, b1, b2) = 
                        match List.find (fun (a, bl) -> 
                           (List.length bl > 1))
                           (ListExtras.reduce_assoc s) with
                        | (a, b1::b2::_) -> (a, b1, b2)
                        | _ -> failwith 
                           ("bug in simplify_roly: "^
                            "List has fewer elements than advertised")
                     in
                     let rest = List.filter (fun (a_f,b_f) ->
                           not ((a = a_f) && ((b1 = b_f) || (b2 = b_f)))
                        ) s in
                     let (sub,nonsub) = 
                        compute_substitution ([],[]) (b1, b2) in
                     let (rcr_sub, rcr_nonsub) = close_step (rest @ (
                        if List.length sub > 0 
                        then (sub @ [a, (snd (List.hd sub))])
                        else [a, b1]
                     )) in (rcr_sub, rcr_nonsub @ nonsub)
                  with Not_found -> (s, [])
               else
                  (* Otherwise, we still have directed closures to clean up *)
               let next = List.hd overlap in
               try 
                  let parent = snd (List.find (fun (x,_) -> x = next) s) in
                     Debug.print "LOG-SIMPLIFY-ROLY" 
                                 (fun () -> "Closing with "^(fst next)^"->"^
                                            (fst parent));
                     close_step (List.map (fun (a,b) -> 
                        if b = next then (a,parent) else (a,b)
                     ) s)
               with Not_found -> 
                  failwith ("Bug in simplify_roly::close_step.  Can't close ["^
                            (string_of_list0 "; " (fun ((a,_),(b,_)) -> 
                                                    a^"->"^b) s)^"]; "^
                            "closing on "^(fst next))
            in
            let (closed_subs, nonsubbed_eqs_from_closure) = 
               close_step (subs @ input_subs) in
            (* We next recur on the term -- this is primarilly to apply the 
               substitution to all of the terms in 't', but at the same time
               we can identify any substitutions that may occur in nested 
               aggsums (which technically shouldn't exist at this point, thanks 
               to roly).  Either way, we won't be getting any more substitutions 
               from the relcalc, so we don't need to recur twice. 
               
               The input variables for this recursion are the input variables
               at the current level of recursion (things that are bound outside)
               unioned with the loop variables (things that are bound at this
               level of recursion).
               *)
            let (subs_with_term_subs, t_subbed) = 
               rcr t closed_subs 
                   (ListAsSet.union input_vars loop_vars)
            in
            Debug.print "LOG-SIMPLIFY-ROLY" (fun () -> 
               "Emerging with subs: ["^
               (string_of_list0 "; " (fun ((a,_),(b,_)) -> a^" -> "^b) 
                                subs_with_term_subs)^"]");
            (* Finally, we recur on each node of the relcalc.  We only need to 
               recur on inequalities and equalities with non-variables in them, 
               but we use this opportunity to apply transformations to variables 
               appearing in input relations.  We also take this opportunity to
               concatenate the nonsubbed equality constraints back in and 
               transform the list back into a product.
               *)
            let r_subbed = CalcRing.mk_prod ((List.map (fun x -> 
               match CalcRing.get_val x with 
                  | False -> x | True -> x
                  | Rel(rn, rv) -> 
                     CalcRing.mk_val (Rel(rn, 
                        List.map (Function.apply_if_present subs_with_term_subs)
                                 rv))
                  | AtomicConstraint(op, t1, t2) ->
                     (* No bindings are propagated through atomic constraints,
                        so we obtain the subterm's input variables from
                        all of the input, bigsum, and loop variables -- that is,
                        all of the variables bound outside of the expression.
                        
                        Also, because no bindings are propagated through
                        atomic constraints, we discard the updated substitution
                        list produced by each recurrance.
                        *)
                     let (_, t1_subbed) = 
                        rcr   t1 subs_with_term_subs
                              (ListAsSet.multiunion [
                                 input_vars;
                                 loop_vars;
                                 bigsum_vars]) in
                     let (_, t2_subbed) = 
                        rcr   t2 subs_with_term_subs
                              (ListAsSet.multiunion [
                                 input_vars;
                                 loop_vars;
                                 bigsum_vars]) in
                        CalcRing.mk_val 
                           (AtomicConstraint(op, t1_subbed, t2_subbed))
               ) r_rest) @ 
               (ListAsSet.uniq (nonsubbed_eqs @ nonsubbed_eqs_from_closure)))
            in
               (  subs_with_term_subs, 
                  if r_subbed == CalcRing.one then 
                     t_subbed
                  else if r_subbed == CalcRing.zero then
                     TermRing.zero
                  else
                     TermRing.mk_val (AggSum(t_subbed, r_subbed)))
         | Var(v) ->  
            (  input_subs,
               TermRing.mk_val (Var(Function.apply_if_present input_subs v))) 
         | External(rn, rv) ->
            (  input_subs, 
               TermRing.mk_val (External(rn, 
                  List.map (Function.apply_if_present input_subs) rv)))
         | _ -> (input_subs, TermRing.mk_val lf)
      )
      term
   in
      Debug.print "LOG-SIMPLIFY-ROLY" (fun () -> 
         "Simplify Result: "^(string_of_term term)^
                    "  ->  "^(string_of_term result)
      ); (mapping, result)
;;
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
   processing, and only "bind" the relation variables when we simplify only the
   top-level terms.
   -- Yanif: it is safe to recursively process in the second pass with only
      relation variables, since bigsum bindings are only used for substitution
      within the same level as the equality constraints defining the binding.
      This level is the enclosing bigsum Aggsum,
      (i.e. bindings in AggSum(AggSum(..., bindings), constraint-only) are
      limited upwards to the outer AggSum).
      Recursive processing here ensures bigsum var elimination for deeply
      nested aggregates as created by the preaggregation (i.e. recursive
      bigsum) rewrite.  
   
   Also note: The top-level terms here, refers to the top level in the 
   comparison dimension.  Consider the following expression:
     AggSum(AggSum(b,c), a < 2) 
   The term 'AggSum(b,c)' is still considered top-level, but the term 'a' is 
   not.
   
*)
let simplify (term: term_t)
             (rel_vars: var_t list)
             (bsum_vars: var_t list)
             (map_params: var_t list) :
             (((var_t list * term_t) * var_t list) list) =
   let simpl f = 
      Debug.print "LOG-SIMPLIFY" (fun () -> 
         "Simplify Root: "^(code_of_term f) ); 
      let (mapping, new_term) =
         simplify_roly f [] rel_vars rel_vars
      in
         Debug.print "LOG-SIMPLIFY" (fun () -> 
            "Simplify Mappings: "^
            (list_to_string (fun ((a,_),(b,_)) -> a^" -> "^b) mapping)); 
         (  (List.map (Vars.apply_mapping mapping) map_params, new_term),
            (* We never replace bigsum variables with other bigsum variables --
               if a bigsum variable has been replaced, that means it is now
               properly bound in the relcalc portion of the AggSum *)
            List.filter (fun x -> not (List.mem_assoc x mapping)) bsum_vars
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
               atoms of r; otherwise, return x monolithically.
               also, flip aggressive in the recursive call to handle deeply
               nested terms, otherwise this yields only the very
               bottom-most aggregates. For deeply nested terms, we only
               want to skip the first constraints_only.
            *)
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
    | _ -> failwith ("Calculus.decode_map_term:"^(string_of_term map_term));;

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
                  (workload: ((var_t list) * term_t) list) :
                  (((var_t list) * term_t) list * term_mapping_t) =
   let extract_from_one_term (params, term) =
      let prepend_params t =
         let p = (Util.ListAsSet.inter (term_vars t)
                    (Util.ListAsSet.union params bound_vars))
         in (p, t)
      in
      let r = extract_aggregates_from_term false term in
      Debug.print "LOG-EXTRACTIONS" (fun () ->
        "Extracting from term:\n"^(term_as_string term)^" ->\n"^
        (String.concat "\n" (List.map term_as_string r))^"\n");
      List.map prepend_params r
   in
   (* eliminate duplicate maps across monomials for this workload alone *)
   let extracted_terms = Util.ListAsSet.no_duplicates
                (List.flatten (List.map extract_from_one_term workload))
   in
   (* create mapping *)
   let theta = mk_term_mapping name_prefix extracted_terms
   in
   (* apply substitutions to input terms. *)
   let terms_after_substitution =
      List.map (fun (p, t) ->  (p, (substitute_in_term theta t))) workload
   in
      Debug.print "LOG-EXTRACTIONS" (fun () ->
        "Extractions:\n"^
        String.concat "\n" (List.map (fun (defn,t) ->
        (term_as_string t)^" := "^(term_as_string defn)^"\n") theta));
   (terms_after_substitution, theta)

let extract_base_relations (workload: (var_t list * term_t) list) :
                           (string list * (var_t list * term_t) list) =
  let union_all l = List.fold_left ListAsSet.union [] l in
  let rec extract_from_one_term term:
      (string list * term_t) = 
    TermRing.extract union_all union_all (fun x -> x)
      (fun term -> 
        match term with 
        | AggSum(subterm, cond) -> 
          let ((cond_rels, onehop_cond_rels), subbed_cond) = 
              extract_from_one_cond cond in
          let (term_rels, subbed_term) = 
              extract_from_one_term subterm in
          let unified_term = 
            TermRing.mk_prod (subbed_term :: 
              (List.map (fun rel -> TermRing.mk_val (External(rel)))
                        onehop_cond_rels))
          in
            (ListAsSet.union cond_rels term_rels, 
             if CalcRing.prod_list subbed_cond = [CalcRing.one] then
               unified_term
             else
               TermRing.mk_val (AggSum(unified_term, subbed_cond)))
        | _ -> ([], TermRing.mk_val term)
      ) term
  and extract_from_one_cond cond: 
        ((string list * (string * var_t list) list) * relcalc_t) =
    let union_all_pairs (l:(string list * (string * var_t list) list) list): 
                        (string list * (string * var_t list) list) = 
      let ((l1, l2):(string list list * (string * var_t list) list list)) = 
        List.split l in 
          (union_all l1, union_all l2)
    in
    CalcRing.extract (fun x -> failwith "TODO: extract base rels from unions") 
                     union_all_pairs 
                     (fun x -> x)
      (fun term ->
        match term with
        | Rel(reln, relvars) -> 
          (([reln], [(reln, relvars)]), CalcRing.one)
        | AtomicConstraint(c, t1, t2) ->
          let (t1_rels, subbed_t1) = extract_from_one_term t1 in
          let (t2_rels, subbed_t2) = extract_from_one_term t2 in
            ( (ListAsSet.union t1_rels t2_rels, []), 
              CalcRing.mk_val (AtomicConstraint(c, subbed_t1, subbed_t2)))
        | _ -> (([], []), CalcRing.mk_val term)
      ) cond
  in
    List.fold_left (fun (old_rels, subbed_terms) (p, term) ->
      let (rels, subbed_term) = extract_from_one_term term in
        (ListAsSet.union rels old_rels, 
         subbed_terms @ [p, subbed_term])
    ) ([], []) workload

let base_relation_expr ((reln:string), (relvars:var_t list)): term_t = 
  TermRing.mk_val (AggSum(TermRing.one, 
                          CalcRing.mk_val (Rel(reln, relvars))))

type bs_rewrite_mode_t = ModeExtractFromCond
                       | ModeGroupCond
                       | ModeIntroduceDomain
                       | ModeOpenDomain

let bigsum_rewriting (mode: bs_rewrite_mode_t)
                     (term: term_t)
                     (params: var_t list)
                     (map_name_prefix: string) :
                     ((var_t list) * term_mapping_t * term_t) =
   let bs_submap_id = ref 0 in
   let leaf_f lf =
      match lf with
         AggSum(t, r) when ((not (constraints_only r)) &&
                           ((snd (split_nested r)) <> relcalc_one)) ->
            (* only do it if not constraints only and there are nested aggs. *)
            let (flat, nested) = split_nested r (* must be a monomial *)
            in
            (* We treat propagated group-by attributes as bigsum vars,
               so that any elimination of these vars are also handled by
               simplify_roly. Later, we can separate bigsum and propagated
               group-bys by checking against map parameters.
               Note that nested aggregates do not have propagated group-bys,
               this only occurs with the outermost bigsum rewrite.
               -- Out vars are locally safe vars that are map params
               -- In vars are free vars that are not range-restricted.
               -- Propagated vars are all vars used in the nested part except
                  in vars (which come from outside)
            *)
            let out_vars = Util.ListAsSet.inter (safe_vars flat []) params in
            let in_vars = Util.ListAsSet.diff params (safe_vars flat []) in
            let prop_vars = Util.ListAsSet.inter (relcalc_vars nested)
               (Util.ListAsSet.diff (safe_vars flat params) in_vars)
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
                       CalcRing.mk_prod(
                         [CalcRing.mk_val(Rel(
                           "Dom_{"^(Util.string_of_list ", " 
                            (fst (List.split prop_vars)))^"}", prop_vars));
                          nested])))))
                          (* TODO: we should also collect the information
                             needed to maintain the domain relation here. *)
                | ModeOpenDomain ->
                  (* like ModeIntroduceDomain, but the domain is implicit.
                     There are free bigsum variables, and the loop to iterate
                     over the relevant valuations of these variables has to
                     be worried about elsewhere. The advantage is that
                     delta applied to this term is guaranteed to be simpler,
                     so we can do recursive delta computation.
                  *)
                  (Util.ListAsSet.union prop_vars out_vars,
                    TermRing.mk_val(
                      AggSum(TermRing.mk_val(AggSum(t, flat)), nested)))
               )
            in
            (* now extract ALL nested aggregates *)
            let agg_plus_vars (params, agg) =
               (Util.ListAsSet.inter (term_vars agg) params, agg)
            in
            let (t_params, r_params) = match mode with
              | ModeExtractFromCond | ModeGroupCond -> (in_vars, params)
              | _ -> (Util.ListAsSet.union prop_vars out_vars, prop_vars)
            in
            let extract_sub_aggregates_from_term t =
               match t with
                | TermRing.Val(AggSum(t, r)) ->
                    (List.map (fun x -> (t_params,x))
                      (extract_aggregates_from_term false t)) @
                    (List.map (fun x -> (r_params,x))
                      (extract_aggregates_from_calc false r))

                | _ -> raise (Assert0Exception
                  "Calculus.bigsum_rewriting.extract_sub_aggregates_from_term")
            in
            let aggs = (List.map agg_plus_vars
               (extract_sub_aggregates_from_term new_term))
            in
            bs_submap_id := (!bs_submap_id) + 1;
            let theta = mk_term_mapping
              (map_name_prefix^"BS"^(string_of_int !bs_submap_id)^"_") aggs
            in
            ([(bigsum_vars, theta)], substitute_in_term theta new_term)
       | _ -> ([([], [])], TermRing.mk_val(lf))
   in
   let (a,  b)  = TermRing.extract List.flatten List.flatten (fun x->x)
                                   leaf_f term in
   let (a1, a2) = List.split a in
   (Util.ListAsSet.no_duplicates (List.flatten a1), List.flatten a2, b)


(* assumes term has already had roly_poly applied *)
let preaggregate (mode: bs_rewrite_mode_t)
                 (term : term_t)
                 (params: var_t list)
                 (map_name_prefix: string) =
  let submap_id = ref 0 in
  let rec rewrite_aux t params : ((var_t list * term_mapping_t) list) * term_t =
    let calc_leaf_f propagated_vars leaf_c =
      match leaf_c with
      | AtomicConstraint(op,t1,t2) ->
        (* Restrict propagations to those used by nested terms.
           Thus params are only in params, these nested aggregates *must* have
           no out params.
        *)
        let aux t =
          let params = Util.ListAsSet.inter propagated_vars (term_vars t) in
          let t_props_substs, new_t = rewrite_aux t params in
          let prop_vars, theta = List.split t_props_substs in
          (Util.ListAsSet.no_duplicates (List.flatten prop_vars),
           List.flatten theta, new_t) 
        in
        let t1_prop_vars, t1_theta, new_t1 = aux t1 in
        let t2_prop_vars, t2_theta, new_t2 = aux t2 in
        ([Util.ListAsSet.union t1_prop_vars t2_prop_vars, t1_theta@t2_theta],
          CalcRing.mk_val(AtomicConstraint(op, new_t1, new_t2)))
      | _ -> ([([],[])], CalcRing.mk_val(leaf_c))
    in
    let term_leaf_f leaf_t =
      match leaf_t with
      | AggSum(f,r) when ((not (constraints_only r)) &&
                         ((snd (split_nested r)) <> relcalc_one)) ->
        (* only do it if not constraints only and there are nested aggs. *)
        let (flat, nested) = split_nested r (* must be a monomial *) in
        
        (* We treat propagated group-by attributes as bigsum vars,
           so that any elimination of these vars are also handled by
           simplify_roly. Later, we can separate bigsum and propagated
           group-bys by checking against map parameters.
           Note that nested aggregates do not have propagated group-bys,
           this only occurs with the outermost bigsum rewrite.
           -- Out vars are locally safe vars that are map params
           -- In vars are free vars that are not range-restricted.
           -- Propagated vars are all vars used in the nested part except
              in vars (which come from outside)
        *)
        let rr_vars = safe_vars flat [] in
        let out_vars = Util.ListAsSet.inter rr_vars params in
        let in_vars = Util.ListAsSet.diff params rr_vars in
        let prop_vars = Util.ListAsSet.inter (relcalc_vars nested)
           (Util.ListAsSet.diff (safe_vars flat params) in_vars)
        in
        
        Debug.print "LOG-PREAGG" (fun() ->
          "Nested term:\n"^(term_as_string (TermRing.mk_val leaf_t))^
          "\nrr_vars: "^(String.concat "," (List.map fst rr_vars))^
          "\nout_vars: "^(String.concat "," (List.map fst out_vars))^
          "\nin_vars: "^(String.concat "," (List.map fst in_vars))^
          "\nprop_vars: "^(String.concat "," (List.map fst prop_vars)));
        
        (* recur on nested part and use in the rewrite of this level *)
        let (nested_props_substs, new_nested) =
          CalcRing.extract List.flatten List.flatten (fun x->x)
            (calc_leaf_f prop_vars) nested
        in
        let (bigsum_vars, new_term) =
          (* For deeply nested queries, preaggregation must be designed with
             the map materialization method in mind. Currently, map
             materialization is done with extract_named_aggregates, and this
             only works with ModeOpenDomain. ModeOpenDomain has the property
             that the rewritten term is a constraint-only form, and any deltas
             of the rewritten term is also in constraint-only form.
             Constraint-only form simplifies the materialization choice: the
             flat part is always materialized, while the nested part is not.
             For all other modes, the nested and flat parts are mixed after a
             delta transform.
             Thus this function should always be invoked with ModeOpenDomain
             on deeply nested queries, however we leave the other modes here
             for illustration on alternative nesting evaluation strategies,
             that could potentially be utilized with a more complex
             materialization function.
             Note that however even with ModeOpenDomain, there can still be
             issues regarding extraction and substitution of the newly created
             external, given the bottom up substitution with the first match
             found. See substitute_in_term for details.
           *)
          match mode with
          | ModeExtractFromCond ->
            (* just extract nested aggregates from conditions. The extraction
               happens below, for all modes. We must execute such a term as is:
               the delta rewriting creates a term that is not simpler.
               (The same is true for ModeGroupCond and ModeIntroduceDomain.) *)
            ([], leaf_t)
          
          | ModeGroupCond ->
            (* extract all the conditions with a nested aggregate, "nested",
               into a single condition AggSum(1, nested) = 1. The advantage of
               this over ModeExtractFromCond is that there is less work to do
               at runtime iterating over all the tuples of flat. *)
            let grouped_conds = TermRing.mk_val(AggSum(term_one, new_nested)) in
              ([],
               (AggSum(t, CalcRing.mk_prod([flat;
                  CalcRing.mk_val(AtomicConstraint(Eq, term_one,
                    grouped_conds))]))))
              (* TODO: the whole point of this mode is that grouped_conds
                 is made external, but this is not happening yet. *)

          | ModeIntroduceDomain ->
            (* introduce explicit domain relations. Like ModeExtractFromCond
               but with duplicate elimination of the values we iterate over,
               so there are fewer iteration steps, but there is the additional
               cost of maintaining the domain relation. *)
            ([],
              (AggSum(
                 TermRing.mk_val(AggSum(f, flat)),
                   CalcRing.mk_prod(
                     [CalcRing.mk_val(Rel(
                       "Dom_{"^(Util.string_of_list ", " 
                         (fst (List.split prop_vars)))^"}", prop_vars));
                      new_nested]))))
            (* TODO: we should also collect the information needed to maintain
               the domain relation here. *)

          | ModeOpenDomain ->
            (* like ModeIntroduceDomain, but the domain is implicit.
               There are free bigsum variables, and the loop to iterate
               over the relevant valuations of these variables has to
               be worried about elsewhere. The advantage is that
               delta applied to this term is guaranteed to be simpler,
               so we can do recursive delta computation. *)
            (Util.ListAsSet.union prop_vars out_vars,
             (AggSum(TermRing.mk_val(AggSum(f, flat)), new_nested)))

        in
        (* now extract ALL nested aggregates *)
        let agg_plus_vars (params, agg) =
           (Util.ListAsSet.inter (term_vars agg) params, agg)
        in
        let (t_params, r_params) = match mode with
          | ModeExtractFromCond | ModeGroupCond -> (in_vars, params)
          | _ -> (Util.ListAsSet.union prop_vars out_vars, prop_vars)
        in
        let extract_sub_aggregates_from_term t =
           match t with
            | TermRing.Val(AggSum(t, r)) ->
              (List.map (fun x -> (t_params,x))
                (extract_aggregates_from_term false t)) @
              (List.map (fun x -> (r_params,x))
                (extract_aggregates_from_calc false r))
            | _ -> raise (Assert0Exception
              "Calculus.bigsum_rewriting.extract_sub_aggregates_from_term")
        in
        let aggs = (List.map agg_plus_vars
                     (extract_sub_aggregates_from_term (TermRing.mk_val new_term)))
        in
        submap_id := (!submap_id) + 1;
        let theta = mk_term_mapping
          (map_name_prefix^"BS"^(string_of_int !submap_id)^"_") aggs
        in
        let nested_props, nested_substs = List.split nested_props_substs in
        ([(Util.ListAsSet.union bigsum_vars
            (Util.ListAsSet.no_duplicates (List.flatten nested_props)),
          (List.flatten nested_substs)@theta)],
         substitute_in_term theta (TermRing.mk_val new_term))

      | _ -> ([([], [])], TermRing.mk_val(leaf_t))  
    in
    let simplified_t = TermRing.mk_sum
      (List.map (fun x ->
(*          snd (simplify_roly true x params []))*)
            snd (simplify_roly x [] 
                               params
                               params))
        (TermRing.sum_list t))
    in TermRing.extract
         List.flatten List.flatten (fun x->x) term_leaf_f simplified_t
  in
  let (props_and_substs, new_term) = rewrite_aux term params in
  let propagations, substitutions = List.split props_and_substs 
  in (Util.ListAsSet.no_duplicates (List.flatten propagations),
      List.flatten substitutions, new_term)



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
type equiv_state_t = (string StringMap.t * StringSet.t) 

let equate_terms (term_a:term_t) (term_b:term_t): (string StringMap.t) =
  let map_var (equiv_state:equiv_state_t)
              ((a_name, a_type):var_t)
              ((b_name, b_type):var_t): equiv_state_t =
    let (l_r_map,r_l_set) = equiv_state in
      if a_type <> b_type then 
        raise (TermsNotEquivalent("Type Mismatch: "^a_name^"->"^b_name))
      else
      if StringMap.mem a_name l_r_map then
        (* If there's a mapping, it'd better be the case that 
           (A => B) exists in l_r_map
           (B) exists in r_l_set
           The latter is really just a sanity check.
        *)
        if (StringMap.find a_name l_r_map) <> b_name then 
          raise (TermsNotEquivalent("Variable Mismatch: "^a_name^"->"^
                                   (StringMap.find a_name l_r_map)^" and "^
                                   b_name))
        else if not (StringSet.mem b_name r_l_set) then 
          failwith "Backwards mapping failed in Calculus.equate_terms:mem"
        else (l_r_map,r_l_set)
      else if StringSet.mem b_name r_l_set then 
        raise (TermsNotEquivalent("Variable Mismatch: Adding "^
                                 a_name^"->"^b_name^
                                 ", but there's already a reverse mapping"))
      else (StringMap.add a_name b_name l_r_map,
            StringSet.add b_name r_l_set)
  in
  let rec cmp_term_lf (a:TermRing.leaf_t) 
                      (b:TermRing.leaf_t)
                      (var_map:equiv_state_t): 
                      equiv_state_t option =
    match a with
    | AggSum(at,aphi)    -> 
      ( match b with 
        | AggSum(bt,bphi) -> Some(cmp_calc aphi bphi (cmp_term at bt var_map))
        | _               -> raise (TermsNotEquivalent("Aggsum Mismatch"))
      )
    | Const(ac)          -> 
      (if b = a then Some(var_map)
       else raise (TermsNotEquivalent("Const Mismatch")))
    | Var(av)            -> 
      ( match b with Var(bv) -> Some(map_var var_map av bv)
                   | _ -> raise (TermsNotEquivalent("var Mismatch") ))
    | External(an,avars) -> 
      ( raise (TermsNotEquivalent("TODO: EXTERNAL") )
      (* We don't support post-compiled comparisons (yet) *) )
  and cmp_calc_lf (a:CalcRing.leaf_t) 
                  (b:CalcRing.leaf_t)
                  (var_map:equiv_state_t): 
                  equiv_state_t option =
    match a with
    | False -> if b = False then Some(var_map) else 
          raise (TermsNotEquivalent("'False' Mismatch"))
    | True  -> if b = True then Some(var_map) else 
          raise (TermsNotEquivalent("'True' Mismatch"))
    | AtomicConstraint(c,al,ar) -> 
      ( match b with 
        (* for now force ac == bc; TODO: comparator-specific equivalencies *)
        | AtomicConstraint(c,bl,br) -> 
          Some(cmp_term ar br (cmp_term al bl var_map))
        | _ -> 
          raise (TermsNotEquivalent("AtomicConstraint Mismatch"))
      )
    | Rel(rel_name, a_vars) -> 
      ( match b with
        | Rel(rel_name2, b_vars) when rel_name2 = rel_name -> 
            if (List.length a_vars) <> (List.length b_vars) then 
              raise (TermsNotEquivalent("Rel Var Mismatch"))
            else Some(List.fold_left2 map_var var_map a_vars b_vars)
        | _ -> 
          raise (TermsNotEquivalent("Rel Mismatch"))

      )
  and cmp_term a b var_map = 
    match (TermRing.cmp_exprs cmp_term_lf a b var_map) with
    | Some(a) -> a
    | None -> raise (TermsNotEquivalent("Term structure mismatch"))
  and cmp_calc a b var_map = 
    match CalcRing.cmp_exprs cmp_calc_lf a b var_map with
    | Some(a) -> a
    | None -> raise (TermsNotEquivalent("Calc structure mismatch"))
  in
    (fst (cmp_term term_a term_b ((StringMap.empty,StringSet.empty))))
;;

type roly_factor = RFCalc of relcalc_t | RFTerm of term_t

let rec un_roly_poly (monomials:term_t list): term_t = 
   Debug.print "LOG-UN-ROLY" (fun () ->
      "Un-Roly Of : \n   " ^
         (string_of_list0 "\n   " string_of_term monomials)
   );
   let extract_candidates (m:term_t): 
                          ((roly_factor * var_t list) * term_t) list = 
      let extract_var (vt:term_t) = match vt with
         TermRing.Val(Var(v)) -> [v] | _ -> [] 
      in
      let rec candidates_from_calc calc = match calc with
         | CalcRing.Prod(p) ->
            let (candidates, cmps) =
               List.split (ListExtras.scan (fun prev curr next -> 
                  let (candidates,cmps) = candidates_from_calc curr in
                  (  List.map (fun (candidate,expr) ->
                        (candidate, CalcRing.mk_prod (prev@[expr]@next))
                     ) candidates,
                     cmps
                  )
               ) p)
            in 
               (List.flatten candidates, List.flatten cmps)
         | CalcRing.Sum(_) -> 
            failwith "UnRolyPoly got a non-monomial expression as input"
         | CalcRing.Neg(n) ->
            let (candidates, cmps) = candidates_from_calc n in
               (  List.map 
                     (fun (candidate, expr) -> 
                        (candidate, CalcRing.mk_neg expr))
                     candidates,
                   cmps)
         | CalcRing.Val(Rel(rn, rv)) -> 
            ([((RFCalc(calc)),rv), CalcRing.one], [])
         | CalcRing.Val(AtomicConstraint(_, a, b)) -> 
            ([], (extract_var a)@(extract_var b))
         | CalcRing.Val(_) -> ([],[])
      in
      let rec candidates_from_term term = 
      match term with 
         | TermRing.Val(AggSum(t,c)) -> 
            let (all_candidates,cmps) = (candidates_from_calc c) in
            let candidates = List.filter (fun ((rc,rv),_) ->
                        not (List.exists (fun x -> List.mem x rv) cmps)
               ) all_candidates
            in
               List.map (fun (candidate,expr) ->
                  (candidate, if (expr = CalcRing.one) then t 
                              else (TermRing.Val(AggSum(t, expr))))
               ) candidates
         | TermRing.Sum(_) -> 
            failwith "UnRolyPoly got a non-monomial expression as input"
         | TermRing.Prod(p) -> 
            List.flatten (ListExtras.scan (fun prev curr next ->
               List.map (fun (candidate,expr) ->
                  (candidate, TermRing.mk_prod (prev@[expr]@next))
               ) (candidates_from_term curr)
            ) p)
         | TermRing.Neg(n) -> List.map (fun (candidate,expr) ->
               (candidate, (TermRing.mk_neg expr))
            ) (candidates_from_term n)
         | TermRing.Val(Const(_)) -> [(((RFTerm(term)), []), TermRing.one)]
         | TermRing.Val(Var(_))   -> [(((RFTerm(term)), []), TermRing.one)]
         | TermRing.Val(External(_,_)) -> []
      in
         candidates_from_term m
   in
   let per_m_candidates:((roly_factor * var_t list) * term_t) list list =
      List.map extract_candidates monomials 
   in
   let candidates = 
      List.fold_left ListAsSet.union 
                     []
                     (List.map (fun (x,_)->[x])
                               (List.flatten per_m_candidates))
   in
   let candidate_counts =
      List.map (fun c -> 
         c, List.fold_left (fun cnt m -> if (List.mem_assoc c m) then cnt+1
                                                                 else cnt)
                           0 per_m_candidates
      ) candidates
   in
   if (candidates = []) || 
      (not (List.exists (fun (_,c) -> c>1) candidate_counts)) 
      then TermRing.mk_sum monomials else
   let (best, _) =
      List.hd (List.sort (fun (_,a) (_,b) -> compare b a) candidate_counts)
   in
   if (fst best) = (RFTerm(TermRing.one)) then 
      TermRing.mk_sum monomials else (
   Debug.print "LOG-UN-ROLY" (fun () ->
      " --> Extracting : "^(
         match (fst best) with 
            | RFCalc(c) -> string_of_relcalc c
            | RFTerm(t) -> string_of_term t
      )
   );
   let (with_term,without_term) = 
      List.partition (fun (_,candidates) ->
         List.mem_assoc best candidates
      ) (List.combine monomials per_m_candidates)
   in
   let with_term_monomials = 
      List.map (fun (_,candidates) -> List.assoc best candidates) with_term
   in
   let without_term_monomials =
      List.map (fun (m,_) -> m) without_term
   in 
      TermRing.mk_sum [
         un_roly_poly without_term_monomials;
         (  match (fst best) with
            | RFCalc(c) ->
               let with_un_roly = (un_roly_poly with_term_monomials) in
                  (  match with_un_roly with
                     | TermRing.Val(AggSum(subt,subc)) -> 
                        TermRing.mk_val(AggSum(
                           subt,
                           (CalcRing.mk_prod [c; subc])
                        ))
                     | _ -> TermRing.mk_val(AggSum(with_un_roly, c))
                  )
            | RFTerm(t) -> 
               TermRing.mk_prod[t;un_roly_poly with_term_monomials]
         )
      ]
   )
;;

let rec const_prod (x:const_t) (y:const_t): const_t = 
   match (x,y) with
      | ((String _),_)                -> failwith "Product of a string"
      | (_,(String _))                -> failwith "Product of a string"
      | ((Int(c1)),(Int(c2)))         -> Int(c1*c2)
      | ((Int(c1)),_)                 -> const_prod y x
      | ((Double(c1)),(Int(c2)))      -> Double(c1 *. (float_of_int c2))
      | ((Double(c1)),(Double(c2)))   -> Double(c1 *. c2)
      | ((Double _),_)                -> const_prod y x
      | ((Long(c1)),(Int(c2)))        -> Long(Int64.mul c1 (Int64.of_int c2))
      | ((Long(c1)),(Double(c2)))     -> Double((Int64.to_float c1) *. c2)
      | ((Long(c1)),(Long(c2)))       -> Long(Int64.mul c1 c2)
      | ((Long _),_)                  -> const_prod y x
      | ((Boolean(c1)),(Boolean(c2))) -> Boolean(c1 && c2)
      | ((Boolean _),_)               -> failwith "Non-and product of boolean"
;;

let rec const_sum (x:const_t) (y:const_t): const_t = 
   match (x,y) with
      | ((String _),_)                -> failwith "Sum of a string"
      | (_,(String _))                -> failwith "Sum of a string"
      | ((Int(c1)),(Int(c2)))         -> Int(c1+c2)
      | ((Int(c1)),_)                 -> const_sum y x
      | ((Double(c1)),(Int(c2)))      -> Double(c1 +. (float_of_int c2))
      | ((Double(c1)),(Double(c2)))   -> Double(c1 +. c2)
      | ((Double _),_)                -> const_prod y x
      | ((Long(c1)),(Int(c2)))        -> Long(Int64.add c1 (Int64.of_int c2))
      | ((Long(c1)),(Double(c2)))     -> Double((Int64.to_float c1) +. c2)
      | ((Long(c1)),(Long(c2)))       -> Long(Int64.add c1 c2)
      | ((Long _),_)                  -> const_prod y x
      | ((Boolean(c1)),(Boolean(c2))) -> Boolean(c1 || c2)
      | ((Boolean _),_)               -> failwith "Non-or sum of boolean"

;;

let un_roly_postprocess (input_term:term_t): term_t = 
   let rec coalesce_flattened_term ((c, t), r) =
      factorize_aggsum_mm (TermRing.mk_prod [TermRing.mk_val (Const(c)); t]) r

   and flatten_calc (calc:relcalc_t): (bool * relcalc_t) =
      CalcRing.fold
         (fun sum_list -> (false, CalcRing.mk_sum
            (List.map 
               (fun (n,c) -> if n then CalcRing.mk_neg c else c) 
               sum_list)))
         (fun prod_list ->
            let (n,c) = List.split prod_list in
               (  List.fold_left (fun x y -> x <> y) false n, 
                  CalcRing.mk_prod c))
         (fun (n,c) -> (not n, c))
         (fun leaf -> 
            match leaf with 
               | False -> (true, CalcRing.one)
               | AtomicConstraint(op, lhs, rhs) ->
                  (false, CalcRing.mk_val (AtomicConstraint(op,
                     coalesce_flattened_term (flatten_term lhs),
                     coalesce_flattened_term (flatten_term rhs))))
               | _ -> (false, CalcRing.mk_val leaf))
         calc
         
   and flatten_term (term:term_t): ((const_t * term_t) * relcalc_t) = 
      TermRing.fold 
         (fun sum_list -> ((Int(1), TermRing.mk_sum 
            (List.map coalesce_flattened_term sum_list)), CalcRing.one))
         (fun prod_list ->
            let (ct,r) = List.split prod_list in
            let (cl,t) = List.split ct in
            let c = List.fold_left const_prod (Int(1)) cl in
               ((c, TermRing.mk_prod t), CalcRing.mk_prod r))
         (fun ((c,t),r) -> ((const_prod c (Int(-1)), t), r))
         (fun leaf -> 
            match leaf with
               | Const(c) -> ((c, TermRing.one), CalcRing.one)
               | AggSum(t,r) -> 
                  let ((t_const,t_term),t_relcalc) = flatten_term t in
                  let (r_const,r_relcalc) = flatten_calc r in
                     (  (  (  if r_const 
                              then const_prod t_const (Int(-1)) 
                              else t_const),
                           t_term),
                        CalcRing.mk_prod [t_relcalc; r_relcalc] )
               | _ -> (((Int(1)), TermRing.mk_val leaf), CalcRing.one))
         term
   
   in
      coalesce_flattened_term (flatten_term input_term)

;;

let poly_factorize (terms:term_t list): term_t =
   let factorized_term = un_roly_poly terms in
   let flattened_term = un_roly_postprocess factorized_term in
   (Debug.print "LOG-POLY-FACTORIZE" (fun () ->
      "------------POLY-FACTOR------------\n    From: " ^
      (string_of_term factorized_term)^"\n    To: "^
      (string_of_term flattened_term)
   )); flattened_term

;;

let term_sum_list = TermRing.sum_list
let term_list_sum = TermRing.mk_sum