(*
   This module is for representing

      * formulae of positive quantifier-free relational calculus/first-order
        logic (i.e., formulae constructed from conjunctions "and",
        disjunctions "or", and atoms) and

      * terms.

   The atomic formulae are relational atoms, false, true,
   and atomic constraints comparing terms using =, neq, <, and <=.

   Terms are built from variables, constants, external function calls,
   and aggregrate sums (AggSum) using addition and multiplication.

   AggSum(t, r), where t is a term and r is a relcalc expression,
   computes the sum of the t values for each of the tuples (no duplicate
   elimination) computed by r. In the case that r is constraints-only
   (i.e., no relational atoms occur in it), AggSum(t, r) computes the
   sum of the t values for those valuations of the variables common to
   both t and r (minus those that are parameters, which would bind their
   domains to a single parameter value) which satisfy condition r.

   Functions for computing delta expressions, variable substitution,
   and simplification are also provided in this module.


   RELATIONSHIP TO RELATIONAL ALGEBRA:

   Our calculus fragment corresponds to a fragment of positive relational
   algebra consisting of

      * the constant nullary singleton relation (~true),
      * the untyped empty relation (~false),
      * base relations (~ relational atoms),
      * selections (~ atomic constraints),
      * unions (~ disjunctions), and
      * natural joins (~ conjunctions)

   with implicit safe projection. That is, free variables/columns that
   are unsafe are projected away as early as possible.
   
   Observe:
     * We use an unordered schema model in our expressions, thus we
       do not need the projection operation for column reordering.
     * The schema of a union is the intersection of the schemas of
       the constituents.
     * Projections on the top-level are not needed because we always have
       an aggregrate on top which just ignores some columns, as if there
       were a projection.
     * Projections anywhere else would only conceivable be needed for
       efficiency, and we achieve efficiency in a nonstandard ways where
       projection pushing does not matter.
     * The variables not projected away are exactly the safe (=rr) variables.
     * Not using projections has the advantage that any calculus formula
       is a semiring expression. But the implication is
       that our implementation cannot be completely encapsulated, because
       some positive relational algebra operations that project away more
       columns than needed to make the unions consistent are not expressible.
*)



type type_t = TInt | TLong | TDouble | TString
type var_t   = string * type_t                (* type of variables *)
type comp_t  = Eq | Lt | Le | Neq             (* comparison operations *)
type const_t = Int    of int                  (* typed constant terms *)
             | Double of float
             | Long   of int64
             | String of string

(* leaves (=atoms) of the calculus *)
type 'term_t generic_relcalc_lf_t =
            False
          | True
          | AtomicConstraint of comp_t * 'term_t * 'term_t
          | Rel of string * (var_t list)
            (* these are relational atoms of the calculus.
               There are obviously no column names.
               Note: Even though a value takes a variable list, these variables
               do not correspond to column names, but are variable that
               may occur elsewhere, and if bound, have to be treated like
               constant terms. That is, if x is not bound but y is,
               Then (R(x) and x=y) is equivalent to R(y), but y remains
               bound. This is important when translated back to relational
               algebra. In that case, if R has schema R(A), the calculus
               formula R(y) translates to select_{A=y}(R), not to R. *)

type ('term_t, 'relcalc_t) generic_term_lf_t =
            AggSum of ('term_t * 'relcalc_t)
          | Const of const_t
          | Var of var_t
          | External of (string * (var_t list))
                        (* name and variable list;
                           could be generalized to terms *)


(* The following types are abstract; their implementation is hidden. *)
type relcalc_t                 (* calculus formulae *)
type relcalc_lf_t
type term_t
type term_lf_t

type var_mapping_t = (var_t * var_t) list

(* the first is always the full term and the second is the external. *)
type term_mapping_t = (term_t * term_t) list




(* Auxiliary types for constructing and accessing values of the abstract
   types.
   TODO: RA_MultiUnion and RA_MultiNatJoin are still concepts of relational
   algebra rather than calculus; we should eventually change this to
   bigand and bigor. *)
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




(* back an forth between relcalc_t and readable_relcalc_t *)
val readable_relcalc:  relcalc_t -> readable_relcalc_t
val make_relcalc:      readable_relcalc_t -> relcalc_t

val readable_term: term_t -> readable_term_t
val make_term:     readable_term_t -> term_t

val decode_map_term: term_t -> (string * (var_t list))
val map_term: string -> var_t list -> term_t

(* other output functions *)

type relalg_t =
   Alg_MultiProd of (relalg_t list)
 | Alg_Selection of (((comp_t * var_t * var_t) list) * relalg_t)
 | Alg_Rel of string * string
 | Alg_True

(* TODO: explain *)
val relcalc_as_algebra: relcalc_t -> string -> (var_t list) -> relalg_t

(* output relcalc or term as string. *)
val relcalc_as_string: relcalc_t -> string
val term_as_string:    term_t    -> string



(* all the variables that occur in the formula resp. term. *)
val relcalc_vars: relcalc_t -> var_t list
val term_vars:       term_t -> var_t list

(* set of safe variables of a formula; a formula phi is range-restricted
   given a set of bound variables (which are treated like constants, i.e.,
   are safe) iff all free variables are in (safe_vars phi bound_vars).
   The result contains all the param_vars.
*)
val safe_vars: relcalc_t -> (var_t list) -> (var_t list)


val relcalc_zero: relcalc_t
val relcalc_one:  relcalc_t

val term_zero: term_t
val term_one:  term_t


(* turns a formula into DNF.  *)
val polynomial: relcalc_t -> relcalc_t

(* return DNF as list of monomials (=conjunctions). *)
val monomials: relcalc_t -> (relcalc_t list)



(* a pudding. Here: recursively turning a term into a
   polynomial: The result does not use union anywhere, and sum is only used
   on the very top level of the expression.

   Also factorizes AggSum monomials, i.e. turns e.g.
   sum{A*B}(R bowtie S) for schema R(A), S(B) into sum{A}(R) * sum{B}(S).
*)
val roly_poly: term_t -> term_t

(* given a union-free term and a list of bound variables,
   (simplify_roly recurse term bound_vars) unifies variables as
   much as possible, but does not rename the bound variables.
   The result is a pair of the unifer employed and the resulting term.
*)
val simplify_roly: bool -> term_t -> (var_t list) ->
                   (((var_t * var_t) list) * term_t)

(* (simplify_calc_monomial recurse phi bound_vars)
   simplifies formula phi by eliminating redundant variable bottom-up
   (recursively through nested terms if the recurse flag is set). *)
val simplify_calc_monomial: bool -> relcalc_t -> (var_t list) ->
                            (((var_t * var_t) list) * relcalc_t)

(* calls roly_poly. For each union-free term returned by roly_poly,
   simplify_roly is called.  Also applies the substitution computed to the
   variables given in the third argument (the parameters). Returns
   pairs consisting of renamed parameters and simplified nested monomials.
*)
val simplify: term_t -> (var_t list) -> (var_t list) ->
                        ((var_t list * term_t) list)



(* Are all the atoms of the given calculus formula constraints?
   That is, can the formula be turned into an equivalent algebra query
   Select_{<constraints>}(Constant_Nullary_Singleton) ?
   If so, this can be done with extract_substitutions.
*)
val constraints_only: relcalc_t -> bool

(* (apply_variable_substitution theta phi) substitutes variables in
    calculus formula phi according to theta. *)
val apply_variable_substitution_to_relcalc: ((var_t * var_t) list)
                                            -> relcalc_t -> relcalc_t

(* (apply_variable_substitution theta term) substitutes variables anywhere
   in term using mapping theta, recursively. This includes relational
   calculus subexpressions.
*)
val apply_variable_substitution_to_term: var_mapping_t -> term_t -> term_t

(* Given monomial m that is constraints-only and a list of variables l,
   (extract_substitutions m preferred_vars) returns a pair of

   * a map f for unifying variables based on the equality constraints in m,
     prioritizing to take an element from preferred_vars for each
     equivalence class of variables in m.

   * the remaining constraints (which are not of type Eq).

   Note: the result map is not necessarily complete;
   for the variables and columns
   on which it is not defined, it is the identity. Thus, if this
   map is to be applied to a Ring, use substitute_many.

   Update: the relational calculus expression does not have to be
   a monomial, but if it isn't, the function works with the
   topmost product of the expression.
*)
val extract_substitutions: relcalc_t -> (var_t list) ->
                           (var_mapping_t * relcalc_t)

(* a list consisting of the maximal AggSum subexpressions of the input
   formula/term. If the first argument is false, we do not pull out
   AggSum(t, r) subexpressions that are ifs (i.e., r is constraints
   only); instead, we recursively extract from t and r. *)
val extract_aggregates_from_calc: bool -> relcalc_t -> (term_t list)
val extract_aggregates_from_term: bool -> term_t -> (term_t list)

(* split calculus monomial into flat and nested part. *)
val split_nested: relcalc_t -> (relcalc_t * relcalc_t)

(* Note: the substitution is bottom-up. This means we greedily replace
   smallest subterms, rather than largest ones. This has to be kept in
   mind if terms in aggsum_theta may mutually contain each other. *)
val substitute_in_term: term_mapping_t -> term_t -> term_t

(* takes a map name prefix string, a list of variables that have to
   be communicated to the map if present, and a
   list of (parameter list, term) pairs, creates named maps from them,
   and returns a term to external map lookup substitution. *)
val mk_term_mapping: string -> (((var_t list) * term_t) list) -> term_mapping_t

(* given a list of pairs of terms and their parameters, this function
   extracts all the nested aggregate sum terms, eliminates duplicates,
   and then names the aggregates.

   We do it in this complicated fashion to avoid creating the same
   child terms redundantly.
*)
val extract_named_aggregates: string -> (var_t list) ->
                              (((var_t list) * term_t) list)  ->
                              ((((var_t list) * term_t) list * term_mapping_t))


type bs_rewrite_mode_t = ModeExtractFromCond
                       | ModeGroupCond
                       | ModeIntroduceDomain
                       | ModeOpenDomain

(* given a term t, a set of bound variables, and a name prefix for maps
   to be generated,

   (bigsum_rewriting t bound_vars map_name_prefix)

   returns a triple

   (bigsum_vars, theta, modified_term)

   such that modified_term in ModeExtractFromCond, ModeGroupCond, and
   ModeIntroduceDomain is equivalent to t, where some aggregate subterms
   may have been pulled out from modified_term, as encoded in the term map
   theta. Only ModeOpenDomain results in a nonempty list of bigsum_vars and a
   modified_term such that t is equivalent to sum_{bigsum_vars} modified_term.
   For details on the modes, see the implementation of this function.
*)
val bigsum_rewriting: bs_rewrite_mode_t -> term_t -> (var_t list) -> string ->
                      ((var_t list) * term_mapping_t * term_t)


(* (delta f n "R" t e) returns the delta on insertion (n=false) or
   deletion (n=true) of tuple t into relation R, for relcalc expression e,
   where f is used to map external terms to their deltas. *)
val relcalc_delta: term_mapping_t ->
                   bool -> string -> (var_t list) -> relcalc_t -> relcalc_t

(* (delta f deletion relname tuple term) computes the delta of term as tuple is
   inserted (deletion=false) or deleted (deletion=true) into relation relname,
   where f is used to map external named terms to their deltas.
*)
val term_delta: term_mapping_t ->
                bool -> string -> (var_t list) -> term_t -> term_t

(*  (equate_terms term_a term_b) -> Map from var names in A to var names in B
  A weak term comparison operator (may produce false negatives)
  
  Short Version: 
    - If term_a and term_b are guaranteed to be equivalent queries, return
    - Otherwise raise TermsNotEquivalent

  Long Version:
    Term equivalency is undecidable.  However, in order to identify duplicate
  maps, we need some code to at least make a halfhearted attempt at the task.
  This method does a parallel depth-first comparison of two provided terms via
  Ring.cmp_exprs.  If the comparison identifies no structural differences, the 
  method returns.  If the two terms appear to be different, throw a 
  TermsNotEquivalent.
  
    The two terms need not use the same variable names, so long as the variables
  are themselves equivalent (ie, the variables appear in the same places in both
  terms).  The return value contains a mapping from all variable names appearing
  in Term A to their corresponding variable names in Term B (including 
  identities).
  
  Known Shortcomings (ie, sources of false negatives):
    - Commutativity isn't supported yet for Add,Mult,Union,Join
    - No flipping of comparators: eg: (A = B) =/= (B = A), (B < A) =/= (A > B)
    - Double-Negation: eg: - ( - (A = B)) =/= (A = B)
    - Negated comparators: eg: - (A = B) =/= (A <> B)
    - Type Escalation: We might be able to salvage something if we have two
      maps, one of which uses Ints as inputs and the other Floats.
    - MapNames: Not necessarilly a shortcoming, but equate_terms assumes that
      there are no External references (ie, the terms are pre-compilation)
*)
exception TermsNotEquivalent of string
val equate_terms: term_t -> term_t -> (string Map.Make(String).t)

(* (fold_calc sum_f prod_f neg_f leaf_f c) scans through c and applies leaf_f
   to all leaves in c, and sum_f, prod_f, and neg_f recursively to the results
   according to the calculus parse tree, respectively at the union, nat join
   and negation nodes
*)
val fold_calc: ('a list -> 'a) -> ('a list -> 'a) -> ('a -> 'a) -> 
               (readable_relcalc_lf_t -> 'a) -> (readable_relcalc_t) -> 'a

(* (fold_term sum_f prod_f neg_f leaf_f t) scans through c and applies leaf_f
   to all leaves in c, and sum_f, prod_f, and neg_f recursively to the results
   according to the calculus parse tree, respectively at the union, nat join
   and negation nodes
*)
val fold_term: ('a list -> 'a) -> ('a list -> 'a) -> ('a -> 'a) -> 
               (readable_term_lf_t -> 'a) -> (readable_term_t) -> 'a
