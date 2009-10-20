(*
   This module is for representing

      * formulae of positive quantifier-free relational calculus/first-order
        logic (i.e., formulae constructed from conjunctions "and",
        disjunctions "or", and atoms) and

      * terms.

   The atomic formulae are relational atoms, false, true,
   and atomic constraints comparing terms using =, neq, <, and <=.

   Terms are built from variables, constants and aggregrate sums (AggSum)
   using addition and multiplication.

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


type var_t   = string                         (* type of variables *)
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




(* The following types are abstract; their implementation is hidden. *)

type relcalc_t                 (* calculus formulae *)
type relcalc_lf_t
type term_t
type term_lf_t





(* Auxiliary types for constructing and accessing values of the abstract
   types.

   TODO: RA_MultiUnion and RA_MultiNatJoin are still concepts of relational
   algebra rather than calculus; we should eventually change this to
   bigand and bigor. *)
type readable_relcalc_lf_t = readable_term_t generic_relcalc_lf_t
and  readable_relcalc_t    = RA_Leaf         of readable_relcalc_lf_t
                           | RA_MultiUnion   of readable_relcalc_t list
                           | RA_MultiNatJoin of readable_relcalc_t list
and  readable_term_lf_t   =
         (readable_term_t, readable_relcalc_t) generic_term_lf_t
and  readable_term_t      = RVal    of readable_term_lf_t
                          | RProd   of readable_term_t list
                          | RSum    of readable_term_t list


(* back an forth between relcalc_t and readable_relcalc_t *)
val readable_relcalc:  relcalc_t -> readable_relcalc_t
val make_relcalc:      readable_relcalc_t -> relcalc_t

val readable_term: term_t -> readable_term_t
val make_term:     readable_term_t -> term_t



(* all the variables that occur in the formula resp. term. *)
val relcalc_vars: relcalc_t -> string list
val term_vars:       term_t -> string list


(* set of safe variables of a formula; a formula phi is range-restricted
   given a set of bound variables (which are treated like constants, i.e.,
   are safe) iff all free variables are in (safe_vars phi bound_vars).
   The result contains all the param_vars.
*)
val safe_vars: relcalc_t -> (var_t list) -> (var_t list)



(* output relcalc or term as string; replace certain nested terms by
                                    named map accesses. *)
val relcalc_as_string: relcalc_t -> ((term_t * string) list) -> string
val term_as_string:    term_t    -> ((term_t * string) list) -> string


(*
module type Formula = sig
*)

val relcalc_one:  relcalc_t
val relcalc_zero: relcalc_t


(* (delta n "R" t e) returns the delta on insertion (n=false) or
   deletion (n=true) of tuple t into relation R, for relcalc expression e. *)
val relcalc_delta: bool -> string -> (string list) -> relcalc_t -> relcalc_t

(* turns an expression into a union of conjunctive queries (i.e., joins);
   or something strictly simpler, i.e., a flat union, a flat join, or a leaf.

   In the first-order logic view, we create a dnf.
*)
val polynomial: relcalc_t -> relcalc_t


(* return polynomial as list of monomials *)
val monomials: relcalc_t -> (relcalc_t list)


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


(* Given monomial m that is constraints-only and a list of variables l,
   (extract_bindings m preferred_vars) returns a pair of

   * a map f for unifying variables based on the equality constraints in m,
     prioritizing to take an element from preferred_vars for each
     equivalence class of variables in m.

   * the remaining constraints (which are not of type Eq).

   Note: the result map is not necessarily complete;
   for the variables and columns
   on which it is not defined, it is the identity. Thus, if this
   map is to be applied to a SemiRing, use substitute_many.

   Update: the relational calculus expression does not have to be
   a monomial, but if it isn't, the function works with the
   topmost product of the expression.
*)
val extract_substitutions: relcalc_t -> (var_t list) ->
   (((var_t * var_t) list) * relcalc_t)

(*
end (* module type Formula *)

module type Term = sig
*)

val term_zero: term_t
val term_one:  term_t

(* (delta deletion relname tuple term) computes the delta of term as tuple is
   inserted (deletion=false) or deleted (deletion=true) into relation relname.
*)
val term_delta: bool -> string -> (string list) -> term_t -> term_t

(* a pudding. Here: recursively turning a term into a
   polynomial: The result does not use union anywhere, and sum is only used
   on the very top level of the expression.

   Also factorizes AggSum monomials, i.e. turns e.g.
   sum{A*B}(R bowtie S) for schema R(A), S(B) into sum{A}(R) * sum{B}(S).
*)
val roly_poly: term_t -> term_t

(* calls roly_poly. Then, for each (possibly nested) monomial -- i.e., sum
   and union-free subexpression -- the function unifies variables as
   much as possible, but does not rename the variables in the second arguments
   (the "bound variables"). Also applies the substitution computed to the
   variables given in the third argument (the parameters). Returns
   a pair consisting of the renamed parameters and the list of simplified
   nested monomials.
*)
val simplify: term_t -> (var_t list) -> (var_t list) ->
                        ((var_t list * term_t) list)

(* (apply_variable_substitution theta term) substitutes variables anywhere
   in term using mapping theta, recursively. This includes relational
   calculus subexpressions.
*)
val apply_variable_substitution_to_term: ((string * string) list) ->
                                         term_t -> term_t

(* a list consisting of the maximal AggSum subexpressions of the input term.  *)
val extract_aggregates: term_t -> (term_t list)

(*
end (* module type Term *)
*)

