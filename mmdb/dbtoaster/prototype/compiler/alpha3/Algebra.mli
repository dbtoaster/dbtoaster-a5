(*
   This module is for representing
   * positive relational algebra expressions with implicit projection and
   * terms.

   Functions for computing delta expressions, variable substitution,
   and simplification are also provided.

   Relational algebra expressions are built from leaves, unions, and
   natural joins.  Leaves are named relations, the empty relation, the
   constant nullary singleton, and atomic constraints.

   Projections are implicitly supported.
     * We use an unordered schema model in our expressions, thus we
       do not need the projection operation for column reordering.
     * The schema of a union is the intersection of the schemas of
       the constituents.
     * Projections on the top-level are not needed because we always have
       an aggregrate on top which just ignores some columns, as if there
       were a projection.
     * projections anywhere else would only conceivable be needed for
       efficiency, and we achieve efficiency in a nonstandard ways where
       projection pushing does not matter.

   Viewed differently, the supported fragment of relational algebra is
   really positive quantifier-free first-order logic.

   Note: Not using projections has the advantage that any positive relational
   algebra expression is a semiring expression. But the implication is
   that our implementation cannot be completely encapsulated, because
   some positive relational algebra operations that project away more
   columns than needed to make the unions consistent are not expressible.

   Terms are built from variables, constants and aggregrate sums (AggSum)
   using addition and multiplication.

   AggSum(t, r), where t is a term and r is a relalg expression,
   computes the sum of the t values for each of the tuples (no duplicate
   elimination) computed by r. In the case that r is constraints-only
   (i.e., no relational atoms occur in it), AggSum(t, r) computes the
   sum of the t values for those valuations of the variables common to
   both t and r (minus those that are parameters, which would bind their
   domains to a single parameter value) which satisfy condition r.
*)

type type_t = TInt | TLong | TDouble | TString
type var_t = string * type_t (* type of variable (name and type) *)
type comp_t = Eq | Lt | Le | Neq (* comparison operations *)

type const_t = 
    | Int of int
    | Double of float
    | Long of int64
    | String of string

(* leaves of the algebra tree *)
type 'term_t generic_relalg_lf_t =
            Empty (* aka false;
                     we do not record a schema or arity for it *)
          | ConstantNullarySingleton (* aka true *)
          | AtomicConstraint of comp_t * 'term_t * 'term_t
          | Rel of string * (var_t list)
            (* there are no column names; we always automatically
               bind variables to the column positions of a relation. *)

type ('term_t, 'relalg_t) generic_term_lf_t =
            AggSum of ('term_t * 'relalg_t)
          | Const of const_t
          | Var of var_t





(* algebra expressions. This is the main type of this module, for the
   things we manipulate here. *)
type relalg_t                 (* abstract. Hides how it is implemented. *)
type relalg_lf_t
type term_t
type term_lf_t





(* This is CK's way of hiding how we deal with everything that is not a
   union or natural join in this module. Right now the encapsulation is
   not finished because relalg_lf_t is visible from the outside. *)


type readable_relalg_lf_t = readable_term_t generic_relalg_lf_t
and  readable_relalg_t    = RA_Leaf         of readable_relalg_lf_t
                          | RA_MultiUnion   of readable_relalg_t list
                          | RA_MultiNatJoin of readable_relalg_t list
and  readable_term_lf_t   =
         (readable_term_t, readable_relalg_t) generic_term_lf_t
and  readable_term_t      = RVal    of readable_term_lf_t
                          | RProd   of readable_term_t list
                          | RSum    of readable_term_t list


(* back an forth between relalg_t and readable_relalg_t *)
val readable_relalg:  relalg_t -> readable_relalg_t
val make_relalg:      readable_relalg_t -> relalg_t

val readable_term: term_t -> readable_term_t
val make_term:     readable_term_t -> term_t

(* conditional aggregate construction *)
val mk_cond_agg : readable_term_t * readable_relalg_t option
    -> readable_term_t

(* expression manipulation *)
val fold_relalg:
    ('a list -> 'a) -> ('a list -> 'a) ->
    (readable_relalg_lf_t -> 'a) ->
    readable_relalg_t -> 'a

val fold_term:
    ('a list -> 'a) -> ('a list -> 'a) ->
    (readable_term_lf_t -> 'a) ->
    readable_term_t -> 'a

(* all the variables that occur in the expression resp. term. *)
val relalg_vars: relalg_t -> var_t list
val term_vars: term_t -> var_t list

val free_relalg_vars : relalg_t -> var_t list
val free_term_vars : term_t -> var_t list

(* output relalg or term as string; replace certain nested terms by
                                    named map accesses. *)
val type_as_string: type_t -> string
val relalg_as_string: relalg_t -> ((term_t * string) list) -> string
val term_as_string:   term_t   -> ((term_t * string) list) -> string


(*
module type RelAlg =
sig
*)

val relalg_one:  relalg_t
val relalg_zero: relalg_t


(* (delta "R" t e) returns the delta on insertion of tuple t into relation R,
   for relalg expression e. *)
val relalg_delta: (term_t -> term_t) -> string -> (var_t list) -> relalg_t
    -> relalg_t

(* turns an expression into a union of conjunctive queries (i.e., joins);
   or something strictly simpler, i.e., a flat union, a flat join, or a leaf.

   In the first-order logic view, we create a dnf.
*)
val polynomial: relalg_t -> relalg_t


(* return polynomial as list of monomials *)
val monomials: relalg_t -> (relalg_t list)

(* Are all the leaves of the given relational algebra
   expression constraints?
   That is, can it be turned into an equivalent query
   Select_{<constraints>}(Constant_Nullary_Singleton) ?
   If so, this can be done with extract_substitutions.
*)
val constraints_only: relalg_t -> bool

(* Complements a constraint-only relalg expressions *)
val complement : relalg_t -> relalg_t

(* returns whether there are any AggSums in the term *)
val has_aggregates : term_t -> bool

(* (apply_variable_substitution theta e) substitutes variables in
    relational algebra expression e according to theta. *)
val apply_variable_substitution_to_relalg: ((var_t * var_t) list)
                                 -> relalg_t -> relalg_t


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

   Update: the relational algebra expression does not have to be
   a monomial, but if it isn't, the function works with the
   topmost product of the expression.
*)
val extract_substitutions: relalg_t -> (var_t list) ->
   (((var_t * var_t) list) * relalg_t)

(*
end (* module type RelAlg *)
*)


(*
module type Term =
sig
*)

val term_zero: term_t
val term_one:  term_t

val negate_term: term_t -> term_t

(* (delta relname tuple term) computes the delta of term as tuple is
   inserted into relation relname.
*)
val term_delta: (term_t -> term_t) -> string -> (var_t list) -> term_t
    -> term_t

(* a pudding. Here: recursively turning a map algebra expression into a
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
   algebra subexpressions.
*)
val apply_variable_substitution_to_term:
    ((var_t * var_t) list) -> term_t -> term_t

(* a list consisting of the maximal AggSum subexpressions of the input
   map algebra expression.
*)
val extract_aggregates: term_t -> (term_t list)

val flatten_term : var_t list -> readable_term_t
    -> ((readable_term_t * readable_relalg_t option) *
        (var_t * var_t * readable_relalg_lf_t) list)

(*
end (* module type Term *)
*)


(* type inference *)
val relalg_schema: readable_relalg_t -> var_t list

(* returns type of a readable_term_t, using the given list of
 * variables defined outside *)
val term_type: readable_term_t -> var_t list -> type_t
