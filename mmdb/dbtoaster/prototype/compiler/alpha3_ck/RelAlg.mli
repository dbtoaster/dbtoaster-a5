(*
   (This is really a module for representing positive quantifier-free
   first-order logic, but we use it for:)

   Positive relational algebra expressions and their simplification.
   (The difference operation is not supported.)

   Expressions have input variables (parameters / bound variables)
   and output variables (the result columns).

   Aggregation is covered elsewhere, in the map algebra.

   Expressions are built from leaves, unions, and natural joins.
   Leaves are named relations, the empty relation, the constant nullary
   singleton, and atomic constraints.

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

   TODO: Not using projections has the advantage that any positive relational
   algebra expression is a semiring expression. But the implication is
   that our implementation cannot be completely encapsulated, because
   some positive relational algebra operations that project away more
   columns than needed to make the unions consistent are not expressible.
*)


type var_t = string (* type of variable (name) *)
type comp_t = Eq | Lt | Le | Neq

(* leaves of the algebra tree *)
type relalg_lf_t =
   Empty    (* we do not record a schema or arity for it *)
 | ConstantNullarySingleton (* aka "true" *)
 | AtomicConstraint of comp_t * var_t * var_t
            (* a comparison operation plus two var names *)
 | Rel of string * (var_t list)
            (* there are no column names; we always automatically
               bind variables to the column positions of a relation. *)


(* TODO: relalg_lf_t should be made abstract to hide how we implement
   selections/constraints and optimizations via Empty/ConstantNullarySingleton.
   But this will mean that we have to extend readable_relalg_t and the
   code for translating back and forth between readable_relalg_t and relalg_t.
*)


(* algebra expressions. This is the main type of this module, for the
   things we manipulate here. *)
type relalg_t                 (* abstract. Hides how it is implemented. *)

val one:  relalg_t
val zero: relalg_t


(* This is CK's way of hiding how we deal with everything that is not a
   union or natural join in this module. Right now the encapsulation is
   not finished because relalg_lf_t is visible from the outside. *)

type readable_relalg_lf_t = relalg_lf_t  (* FIXME, once relalg_lf_t is
                                            abstract, we need a separate
                                            implementation of this type. *)
type readable_relalg_t = RA_Leaf         of readable_relalg_lf_t
                       | RA_MultiUnion   of readable_relalg_t list
                       | RA_MultiNatJoin of readable_relalg_t list
                         (* use for relational product too *)



(* back an forth between relalg_t and readable_relalg_t *)
val readable:  relalg_t -> readable_relalg_t
val make:      readable_relalg_t -> relalg_t


(* since we do not record column names, these are the variables used. *)
val lf_schema: relalg_lf_t -> string list

(* free output variables. These are not necessarily the variables we
   intended as output, but all the variables/columns we can produce.
   See discussion of implicit projection above.

   If relalg_t expressions are considered positive quantifier-free
   formulae, then schema essentially returns the range-restricted
   ("safe") variables. It only does that "essentially" because to do
   it fully, one would have to add the variables set equal to variables
   that are safe. See Abiteboul-Hull-Vianu.
*)
val schema:    relalg_t    -> string list

(* all the variables that occur in the expression. *)
val vars: relalg_t -> string list


(* (delta "R" t e) returns the delta on insertion of tuple t into relation R,
   for relalg expression e. *)
val delta:     string -> (string list) -> relalg_t -> relalg_t

(* turns an expression into a union of conjunctive queries (i.e., joins);
   or something strictly simpler, i.e., a flat union, a flat join, or a leaf.

   In the first-order logic view, we create a dnf.
*)
val polynomial: relalg_t -> relalg_t


(* return polynomial as list of monomials *)
val monomials: relalg_t -> (relalg_t list)


(* back and forth between a monomial and its hypergraph *)
val monomial_as_hypergraph: relalg_t -> (relalg_t list)
val hypergraph_as_monomial: (relalg_t list) -> relalg_t


(* Are all the leaves of the given relational algebra
   expression constraints?
   That is, can it be turned into an equivalent query
   Select_{<constraints>}(Constant_Nullary_Singleton) ?
   If so, this can be done with extract_substitutions.
*)
val constraints_only: relalg_t -> bool


(* (apply_variable_substitution theta e) substitutes variables in
    relational algebra expression e according to theta. *)
val apply_variable_substitution: ((var_t * var_t) list)
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


