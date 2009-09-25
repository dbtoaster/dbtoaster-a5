
type mapalg_t
type mapalg_lf_t

val zero: mapalg_t


type readable_mapalg_lf_t = RConst of int
                          | RVar of string
                          | RAggSum of readable_mapalg_t
                                     * RelAlg.readable_relalg_t
and  readable_mapalg_t    = RVal  of readable_mapalg_lf_t
                          | RProd of readable_mapalg_t list
                          | RSum  of readable_mapalg_t list

val readable: mapalg_t -> readable_mapalg_t
val make:     readable_mapalg_t -> mapalg_t
val rfold: ('b list -> 'b) -> ('b list -> 'b) ->
           (readable_mapalg_lf_t -> 'b) -> readable_mapalg_t -> 'b


(* (delta relname tuple mapalg) computes the delta of mapalg as tuple is
   inserted into relation relname.
*)
val delta: string -> (string list) -> mapalg_t -> mapalg_t

(* a pudding. Here: recursively turning a map algebra expression into a
   polynomial: The result does not use union anywhere, and sum is only used
   on the very top level of the expression.

   Also factorizes AggSum monomials, i.e. turns e.g.
   sum{A*B}(R bowtie S) for schema R(A), S(B) into sum{A}(R) * sum{B}(S).
*)
val roly_poly: mapalg_t -> mapalg_t

(* calls roly_poly. Then, for each (possibly nested) monomial -- i.e., sum
   and union-free subexpression -- the function unifies variables as
   much as possible, but does not rename the variables in the second arguments
   (the "bound variables"). Also applies the substitution computed to the
   variables given in the third argument (the parameters). Returns
   a pair consisting of the renamed parameters and the list of simplified
   nested monomials.
*)
val simplify: mapalg_t -> (string list) -> (string list) ->
                          ((string list * mapalg_t) list)

(* all the variables that occur in the expression *)
val vars: mapalg_t -> string list

(* (apply_variable_substitution theta mapalg) substitutes variables anywhere
   in mapalg using mapping theta, recursively. This includes relational
   algebra subexpressions.
*)
val apply_variable_substitution: ((string * string) list) ->
                                 mapalg_t -> mapalg_t

(* a list consisting of the maximal AggSum subexpressions of the input
   map algebra expression.
*)
val extract_aggregates: mapalg_t -> (mapalg_t list)


