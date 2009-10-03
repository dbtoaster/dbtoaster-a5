(* A semiring module for representing algebraic expressions (using
   the operations '+' and '*') over a base type, and for simplifying
   expressions. The module contains a function for turning an expression
   into a polynomial.

   There is no output method here and CK thinks no one should be added.
   In general, it depends on the application how the (debug) output
   should be generated; for example, the OCAML standard library Set(A)
   also does not know how to print itself.
*)



(* The base type t over which we define the semiring, plus its zero and one
   elements.
*) 
module type Base =
sig
  type t
  val  zero: t
  val  one:  t
end
;;


(* We build semirings using the Make functor over the Base type.
   Examples are in SemiRingTest.ml.
*)
module type SemiRing =
sig
   (* the type of base values. *)
   type leaf_t

   (* expression type. The implementation is hidden (abstract), so it
      can only be accessed through special access functions.

      Making expr_t abstract:
      Since the nesting structure of an expression in general matters to us,
      being flexible in the implementation may not be so necessary. However,
      we want the user to use mk_sum and m_prod to construct sums and
      products, rather than the Sum and Prod constructors from expr_t, because
      the mk-functions automatically eliminate certain redundancies on the
      fly that the implementations of other functions of SemiRing may
      depend upon. 
   *)
(*
   type expr_t
*)

(* currently not abstract, for debugging in the interactive ocaml toplevel *)
   type expr_t = Val  of leaf_t
               | Sum  of expr_t list
               | Prod of expr_t list

   type mono_t = (leaf_t list)


   (* the zero- and one-elements of the semiring *)
   val zero: expr_t
   val one:  expr_t


   (* constructing a (nested) expression using base values,
      sums, and products of expressions *)

   (* turns a value of type T.t into an expression (consisting just of
      that value) *)
   val mk_val: leaf_t -> expr_t

   (* turns a list l of expressions into \bigsum l and \bigprod l, resp. *)
   val mk_sum:  (expr_t list) -> expr_t
   val mk_prod: (expr_t list) -> expr_t


   (* accessing the contents of an expression *)
   exception NotAValException
   val get_val: expr_t -> leaf_t

   (* returns a list l of expressions such that (mk_sum l) resp. (mk_prod l)
      are equivalent to the input.

      Applied to a polynomial, sum_list returns the list of monomials.
      Applied to a monomial, prod_list returns the list of the constituent
      base values. But both functions can be applied to any expression. *)
   val  sum_list: expr_t -> (expr_t list)
   val prod_list: expr_t -> (expr_t list)


   (* some functions for constructing modified expressions *)

   (* (fold sum_f prod_f leaf_f e) folds the expression by proceeding
      bottom-up, applying
      leaf_f to leaves;
      sum_f  to the fold result of the constituents of sums; and
      prod_f to the fold result of the constituents of products.

      For example, (apply_to_leaves f e) below can be implemented as

      (fold (fun x -> mk_sum x) (fun x -> mk_prod x) (fun x -> (f x)) e)
   *)
   val fold: ('b list -> 'b) -> ('b list -> 'b) -> (leaf_t -> 'b) ->
             expr_t -> 'b


   (* (apply_to_leaves f e)  applies function f to each base value leaf of
      expression e, i.e., replaces the base value v by expression (f v) *)
   val apply_to_leaves: (leaf_t -> expr_t) -> expr_t -> expr_t

   (* (substitute f e1 e2 e3) replaces each occurrence of e1 in e3 by e2. *)
   val substitute: expr_t -> (* replace_this *)
                   expr_t -> (* by_that *)
                   expr_t -> (* in_expr *)
                   expr_t    (* returns expr with substitutions applied *)

   (* make a set of substitutions given by a list of
     (replace_this by_that) pairs. The order in which the replacements are
     made is arbitrary, so you must not have dependent chain of replacements
     such as in (substitute_many [(a,b), (b,c)] e); you must not assume that
     this will replace a by c in e. There should not be any overlap between
     the domain and the image of the mapping specified by l.
   *)
   val substitute_many: ((expr_t * expr_t) list) -> expr_t -> expr_t

   (* (delta f e) returns an expression equivalent to

      (apply_to_leaves (fun x -> mk_sum [(mk_val x); f x]) e) - e

      but simpler, by exploiting linearity and something related to
      the product rule of differentiation in calculus.

      That is, if (f x) expresses change to a leaf base value x of the
      expression e, then (delta f e) expresses the overall change to e.
   *)
   val delta: (leaf_t -> expr_t) -> expr_t -> expr_t

   (* a polynomial is a sum of monomials.
      a monomial is a product of base values *)
   val polynomial: expr_t -> expr_t

   (* computes a polynomial equivalent to the input but expresses it as
      a list of monomials. *)
   val monomials: expr_t -> (mono_t list)

   (* simplifies expressions by unnesting sums of sums and products
      of products, and simplifies using ones and zeros.
      The only real difference to the function polynomial is that
      products of sums are not turned into sums of products using
      distributivity. Thus, while polynomial may create exponential-factor
      blow-up in the worst case, simplify is always polynomial-time.
   *)
   val simplify: expr_t -> expr_t
end




(* For using SemiRing, there should be no need to read on below.
   The following code implements a semiring with some standard operations
   on expressions; there is nothing specific to DBToaster in here.
*)


module Make = functor (T : Base) ->
struct
   type leaf_t = T.t

   type expr_t = Val of leaf_t
               | Sum  of expr_t list
               | Prod of expr_t list

   type mono_t = (leaf_t list)

   let zero = Val(T.zero)  (* Sum [] *)
   let one  = Val(T.one)   (* Prod [] *)

   let mk_val a = Val(a)

   let mk_sum  l      =
      let l2 = (List.filter (fun x -> x <> zero) l)
      in
      if(l2 = []) then zero
      else if (List.tl l2) = [] then (List.hd l2)
      else Sum(l2)

   let mk_prod l =
      let zeroes = (List.filter (fun x -> x = zero) l)
      in
      if (zeroes <> []) then zero
      else
      let l2 = (List.filter (fun x -> x <> one) l)
      in
      if (l2 = []) then one
      else if ((List.tl l2) = []) then List.hd l2
      else Prod(l2)

   exception NotAValException
   let get_val e =
      match e with
         Val(x) -> x
       | _ -> raise NotAValException

   let  sum_list e = match e with  Sum(l) -> l | _ -> [e]
   let prod_list e = match e with Prod(l) -> l | _ -> [e]

   let rec fold ( sum_f: 'b list -> 'b)
                (prod_f: 'b list -> 'b)
                (leaf_f: leaf_t -> 'b)
                (e: expr_t) =
      match e with
         Sum(l)  ->  sum_f(List.map (fold sum_f prod_f leaf_f) l)
       | Prod(l) -> prod_f(List.map (fold sum_f prod_f leaf_f) l)
       | Val(x)  -> leaf_f x

   let rec apply_to_leaves (f: leaf_t -> expr_t) (e: expr_t) =
      fold (fun x -> mk_sum x) (fun x -> mk_prod x) (fun x -> (f x)) e

   let rec substitute replace_this by_that in_expr =
      match in_expr with
         x when (x = replace_this) -> by_that
       | Sum(l)  -> mk_sum (List.map (substitute replace_this by_that) l)
       | Prod(l) -> mk_prod(List.map (substitute replace_this by_that) l)
       | Val(x)  -> Val(x)

   let substitute_many l in_expr =
       List.fold_right (fun (x,y) -> (substitute x y)) l in_expr

   let rec delta (lf_delta: leaf_t -> expr_t) (e: expr_t) =
      match e with
         Sum(l)   -> mk_sum(List.map (fun x -> delta lf_delta x) l)
       | Prod([]) -> zero
       | Prod(x::l) ->
         mk_sum([
            mk_prod((delta lf_delta x)::l);
            mk_prod([mk_sum[x; (delta lf_delta x)];
                     (delta lf_delta (mk_prod(l)))])
            (* this nesting makes sense because it renders the delta
               computation cheaper: delta for l is only computed once here;
               we can still distribute later if we need it. *)
         ])
       | Val(x) -> lf_delta x

   (* create a flat sum of flat products of constants, vars, and
      query aggregates *)
   let rec polynomial (e: expr_t) =
      let mk_flat_sum x = sum_list (polynomial x)
      in
      match e with
         Sum(l) ->
            mk_sum (List.flatten (List.map mk_flat_sum l))
       | Prod(l) ->
            let mk_flat_prod x = mk_prod (List.flatten (List.map prod_list x))
            in
            mk_sum (List.map mk_flat_prod
                       (Util.ListAsSet.distribute (List.map mk_flat_sum l)))
       | _ -> e

   let monomials (e: expr_t) =
      let p = polynomial e
      in
      if (p = zero) then []
      else
         List.map (fun x -> List.map get_val (prod_list x))
                  (sum_list (polynomial e))

   let simplify (e: expr_t) =
      apply_to_leaves (fun x -> mk_val x) e
end
