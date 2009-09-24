(* types *)

type var_t = string (* type of variable name *)
type comp_t = Eq | Lt | Le | Neq

type relalg_lf_t = Empty (* we do not record a schema or arity for it *)
                 | ConstantNullarySingleton
                 | AtomicConstraint of comp_t * var_t * var_t
                            (* a comparison operation plus two column names *)
                 | Rel of string * (var_t list)


module REL_BASE =   (*: SemiRing.Base = *)
struct
   type t = relalg_lf_t

   let  zero = Empty
   let  one  = ConstantNullarySingleton
end

module RelSemiRing = SemiRing.Make(REL_BASE)


type relalg_t = RelSemiRing.expr_t

type readable_relalg_lf_t = relalg_lf_t
type readable_relalg_t = RA_Leaf         of readable_relalg_lf_t
                       | RA_MultiUnion   of readable_relalg_t list
                       | RA_MultiNatJoin of readable_relalg_t list





(* functions *)

open REL_BASE


let readable (relalg: RelSemiRing.expr_t) =
   RelSemiRing.fold (fun x -> RA_MultiUnion x)
                    (fun y -> RA_MultiNatJoin y)
                    (fun z -> RA_Leaf z)
                    relalg

let rec make readable_relalg =
   match readable_relalg with
      RA_Leaf(x)         -> RelSemiRing.mk_val(x)
    | RA_MultiUnion(l)   -> RelSemiRing.mk_sum  (List.map make l)
    | RA_MultiNatJoin(l) -> RelSemiRing.mk_prod (List.map make l)


let lf_schema x =
   match x with
      Empty    -> [] (* to check consistency of unions, ignore relational
                        algebra expressions that are equivalent to empty *)
    | ConstantNullarySingleton -> []
    | Rel(n,s) -> Util.ListAsSet.no_duplicates s
    | AtomicConstraint (_, c1, c2) -> Util.ListAsSet.no_duplicates [c1; c2]


(* There can never be schema inconsistencies.
*)
let schema (relalg: RelSemiRing.expr_t) =
   let  sumf l = List.fold_left Util.ListAsSet.inter [] l
                 (* we perform implicit projections: the schema is the
                    intersection of the schemas of the constituents of the sum.
                 *)
   in
   RelSemiRing.fold sumf Util.ListAsSet.multiunion lf_schema relalg

let vars relalg = RelSemiRing.fold Util.ListAsSet.multiunion
                                   Util.ListAsSet.multiunion
                                   lf_schema
                                   relalg



let delta_leaf relname tuple (lf: RelSemiRing.leaf_t) = match lf with
   Empty -> RelSemiRing.mk_val(Empty)
 | Rel(x, l) when relname = x ->
      let f (x,y) = RelSemiRing.mk_val(AtomicConstraint(Eq, x,y))
      in
      RelSemiRing.mk_prod (List.map f (List.combine l tuple))
 | Rel(x, l) -> RelSemiRing.mk_val(Empty)
 | ConstantNullarySingleton -> RelSemiRing.mk_val(Empty)
 | AtomicConstraint(_) -> RelSemiRing.mk_val(Empty)

let delta relname tuple relalg =
   RelSemiRing.delta (delta_leaf relname tuple) relalg


let polynomial (q: RelSemiRing.expr_t) = RelSemiRing.polynomial q

let monomials (q: relalg_t) =
   let p = RelSemiRing.polynomial q in
   if (p = RelSemiRing.zero) then []
   else RelSemiRing.sum_list p

let one  = RelSemiRing.mk_val(one)
let zero = RelSemiRing.mk_val(zero)



let constraints_only (r: relalg_t) =
   let leaves = RelSemiRing.fold List.flatten List.flatten (fun x -> [x]) r
   in
   let bad x = match x with
         Rel(_) -> true
       | Empty  -> true
       | ConstantNullarySingleton -> false
       | AtomicConstraint(_,_,_) -> false
   in
   (List.length (List.filter bad leaves) = 0)


let monomial_as_hypergraph (monomial: relalg_t): (relalg_t list) =
   RelSemiRing.prod_list monomial

let hypergraph_as_monomial (hypergraph: relalg_t list): relalg_t =
   RelSemiRing.mk_prod hypergraph



let split_off_equalities (monomial: relalg_t) :
      (((var_t * var_t) list) * relalg_t) =
   let atoms = RelSemiRing.prod_list monomial
   in
   let split0 lf =
      match lf with
         RelSemiRing.Val(AtomicConstraint(Eq, x, y)) -> ([(x, y)], [])
       | _                                           -> ([],       [lf])
   in
   let (eqs, rest) = (List.split (List.map split0 atoms))
   in
   (List.flatten eqs, RelSemiRing.mk_prod(List.flatten rest))


type substitution_t = var_t Util.Vars.substitution_t
                      (* (var_t * var_t) list *)


let apply_variable_substitution (theta: substitution_t) (alg: relalg_t):
      relalg_t =
   let substitute_leaf lf =
      match lf with
         Rel(n, vars) ->
            RelSemiRing.mk_val (Rel(n, Util.Vars.apply_mapping theta vars))
       | AtomicConstraint(comp, x, y) ->
            let x2 = List.hd (Util.Vars.apply_mapping theta [x]) in
            let y2 = List.hd (Util.Vars.apply_mapping theta [y]) in
            (RelSemiRing.mk_val (AtomicConstraint(comp, x2, y2)))
       | _ -> (RelSemiRing.mk_val lf)
   in
   (RelSemiRing.apply_to_leaves substitute_leaf alg)



(* TODO: if at least two distinct bound vars occur in a single component
   (this happens in the delta of a self-join), then we cannot unify these
    variables but have to keep an equality condition around to check
    whether they are equal in the input.
*)
let extract_substitutions (monomial: relalg_t) (bound_vars: var_t list):
      (substitution_t * relalg_t) =
   let (eqs, rest) = split_off_equalities monomial
   in
   let substitutions =
      (List.flatten
         (List.map (fun comp -> (Util.Vars.unifier comp bound_vars))
            (Util.Vars.equivalence_classes eqs)))
   in
   let rest2 = apply_variable_substitution substitutions rest
   in
   (substitutions, rest2)




