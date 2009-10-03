
(* types *)

type var_t = string (* type of variable name *)
type comp_t = Eq | Lt | Le | Neq
type const_t = Int of int | String of string

type 'term_t generic_relalg_lf_t =
            Empty (* aka false;
                     we do not record a schema or arity for it *)
          | ConstantNullarySingleton (* aka true *)
          | AtomicConstraint of comp_t * 'term_t * 'term_t
          | Rel of string * (var_t list)

type ('term_t, 'relalg_t) generic_term_lf_t =
            AggSum of ('term_t * 'relalg_t)
          | Const of const_t
          | Var of string


module rec REL_BASE :
sig
   type t    = TermSemiRing.expr_t generic_relalg_lf_t
   val  zero : t
   val  one  : t
end =
struct
   type t    = TermSemiRing.expr_t generic_relalg_lf_t
   let  zero = Empty
   let  one  = ConstantNullarySingleton
end
and RelSemiRing : SemiRing.SemiRing
                  with type leaf_t = REL_BASE.t
    = SemiRing.Make(REL_BASE)
and TERM_BASE :
sig
   type t    = (TermSemiRing.expr_t, RelSemiRing.expr_t) generic_term_lf_t
   val  zero : t
   val  one  : t
end =
struct
   type t    = (TermSemiRing.expr_t, RelSemiRing.expr_t) generic_term_lf_t
   let  zero = Const(Int(0))  (* I think I mean this, even if we want to *)
   let  one  = Const(Int(1))  (* support floating point numbers.
                                 The consequence is that some optimizations
                                 will not apply for AggSum(1.0, ...). *)
end
and TermSemiRing : SemiRing.SemiRing
                 with type leaf_t = TERM_BASE.t
                 (* makes leaf_t visible to the outside *)
    = SemiRing.Make(TERM_BASE)


type relalg_lf_t = RelSemiRing.leaf_t
type relalg_t    = RelSemiRing.expr_t
type term_lf_t   = TermSemiRing.leaf_t
type term_t      = TermSemiRing.expr_t





(* accessing relational algebra expressions and terms once the above types
   are made abstract. *)


type readable_relalg_lf_t = readable_term_t generic_relalg_lf_t
and  readable_relalg_t    = RA_Leaf         of readable_relalg_lf_t
                          | RA_MultiUnion   of readable_relalg_t list
                          | RA_MultiNatJoin of readable_relalg_t list
and  readable_term_lf_t   =
         (readable_term_t, readable_relalg_t) generic_term_lf_t
and  readable_term_t      = RVal    of readable_term_lf_t
                          | RProd   of readable_term_t list
                          | RSum    of readable_term_t list


let rec readable_relalg (relalg: relalg_t): readable_relalg_t =
   let lf_readable (lf: relalg_lf_t): readable_relalg_lf_t =
      match lf with
         AtomicConstraint(comp, x, y) ->
            AtomicConstraint(comp, (readable_term x), (readable_term y))
       | Rel(r, sch)              -> Rel(r, sch)
       | Empty                    -> Empty
       | ConstantNullarySingleton -> ConstantNullarySingleton
   in
   RelSemiRing.fold (fun x -> RA_MultiUnion x)
                    (fun y -> RA_MultiNatJoin y)
                    (fun z -> RA_Leaf(lf_readable z))
                    relalg

and readable_term (term: term_t): readable_term_t =
   let lf_readable lf =
      match lf with
         Const(x)     -> Const(x)
       | Var(x)       -> Var(x)
       | AggSum(f, r) -> AggSum(readable_term f, readable_relalg r)
   in
   TermSemiRing.fold (fun l -> RSum l) (fun l -> RProd l)
                   (fun x -> RVal (lf_readable x)) term



let rec make_relalg readable_ra =
   let lf_make lf =
      match lf with
         AtomicConstraint(comp, x, y) ->
            AtomicConstraint(comp, (make_term x), (make_term y))
       | Rel(r, sch) -> Rel(r, sch)
       | Empty -> Empty
       | ConstantNullarySingleton -> ConstantNullarySingleton
   in
   match readable_ra with
      RA_Leaf(x)         -> RelSemiRing.mk_val(lf_make x)
    | RA_MultiUnion(l)   -> RelSemiRing.mk_sum  (List.map make_relalg l)
    | RA_MultiNatJoin(l) -> RelSemiRing.mk_prod (List.map make_relalg l)

and make_term readable_term =
   let lf_make lf =
      match lf with
         Const(x)    -> Const(x)
       | Var(x)      -> Var(x)
       | AggSum(f,r) -> AggSum(make_term f, make_relalg r)
   in
   match readable_term with
      RSum(x)  -> TermSemiRing.mk_sum( List.map make_term x)
    | RProd(x) -> TermSemiRing.mk_prod(List.map make_term x)
    | RVal(x)  -> TermSemiRing.mk_val(lf_make x)




(* functions *)

let rec relalg_vars relalg: var_t list =
   let lf_vars lf =
      match lf with
         Empty    -> []
                        (* to check consistency of unions, ignore relational
                           algebra expressions that are equivalent to empty *)
       | ConstantNullarySingleton -> []
       | Rel(n,s) -> Util.ListAsSet.no_duplicates s
       | AtomicConstraint (_, c1, c2) ->
            Util.ListAsSet.union (term_vars c1) (term_vars c2)
   in
   RelSemiRing.fold Util.ListAsSet.multiunion
                    Util.ListAsSet.multiunion lf_vars relalg

and term_vars term: var_t list =
   let leaf_f x = match x with
      Var(y) -> [y]
    | AggSum(f, r) -> Util.ListAsSet.union (term_vars f) (relalg_vars r)
    | _ -> []
   in
   TermSemiRing.fold Util.ListAsSet.multiunion
                     Util.ListAsSet.multiunion leaf_f term




(* TODO: enforce that the variables occurring in f are among the
   range-restricted variables of r. Otherwise throw exception. *)
let make_aggsum f r =
   if (r = RelSemiRing.zero) then TermSemiRing.zero
   else if (f = TermSemiRing.zero) then TermSemiRing.zero
   else if (r = RelSemiRing.one) then f
   else TermSemiRing.mk_val (AggSum (f, r))

(*
let make_aggsum f r = TermSemiRing.mk_val (AggSum (f, r))
*)


let constraints_only (r: relalg_t): bool =
   let leaves = RelSemiRing.fold List.flatten List.flatten (fun x -> [x]) r
   in
   let bad x = match x with
         Rel(_) -> true
       | Empty  -> true
       | ConstantNullarySingleton -> false
       | AtomicConstraint(_,_,_) -> false
   in
   (List.length (List.filter bad leaves) = 0)


exception Assert0Exception (* should never be reached *)

(* auxiliary: complement a constraint-only relalg expressions *)
let complement relalg =
   let leaff lf =
      match lf with
         AtomicConstraint(comp, t1, t2) ->
            RelSemiRing.mk_val(AtomicConstraint(
               (match comp with Eq -> Neq | Neq -> Eq
                              | Lt  -> Le | Le  -> Lt), t2, t1))
       | ConstantNullarySingleton -> RelSemiRing.mk_val(Empty)
       | Empty -> RelSemiRing.mk_val(ConstantNullarySingleton)
       | _ -> raise Assert0Exception
   in
   (* switch prod and sum *)
   (RelSemiRing.fold RelSemiRing.mk_prod RelSemiRing.mk_sum leaff relalg)




let rec relalg_delta relname (tuple: var_t list) (relalg: relalg_t) =
   let delta_leaf lf =
      match lf with
         Empty -> RelSemiRing.zero
       | Rel(r, l) when relname = r ->
            let f (x,y) = RelSemiRing.mk_val(
                             AtomicConstraint(Eq, TermSemiRing.mk_val(Var(x)),
                                                  TermSemiRing.mk_val(Var(y))))
            in
            RelSemiRing.mk_prod (List.map f (List.combine l tuple))
       | Rel(x, l) -> RelSemiRing.zero
       | ConstantNullarySingleton -> RelSemiRing.zero
       | AtomicConstraint(comp, t1, t2) ->
            if(((term_delta relname tuple t1) = TermSemiRing.zero) &&
               ((term_delta relname tuple t2) = TermSemiRing.zero)) then
                  RelSemiRing.zero
            else raise Assert0Exception
                 (* the terms with nonzero delta should have been pulled
                    out of the constraint elsewhere. *)
   in
   RelSemiRing.delta delta_leaf relalg

and term_delta relname tuple (term: term_t) =
   let rec leaf_delta lf =
      match lf with
         Const(c) -> TermSemiRing.zero
       | Var(x)   -> TermSemiRing.zero
       | AggSum(f, r) ->
            let d_f = (term_delta relname tuple f)
            in
            if (constraints_only r) then
               (* FIXME: this is overkill (but supposedly correct) in the
                  case that r does not contain AggSum terms, i.e., in the
                  case that all contained terms have zero delta. *)
               let new_r = (* replace each term t in r by (t + delta t) *)
                  let leaf_f lf =
                     (match lf with
                        AtomicConstraint(c, t1, t2) ->
                        RelSemiRing.mk_val(AtomicConstraint(c,
                           TermSemiRing.mk_sum [t1;
                              (term_delta relname tuple t1)],
                           TermSemiRing.mk_sum [t2;
                              (term_delta relname tuple t2)]))
                      | ConstantNullarySingleton ->
                        RelSemiRing.mk_val(ConstantNullarySingleton)
                      | _ -> raise Assert0Exception)
                  in
                  RelSemiRing.apply_to_leaves leaf_f r
               in
               (*
                  (if (     new_r             ) then (delta f) else 0)
                + (if (     new_r  and (not r)) then        f  else 0)
                - (if ((not new_r) and      r ) then        f  else 0)
               *)
               TermSemiRing.mk_sum [
                  make_aggsum d_f new_r;
                  make_aggsum f (RelSemiRing.mk_prod [new_r; (complement r)]);
                  make_aggsum (TermSemiRing.mk_prod[
                                  TermSemiRing.mk_val(Const(Int (-1))); f])
                              (RelSemiRing.mk_prod [ (complement new_r); r ])
               ]
            else
               let d_r = (relalg_delta relname tuple r)
               in
               TermSemiRing.mk_sum [ (make_aggsum d_f   r);
                                     (make_aggsum   f d_r);
                                     (make_aggsum d_f d_r) ]
   in
   TermSemiRing.delta leaf_delta term




type substitution_t = var_t Util.Vars.mapping_t
                      (* (var_t * var_t) list *)


let rec apply_variable_substitution_to_relalg (theta: substitution_t)
                                              (alg: relalg_t): relalg_t =
   let substitute_leaf lf =
      match lf with
         Rel(n, vars) ->
            RelSemiRing.mk_val
               (Rel(n, List.map (Util.Vars.apply_mapping theta) vars))
       | AtomicConstraint(comp, x, y) ->
            (RelSemiRing.mk_val (AtomicConstraint(comp,
               (apply_variable_substitution_to_term theta x),
               (apply_variable_substitution_to_term theta y) )))
       | _ -> (RelSemiRing.mk_val lf)
   in
   (RelSemiRing.apply_to_leaves substitute_leaf alg)

and apply_variable_substitution_to_term theta (m: term_t) =
   let leaf_f lf = match lf with
      Var(y) -> TermSemiRing.mk_val(
                           Var(Util.Vars.apply_mapping theta y))
    | AggSum(f, r) ->
         TermSemiRing.mk_val(
            AggSum(apply_variable_substitution_to_term theta f,
                           apply_variable_substitution_to_relalg theta r))
    | _ -> TermSemiRing.mk_val(lf)
   in 
   (TermSemiRing.apply_to_leaves leaf_f m)




let polynomial (q: relalg_t): relalg_t = RelSemiRing.polynomial q

let monomials (q: relalg_t): (relalg_t list) =
   let p = RelSemiRing.polynomial q in
   if (p = RelSemiRing.zero) then []
   else RelSemiRing.sum_list p

let relalg_one  = RelSemiRing.mk_val(REL_BASE.one)
let relalg_zero = RelSemiRing.mk_val(REL_BASE.zero)

let monomial_as_hypergraph (monomial: relalg_t): (relalg_t list) =
   RelSemiRing.prod_list monomial

let hypergraph_as_monomial (hypergraph: relalg_t list): relalg_t =
   RelSemiRing.mk_prod hypergraph



(* auxiliary function used in extract_substitutions. *)
let split_off_equalities (monomial: relalg_t) :
                         (((var_t * var_t) list) * relalg_t) =
   let atoms = RelSemiRing.prod_list monomial
   in
   let split0 lf =
      match lf with
         RelSemiRing.Val(AtomicConstraint(Eq, TermSemiRing.Val(Var(x)),
            TermSemiRing.Val(Var(y)))) -> ([(x, y)], [])
         (* TODO: this can be generalized.
            We could replace variables by non-variable terms in some
            places. Question: do we want to do this in Rel-atoms too? *)
       | _ -> ([],     [lf])
   in
   let (eqs, rest) = (List.split (List.map split0 atoms))
   in
   (List.flatten eqs, RelSemiRing.mk_prod(List.flatten rest))


let extract_substitutions (monomial: relalg_t)
                          (bound_vars: var_t list) :
                          (substitution_t * relalg_t) =
   let (eqs, rest) = split_off_equalities monomial
   in
   let (theta, incons) = Util.Vars.unifier eqs bound_vars
   in
   let f (x,y) = RelSemiRing.mk_val (AtomicConstraint(Eq,
                    TermSemiRing.Val(Var(x)), TermSemiRing.Val(Var(y))))
   in
   (* add the inconsistent equations again as constraints. *)
   let rest2 = RelSemiRing.mk_prod(
      [(apply_variable_substitution_to_relalg theta rest)]
      @ (List.map f incons))
   in
   (theta, rest2)





let term_zero = TermSemiRing.mk_val (TERM_BASE.zero)
let term_one  = TermSemiRing.mk_val (TERM_BASE.one)


module MixedHyperGraph =
struct
   type edge_t = RelEdge of relalg_t | TermEdge of term_t

   let make f r =
        (List.map (fun x -> TermEdge x) (TermSemiRing.prod_list f))
      @ (List.map (fun x -> RelEdge x) (monomial_as_hypergraph r))

   let get_nodes hyperedge =
      match hyperedge with RelEdge(r)  -> relalg_vars r
                         | TermEdge(f) -> term_vars f

   let factorize hypergraph =
      Util.HyperGraph.connected_components get_nodes hypergraph

   let extract_rel_atoms l =
      let extract_rel_atom x = match x with RelEdge(r) -> [r] | _ -> [] in
      (List.flatten (List.map extract_rel_atom l))

   let extract_map_atoms l =
      let extract_map_atom x = match x with TermEdge(r) -> [r] | _ -> [] in
      TermSemiRing.mk_prod (List.flatten (List.map extract_map_atom l))
end

(* factorize an AggSum(f, r) where f and r are monomials *)
let factorize_aggsum_mm (f_monomial: term_t)
                        (r_monomial: relalg_t) : term_t =
   if (r_monomial = relalg_zero) then TermSemiRing.zero
   else
      let factors = (MixedHyperGraph.factorize
                       (MixedHyperGraph.make f_monomial r_monomial))
      in
      let mk_aggsum component =
         let f = MixedHyperGraph.extract_map_atoms component in
         let r = MixedHyperGraph.extract_rel_atoms component in
         (make_aggsum f (hypergraph_as_monomial r))
      in
      TermSemiRing.mk_prod (List.map mk_aggsum factors)


(* polynomials, recursively: in the end, +/union only occurs on the topmost
   level.
*)
let rec roly_poly (ma: term_t) : term_t =
   let r_leaf_f (lf: relalg_lf_t) : relalg_t =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            RelSemiRing.mk_val(AtomicConstraint(c, roly_poly t1, roly_poly t2))
       | _ -> RelSemiRing.mk_val(lf)
   in
   let t_leaf_f (lf: term_lf_t): term_t =
      match lf with
        Const(_)     -> TermSemiRing.mk_val(lf)
      | Var(_)       -> TermSemiRing.mk_val(lf)
      | AggSum(f, r) ->
         (
            let r2 = RelSemiRing.apply_to_leaves r_leaf_f r in
            let r_monomials = monomials r2 in
            let f_monomials = TermSemiRing.sum_list (roly_poly f)
            in
            TermSemiRing.mk_sum (List.flatten (List.map
               (fun y -> (List.map (fun x -> factorize_aggsum_mm x y)
                                    f_monomials))
               r_monomials))
         )
   in
   TermSemiRing.polynomial (TermSemiRing.apply_to_leaves t_leaf_f ma)




(* the input map algebra expression ma must be sum- and union-free.
   Call roly_poly first to ensure this.

   Auxiliary function not visible to the outside world.
*)
let rec simplify_roly (ma: term_t) (bound_vars: var_t list)
                        : (((var_t * var_t) list) * term_t) =
   let augment_bound_vars bound_vars b =
      let (_, b_img) = List.split b in
      Util.ListAsSet.union bound_vars b_img
   in
   let my_flatten combinator_fn l =
      let (b, f) = List.split l
      in ((List.flatten b), (combinator_fn f))
      (* FIXME: make sure that the concatenation of bindings does not
         lead to contradictions. *)
   in
   let r_leaf_f lf =
      match lf with
         AtomicConstraint(c, t1, t2) ->
            let (l1, t1b) = simplify_roly t1 bound_vars in
            let (l2, t2b) = simplify_roly t2 bound_vars in
                            (* TODO: pass bindings from first to second *)
            (l1 @ l2, RelSemiRing.mk_val(AtomicConstraint(c, t1b, t2b)))
       | _ -> ([], RelSemiRing.mk_val lf)
   in
   let t_leaf_f lf =
      match lf with
        Const(_)     -> ([], TermSemiRing.mk_val(lf))
      | Var(_)       -> ([], TermSemiRing.mk_val(lf))
      | AggSum(f, r) ->
        (
           (* we test equality, not equivalence, to zero here. Sufficient
              if we first normalize using roly_poly. *)
           if (r = relalg_zero) then raise Assert0Exception
           else if (f = TermSemiRing.zero) then ([], TermSemiRing.zero)
           else
              (* simplify terms in atomic constraints *)
              let (b1, r2) = RelSemiRing.fold (my_flatten RelSemiRing.mk_sum)
                                              (my_flatten RelSemiRing.mk_prod)
                                              r_leaf_f r
                 (* TODO: use b1 *)
              in
              let (b, non_eq_cons) = extract_substitutions r2 bound_vars
              in
              let (b2, f2) = simplify_roly
                                (apply_variable_substitution_to_term b f)
                                (augment_bound_vars bound_vars b)
              in
              let b3 = Util.ListAsSet.union b b2
              in
              if (non_eq_cons = relalg_one) then (b3, f2)
              else (b3, TermSemiRing.mk_val(AggSum(f2, non_eq_cons)))
                (* we represent the if-condition as a relational algebra
                   expression to use less syntax *)
        )
   in
   TermSemiRing.fold (my_flatten TermSemiRing.mk_sum)
                     (my_flatten TermSemiRing.mk_prod) t_leaf_f ma


(* apply roly_poly and simplify by unifying variables.
   returns a list of pairs (dimensions', monomial)
   where monomial is the simplified version of a nested monomial of ma
   and dimensions' is dimensions -- a set of variables occurring in term --
   after application of the substitution used to simplify monomial. *)
let simplify (ma: term_t)
             (bound_vars: var_t list)
             (dimensions: var_t list) :
             ((var_t list * term_t) list) =
   let simpl f =
      let (b, f2) = simplify_roly f bound_vars in
      ((List.map (Util.Vars.apply_mapping b) dimensions), f2)
   in
   List.map simpl (TermSemiRing.sum_list (roly_poly ma))


let rec extract_aggregates term =
   let r_aggs r =
      let r_leaf_f lf =
         match lf with
            AtomicConstraint(_, t1, t2) ->
               (extract_aggregates t1) @ (extract_aggregates t2)
          | _ -> []
      in
      RelSemiRing.fold List.flatten List.flatten r_leaf_f r
   in
   let t_leaf_f x =
      match x with
         AggSum(f, r) ->
            (* if r is constraints_only, take the aggregates from the
               atoms of r; otherwise, return x monolithically. *)
            if (constraints_only r) then ((extract_aggregates f) @ (r_aggs r))
            else [TermSemiRing.Val x]
       | _ -> []
   in
   Util.ListAsSet.no_duplicates
      (TermSemiRing.fold List.flatten List.flatten t_leaf_f term)





let rec relalg_as_string (relalg: relalg_t)
                         (theta: (term_t * string) list): string =
   let sum_f  l = "(" ^ (Util.string_of_list " or " l) ^ ")" in
   let prod_f l = (Util.string_of_list " and " l) in
   let leaf_f lf =
      match lf with
         AtomicConstraint(Eq, x, y)  ->
            (term_as_string x theta) ^ "="  ^ (term_as_string y theta)
       | AtomicConstraint(Lt, x, y)  ->
            (term_as_string x theta) ^ "<"  ^ (term_as_string y theta)
       | AtomicConstraint(Le, x, y)  ->
            (term_as_string x theta) ^ "<=" ^ (term_as_string y theta)
       | AtomicConstraint(Neq, x, y) ->
            (term_as_string x theta) ^ "<>" ^ (term_as_string y theta)
       | Empty -> "false"
       | ConstantNullarySingleton -> "true"
       | Rel(r, sch) -> r^"("^(Util.string_of_list ", " sch)^")"
   in
   RelSemiRing.fold sum_f prod_f leaf_f relalg

and term_as_string (m: term_t)
                   (aggsum_theta :(term_t * string) list):
                   string =
   let leaf_f (lf: term_lf_t) =
   (match lf with
      Const(c)    ->
      (
         match c with
            Int(i) -> string_of_int i
          | String(s) -> "'" ^ s ^ "'"
      )
    | Var(x)      -> x
    | AggSum(f,r) ->
      if (constraints_only r) then
         "(if " ^ (relalg_as_string r aggsum_theta) ^ " then " ^
                  (term_as_string   f aggsum_theta) ^ " else 0)"
      else
         Util.apply aggsum_theta
                    ("AggSum("^(term_as_string   f aggsum_theta)^", "
                              ^(relalg_as_string r aggsum_theta)^")")
                    (TermSemiRing.mk_val(lf))
   )
   in (TermSemiRing.fold (fun l -> "("^(Util.string_of_list "+" l)^")")
                         (fun l -> "("^(Util.string_of_list "*" l)^")")
                         leaf_f m)


