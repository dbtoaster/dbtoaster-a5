
(* types *)

type type_t = TInt | TLong | TDouble | TString
type var_t = string * type_t (* type of variable name *)
type comp_t = Eq | Lt | Le | Neq

type const_t = 
    | Int of int
    | Double of float
    | Long of int64
    | String of string

type 'term_t generic_relalg_lf_t =
            Empty (* aka false;
                     we do not record a schema or arity for it *)
          | ConstantNullarySingleton (* aka true *)
          | AtomicConstraint of comp_t * 'term_t * 'term_t
          | Rel of string * (var_t list)

type ('term_t, 'relalg_t) generic_term_lf_t =
            AggSum of ('term_t * 'relalg_t)
          | Const of const_t
          | Var of var_t


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

(* fold method wrappers *)
let fold_relalg (sum_f: ('a list -> 'a))
                (prod_f: ('a list -> 'a))
                (leaf_f: (readable_relalg_lf_t -> 'a))
                (r: readable_relalg_t) : 'a =
    let rec fold_aux rr =
        match rr with 
            | RA_MultiUnion(l)   ->  sum_f(List.map fold_aux l)
            | RA_MultiNatJoin(l) -> prod_f(List.map fold_aux l)
            | RA_Leaf(x)         -> leaf_f x
    in
        fold_aux r
        

let fold_term (sum_f: ('a list -> 'a))
              (prod_f: ('a list -> 'a))
              (leaf_f: (readable_term_lf_t -> 'a))
              (t: readable_term_t) : 'a =
    let rec fold_aux rt =
        match rt with 
            | RSum(l)  ->  sum_f(List.map fold_aux l)
            | RProd(l) -> prod_f(List.map fold_aux l)
            | RVal(x)  -> leaf_f x
    in
        fold_aux t

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


let rec declared_plan_vars (r : relalg_t) : var_t list =
    let relalg_lf lf = match lf with
        | Rel(rn,v) ->
              Util.ListAsSet.no_duplicates
                  (v@(List.map (fun (n,t) -> (rn^"."^n, t)) v))
        | AtomicConstraint(_, t1, t2) ->
              Util.ListAsSet.union
                  (declared_term_vars t1) (declared_term_vars t2)
        | _ -> []
    in
        RelSemiRing.fold Util.ListAsSet.multiunion
            Util.ListAsSet.multiunion relalg_lf r

and declared_term_vars (t: term_t) : var_t list =
    let term_lf lf = match lf with
        | AggSum(f,r) -> Util.ListAsSet.union
              (declared_term_vars f) (declared_plan_vars r)
        | _ -> []
    in
        TermSemiRing.fold Util.ListAsSet.multiunion
            Util.ListAsSet.multiunion term_lf t

let free_relalg_vars (r: relalg_t) : var_t list =
    Util.ListAsSet.diff (relalg_vars r) (declared_plan_vars r)

let free_term_vars (t: term_t) : var_t list = 
    Util.ListAsSet.diff (term_vars t) (declared_term_vars t)

let has_aggregates (m: term_t) : bool =
    let leaves =
        TermSemiRing.fold List.flatten List.flatten (fun x -> [x]) m in
    let is_aggregate x = match x with | AggSum(_) -> true | _ -> false in
        List.exists is_aggregate leaves



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
   let leaf_f lf =
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
   (RelSemiRing.fold RelSemiRing.mk_prod RelSemiRing.mk_sum leaf_f relalg)



(* Relational and map algebra stringification *)
let type_as_string t = match t with
    | TInt -> "int"
    | TLong -> "long"
    | TDouble -> "double"
    | TString -> "string"

let var_as_string (n,t) =
    (* n^":"^(type_as_string t) *)
    n

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
       | Rel(r, sch) -> r^"("^(Util.string_of_list ", "
             (List.map var_as_string sch))^")"
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
          | Double(f) -> string_of_float f
          | Long(l) -> Int64.to_string l
          | String(s) -> "'" ^ s ^ "'"
      )
    | Var(x)      -> var_as_string x
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



(* Delta computation and simplification *)

(* Negate arithmetic terms, since relational algebra is positive *)
let negate_term x =
    TermSemiRing.mk_prod([TermSemiRing.mk_val(Const(Int(-1))); x])


let rec relalg_delta (negate: bool) (relname: string)
                     (tuple: var_t list) (relalg: relalg_t) =
   let delta_leaf negate lf =
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
            if(((term_delta_aux negate relname tuple t1) = TermSemiRing.zero) &&
               ((term_delta_aux negate relname tuple t2) = TermSemiRing.zero))
            then
                  RelSemiRing.zero
            else raise Assert0Exception
                 (* the terms with nonzero delta should have been pulled
                    out of the constraint elsewhere. *)
   in
       RelSemiRing.delta (delta_leaf negate) relalg

and term_delta_aux (negate: bool) (relname: string)
               (tuple: var_t list) (term: term_t)  =
   let negate_f t = if negate then negate_term t else t
   in
   let t_pm_dt t =
      TermSemiRing.mk_sum[t; negate_f (term_delta_aux negate relname tuple t)]
   in
   let rec leaf_delta negate lf =
      match lf with
         Const(c) -> TermSemiRing.zero
       | Var(x)   -> TermSemiRing.zero
       | AggSum(f, r) ->
            let d_f = term_delta_aux negate relname tuple f in
            if (constraints_only r) then
               (* FIXME: this is overkill (but supposedly correct) in the
                  case that r does not contain AggSum terms, i.e., in the
                  case that all contained terms have zero delta. *)
               let new_r = (* replace each term t in r by (t + delta t) *)
                  let leaf_f lf =
                     (match lf with
                        AtomicConstraint(c, t1, t2) ->
                        RelSemiRing.mk_val(
                            AtomicConstraint(c, t_pm_dt t1, t_pm_dt t2))
                      | ConstantNullarySingleton ->
                        RelSemiRing.mk_val(ConstantNullarySingleton)
                      | _ -> raise Assert0Exception)
                  in
                  RelSemiRing.apply_to_leaves leaf_f r
               in
               (* Delta +:
                    (if (     new_r+             ) then (delta +  f) else 0)
                  + (if (     new_r+  and (not r)) then           f  else 0)
                  + (if ((not new_r+) and      r ) then          -f  else 0)

                  Delta -:
                    (if (    new_r-             ) then (delta -  f) else 0)
                  + (if (    new_r-  and (not r)) then          -f  else 0)
                  + (if (not(new_r-) and      r ) then           f  else 0)
               *)
               let delta_constraint =
                   TermSemiRing.mk_sum [
                      make_aggsum d_f new_r;
                      make_aggsum (negate_f f)
                          (RelSemiRing.mk_prod [new_r; (complement r)]);
                      make_aggsum
                          (if negate then f else negate_term f)
                          (RelSemiRing.mk_prod [ (complement new_r); r ])
                   ]
               in
                   delta_constraint
            else
               let d_r = (relalg_delta negate relname tuple r) in
                   TermSemiRing.mk_sum [ (make_aggsum  d_f   r);
                                         (make_aggsum    f d_r);
                                         (make_aggsum  d_f d_r) ]
   in
       TermSemiRing.delta (leaf_delta negate) term

and term_delta (negate: bool) (relname: string)
               (tuple: var_t list) (term: term_t)  =
   let negate_f t = if negate then negate_term t else t
   in
       negate_f (term_delta_aux negate relname tuple term)
               


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
   let (theta, incons) =
       Util.Vars.unifier eqs bound_vars (fun v_l -> List.map fst v_l)
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
      | _ -> TermSemiRing.mk_val(lf)
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
      | _ -> ([], TermSemiRing.mk_val(lf))
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


(* Nested map term flattening *)

(* (readable_term_t * readable_relalg_t opt) -> readable_term_t *)
let mk_cond_agg (term, constraint_opt) =
    begin match constraint_opt with
        | None -> term
        | Some(constraints) -> RVal(AggSum(term, constraints))
    end

(* TODO: we currently require the non-nested parts of a relalg to be
 * identical, including non-nested constraints. Thus for flattening to
 * work for unions of relalg terms, AggSums should be distributed
 * over unions in the input,
 * i.e. AggSum(Q union R) => AggSum(Q) + AggSum(R)
 * where Q,R contain no further unions.
 * This can be done with roly_poly, and then distributing what's left. *)

(* readable_relalg_t -> readable_relalg_t list * readable_relalg_t option *)
let rec extract_nested_constraints r =
    match r with
        | RA_Leaf(AtomicConstraint(op, t1, t2)) ->
              if ((has_aggregates (make_term t1)) or
                  (has_aggregates (make_term t2)))
              then ([r], None) else ([], Some(r))

        | RA_Leaf(_) -> ([], Some(r))

        | RA_MultiUnion(l) ->
              let (nc_ll, rest_l) =
                  List.split (List.map extract_nested_constraints l)
              in
                  if not (List.for_all
                      (fun r -> r = (List.hd rest_l)) (List.tl rest_l))
                  then raise (Failure ("Flattening failed."))
                  else (List.flatten nc_ll, List.hd rest_l)

        | RA_MultiNatJoin(l) ->
              let (nc_ll, rest_l) = List.split
                  (List.map extract_nested_constraints l) in
              let nr = List.fold_left
                  (fun acc x -> match x with
                      | None -> acc | Some(y) -> acc@[y])
                  [] rest_l
              in
                  (List.flatten nc_ll, Some(RA_MultiNatJoin(nr)))


(* Flattening rewrites nested aggregates in predicates to nested
 * conditional aggregates. A conditional aggregate separates
 * an attribute (i.e. relational variable) from its usage in a nested
 * query, replacing that usage with a bigsum_var. Note conditional
 * aggregates may themselves be nested, since we cannot pull bigsum_vars
 * above conditionals (i.e. in a triple-nested query).
 * Flattening process:
 * -- recursively flatten aggregate argument term
 * -- extract nested constraints from relational part
 * -- recursively flatten nested constraints
 * -- identify bigsum_vars as free vars in nested constraints that
 *    are not bigsum vars from below, or params (i.e. bound from above)
 * -- create flattened term by rotating the aggsum, as
 *        AggSum(AggSum(arg, non-nested), nested constraints)
*)
(* Note: we assume top level aggregates have the same relational part,
 * ensuring equivalent nesting and scoping of bigsum vars *)
(* var_t list -> readable_term_t ->
 *     ((readable_term_t * readable_relalg_t opt) *
 *     (var_t * var_t * readable_relalg_lf_t) list) *)
let rec flatten_term params term =
    (*
    let debug_nested_constraints nc rest = 
        print_endline ("# new c: "^(string_of_int (List.length nc)));
        List.iter 
            (fun r -> print_endline ("NC: "^
                (relalg_as_string (make_relalg r) [])))
            nc;
        print_endline ("REST: "^(relalg_as_string (make_relalg rest) []));
    in
    let debug_f_lifted_constraint f_r =
        print_endline ("NCR: "^(relalg_as_string (make_relalg f_r) []))
    in
    let debug_nc_size c =
        print_endline ("# new c: "^(string_of_int (List.length c)))
    in
    *)
    let get_relations r =
        fold_relalg List.flatten List.flatten
            (fun x -> match x with | Rel _ -> [x] | _ -> []) r
    in
    let find_var relations (n,t) =
        try
            List.find (fun r -> match r with
                | Rel(_,f) ->
                      List.exists (fun (n2,t2) -> n=n2 && t=t2) f
                | _ -> raise (Failure
                      ("Internal error: expected a relation.")))
                relations
        with Not_found ->
            raise (Failure ("Could not find "^n^" in relations."))
    in
    let mk_var_constraint l r : readable_relalg_t = 
        RA_Leaf(AtomicConstraint(Eq, RVal(Var(l)), RVal(Var(r))))
    in
    let flatten_list l f g = 
        let (new_terms_and_constraints, new_bigsum_l) =
            List.split (List.map (flatten_term params) l)
        in
        let (new_terms, new_constraints) =
            List.split new_terms_and_constraints
        in
            ((f new_terms new_constraints, g new_constraints),
            Util.ListAsSet.multiunion new_bigsum_l)
    in
    let flatten_constraints cl =
        let recur_if_nested t =
            if has_aggregates (make_term t) then
                let (tc,bsv) = flatten_term params t in
                    (mk_cond_agg tc, bsv)
            else (t, [])
        in
        let (cstrt_l, bigsum_l) = List.split (List.map
            (fun r -> match r with
                | RA_Leaf(AtomicConstraint(op, t1, t2)) ->
                      let (new_t1, nbsv1) = recur_if_nested t1 in
                      let (new_t2, nbsv2) = recur_if_nested t2 in
                          (RA_Leaf(AtomicConstraint(op, new_t1, new_t2)),
                          Util.ListAsSet.union nbsv1 nbsv2)
                | _ -> raise (Failure ("Expected a constraint.")))
            cl)
        in
            (cstrt_l, Util.ListAsSet.multiunion bigsum_l)
    in
    let local_bigsum_and_mappings params r nc nc_bigsum =
        let nc_bigsum_vars = List.map (fun (x,_,_) -> x) nc_bigsum in
        let nc_free_vars = List.map
            (fun x -> Util.ListAsSet.diff
                (free_relalg_vars (make_relalg x)) nc_bigsum_vars)
            nc
        in
        let local_bigsum_vars =
            Util.ListAsSet.diff
                (Util.ListAsSet.multiunion nc_free_vars) params in
        let mapping = 
            List.map (fun ((n,t) as v) ->
                let new_v = ("loop_"^n, t) in (v, new_v))
                local_bigsum_vars
        in
        let local_bigsum = 
            let relations = get_relations r in
                List.map (fun (v,bsv) ->
                    let vr = find_var relations v in (bsv,v,vr))
                    mapping
        in
            (local_bigsum, mapping)
    in
        begin match term with
            | RVal(AggSum(f,r)) ->
                  (* flatten recursively *)
                  let ((nf, nf_r), f_bigsum) = flatten_term params f in

                  (* extract nested constraints from relational part *)
                  let (nested_constraints, rest) =
                      match extract_nested_constraints r with
                          | (_,None) ->
                                raise (Failure
                                    "No relalg left during flattening.")
                          | (x,Some(y)) ->
                                (*debug_nested_constraints x y;*)
                                x,y
                  in

                  (* recursively flatten within constraints *)
                  let (nc, nc_bigsum) =
                      flatten_constraints nested_constraints in

                  (* rename and track bigsum vars *)
                  let (local_bigsum, local_bsv_mapping) =
                      local_bigsum_and_mappings params r nc nc_bigsum
                  in

                  (* rotate term *)
                  let new_bigsum_constraints =
                      List.map
                          (fun (v, bsv) -> mk_var_constraint v bsv)
                          local_bsv_mapping
                  in

                  let new_term =
                      let new_sub_r =
                          RA_MultiNatJoin([rest]@new_bigsum_constraints)
                      in
                          RVal(AggSum(nf, new_sub_r)) in

                  let new_constraint =
                      let c = match nf_r with
                          | None -> nc
                          | Some(f_r) ->
                                (*debug_f_lifted_constraint f_r;*)
                                [f_r]@nc
                      in
                      let sub_f r = readable_relalg
                          (apply_variable_substitution_to_relalg
                              local_bsv_mapping (make_relalg r))
                      in
                          (*debug_nc_size c;*)
                          if c = [] then None
                          else Some(RA_MultiNatJoin(List.map sub_f c))
                  in

                  (* track all bigsums below *)
                  let bigsum =
                      Util.ListAsSet.multiunion
                          [f_bigsum; nc_bigsum; local_bigsum]
                  in
                      ((new_term, new_constraint), bigsum)
                          
            | RVal(_) -> ((term, None), [])

            | RSum(l) ->
                  flatten_list l
                      (fun nl nc ->
                          RSum(List.map mk_cond_agg (List.combine nl nc)))
                      (fun nc -> None)

            | RProd(l) ->
                  flatten_list l
                      (fun nl nc -> RProd(nl))
                      (fun nc ->
                          let valid_c = List.fold_left
                              (fun acc x -> match x with
                                  | None -> acc | Some(y) -> acc@[y]) [] nc
                          in
                              if valid_c = [] then None
                              else Some(RA_MultiNatJoin(valid_c)))
        end


(* Typing helpers *)
(* TODO: unit tests for typing *)
exception TypeException of string

(* returns the schema for a relalg, including applying implicit projections
 * for a union and natural join *)
let rec relalg_schema (r : readable_relalg_t) : var_t list =
    let leaf_f lf =
        match lf with
            | AtomicConstraint(comp, x, y) -> []
            | Rel(r, sch) -> sch
            | Empty -> []
            | ConstantNullarySingleton -> []
    in
    let prod_f l =
        List.fold_left
            (fun a b -> Util.ListAsSet.diff (a@b) (Util.ListAsSet.inter a b))
            (List.hd l) (List.tl l)
    in
    let sum_f l =
        List.fold_left Util.ListAsSet.inter (List.hd l) (List.tl l)
    in
        RelSemiRing.fold sum_f prod_f leaf_f (make_relalg r)

(* returns the type of a map term, where the term may include params,
 * bigsum vars and loop vars. Such vars must be passed in via the
 * 'extra_vars' argument. 
 * This method also ensures free variables in aggregate arguments 
 * appear in the relational part's schema. *)
let rec term_type (t: readable_term_t) (extra_vars: var_t list) : type_t =
    let debug_undefined_var (n,t) f r =
        print_endline ("Could not find var "^n);
        print_endline ("Relational part: "^(relalg_as_string r []));
        print_endline ("Map part: "^(term_as_string f []));
    in
    let promote_type a b =
        let msg = "Type promotion mismatch:" in
            match (a,b) with
                | (TString, TString) -> TString
                | (TString, _) -> raise (TypeException msg)
                | (TDouble, TLong) | (TLong, TDouble) ->
                      raise (TypeException (msg^" precision exception"))
                | (TDouble, _) | (_, TDouble) -> TDouble
                | (TLong,_) | (_, TLong) -> TLong
                | _ -> TInt
    in
    let leaf_f lf = match lf with
        | Const (Int(_))    -> TInt
        | Const (Double(_)) -> TDouble
        | Const (Long(_))   -> TLong
        | Const (String(_)) -> TString
        | Var (n,t) -> t
        | AggSum (f, r) ->
              (* validate types of vars used in f with those defined in r *)
              let r_vars = relalg_schema (readable_relalg r) in
              let f_vars = free_term_vars f in
              let eq_name v1 v2 = (fst v1) = (fst v2) in
              let inconsistent_vars =
                  List.filter (fun ((n,t) as v) ->
                      let (n2,t2) =
                          try List.find (eq_name v)
                              (Util.ListAsSet.union r_vars extra_vars)
                          with Not_found ->
                              debug_undefined_var v f r;
                              raise (TypeException
                                  ("No such var "^n^" in relational part"))
                      in t <> t2)
                      f_vars
              in
                  if inconsistent_vars = [] then
                      term_type (readable_term f) extra_vars
                  else raise (TypeException
                      "Type inconsistencies in aggregate variable usage.")
    in
    let list_f l = List.fold_left promote_type (List.hd l) (List.tl l)
    in
        TermSemiRing.fold list_f list_f leaf_f (make_term t)
