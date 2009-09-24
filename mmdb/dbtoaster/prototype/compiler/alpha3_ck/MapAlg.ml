

module rec MA_BASE :
sig
   (* include SemiRing.Base *)

   type t    = AggSum of (MASemiRing.expr_t * RelAlg.relalg_t)
             | Const of int
             | Var of string

   val zero: t
   val one: t
end =
struct
   type t    = AggSum of (MASemiRing.expr_t * RelAlg.relalg_t)
             | Const of int
             | Var of string

   let  zero = Const(0)
   let  one  = Const(1)
end
and MASemiRing : SemiRing.SemiRing
                 with type leaf_t = MA_BASE.t
                 (* makes leaf_t visible to the outside *)
    = SemiRing.Make(MA_BASE)


type mapalg_t    = MASemiRing.expr_t
type mapalg_lf_t = MASemiRing.leaf_t


type readable_mapalg_lf_t = RConst  of int
                          | RVar    of string
                          | RAggSum of readable_mapalg_t
                                     * RelAlg.readable_relalg_t

and  readable_mapalg_t    = RVal    of readable_mapalg_lf_t
                          | RProd   of readable_mapalg_t list
                          | RSum    of readable_mapalg_t list




(* function implementations *)

let zero = MASemiRing.mk_val (MA_BASE.zero)

let rec readable mapalg =
   let lf_readable lf =
      match lf with
         MA_BASE.Const(x)     -> RConst(x)
       | MA_BASE.Var(x)       -> RVar(x)
       | MA_BASE.AggSum(f, r) -> RAggSum(readable f, RelAlg.readable r)
   in
   MASemiRing.fold (fun l -> RSum l) (fun l -> RProd l)
                   (fun x -> RVal (lf_readable x)) mapalg

let rec make readable_mapalg =
   let lf_make lf = match lf with
      RConst(x)    -> MA_BASE.Const(x)
    | RVar(x)      -> MA_BASE.Var(x)
    | RAggSum(f,r) -> MA_BASE.AggSum(make f, RelAlg.make r)
   in
   match readable_mapalg with
      RSum(x)  -> MASemiRing.mk_sum( List.map make x)
    | RProd(x) -> MASemiRing.mk_prod(List.map make x)
    | RVal(x)  -> MASemiRing.mk_val(lf_make x)

let rec rfold ( sum_f: 'b list -> 'b)
              (prod_f: 'b list -> 'b)
              (leaf_f: readable_mapalg_lf_t -> 'b)
              (e: readable_mapalg_t) =
   match e with
      RSum(l)  ->  sum_f(List.map (rfold sum_f prod_f leaf_f) l)
    | RProd(l) -> prod_f(List.map (rfold sum_f prod_f leaf_f) l)
    | RVal(x)  -> leaf_f x



let mk_aggsum f r =
   if (r = RelAlg.zero) then MASemiRing.zero
   else if (f = MASemiRing.zero) then MASemiRing.zero
   else if (r = RelAlg.one) then f
   else MASemiRing.mk_val (MA_BASE.AggSum (f, r))

let mk_aggsum0 f r =
   MASemiRing.mk_val (MA_BASE.AggSum (f, r))


(* TODO: mk_aggsum0 is just for testing. replace by mk_aggsum *)
let rec delta relname tuple (mapalg: mapalg_t) =
   let rec leaf_delta lf =
      match lf with
         MA_BASE.Const(c) -> MASemiRing.zero
       | MA_BASE.Var(x)   -> MASemiRing.zero
       | MA_BASE.AggSum(f, relalg) ->
            MASemiRing.mk_sum [
               mk_aggsum0 (delta relname tuple f) relalg;
               mk_aggsum0 f (RelAlg.delta relname tuple relalg);
               mk_aggsum0 (delta relname tuple f)
                          (RelAlg.delta relname tuple relalg) ]
   in
   MASemiRing.delta leaf_delta mapalg





let rec collect_vars mapalg: string list =
   let leaf_f x = match x with
      MA_BASE.Var(y) -> [y]
    | MA_BASE.AggSum(f, r) -> Util.ListAsSet.union (collect_vars f)
                                                   (RelAlg.vars r)
    | _ -> []
   in
   MASemiRing.fold Util.ListAsSet.multiunion
                   Util.ListAsSet.multiunion
                   leaf_f
                   mapalg





module MixedHyperGraph =
struct
   type edge_t = RelEdge of RelAlg.relalg_t
               | MapEdge of MASemiRing.expr_t

   exception NotImplementedException

   let get_nodes hyperedge =
      match hyperedge with RelEdge(r) -> RelAlg.vars r (* or RelAlg.schema? *)
                         | MapEdge(f) -> collect_vars f

   let extract_rel_atoms l =
      let extract_rel_atom x = match x with RelEdge(r) -> [r] | _ -> []
      in
      (List.flatten (List.map extract_rel_atom l))

   let extract_map_atoms l =
      let extract_map_atom x = match x with MapEdge(r) -> [r] | _ -> []
      in
      MASemiRing.mk_prod (List.flatten (List.map extract_map_atom l))

   let make f r =
        (List.map (fun x -> MapEdge x) (MASemiRing.prod_list f))
      @ (List.map (fun x -> RelEdge x) (RelAlg.monomial_as_hypergraph r))

   let factorize hypergraph =
      Util.HyperGraph.connected_components get_nodes hypergraph

   let extract_from component =
      (extract_map_atoms component, extract_rel_atoms component)
end




(* factorize an AggSum(f, r) where f and r are monomials
*)
let factorize_aggsum_mm (f_monomial: mapalg_t)
                        (r_monomial: RelAlg.relalg_t) : mapalg_t =
   if (r_monomial = RelAlg.zero) then MASemiRing.zero
   else
      let factors = (MixedHyperGraph.factorize
                       (MixedHyperGraph.make f_monomial r_monomial))
      in
      let mk_aggsum component =
         match (MixedHyperGraph.extract_from component) with (f, r) ->
         (mk_aggsum f (RelAlg.hypergraph_as_monomial r))
      in
      MASemiRing.mk_prod (List.map mk_aggsum factors)



(* polynomials, recursively: in the end, +/union only occurs on the topmost
   level.
*)
let rec roly_poly (ma: mapalg_t) : mapalg_t =
   let leaf_f (lf: mapalg_lf_t): mapalg_t =
      match lf with
        MA_BASE.Const(_)     -> MASemiRing.mk_val(lf)
      | MA_BASE.Var(_)       -> MASemiRing.mk_val(lf)
      | MA_BASE.AggSum(f, r) ->
         (
            let r_monomials = RelAlg.monomials r in
            let f_monomials = MASemiRing.sum_list (roly_poly f)
            in
            MASemiRing.mk_sum (List.flatten (List.map
               (fun y -> (List.map (fun x -> factorize_aggsum_mm x y)
                                    f_monomials))
               r_monomials))
         )
   in
   MASemiRing.polynomial (MASemiRing.apply_to_leaves leaf_f ma)



(* TODO: also substitute in nested aggsum expressions. *)
let apply_variable_substitution theta m =
   let b = List.map (fun (x,y) -> (MASemiRing.mk_val(MA_BASE.Var(x)),
                                   MASemiRing.mk_val(MA_BASE.Var(y)))) theta
   in
   (MASemiRing.substitute_many b m)





exception Assert0Exception (* should never be reached *)


(* the input map algebra expression ma must be sum- and union-free.
   Call roly_poly first to ensure this.

   Auxiliary function not visible to the outside world.
*)
let rec simplify_roly (ma: mapalg_t) (bound_vars: string list)
                        : (((string * string) list) * mapalg_t) =
   let sum_f l =
      let (b, f) = List.split l in
      ((List.flatten b), MASemiRing.mk_sum(f))
      (* FIXME: make sure that the concatenation of bindings does not
         lead to contradictions. *)
   in
   let prod_f l =
      let (b, f) = List.split l in
      ((List.flatten b), MASemiRing.mk_prod(f))
      (* FIXME: make sure that the concatenation of bindings does not
         lead to contradictions. *)
   in
   let leaf_f lf =
      match lf with
        MA_BASE.Const(_)     -> ([], MASemiRing.mk_val(lf))
      | MA_BASE.Var(_)       -> ([], MASemiRing.mk_val(lf))
      | MA_BASE.AggSum(f, r) ->
        (
           (* we test equality, not equivalence, to zero here. Sufficient
              if we first normalize using roly_poly. *)
           if (r = RelAlg.zero) then raise Assert0Exception
           else if (f = MASemiRing.zero) then ([], MASemiRing.zero)
           else
              let (b, non_eq_cons) =
                 RelAlg.extract_substitutions r bound_vars
              in
              let (_, b_img) = List.split b
              in
              let new_bound_vars = Util.ListAsSet.union bound_vars b_img
              in
              let (b2, f2) = simplify_roly (apply_variable_substitution b f)
                                           new_bound_vars
              in
              let b3 = Util.ListAsSet.union b b2
              in
              if (non_eq_cons = RelAlg.one) then (b3, f2)
              else (b3, MASemiRing.mk_val(MA_BASE.AggSum(f2, non_eq_cons)))
                (* we represent the if-condition as a relational algebra
                   expression to use less syntax *)
        )
   in
   MASemiRing.fold sum_f prod_f leaf_f ma



let simplify (ma: mapalg_t)
             (bound_vars: string list)
             (dimensions: string list) :
             (string list * mapalg_t) =
   let simpl f =
      let (b, f2) = simplify_roly f bound_vars
      in
      ((Util.Vars.apply_mapping b dimensions), f2)
   in
   let l = List.map simpl (MASemiRing.sum_list (roly_poly ma))
   in
   if (List.length l) <> 1 then raise Assert0Exception
   else (List.hd l)




let extract_aggregates mapalg =
   let leaf_f x = match x with
    | MA_BASE.AggSum(f, r) -> [x]
    | _ -> []
   in
   List.map (fun x -> MASemiRing.Val x)
            (MASemiRing.fold List.flatten List.flatten leaf_f mapalg)


