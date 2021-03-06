(**
   A module for expressing and performing basic operations over DBToaster
   relational calculus.  Operations are provided for taking deltas.
   
   Calculus rings are functorized by an 'External' module that makes it possible
   to insert arbitrary subexpressions, the meaning of which is defined outside
   the calculus proper (i.e., datastructures, UDFs, etc...).
   
   A default functorization is provided and included in the calculus base 
   module.  The default calculus functorization supports the inclusion of 
   externals in a calculus expression, but does not attempt to interpret them 
   in any way.
*)

open Type
open Arithmetic
open Constants

(**
   Template for the base type for externals (maps).  Consists of (respectively):
      - The external's name
      - The external's input variables
      - The external's output variables
      - The external's type
      - Initial value computation metadata
*)
type 'meta_t external_leaf_t = 
   string *       
   var_t list *   
   var_t list *   
   type_t *       
   'meta_t option 

(**
   Template for the base type for the Calculus ring.
*)
type ('term_t) calc_leaf_t = 
   | Value    of value_t                     (** A value expression *)
   | AggSum   of var_t list * 'term_t        (** A sum aggregate, consisting of
                                                 group-by variables and the 
                                                 nested sub-term being 
                                                 aggregated over *)
   | Rel      of string * var_t list         (** A base relation *)
   | DeltaRel of string * var_t list         (** A delta relation 
                                                 (for batch updates) *)
   | DomainDelta of 'term_t                  (** A domain of delta 
                                                 (for batch updates) *)
   | External of 'term_t external_leaf_t     (** An external (map) *)
   | Cmp      of cmp_t * value_t * value_t   (** A comparison between two 
                                                 values *)
(** BEGIN OR LIST HACK **)
   | CmpOrList of value_t * const_t list     (** An OR list comparison *)
(** END OR LIST HACK **)   
   | Lift     of var_t * 'term_t             (** A Lift expression.  The nested
                                                 sub-term's value is lifted
                                                 into the indicated variable *)
(***** BEGIN EXISTS HACK *****)
   | Exists   of 'term_t                     (** An existence test.  The value 
                                                 of this term is 1 if and only 
                                                 if the nested expression's
                                                 value is 1 *)
(***** END EXISTS HACK *****)

module rec
CalcBase : sig
      type t = (CalcRingBase.expr_t) calc_leaf_t
      val  zero: t
      val  one: t

      val is_zero: t -> bool
      val is_one: t -> bool
   end = struct
      type t = (CalcRingBase.expr_t) calc_leaf_t
      let zero = Value(Arithmetic.mk_int 0)
      let one  = Value(Arithmetic.mk_int 1)

      let is_zero (v: t) = match v with 
         | Value(c) -> ValueRing.is_zero c 
         | _        -> false

      let is_one  (v: t) = match v with
         | Value(c) -> ValueRing.is_one c
         | _        -> false   
   end and

(** The base for the Calculus ring *)
CalcRingBase : Ring.Ring with type leaf_t = CalcBase.t
         = Ring.Make(CalcBase)

(** Elements of the Calculus ring. *)
type expr_t = CalcRingBase.expr_t
(** Base type for describing an External (map). *)
type external_t = CalcRingBase.expr_t external_leaf_t

(**
   The calculus ring with overwritten mk_sum/mk_prod functions that preserve the 
   type of the expression when simplifying.
*)
module CalcRing = struct
   include CalcRingBase

   (**
      Compute the type of the specified Calculus expression
      @param expr  A Calculus expression
      @return      The type that [expr] evaluates to
   *)
   let rec type_of_expr (expr:expr_t): type_t =
      let rcr a = type_of_expr a in
      fold
         (escalate_type_list ~opname:"[+]")
         (escalate_type_list ~opname:"[*]")
         (fun x->x)
         (fun lf -> begin match lf with
            | Value(v)                -> (type_of_value v)
            | External(_,_,_,etype,_) -> etype
            | AggSum(_, subexp)       -> rcr subexp
            | Rel(_,_)                -> TInt
            | DeltaRel(_,_)           -> TInt
            | DomainDelta(_)          -> TInt
            | Cmp(_,_,_)              -> TInt (* Since we're not using truth, but
                                                 rather truthiness: the # of
                                                 truths encountered thus far *)
            | CmpOrList(_,_)          -> TInt
            | Lift(_,_)               -> TInt
(***** BEGIN EXISTS HACK *****)
            | Exists(_)               -> TInt
(***** END EXISTS HACK *****)
         end)
         expr

   let mk_sum  l =
      let t = type_of_expr (Sum(List.flatten (List.map sum_list l))) in
      let z = 
         begin match zero_of_type_opt t with
         | Some(z) -> Some(mk_val (Value(ValueRing.mk_val (AConst(z)))))
         | None    -> None
         end
      in
      CalcRingBase.mk_sum_defs z l

   let mk_prod l =
      let t = type_of_expr (Prod(List.flatten (List.map prod_list l))) in
      let z = 
         begin match zero_of_type_opt t with
         | Some(z) -> Some(mk_val (Value(ValueRing.mk_val (AConst(z)))))
         | None    -> None
         end
      in
      let o = 
         begin match one_of_type_opt t with
         | Some(o) -> Some(mk_val (Value(ValueRing.mk_val (AConst(o)))))
         | None    -> None
         end
      in
      CalcRingBase.mk_prod_defs z o l
end

(** The scope and schema of an expression, the available input variables when
    an expression is evaluated, and the set of output variables that the 
    expression is expected to produce. *)
type schema_t = (var_t list * var_t list)


(*** Stringifiers ***)
(** 
   Generate the (Calculusparser-compatible) string representation of a base
   element of the Calculus ring.
   @param leaf  A base element of the Calculus ring
   @return      The string representation of [leaf]
*)
let rec string_of_leaf (leaf:CalcRing.leaf_t): string =
   begin match leaf with
      | Value(ValueRing.Val(AVar(_)|AConst(_)) as v) ->
         (Arithmetic.string_of_value v)
      | Value(v) ->
         "{"^(Arithmetic.string_of_value v)^"}"
      | External(ename,eins,eouts,etype,emeta) ->
         ename^
         "("^(string_of_type etype)^")"^"["^
         (ListExtras.string_of_list ~sep:", " string_of_var eins)^"]["^
         (ListExtras.string_of_list ~sep:", " string_of_var eouts)^"]"^
         (match emeta with | None -> ""
                           | Some(s) -> ":("^(string_of_expr s)^")")
      | AggSum(gb_vars, subexp) ->
         "AggSum(["^(ListExtras.string_of_list ~sep:", " string_of_var gb_vars)^
         "],("^(string_of_expr subexp)^"))"
      | Rel(rname, rvars)       ->
         rname^"("^(ListExtras.string_of_list ~sep:", " string_of_var rvars)^")"
      | DeltaRel(rname, rvars)  -> 
         "(DELTA "^rname^")("^
         (ListExtras.string_of_list ~sep:", " string_of_var rvars)^")"
      | DomainDelta(subexp) -> 
         "DOMAIN("^(string_of_expr subexp)^")"
      | Cmp(op,subexp1,subexp2) ->
         "{"^(string_of_value subexp1)^" "^
             (string_of_cmp op)^
         " "^(string_of_value subexp2)^"}"
      | CmpOrList(subexp, consts) ->   
         "{"^(string_of_value subexp)^" IN "^
         ListExtras.ocaml_of_list string_of_const consts^"}"
      | Lift(target, subexp)    ->
         "("^(string_of_var target)^" ^= "^(string_of_expr subexp)^")"
(***** BEGIN EXISTS HACK *****)
      | Exists(subexp) ->
         "Exists("^(string_of_expr subexp)^")"
(***** END EXISTS HACK *****)
   end
(**
   Generate the (Calculusparser-compatible) string representation of an
   element of the Calculus ring.
   @param expr  An element of the Calculus ring
   @return      The string representation of [expr]
*)
and string_of_expr (expr:expr_t): string =
   let (sum_op, prod_op, neg_op) = 
      if Debug.active "PRINT-RINGS" then (" U ", " |><| ", "(<>:-1)*")
                                    else (" + ", " * ", "NEG * ")
   in
   CalcRing.fold
      (fun sum_list  -> "("^(String.concat sum_op sum_list )^")")
      (fun prod_list -> "("^(String.concat prod_op prod_list)^")")
      (fun neg_term  -> "("^neg_op^neg_term^")")
      string_of_leaf
      expr

(*** Utility ***)
(** A generic exception pertaining to Calculus.  The first parameter is the 
    Calculus expression that triggered the failure *)
exception CalculusException of expr_t * string
;;
(**/**)
let bail_out expr msg = 
   raise (CalculusException(expr, msg))
;;
(**/**)

(*** Informational Operations ***)
(** 
   Compute the schema of a given expression
   @param expr  A Calculus expression
   @return      A pair of the set of input and output variables of [expr]
*)
let rec schema_of_expr ?(lift_group_by_vars_are_inputs = 
                                (not (Debug.active "LIFTS-PRESERVE-OVARS")))
                       (expr:expr_t):(var_t list * var_t list) =
   let rcr a = schema_of_expr a in
   CalcRing.fold 
      (fun sum_vars ->
         let ivars, ovars = List.split sum_vars in
            let old_ivars = ListAsSet.multiunion ivars in
            let old_ovars = ListAsSet.multiunion ovars in
            (* Output variables that appear on only one side of the
               union operator lose finite support and must now be treated
               as input variables *)
            let new_ivars = ListAsSet.diff old_ovars 
                                           (ListAsSet.multiinter ovars)
            in
               (  ListAsSet.union old_ivars new_ivars,
                  ListAsSet.diff  old_ovars new_ivars )
      )
      (fun prod_vars ->
         List.fold_left (fun (old_ivars, old_ovars) (new_ivars, new_ovars) ->
            (* The following expression treats [A] * R(A) as having A as an
               input variable.  This is correct as long as A is in the scope
               when this expression is evaluated, but that might be incorrect
            *)
            (  (ListAsSet.union (ListAsSet.diff new_ivars old_ovars)
                                old_ivars),
               (ListAsSet.diff (ListAsSet.union old_ovars new_ovars)
                               old_ivars)
            )
         ) ([],[]) prod_vars
      )
      (fun child_vars -> child_vars)
      (fun lf -> begin match lf with
         | Value(v) -> (vars_of_value v,[])
         | External(_,eins,eouts,_,_) -> (eins,eouts)
         | AggSum(gb_vars, subexp) -> 
            let (ivars, ovars) = rcr subexp in
               let trimmed_gb_vars = (ListAsSet.inter ovars gb_vars) in
               if not (ListAsSet.seteq trimmed_gb_vars gb_vars)
               then bail_out expr (
                  "Calculus expr with invalid group by vars: "^
                     (ListExtras.ocaml_of_list string_of_var ovars)^
                     " instead of "^
                     (ListExtras.ocaml_of_list string_of_var gb_vars)
                  )
               else (ivars, gb_vars)
         | Rel(_,rvars) -> ([],rvars)
         | DeltaRel(_,rvars) -> ([],rvars)
         | DomainDelta(subexp) -> rcr subexp
         | Cmp(_,v1,v2) ->
            (ListAsSet.union (vars_of_value v1) (vars_of_value v2), [])
         | CmpOrList(v,_) -> (vars_of_value v, [])
         | Lift(target, subexp) ->
            let ivars, ovars = rcr subexp in
               if lift_group_by_vars_are_inputs then
                  (ListAsSet.union ivars ovars, [target])
               else 
                  (ivars, ListAsSet.union [target] ovars)
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp) -> rcr subexp
(***** END EXISTS HACK *****)
      end)
      expr 

(**
   Compute the type of the specified Calculus expression
   @param expr  A Calculus expression
   @return      The type that [expr] evaluates to
*)
let type_of_expr (expr:expr_t): type_t =
   CalcRing.type_of_expr expr

(**
   Obtain the set of all relations appearing in the specified Calculus 
   expression
   @param expr  A Calculus expression
   @return      The set of all relation names that appear in [expr]
*)
let rec rels_of_expr (expr:expr_t): string list =
   let rcr a = rels_of_expr a in
      CalcRing.fold
         ListAsSet.multiunion
         ListAsSet.multiunion
         (fun x -> x)
         (fun lf -> begin match lf with
            | Value(_)            -> []
            | External(_,_,_,_,None) -> []
            | External(_,_,_,_,Some(em)) -> rcr em
            | AggSum(_, subexp)   -> rcr subexp
            | Rel(rn,_)           -> [rn]
            | DeltaRel(rn,_)      -> []
            | DomainDelta(subexp) -> rcr subexp
            | Cmp(_,_,_)          -> []
            | CmpOrList(_,_)      -> []
            | Lift(_,subexp)      -> rcr subexp
(***** BEGIN EXISTS HACK *****)
            | Exists(subexp)      -> rcr subexp
(***** END EXISTS HACK *****)
         end)
         expr

(**
   Obtain the set of all delta relations appearing in the specified Calculus 
   expression
   @param expr  A Calculus expression
   @return      The set of all delta relation names that appear in [expr]
*)
let rec deltarels_of_expr (expr:expr_t): string list =
   let rcr a = deltarels_of_expr a in
      CalcRing.fold
         ListAsSet.multiunion
         ListAsSet.multiunion
         (fun x -> x)
         (fun lf -> begin match lf with
            | Value(_)            -> []
            | External(_,_,_,_,None) -> []
            | External(_,_,_,_,Some(em)) -> rcr em
            | AggSum(_, subexp)   -> rcr subexp
            | Rel(rn,_)           -> []
            | DeltaRel(rn,_)      -> [rn]
            | DomainDelta(subexp) -> rcr subexp
            | Cmp(_,_,_)          -> []
            | CmpOrList(_,_)      -> []
            | Lift(_,subexp)      -> rcr subexp
(***** BEGIN EXISTS HACK *****)
            | Exists(subexp)      -> rcr subexp
(***** END EXISTS HACK *****)
         end)
         expr

(**
   Obtain the set of all externals appearing in the specified Calculus 
   expression
   @param expr  A Calculus expression
   @return      The set of all external names that appear in [expr]
*)
let rec externals_of_expr (expr:expr_t): string list =
   let rcr a = externals_of_expr a in
      CalcRing.fold
         ListAsSet.multiunion
         ListAsSet.multiunion
         (fun x -> x)
         (fun lf -> begin match lf with
            | Value(_)            -> []
            | External(en,_,_,_,Some(ivc)) -> en::(externals_of_expr ivc)
            | External(en,_,_,_,None) -> [en]
            | AggSum(_, subexp)   -> rcr subexp
            | Rel(_,_)            -> []
            | DeltaRel(_,_)       -> []
            | DomainDelta(subexp) -> rcr subexp
            | Cmp(_,_,_)          -> []
            | CmpOrList(_,_)    -> []
            | Lift(_,subexp)      -> rcr subexp
(***** BEGIN EXISTS HACK *****)
            | Exists(subexp)      -> rcr subexp
(***** END EXISTS HACK *****)
         end)
         expr

(**
   Compute the degree (as defined by the DBtoaster PODS paper) of the
   specified Calculus expression
   @param expr  A Calculus expression
   @return      The degree of [expr]
*)
let rec degree_of_expr (expr:expr_t): int =
   let rcr a = degree_of_expr a in
      CalcRing.fold
         ListExtras.max
         ListExtras.sum
         (fun (x:int) -> x)
         (fun lf -> begin match lf with
            | Value(_)            -> 0
            | External(_,_,_,_,_) -> 0
            | AggSum(_, subexp)   -> rcr subexp
            | Rel(rn,_)           -> 1
            | DeltaRel(rn,_)      -> 1
            | DomainDelta(subexp) -> rcr subexp
            | Cmp(_,_,_)          -> 0
            | CmpOrList(_,_)      -> 0
            | Lift(_,subexp)      -> rcr subexp
(***** BEGIN EXISTS HACK *****)
            | Exists(subexp)      -> rcr subexp
(***** END EXISTS HACK *****)
         end)
         expr


(*** Iteration operations ***)
(**
   Recursively fold over the elements of a Calculus ring expression.  This is
   equivalent to the behavior of CalcRing.fold, except that the fold functions
   are provided with the scope and schema of the expression at each node.

   {b WARNING}: [Calculus.fold] does {b NOT} descend into AggSums, Lifts, or 
   externals.  This must be done manually.
   @param scope    (optional) The scope in which [e] is evaluated
   @param schema   (optional) The schema expected of [e]
   @param sum_fn   The function to fold down elements joined by a sum
   @param prod_fn  The function to fold down elements joined by a product
   @param neg_fn   The function to fold down a negated element
   @param leaf_fn  The function to transform a leaf element
   @param e        A Calculus expression
   @return         The final folded value
*)
let rec fold ?(scope = []) ?(schema = [])
             (sum_fn:   schema_t -> 'a list         -> 'a)
             (prod_fn:  schema_t -> 'a list         -> 'a)
             (neg_fn:   schema_t -> 'a              -> 'a)
             (leaf_fn:  schema_t -> CalcRing.leaf_t -> 'a)
             (e: expr_t): 'a =
   let rcr e_scope e_schema e2 = 
      fold ~scope:e_scope ~schema:e_schema sum_fn prod_fn neg_fn leaf_fn e2 
   in
   begin match e with
      | CalcRing.Sum(terms) -> 
         sum_fn (scope,schema) (List.map (rcr scope schema) terms)
      | CalcRing.Prod(terms) -> 
         prod_fn (scope,schema) (ListExtras.scan_map (fun prev curr next ->
            rcr ( 
               (* extend the scope with variables defined by the prev *)
               ListAsSet.multiunion 
                  (scope::(List.map (fun x -> snd (schema_of_expr x)) prev))
            ) (
               (* extend the schema with variables required by the next *)
               ListAsSet.multiunion
                  (schema::(List.map (fun x -> 
                     let (xin, xout) = schema_of_expr x in
                     ListAsSet.union xin xout) next))
            ) curr
         ) terms)
      | CalcRing.Neg(term) -> neg_fn (scope,schema) (rcr scope schema term)
      | CalcRing.Val(leaf) -> leaf_fn (scope,schema) leaf
   end

(**
   Determine whether the indicated expression is a singleton (i.e., it evaluates
   to a constant value, rather than a collection)
   @param scope   (optional) The scope in which [expr] is evaluated
   @param expr    A Calculus expression
   @return        True if [expr] evaluates to a constant
*)
let expr_is_singleton ?(scope=[]) (expr:expr_t): bool =
    (ListAsSet.diff  (snd (schema_of_expr expr))  scope ) = []

(**
   Recursively rewrite the elements of a Calculus ring expression.  This is
   similar to the behavior of CalcRing.fold, except that the fold functions
   are provided with the scope and schema of the expression at each node, and 
   that the folded value is a Calculus expression.

   {b WARNING}: [Calculus.rewrite] {b does} descend into AggSums, Lifts, and 
   externals.  This {b UNLIKE} [Calculs.fold].
   @param scope    (optional) The scope in which [e] is evaluated
   @param schema   (optional) The schema expected of [e]
   @param sum_fn   The function to fold down elements joined by a sum
   @param prod_fn  The function to fold down elements joined by a product
   @param neg_fn   The function to fold down a negated element
   @param leaf_fn  The function to transform a leaf element
   @param leaf_descend_fn The function to decide whether to descent 
   @param e        A Calculus expression
   @return         The final folded value
*)
let rec rewrite ?(scope = []) ?(schema = []) 
             (sum_fn:   schema_t -> expr_t list     -> expr_t)
             (prod_fn:  schema_t -> expr_t list     -> expr_t)
             (neg_fn:   schema_t -> expr_t          -> expr_t)
             (leaf_fn:  schema_t -> CalcRing.leaf_t -> expr_t)
             (leaf_descend_fn: schema_t -> CalcRing.leaf_t -> bool)
             (e: expr_t): expr_t =
   let rcr e_scope e_schema = 
      rewrite ~scope:e_scope ~schema:e_schema 
        sum_fn prod_fn neg_fn leaf_fn leaf_descend_fn
   in   
   begin match e with
      | CalcRing.Sum(terms) -> 
         sum_fn (scope,schema) (List.map (rcr scope schema) terms)
      | CalcRing.Prod(terms) -> 
         prod_fn (scope,schema) (ListExtras.scan_map2 (fun prev curr next ->
            rcr ( 
               (* extend the scope with variables defined by the prev *)
               ListAsSet.multiunion 
                  (scope::(List.map (fun x -> snd (schema_of_expr x)) prev))
            ) (
               (* extend the schema with variables required by the next *)
               ListAsSet.multiunion
                  (schema::(List.map (fun x -> 
                     let (xin, xout) = schema_of_expr x in
                     ListAsSet.union xin xout) next))
            ) curr
         ) terms)
      | CalcRing.Neg(term) -> neg_fn (scope,schema) (rcr scope schema term)
      | CalcRing.Val(lf) -> leaf_fn (scope,schema) (
          let (lf_ivars, lf_ovars) = schema_of_expr (CalcRing.mk_val lf) in
          let lf_vars = ListAsSet.union lf_ivars lf_ovars in
          let lf_scope = ListAsSet.inter scope lf_vars in 
          let lf_schema = ListAsSet.inter (ListAsSet.union scope schema) 
                                          lf_ovars in
          begin match lf with
              | AggSum(gb_vars,sub_t) -> 
                 if (leaf_descend_fn (lf_scope, lf_schema) lf) 
                 then (AggSum(gb_vars,(rcr lf_scope lf_schema sub_t)))
                 else lf
              | Lift(v,sub_t) -> 
                 let sub_scope = ListAsSet.diff lf_scope [v] in 
                 let sub_schema = ListAsSet.diff lf_schema [v] in
                 if (leaf_descend_fn (lf_scope, lf_schema) lf) 
                 then (Lift(v,(rcr sub_scope sub_schema sub_t)))
                 else lf
              | External(en, eiv, eov, et, Some(em)) ->
                 if (leaf_descend_fn (lf_scope, lf_schema) lf) 
                 then (External(en, eiv, eov, et, Some(rcr eiv eov em)))
                 else lf
              | Exists(subexp) -> 
                 if (leaf_descend_fn (lf_scope, lf_schema) lf) 
                 then (Exists(rcr lf_scope lf_schema subexp))
                 else lf
              | _ -> lf
          end)
   end

(**
   Recursively rewrite the leaf elements of a Calculus ring expression.  This is
   similar to the behavior of CalcRing.fold, except that the fold functions
   are provided with the scope and schema of the expression at each node, and 
   that the folded value is a Calculus expression.

   {b WARNING}: [Calculus.rewrite_leaves] {b does} descend into AggSums, Lifts, 
   and externals.  This {b UNLIKE} [Calculs.fold].
   @param scope    (optional) The scope in which [e] is evaluated
   @param schema   (optional) The schema expected of [e]
   @param leaf_fn  The function to transform a leaf element
   @param leaf_descend_fn The function to decide whether to descent    
   @param e        A Calculus expression
   @return         The final folded value
*)
let rewrite_leaves ?(scope = []) ?(schema = []) 
                   (leaf_fn:schema_t -> CalcRing.leaf_t -> expr_t)
                   (leaf_descend_fn:schema_t -> CalcRing.leaf_t -> bool)
                   (e: expr_t): expr_t =
   rewrite ~scope:scope ~schema:schema 
      (fun _ e -> CalcRing.mk_sum e)
      (fun _ e -> CalcRing.mk_prod e)
      (fun _ e -> CalcRing.mk_neg e)
      leaf_fn 
      leaf_descend_fn e

(**
   Erase all the IVC metadata from externals in the specified expression
   @param expr  A Calculus expression
   @return      [expr] with all Externals rewritten to eliminate IVC metadata
*)
let strip_calc_metadata:(expr_t -> expr_t) =
   rewrite_leaves (fun _ lf -> match lf with
      | External(en, eiv, eov, et, em) ->
         CalcRing.mk_val (External(en, eiv, eov, et, None))
      | _ -> CalcRing.mk_val lf
   ) (fun _ _ -> true)

(**
   Obtain a list of all variables that appear in the specified expression, even 
   if those variables are projected away or otherwise invisible from the 
   outside.
   @param expr  A Calculus expression
   @return      A list set of all variables that appear somewhere in [expr]
*)
let rec all_vars (expr:expr_t): var_t list =
   CalcRing.fold (ListAsSet.multiunion) (ListAsSet.multiunion) (fun x->x)
      (fun lf -> begin match lf with
         | Value(v) -> ListAsSet.uniq (Arithmetic.vars_of_value v)
         | External(_,iv,ov,_,m) -> 
            ListAsSet.uniq (iv @ ov @ (match m with | None -> []
                                                    | Some(s) -> all_vars s))
         | AggSum(_, subexp) -> all_vars subexp
         | Rel(_,ov) -> ListAsSet.uniq ov
         | DeltaRel(_,ov) -> ListAsSet.uniq ov
         | DomainDelta(subexp) -> all_vars subexp
         | Cmp(_,v1,v2) -> ListAsSet.uniq ((Arithmetic.vars_of_value v1) @
                                           (Arithmetic.vars_of_value v2))
         | CmpOrList(v,_) -> Arithmetic.vars_of_value v
         | Lift(var,subexp) -> ListAsSet.union [var] (all_vars subexp)
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp) -> all_vars subexp
(***** END EXISTS HACK *****)
      end)
      expr

(**
   Given an expression and a variable mapping, generate an extended mapping that
   ensures that existing variable names don't get clobbered (by remapping those 
   names to new, distinct ones).
   @param mapping    A variable mapping
   @param expr       A Calculus expression
   @return           [mapping] extended such that it doesn't clobber any 
                     existing variables in [expr]
*)
let find_safe_var_mapping (mapping:(var_t,var_t) ListAsFunction.table_fn_t) 
                          (expr:expr_t): 
                          (var_t,var_t) ListAsFunction.table_fn_t =
   let expr_vars = all_vars expr in
   let static_names = 
      ListAsSet.diff (expr_vars) (ListAsFunction.dom mapping) in
   let problem_names = 
      ListAsSet.inter static_names (ListAsFunction.img mapping) in
   let final_var_names = 
      ListAsSet.diff (ListAsSet.union static_names (ListAsFunction.img mapping))
                     problem_names
   in
   (* Use a simple iterative approach to find a safe name to reassign each 
      problem names to. *)
   let (safe_mapping,_) = 
      List.fold_left (fun (curr_mapping,curr_var_names) 
                          (prob_name,prob_type) ->
         let rec fix_name n i =
            let attempt = n^"_"^(string_of_int i) in
            if List.mem attempt curr_var_names 
            then fix_name n (i+1)
            else attempt
         in 
         let fixed = (fix_name prob_name 1, prob_type) in
            (  ((prob_name,prob_type), fixed)::curr_mapping, 
               (fst fixed)                   ::curr_var_names
            )
      ) (mapping, List.map fst final_var_names) problem_names
   in
      safe_mapping

(**
   Apply a given variable mapping to the specified Calculus expression.
   @param mapping    A variable mapping
   @param expr       A Calculus expression
   @return           [expr] with all variables subjected to [mapping]
*)
let rename_vars (mapping:(var_t,var_t)ListAsFunction.table_fn_t) 
                (expr:expr_t):expr_t = 
   let remap_one = (ListAsFunction.apply_if_present mapping) in
   let remap = List.map remap_one in
   let remap_value = Arithmetic.rename_vars mapping in
   rewrite_leaves
      (fun _ lf -> CalcRing.mk_val (begin match lf with
         | Value(v)                   -> Value(remap_value v)
         | External(en,eiv,eov,et,em) -> External(en, remap eiv, remap eov,   
                                                  et, em)
         | AggSum(gb_vars, subexp)    -> AggSum(remap gb_vars, subexp)
         | Rel(rn,rv)                 -> Rel(rn, remap rv)
         | DeltaRel(rn,rv)            -> DeltaRel(rn, remap rv)
         | DomainDelta(subexp)        -> DomainDelta(subexp)
         | Cmp(op,v1,v2)              -> Cmp(op, remap_value v1, 
                                                 remap_value v2)
         | CmpOrList(v,cl)            -> CmpOrList(remap_value v, cl)
         | Lift(var,subexp)           -> Lift(remap_one var, subexp)
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp)             -> Exists(subexp)
(***** END EXISTS HACK *****)
      end)) (fun _ _ -> true)
      expr

(* BEGIN CARDINALITY HACK *)
let rec expr_has_deltarels expr = 
   let contains s1 s2 =
      try
         let len = String.length s2 in
         for i = 0 to String.length s1 - len do
            if String.sub s1 i len = s2 then raise Exit
         done;
         false
      with Exit -> true   
   in
   let rcr = expr_has_deltarels in
   match expr with
      | CalcRing.Prod(pl) -> List.exists (fun x -> x) (List.map rcr pl)
      | CalcRing.Sum(sl) -> List.for_all (fun x -> x) (List.map rcr sl)
      | CalcRing.Neg(e) -> rcr e
      | CalcRing.Val(lf) -> 
         begin match lf with
            | Value _ | Cmp _ | CmpOrList _ | Rel _ -> false
            | DeltaRel _ -> true
            | External(ename,_,_,_,_) -> (contains ename "DELTA" || 
                                          contains ename "DOMAIN")
            | AggSum(gb_vars, subexp) -> rcr subexp 
            | DomainDelta(subexp) -> rcr subexp
            | Lift(v, subexp) -> rcr subexp
   (***** BEGIN EXISTS HACK *****)
            | Exists(subexp) -> rcr subexp
   (***** END EXISTS HACK *****)
         end

let expr_has_low_cardinality scope expr = 
   expr_has_deltarels expr 

let expr_has_high_cardinality scope expr = 
   not (expr_has_low_cardinality scope expr) && 
   (rels_of_expr expr <> [] || externals_of_expr expr <> [])

(* END CARDINALITY HACK *)
   
(**
   Determine whether two expressions safely commute (in a product).
   
   @param scope  (optional) The scope in which the expression is evaluated 
                 (having a subset of the scope may produce false negatives)
   @param e1     The left hand side expression
   @param e2     The right hand side expression
   @return       true if and only if e1 * e2 = e2 * e1
*)
let commutes ?(scope = []) (e1:expr_t) (e2:expr_t): bool =
   let (_,ovar1) = schema_of_expr ~lift_group_by_vars_are_inputs:true e1 in
   let (ivar2,_) = schema_of_expr ~lift_group_by_vars_are_inputs:true e2 in
      (* Commutativity within a product is possible if and only if all input 
         variables on the right hand side do not enter scope on the left hand 
         side.  A variable enters scope in an expression if it is an output 
         variable, and not already present in the scope (which we're given as  
         a parameter).  *)
   (ListAsSet.inter (ListAsSet.diff ovar1 scope) ivar2) = [] 
(* START CARDINALITY HACK *)   
   && not (expr_has_low_cardinality scope e1 && 
           expr_has_high_cardinality scope e2) 
(* END CARDINALITY HACK *)

(**
   Generate a singleton term from a list of column/expression pairs
   @param multiplicity   (optional) The multiplicity of the singleton term
   @param cols_and_vals  The columns+expressions to compute the column values
   @return               An expression to generate the specified singleton
*)
let singleton ?(multiplicity = CalcRing.one) 
              (cols_and_vals:(var_t * expr_t) list) =
   CalcRing.mk_prod (
      multiplicity :: (
         List.map (fun (col, v) -> 
            CalcRing.mk_val (Lift(col, v))
         ) cols_and_vals
      )
   )

(**
   Generate a singleton term from a list of column/value pairs
   @param multiplicity   (optional) The multiplicity of the singleton term
   @param cols_and_vals  The columns+values of the columns
   @return               An expression to generate the specified singleton
*)
let value_singleton ?(multiplicity = CalcRing.one)
                    (cols_and_vals:(var_t * value_t) list) =
   singleton ~multiplicity:multiplicity 
             (List.map (fun (c,v) -> (c,(CalcRing.mk_val (Value(v))))) 
                       cols_and_vals)

(**
   Determine whether two expressions compute the same thing.  If so, generate
   a schema mapping between the two expressions.
   
   Note that cmp_exprs is only best-effort, and may return false negatives.
   If it returns a mapping, the mapping is guaranteed to be correct.  However, 
   it might not return a mapping, even though one exists.
      
   @param cmp_opts  (optional) See [CalcRing.cmp_opt_t]
   @param e1        The first expression
   @param e2        The second expression
   @return          If [e1] and [e2] compute the same thing, return a Some-
                    wrapped mapping from the schema/variables of [e1] to the 
                    schema/variables of [e2], suitable for application with the 
                    [ListAsFunction] module.  Otherwise return None.  
*)
let rec cmp_exprs ?(cmp_opts:CalcRing.cmp_opt_t list = 
                        if Debug.active "STRONG-EXPR-EQUIV" then 
                        CalcRing.full_cmp_opts
                        else if Debug.active "WEAK-EXPR-EQUIV" then [] 
                        else CalcRing.default_cmp_opts) 
                  ?(validate = (fun _ -> true))
                  (e1:expr_t) (e2:expr_t):((var_t * var_t) list option) =
   let validate_mapping wrapped_mapping = 
      begin match wrapped_mapping with
         | None -> None
         | Some(mapping) -> 
            if validate mapping then Some(mapping) else None
      end
   in
   let rcr a b = cmp_exprs ~cmp_opts:cmp_opts a b in
   let merge_variables rv1 rv2 =
      if ((List.length rv1) != (List.length rv2)) then None
      else try 
         ListAsFunction.multimerge (List.map2 (fun a b -> [a,b]) rv1 rv2)
       with Not_found -> None
   in   
   let merge components = 
      validate_mapping (ListAsFunction.multimerge components) 
   in
   validate_mapping (CalcRing.cmp_exprs ~cmp_opts:cmp_opts merge merge
      (fun lf1 lf2 -> validate_mapping (
      begin match (lf1,lf2) with
         | ((Value v1), (Value v2)) -> 
            Arithmetic.cmp_values v1 v2
         
         | ((AggSum(gb1, sub1)), (AggSum(gb2, sub2))) ->
            begin match rcr sub1 sub2 with
               | None -> None
               | Some(mappings) ->
                  let map_fn = ListAsFunction.apply_strict mappings in
                  if List.length gb1 = List.length gb2 &&
                     ListAsSet.seteq (List.map map_fn gb1) gb2
                  (* Relevant mappings involve only input/output variables *)
                  then Some(List.map (fun v -> (v, map_fn v)) 
                              (ListAsSet.union gb1 (fst(schema_of_expr sub1))))
                  else None
            end
                  
         | ((DomainDelta(sub1)),(DomainDelta(sub2))) -> rcr sub1 sub2

         | ((DeltaRel(rn1,rv1)), (DeltaRel(rn2,rv2))) ->
            if (rn1 <> rn2) then None else merge_variables rv1 rv2

         | ((Rel(rn1,rv1)), (Rel(rn2,rv2))) ->
            if (rn1 <> rn2) then None else merge_variables rv1 rv2

         | ((External(en1,eiv1,eov1,et1,em1)), 
            (External(en2,eiv2,eov2,et2,em2))) ->
            if ((en1 = en2) && (et1 = et2))
            then 
               let mapping = 
                  begin match (em1, em2) with
                     | (None, None)         -> Some([])
                     | (Some(s1), Some(s2)) -> rcr s1 s2
                     | _                    -> None
                  end
               in
               begin match mapping with
                  | None -> None
                  | Some(s) -> 
                     match (merge_variables eiv1 eiv2, 
                            merge_variables eov1 eov2) with
                        | (Some(eiv_mapping), Some(eov_mapping)) ->
                           ListAsFunction.multimerge [s;
                                                      eiv_mapping;
                                                      eov_mapping]
                        | (_,_) -> None
               end
            else None

         | ((Cmp(op1,va1,vb1)), (Cmp(op2,va2,vb2))) ->
            if op1 <> op2 then None else
            begin match (Arithmetic.cmp_values va1 va2,
                         Arithmetic.cmp_values vb1 vb2) with
               | ((Some(mappings1)),(Some(mappings2))) ->
                  ListAsFunction.merge mappings1 mappings2
               | _ -> None
            end

         | ((CmpOrList(v1,l1)), (CmpOrList(v2,l2))) ->   
            if (ListAsSet.seteq l1 l2) then Arithmetic.cmp_values v1 v2 
            else None
            
         | ((Lift(v1,sub1)), (Lift(v2,sub2))) ->
            begin match rcr sub1 sub2 with
               | None -> None
               | Some(new_mappings) -> ListAsFunction.merge [v1,v2] new_mappings
            end
         
(***** BEGIN EXISTS HACK *****)
         | ((Exists(sub1)),(Exists(sub2))) -> rcr sub1 sub2
(***** END EXISTS HACK *****)
            
         | (_,_) -> None
      end)
   ) e1 e2)

(**
   Determine whether two expressions produce entirely identical outputs.  This 
   is a step more strict than cmp_exprs:

   While cmp_exprs attempts to find a variable mapping from one expression to 
   another, exprs_are_identical determines whether the schema of the expressions
   are identical (and contain the same things).
*)   
let exprs_are_identical ?(cmp_opts:CalcRing.cmp_opt_t list = 
                              if Debug.active "STRONG-EXPR-EQUIV" then 
                              CalcRing.full_cmp_opts
                              else if Debug.active "WEAK-EXPR-EQUIV" then [] 
                              else CalcRing.default_cmp_opts) 
                        (e1:expr_t) (e2:expr_t): bool = 
   None <> cmp_exprs ~cmp_opts:cmp_opts 
                     ~validate:ListAsFunction.is_identity
                     e1 e2

(** 
   The full name of a variable includes its type.  That is, a variable with the
   same string name but different types will be treated as different variables.
   
   This can be a bit confusing, so in order to ameliorate this confusion, we 
   have this handy dandy method that will raise an error if the same variable
   name appears twice in the expression. 
*)
let sanity_check_variable_names expr =
   let vars = all_vars expr in
   let var_names = ListExtras.reduce_assoc vars in
      List.iter (fun (vn, vtypes) ->
         if List.length vtypes > 1 then
            bail_out expr (
               "Variable name sanity check failed.  Variable "^vn^
               " appears with types: "^
               (ListExtras.ocaml_of_list string_of_type vtypes)
            )
      ) var_names

(**
  Check whether the given expression produces only 
  tuples with multiplicity zero or one.
*)
let rec expr_has_binary_multiplicity ?(scope = []) expr = 
   fold ~scope:scope
      (fun _ sl -> match sl with | [x] -> x | _ -> false)
      (fun _ pl -> List.for_all (fun x -> x) pl) 
      (fun _ x -> x)
      (fun (scope, _) lf -> begin match lf with
         | Value(ValueRing.Val(AConst(CInt(x)))) when x = 0 || x = 1 -> true
         | Value _ -> false
         | External _ -> false
         | AggSum(gb_vars, subexp) -> 
             (* if AggSum is singleton and subexp returns true *)
             ListAsSet.diff gb_vars scope = [] &&
             expr_has_binary_multiplicity ~scope:gb_vars subexp
         | Rel _ -> false
         | DeltaRel _ -> false
         | DomainDelta _ -> true
         | Cmp _ -> true
         | CmpOrList _ -> true
         | Lift _ -> true
(***** BEGIN EXISTS HACK *****)
         | Exists _ -> true
(***** END EXISTS HACK *****)
      end)
      expr 


let mk_fresh_var = FreshVariable.declare_class "calculus/Calculus" ""

(** Erase AggSums from the given expression, while renaming 
    any new variables that might get introduced. The method 
    descends only into AggSums. Assumes expr is monomial. *)
let rec erase_aggsums ?(scope = []) (expr: expr_t) : expr_t = 
   let map_var (vname, vtype) =
      ((vname, vtype), (mk_fresh_var ~inline:vname (), vtype)) 
   in
   rewrite_leaves
      (fun (scope, schema) lf -> begin match lf with
         | AggSum(gb_vars, subexp) ->           
         begin
           (* Removing AggSum might introduce variable name conflicts *)
           let subexp_ovars = snd (schema_of_expr subexp) in
           let new_ovars = ListAsSet.diff subexp_ovars gb_vars in
           let conflict_vars = 
             ListAsSet.inter new_ovars (ListAsSet.union scope schema) 
           in
           let rewritten_subexp = 
             if (conflict_vars == []) then subexp
             else begin
               let unsafe_mapping = (List.map map_var conflict_vars) in
               let safe_mapping = 
                 find_safe_var_mapping unsafe_mapping subexp 
               in
                 rename_vars safe_mapping subexp             
             end
           in
             erase_aggsums ~scope:scope rewritten_subexp
         end
        | _ -> CalcRing.mk_val lf
      end)
      (fun _ _ -> false)
      expr


(* Construction Helpers *)

(** Create a Value expression *)
let mk_value (value: value_t) : expr_t = 
   CalcRing.mk_val (Value(value))

(** Create an AggSum expression if necessary *)            
let mk_aggsum (gb_vars: var_t list) (expr: expr_t) : expr_t =
   let expr_ovars = snd (schema_of_expr expr) in
   let new_gb_vars = ListAsSet.inter gb_vars expr_ovars in
   if (ListAsSet.seteq expr_ovars new_gb_vars) then expr
   else CalcRing.mk_val (AggSum(new_gb_vars, expr))

(** Create a Rel expression *)            
let mk_rel (name: string) (vars: var_t list) : expr_t =
   CalcRing.mk_val (Rel(name, vars))
   
(** Create a DeltaRel expression *)            
let mk_deltarel (name: string) (vars: var_t list) : expr_t =
   CalcRing.mk_val (DeltaRel(name, vars))

(** Create a DomainDelta expression *)            
let mk_domain (expr: expr_t) : expr_t =
   CalcRing.mk_val (DomainDelta(expr))

(** Create an External expression *)   
let mk_external (name: string) (ivars: var_t list) (ovars: var_t list)
                (base_type: type_t) (ivc_expr: expr_t option) : expr_t =
   CalcRing.mk_val (External(name, ivars, ovars, base_type, ivc_expr))

(** Create a Cmp expression *)
let mk_cmp (op: cmp_t) (lhs: value_t) (rhs: value_t) : expr_t = 
   CalcRing.mk_val (Cmp(op, lhs, rhs))
   
(** Create a CmpOrList expression *)
let mk_cmp_or_list (v: value_t) (consts: const_t list) : expr_t = 
   CalcRing.mk_val (CmpOrList(v, consts))

(** Create a Lift expression *)   
let mk_lift (var: var_t) (expr: expr_t) : expr_t =
   CalcRing.mk_val (Lift(var, expr))
      
(***** BEGIN EXISTS HACK *****)
(** Create an Exists expression *)      
let mk_exists (expr: expr_t) : expr_t =
   CalcRing.mk_val (Exists(expr))
   
(***** END EXISTS HACK *****)
      
