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

open Types
open Arithmetic

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
   | External of 'term_t external_leaf_t     (** An external (map) *)
   | Cmp      of cmp_t * value_t * value_t   (** A comparison between two 
                                                 values *)
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
      type t = (CalcRing.expr_t) calc_leaf_t
      val  zero: t
      val  one: t
   end = struct
      type t = (CalcRing.expr_t) calc_leaf_t
      let zero = Value(mk_int 0)
      let one  = Value(mk_int 1)
   end and
(** The Calculus ring *)
CalcRing : Ring.Ring with type leaf_t = CalcBase.t
         = Ring.Make(CalcBase)

(** Elements of the Calculus ring. *)
type expr_t = CalcRing.expr_t
(** Base type for describing an External (map). *)
type external_t = CalcRing.expr_t external_leaf_t

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
      | Cmp(op,subexp1,subexp2) -> 
         "{"^(string_of_value subexp1)^" "^
             (string_of_cmp op)^
         " "^(string_of_value subexp2)^"}"
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
      if Debug.active "PRINT-VERBOSE" then (" U ", " |><| ", "(<>:-1)*")
                                      else (" + ", " * ", "-1 * ")
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
let rec schema_of_expr ?(lift_group_by_vars_are_inputs = false)
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
               output variable.  This is correct as long as A is in the scope
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
         | Cmp(_,v1,v2) ->
            (ListAsSet.union (vars_of_value v1) (vars_of_value v2), [])
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
let rec type_of_expr (expr:expr_t): type_t =
   let rcr a = type_of_expr a in
   CalcRing.fold
      (escalate_type_list ~opname:"[+]")
      (escalate_type_list ~opname:"[*]")
      (fun x->x)
      (fun lf -> begin match lf with
         | Value(v)                -> (type_of_value v)
         | External(_,_,_,etype,_) -> etype
         | AggSum(_, subexp)       -> rcr subexp
         | Rel(_,_)                -> TInt
         | Cmp(_,_,_)              -> TInt (* Since we're not using truth, but
                                              rather truthiness: the # of
                                              truths encountered thus far *)
         | Lift(_,_)               -> TInt
(***** BEGIN EXISTS HACK *****)
         | Exists(_)               -> TInt
(***** END EXISTS HACK *****)
      end)
      expr

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
            | External(_,_,_,_,Some(em)) -> rels_of_expr em
            | AggSum(_, subexp)   -> rcr subexp
            | Rel(rn,_)           -> [rn]
            | Cmp(_,_,_)          -> []
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
            | Rel(_,_)           -> []
            | Cmp(_,_,_)          -> []
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
            | Cmp(_,_,_)          -> 0
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
                  (schema::(List.map (fun x -> fst (schema_of_expr x)) next))
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
   @param e        A Calculus expression
   @return         The final folded value
*)
let rec rewrite ?(scope = []) ?(schema = [])
             (sum_fn:   schema_t -> expr_t list     -> expr_t)
             (prod_fn:  schema_t -> expr_t list     -> expr_t)
             (neg_fn:   schema_t -> expr_t          -> expr_t)
             (leaf_fn:  schema_t -> CalcRing.leaf_t -> expr_t)
             (e: expr_t): expr_t =
   let rcr e_scope e_schema = 
      rewrite ~scope:e_scope ~schema:e_schema sum_fn prod_fn neg_fn leaf_fn
   in
   fold ~scope:scope ~schema:schema sum_fn prod_fn neg_fn 
        (fun (local_scope, local_schema) lf ->
            leaf_fn (local_scope, local_schema) (begin match lf with
                  | AggSum(gb_vars,sub_t) -> 
                     (AggSum(gb_vars,(rcr local_scope gb_vars sub_t)))
                  | Lift(v,sub_t) -> 
                     (Lift(v,(rcr local_scope 
                                  (ListAsSet.diff local_schema [v])
                                  sub_t)))
                  | External(en, eiv, eov, et, Some(em)) ->
                     (External(en, eiv, eov, et, Some(rcr eiv eov em)))
(***** BEGIN EXISTS HACK *****)
                  | Exists(subexp) -> 
                     (Exists(rcr local_scope local_schema subexp))
(***** END EXISTS HACK *****)
                  | _ -> lf
         end)
   ) e


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
   @param e        A Calculus expression
   @return         The final folded value
*)
let rewrite_leaves ?(scope = []) ?(schema = [])
                   (leaf_fn:schema_t -> CalcRing.leaf_t -> expr_t)
                   (e: expr_t): expr_t =
   rewrite ~scope:scope ~schema:schema
      (fun _ e -> CalcRing.mk_sum e)
      (fun _ e -> CalcRing.mk_prod e)
      (fun _ e -> CalcRing.mk_neg e)
      leaf_fn e

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
   )

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
         | Cmp(_,v1,v2) -> ListAsSet.uniq ((Arithmetic.vars_of_value v1) @
                                           (Arithmetic.vars_of_value v2))
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
let find_safe_var_mapping (mapping:(var_t,var_t) Function.table_fn_t) 
                          (expr:expr_t): (var_t,var_t) Function.table_fn_t =
   let expr_vars = all_vars expr in
   let static_names = ListAsSet.diff (expr_vars) (Function.dom mapping) in
   let problem_names = ListAsSet.inter static_names (Function.img mapping) in
   let final_var_names = 
      ListAsSet.diff (ListAsSet.union static_names (Function.img mapping))
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
let rename_vars (mapping:(var_t,var_t)Function.table_fn_t) 
                (expr:expr_t):expr_t = 
   let remap_one = (Function.apply_if_present mapping) in
   let remap = List.map remap_one in
   let remap_value = Arithmetic.rename_vars mapping in
   rewrite_leaves
      (fun _ lf -> CalcRing.mk_val (begin match lf with
         | Value(v)                   -> Value(remap_value v)
         | External(en,eiv,eov,et,em) -> External(en, remap eiv, remap eov,   
                                                  et, em)
         | AggSum(gb_vars, subexp)    -> AggSum(remap gb_vars, subexp)
         | Rel(rn,rv)                 -> Rel(rn, remap rv)
         | Cmp(op,v1,v2)              -> Cmp(op, remap_value v1, 
                                                 remap_value v2)
         | Lift(var,subexp)           -> Lift(remap_one var, subexp)
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp)             -> Exists(subexp)
(***** END EXISTS HACK *****)
      end))
      expr
   

let commutes ?(scope = []) (e1:expr_t) (e2:expr_t): bool =
   let (_,ovar1) = schema_of_expr ~lift_group_by_vars_are_inputs:true e1 in
   let (ivar2,_) = schema_of_expr ~lift_group_by_vars_are_inputs:true e2 in
      (* Commutativity within a product is possible if and only if all input 
         variables on the right hand side do not enter scope on the left hand 
         side.  A variable enters scope in an expression if it is an output 
         variable, and not already present in the scope (which we're given as  
         a parameter).  *)
   (ListAsSet.inter (ListAsSet.diff ovar1 scope) ivar2) = []


let rec cmp_exprs ?(cmp_opts:CalcRing.cmp_opt_t list = 
                        if Debug.active "WEAK-EXPR-EQUIV" 
                           then [] else CalcRing.default_cmp_opts) 
                   (e1:expr_t) (e2:expr_t):((var_t * var_t) list option) =
   let rcr a b = 
      cmp_exprs ~cmp_opts:cmp_opts a b
   in
   CalcRing.cmp_exprs ~cmp_opts:cmp_opts Function.multimerge Function.multimerge
                     (fun lf1 lf2 -> 
      begin match (lf1,lf2) with
         | ((Value v1), (Value v2)) ->
            Arithmetic.cmp_values v1 v2
         
         | ((AggSum(gb1, sub1)), (AggSum(gb2, sub2))) ->
            begin match rcr sub1 sub2 with
               | None -> None
               | Some(mappings) -> 
                    if ((List.length gb1) = (List.length gb2)) then  
                        Function.merge mappings (List.combine gb1 gb2)
                    else None                     
            end
         
         | ((Rel(rn1,rv1)), (Rel(rn2,rv2))) ->
            if (rn1 <> rn2) then None else
            if ((List.length rv1) = (List.length rv2)) 
            then Some(List.combine rv1 rv2)
            else None

         | ((External(en1,eiv1,eov1,et1,em1)), 
            (External(en2,eiv2,eov2,et2,em2))) ->
            if ((en1 <> en2) || (et1 <> et2))
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
                     if ((List.length eiv1) = (List.length eiv2)) &&
                        ((List.length eov1) = (List.length eov2)) 
                     then Function.multimerge 
                             [ s; 
                               (List.combine eiv1 eiv2); 
                               (List.combine eov1 eov2) ]
                     else None
               end
            else None

         | ((Cmp(op1,va1,vb1)), (Cmp(op2,va2,vb2))) ->
            if op1 <> op2 then None else
            begin match (Arithmetic.cmp_values va1 va2,
                         Arithmetic.cmp_values vb1 vb2) with
               | ((Some(mappings1)),(Some(mappings2))) ->
                  Function.merge mappings1 mappings2
               | _ -> None
            end
            
         | ((Lift(v1,sub1)), (Lift(v2,sub2))) ->
            begin match rcr sub1 sub2 with
               | None -> None
               | Some(new_mappings) -> Function.merge [v1,v2] new_mappings
            end
         
(***** BEGIN EXISTS HACK *****)
         | ((Exists(sub1)),(Exists(sub2))) -> rcr sub1 sub2
(***** END EXISTS HACK *****)
            
         | (_,_) -> None
      end
   ) e1 e2

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