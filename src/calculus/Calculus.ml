(* 
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

type 'meta_t external_t = 
   string *       (* The external's name *)
   var_t list *   (* The external's input variables *)
   var_t list *   (* The external's output variables *)
   type_t *       (* The external's type (return value) *)
   'meta_t option (* Metadata associated with the external *)

type ('term_t) calc_leaf_t = 
   | Value    of value_t
   | AggSum   of var_t list * 'term_t (* vars listed are group-by vars (i.e., 
                                         the schema of this AggSum) *)
   | Rel      of string * var_t list * type_t
   | External of 'term_t external_t
   | Cmp      of cmp_t * value_t * value_t
   | Lift     of var_t * 'term_t

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
CalcRing : Ring.Ring with type leaf_t = CalcBase.t
         = Ring.Make(CalcBase)

type expr_t = CalcRing.expr_t
                (* Scope:   Available Input Variables
                   x
                   Schema:  Expected Output Variables *)
type schema_t = (var_t list * var_t list)

(*** Stringifiers ***)
let rec string_of_leaf (leaf:CalcRing.leaf_t): string = 
   begin match leaf with
      | Value(v)                -> Arithmetic.string_of_value v
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
      | Rel(rname, rvars, _)    -> 
         rname^"("^(ListExtras.string_of_list ~sep:", " string_of_var rvars)^")"
      | Cmp(op,subexp1,subexp2) -> 
         "["^(string_of_value subexp1)^" "^
             (string_of_cmp op)^
         " "^(string_of_value subexp2)^"]"
      | Lift(target, subexp)    -> 
         "("^(string_of_var target)^" ^= "^(string_of_expr subexp)^")"
   end
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

exception CalculusException of expr_t * string
;;
let bail_out expr msg = 
   Debug.print "LOG-CALC-FAILURES" (fun () ->
      msg^" while processing "^(string_of_expr expr)
   );
   raise (CalculusException(expr, msg))
;;
(*** Informational Operations ***)
let rec schema_of_expr (expr:expr_t): (var_t list * var_t list) =
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
                           (  ListAsSet.union 
                                 old_ivars
                                 (ListAsSet.diff new_ivars old_ovars),
                              ListAsSet.union old_ovars new_ovars 
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
         | Rel(_,rvars,_) -> ([],rvars)
         | Cmp(_,v1,v2) ->
            (ListAsSet.union (vars_of_value v1) (vars_of_value v2), [])
         | Lift(target, subexp) ->
            let ivars, ovars = rcr subexp in
               (ivars, ListAsSet.union [target] ovars)
      end)
      expr

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
         | Rel(_,_,rtype)          -> rtype
         | Cmp(_,_,_)              -> TBool
         | Lift(_,_)               -> TInt
      end)
      expr

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
            | Rel(rn,_,_)         -> [rn]
            | Cmp(_,_,_)          -> []
            | Lift(_,subexp)      -> rcr subexp
         end)
         expr

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
            | Rel(rn,_,_)         -> 1
            | Cmp(_,_,_)          -> 0
            | Lift(_,subexp)      -> rcr subexp
         end)
         expr

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

let rec expr_is_singleton ?(scope=[]) (expr:expr_t): bool =
   (ListAsSet.inter scope (snd (schema_of_expr expr))) = []

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
                  | _ -> lf
         end)
   ) e

let rewrite_leaves ?(scope = []) ?(schema = [])
                   (leaf_fn:schema_t -> CalcRing.leaf_t -> expr_t)
                   (e: expr_t): expr_t =
   rewrite ~scope:scope ~schema:schema
      (fun _ e -> CalcRing.mk_sum e)
      (fun _ e -> CalcRing.mk_prod e)
      (fun _ e -> CalcRing.mk_neg e)
      leaf_fn e

let strip_calc_metadata:(expr_t -> expr_t) =
   rewrite_leaves (fun _ lf -> match lf with
      | External(en, eiv, eov, et, em) ->
         CalcRing.mk_val (External(en, eiv, eov, et, None))
      | _ -> CalcRing.mk_val lf
   )

let rec all_vars (expr:expr_t): var_t list =
   CalcRing.fold (ListAsSet.multiunion) (ListAsSet.multiunion) (fun x->x)
      (fun lf -> begin match lf with
         | Value(v) -> ListAsSet.uniq (Arithmetic.vars_of_value v)
         | External(_,iv,ov,_,m) -> 
            ListAsSet.uniq (iv @ ov @ (match m with | None -> []
                                                    | Some(s) -> all_vars s))
         | AggSum(_, subexp) -> all_vars subexp
         | Rel(_,ov,_) -> ListAsSet.uniq ov
         | Cmp(_,v1,v2) -> ListAsSet.uniq ((Arithmetic.vars_of_value v1) @
                                           (Arithmetic.vars_of_value v2))
         | Lift(var,subexp) -> ListAsSet.union [var] (all_vars subexp)
      end)
      expr

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
         | Rel(rn,rv,rt)              -> Rel(rn, remap rv, rt)
         | Cmp(op,v1,v2)              -> Cmp(op, remap_value v1, 
                                                 remap_value v2)
         | Lift(var,subexp)           -> Lift(remap_one var, subexp)
      end))
      expr
   

let commutes ?(scope = []) (e1:expr_t) (e2:expr_t): bool =
   let (_,ovar1) = schema_of_expr e1 in
   let (ivar2,_) = schema_of_expr e2 in
      (* Commutativity within a product is possible if and only if all input 
         variables on the right hand side do not enter scope on the left hand 
         side.  A variable enters scope in an expression if it is an output 
         variable, and not already present in the scope (which we're given as  
         a parameter).  *)
   (ListAsSet.inter (ListAsSet.diff ovar1 scope) ivar2) = []

let rec cmp_exprs (e1:expr_t) (e2:expr_t):((var_t * var_t) list option) = 
   let rcr a b = 
      cmp_exprs a b
   in
   CalcRing.cmp_exprs Function.multimerge Function.multimerge
                     (fun lf1 lf2 -> 
      begin match (lf1,lf2) with
         | ((Value v1), (Value v2)) ->
            Arithmetic.cmp_values v1 v2
         
         | ((AggSum(gb1, sub1)), (AggSum(gb2, sub2))) ->
            begin match rcr sub1 sub2 with
               | None -> None
               | Some(mappings) -> 
                  Function.merge mappings (List.combine gb1 gb2)
            end
         
         | ((Rel(rn1,rv1,rt1)), (Rel(rn2,rv2,rt2))) ->
            if (rn1 <> rn2) || (rt1 <> rt2) then None else
               Some(List.combine rv1 rv2)
            
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
                     Function.multimerge 
                        [s; (List.combine eiv1 eiv2); (List.combine eov1 eov2)]
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
            
         | (_,_) -> None
      end
   ) e1 e2

