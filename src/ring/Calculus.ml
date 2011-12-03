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

open GlobalTypes
open Arithmetic

type 'meta_t external_t = 
   string *       (* The external's name *)
   var_t list *   (* The external's input variables *)
   var_t list *   (* The external's output variables *)
   type_t *       (* The external's type (return value) *)
   'meta_t        (* Metadata associated with the external *)

type ('term_t,'meta_t) calc_leaf_t = 
   | Value    of ValueRing.expr_t
   | AggSum   of var_t list * 'term_t (* vars listed are group-by vars (i.e., 
                                         the schema of this AggSum) *)
   | Rel      of string * var_t list * type_t
   | External of 'meta_t external_t
   | Cmp      of cmp_t * value_t * value_t
   | Lift     of var_t * 'term_t

module type ExternalMeta = sig
   type meta_t
   val string_of_meta : meta_t -> string
end

module type Calculus = sig
   type external_meta_t
   module rec CalcRing : Ring.Ring with
      type leaf_t = (CalcRing.expr_t, external_meta_t) calc_leaf_t
   type expr_t = CalcRing.expr_t
   
   val string_of_leaf: CalcRing.leaf_t -> string
   val string_of_expr: CalcRing.expr_t -> string
   val schema_of_expr: CalcRing.expr_t -> (var_t list * var_t list)
   val type_of_expr:   CalcRing.expr_t -> type_t
   val rels_of_expr:   CalcRing.expr_t -> string list
end

module Make(T : ExternalMeta) = struct
   module rec 
   CalcBase : sig
         type t = (CalcRing.expr_t,T.meta_t) calc_leaf_t
         val  zero: t
         val  one: t
      end = struct
         type t = (CalcRing.expr_t,T.meta_t) calc_leaf_t
         let zero = Value(mk_int 0)
         let one  = Value(mk_int 1)
      end and
   CalcRing : Ring.Ring with type leaf_t = CalcBase.t
            = Ring.Make(CalcBase)
   
   type expr_t = CalcRing.expr_t
   type external_meta_t = T.meta_t
   
   (*** Stringifiers ***)
   let rec string_of_leaf (leaf:CalcRing.leaf_t): string = 
      begin match leaf with
         | Value(v)                -> Arithmetic.string_of_value v
         | External(ename,eins,eouts,etype,emeta) ->
            ename^
            (ListExtras.ocaml_of_list string_of_var eins)^
            (ListExtras.ocaml_of_list string_of_var eins)^
            (string_of_type etype)^
            (T.string_of_meta emeta)
         | AggSum(gb_vars, subexp) -> 
            "Sum"^(ListExtras.ocaml_of_list  string_of_var gb_vars)^
                  "("^(string_of_expr subexp)^")"
         | Rel(rname, rvars, _)    -> 
            rname^"("^(ListExtras.string_of_list string_of_var rvars)^")"
         | Cmp(op,subexp1,subexp2) -> 
            "("^(string_of_value subexp1)^" "^
                (string_of_cmp op)^
            " "^(string_of_value subexp2)^")"
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
   
   (*** Informational Operations ***)
   let rec schema_of_expr (expr:expr_t): (var_t list * var_t list) =
      let rcr a = schema_of_expr a in
      CalcRing.fold 
         (fun sum_vars ->
            let ivars, ovars = List.split sum_vars in
               if not (ListAsSet.seteq (ListAsSet.multiinter ovars)
                                       (ListAsSet.multiunion ovars))
               then failwith "Calculus expr with sum of incompatible schemas"
               else (ListAsSet.multiunion ivars, ListAsSet.multiunion ovars)
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
               let ivars, ovars = rcr subexp in
                  if not (ListAsSet.seteq (ListAsSet.inter ovars gb_vars)
                                          gb_vars)
                  then failwith "Calculus expr with invalid group by var"
                  else (ivars, gb_vars)
            | Rel(_,rvars,_) -> ([],rvars)
            | Cmp(_,v1,v2) ->
               (ListAsSet.union (vars_of_value v1) (vars_of_value v2), [])
            | Lift(target, subexp) ->
               let ivars, ovars = rcr subexp in
                  if List.mem target ovars
                  then failwith "Calculus lift statement redefines var"
                  else (ivars, target::ovars)
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
               | External(_,_,_,_,_) -> []
               | AggSum(_, subexp)   -> rcr subexp
               | Rel(rn,_,_)         -> [rn]
               | Cmp(_,_,_)          -> []
               | Lift(_,subexp)      -> rcr subexp
            end)
            expr
end

module Translator(C1 : Calculus)(C2 : Calculus) = struct
   let rec translate (translate_meta: C1.external_meta_t external_t -> 
                                      C2.external_meta_t) 
                     (e1:C1.expr_t): C2.expr_t =
      let rcr = translate translate_meta in
      C1.CalcRing.fold 
         C2.CalcRing.mk_sum
         C2.CalcRing.mk_prod
         C2.CalcRing.mk_neg
         (fun (lf:C1.CalcRing.leaf_t) ->
            C2.CalcRing.mk_val
               begin match lf with
                  | Value(v) -> Value(v)
                  | External(ename,eins,eouts,etype,emeta) ->
                     External(ename,eins,eouts,etype,
                              (translate_meta (ename,eins,eouts,etype,emeta)))
                  | AggSum(gbvars,subexp) ->
                     AggSum(gbvars, (rcr subexp))
                  | Rel(rname, rvars, rtype) ->
                     Rel(rname, rvars, rtype)
                  | Cmp(op, subexp1, subexp2) ->
                     Cmp(op, subexp1, subexp2)
                  | Lift(target, subexp) ->
                     Lift(target, (rcr subexp))
               end
         )   
         e1
end

module NullMeta : ExternalMeta = struct
   type meta_t = unit
   let string_of_meta _ = ""
end

module BasicCalculus = Make(NullMeta)
module BasicCalculusTranslator = Translator(BasicCalculus)

module rec IVCMeta : ExternalMeta = struct
   type meta_t = IVCCalculus.expr_t option
   let string_of_meta meta = 
      begin match meta with 
         | Some(s) -> IVCCalculus.string_of_expr s
         | None -> ""
      end
end and IVCCalculus : Calculus = Make(IVCMeta)
module BasicToIVC = BasicCalculusTranslator(IVCCalculus)
