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
   module CalcRing : Ring.Ring
   
   type expr_t = CalcRing.expr_t
   
   val string_of_leaf: CalcRing.leaf_t -> string
   val string_of_expr: CalcRing.expr_t -> string
end

module Make(T : ExternalMeta) : Calculus = struct
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
            "("^(string_of_value subexp1)^") "^
                (string_of_cmp op)^
            "("^(string_of_value subexp2)^") "
         | Lift(target, subexp)    -> 
            (string_of_var target)^" <= ("^(string_of_expr subexp)^")"
      end
   and string_of_expr (expr:CalcRing.expr_t): string =
      CalcRing.fold
         (fun sum_list  -> "("^(String.concat " U " sum_list )^")")
         (fun prod_list -> "("^(String.concat " |><| " prod_list)^")")
         (fun neg_term  -> "((<>:-1)*"^neg_term^")")
         string_of_leaf
         expr
end

module Translator(T1 : ExternalMeta, T2 : ExternalMeta) = struct
   module C1 = Make(T1)
   module C2 = Make(T2)
   let rec translate (translate_meta: T1.meta_t -> T2.meta_t) 
                     (e1:C1.expr_t): C2.expr_t =
      let rcr = translate translate_meta in
      C1.CalcRing.fold 
         C2.CalcRing.mk_sum
         C2.CalcRing.mk_prod
         C2.CalcRing.mk_neg
         (fun lf ->
            begin match lf with
               | Value(v) -> Value(v)
               | External(ename,eins,eouts,etype,emeta)
                  External(ename,eins,eouts,etype,(translate_meta emeta))
               | AggSum(gbvars,subexp) ->
                  AggSum(gbvars, (rcr subexp))
               | Rel(rname, rvars, rtype) ->
                  Rel(rname, rvars, rtype)
               | Cmp(op, subexp1, subexp2) ->
                  Cmp(op, (rcr subexp1), (rcr subexp2))
               | Lift(target, subexp) ->
                  Lift(target, (rcr subexp))
         )   
end

module NullMeta : ExternalMeta = struct
   type meta_t = unit
   let string_of_meta _ = ""
end

module BasicCalculus = Make(NullMeta)
