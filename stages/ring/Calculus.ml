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

type 'external_meta_t external_t = 
   string           * (* External name *)
   var_t list       * (* Input Variables *)
   var_t list       * (* Output Variables *)
   type_t           * (* External Type *)
   'external_meta_t   (* External Metadata *)

type ('term_t,'e_meta_t) calc_leaf_t = 
   | Value    of value_t
   | AggSum   of var_t list * 'term_t
   | Rel      of string * var_t list * type_t
   | External of 'e_meta_t external_t
   | Cmp      of cmp_t * value_t * value_t
   | Defn     of var_t * 'term_t


module type External = sig
   type meta_t
   
   val string_of_external: meta_t external_t -> string
   val delta_of_external : meta_t external_t -> ('term_t,meta_t) calc_leaf_t
end

module Make(T : External) = struct

   module rec 
   CalcBase : sig
         type t = (CalcRing.expr_t,T.meta_t) calc_leaf_t
         val  zero: t
         val  one: t
      end = struct
         type t = (CalcRing.expr_t,T.meta_t) calc_leaf_t
         let zero = Value(VConst(CInt(0)))
         let one  = Value(VConst(CInt(0)))
      end and
   CalcRing : Ring.Ring with type leaf_t = CalcBase.t
            = Ring.Make(CalcBase)
   
   include CalcRing
   
   (*** Constructors ***)
   let mk_bool   (b:bool)  : expr_t = 
      CalcRing.mk_val (Value(VConst(CBool(  b))))

   let mk_int    (i:int)   : expr_t = 
      CalcRing.mk_val (Value(VConst(CInt(   i))))

   let mk_float  (f:float) : expr_t = 
      CalcRing.mk_val (Value(VConst(CFloat( f))))

   let mk_string (s:string): expr_t = 
      CalcRing.mk_val (Value(VConst(CString(s))))

   let mk_var   (var:var_t): expr_t = 
      CalcRing.mk_val (Value(VVar(var))         )

   let mk_aggsum (gb_vars:var_t list) (expr:expr_t): expr_t = 
      CalcRing.mk_val (AggSum(gb_vars, expr))

   let mk_rel (rname:string) (rvars:var_t list) (rtype:type_t): expr_t =
      CalcRing.mk_val (Rel(rname, rvars, rtype))

   let mk_external (ext: T.meta_t external_t): expr_t =
      CalcRing.mk_val (External(ext))

   let mk_cmp (op: cmp_t) (a: value_t) (b: value_t): expr_t =
      CalcRing.mk_val (Cmp(op, a, b))

   let mk_defn (var: var_t) (a: expr_t): expr_t =
      CalcRing.mk_val (Defn(var, a))
   
   (*** Stringifiers ***)
   let rec string_of_leaf (leaf:leaf_t): string = 
      begin match leaf with
         | Value(v)                -> string_of_value(v)
         | External(ext_info)      -> T.string_of_external ext_info
         | AggSum(gb_vars, subexp) -> 
            "Sum"^(ListExtras.ocaml_of_list  string_of_var gb_vars)^
                  "("^(string_of_expr subexp)^")"
         | Rel(rname, rvars, _)    -> 
            rname^"("^(ListExtras.string_of_list string_of_var rvars)^")"
         | Cmp(op,subexp1,subexp2) -> 
            "("^(string_of_value subexp1)^") "^
                (string_of_cmp op)^
            "("^(string_of_value subexp2)^") "
         | Defn(target, subexp)    -> 
            (string_of_var target)^" <= ("^(string_of_expr subexp)^")"
      end
   and string_of_expr (expr:expr_t): string =
      CalcRing.fold
         (fun sum_list  -> "("^(String.concat " + " sum_list )^")")
         (fun prod_list -> "("^(String.concat " * " prod_list)^")")
         (fun neg_term  -> "(-1*"^neg_term^")")
         string_of_leaf
         expr
   
end (* module Make(T:External) = struct *)

let string_of_external (string_of_meta: 'a -> string) 
                       ((ename,eins,eouts,etype,emeta): 'a external_t): string =
   ename^
   (ListExtras.ocaml_of_list string_of_var eins)^
   (ListExtras.ocaml_of_list string_of_var eins)^
   (string_of_meta emeta)

module DefaultExternal : External = struct
   type meta_t = type_t
   
   let type_of_external (_,_,_,et) = et
   let string_of_external = string_of_external (fun _ -> "")
   let delta_of_external _ = failwith "Cannot compute delta of unit external" 
end

include Make(DefaultExternal)
