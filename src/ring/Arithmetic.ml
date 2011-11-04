(*
   A ring of values (i.e., operations for doing arithmetic over constants),
   variables, and functions thereof.
   
   This approach nearly matches the syntax given in the SIGMOD submission, 
   except that we don't include singleton sums.  These can be effected by 
   computing a product with a definition term.  Doing so saves us from having
   a mutual recursion between ValueRing and CalcRing (which in turn saves 
   Oliver's sanity), and ensures that the delta of a value is always 0.
   
   Note that the functions defined herein are assumed to be deterministic and
   have no side effects.
*)

open GlobalTypes

type 'term arithmetic_leaf_t =
   | AConst of const_t
   | AVar   of var_t
   | AFn    of string * 'term list * type_t

module rec 
ValueBase : sig
      type t = ValueRing.expr_t arithmetic_leaf_t
      val  zero: t
      val  one: t
   end = struct
      type t = ValueRing.expr_t arithmetic_leaf_t
      let zero = AConst(CInt(0))
      let one  = AConst(CInt(1))
   end and
ValueRing : Ring.Ring with type leaf_t = ValueBase.t
         = Ring.Make(ValueBase)

type value_leaf_t = ValueRing.leaf_t
type value_t      = ValueRing.expr_t

(**** Stringifiers ****)
let rec string_of_value_leaf (leaf:value_leaf_t): string =
   begin match leaf with
      | AConst(c) -> string_of_const c
      | AVar(v)   -> string_of_var v
      | AFn(fname,fargs,ftype) ->
         fname^"("^(ListExtras.string_of_list string_of_value fargs)^")"
   end
and string_of_value: (value_t -> string) = 
   ValueRing.fold
      (fun sum_list  -> "("^(String.concat " + " sum_list )^")")
      (fun prod_list -> "("^(String.concat " * " prod_list)^")")
      (fun neg_term  -> "(-1*"^neg_term^")")
      string_of_value_leaf

(**** Typechecker ****)
let escalate_type ?(opname="<op>") (a:type_t) (b:type_t): type_t = 
   begin match (a,b) with
      | (at,bt) when at = bt -> at
      | (TInt,TFloat) | (TFloat,TInt) -> TFloat
      | _ -> failwith ("Can not compute type of "^(string_of_type a)^" "^
                       opname^" "^(string_of_type b))
   end

let rec type_of_value ?(default_type = TInt) (v: value_t): type_t =
   let escalate_list op tlist = 
      if tlist = [] then TInt
      else 
         List.fold_left (escalate_type ~opname:op) 
                        (List.hd tlist) (List.tl tlist)
   in
      ValueRing.fold
         (escalate_list "+")
         (escalate_list "*")
         (fun t -> match t with | TInt | TFloat -> t 
           | _ -> failwith ("Can not compute type of -1 * "^(string_of_type t)))
         (fun leaf -> match leaf with 
            | AConst(c)  -> type_of_const c
            | AVar(_,vt) -> vt
            | AFn(_,fn_args,fn_type) ->
               List.iter (fun x->type_of_value ~default_type:default_type x)
                         fn_args;
               fn_type
         )

(**** Arithmetic ****)
let binary_op (b_op: bool   -> bool   -> bool)
              (i_op: int    -> int    -> int)
              (f_op: float  -> float  -> float)
              (a: const_t) (b: const_t): const_t =
   begin match (a,b) with
      | (CBool(av),  CBool(bv)) -> 
         CBool(b_op av bv)
      | (CBool(_),   CInt(_))
      | (CInt(_),    CBool(_)) 
      | (CInt(_),    CInt(_)) -> 
         CInt(i_op (int_of_const a) (int_of_const b))
      | (CFloat(_), (CBool(_)|CInt(_)|(CFloat(_))))
      | ((CBool(_)|CInt(_)), CFloat(_)) ->
         CFloat(f_op (float_of_const a) (float_of_const b))
      | (CString(_), _) | (_, CString(_)) -> 
         failwith "Binary math op over a string"
   end

let sum  = binary_op ( || ) ( + ) ( +. )
let prod = binary_op ( && ) ( * ) ( *. )