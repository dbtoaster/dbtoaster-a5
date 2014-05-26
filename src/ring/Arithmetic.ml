(**
   A ring of values (i.e., operations for doing arithmetic over constants),
   variables, and functions thereof.
   
   This approach nearly matches the syntax given in the SIGMOD submission, 
   except that we don't include singleton sums.  These can be effected by 
   computing a product with a definition term.  Doing so saves us from having
   a mutual recursion between ValueRing and CalcRing (which in turn saves 
   Oliver's sanity), and ensures that the delta of a value is always 0.
   
   Note that the functions that appear here and operate over values are assumed 
   to be deterministic and to have no side effects.
*)

open Type
open Constants

(**
   Template for the base type for the Arithmetic ring
*)
type 'term arithmetic_leaf_t =
   | AConst of const_t                       (** A constant value *)
   | AVar   of var_t                         (** A variable *)
   | AFn    of string * 'term list * type_t  (** A function application *)

module rec 
ValueBase : sig
      type t = ValueRingBase.expr_t arithmetic_leaf_t
      val  zero: t
      val  one: t

      val is_zero: t -> bool
      val is_one: t -> bool
   end = struct
      type t = ValueRingBase.expr_t arithmetic_leaf_t
      let zero = AConst(CInt(0))
      let one  = AConst(CInt(1))

      let is_zero (v: t) =
         match v with
         | AConst(c) -> is_zero c
         | _         -> false

      let is_one (v: t) =
         match v with
         | AConst(c) -> is_one c
         | _         -> false   
   end and

(**
   The base for the value ring
*)
ValueRingBase : Ring.Ring with type leaf_t = ValueBase.t
         = Ring.Make(ValueBase)

(**
   The base type for the Arithmetic ring (see [arithmetic_leaf_t] above)
*)
type value_leaf_t = ValueRingBase.leaf_t

(**
   Values, or elements of the Arithmetic ring
*)
type value_t      = ValueRingBase.expr_t

(**
   The value ring with overwritten mk_sum/mk_prod functions that preserve the 
   type of the expression when simplifying.
*)
module ValueRing = struct
   include ValueRingBase

   (**** Typechecker ****)
   (**
      Compute the type of the specified value.
      @param v   A value
      @return    The type of [v]
   *)
   let rec type_of_value (a_value: value_t): type_t =
      fold
         (escalate_type_list ~opname:"+")
         (escalate_type_list ~opname:"*")
         (fun t -> match t with | TInt | TFloat -> t 
           | _ -> failwith ("Can not compute type of -1 * "^(string_of_type t)))
         (fun leaf -> match leaf with 
            | AConst(c)  -> type_of_const c
            | AVar(_,vt) -> vt
            | AFn(_,fn_args,fn_type) ->
               List.iter (fun x -> let _ = type_of_value x in ())
                         fn_args;
               fn_type
         )
         a_value

   let mk_sum  l =
      let t = type_of_value (Sum(List.flatten (List.map sum_list l))) in
      let z = 
         begin match zero_of_type_opt t with
         | Some(z) -> Some(Val(AConst(z)))
         | None    -> None
         end
      in
      ValueRingBase.mk_sum_with_elem z l

   let mk_prod l =
      let t = type_of_value (Prod(List.flatten (List.map prod_list l))) in
      let z = 
         begin match zero_of_type_opt t with
         | Some(z) -> Some(Val(AConst(z)))
         | None    -> None
         end
      in
      let o = 
         begin match one_of_type_opt t with
         | Some(o) -> Some(Val(AConst(o)))
         | None    -> None
         end
      in
      ValueRingBase.mk_prod_with_elem z o l
end

(**** Constructors ****)
(** Produce the value equivalent of a boolean *)
let mk_bool   (b:bool   ):value_t = ValueRing.mk_val (AConst(CBool(b)))
(** Produce the value equivalent of an integer *)
let mk_int    (i:int    ):value_t = ValueRing.mk_val (AConst(CInt(i)))
(** Produce the value equivalent of a floating point number *)
let mk_float  (f:float  ):value_t = ValueRing.mk_val (AConst(CFloat(f)))
(** Produce the value equivalent of a string *)
let mk_string (s:string ):value_t = ValueRing.mk_val (AConst(CString(s)))
(** Produce the value equivalent of an arbitrary constant *)
let mk_const  (c:const_t):value_t = ValueRing.mk_val (AConst(c))
(** Produce the value equivalent of a variable *)
let mk_var    (v:var_t  ):value_t = ValueRing.mk_val (AVar(v))
(** Produce the value equivalent of a function *)
let mk_fn (n:string) (a:value_t list) (t:type_t):value_t = 
   ValueRing.mk_val (AFn(n,a,t))
             

(**** Stringifiers ****)
(**
   Generate the (Calculusparser-compatible) string representation of a base 
   type in the Arithmetic ring
   @param leaf   A base (leaf) element of the Arithmetic ring
   @return       The string representation of [leaf]
*)
let rec string_of_value_leaf (leaf:value_leaf_t): string =
   begin match leaf with
   | AConst(c) -> sql_of_const c
   | AVar(v)   -> string_of_var v
   | AFn(fname,fargs,ftype) ->
      "[" ^ fname ^ " : " ^ (string_of_type ftype) ^ "](" ^
      (ListExtras.string_of_list ~sep:", " string_of_value fargs) ^ ")"
   end

(**
   Generate the (Calculusparser-compatible) string representation of a value.
   @param v       A value (Arithmetic ring expression)
   @return        The string representation of [v]
*)
and string_of_value (a_value:value_t): string = 
   ValueRing.fold
      (fun sum_list  -> "("^(String.concat " + " sum_list )^")")
      (fun prod_list -> "("^(String.concat " * " prod_list)^")")
      (fun neg_term  -> "(-1*"^neg_term^")")
      string_of_value_leaf
      a_value

(**** Variable Operations ****)
(**
   Obtain the set of variables that appear in the specified value.
   @param v    A value
   @return     The set of variables that appear in [v] as a list
*)
let rec vars_of_value (v: value_t): var_t list =
   ValueRing.fold
      ListAsSet.multiunion
      ListAsSet.multiunion
      (fun x -> x)
      (fun x -> begin match x with
         | AConst(_) -> []
         | AVar(v) -> [v]
         | AFn(_,tl,_) -> ListAsSet.multiunion (List.map vars_of_value tl)
      end)
      v

(**
   Apply the provided mapping to the variables appearing in the specified value.
   @param mapping The mapping to apply to [v]
   @param v       A value
   @return        [v] with [mapping] applied to all of its variables
*)
let rec rename_vars (mapping:(var_t,var_t)ListAsFunction.table_fn_t) 
                    (v: value_t): value_t =
   ValueRing.fold
      ValueRing.mk_sum
      ValueRing.mk_prod
      ValueRing.mk_neg
      (fun lf -> ValueRing.mk_val (begin match lf with
         | AConst(c)     -> lf
         | AVar(v)       -> AVar(ListAsFunction.apply_if_present mapping v)
         | AFn(fn,fa,ft) -> AFn(fn, List.map (rename_vars mapping) fa, ft)
      end))
      v

(**** Typechecker ****)
(**
   Compute the type of the specified value.
   @param v   A value
   @return    The type of [v]
*)
let rec type_of_value (a_value: value_t): type_t =
   ValueRing.type_of_value a_value


(**
   Compare two values for equivalence under an undefined mapping from variable
   names in one value to variable names in the other.
   @param val1   A value
   @param val2   A value
   @return       [None] if no variable mapping exists to transform [val1] into 
                 [val2].  If such a mapping exists, it is returned wrapped in
                 a [Some]
*)
let rec cmp_values ?(cmp_opts:ValueRing.cmp_opt_t list =
                     ValueRing.default_cmp_opts) 
                   (val1:value_t) (val2:value_t):
                   ((var_t * var_t) list option) =
   let rcr = cmp_values ~cmp_opts:cmp_opts in
   
   ValueRing.cmp_exprs ~cmp_opts:cmp_opts 
      ListAsFunction.multimerge 
      ListAsFunction.multimerge 
      (fun lf1 lf2 ->
         match (lf1,lf2) with 
            | ((AConst(c1)),(AConst(c2))) ->
               if c1 <> c2 then None else Some([])
         
            | ((AVar(v1)),(AVar(v2))) ->
               Some([v1,v2])
         
            | ((AFn(fn1,subt1,ft1)),(AFn(fn2,subt2,ft2))) ->
               if (fn1 <> fn2) || (ft1 <> ft2) then None
               else begin try 
                  ListAsFunction.multimerge (List.map2 (fun a b -> 
                  begin match rcr a b with 
                     | None -> raise Not_found
                     | Some(s) -> s
                  end) subt1 subt2)
               with Not_found -> None
               end
      
            | (_,_) -> None
      ) val1 val2
   


(**
   Returns sign of a value.
   @param a_value       A value (Arithmetic ring expression)
   @return              Sign of [a_value]
*)
let rec sign_of_value (a_value:value_t): value_t =
  let one_expr = ValueRing.Val(ValueBase.one) in
  let zero_expr = ValueRing.Val(ValueBase.zero) in
  let sign_of_leaf (a_leaf: value_leaf_t): value_t = 
    match a_leaf with
      | AConst(c) ->
        begin match c with
          | CInt(number) -> 
             if number > 0 
             then one_expr 
             else if number < 0 
                  then ValueRing.Neg(one_expr) 
                  else zero_expr
          | CFloat(number) -> 
             if number > 0. 
             then one_expr 
             else if number < 0. 
                  then ValueRing.Neg(one_expr) 
                  else zero_expr
          | _ -> one_expr
        end
      | AVar(v) -> a_value
      | AFn(_, _, _) -> one_expr
  in
  match a_value with
    | ValueRing.Val(v) -> sign_of_leaf(v)
    | ValueRing.Sum(l) -> ValueRing.Sum(List.map (sign_of_value) l)
    | ValueRing.Prod(l)-> ValueRing.Prod(List.map (sign_of_value) l)
    | ValueRing.Neg(a) ->
      let sign_of_a = sign_of_value a in
      match sign_of_a with
        | ValueRing.Neg(x) -> sign_of_value x
        | _                -> ValueRing.Neg(sign_of_a)

(**** Evaluation ****)
(**
   Evaluate the specified value to a constant
   @param scope (optional) A set of variable->value mappings
   @param v     The value to evaluate
   @return      The constant value that v evaluates to
   @raise Failure If [v] contains a variable that is not defined in [scope] or
                  if [v] contains a function that is not defined in the globally
                  maintained set of [standard_functions].
*)
let rec eval ?(scope=StringMap.empty) (v:value_t): const_t = 
   ValueRing.fold Constants.Math.suml Constants.Math.prodl Constants.Math.neg 
                  (fun lf ->
      match lf with 
         | AConst(c) -> c
         | AVar(v,_) -> 
            if StringMap.mem v scope then StringMap.find v scope
            else failwith 
                    ("Variable "^v^" not found while evaluating arithmetic")
         | AFn(fn,fargs,ftype) -> Functions.invoke fn
                                     (List.map (eval ~scope:scope) fargs)
                                     ftype
   ) v

(**
   Evaluate/reduce the specified value as far as possible
   @param scope (optional) A set of variable->value mappings
   @param v     The value to evaluate
   @return      A value representing the most aggressively reduced/evaluated
                form of [v] possible.
*)
let rec eval_partial ?(scope=[]) (v:value_t): value_t = 
   let merge v_op c_op (term_list:value_t list): value_t = 
      let (v, c) = List.fold_right (fun (term) (v,c) ->
         match (term, c) with
            | (ValueRing.Val(AConst(c2)), None) -> (v, Some(c2))
            | (ValueRing.Val(AConst(c2)), Some(c1)) -> (v, Some(c_op c1 c2))
            | (_,_) -> (term :: v, c)
      ) term_list ([], None) 
      in v_op ((match c with 
         | None -> [] 
         | Some(c) -> [mk_const c]
      ) @ v)
   in
   ValueRing.fold 
      (merge ValueRing.mk_sum Constants.Math.sum)
      (merge ValueRing.mk_prod Constants.Math.prod)
      (fun x -> merge ValueRing.mk_prod Constants.Math.prod [mk_int (-1); x])
      (fun lf -> match lf with
         | AFn(fname, fargs_unevaled, ftype) -> 
            let fargs = List.map (eval_partial ~scope:scope) fargs_unevaled in
            begin try 
               let farg_vals = 
                  (List.map (fun x -> match x with 
                                      | ValueRing.Val(AConst(c)) -> c
                                      | _ -> raise Not_found) fargs)
               in
                  mk_const (Functions.invoke fname farg_vals ftype)
            with 
               | Not_found
               | Functions.InvalidInvocation(_) ->
                  ValueRing.mk_val (AFn(fname, fargs, ftype))
            end
         | AVar(vn, vt) ->
            if List.mem_assoc (vn,vt) scope 
               then (List.assoc (vn,vt) scope)
               else ValueRing.mk_val lf
         | AConst(c) -> ValueRing.mk_val lf
      )
      v
