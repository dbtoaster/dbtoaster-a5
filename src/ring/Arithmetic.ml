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

open Types

(**
   Template for the base type for the Arithmetic ring
*)
type 'term arithmetic_leaf_t =
   | AConst of const_t                       (** A constant value *)
   | AVar   of var_t                         (** A variable *)
   | AFn    of string * 'term list * type_t  (** A function application *)

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
(**
   The value ring
*)
ValueRing : Ring.Ring with type leaf_t = ValueBase.t
         = Ring.Make(ValueBase)

(**
   The base type for the Arithmetic ring (see [arithmetic_leaf_t] above)
*)
type value_leaf_t = ValueRing.leaf_t

(**
   Values, or elements of the Arithmetic ring
*)
type value_t      = ValueRing.expr_t

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
         "["^fname^" : "^(string_of_type ftype)^"]("^(ListExtras.string_of_list string_of_value fargs)^")"
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
let rec rename_vars (mapping:(var_t,var_t)Function.table_fn_t) (v: value_t): 
                    value_t =
   ValueRing.fold
      ValueRing.mk_sum
      ValueRing.mk_prod
      ValueRing.mk_neg
      (fun lf -> ValueRing.mk_val (begin match lf with
         | AConst(c)     -> lf
         | AVar(v)       -> AVar(Function.apply_if_present mapping v)
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
   ValueRing.fold
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

(**** Arithmetic ****)
(**
   Perform a type-escalating binary arithmetic operation over two constants
   @param b_op   The operation to apply to boolean constants
   @param i_op   The operation to apply to integer constants
   @param f_op   The operation to apply to floating point constants
   @param a      A constant
   @param b      A constant
   @return       The properly wrapped result of applying [b_op], [i_op], or 
                 [f_op] to [a] and [b], as appropriate.
   @raise Failure If [a] or [b] is a string.
*)
let binary_op (b_op: bool   -> bool   -> bool)
              (i_op: int    -> int    -> int)
              (f_op: float  -> float  -> float)
				  (op_type: type_t)
              (a: const_t) (b: const_t): const_t =
   begin match (a,b,op_type) with
      | (CBool(av),  CBool(bv), (TBool|TAny)) -> CBool(b_op av bv)
		| (CBool(_),   CBool(_),  TInt)
      | (CBool(_),   CInt(_),   (TInt|TAny))
      | (CInt(_),    CBool(_),  (TInt|TAny)) 
      | (CInt(_),    CInt(_),   (TInt|TAny))  -> CInt(i_op (int_of_const a) (int_of_const b))
      
		| (CBool(_),   CBool(_),  TFloat)
      | (CInt(_),    CInt(_),   TFloat)      
		| (CFloat(_), (CBool(_)|CInt(_)|(CFloat(_))), (TFloat|TAny))
      | ((CBool(_)|CInt(_)), CFloat(_), (TFloat|TAny)) -> CFloat(f_op (float_of_const a) (float_of_const b))
      | (CString(_), _, _) | (_, CString(_), _) -> failwith "Binary math op over a string"
		| (_,   _,  _) -> 
			failwith ("Binary math op with incompatible return type: "^
		             (string_of_const a)^" "^(string_of_const b)^
						 " -> "^(string_of_type op_type))
   end

(** Perform type-escalating addition over two constants *)
let sum  = binary_op ( fun x->failwith "sum of booleans" ) ( + ) ( +. ) TAny
(** Perform type-escalating addition over an arbitrary number of constants *)
let suml = List.fold_left sum (CInt(0))
(** Perform type-escalating multiplication over two constants *)
let prod = binary_op ( && ) ( * ) ( *. ) TAny
(** Perform type-escalating multiplication over an arbitrary number of 
    constants *)
let prodl= List.fold_left prod (CInt(1))
(** Negate a constant *)
let neg  = binary_op (fun _-> failwith "Negation of a boolean") 
                     ( * ) ( *. ) TAny (CInt(-1))
(** Compute the multiplicative inverse of a constant *)
let div1 dtype a   = binary_op (fun _->failwith "Dividing a boolean 1") 
                         (/) (/.) dtype (CInt(1)) a
(** Perform type-escalating division of two constants *)
let div2 dtype a b = binary_op (fun _->failwith "Dividing a boolean 2")
                         (/) (/.) dtype a b

let comparison_op (opname:string) (iop:int -> int -> bool) 
                  (fop:float -> float -> bool) (a:const_t) (b:const_t):const_t =
   begin match (escalate_type ~opname:opname (type_of_const a) 
                                             (type_of_const b)) with
      | TInt -> CBool(iop (int_of_const a) (int_of_const b))
      | TFloat -> CBool(fop (float_of_const a) (float_of_const b))
      | _ -> failwith (opname^" over invalid types")
   end

(** Perform a type-escalating less-than comparison *)
let cmp_lt  = comparison_op "<"  (<)  (<)
(** Perform a type-escalating less-than or equals comparison *)
let cmp_lte = comparison_op "<=" (<=) (<=)
(** Perform a type-escalating greater-than comparison *)
let cmp_gt  = comparison_op ">"  (>)  (>)
(** Perform a type-escalating greater-than or equals comparison *)
let cmp_gte = comparison_op ">=" (>=) (>=)
(** Perform a type-escalating equals comparison *)
let cmp_eq a b = 
   CBool(begin match (a,b) with
      | (CBool(av), CBool(bv))        -> av = bv
      | (CBool(_), _)  | (_,CBool(_)) -> failwith "= of boolean and other"
      | (CString(av), CString(bv))    -> av = bv
      | (CString(_), _) | (_,CString(_))-> failwith "= of string and other"
      | (CFloat(_), _) | (_,CFloat(_))-> (float_of_const a) = (float_of_const b)
      | (CInt(av), CInt(bv))          -> av = bv
   end)
(** Perform a type-escalating not-equals comparison *)
let cmp_neq a b = CBool((cmp_eq a b) = CBool(false))

(**** Functions ****)
(**
   An internally maintained set of arithmetic functions (Arithmetic ring 
   elements of type [AFn]) for doing inline evaluation.
   
   Initially, a single function "/" is defined, which computes 1/x if it is 
   invoked with one parameter, or x/y if invoked with two parameters.
*)
let arithmetic_functions: 
   (const_t list -> type_t -> const_t) StringMap.t ref = ref StringMap.empty

(**
   Determine whether an arithmetic function is defined internally.
   
   @param name   The name of a cuntion
   @return       True if [name] is defined
*)
let function_is_defined (name:string) =
   StringMap.mem name !arithmetic_functions

(**
   Declare a new arithmetic function.
*)
let declare_arithmetic_function (name:string)  
                                (fn:const_t list -> type_t -> const_t): unit =
   arithmetic_functions := 
      StringMap.add name fn !arithmetic_functions
;;
declare_arithmetic_function "/" 
   (fun arglist ftype -> 
		match arglist with
         | [v] -> div1 ftype v
         | [v1;v2] -> div2 ftype v1 v2
         | _ ->
            failwith "Invalid arguments to division function"
   )
;;
declare_arithmetic_function "min"
   (fun arglist ftype ->
      let (start,cast_type) = 
         match ftype with TInt -> (CInt(max_int), TInt)
                        | TAny 
                        | TFloat -> (CFloat(max_float), TFloat)
                        | _ -> failwith ("min of "^(string_of_type ftype))
      in List.fold_left min start (List.map (Types.type_cast cast_type) arglist)
   )
;;
declare_arithmetic_function "max"
   (fun arglist ftype ->
      let (start,cast_type) = 
         match ftype with TInt -> (CInt(min_int), TInt)
                        | TAny 
                        | TFloat -> (CFloat(min_float), TFloat)
                        | _ -> failwith ("max of "^(string_of_type ftype))
      in List.fold_left max start (List.map (Types.type_cast cast_type) arglist)
   )


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
          | CInt(number) -> if number > 0 then one_expr else if number < 0 then ValueRing.Neg(one_expr) else zero_expr
          | CFloat(number) -> if number > 0. then one_expr else if number < 0. then ValueRing.Neg(one_expr) else zero_expr
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
                  maintained set of [arithmetic_functions].
*)
let rec eval ?(scope=StringMap.empty) (v:value_t): const_t = 
   ValueRing.fold suml prodl neg (fun lf ->
      match lf with 
         | AConst(c) -> c
         | AVar(v,_) -> 
            if StringMap.mem v scope then StringMap.find v scope
            else failwith ("Variable "^v^" not found while evaluating arithmetic")
         | AFn(fn,fargs,ftype) ->
            if StringMap.mem fn !arithmetic_functions then
               let fn_def = StringMap.find fn !arithmetic_functions
               in fn_def (List.map (eval ~scope:scope) fargs) ftype
            else failwith ("Function "^fn^" is undefined")
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
      (merge ValueRing.mk_sum sum)
      (merge ValueRing.mk_prod prod)
      (fun x -> merge ValueRing.mk_prod prod [mk_int (-1); x])
      (fun lf -> match lf with
         | AFn(fname, fargs_unevaled, ftype) -> 
            let fargs = List.map (eval_partial ~scope:scope) fargs_unevaled in
            begin try 
               let farg_vals = 
                  (List.map (fun x -> match x with 
                                      | ValueRing.Val(AConst(c)) -> c
                                      | _ -> raise Not_found) fargs)
               in
               let fn_def = StringMap.find fname !arithmetic_functions in
                  mk_const (fn_def farg_vals ftype)
            with Not_found ->
               ValueRing.mk_val (AFn(fname, fargs, ftype))
            end
         | AVar(vn, vt) ->
            if List.mem_assoc (vn,vt) scope 
               then (List.assoc (vn,vt) scope)
               else ValueRing.mk_val lf
         | AConst(c) -> ValueRing.mk_val lf
      )
      v

(**
   Compare two values for equivalence under an undefined mapping from variable
   names in one value to variable names in the other.
   @param val1   A value
   @param val2   A value
   @return       [None] if no variable mapping exists to transform [val1] into 
                 [val2].  If such a mapping exists, it is returned wrapped in
                 a [Some]
*)
let rec cmp_values ?(cmp_opts:ValueRing.cmp_opt_t list = ValueRing.default_cmp_opts) 
                    (val1:value_t) (val2:value_t):((var_t * var_t) list option) =
   let rcr = cmp_values ~cmp_opts:cmp_opts in											
   ValueRing.cmp_exprs ~cmp_opts:cmp_opts Function.multimerge Function.multimerge 
                      (fun lf1 lf2 ->
      match (lf1,lf2) with 
      | ((AConst(c1)),(AConst(c2))) ->
         if c1 <> c2 then None else Some([])
         
      | ((AVar(v1)),(AVar(v2))) ->
         Some([v1,v2])
         
      | ((AFn(fn1,subt1,ft1)),(AFn(fn2,subt2,ft2))) ->
         if (fn1 <> fn2) || (ft1 <> ft2) then None
         else begin try 
            Function.multimerge (List.map2 (fun a b -> 
            begin match rcr a b with 
               | None -> raise Not_found
               | Some(s) -> s
            end) subt1 subt2)
         with Not_found -> None
         end
      
      | (_,_) -> None
   ) val1 val2
   
