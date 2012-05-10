(**
   Global typesystem for use by all of DBToaster's compilation stages.
*)

(**/**)
module StringMap = Map.Make(String)
(**/**)

(**** Global type definitions ****)
(** Comparison operations *)
type cmp_t = 
   Eq (** Equals *) | Lt (** Less than *) | Lte (** Less than or equal *)
 | Gt (** Greater than *) | Gte (** Greater than or equal *) 
 | Neq (** Not equal *)

(** Basic Types *)
type type_t = 
   | TBool                 (** Boolean *)
   | TInt                  (** Integer *)
   | TFloat                (** Floating point number *)
   | TString               (** A string of bounded length n (0 is infinite) *)
   | TAny                  (** An unspecified type *)
   | TExternal of string   (** An externally defined type *)

(** Basic Constants *)
type const_t = 
   | CBool   of bool       (** Boolean  *)
   | CInt    of int        (** Integer *)
   | CFloat  of float      (** Float *)
   | CString of string     (** String *)

(** Basic (typed) variables *)
type var_t = string * type_t

(**** Basic Operations ****)
(** 
   Compute the type of a given constant
   @param a   A constant
   @return    The type of [a]
*)
let type_of_const (a:const_t): type_t =
   begin match a with
      | CBool(_)   -> TBool
      | CInt(_)    -> TInt
      | CFloat(_)  -> TFloat
      | CString(s) -> TString
   end

(** 
   Cast a constant to an integer.  Floats are truncated, and booleans are 
   converted to 1/0.  Strings produce an error.
   @param a   A constant
   @return    The integer value of [a]
   @raise Failure If the constant can not be cast to an integer
*)
let int_of_const (a:const_t): int = 
   begin match a with
      | CBool(true)  -> 1
      | CBool(false) -> 0
      | CInt(av)     -> av
      | CFloat(av)   -> int_of_float av
      | CString(av)  -> failwith ("Cannot produce integer of string '"^av^"'")
   end

(**
   Cast a constant to a float.  Integers are promoted, booleans are converted to 
   1./0..  Strings produce an error.
   @param a   A constant
   @return    The floating point value of [a]
   @raise Failure If the constant can not be cast to a float
*)
let float_of_const (a:const_t): float = 
   begin match a with
      | CBool(true)  -> 1.
      | CBool(false) -> 0.
      | CInt(av)     -> float_of_int av
      | CFloat(av)   -> av
      | CString(av)  -> failwith ("Cannot produce float of string '"^av^"'")
   end	
	
(**** Conversion to Strings ****)
(**
   Get the string representation (according to SQL syntax) of a comparison 
   operation.
   @param op   A comparison operation
   @return     The string representation of [op]
*)
let string_of_cmp (op:cmp_t): string = 
   begin match op with 
      | Eq  -> "="  | Lt -> "<" | Lte -> "<="
      | Neq -> "!=" | Gt -> ">" | Gte -> ">="
   end

(**
   Get the string representation (corresponding to the OCaml defined above) of
   a type
   @param ty   A type
   @return     The string representation of the OCaml type declaration of [ty]
*)
let ocaml_of_type (ty: type_t): string =
   begin match ty with
      | TAny             -> "TAny"
      | TBool            -> "TBool"
      | TInt             -> "TInt"
      | TFloat           -> "TFloat"
      | TString          -> "TString"
      | TExternal(etype) -> "TExternal(\""^etype^"\")"
   end

(**
   Get the human-readable string representation of a type.  (Corresponds to
   values accepted by Calculusparser)
   @param ty   A type
   @return     The human-readable representation of [ty]
*)
let string_of_type (ty: type_t): string =
   begin match ty with
      | TAny             -> "?"
      | TBool            -> "bool"
      | TInt             -> "int"
      | TFloat           -> "float"
      | TString          -> "string"
      | TExternal(etype) -> etype
   end

(**
   Get the human-readable string representation of a constant.
   @param a   A constant
   @return    The human-readable string-representation of [a]
*)
let string_of_const (a: const_t): string = 
   begin match a with
      | CBool(true)  -> "true"
      | CBool(false) -> "false"
      | CInt(av)     -> string_of_int av
      | CFloat(av)   -> string_of_float av
      | CString(av)  -> av
   end

(**
   Get the string representation (corresponding to the OCaml defined above) of
   a constant
   @param a   A constant
   @return    The string representation of the OCaml constant declaration of [a]
*)
let ocaml_of_const (a: const_t): string =
   begin match a with
      | CBool(true)  -> "CBool(true)"
      | CBool(false) -> "CBool(false)"
      | CInt(i)      -> "CInt("^(string_of_int i)^")"
      | CFloat(f)    -> "CInt("^(string_of_float f)^")"
      | CString(s)   -> "CString(\""^s^"\")"
   end

(**
   Get the string representation (corresponding to what the Sql and Calculus 
   parsers expect) of a constant
   @param a   A constant
   @return    The string representation of the SQL constant form of [a]
*)
let sql_of_const (a: const_t): string =
   begin match a with
      | CBool(true)  -> "TRUE"
      | CBool(false) -> "FALSE"
      | CInt(i)      -> (string_of_int i)
      | CFloat(f)    -> (string_of_float f)
      | CString(s)   -> "'"^s^"'"
   end

(**
   Get the string representation of a variable.  If the PRINT-VERBOSE debug mode
   is active, the variable will be printed with its full type using syntax
   accepted by Calculusparser.
   @param var   A variable
   @return      The string representation of [var]
*)
let string_of_var ?(verbose = Debug.active "PRINT-VERBOSE")
                  ((name, vt): var_t): string =
   if verbose then name^":"^(string_of_type vt)
              else name

(**
   Get the string representation of a list of variables.  If the PRINT-VERBOSE
   debug mode is active, the variable will be printed with its full type using
   syntax accepted by Calculusparser.
   @param vars  A list of variables
   @return      The string representation of [vars]
*)
let string_of_vars ?(verbose = Debug.active "PRINT-VERBOSE")
						 (vars : var_t list): string = 
	ListExtras.string_of_list (string_of_var ~verbose:verbose) vars

(**** Zero Constants ****)
(**
   Returns a constant reprezenting zero of type [zt].
   @param zt A type. Can be TBool, TInt or TFloat.
   @return     The constant zero of type [zt]
   @raise Failure If there is no zero constant corrsponding to [zt]
*)  
let zero_of_type zt : const_t = 
	begin match zt with
		| TBool -> CBool(false)
		| TInt  -> CInt(0)
		| TFloat -> CFloat(0.)
		| _ -> failwith ("Cannot produce zero of type '"^(string_of_type zt)^"'")
	end
	
(**** Escalation ****)
(**
   Given two types, return the "greater" of the two.  
   {ul
      {- [TAny] can be escalated to any other type}
      {- [TBool] can be escalated to [TInt]}
      {- [TInt] can be escalated to [TFloat]}
      {- Two types that can not be escalated will trigger an error.}
   }
   @param opname  (optional) The operation name to include in error messages
   @param a       The first type
   @param b       The second type
   @return        A type that both [a] and [b] escalate to.
*)
let escalate_type ?(opname="<op>") (a:type_t) (b:type_t): type_t = 
   begin match (a,b) with
      | (at,bt) when at = bt -> at
      | (TAny,t) | (t,TAny) -> t
      | (TInt,TBool) | (TBool,TInt) -> TInt
      | (TBool,TFloat) | (TFloat,TBool) -> TFloat
      | (TInt,TFloat) | (TFloat,TInt) -> TFloat
      | _ -> failwith ("Can not compute type of "^(string_of_type a)^" "^
                       opname^" "^(string_of_type b))
   end

(**
   Given a list of types, return the "greatest" (as [escalate_type])
   @param opname  (optional) The operation name to include in error messages
   @param tlist   A list of types
   @return        A type that every element of [tlist] escalates to
*)
let escalate_type_list ?(opname="<op>") tlist = 
   if tlist = [] then TInt
   else 
      List.fold_left (escalate_type ~opname:opname) 
                     (List.hd tlist) (List.tl tlist)
