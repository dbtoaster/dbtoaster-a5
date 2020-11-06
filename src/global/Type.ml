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

(** Interval types *)
type interval_type_t = 
   | TYearMonth
   | TDay

(** Basic Types *)
type type_t = 
   | TBool                 (** Boolean *)
   | TInt                  (** Integer *)
   | TFloat                (** Floating point number *)
   | TChar                 (** Character *)
   | TString               (** A string of bounded length n (0 is infinite) *)
   | TDate                 (** Date *)
   | TInterval of interval_type_t   (** Year-Month Interval *)
   | TAny                  (** An unspecified type *)
   | TExternal of string   (** An externally defined type *)

(** Basic (typed) variables *)
type var_t = string * type_t

(** Equality over variables *)
let var_eq ((a,_):var_t) ((b,_):var_t) = (a = b)

(** Inequality over variables *)
let var_neq ((a,_):var_t) ((b,_):var_t) = (a <> b)

(** Membership testing *)
let mem_var ((a,_):var_t) = List.find (fun (b,_) -> a = b)

(**
   Identify the inverse of a comparison operation (Eq -> Neq, etc...)
*)
let inverse_of_cmp = function
 | Eq -> Neq | Neq -> Eq
 | Lt -> Gte | Lte -> Gt
 | Gt -> Lte | Gte -> Lt

 
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
      | TChar            -> "TChar"
      | TString          -> "TString"
      | TDate            -> "TDate"
      | TInterval(TYearMonth) -> 
         "TInterval(TYearMonth)"
      | TInterval(TDay) ->
         "TInterval(TDay)" 
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
      | TInt             -> "long"
      | TFloat           -> "double"
      | TChar            -> "char"
      | TString          -> "string"
      | TDate            -> "date"
      | TInterval(TYearMonth) -> 
         "year_month_interval"
      | TInterval(TDay) -> 
         "day_interval"
      | TExternal(etype) -> etype
   end

(**
   Get the human-readable string representation of a type.  (Corresponds to
   values accepted by Calculusparser)
   @param ty   A type
   @return     The human-readable representation of [ty]
*)
let cpp_of_type (ty: type_t): string =
   begin match ty with
      | TAny             -> "?"
      | TBool            -> "bool"
      | TInt             -> "long"
      | TFloat           -> "double"
      | TChar            -> "char"
      | TString          -> "string"
      | TDate            -> "date"
      | TInterval(TYearMonth) -> 
         "year_month_interval"
      | TInterval(TDay) -> 
         "day_interval"
      | TExternal(etype) -> etype
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
      | (TDate,TInterval _) | (TInterval _, TDate) -> TDate
      | (TInterval(it), TInt) | (TInt, TInterval(it)) -> TInterval(it)
      | (TInterval(it), TFloat) | (TFloat, TInterval(it)) -> TInterval(it)
      | (TChar, TString) | (TString, TChar) -> TString
      | _ -> failwith ("Can not compute type of "^(string_of_type a)^" "^
                       opname^" "^(string_of_type b))
   end

(**
   Given two types, determine if the first may be escalated into the second
   @param from_type  The source type
   @param to_type    The destination type
   @return true if [from_type] escalates safely to [to_type]
*)
let can_escalate_type (from_type:type_t) (to_type:type_t): bool =
   try (escalate_type from_type to_type) = to_type
   with Failure(_) -> false

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
