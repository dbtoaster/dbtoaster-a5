(**   
   Global system for wrapping typed primitive constants throughout DBToaster
*)

open Type
;;

(** Basic Constants *)
type const_t = 
   | CBool   of bool            (** Boolean  *)
   | CInt    of int             (** Integer *)
   | CFloat  of float           (** Float *)
   | CString of string          (** String *)
   | CDate   of int * int * int (** Date *)

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
      | CDate _    -> TDate
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
      | CString(av)  -> int_of_string av
      | CDate _      -> failwith ("Cannot produce integer of date")
   end

(**
   Cast a constant to a float. Integers are promoted, booleans are converted to
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
      | CString(av)  -> float_of_string av
      | CDate _      -> failwith ("Cannot produce float of date")
   end


(**
   Parses a string and converts it into corresponding Date constant.
   @param str  A string
   @return     The Date value of [str]
   @raise Failure If the string does not correspond to any date
*)
let parse_date str = 
   if (Str.string_match
          (Str.regexp "\\([0-9]+\\)-\\([0-9]+\\)-\\([0-9]+\\)") 
                      str 0)
   then (
      let y = (int_of_string (Str.matched_group 1 str)) in
      let m = (int_of_string (Str.matched_group 2 str)) in
      let d = (int_of_string (Str.matched_group 3 str)) in
      if (m > 12) then failwith 
         ("Invalid month ("^(string_of_int m)^") in date: "^str);
      if (d > 31) then failwith
         ("Invalid day ("^(string_of_int d)^") in date: "^str);
      CDate(y,m,d)
   ) else
      failwith ("Improperly formatted date: "^str)    
      
(**** Conversion to Strings ****)

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
      | CDate(y,m,d) -> (string_of_int y) ^ "-" ^
                        (string_of_int m) ^ "-" ^
                        (string_of_int d)
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
      | CDate(y,m,d) -> "CDate("^(string_of_int y)^","^
                                 (string_of_int m)^","^
                                 (string_of_int d)^")"
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
      | CDate(y,m,d) -> "DATE('"^(string_of_const a)^"')"
   end

(**** Zero Constants ****)
(**
   Returns a constant reprezenting zero of type [zt].
   @param zt    A type. Can be TBool, TInt or TFloat.
   @return      The constant zero of type [zt]
   @raise Failure If there is no zero constant corrsponding to [zt]
*)  
let zero_of_type zt : const_t = 
   begin match zt with
      | TBool -> CBool(false)
      | TInt  -> CInt(0)
      | TFloat -> CFloat(0.)
      | _ -> failwith ("Cannot produce zero of type '"^(string_of_type zt)^"'")
   end

(**** Type Casting ****)
(**
   Type-cast a constant to a specified type.  Raise an error if the type 
   conversion is not permitted.
   @param t  A type
   @param a  A constant
   @return   [a] cast to [t]
*)
let type_cast (t:type_t) (a:const_t) =
   begin match (t,a) with
      | (TInt, (CInt(_) | CBool(_))) -> CInt(int_of_const a)
      | (TFloat, (CInt(_) | CBool(_) | CFloat(_))) -> CFloat(float_of_const a)
      | (_, _) when t = (type_of_const a) -> a
      | (_, _) ->
         failwith ("Cannot cast "^(string_of_const a)^" to "^(string_of_type t))
   end

(** Math operations over constants ****)
module Math = struct
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
         | (CInt(_),    CInt(_),   (TInt|TAny))  -> 
            CInt(i_op (int_of_const a) (int_of_const b))
         
         | (CBool(_),   CBool(_),  TFloat)
         | (CInt(_),    CInt(_),   TFloat)      
         | (CFloat(_), (CBool(_)|CInt(_)|(CFloat(_))), (TFloat|TAny))
         | ((CBool(_)|CInt(_)), CFloat(_), (TFloat|TAny)) -> 
            CFloat(f_op (float_of_const a) (float_of_const b))
         | (CString(_), _, _) | (_, CString(_), _) -> 
            failwith "Binary math op over a string"
         | (CDate _, _, _) | (_, CDate _, _) -> 
            failwith "Binary math op over a date"
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
   
   (**/**)
   let comparison_op (opname:string) (iop:int -> int -> bool) 
                     (fop:float -> float -> bool) (a:const_t)
                     (b:const_t):const_t =
      let op_type = (escalate_type ~opname:opname (type_of_const a) 
                                                  (type_of_const b)) in
      begin match op_type with
         | TInt -> CBool(iop (int_of_const a) (int_of_const b))
         | TFloat -> CBool(fop (float_of_const a) (float_of_const b))
         | TDate -> 
           begin
             match a, b with
               | CDate(y1,m1,d1), CDate(y2,m2,d2) ->
                 CBool(iop (y1*10000+m1*100+d1) (y2*10000+m2*100+d2))
               | _ -> failwith (opname^" over invalid types")
           end
         | _ -> failwith (opname^" over invalid types")
      end
   (**/**)
   
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
         | (CDate(y1,m1,d1), CDate(y2,m2,d2))-> y1=y2 && m1=m2 && d1=d2
         | (CDate _, _) | (_, CDate _)     -> failwith "= of date and other"
         | (CFloat(_), _) | (_,CFloat(_))-> 
            (float_of_const a) = (float_of_const b)
         | (CInt(av), CInt(bv))          -> av = bv
      end)
   (** Perform a type-escalating not-equals comparison *)
   let cmp_neq a b = CBool((cmp_eq a b) = CBool(false))
   (** Find the type-escalating comparison operation for a Type.cmp_t *)
   let cmp op =
      begin match op with 
         | Lt  -> cmp_lt
         | Lte -> cmp_lte
         | Gt  -> cmp_gt
         | Gte -> cmp_gte
         | Eq  -> cmp_eq
         | Neq -> cmp_neq
      end
end
