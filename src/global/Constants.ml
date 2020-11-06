(**   
   Global system for wrapping typed primitive constants throughout DBToaster
*)

open Type
;;

type interval_t =
   | CYearMonth of int * int (** Year-month-interval *)
   | CDay       of int       (** Day interval *) 

(** Basic Constants *)
type const_t = 
   | CBool         of bool              (** Boolean  *)
   | CInt          of int               (** Integer *)
   | CFloat        of float             (** Float *)
   | CChar         of char              (** Character *)
   | CString       of string            (** String *)
   | CDate         of int * int * int   (** Date *)
   | CInterval     of interval_t        (** Interval *)

(**** Basic Operations ****)
(** 
   Compute the type of a given constant
   @param a   A constant
   @return    The type of [a]
*)
let type_of_const (a:const_t): type_t =
   begin match a with
      | CBool(_)     -> TBool
      | CInt(_)      -> TInt
      | CFloat(_)    -> TFloat
      | CChar(_)     -> TChar
      | CString(_)   -> TString
      | CDate _      -> TDate
      | CInterval(CYearMonth _) -> TInterval(TYearMonth)
      | CInterval(CDay _)       -> TInterval(TDay)
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
      | CChar(av)    -> int_of_char av
      | CString(av)  -> int_of_string av
      | CDate _      -> failwith ("Cannot produce integer of date")
      | CInterval _  -> failwith ("Cannot produce integer of interval")
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
      | CChar(av)    -> float_of_int (int_of_char av)
      | CString(av)  -> float_of_string av
      | CDate _      -> failwith ("Cannot produce float of date")
      | CInterval _  -> failwith ("Cannot produce float of interval")
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

(**
   [parse_interval time_unit value]
   
   Parses a time unit and a value encoded as a string and converts 
   it into an Interval constant.
   @param time_unit  The time unit (day, month, year)
   @param value      The value
   @iv_qual          Interval qualifier as defined in the SQL 92 standard
   @return           The Interval constant
*)
let parse_interval (time_unit: string) 
                   (value: string) 
                   (iv_qual: int option): const_t =
   let int_value = int_of_string value in
   match time_unit, iv_qual with
   | "DAY", Some(3) -> 
      CInterval(CDay(int_value))
   | "DAY", _ -> failwith ("DBToaster currently only support dates, no times")
   | "MONTH", _ ->  
      CInterval(CYearMonth(0, int_value))
   | "YEAR", _ -> 
      CInterval(CYearMonth(int_value, 0))
   | str, _ -> failwith ("Expected DAY, MONTH or YEAR found: " ^ str)
      
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
      | CChar(av)    -> String.make 1 av
      | CString(av)  -> av
      | CDate(y,m,d) -> (string_of_int y) ^ "-" ^
                        (string_of_int m) ^ "-" ^
                        (string_of_int d)
      | CInterval(CDay(d)) -> string_of_int d   
      | CInterval(CYearMonth(y,m)) -> (string_of_int y) ^ "-" ^ 
                                      (string_of_int m)
end

(**
   Get the string representation (corresponding to the OCaml defined above) of
   a constant
   @param a   A constant
   @return    The string representation of the OCaml constant declaration of [a]
*)
let ocaml_of_const ?(prefix=false) (a: const_t): string =
   let str_pfx = if(prefix) then "Constants." else "" in
   begin match a with
      | CBool(true)  -> str_pfx ^ "CBool(true)"
      | CBool(false) -> str_pfx ^ "CBool(false)"
      | CInt(i)      -> str_pfx ^ "CInt("^(string_of_int i)^")"
      | CFloat(f)    -> str_pfx ^ "CInt("^(string_of_float f)^")"
      | CChar(c)     -> str_pfx ^ "CChar("^(String.make 1 c)^")"
      | CString(s)   -> str_pfx ^ "CString(\""^s^"\")"
      | CDate(y,m,d) -> str_pfx ^ "CDate(" ^ (string_of_int y) ^ "," ^
                                             (string_of_int m) ^ "," ^
                                             (string_of_int d) ^ ")"
      | CInterval(CDay(d)) -> 
         str_pfx ^ "CInterval(" ^ str_pfx ^ "CDay(" ^ (string_of_int d) ^ "))"   
      | CInterval(CYearMonth(y,m)) ->
         str_pfx ^ "CInterval(" ^ str_pfx ^ "CYearMonth(" ^ 
         (string_of_int y) ^ "," ^ (string_of_int m) ^ "))"
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
      | CChar(c)     -> "'"^(String.make 1 c)^"'"
      | CString(s)   -> "'"^s^"'"
      | CDate(y,m,d) -> "DATE('"^(string_of_const a)^"')"
      | CInterval(CYearMonth(y,m)) -> 
         "INTERVAL '" ^ (string_of_int y) ^ "-" ^ (string_of_int m) ^ "' YEAR" 
      | CInterval(CDay(d)) -> "INTERVAL '" ^ (string_of_int d) ^ "' DAY '3'" 
   end

(**
   Checks whether a value is zero.
   @param v     The value.
   @return      True if the value is zero.
*)  
let is_zero (a:const_t): bool =
   match a with 
   | CInt(0)
   | CBool(false) 
   | CFloat(0.0) -> true
   | _ -> false

(**
   Checks whether a value is one.
   @param a     The value.
   @return      True if the value is one.
*)  
let is_one (a:const_t): bool =
   match a with 
   | CInt(1)
   | CBool(true) 
   | CFloat(1.0) -> true
   | _ -> false

(**** Zero Constants ****)
(**
   Returns a constant representing zero of type [zt].
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

(**
   Returns a constant option representing zero of type [zt].
   @param zt    A type. Can be TBool, TInt or TFloat.
   @return      The constant zero of type [zt], None if it does not exist for
                the given type
*)  
let zero_of_type_opt zt : const_t option = 
   begin match zt with
      | TBool -> Some(CBool(false))
      | TInt  -> Some(CInt(0))
      | TFloat -> Some(CFloat(0.))
      | _ -> None
   end


(**** One Constants ****)
(**
   Returns a constant representing one of type [ot].
   @param ot    A type. Can be TBool, TInt or TFloat.
   @return      The constant one of type [ot]
   @raise Failure If there is no one constant corrsponding to [ot]
*)  
let one_of_type ot : const_t = 
   begin match ot with
      | TBool -> CBool(true)
      | TInt  -> CInt(1)
      | TFloat -> CFloat(1.)
      | _ -> failwith ("Cannot produce one of type '"^(string_of_type ot)^"'")
   end

(**
   Returns a constant optionrepresenting one of type [ot].
   @param ot    A type. Can be TBool, TInt or TFloat.
   @return      The constant one of type [ot], None if it does not exist for
                the given type
*)  
let one_of_type_opt ot : const_t option = 
   begin match ot with
      | TBool -> Some(CBool(true))
      | TInt  -> Some(CInt(1))
      | TFloat -> Some(CFloat(1.))
      | _ -> None
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

(**
   Casts a DBToaster date into a unix time
   @param date A date in the DBToaster form
   @return The unix time of [date]
*) 
let date_to_unix_time (date: const_t): Unix.tm = 
   begin match date with
   | CDate(y,m,d) -> 
      {Unix.tm_sec=0; tm_min=0; tm_hour=0;
            tm_mday=d; tm_mon=m; tm_year=y;
            tm_wday=0; tm_yday=0; tm_isdst=false}
   | _ -> failwith ("Expected date, found: " ^ (string_of_const date))
   end

(**
   Casts a unix time to a DBToaster date (ignoring time of the date)
   @param tm A unix time
   @return The DBToaster date of [tm]
*)
let unix_time_to_date (tm: float): const_t =
   let {Unix.tm_sec=_; tm_min=_; tm_hour=_;
     tm_mday=d; tm_mon=m; tm_year=y;
     tm_wday=_; tm_yday=_; tm_isdst=_} = Unix.localtime tm 
   in
   CDate(y, m, d)

(**
   Adds an interval to a date
   @param a A date
   @param b An interval
   @result The result of adding [b] to date [a]
*)
let (+<>) (a: const_t) (b: const_t): const_t =
   match a, b with
   | CDate(_), CInterval(CDay(ds))->
      let tm, _ = Unix.mktime (date_to_unix_time a) in
      let res = tm +. 24. *. 60. *. 60. *. (float_of_int ds) in 
      unix_time_to_date res 
   | CDate(y,m,d), CInterval(CYearMonth(y_iv,m_iv)) ->
      CDate(y + y_iv, m + m_iv, d)
   | _, _ -> failwith ("Expected date + interval") 

(** Math operations over constants ****)
module Math = struct
   (** Perform type-escalating addition over two constants *)
   let sum (a: const_t) (b: const_t): const_t = 
      begin match (a, b) with
      | CBool(av), CBool(bv) -> failwith ("Sum of booleans") 
      | CBool(_), CInt(_) | CInt(_),  CBool(_) | CInt(_),  CInt(_) ->
         CInt((int_of_const a) + (int_of_const b))
      | CFloat(_), (CBool(_) | CInt(_) | CFloat(_))
      | (CBool(_) | CInt(_)), CFloat(_) -> 
         CFloat((float_of_const a) +. (float_of_const b))
      | (CDate(_) as d), (CInterval(_) as iv)
      | (CInterval(_) as iv), (CDate(_) as d) -> d +<> iv
      | CInterval(CDay(ds1)), CInterval(CDay(ds2)) -> CInterval(CDay(ds1 + ds2))
      | CInterval(CYearMonth(y1,m1)), CInterval(CYearMonth(y2,m2)) ->
         CInterval(CYearMonth(y1 + y2, m1 + m2))
      | _, _ -> 
         failwith ("Sum of " ^
                   (string_of_const a) ^ " and " ^ (string_of_const b))
      end    

   (** Perform type-escalating addition over an arbitrary number of constants *)
   let suml = List.fold_left sum (CInt(0))

   (** Perform type-escalating multiplication over two constants *)
   let prod (a: const_t) (b: const_t): const_t = 
      begin match (a, b) with
      | CBool(av), CBool(bv) -> CBool(av && bv) 
      | CBool(_), CInt(_) | CInt(_),  CBool(_) | CInt(_),  CInt(_) ->
         CInt((int_of_const a) * (int_of_const b))
      | CFloat(_), (CBool(_) | CInt(_) | CFloat(_))
      | (CBool(_) | CInt(_)), CFloat(_) -> 
         CFloat((float_of_const a) *. (float_of_const b))
      | ((CBool(_) | CInt(_)) as i), CInterval(CDay(ds))
      | CInterval(CDay(ds)), ((CBool(_) | CInt(_)) as i) ->
         CInterval(CDay(ds * (int_of_const i)))
      | ((CBool(_) | CInt(_)) as i), CInterval(CYearMonth(y,m))
      | CInterval(CYearMonth(y,m)), ((CBool(_) | CInt(_)) as i) ->
         let ib = int_of_const i in
         CInterval(CYearMonth(y * ib, m * ib))
      | _, _ -> 
         failwith ("Multiplication of " ^
                   (string_of_const a) ^ " and " ^ (string_of_const b))
      end   
 
   (** Perform type-escalating multiplication over an arbitrary number of 
       constants *)
   let prodl = List.fold_left prod (CInt(1))

   (** Negate a constant *)
   let neg (a: const_t): const_t = prod (CInt(-1)) a   

   (** Perform type-escalating division of two constants *)
   let div2 dtype a b =       
      begin match (a, b, dtype) with
      | (CBool(av),  CBool(bv), (TBool|TAny)) -> 
         failwith ("Division of booleans")
      | (CBool(_),   CBool(_),  TInt)
      | (CBool(_),   CInt(_),   (TInt|TAny))
      | (CInt(_),    CBool(_),  (TInt|TAny)) 
      | (CInt(_),    CInt(_),   (TInt|TAny))  -> 
         CInt((int_of_const a) / (int_of_const b))      
      | (CBool(_),   CBool(_),  TFloat)
      | (CInt(_),    CInt(_),   TFloat)      
      | (CFloat(_), (CBool(_)|CInt(_)|(CFloat(_))), (TFloat|TAny))
      | ((CBool(_)|CInt(_)), CFloat(_), (TFloat|TAny)) -> 
         CFloat((float_of_const a) /. (float_of_const b))
      | (_,   _,  _) -> 
         failwith ("Division of " ^
                   (string_of_const a) ^ " and " ^ (string_of_const b)) 
      end

   (** Compute the multiplicative inverse of a constant *)
   let div1 dtype a = div2 dtype (CInt(1)) a 
   
   (**/**)
   let comparison_op (opname:string) 
                     (iop:int -> int -> bool) 
                     (fop:float -> float -> bool) 
                     (a:const_t)
                     (b:const_t):const_t =
      let op_type = (escalate_type ~opname:opname (type_of_const a) 
                                                  (type_of_const b)) in
      begin match op_type with
      | TInt -> CBool(iop (int_of_const a) (int_of_const b))
      | TFloat -> CBool(fop (float_of_const a) (float_of_const b))
      | TDate -> 
         begin match a, b with
         | CDate(y1,m1,d1), CDate(y2,m2,d2) ->
            CBool(iop (y1*10000+m1*100+d1) (y2*10000+m2*100+d2))
         | _ -> failwith (opname^" over invalid types")
         end
      | TInterval(TYearMonth) ->
         begin match a, b with
         | CInterval(CYearMonth(y1,m1)), CInterval(CYearMonth(y2,m2)) ->
            CBool(iop (y1 * 10000 + m1) (y2 * 10000 + m2))
         | _ -> failwith (opname^" over invalid types")
         end
      | TInterval(TDay) ->
         begin match a, b with
         | CInterval(CDay(d1)), CInterval(CDay(d2)) ->
            CBool(iop d1 d2)
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
         | (CChar(av), CChar(bv))        -> av = bv
         | (CChar(_), _) | (_,CChar(_))  -> failwith "= of char and other"
         | (CString(av), CString(bv))    -> av = bv
         | (CString(_), _) | (_,CString(_))-> failwith "= of string and other"
         | (CDate(y1,m1,d1), CDate(y2,m2,d2))-> y1=y2 && m1=m2 && d1=d2
         | (CDate _, _) | (_, CDate _)     -> failwith "= of date and other"
         | (CInterval _, _) | (_, CInterval _) -> 
            failwith "= of interval and other"
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
