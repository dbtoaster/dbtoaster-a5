(**
   An internally library of reference implementations of External functions.  
   These correspond to:
      - AFn in Arithmetic
      - ExternalLambda in K3
   and should be implemented by all runtimes.
*)

open Types
open Constants

(**/**)
(* We keep separate maps containing the reference implementation of the 
   standard library functions, and a separate map for typing rules -- the latter
   is needed, since we will also use it for custom user-provided functions. *)
let standard_functions: 
   (const_t list -> type_t -> const_t) StringMap.t ref = ref StringMap.empty

let typing_rules:
   (type_t list -> type_t) StringMap.t ref = ref StringMap.empty
(**/**)

(**
   Determine whether an arithmetic function is defined internally.
   
   @param name   The name of a cuntion
   @return       True if [name] is defined
*)
let function_is_defined (name:string) =
   StringMap.mem name !standard_functions

(**
   Declare a new standard library function.
*)
let declare_std_function (name:string)  
                         (fn:const_t list -> type_t -> const_t)
                         (typing_rule:type_t list -> type_t): unit =
   standard_functions := 
      StringMap.add (String.uppercase name) fn !standard_functions;
   typing_rules := 
      StringMap.add (String.uppercase name) typing_rule !typing_rules
;;

(** 
   Declare a new user-defined function
*)
let declare_usr_function (name:string)
                         (typing_rule:type_t list -> type_t): unit =
   standard_functions := 
      StringMap.remove (String.uppercase name) !standard_functions;
   typing_rules := 
      StringMap.add (String.uppercase name) typing_rule !typing_rules

(**
   This exception is thrown if a function is invoked with invalid or otherwise
   mis-typed arguments.
*)
exception InvalidFunctionArguments of string

(** An invalid function invocation -- This is typically non-fatal, as long as it
    happens before the interpreter stage, as it means that insufficient 
    information exists to pre-evaluate the specified function *)
exception InvalidInvocation of string;;

(**/**)
let invalid_args (fn:string) (arglist:const_t list) (ftype:type_t): const_t =
   raise (InvalidFunctionArguments(
      "Invalid arguments of "^fn^(begin match ftype with
         | TAny -> "" 
         | _ -> ":"^(string_of_type ftype)
      end)^"("^(ListExtras.string_of_list ~sep:"," string_of_const arglist)^")"
   ))
(**/**)

(**
   Invoke an arithmetic function
*)
let invoke (fn:string) (arglist:const_t list) (ftype:type_t) =
   if not (function_is_defined (String.uppercase fn)) then
      raise (InvalidInvocation("Undefined function: "^fn))
   else
      (StringMap.find (String.uppercase fn) !standard_functions) arglist ftype

(**
   Compute the type of a function
*)
let infer_type (fn:string) (argtypes:type_t list) =
   (StringMap.find (String.uppercase fn) !typing_rules) argtypes

(**/**)
let escalate (argtypes:type_t list) = 
   try Types.escalate_type_list argtypes
   with Failure(msg) -> raise (InvalidFunctionArguments(msg))

let inference_error (): type_t =
   raise (InvalidFunctionArguments(""))
(**/**)

(**
   Floating point division.  
    - [fp_div] num returns [1/(float)num]
    - [fp_div] a;b returns [(float)a / (float)b]
*)
let fp_div (arglist:const_t list) (ftype:type_t) =
   begin match arglist with
      | [v]     -> Constants.Math.div1 ftype v
      | [v1;v2] -> Constants.Math.div2 ftype v1 v2
      | _ -> invalid_args "fp_div" arglist ftype 
   end
;; declare_std_function "/" fp_div 
   (function | (([_;_] as x) | ([_] as x)) -> (escalate (TFloat::x))
             | _ -> inference_error ());;

(**
   Bounded fan-in list minimum.
    - [min_of_list] elems returns the smallest number in the list.
*)
let min_of_list (arglist:const_t list) (ftype:type_t) =
   let (start,cast_type) = 
      match ftype with TInt -> (CInt(max_int), TInt)
                     | TAny 
                     | TFloat -> (CFloat(max_float), TFloat)
                     | _ -> (invalid_args "min" arglist ftype, TAny)
   in List.fold_left min start (List.map (Constants.type_cast cast_type) 
                                         arglist)
;; declare_std_function "min" min_of_list (fun x -> escalate (TInt::x)) ;;

(**
   Bounded fan-in list maximum.
    - [max_of_list] elems returns the largest number in the list.
*)
let max_of_list (arglist:const_t list) (ftype:type_t) =
   let (start,cast_type) = 
      match ftype with TInt -> (CInt(min_int), TInt)
                     | TAny 
                     | TFloat -> (CFloat(min_float), TFloat)
                     | _ -> (invalid_args "max" arglist ftype, TAny)
   in List.fold_left max start (List.map (Constants.type_cast cast_type) 
                                         arglist)
;; declare_std_function "max" max_of_list (fun x -> escalate (TInt::x)) ;;

(**
   Date part extraction
    - [date_part] 'year';date returns the year of a date as an int
    - [date_part] 'month';date returns the month of a date as an int
    - [date_part] 'day';date returns the day of the month of a date as an int
*)
let date_part (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CString(part);  CDate(y,m,d)] -> 
         begin match String.uppercase part with
            | "YEAR"  -> CInt(y)
            | "MONTH" -> CInt(m)
            | "DAY"   -> CInt(d)
            | _       -> invalid_args "date_part" arglist ftype
         end
      | _ -> invalid_args "date_part" arglist ftype
;; declare_std_function "date_part" date_part 
               (function [TString; TDate] -> TInt | _-> inference_error ());;

(**
   Regular expression matching
   - [regexp_match] regex_str; str returns true if regex_str matches str
*)
let regexp_match (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CString(regexp); CString(str)] ->
         Debug.print "LOG-REGEXP" (fun () -> "/"^regexp^"/ =~ '"^str^"'");
         if Str.string_match (Str.regexp regexp) str 0
         then CInt(1) else CInt(0)
      | _ -> invalid_args "regexp_match" arglist ftype
;; declare_std_function "regexp_match" regexp_match
               (function [TString; TString] -> TInt | _-> inference_error ()) ;;

(**
   Substring
   - [substring] [str; start; len] returns the substring of str from start to start+len
*)
let substring (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CString(str); CInt(start); CInt(len)] ->
            CString(String.sub str start len)
      | _ -> invalid_args "substring" arglist ftype
;; declare_std_function "substring" substring
         (function [TString; TInt; TInt] -> TString | _-> inference_error ()) ;;
