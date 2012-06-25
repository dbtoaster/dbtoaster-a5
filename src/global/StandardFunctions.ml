open Types
open Constants

(**
   An internally maintained set of arithmetic functions (Arithmetic ring 
   elements of type [AFn]) for doing inline evaluation.
   
   Initially, a single function "/" is defined, which computes 1/x if it is 
   invoked with one parameter, or x/y if invoked with two parameters.
*)
let standard_functions: 
   (const_t list -> type_t -> const_t) StringMap.t ref = ref StringMap.empty

(**
   Determine whether an arithmetic function is defined internally.
   
   @param name   The name of a cuntion
   @return       True if [name] is defined
*)
let function_is_defined (name:string) =
   StringMap.mem name !standard_functions

(**
   Declare a new arithmetic function.
*)
let declare_function (name:string)  
                                (fn:const_t list -> type_t -> const_t): unit =
   standard_functions := 
      StringMap.add (String.uppercase name) fn !standard_functions
;;

(** An invalid function invocation -- This is typically non-fatal, as long as it
    happens before the interpreter stage, as it means that insufficient 
    information exists to pre-evaluate the specified function *)
exception InvalidInvocation of string;;

(** A runtime function invocation error.  This is always fatal. *)
exception FatalInvocationError of string;;

(**/**)
let invalid_args (fn:string) (arglist:const_t list) (ftype:type_t): const_t =
   raise (FatalInvocationError(
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
;; declare_function "/" fp_div ;;

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
;; declare_function "min" min_of_list ;;

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
;; declare_function "max" max_of_list ;;

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
            | "year"  -> CInt(y)
            | "month" -> CInt(m)
            | "day"   -> CInt(d)
            | _       -> invalid_args "date_part" arglist ftype
         end
      | _ -> invalid_args "date_part" arglist ftype
;; declare_function "date_part" date_part ;;

