(**
   A module for managing global definitions of library and user-defined 
   functions.  
   
   The functions defined using this module are referenced in
      - Sql via ExternalFn
      - Calculus/M3 via Arithmetic's AFn type
      - K3 via ExternalLambda()
   
   Each function has two names: 
      - A sql-friendly string identifier that can be used in Sql queries.
      - A more complex string identifier that corresponds to name of the
        function in the target environments.  
   
   Sql's ExternalFn refers to the first of these.  The SqlToCalculus module 
   translates the sql-friendly string name of the function into the more 
   complex target-language string.
   
   okennedy: This might be considered to be a hack, and we may want to put this
   translation into the K3 stage, or even the code generators.  That would allow
   users to declare separate identifiers for each target-language.  That said,
   Calculus and K3 both require function invocations to explicitly provide 
   typing information (while SQL infers it).  This saves us from having to use
   separate function declarations in Calculus/M3/K3.  
   
   A set of standard library functions may be found in the StandardFunctions
   module.
*)

open Types
open Constants
;;

type decl_t = {
  ret_type : type_t;
  implementation : string;
};;

type f_typechecker = type_t list -> decl_t;;

(**/**)
(* We keep separate maps containing the reference implementation of the 
   standard library functions, and a separate map for typing rules -- the latter
   is needed, since we will also use it for custom user-provided functions. *)
let standard_functions: 
   (const_t list -> type_t -> const_t) StringMap.t ref 
                           = ref StringMap.empty

let function_definitions:
   f_typechecker StringMap.t ref = ref StringMap.empty
(**/**)

(**
   Determine whether an arithmetic function is defined internally.
   
   @param name   The name of a cuntion
   @return       True if [name] is defined
*)
let function_is_defined (name:string) =
   StringMap.mem name !standard_functions

(**
   This exception is thrown if a function is invoked with invalid or otherwise
   mis-typed arguments.
*)
exception InvalidFunctionArguments of string

(**
   Declare a new standard library function.
*)
let declare_std_function (name:string)  
                         (fn:const_t list -> type_t -> const_t)
                         (typing_rule:type_t list -> type_t): unit =
   standard_functions := 
      StringMap.add (String.uppercase name) fn !standard_functions;
   function_definitions := 
      StringMap.add (String.uppercase name) (
         (fun (tl:type_t list) -> {
            ret_type = typing_rule tl;
            implementation = name
         })
      ) !function_definitions
;;

(** 
   Declare a new user-defined function
*)
let declare_usr_function (name:string) (arg_types:type_t list) (ret_type:type_t) 
                         (implementation:string): unit =
   let upper_name = String.uppercase name in
   standard_functions := 
      StringMap.remove upper_name !standard_functions;
   function_definitions := StringMap.add upper_name (
      let old_decl = 
         if StringMap.mem upper_name !function_definitions 
         then StringMap.find upper_name !function_definitions
         else (fun _ -> raise (InvalidFunctionArguments("")))
      in
         (fun cmp_types ->
            if cmp_types = arg_types 
            then {   ret_type = ret_type;
                     implementation = implementation; }
            else (old_decl cmp_types))
   ) !function_definitions

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
      end)^"("^(ListExtras.string_of_list ~sep:"," sql_of_const arglist)^")"
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
   Find a function declaration
*)
let declaration (fn:string) (argtypes:type_t list):decl_t =
   (StringMap.find (String.uppercase fn) !function_definitions) argtypes

(**
   Compute the type of a function
*)
let infer_type (fn:string) (argtypes:type_t list):type_t =
   let { ret_type = ret_type } = declaration fn argtypes in ret_type

(**
   Compute the implementation name of a function
*)
let implementation (fn:string) (argtypes:type_t list):string =
   let { implementation = impl } = declaration fn argtypes in impl

(**/**)
let escalate (argtypes:type_t list) = 
   try Types.escalate_type_list argtypes
   with Failure(msg) -> raise (InvalidFunctionArguments(msg))

let inference_error (): type_t =
   raise (InvalidFunctionArguments(""))
(**/**)
