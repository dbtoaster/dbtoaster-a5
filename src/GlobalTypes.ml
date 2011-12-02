(*
   Global typesystem for use by all of DBToaster's compilation stages.
*)

module StringMap = Map.Make(String)

(**** Global type definitions ****)
type cmp_t = Eq | Lt | Lte | Gt | Gte | Neq
type type_t = 
   | TBool
   | TInt
   | TFloat
   | TString   of int
   | TExternal of string
type const_t = 
   | CBool   of bool
   | CInt    of int
   | CFloat  of float
   | CString of string
type var_t = string * type_t

(**** Conversion to Type ****)
let type_of_const (a:const_t): type_t =
   begin match a with
      | CBool(_)   -> TBool
      | CInt(_)    -> TInt
      | CFloat(_)  -> TFloat
      | CString(s) -> TString(String.length s)
   end

(**** Number conversions ****)
let int_of_const (a:const_t): int = 
   begin match a with
      | CBool(true)  -> 1
      | CBool(false) -> 0
      | CInt(av)     -> av
      | CFloat(av)   -> int_of_float av
      | CString(av)  -> failwith "Cannot produce string of integer"
   end

let float_of_const (a:const_t): float = 
   begin match a with
      | CBool(true)  -> 1.
      | CBool(false) -> 0.
      | CInt(av)     -> float_of_int av
      | CFloat(av)   -> av
      | CString(av)  -> failwith "Cannot produce string of float"
   end
   
(**** Conversion to Strings ****)
let string_of_cmp (op:cmp_t): string = 
   begin match op with 
      | Eq  -> "="  | Lt -> "<" | Lte -> "<="
      | Neq -> "!=" | Gt -> ">" | Gte -> ">="
   end
   
let string_of_type (ty: type_t): string =
   begin match ty with
      | TBool            -> "bool"
      | TInt             -> "int"
      | TFloat           -> "float"
      | TString(len)     -> "string("^(string_of_int len)^")"
      | TExternal(etype) -> "external<"^etype^">"
   end

let string_of_const (a: const_t): string = 
   begin match a with
      | CBool(true)  -> "true"
      | CBool(false) -> "false"
      | CInt(av)     -> string_of_int av
      | CFloat(av)   -> string_of_float av
      | CString(av)  -> av
   end

let string_of_var ((name, vt): var_t): string =
   "<"^name^":"^(string_of_type vt)^">"
