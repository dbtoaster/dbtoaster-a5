(*
   Global typesystem for use by all of DBToaster's compilation stages.
*)

(**** Global type definitions ****)
type cmp_t = Eq | Lt | Lte | Gt | Gte | Neq
type type_t = 
   | TBool
   | TInt
   | TFloat
   | TString
   | TExternal of string
type const_t = 
   | CBool   of bool
   | CInt    of int
   | CFloat  of float
   | CString of string   
type var_t = string * type_t
type value_t = 
   | VConst of const_t
   | VVar   of var_t


(**** Conversion to Type ****)
let type_of_const (a:const_t): type_t =
   begin match a with
      | CBool(_)   -> TBool
      | CInt(_)    -> TInt
      | CFloat(_)  -> TFloat
      | CString(_) -> TString
   end
   
let type_of_value (v:value_t): type_t = 
   begin match v with
      | VConst(c)  -> type_of_const c
      | VVar(_,vt) -> vt
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
      | TString          -> "string"
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

let string_of_value (v: value_t): string =
   begin match v with
      | VConst(c) -> string_of_const c
      | VVar(v)   -> string_of_var v
   end

(**** Arithmetic ****)
let binary_op (b_op: bool   -> bool   -> bool)
              (i_op: int    -> int    -> int)
              (f_op: float  -> float  -> float)
              (a: const_t) (b: const_t): const_t =
   begin match (a,b) with
      | (CBool(av),  CBool(bv)) -> 
         CBool(b_op av bv)
      | (CBool(_),   CInt(_))
      | (CInt(_),    CBool(_)) 
      | (CInt(_),    CInt(_)) -> 
         CInt(i_op (int_of_const a) (int_of_const b))
      | (CFloat(_), (CBool(_)|CInt(_)|(CFloat(_))))
      | ((CBool(_)|CInt(_)), CFloat(_)) ->
         CFloat(f_op (float_of_const a) (float_of_const b))
      | (CString(_), _) | (_, CString(_)) -> 
         failwith "Binary math op over a string"
   end

let sum  = binary_op ( || ) ( + ) ( +. )
let prod = binary_op ( && ) ( * ) ( *. )
