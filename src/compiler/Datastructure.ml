(* Mostly placeholders here for now *)

type type_t = 
   | TMap of Types.var_t list * Types.var_t list * Types.type_t

type t = string * type_t

