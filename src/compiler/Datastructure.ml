open GlobalTypes

(* Mostly placeholders here for now *)

type type_t = 
   | MapT of var_t list * var_t list * type_t

type t = string * type_t

