open GlobalTypes

type type_t = 
   | Map    of var_t list * type_t
   | Cache  of var_t list * var_t list * type_t

type t = string * type_t
