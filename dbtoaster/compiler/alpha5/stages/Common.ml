
module Input = struct

   type frame_t = 
    | Fixed       of int
    | Delimited   of string
      
   type adaptor_t = string * (string * string) list
   
   type bytestream_options_t = frame_t * adaptor_t
   
   type source_t = 
    | Manual 
    | File   of string (*path*)               * bytestream_options_t
    | Socket of Unix.inet_addr * int (*port*) * bytestream_options_t
   
end

module Types = struct
   type type_t = IntegerT | DoubleT | StringT | AnyT
   
   type const_t = 
    | Integer of int 
    | Double  of float
    | String  of string
   
   type var_t = string * type_t

   type cmp_t = Lt | Gt | Lte | Gte | Eq | Neq

   type pm_t = Insert | Delete
   type event_t = pm_t * string
end
