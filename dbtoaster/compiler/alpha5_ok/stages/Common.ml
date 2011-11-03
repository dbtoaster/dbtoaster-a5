
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

module Arithmetic = struct
   let lift_op (int_op: int   -> int   -> int)
               (f_op  : float -> float -> float)
               (a:Types.const_t) (b:Types.const_t): Types.const_t =
      match (a,b) with
      | ((Types.Integer(ai)),(Types.Integer(bi))) -> 
            Types.Integer(int_op ai bi)
      | ((Types.Double(af)),(Types.Integer(bi))) -> 
            Types.Double(f_op af (float_of_int bi))
      | ((Types.Integer(ai)),(Types.Double(bf))) -> 
            Types.Double(f_op (float_of_int ai) bf)
      | ((Types.Double(af)),(Types.Double(bf))) -> 
            Types.Double(f_op af bf)
      | ((Types.String(_)),_) ->
            failwith ("BUG: Arithmetic over string")
      | (_,(Types.String(_))) ->
            failwith ("BUG: Arithmetic over string")
   
   let add = lift_op (fun a b -> a + b) (fun a b -> a +. b)
   let mult = lift_op (fun a b -> a * b) (fun a b -> a *. b)
   let add_list = List.fold_left add (Types.Integer(0))
   let mult_list = List.fold_left mult (Types.Integer(1))
end