open Common

type schema_t = (Calculus.var_t * bool) list

type external_t = External of 
   string      (* The name of the map being referenced *)
 * ( calc_t       (* The map's defining expression *)
   * invocation_t (* The initializer for the map *)
 )

and calc_t = external_t Calculus.calc_t

and map_t =
   string            (* Name *)
 * ( schema_t          (* Map Schema w.r.t. the defining expression *)
   * calc_t            (* Defining expression *)
   * (  Types.event_t 
      * map_op_t option (* LHS Initializer (if needed) *)
      * map_op_t list   (* Update Operations *)
     ) list
 )
 
and map_op_t = MapOp of schema_t * invocation_t

and invocation_t =
   calc_t            (* Defining expression *)
 * (  calc_t         (* One of several possible implementations of the defining
                        expression; Generally instantiated over several maps
                        expressed as externals within the invocation *)
   *  map_t ref list (* Maps used in the expression's definition *)
   ) list

val convert_calc: 'a Calculus.calc_t -> calc_t

val compile_map: string -> calc_t -> map_t
val compile_invocation: calc_t -> invocation_t

val string_of_map: ?indent:string -> map_t -> string
val string_of_invocation: ?indent:string -> invocation_t -> string
val string_of_schema: schema_t -> string
