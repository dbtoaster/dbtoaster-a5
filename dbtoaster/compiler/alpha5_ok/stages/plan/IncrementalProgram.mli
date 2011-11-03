
type schema_t = IncrementalPlan.schema_t
type map_op_t = IncrementalPlan.map_op_t

type trigger_t = 
 ( Types.event_t
 * ( string 
   * map_op_t option
   * map_op_t list
   )
 ) 

type map_defn_t = string * schema_t * Common.Types.type_t

type prog_t = source_t list * map_defn_t list * trigger_t list


