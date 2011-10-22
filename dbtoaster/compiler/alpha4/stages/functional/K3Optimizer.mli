type optimization_t = CSE | Beta | NoFilter
val optimize :
  ?optimizations: optimization_t list
  -> M3.var_t list -> K3.SR.expr_t -> K3.SR.expr_t

type ds_trig_decl = M3.pm_t * M3.rel_id_t * M3.var_t list 
type ds_map_decls = M3.map_type_t list * M3Common.Patterns.pattern_map 

type ds_statement = ds_map_decls * K3.SR.expr_t list
type ds_trigger = ds_trig_decl * ds_statement list
type ds_program = ds_map_decls * ds_trigger list

val optimize_with_datastructures :
  (string * Calculus.var_t list) list -> (* dbschema *)
  K3.SR.program -> ds_program

(******** Debugging interface.  DO NOT USE ******)
val inline_collection_functions :
   ((K3.SR.id_t * K3.SR.type_t) * (K3.SR.expr_t)) list ->
   K3.SR.expr_t ->
   K3.SR.expr_t