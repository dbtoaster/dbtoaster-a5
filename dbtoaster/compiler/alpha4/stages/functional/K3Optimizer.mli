type optimization_t = CSE | Beta 
val optimize :
  ?optimizations: optimization_t list
  -> M3.var_t list -> K3.SR.expr_t -> K3.SR.expr_t

(******** Debugging interface.  DO NOT USE ******)
val inline_collection_functions :
   ((K3.SR.id_t * K3.SR.type_t) * (K3.SR.expr_t)) list ->
   K3.SR.expr_t ->
   K3.SR.expr_t