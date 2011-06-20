type optimization_t = CSE | Beta 
val optimize :
  ?optimizations: optimization_t list
  -> M3.var_t list -> K3.SR.expr_t -> K3.SR.expr_t