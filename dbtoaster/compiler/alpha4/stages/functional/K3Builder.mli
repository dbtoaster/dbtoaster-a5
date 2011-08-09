open K3.SR

type map_sig_t = M3.map_id_t * M3.var_id_t list * M3.var_id_t list

(* Construction from M3 *)
val calc_to_singleton_expr :
  map_sig_t list -> M3.Prepared.calc_t -> map_sig_t list * expr_t

val op_to_expr :
  map_sig_t list ->
  (M3.var_t list -> expr_t -> expr_t -> expr_t) ->
  M3.Prepared.calc_t -> M3.Prepared.calc_t -> M3.Prepared.calc_t
  -> map_sig_t list * expr_t

val calc_to_expr : 
  map_sig_t list -> M3.Prepared.calc_t -> map_sig_t list * expr_t

val m3rhs_to_expr : M3.var_t list -> M3.Prepared.aggecalc_t -> expr_t

(* Incremental section *)
val collection_stmt : M3.var_t list -> M3.Prepared.stmt_t -> statement
val collection_trig : M3.Prepared.trig_t -> trigger
val collection_prog :
    M3.Prepared.prog_t -> M3Common.Patterns.pattern_map -> program

val m3_to_k3     : M3.prog_t -> (K3.SR.trigger list)
val m3_to_k3_opt :
  ?optimizations: K3Optimizer.optimization_t list
  -> M3.prog_t -> (K3.SR.trigger list)
