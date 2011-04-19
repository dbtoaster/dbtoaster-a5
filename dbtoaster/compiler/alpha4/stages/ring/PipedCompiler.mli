
val generate_unit_test_code: PipedCalculus.piped_term_t -> string

val compile: 
   ?compile_deletes:bool ->
   ?bs_rewrite_mode:Calculus.bs_rewrite_mode_t ->
   (string * Calculus.var_t list) list ->
   PipedCalculus.piped_term_t -> 
   unit

