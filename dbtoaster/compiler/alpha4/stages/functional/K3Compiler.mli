module Make : functor(CG : K3Codegen.CG) ->  
sig
  (* Export for debugging purposes *)
  val compile_k3_expr : K3.SR.expr_t -> CG.code_t

  val compile_triggers : K3.SR.trigger list -> CG.code_t list

  (* Same interface as M3Compiler.compile_query *)
  val compile_query_to_code:
    ?disable_opt : bool
    -> (string * Calculus.var_t list) list
    -> M3.prog_t * M3.relation_input_t list
    -> string list -> CG.code_t
   
  val compile_query_to_string:
    (string * Calculus.var_t list) list
    -> M3.prog_t * M3.relation_input_t list
    -> string list -> string
   
  val compile_query :
    (string * Calculus.var_t list) list
    -> M3.prog_t * M3.relation_input_t list
    -> string list -> Util.GenericIO.out_t -> unit
end

(* K3 staging *)

(* Compile to a K3 program *)
val compile_query_to_program :
  ?disable_opt : bool -> ?optimizations : K3Optimizer.optimization_t list
  -> M3.prog_t -> K3.SR.program

