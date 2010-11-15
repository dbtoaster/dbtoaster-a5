module Make : functor(CG : K3Codegen.CG) ->  
sig
  (* Export for debugging purposes *)
  val compile_k3_expr : K3.SR.expr_t -> CG.code_t

  val compile_triggers : K3.SR.trigger list -> CG.code_t list

  (* Same interface as M3Compiler.compile_query *)
  val compile_query :
    (string * Calculus.var_t list) list
    -> M3.prog_t * M3.relation_input_t list
    -> string list -> Util.GenericIO.out_t -> unit
end