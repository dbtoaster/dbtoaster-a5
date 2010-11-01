module Make : functor(CG : K3Codegen.CG) ->  
sig
  val compile_triggers : K3.SR.trigger list -> CG.code_t list

  val compile_query : M3.prog_t * M3.relation_input_t list ->
    string list -> Util.GenericIO.out_t -> unit
end