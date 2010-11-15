(* M3Compiler.
 * A compilation framework for producing machine code for an M3 program. *)
 
(* M3 preprocessing function to prepare M3 programs.
 * Decorates M3 programs (see M3Compiler.ml for details), and normalizes
 * trigger variables. *)
val prepare_triggers :
   M3.trig_t list -> M3.Prepared.trig_t list * M3Common.Patterns.pattern_map

(* A functorized compiler, taking a language-specific code generator, to
 * compile an M3 program to source code in the target language. *)
module Make : functor(CG : M3Codegen.CG) ->
sig
   (* An internal function for compiling a list of triggers, currently
    * used by unit tests and examples.
    * TODO: these unit tests/examples should be rewritten to use compile_query
    * below, and require a simple source/adaptor for feeding inputs.
    *) 
   val compile_ptrig : M3.Prepared.trig_t list * M3Common.Patterns.pattern_map
      -> CG.code_t list
      
   (* M3 compilation, requiring an DB schema, M3 program,
    * a list of inputs for the program (i.e. sources,adaptors, etc.),
    * a list of toplevel query names, and an output file.
    * Writes source code to the output file.  *)
   val compile_query :
      (string * Calculus.var_t list) list
      -> M3.prog_t * M3.relation_input_t list
      -> string list -> Util.GenericIO.out_t -> unit
end