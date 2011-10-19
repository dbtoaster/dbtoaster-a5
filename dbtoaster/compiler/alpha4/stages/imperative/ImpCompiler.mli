type compiler_options = { desugar : bool; profile : bool }

(* Type signature for target language functor *)
module type ImpTarget =
sig
  open Imperative
  open Imperative.Program

  type ext_type
  type ext_fn
  type type_env_t    

  val string_of_ext_type : ext_type type_t -> string
  val string_of_ext_fn : ext_fn -> string
  val string_of_ext_imp : (ext_type, ext_fn) typed_imp_t -> string

  (* Serialization code generation *)
  
  (* Generate source code that resides within an entry definition
   * for serialization *)
  val serialization_source_code_of_entry :
    K3.SR.id_t -> (K3.SR.id_t * ext_type type_t) list -> source_code_t
    
  (* Map serialization invocation *)
  val source_code_of_map_serialization :
    K3.SR.id_t -> K3.SR.id_t -> source_code_t

  (* Source code generation *)
  val source_code_of_imp : (ext_type, ext_fn) typed_imp_t -> source_code_t
  val source_code_of_trigger :
    (string * Calculus.var_t list) list ->
    (M3.pm_t * M3.rel_id_t * M3.var_t list * (ext_type, ext_fn) typed_imp_t) 
    -> source_code_t list
    
  val desugar_expr :
    compiler_options -> type_env_t
    -> (ext_type, ext_fn)  typed_expr_t 
    -> ((ext_type, ext_fn) typed_imp_t list) option * 
       ((ext_type, ext_fn) typed_expr_t) option

  val desugar_imp :
    compiler_options -> type_env_t
    -> (ext_type, ext_fn) typed_imp_t -> (ext_type, ext_fn) typed_imp_t

  (* Typing interface *)
  val var_type_env : type_env_t -> (K3.SR.id_t * ext_type type_t) list

  val type_env_of_declarations :
    (K3.SR.id_t * ext_type type_t) list ->
    M3.map_type_t list -> M3Common.Patterns.pattern_map -> type_env_t

  val infer_types :
    type_env_t -> ('a option, ext_type, ext_fn) imp_t
    -> (ext_type, ext_fn) typed_imp_t

  (* Profiler code generation *)
  val declare_profiling : M3.map_type_t list -> source_code_t
  val profile_trigger :
       int                         (* statement id offset, across all triggers *)
    -> (int * (string * int list) list) (* map ivc offset, across all triggers *)
    -> (string * Calculus.var_t list) list
    -> M3.map_type_t list
    -> (ext_type type_t, ext_type, ext_fn) Program.trigger_t
    -> (int * (string * int list) list) *
       (source_code_t list  *
         (ext_type type_t, ext_type, ext_fn) Program.trigger_t) 

  (* Toplevel code generation *)

  (* TODO: all of these should accept a compiler options record as input *)
  val preamble : compiler_options -> source_code_t

  (* map schemas -> patterns -> type declarations * instance declarations *) 
  val declare_maps_of_schema :
    M3.map_type_t list -> M3Common.Patterns.pattern_map
    -> source_code_t * ((ext_type, ext_fn) typed_imp_t) list

  val declare_streams :
    (string * Calculus.var_t list) list -> (string * int) list * source_code_t

  val declare_sources_and_adaptors :
    M3.relation_input_t list -> (ext_type, ext_fn) typed_expr_t list * source_code_t

  val declare_main :
    compiler_options
    -> (string * int) list -> M3.map_type_t list
    -> (ext_type, ext_fn) typed_expr_t list
       (* Trigger registration metadata *)
    -> (string * string * string * (string * string) list) list *
       (string * int list) list
    -> string list -> source_code_t * source_code_t
end

module CPPTarget : ImpTarget

(* Imperative stage: supports compilation from a K3 program *)

module type CompilerSig =
sig
  type trigger_setup_t
  type compiler_trig_t =
    (CPPTarget.ext_type Imperative.type_t,
     CPPTarget.ext_type, CPPTarget.ext_fn) Imperative.Program.trigger_t

  val compile_triggers :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3.SR.program                    (* K3 program *)
    -> (SourceCode.source_code_t list * compiler_trig_t) list * trigger_setup_t

  val compile_query_to_string :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3.SR.program                    (* K3 program *)
    -> M3.relation_input_t list         (* sources *)
    -> string list -> string

  val compile_imp :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> M3.map_type_t list * M3Common.Patterns.pattern_map *
       ((SourceCode.source_code_t list * compiler_trig_t) list * trigger_setup_t)
    -> M3.relation_input_t list         (* sources *)
    -> string list -> Util.GenericIO.out_t -> unit

  val compile_query :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3.SR.program                    (* K3 program *)
    -> M3.relation_input_t list         (* sources *)
    -> string list -> Util.GenericIO.out_t -> unit
    
  (* DS program compilation *)
  val compile_ds_triggers :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3Optimizer.ds_program           (* K3 datastructure program *)
    -> (SourceCode.source_code_t list * compiler_trig_t) list * trigger_setup_t

  val compile_ds_query :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3Optimizer.ds_program           (* K3 datastructure program *)
    -> M3.relation_input_t list         (* sources *)
    -> string list -> Util.GenericIO.out_t -> unit

end

module Compiler : CompilerSig
