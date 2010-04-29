
(* A map_ref_t defines a map in compiler terms.  It consists of two components:
   1) definition: The Calculus expression that defines the map.  Evaluating this
      expression should produce the same output as the compiled triggers
   2) term: A Calculus term describing the map as seen by an external trigger.
      Use Calculus.decode_map_term to get it into a reasonable form; 
*)
(*               map_definition,    map_term, *)
type map_ref_t = (Calculus.term_t * Calculus.term_t)

type bound_vars_t = 
(* params,               bigsum_vars *)
  (Calculus.var_t list * Calculus.var_t list)
  
type trigger_definition_t =
(* delete, rel,    relvars,              var types,     trigger expr *)
  (bool * string * Calculus.var_t list * bound_vars_t * Calculus.term_t)

(* Output translator will be called on maps BREADTH FIRST *)
type 'a output_translator_t = 
  (string * (Calculus.var_t list)) list ->(* Database Schema *)
  map_ref_t ->                  (* The target map *)
  trigger_definition_t list ->  (* All the triggers for the map *)
  'a ->                         (* A target-specific accumulator field *)
  'a                            (* The new value of the accumulator *)

val compile: ?dup_elim:Calculus.term_t Util.StringMap.t ref -> (* Ignore *)
             Calculus.bs_rewrite_mode_t ->            (* Bigsum Rewrite Mode *)
             (string * (Calculus.var_t list)) list -> (* Schema (Rel*Vars) *)
             map_ref_t ->                             (* Term to compile *)
             'a output_translator_t ->                (* Output Translator *)
             'a -> 'a                                 (* Output Accumulator *)


(* Auxilliary compilation function. 
   You should not need to call this. *)
val generate_unit_test_code: (string list) output_translator_t

val compile_delta_for_rel:
  string -> Calculus.var_t list -> bool -> Calculus.term_t ->
  Calculus.var_t list -> Calculus.term_mapping_t -> Calculus.term_t ->
  ((bool * string * Calculus.var_t list * bound_vars_t * 
    Calculus.term_t) list * Calculus.term_mapping_t)
