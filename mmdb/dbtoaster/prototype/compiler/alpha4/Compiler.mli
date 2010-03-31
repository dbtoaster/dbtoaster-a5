
(* A map_ref_t defines a map in compiler terms.  It consists of two components:
   1) definition: The Calculus expression that defines the map.  Evaluating this
      expression should produce the same output as the compiled triggers
   2) term: A Calculus term describing the map as seen by an external trigger.
      Use Calculus.decode_map_term to get it into a reasonable form; 
*)
(*               map_definition,   map_term *)
type map_ref_t = (Calculus.term_t * Calculus.term_t)

type trigger_definition =
(* delete, rel,    relvars,              params,               trigger expr *)
  (bool * string * Calculus.var_t list * Calculus.var_t list * Calculus.term_t)

(* Output translator will be called on maps DEPTH FIRST *)
type 'a output_translator = map_ref_t -> trigger_definition list -> 'a -> 'a

val compile: Calculus.bs_rewrite_mode_t ->            (* Bigsum Rewrite Mode *)
             (string * (Calculus.var_t list)) list -> (* Schema (Rel*Vars) *)
             map_ref_t ->                             (* Term to compile *)
             'a output_translator ->                  (* Output Translator *)
             'a -> 'a                                 (* Output Accumulator *)
