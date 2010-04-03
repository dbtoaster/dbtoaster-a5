
(* Prettyprinting operations *)
val vars_to_string: M3.var_t list -> string
val string_of_const: M3.const_t -> string
val string_of_var_type: M3.var_type_t -> string

val pretty_print_map_access: M3.mapacc_t -> string
val pretty_print_calc: M3.calc_t -> string
val pretty_print_stmt: M3.stmt_t -> string
val pretty_print_trig: M3.trig_t -> string
val pretty_print_type_list: M3.var_type_t list -> string
val pretty_print_map: M3.map_type_t -> string
val pretty_print_prog: M3.prog_t -> string

val pcalc_to_string: M3.Prepared.pcalc_t -> string

(* Extract all output variables from a calculus expression *)
val calc_schema: M3.calc_t -> M3.var_t list
(* Extract ALL variables from a calculus expression *)
val calc_vars: M3.calc_t -> M3.var_t list

(*
  rename_maps src_maps dst_maps stmt
  Assumes that all the map name replacements are safe.
*)
val rename_maps: (M3.map_id_t) Map.Make(String).t -> M3.stmt_t -> M3.stmt_t

(* 
  rename_vars src_vars dst_vars stmt
  Replace all instances of src_vars with the corresponding entry in dst_vars.
  Also, makes sure that variable replacements are safe: If any variable in
  dst_vars is already bound in stmt, it will be replaced as needed.
*)
val rename_vars: M3.var_t list -> M3.var_t list -> M3.stmt_t -> M3.stmt_t

(* 
  Type-escalating algebra operations
*)
val c_sum: M3.const_t -> M3.const_t -> M3.const_t
val c_prod: M3.const_t -> M3.const_t -> M3.const_t

(* Prepared statemtent helpers - TODO: Yanif Document + types?*)
val get_calc: M3.Prepared.ecalc_t -> M3.Prepared.pcalc_t
val get_meta: M3.Prepared.ecalc_t -> M3.Prepared.pcalcmeta_t
val get_extensions: M3.Prepared.ecalc_t -> M3.Prepared.pextension_t
val get_id: M3.Prepared.ecalc_t -> int
val get_singleton: M3.Prepared.ecalc_t -> bool
val get_product: M3.Prepared.ecalc_t -> bool

val get_ecalc: M3.Prepared.aggecalc_t -> M3.Prepared.ecalc_t
val get_agg_meta: M3.Prepared.aggecalc_t -> M3.Prepared.paggmeta_t
val get_agg_name: M3.Prepared.paggmeta_t -> string
val get_full_agg: M3.Prepared.paggmeta_t -> bool

val get_inv_extensions: M3.Prepared.pstmtmeta_t -> M3.Prepared.pextension_t

val pcalc_schema: M3.Prepared.pcalc_t -> M3.var_t list

module Patterns : sig
  type pattern =
     In of (M3.var_t list * int list)
   | Out of (M3.var_t list * int list)
  type pattern_map = (string * pattern list) list
  
  val index: M3.var_t list -> M3.var_t -> int
  
  val make_in_pattern: M3.var_t list -> M3.var_t list -> pattern
  val make_out_pattern: M3.var_t list -> M3.var_t list -> pattern
  
  val get_pattern: pattern -> int list
  val get_pattern_vars: pattern -> M3.var_t list
  
  val empty_pattern_map: unit-> pattern_map
  
  val get_in_patterns: pattern_map -> string -> int list list
  val get_out_patterns: pattern_map -> string -> int list list
  
  val get_out_pattern_by_vars: 
          pattern_map -> string -> M3.var_t list -> int list
  
  val add_pattern: pattern_map -> (string * pattern) -> pattern_map
  
  val merge_pattern_maps: pattern_map -> pattern_map -> pattern_map
  
  val singleton_pattern_map: (string * pattern) -> pattern_map
  
  val patterns_to_string: pattern_map -> string

end
