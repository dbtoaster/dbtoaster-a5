(******************* Utility Operations *******************)

(* Tree traversals *)
val recurse_calc :
      (* op (+,*,<,<=,=) -> lhs -> rhs -> ret *)
    (string -> 'b -> 'b -> 'b) ->
      (* condition -> term -> ret *)
    ('b -> 'b -> 'b) ->
      (* inline_agg -> (map id * ivars * ovars *init) -> ret *)
    (bool -> ('c,'a) M3.generic_mapacc_t -> 'b) ->
    (M3.const_t -> 'b) ->
    (M3.var_t -> 'b) ->
    (('c,'a) M3.generic_calc_t) -> 'b

val recurse_calc_with_meta :
      (* meta -> op (+,*,<,<=,=) -> lhs -> rhs -> ret *)
    ('c -> string -> 'b -> 'b -> 'b) ->
      (* meta -> condition -> term -> ret *)
    ('c -> 'b -> 'b -> 'b) ->
      (* inline_agg -> meta -> (map id * ivars * ovars *init) -> ret *)
    ('c -> bool -> ('c,'a) M3.generic_mapacc_t -> 'b) ->
    ('c -> M3.const_t -> 'b) ->
    ('c -> M3.var_t -> 'b) ->
    (('c,'a) M3.generic_calc_t) -> 'b

val recurse_calc_lf :
    ('b -> 'b -> 'b) ->
      (* inline_agg -> (map id * ivars * ovars *init) -> ret *)
    (bool -> ('c,'a) M3.generic_mapacc_t -> 'b) ->
    (M3.const_t -> 'b) ->
    (M3.var_t -> 'b) ->
    (('c,'a) M3.generic_calc_t) -> 'b

val replace_calc_lf :
      (* inline_agg -> (map id * ivars * ovars *init) -> ret *)
    (bool->('c,'a) M3.generic_mapacc_t -> ('c,'a) M3.generic_calc_contents_t) ->
    (M3.const_t -> ('c,'a) M3.generic_calc_contents_t) ->
    (M3.var_t -> ('c,'a) M3.generic_calc_contents_t) ->
    (('c,'a) M3.generic_calc_t) -> ('c,'a) M3.generic_calc_t

val replace_calc_lf_with_meta :
      (* meta -> inline_agg -> (map id * ivars * ovars *init) -> ret *)
    ('c -> bool -> ('c,'a) M3.generic_mapacc_t -> ('c,'a) M3.generic_calc_t) ->
    ('c -> M3.const_t -> ('c,'a) M3.generic_calc_t) ->
    ('c -> M3.var_t -> ('c,'a) M3.generic_calc_t) ->
    (('c,'a) M3.generic_calc_t) -> ('c,'a) M3.generic_calc_t

(* Querying *)
  (* Output variables from maps in calculus expression *)
val calc_schema : (('c,'a) M3.generic_calc_t) -> M3.var_t list;;
  (* Input variables from maps in calculus expression *)
val calc_params : (('c,'a) M3.generic_calc_t) -> M3.var_t list;;
  (* All variables used by a calculus expression *)
val calc_vars   : (('c,'a) M3.generic_calc_t) -> M3.var_t list;;


(* Batch Modification *)
  (* Replace every occurrance of a map name in the statement with the 
     corresponding entry in the StringMap (if one exists) *)
val rename_maps : 
    M3.map_id_t Util.StringMap.t -> (('c,'a,'s) M3.generic_stmt_t) -> 
    (('c,'a,'s) M3.generic_stmt_t);;
  (* Replace every occurrance of a var name in the first var list with the
     corresponding var name in the second var list.  Any var names in the
     second var list already ocurring in the statement will be replaced with
     unique names.  (ie, the resulting statement is guaranteed to use 
     variable names from the replacement (second) list only where the original
     statement used the corresponding variable name from the first list.
  *)
val rename_vars : 
    M3.var_t list -> M3.var_t list -> (('c,'a,'s) M3.generic_stmt_t) -> 
    (('c,'a,'s) M3.generic_stmt_t)

(* String Output *)
val vars_to_string : M3.var_t list -> string
val string_of_const : M3.const_t -> string
val string_of_var_type : M3.var_type_t -> string
val string_of_type_list : M3.var_type_t list -> string
val opstring_of_stmt_type : M3.stmt_type_t -> string

val pretty_print_map_access : M3.mapacc_t   -> string;;
val pretty_print_calc       : M3.calc_t     -> string;;
val pretty_print_stmt       : M3.stmt_t     -> string;;
val pretty_print_trig       : M3.trig_t     -> string;;
val pretty_print_map        : M3.map_type_t -> string;;
val pretty_print_prog       : M3.prog_t     -> string;;

module PreparedPrinting : sig
  val pretty_print_map_access : M3.Prepared.mapacc_t   -> string;;
  val pretty_print_calc       : M3.Prepared.calc_t     -> string;;
  val pretty_print_stmt       : M3.Prepared.stmt_t     -> string;;
  val pretty_print_trig       : M3.Prepared.trig_t     -> string;;
  val pretty_print_map        : M3.map_type_t           -> string;;
  val pretty_print_prog       : M3.Prepared.prog_t     -> string;;
end

val code_of_map_access : ('c,'a) M3.generic_mapacc_t  -> string;;
val code_of_calc       : ('c,'a) M3.generic_calc_t    -> string;;
val code_of_stmt       : ('c,'a,'s) M3.generic_stmt_t -> string;;

(******************* Patterns *******************)
module Patterns : sig
  (* positive patterns: vars that are bound (i.e. not wildcarded) *)
  type pattern =
     In of (M3.var_t list * int list)
   | Out of (M3.var_t list * int list)

  (* associative list of map name -> in/out patterns *)
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
  
  val string_of_pattern: pattern -> string

  val patterns_to_string: pattern_map -> string

end

val string_of_declarations :
  M3.map_type_t list * Patterns.pattern_map -> string list
