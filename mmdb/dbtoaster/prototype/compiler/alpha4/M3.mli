(* m3 interface module *)

type const_t = (* CInt of int | *) CFloat of float (* | CBool of bool *)
type var_id_t = string
type var_type_t = VT_String | VT_Int | VT_Float
type var_t = var_id_t (*  * var_type_t *)
type map_id_t = string
type pm_t = Insert | Delete
type rel_id_t = string
(*                name       in_vars             out_vars *)
type map_type_t = map_id_t * (var_type_t list) * (var_type_t list)

(*
  There are several variants of M3.  These variants differ only in the metadata
  that they keep around.  Because they share a common structure, most utility
  operations are defined using a generic functorized module.  The base version
  of M3 (without metadata) is included into the M3 module itself, while other(s)
  need to be referenced via their sub-module.
*)

(* 
  IMPORTANT NOTE:
  M3 Calculus is NOT guaranteed to be commutative.  Evaluation order is 
  important, because of information flow.  That is, output variables from
  one map access might be used as input variables in another map access, or
  equivalently, may appear alone.
  
  M3 follows the ordering produced by the compiler:
  
  Add/Mult/Leq/Eq/Lt: 
    Evaluate LEFT hand side first.  In most binary operators, variables are 
    referenced to the right of the map access that binds them to a value 
    (unless they're bound to the update relation or as an input variable)
      
  IfThenElse0
    Evaluate RIGHT hand side first.  Unlike other binary operators, 
    IfThenElse0's left hand side acts as a filter over the slice returned by
    the right hand side.
  
  MapAccess/Const/Var:
    Not binary operators.
*)
type ('calcmeta_t, 'aggmeta_t) generic_aggcalc_t = 
  ('calcmeta_t, 'aggmeta_t) generic_calc_t * 'aggmeta_t
and ('c, 'a) generic_calc_contents_t = 
    Add         of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
  | Mult        of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t 
             (* if (cond != 0) then calc else 0 *)
  | IfThenElse0 of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t 
  | MapAccess   of ('c,'a) generic_mapacc_t
  | Const of const_t
  | Var   of var_t
             (* conditions: 1 if calc OP calc else 0 *)
  | Leq of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
  | Eq  of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
  | Lt  of ('c,'a) generic_calc_t * ('c,'a) generic_calc_t
and ('c, 'a) generic_calc_t = ('c,'a) generic_calc_contents_t * 'c
 
(* Map Access:
  the variables are either of In or Out type; this is globally declared in
  the M3 program's (map_type_t list) field (see below). 
*)
and ('c, 'a) generic_mapacc_t = 
  map_id_t * (var_t list) * (var_t list) * ('c,'a) generic_aggcalc_t

(* left-hand side, increment *)
type ('c, 'a, 'sm) generic_stmt_t = 
  ('c, 'a) generic_mapacc_t * ('c, 'a) generic_aggcalc_t * 'sm

(*
The loop vars are implicit: the variables of the left-hand side minus
the trigger arguments.

The bigsum vars are implicit: (the variables of the right-hand side
minus left-hand-side variables) - trigger vars
*)
(* (Insert/Delete, rel name, trigger args, ordered block of statements) *)
type ('c, 'a, 'sm) generic_trig_t = 
  pm_t * rel_id_t * (var_t list) * (('c, 'a, 'sm) generic_stmt_t list)

(* the front-end guarantees that the statements of the trigger are ordered
   in a way that old and new versions are accessed correctly, e.g.

   [ q  += q1; q1 += q ]

   if q wants to use the old version of q1, before it is updated.
   Thus the backend does not need to re-order maintain multiple versions of
   the maps.
*)   

(* the dependency graph of the maps must form a dag. All the top elements
   are queries accessible from the outside; all arguments of those maps must
   be of Out type. *)

type ('c,'a,'sm) generic_prog_t = 
  (map_type_t list) * (('c,'a,'sm) generic_trig_t list)

(******************* Base Types *******************)

type calc_t = (unit, unit) generic_calc_t;;
type mapacc_t = (unit, unit) generic_mapacc_t;;
type stmt_t = (unit, unit, unit) generic_stmt_t;;
type trig_t = (unit, unit, unit) generic_trig_t;;
type prog_t = (unit, unit, unit) generic_prog_t;;

module Prepared : sig

  type pextension_t   = var_t list
   
  (* id, theta extension, singleton, cross product *)
  type pcalcmeta_t    = int * pextension_t * bool * bool
  
  (* name, full aggregation *)   
  type paggmeta_t     = string * bool
  
  (* loop in vars extension *)
  type pstmtmeta_t    = pextension_t
  
  type pcalc_t = (pcalcmeta_t, paggmeta_t) generic_calc_contents_t
  type ecalc_t = (pcalcmeta_t, paggmeta_t) generic_calc_t
  type aggecalc_t = (pcalcmeta_t, paggmeta_t) generic_aggcalc_t
  
  type pmapacc_t = (pcalcmeta_t, paggmeta_t) generic_mapacc_t
  type pstmt_t = (pcalcmeta_t, paggmeta_t, pstmtmeta_t) generic_stmt_t
  type ptrig_t = (pcalcmeta_t, paggmeta_t, pstmtmeta_t) generic_trig_t
  type pprog_t = (pcalcmeta_t, paggmeta_t, pstmtmeta_t) generic_prog_t

  (* Accessors *)
  val get_calc :        ecalc_t -> pcalc_t
  val get_meta :        ecalc_t -> pcalcmeta_t
  val get_extensions :  ecalc_t -> pextension_t
  val get_id :          ecalc_t -> int
  val get_singleton :   ecalc_t -> bool
  val get_product :     ecalc_t -> bool
  
  val get_ecalc :       aggecalc_t -> ecalc_t
  val get_agg_meta :    aggecalc_t -> paggmeta_t
  val get_agg_name :    paggmeta_t -> string
  val get_full_agg :    paggmeta_t -> bool
  
  val get_inv_extensions : pstmtmeta_t -> pextension_t
end

(******************* Utility Operations *******************)
(* Construction *)
val mk_sum : calc_t -> calc_t -> calc_t 
val mk_prod : calc_t -> calc_t -> calc_t 
val mk_lt : calc_t -> calc_t -> calc_t
val mk_leq : calc_t -> calc_t -> calc_t 
val mk_eq : calc_t -> calc_t -> calc_t 
val mk_if : calc_t -> calc_t -> calc_t 
val mk_ma : mapacc_t -> calc_t 
val mk_c : float -> calc_t
val mk_v : string -> calc_t

(* Tree traversals *)
val recurse_calc :
      (* op (+,*,<,<=,=) -> lhs -> rhs -> ret *)
    (string -> 'b -> 'b -> 'b) ->
      (* condition -> term -> ret *)
    ('b -> 'b -> 'b) ->
      (* map id -> ivars -> ovars -> init -> ret *)
    (('c,'a) generic_mapacc_t -> 'b) ->
    (const_t -> 'b) ->
    (var_t -> 'b) ->
    (('c,'a) generic_calc_t) -> 'b

val recurse_calc_with_meta :
      (* meta -> op (+,*,<,<=,=) -> lhs -> rhs -> ret *)
    ('c -> string -> 'b -> 'b -> 'b) ->
      (* meta -> condition -> term -> ret *)
    ('c -> 'b -> 'b -> 'b) ->
      (* meta -> map id -> ivars -> ovars -> init -> ret *)
    ('c -> ('c,'a) generic_mapacc_t -> 'b) ->
    ('c -> const_t -> 'b) ->
    ('c -> var_t -> 'b) ->
    (('c,'a) generic_calc_t) -> 'b

val recurse_calc_lf :
    ('b -> 'b -> 'b) ->
      (* map id -> ivars -> ovars -> init -> ret *)
    (('c,'a) generic_mapacc_t -> 'b) ->
    (const_t -> 'b) ->
    (var_t -> 'b) ->
    (('c,'a) generic_calc_t) -> 'b

val replace_calc_lf :
      (* map id -> ivars -> ovars -> init -> ret *)
    (('c,'a) generic_mapacc_t -> ('c,'a) generic_calc_contents_t) ->
    (const_t -> ('c,'a) generic_calc_contents_t) ->
    (var_t -> ('c,'a) generic_calc_contents_t) ->
    (('c,'a) generic_calc_t) -> ('c,'a) generic_calc_t

val replace_calc_lf_with_meta :
      (* meta -> map id -> ivars -> ovars -> init -> ret *)
    ('c -> ('c,'a) generic_mapacc_t -> ('c,'a) generic_calc_t) ->
    ('c -> const_t -> ('c,'a) generic_calc_t) ->
    ('c -> var_t -> ('c,'a) generic_calc_t) ->
    (('c,'a) generic_calc_t) -> ('c,'a) generic_calc_t

(* Math *)
val c_sum  : const_t -> const_t -> const_t;;
val c_prod : const_t -> const_t -> const_t;;

(* Querying *)
  (* Output variables used by a calculus expression *)
val calc_schema : (('c,'a) generic_calc_t) -> var_t list;;
  (* All variables used by a calculus expression *)
val calc_vars   : (('c,'a) generic_calc_t) -> var_t list;;

(* Batch Modification *)
  (* Replace every occurrance of a map name in the statement with the 
     corresponding entry in the StringMap (if one exists) *)
val rename_maps : map_id_t Util.StringMap.t -> 
                  (('c,'a,'s) generic_stmt_t) -> (('c,'a,'s) generic_stmt_t);;
  (* Replace every occurrance of a var name in the first var list with the
     corresponding var name in the second var list.  Any var names in the
     second var list already ocurring in the statement will be replaced with
     unique names.  (ie, the resulting statement is guaranteed to use 
     variable names from the replacement (second) list only where the original
     statement used the corresponding variable name from the first list.
  *)
val rename_vars : var_t list -> var_t list -> 
                  (('c,'a,'s) generic_stmt_t) -> (('c,'a,'s) generic_stmt_t)

(* String Output *)
val vars_to_string : var_t list -> string
val string_of_const : const_t -> string
val string_of_var_type : var_type_t -> string
val string_of_type_list : var_type_t list -> string

val pretty_print_map_access : ('c,'a) generic_mapacc_t   -> string;;
val pretty_print_calc       : ('c,'a) generic_calc_t     -> string;;
val pretty_print_stmt       : ('c,'a,'s) generic_stmt_t  -> string;;
val pretty_print_trig       : ('c,'a,'s) generic_trig_t  -> string;;
val pretty_print_map        :         map_type_t         -> string;;
val pretty_print_prog       : ('c,'a,'s) generic_prog_t  -> string;;

val code_of_map_access : ('c,'a) generic_mapacc_t  -> string;;
val code_of_calc       : ('c,'a) generic_calc_t    -> string;;
val code_of_stmt       : ('c,'a,'s) generic_stmt_t -> string;;

(******************* Patterns *******************)
module Patterns : sig
  type pattern =
     In of (var_t list * int list)
   | Out of (var_t list * int list)
  type pattern_map = (string * pattern list) list
  
  val index: var_t list -> var_t -> int
  
  val make_in_pattern: var_t list -> var_t list -> pattern
  val make_out_pattern: var_t list -> var_t list -> pattern
  
  val get_pattern: pattern -> int list
  val get_pattern_vars: pattern -> var_t list
  
  val empty_pattern_map: unit-> pattern_map
  
  val get_in_patterns: pattern_map -> string -> int list list
  val get_out_patterns: pattern_map -> string -> int list list
  
  val get_out_pattern_by_vars: 
          pattern_map -> string -> var_t list -> int list
  
  val add_pattern: pattern_map -> (string * pattern) -> pattern_map
  
  val merge_pattern_maps: pattern_map -> pattern_map -> pattern_map
  
  val singleton_pattern_map: (string * pattern) -> pattern_map
  
  val patterns_to_string: pattern_map -> string

end

(******************* M3 Data Sources *******************)

(* events returned by adaptors, implicit to a single stream *)
type event = pm_t * const_t list

(* events returned by sources, where sources decorate adaptor events
 * w/ their associated stream *)
(* TODO: use integers for relation ids for dispatching rather than strings *)
type stream_event = pm_t * rel_id_t * const_t list

(* Data source model
 * -- sources correspond to I/O resources.
 * -- adaptors correspond to events per relation/stream.
 * -- 1 source may feed multiple adaptors, e.g. sources can represent
 *    multiplexed data streams (i.e. union streams).
 * -- source implementations must perform dispatching to adaptors.
 *)


(* 
  Most of these are self explanatory.  VarSize is a little nonintuitive.  A
  VarSize-delimited frame is assumed to have an integer (4 byte) size field
  located at off_to_size bytes into the stream.  The size field is assumed to
  contain the number of bytes remaining in the frame AFTER THE SIZE FIELD plus
  the value of off_to_end. (so bytes remaining = [size] - off_to_end)
  
  In other words, the size of the frame delivered to the adaptor will be:
    off_to_size + 4 + [size] - off_to_end
  
  If the size field contains the size of the data following it, off_to_end = 0
  If the size field contains the total frame size, off_to_end = off_to_size
*)
type framing_t = 
    FixedSize of int     (* FixedSize(width)     *)
  | Delimited of string  (* Delimited(delimiter) *)
  | VarSize of int * int (* VarSize(off_to_size, off_to_end) *)

type source_t =
   FileSource of string
 | PipeSource of string
 | SocketSource of Unix.inet_addr * int
 
(* adaptor name, (param = value) list *)
type adaptor_t = string * (string * string) list

(* A fully described relation/input source: 
     file|socket * framing descriptor * relation_name * adaptor 
*)
type relation_input_t = source_t * framing_t * string * adaptor_t