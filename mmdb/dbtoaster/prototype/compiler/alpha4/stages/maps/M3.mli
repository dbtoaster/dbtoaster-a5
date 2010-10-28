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


module type Metadata = sig
  type calcmeta_t
  type aggmeta_t
  type stmtmeta_t
  
  type calc_t =   (calcmeta_t, aggmeta_t) generic_calc_t
  type mapacc_t = (calcmeta_t, aggmeta_t) generic_mapacc_t
  type stmt_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_stmt_t
  type trig_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_trig_t
  type prog_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_prog_t
  
  val string_of_calcmeta : calcmeta_t -> string
  val string_of_aggmeta  : aggmeta_t  -> string
  val string_of_stmtmeta : stmtmeta_t -> string
end

(******************* Base Types *******************)

type aggmeta_t = unit;;
type calcmeta_t = unit;;
type stmtmeta_t = unit;;

type calc_t =   (calcmeta_t, aggmeta_t) generic_calc_t;;
type mapacc_t = (calcmeta_t, aggmeta_t) generic_mapacc_t;;
type stmt_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_stmt_t;;
type trig_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_trig_t;;
type prog_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_prog_t;;

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

(* Math *)
val c_sum  : const_t -> const_t -> const_t;;
val c_prod : const_t -> const_t -> const_t;;

(* Meta-to-string *)
val string_of_calcmeta : calcmeta_t -> string
val string_of_aggmeta  : aggmeta_t  -> string
val string_of_stmtmeta : stmtmeta_t -> string

(******************* Compiler Metadata Extension *******************)

module Prepared : sig
  type pextension_t   = var_t list
   
  type pprebind_t        = (var_t * var_t) list
  type pinbind_t         = (var_t * int) list
  
  (* short-circuit, bf-equality bindings *)
  type pcondition_meta_t = bool * pprebind_t * pinbind_t 
   
  (* id, theta extension, singleton, cross product, condition metadata *)
  type calcmeta_t    = int * pextension_t * bool * bool * (pcondition_meta_t option)  
  
  (* name, full aggregation *)   
  type aggmeta_t     = string * bool
  
  (* loop in vars extension *)
  type stmtmeta_t    = pextension_t
  
  type pcalc_t = (calcmeta_t, aggmeta_t) generic_calc_contents_t
  type calc_t = (calcmeta_t, aggmeta_t) generic_calc_t
  type aggecalc_t = (calcmeta_t, aggmeta_t) generic_aggcalc_t
  
  type mapacc_t = (calcmeta_t, aggmeta_t) generic_mapacc_t
  type stmt_t = (calcmeta_t, aggmeta_t, stmtmeta_t) generic_stmt_t
  type trig_t = (calcmeta_t, aggmeta_t, stmtmeta_t) generic_trig_t
  type prog_t = (calcmeta_t, aggmeta_t, stmtmeta_t) generic_prog_t

  (* Accessors *)
  val get_calc :          calc_t -> pcalc_t
  val get_meta :          calc_t -> calcmeta_t
  val get_extensions :    calc_t -> pextension_t
  val get_id :            calc_t -> int
  val get_singleton :     calc_t -> bool
  val get_product :       calc_t -> bool
  val get_cond_meta :     calc_t -> pcondition_meta_t option
  val get_short_circuit : calc_t -> bool
  val get_prebind :       calc_t -> pprebind_t
  val get_inbind :        calc_t -> pinbind_t
  
  val get_ecalc :       aggecalc_t -> calc_t
  val get_agg_meta :    aggecalc_t -> aggmeta_t
  val get_agg_name :    aggmeta_t -> string
  val get_full_agg :    aggmeta_t -> bool
  
  val get_inv_extensions : stmtmeta_t -> pextension_t
  
  val string_of_calcmeta : calcmeta_t -> string
  val string_of_aggmeta  : aggmeta_t  -> string
  val string_of_stmtmeta : stmtmeta_t -> string
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