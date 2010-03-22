(* m3 interface module *)


type const_t = (* CInt of int | *) CFloat of float (* | CBool of bool *)
type var_id_t = string
type var_type_t = VT_String | VT_Int
type var_t = var_id_t (*  * var_type_t *)
type map_id_t = string

                                                        (* inital val *)
type mapacc_t = map_id_t * (var_t list) * (var_t list) * calc_t
(* the variables are either of In or Out type; this is globally declared in
   the M3 program's (map_type_t list) field (see below). *)

and  calc_t = Add  of calc_t * calc_t
            | Mult of calc_t * calc_t
            | Const of const_t
            | Var of var_t
               (* if (cond != 0) then calc else 0 *)
            | IfThenElse0 of calc_t * calc_t
            | MapAccess of mapacc_t
               (* conditions*)
            | Leq of calc_t * calc_t
            | Eq  of calc_t * calc_t
            | Lt  of calc_t * calc_t


(*
type full_calc_t
*)
(* 
the calculus with Sum(t, Q) expressions
(including relational calculus or algebra)

Q is a conjunctive query, so it can be represented as a *list* of
atomic formulae. Or shall the frontend decide the join order?
*)

              (* left-hand side, increment *)
type stmt_t = mapacc_t * calc_t
(*
The loop vars are implicit: the variables of the left-hand side minus
the trigger arguments.

The bigsum vars are implicit: (the variables of the right-hand side
minus left-hand-side variables) - trigger vars
*)

type pm_t = Insert | Delete
type rel_id_t = string

(* (Insert/Delete, relation name, trigger args, ordered block of statements) *)
type trig_t = pm_t * rel_id_t * (var_t list) * (stmt_t list)
(* the front-end guarantees that the statements of the trigger are ordered
   in a way that old and new versions are accessed correctly, e.g.

   [ q  += q1; q1 += q ]

   if q wants to use the old version of q1, before it is updated.
   Thus the backend does not need to re-order maintain multiple versions of
   the maps.
*)   

(*                name       in_vars             out_vars *)
type map_type_t = map_id_t * (var_type_t list) * (var_type_t list)
(* the dependency graph of the maps must form a dag. All the top elements
   are queries accessible from the outside; all arguments of those maps must
   be of Out type. *)

type prog_t = (map_type_t list) * (trig_t list)


module Prepared =
struct
   
   type pextension_t   = var_t list
   
   (* id, theta extension, singleton, cross product *)
   type pcalcmeta_t    = int * pextension_t * bool * bool

   (* name, full aggregation *)   
   type paggmeta_t     = string * bool
   
   (* loop in vars extension *)
   type pstmtmeta_t    = pextension_t

   type pmapacc_t =
      map_id_t * (var_t list) * (var_t list) * aggecalc_t

   and  pcalc_t =
                | Const of const_t
                | Var of var_t
                | Add  of        ecalc_t * ecalc_t
                | Mult of        ecalc_t * ecalc_t
                | Leq  of        ecalc_t * ecalc_t
                | Eq   of        ecalc_t * ecalc_t
                | Lt   of        ecalc_t * ecalc_t
                | IfThenElse0 of ecalc_t * ecalc_t
                | MapAccess   of pmapacc_t
                
   and ecalc_t     = pcalc_t * pcalcmeta_t
   and aggecalc_t  = ecalc_t * paggmeta_t

   type pstmt_t    = pmapacc_t * aggecalc_t * pstmtmeta_t
   type ptrig_t    = pm_t * rel_id_t * (var_t list) * (pstmt_t list)
   type pprog_t    = (map_type_t list) * (ptrig_t list)
end

(* M3 data sources *)
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
  the value of off_to_end.
  
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
 | SocketSource of inet_addr
 
(* adaptor name, (param = value) list *)
type adaptor_t = string * (string * string) list

