(* m3 interface module *)


type const_t = int;; (* CString of string | CInt of int | CFloat of float;; *)
type var_id_t = string;;
type var_type_t = VT_String | VT_Int;;
type var_t = var_id_t;;  (*  * var_type_t *)
type map_id_t = string;;

                                                        (* inital val *)
type mapacc_t = map_id_t * (var_t list) * (var_t list) * calc_t
(* the variables are either of In or Out type; this is globally declared in
   the M3 program's (map_type_t list) field (see below). *)

and  calc_t = Add  of calc_t * calc_t
            | Mult of calc_t * calc_t
            | Const of const_t
            | Var of var_t
            | IfThenElse0 of calc_t * calc_t
                  (* if (cond != 0) then calc else 0 *)
            | MapAccess of mapacc_t
            | Null of (var_t list) (* the mythical null-slice *)
(* conditions*)
            | Leq of calc_t * calc_t
            | Eq  of calc_t * calc_t
            | Lt  of calc_t * calc_t
;;

(*
type full_calc_t;;
*)
(* 
the calculus with Sum(t, Q) expressions
(including relational calculus or algebra)

Q is a conjunctive query, so it can be represented as a *list* of
atomic formulae. Or shall the frontend decide the join order?
*)

              (* left-hand side, increment *)
type stmt_t = mapacc_t * calc_t;;
(*
The loop vars are implicit: the variables of the left-hand side minus
the trigger arguments.

The bigsum vars are implicit: (the variables of the right-hand side
minus left-hand-side variables) - trigger vars
*)

type pm_t = Insert | Delete;;
type rel_id_t = string;;

(* (Insert/Delete, relation name, trigger args, ordered block of statements) *)
type trig_t = pm_t * rel_id_t * (var_t list) * (stmt_t list);;
(* the front-end guarantees that the statements of the trigger are ordered
   in a way that old and new versions are accessed correctly, e.g.

   [ q  += q1; q1 += q ]

   if q wants to use the old version of q1, before it is updated.
   Thus the backend does not need to re-order maintain multiple versions of
   the maps.
*)   

(*                name       in_vars             out_vars *)
type map_type_t = map_id_t * (var_type_t list) * (var_type_t list);;
(* the dependency graph of the maps must form a dag. All the top elements
   are queries accessible from the outside; all arguments of those maps must
   be of Out type. *)

type prog_t = (map_type_t list) * (trig_t list);;


(*
let validate_prog prog =
   ()
*)




