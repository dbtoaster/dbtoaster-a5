(* m3 interface module *)


type const_t = int;; (* CString of string | CInt of int | CFloat of float;; *)
type var_id_t = string;;
type var_type_t = VT_String | VT_Int;;
type var_t = var_id_t;;  (*  * var_type_t *)
type map_id_t = string;;

type mapacc_t = map_id_t * (var_t list) * (var_t list);;
(* the variables are either of In or Out type; this is globally declared in
   the M3 program's (map_type_t list) field (see below). *)

type calc_t = Mult of calc_t * calc_t
            | Add  of calc_t * calc_t
            | Neg  of calc_t
            | Const of const_t
            | Var of var_t
            | IfThenElse0 of cond_t * calc_t  (* if cond then calc else 0 *)
            | MapAccess of mapacc_t
            | BigSum of (var_t list) * cond_t * calc_t
              (* BigSum(vs, cond[vs; ...], calc[vs; ...]):
                 all vars of vs must be of type Out in calc.
                 Sum up the calc values for those values of vs that
                 satisfy cond, where vs are In vars in cond. *)
and  cond_t = Leq of calc_t * calc_t
            | Eq  of calc_t * calc_t
            | Lt  of calc_t * calc_t;;
            (* Eq is eliminated by front-end *)

(*
type full_calc_t;;
*)
(* 
the calculus with Sum(t, Q) expressions
(including relational calculus or algebra)

Q is a conjunctive query, so it can be represented as a *list* of
atomic formulae. Or shall the frontend decide the join order?
*)

(*             (left-hand side,  init,         increment) *)
(*
type stmt_t = mapacc_t       * full_calc_t * calc_t;;
*)
type stmt_t = mapacc_t       *      calc_t * calc_t;;
(*
The loop vars are implicit: the variables of the left-hand side minus
the trigger arguments.
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


