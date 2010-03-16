(* m3 interface module *)


type const_t = CInt of int | CFloat of float | CBool of bool
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
            | IfThenElse0 of calc_t * calc_t
                  (* if (cond != 0) then calc else 0 *)
            | MapAccess of mapacc_t
            | Null of (var_t list) (* the mythical null-slice *)
(* conditions*)
            | Leq of calc_t * calc_t
            | Eq  of calc_t * calc_t
            | Lt  of calc_t * calc_t
            | And of calc_t * calc_t


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

let vars_to_string vs = Util.list_to_string (fun x->x) vs;;

let rec pretty_print_map_access (mapn, inv, outv, init_calc): string =
  "{ "^mapn^"["^(vars_to_string inv)^"]["^(vars_to_string outv)^"] := ("^(pretty_print_calc init_calc)^") }"

and string_of_const v =
  match v with
      CInt(i)   -> string_of_int i
    | CFloat(f) -> string_of_float f
    | CBool(b)  -> if (b) then "false" else "true"

and string_of_var_type v = 
  match v with
      VT_String -> "STRING"
    | VT_Int    -> "INT"

and pretty_print_calc calc : string =
  let ots op c1 c2 = "("^(pretty_print_calc c1)^" "^op^" "^(pretty_print_calc c2)^")" in
  match calc with
      MapAccess(mapacc)  -> pretty_print_map_access(mapacc)
    | Add(c1, c2)        -> ots "+" c1 c2
    | Mult(c1, c2)       -> ots "*" c1 c2
    | Lt(c1, c2)         -> ots "<" c1 c2
    | Leq(c1, c2)        -> ots "<=" c1 c2
    | Eq(c1, c2)         -> ots "==" c1 c2
    | And(c1, c2)        -> ots "AND" c1 c2
    | IfThenElse0(c1,c2) -> "{ IF "^(pretty_print_calc c1)^" THEN "^(pretty_print_calc c2)^" }"
    | Null(outv)         -> "Null["^(vars_to_string outv)^"]"
    | Const(c)           -> string_of_const(c)
    | Var(x)             -> x

and pretty_print_trig (pm, rel_id, var_id, statements) : string =
  "ON " ^
  (match pm with Insert -> "+" | Delete -> "-") ^
  rel_id ^ "[" ^(vars_to_string var_id)^ "] : " ^
  (List.fold_left (fun oldstr (mapacc, delta_term) ->
    oldstr^"\n"^(pretty_print_map_access mapacc)^" += "^(pretty_print_calc delta_term)
  ) " " statements)^"\n"
  
and pretty_print_type_list (l:var_type_t list) : string = 
  "["^(List.fold_left (fun old_str v -> old_str^(string_of_var_type v)^", ") "" l)^"]"

and pretty_print_map (mapn, input_var_types, output_var_types) : string =
  mapn ^ " " ^ (pretty_print_type_list input_var_types) ^(pretty_print_type_list output_var_types)^"\n"

and pretty_print_prog ((maps, trigs):prog_t) = 
  (List.fold_left (fun oldstr map -> oldstr^(pretty_print_map map)) "" maps)^
  (List.fold_left (fun oldstr trig -> oldstr^(pretty_print_trig trig)) "" trigs);;

let rec c_sum (a : const_t) (b : const_t) : const_t = 
  match a with
      CInt(i1) -> (
        match b with
            CInt(i2) -> CInt(i1 + i2)
          | CFloat(f2) -> CFloat((float_of_int i1) +. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
    | CFloat(f1) -> (
        match b with
            CInt(i2) -> CFloat(f1 +. (float_of_int i2))
          | CFloat(f2) -> CFloat(f1 +. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
    | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)";;

let rec c_prod (a : const_t) (b : const_t) : const_t =
  match a with
      CInt(i1) -> (
        match b with
            CInt(i2) -> CInt(i1 * i2)
          | CFloat(f2) -> CFloat((float_of_int i1) *. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
    | CFloat(f1) -> (
        match b with
            CInt(i2) -> CFloat(f1 *. (float_of_int i2))
          | CFloat(f2) -> CFloat(f1 *. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
    | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)";;
    
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
                | And  of        ecalc_t * ecalc_t
                | IfThenElse0 of ecalc_t * ecalc_t
                | MapAccess   of pmapacc_t
                | Null of (var_t list)
                
   and ecalc_t     = pcalc_t * pcalcmeta_t
   and aggecalc_t  = ecalc_t * paggmeta_t

   type pstmt_t    = pmapacc_t * aggecalc_t * pstmtmeta_t
   type ptrig_t    = pm_t * rel_id_t * (var_t list) * (pstmt_t list)
   type pprog_t    = (map_type_t list) * (ptrig_t list)
end


