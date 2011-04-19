(*
EXTRACTING MAP ACCESS STRING FROM STATEMENT
SPLITING BETWEEN THE LEFT AND THE RIGHT HAND SIDE MAP ACCESSES
NO INITIALIZERS COVERED

Here we created statement and then using "code_of_map_access" method 
we created string. 
Note that this string is not nicely indented.
Point of this example is to make basics for extracting map accesses from statements.

COPIED AND MODIFIED FROM M3Common.ml, 
String, Utility and lines 285-298    
*)

open Util;;
open M3;;

(******************* Utility Operations *******************)
(* Extract map name from map_access *)
let extract_map_name 
  ((mapn, inv, outv, init) : ('c,'a) generic_mapacc_t) : string =
  mapn
;;

let rec mapacc_from_calc 
  (calc : ('c, 'a) generic_calc_t) : ('c,'a) generic_mapacc_t list = 
  match (fst calc) with
    | MapAccess(ma) -> [ma]
    | Add (c1, c2) -> (mapacc_from_calc c1) @ (mapacc_from_calc c2)
    | Mult(c1, c2) -> (mapacc_from_calc c1) @ (mapacc_from_calc c2)
    | Lt  (c1, c2) -> (mapacc_from_calc c1) @ (mapacc_from_calc c2)
    | Leq (c1, c2) -> (mapacc_from_calc c1) @ (mapacc_from_calc c2)
    | Eq  (c1, c2) -> (mapacc_from_calc c1) @ (mapacc_from_calc c2)
    | IfThenElse0(c1, c2) -> (mapacc_from_calc c1) @ (mapacc_from_calc c2)
    | Const(c) -> []
    | Var(v) -> []
;;

(*
In order for initializers to work 
(IfThenElse may be inside initializer of MapAccess, which we skip,
and may contain another Map Accesses)
we need to process initializer argument of MapAccess with 
snd calc
on line
MapAccess(ma) -> [ma]
*)
let maps_of_stmt
  ((mapacc, (calc, _), _) : ('c,'a,'sm) generic_stmt_t)
  : ((('c,'a) generic_mapacc_t) * (('c,'a) generic_mapacc_t list)) =
  (mapacc, (mapacc_from_calc calc))
;;
(*left side map,  right hand side maps     *)


(******************* String Output *******************)
(* Printing basic types *)
let vars_to_string vs = Util.list_to_string (fun x -> x) vs

let string_of_var_type v = 
  match v with
    | VT_String -> "STRING"
    | VT_Int    -> "INT"
    | VT_Float  -> "FLOAT"

let string_of_map_access 
  ((mapn, inv, outv, init) : ('c,'a) generic_mapacc_t) : string = 
  mapn ^ ", " ^ (vars_to_string inv) ^ ", " ^ (vars_to_string outv)
;;
(* (code_of_calc (fst init)) 
This is why initializers do not work*)

let stmt_to_string 
  (statement : (('c, 'a, 'sm) generic_stmt_t)) : string =
  let (left_ma, right_ma_list) = maps_of_stmt statement in
  "\nLEFT SIDE: \n" ^ string_of_map_access left_ma ^ "\n" ^
  "\nRIGHT SIDE: \n" ^ Util.list_to_string string_of_map_access right_ma_list ^ "\n"
;;
