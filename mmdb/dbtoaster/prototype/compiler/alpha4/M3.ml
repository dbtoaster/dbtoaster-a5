(* m3 interface module *)
open Util

type const_t = (* CInt of int | *) CFloat of float (* | CBool of bool *)
type var_id_t = string
type var_type_t = VT_String | VT_Int | VT_Float
type var_t = var_id_t (*  * var_type_t *)
type map_id_t = string
type pm_t = Insert | Delete
type rel_id_t = string
type map_type_t = map_id_t * (var_type_t list) * (var_type_t list)

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

and ('c, 'a) generic_mapacc_t = 
  map_id_t * (var_t list) * (var_t list) * ('c,'a) generic_aggcalc_t
type ('c, 'a, 'sm) generic_stmt_t = 
  ('c, 'a) generic_mapacc_t * ('c, 'a) generic_aggcalc_t * 'sm
type ('c, 'a, 'sm) generic_trig_t = 
  pm_t * rel_id_t * (var_t list) * (('c, 'a, 'sm) generic_stmt_t list)
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

(* M3 Basic - Types *)
type aggmeta_t = unit;;
type calcmeta_t = unit;;
type stmtmeta_t = unit;;

type calc_t =   (calcmeta_t, aggmeta_t) generic_calc_t;;
type mapacc_t = (calcmeta_t, aggmeta_t) generic_mapacc_t;;
type stmt_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_stmt_t;;
type trig_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_trig_t;;
type prog_t =   (calcmeta_t, aggmeta_t, stmtmeta_t) generic_prog_t;;

(******************* Basic Type Constructors/Operators *******************)
let mk_sum l r = (Add(l,r), ())
let mk_prod l r = (Mult(l,r), ())
let mk_lt l r = (Lt(l,r), ())
let mk_leq l r = (Leq(l,r), ())
let mk_eq l r = (Eq(l,r), ())
let mk_if l r = (IfThenElse0(l,r), ())
let mk_ma ma = (MapAccess(ma), ())
let mk_c c = (Const(CFloat(c)), ())
let mk_v v = (Var(v), ())

let string_of_calcmeta _ = ""
let string_of_aggmeta  _ = ""
let string_of_stmtmeta _ = ""

(******************* Generic Type Math Operations *******************)
let c_sum (a : const_t) (b : const_t) : const_t = 
  match a with
 (*   CInt(i1) -> (
        match b with
            CInt(i2) -> CInt(i1 + i2)
          | CFloat(f2) -> CFloat((float_of_int i1) +. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
 *)
    | CFloat(f1) -> (
        match b with
 (*         CInt(i2) -> CFloat(f1 +. (float_of_int i2)) *)
          | CFloat(f2) -> CFloat(f1 +. f2)
 (*       | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)
      )
 (* | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)
;; 

let c_prod (a : const_t) (b : const_t) : const_t =
  match a with
 (*   CInt(i1) -> (
        match b with
            CInt(i2) -> CInt(i1 * i2)
          | CFloat(f2) -> CFloat((float_of_int i1) *. f2)
          | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)"
      )
 *)
    | CFloat(f1) -> (
        match b with
 (*         CInt(i2) -> CFloat(f1 *. (float_of_int i2)) *)
          | CFloat(f2) -> CFloat(f1 *. f2)
 (*       | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)
      )
 (* | _ -> failwith "Unhandled Arithmetic Operation (M3.ml)" *)


(******************* M3 Prepared *******************)
module Prepared = struct

  type pextension_t   = var_t list
   
  (* id, theta extension, singleton, cross product *)
  type calcmeta_t    = int * pextension_t * bool * bool
  
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
  
  (* accessors *)
  let get_calc ecalc = fst ecalc
  let get_meta ecalc = snd ecalc
  let get_extensions ecalc = let (_,x,_,_) = get_meta ecalc in x
  let get_id ecalc = let (x,_,_,_) = get_meta ecalc in x
  let get_singleton ecalc = let (_,_,x,_) = get_meta ecalc in x
  let get_product ecalc = let (_,_,_,x) = get_meta ecalc in x
  
  let get_ecalc aggecalc = fst aggecalc
  let get_agg_meta aggecalc = snd aggecalc
  let get_agg_name aggmeta = fst aggmeta
  let get_full_agg aggmeta = snd aggmeta
  
  let get_inv_extensions stmtmeta = stmtmeta
  
  let string_of_calcmeta (id, theta, singleton, product) = 
    "{"^(string_of_int id)^": theta += "^
    (list_to_string (fun x->x) theta)^
    (if singleton then "; singleton" else "")^
    (if product then "; product" else "")^
    "}"
  
  let string_of_aggmeta  (name,fullagg) = 
    "{"^name^(if fullagg then "; fullagg" else "")^"}"
    
  let string_of_stmtmeta (theta) = 
    "{"^(list_to_string (fun x->x) theta)^"}"
  
end

(******************* M3 Data Sources *******************)

type event = pm_t * const_t list
type stream_event = pm_t * rel_id_t * const_t list
type framing_t = 
    FixedSize of int
  | Delimited of string
  | VarSize of int * int
type source_t =
   FileSource of string
 | PipeSource of string
 | SocketSource of Unix.inet_addr * int
type adaptor_t = string * (string * string) list
type relation_input_t = source_t * framing_t * string * adaptor_t
