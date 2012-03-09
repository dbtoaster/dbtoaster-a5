(**
   Breaking a Calculus expression up into its component parts.
*)
open Types
open Ring
open Arithmetic
open Calculus

module C = Calculus


(******************************************************************************)

let decompose_poly (expr:C.expr_t):(var_t list * C.expr_t) list = 
   let schema = snd (C.schema_of_expr expr) in
   let rec erase_aggsums e = 
      C.CalcRing.fold C.CalcRing.mk_sum C.CalcRing.mk_prod C.CalcRing.mk_neg
         (fun lf -> begin match lf with
            | AggSum(s, subexp) -> erase_aggsums subexp
            | _ -> C.CalcRing.mk_val lf
         end) e
   in
      List.map (fun x -> (schema, x))
               (C.CalcRing.sum_list 
                  (C.CalcRing.polynomial_expr (erase_aggsums expr)))

(******************************************************************************)

let decompose_graph (scope:var_t list) ((schema,expr):(var_t list * C.expr_t)):
                    var_t list * (var_t list * C.expr_t) list =
   let get_vars term = 
      let i,o = C.schema_of_expr term 
      in ListAsSet.diff (ListAsSet.union i o) scope
   in
      (  schema, 
         List.map (fun term_list ->
            let term = CalcRing.mk_prod term_list in
            let term_schema = snd (C.schema_of_expr term) in
               ( ListAsSet.inter schema term_schema, term )
         ) (HyperGraph.connected_components get_vars (CalcRing.prod_list expr))
      )
   
