(**
   Tools for breaking a Calculus expression up into its component parts.
*)
open Types
open Ring
open Arithmetic
open Calculus

(**/**)
module C = Calculus
(**/**)


(******************************************************************************)
(**
   [decompose_poly scope expr]
   
   Perform polynomial expansion on a given expression, erasing all intermediate 
   AggSums.
   @param expr  A calculus expression
   @return      A list of terms that can be summed together to re-create an
                expression equivalent to [expr], together with the schema of
                each element.  This schema can be used as the group-by variables
                of an AggSum to re-create the schema of the original expression.
*)
let decompose_poly (expr:C.expr_t):(var_t list * C.expr_t) list = 
(* TODO: ensure that different expessions under AggSum have disjunct schemas *)

  let rec erase_aggsums e = 
		C.CalcRing.fold C.CalcRing.mk_sum C.CalcRing.mk_prod C.CalcRing.mk_neg
		  (fun lf -> begin match lf with
        | AggSum(s, subexp) -> erase_aggsums subexp
        | _ -> C.CalcRing.mk_val lf
      end) e
  in
	let rec decompose_term term =
		let schema = snd (C.schema_of_expr term) in 
    List.map (fun x -> (schema, x))
               (C.CalcRing.sum_list 
                  (C.CalcRing.polynomial_expr (erase_aggsums term)))
	in 
    List.fold_left (fun result term ->
	    result @ (decompose_term term)
		) [] (C.CalcRing.sum_list (C.CalcRing.polynomial_expr expr))
    
(*    
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
*)
		
(******************************************************************************)
(**
   [decompose_graph scope (schema,expr)]
   
   Do graph factorization on a monomial term.  Graph factorization splits 
   a product expression into multiple chunks, where each chunk is disconnected
   from the others.  That is, each produced chunk shares no variables (other 
   than those present in the scope) with any other chunk.
   @param scope  The scope in which [expr] is evaluated
   @param schema The expected output schema of [expr]
   @param expr   A {b monomial} (i.e., Sum-free) Calculus expression
   @return       The original schema, and a list of disconnected components
                 together with their corresponding individual schemas
*)
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
   
