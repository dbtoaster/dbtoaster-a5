open Types
open Calculus
;;

(** Compute the domain maintenance expression of the provided calculus 
    expression. *)
let rec maintain (formula: Calculus.expr_t) : Calculus.expr_t =
   Calculus.fold (fun _ -> CalcRing.mk_sum) (fun _ -> CalcRing.mk_prod)
                 (fun _ -> CalcRing.mk_neg)
      (fun _ lf -> match lf with
         | Value(_) -> CalcRing.one
         | External _ -> 
            failwith "Domain Maintenance on a materialized expression"
         | AggSum(gb_vars,subexp) -> 
            CalcRing.mk_val (AggSum(gb_vars,maintain subexp))
         | Rel _
         | Cmp _
         | Lift _ -> CalcRing.mk_val lf
      )
      formula
(*
    begin match formula with
    | CalcRing.Sum(q1::qo) -> 
        let (context1) = maintain(context)(q1) in
            let (context2) = maintain(context)(if List.length qo = 1 then List.hd qo else CalcRing.mk_sum qo) in 
                (CalcRing.mk_sum([context1; context2]))
    | CalcRing.Prod(q1::qo) -> 
        let (context1) = maintain(context)(q1) in
            let (context2) = maintain(context1)(if List.length qo = 1 then List.hd qo else CalcRing.mk_prod qo) in 
                (context2)
    | CalcRing.Neg(q1) ->
        maintain(context)(q1)
    | CalcRing.Val(leaf) ->
        begin match leaf with
          | Value(v) -> (context)
          | External(ename,eins,eouts,etype,emeta) ->
            failwith "Domain Maintenance on a materialized expression"
          | AggSum(gb_vars, subexp) -> 
            let (context1) = maintain (CalcRing.one) (subexp) in
                let right_context = CalcRing.Val(AggSum(gb_vars, context1)) in
                    (CalcRing.mk_prod ([context; right_context]))
          | Rel(rname, rvars)    -> 
            (CalcRing.mk_prod ([context; formula]))
          | Cmp(op,subexp1,subexp2) -> 
              let right_context = formula in
              (CalcRing.mk_prod ([context; formula]))
            else
              (context)
          | Lift(target, subexp)    -> 
            (CalcRing.mk_prod ([context; formula]))
        end
    | _ -> failwith ("Incorrect formula")
    end
*)

let mk_dom_var =
   FreshVariable.declare_class "calculus/SqlToCalculus" "domain"

(**
   Lifts in Calculus do not have finite support.  Even if the expression nested
   within the lift has finite support, the lift itself is supported as long as
   the lifted variable has the value obtained by evaluating the nested 
   expression.
   
   This function provides an alternative option: a domain-restricted lift
   operation.  The idea of a domain-restricted lift is that the domain of the
   entire expression (i.e., the set of values on which it has support) is equal
   to the domain of the nested expression. [mk_domain_restricted_lift] produces
   an expression that has these semantics.  
*)
let mk_domain_restricted_lift (lift_v:var_t) (lift_expr:expr_t): expr_t =
   let lift = CalcRing.mk_val (Lift(lift_v, lift_expr)) in

   Debug.print "LOG-MK-DR-LIFT" (fun () ->
      "Generating domain restricted lift of : ("^
      (string_of_var lift_v)^" ^= \n"^
      (CalculusPrinter.string_of_expr lift_expr)^"\n)"
   );
   (* If the lifted expression has no schema, the resultant lift is already
      domain-restricted. *)
   let (_,ovars) = Calculus.schema_of_expr lift_expr in
   if ovars = [] then (
      Debug.print "LOG-MK-DR-LIFT" (fun () ->
         "Lift is already domain-restricted"
      );
      lift 
   ) else

   (* Otherwise we need to actually do some domain tracking. *)
   let dom_expr = (maintain lift_expr) in

   (* If the domain definition expression is identical to the original 
      expression (which is possible), then we don't need to lift twice.
      
      Note: Calculus.cmp_exprs returns a Some(mappings), where mappings is
      a set of variable renamings that can be applied to go from one expression 
      to the other.  We want to test for equivalence, but the presence of 
      mappings in the return means that the two expressions are not, in fact, 
      equivalent. *)
   Debug.print "LOG-MK-DR-LIFT" (fun () ->
      "Domain definition expression is : \n"^(
         (CalculusPrinter.string_of_expr dom_expr)
      )^"\n"^(
      match Calculus.cmp_exprs dom_expr lift_expr with
         | None     -> "Lift needs an explicit domain maintenance expression"
         | Some(m) when Function.is_identity m ->
                       "Lift is its own domain maintenance expression"
         | Some(m)  -> "Domain maintenance expression is mappable: "^(
            Function.string_of_table_fn m string_of_var string_of_var
         )
      )
   );
   match Calculus.cmp_exprs dom_expr lift_expr with 
      | Some(m) when Function.is_identity m ->
         CalcRing.mk_prod [
            lift; CalcRing.mk_val (Cmp(Neq, Arithmetic.mk_int 0, 
                                            Arithmetic.mk_var lift_v))]
      | _ ->
   (* If all else fails, we need to create a new variable for the lifted 
      expression, test, and then project it all away. *)
   let dom_var = ((mk_dom_var ()^"_"^(fst lift_v)), TInt) in
      CalcRing.mk_val (AggSum(lift_v::ovars, 
         CalcRing.mk_prod [
            CalcRing.mk_val (Lift(dom_var, dom_expr));
            CalcRing.mk_val (Cmp(Neq, Arithmetic.mk_int 0,
                                      Arithmetic.mk_var dom_var));
            lift
         ]
      ))