(** 
   A module for computing initialization code for a given expression.
   
   This is required in two cases:
      - When we acccess (on the RHS) a map with a set of input variables that
        we have never encountered before (i.e., we extend the input domain of a
        map).  Note that we will never update a map (i.e., on the LHS) in a way
        that extends its input domain, because the input variables are, by 
        definition, not bound within the defining expression.
      - When we access or update an expression that does not have input 
        variables, but still does not have finite support.  This can only happen
        when the root level of the expression (or one of the terms of a sum 
        at the root level of the expression) is a lift expression (or is the
        product of several lifts).  In this case, there is a finite domain, 
        but elements of the domain have nonzero initial values.
   
   For now, we ignore the case of input variables, as we never generate 
   maps with input variables in the compiler (for now).  Our approach to 
   performing IVC on output-only expressions is based around a reductive 
   approach: For a specific type of IVC (of which there are several, each 
   corresponding to one of the functions defined below), zero out any relations
   which are guaranteed to be empty (or have an empty relevant slice), and then
   propagate the zero as aggressively as possible.
   
   In general, there are three (and a half) cases:
      - The expression has finite support. (no IVC is required)
      - The expression produces a nonzero value for a fixed number of rows.
        This is the case when the value is nonzero (as defined above), and
        all of its output variables are defined either entirely by inlining or
        in a static table. (IVC can be done entirely at startup)
      - The expression produces a nonzero value for an infinite number of rows.
        This is the case when the value is nonzero, and at least one of its 
        output variables receives its domain (at least in part) from a stream.
        (runtime IVC is required).  In this case, however, we might be able to
        limit the set of delta queries for which IVC is actually required.
      - The expression produces a nonzero value for an infinite number of rows,
        but a finite number of these rows are known at startup.  This can happen
        when the domain of a variable is derived from the union of the domain
        of a table/inline expression and a stream (we do as much IVC at
        startup as possible, and then do the rest at runtime)
   
*)

open Calculus
open Types
open Provenance

(**/**)
let unsupported expr msg = Debug.Logger.error "IVC" 
                     ~detail:(fun () -> CalculusPrinter.string_of_expr expr)
                     ("Unsupported query: "^msg)
let bug expr = Debug.Logger.bug "IVC" 
                     ~detail:(fun () -> CalculusPrinter.string_of_expr expr)

let test_provenance = 
   Provenance.fold (List.fold_left (||) false) (List.fold_left (&&) true) 
                   (fun x->x)
(**/**)

(**
   Compute the IVC of a given expression for use at startup 

   At startup, all stream relations are guaranteed to be empty.  This makes
   for extremely trivial IVC generation.  We just replace all the stream
   relations with zeroes, do some basic propagation, and we're done.
   
   Of course, we need to make sure that these zeroes get propagated down through
   aggsums.
   
   Before we do this, however, we first determine whether or not we actually
   need to perform IVC at startup.  This falls into the second of the three
   cases.  There are two reasons why we wouldn't do IVC here:
      - The expression's has a zero default value to begin with (in which case 
        we never need to do IVC)
      - One or more of the expression's output variables have a domain which
        is defined entirely by a stream (in which case all IVC must be done at
        runtime).
   
   Note that this IVC expression does not need any materialization.  The only 
   relations it contains are static tables (which have already been individually
   materialized), and there's no performance benefit to be gained by doing any
   piecewise materialization on the expression itself -- The best we can do is
   implement any relevant joins efficiently (which is not something that we're
   able to do in Calculus).
*)
let needs_startup_ivc (table_rels:Schema.rel_t list) (expr:expr_t): bool =
   let table_names = List.map Schema.name_of_rel table_rels in
   let (var_provenance,val_provenance) =  Provenance.provenance_of_expr expr in
   let at_startup = (function 
      | RelSource(rn) -> (List.mem rn table_names)
      | InlineSource -> true
      | NoSource -> false
      | AnySource -> true
   ) in
      (test_provenance at_startup val_provenance) &&
      (List.for_all (fun (_,v) -> test_provenance at_startup v) var_provenance)


(**
   Determine whether the expression needs runtime IVC (if so, we can't do 
   anything clever to simplify the expression, we just need to materialize a 
   simplified version).
   
   The expression will not need runtime IVC if one of two things is true:
      - The expression's initial value is derived exclusively from stream
        relations. (i.e., stream + stream, but not stream + table, or lift(...))
        In this case, the expression's value will always be zero at init.
      - All of the expression's output variables receive their domains from 
        static (inline or table) sources.  In this case, the expression will 
        have been entirely initialized at system ready.
   
   Because it's simpler, we test for the contrapositive of each case.  We first
   determine if the value has any inline or table influences, and then test if
   any of the expression's output variables have a domain that is influenced by
   a stream relation.
*)
let needs_runtime_ivc (table_rels:Schema.rel_t list) (expr:expr_t): 
                      bool =
   let table_names = List.map Schema.name_of_rel table_rels in
   let (var_provenance,val_provenance) =  Provenance.provenance_of_expr expr in
   Debug.print "LOG-IVC-TEST" (fun () ->
      "Provenance for: \n"^(CalculusPrinter.string_of_expr expr)^"\n is: \n"^
      "[value] -> "^(Provenance.string_of_provenance val_provenance)^
      (ListExtras.string_of_list ~sep:"" (fun ((vn,_),p) ->
         "\n"^vn^" -> "^(Provenance.string_of_provenance p)
         ) var_provenance)
   );
   let for_inline_or_table_influence = (function 
      | RelSource(rn) -> (List.mem rn table_names)
      | InlineSource -> true
      | NoSource -> false
      | AnySource -> true
   ) in
   let for_stream_influence = (function
      | RelSource(rn) -> not (List.mem rn table_names)
      | InlineSource -> false
      | NoSource -> false
      | AnySource -> true
   ) in
      (test_provenance for_inline_or_table_influence val_provenance)
          &&
      (List.exists (fun (_,v) -> test_provenance for_stream_influence v)
                   var_provenance)
      
let rec naive_needs_runtime_ivc (table_rels:Schema.rel_t list) (expr:expr_t): 
                     bool = 
   let table_names = List.map Schema.name_of_rel table_rels in
   CalcRing.fold 
      (List.fold_left (||) false) 
      (List.fold_left (&&) true) 
      (fun x -> x)
      (function 
         | Rel(rn,rv) -> List.mem rn table_names
         | AggSum(gb_vars, subexp) -> naive_needs_runtime_ivc table_rels subexp
         | _ -> true)
      expr

let derive_initializer ?(scope = [])
                       (table_rels:Schema.rel_t list)
                       (expr:expr_t): expr_t =
   let table_names = List.map (fun (rn,_,_) -> rn) table_rels in
   let optimized_expr = (CalculusTransforms.optimize_expr
                              (Calculus.schema_of_expr expr) expr)
   in
   Debug.print "LOG-IVC-DERIVATION" (fun () ->
      "[Initializer] Input is (scope: "^(ListExtras.ocaml_of_list fst scope)^
      "): "^(CalculusPrinter.string_of_expr optimized_expr)
   );
   let rec rcr curr_expr = 
      Calculus.fold 
         (fun _ -> CalcRing.mk_sum) 
         (fun _ -> CalcRing.mk_prod) 
         (fun _ -> CalcRing.mk_neg)
         (fun _ lf ->
         Debug.print "LOG-IVC-DERIVATION" (fun () ->
            "[Initializer] Deriving Initializer for : "^(string_of_leaf lf)
         );
         match lf with
         | Rel(rn, rv) ->
            if List.mem rn table_names
            then CalcRing.mk_val (Rel(rn,rv))
            else CalcRing.zero
         | AggSum(gb_vars, original_subexp) ->
            let subexp = rcr original_subexp in
            (* If the subexpression is zero, then the aggsum is zero *)
            if subexp = CalcRing.zero then CalcRing.zero else
           
            (* It's also possible that the subexpression will have a narrower
               schema than before without becoming zero.  For example
                  AggSum([A], (B ^= R(A))
               would become
                  AggSum([A], (B ^= 0))
               In this case, there will be no rows in the output schema of this
               expression, and we can replace the nested expression by zero.
               
               Also note that this is not the case if any of those variables are
               bound on the outside (i.e., in the scope)
            *)
            let subexp_ovars = (snd (schema_of_expr subexp)) in
            let bound_schema = ListAsSet.union scope subexp_ovars in
            let unbound_schema = ListAsSet.diff gb_vars bound_schema in
               if unbound_schema = []
               then CalcRing.mk_val (AggSum(ListAsSet.inter gb_vars
                                                            subexp_ovars,
                                            subexp))
               else CalcRing.zero

         | Lift(v, original_subexp) ->
            let (_,original_ovars) = Calculus.schema_of_expr original_subexp in
            let subexp = rcr original_subexp in
            let (_,new_ovars) = Calculus.schema_of_expr subexp in
            Debug.print "LOG-IVC-DERIVATION" (fun () ->
               "IVC For Lift into "^(string_of_var v)^" went from: \n"^
               (CalculusPrinter.string_of_expr original_subexp)^"\nto: \n"^
               (CalculusPrinter.string_of_expr subexp)^"\n ovars:"^
               (ListExtras.ocaml_of_list string_of_var original_ovars)^"->"^
               (ListExtras.ocaml_of_list string_of_var new_ovars)
            );
               if ListAsSet.diff original_ovars new_ovars <> [] 
                  then CalcRing.zero
                  else CalcRing.mk_val (Lift(v, subexp))

         | _ -> CalcRing.mk_val lf
      ) (CalculusTransforms.optimize_expr (Calculus.schema_of_expr curr_expr) 
                                          curr_expr)
   in
   let init_expr = rcr expr in
      (* It's possible that a zero will narrow the schema of an expression
         without zeroing out the entire expression thanks to a lift.  We need
         to detect this here, rather than above, since we don't have schema
         information in rewrite_leaves (And for AggSum, it actually matters that
         the group-by vars be a superset of the schema). *)
      if ListAsSet.diff (snd (Calculus.schema_of_expr expr))
                        (snd (Calculus.schema_of_expr init_expr)) <> []
      then (
         Debug.print "LOG-IVC-DERIVATION" (fun () ->
            "[Initializer] Initializer is zero");
         CalcRing.zero
      ) else (
         Debug.print "LOG-IVC-DERIVATION" (fun () ->
            "[Initializer] Initializer is "^
               (CalculusPrinter.string_of_expr init_expr)
         );
         if (Debug.active "IVC-IVC-DERIVATION") then
            CalculusTransforms.optimize_expr (Calculus.schema_of_expr init_expr)
                                             init_expr
                        else init_expr
      )
 
