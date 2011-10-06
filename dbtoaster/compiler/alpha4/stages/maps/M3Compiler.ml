(* M3Compiler.
 * A compilation framework for producing machine code for an M3 program.
 * This framework involves two parts:
 * -- preprocessing of an M3 program, computing evaluation contexts (scopes) for
 *    each M3 statement, and analyzing for optimizations common to the currently
 *    handled code generators.
 *    These are singleton, and cross product optimizations, which respectively
 *    indicate whether (sub-)expression evaluation yields a single value or an
 *    out tier slice, and whether an operator expression needs to propagate
 *    variables from its lhs operand to its rhs operand.
 * -- code generation, via a functorized module taking a language-specific
 *    code generator with interface given by M3Codegen, to produce source output
 *    in the given target language.
*)

open M3
open M3Common
open M3Common.Patterns

module M3P = M3.Prepared

(* Trigger variable normalization.
 * Currently the compiler adds a prefix to trigger variables of the LHS map
 * name of each statement being compiled, resulting in different trigger
 * variable names for the statements belonging in a single trigger. We normalize
 * these trigger variable names in the output of M3 preparation. *)

(* returns all map names in an M3 trigger *)
let get_map_names (trigger : M3.trig_t) : map_id_t list =
   let get_names_from_stmt ((mapn,_,_,_),(c,_),_) =
      (recurse_calc_lf Util.ListAsSet.union
         (fun _ (mapn, _, _, _) -> [mapn]) (fun _ -> []) (fun _ -> []) c)@[mapn]
   in let (_,_,_,stmts) = trigger
   in Util.ListAsSet.no_duplicates
      (List.flatten (List.map get_names_from_stmt stmts))

(* returns a prefix and list of suffixes for a list of trigger args
 * -- an empty prefix result indicates the trigger args are not in the
 *    expected format (e.g. for hand-coded M3 programs, as in our examples) *)
let get_trigger_arg_normalization (trigger : M3.trig_t) =
   let (_,rel,args,_) = trigger in
   let get_prefix arg =
      let rel_start =
         try Str.search_forward (Str.regexp (Str.quote rel)) arg 0
         with Not_found -> failwith "Invalid trigger arg format"
      in String.sub arg 0 rel_start
   in
   match args with
    | [] -> failwith "Invalid trigger, has no arguments"
    | h::t ->
       try 
          let prefix = get_prefix h in
          let prefix_len = String.length prefix in
          let suffix s = String.sub s prefix_len ((String.length s) - prefix_len)
          in (prefix, List.map suffix args)
       with Failure _ -> ("", args)

(* returns a mapping of: var -> trigger arg
 * -- given a set of maps, and trigger arg suffixes, and prefix, builds a
 *    mapping of <map name>^<suffix> -> <prefix>^<suffix> for each map,
 *    suffix combination.
 *)
let get_arg_substitutions prefix suffixes map_names =
    List.fold_left (fun acc suffix ->
       let dest = prefix^suffix in acc@(List.fold_left
          (fun acc mapn ->
             (*print_endline ("Adding subst: "^(mapn^suffix)^"->"^dest);*)
             (mapn^suffix, dest)::acc) [] map_names))
       [] suffixes

(* for each trigger:
 * -- get arg substitutions
 * -- substitute any vars, by check if var has a binding in the mapping
 *) 
let normalize_triggers (triggers : M3.trig_t list) =
   let normalize trig =
      let (prefix, suffixes) = get_trigger_arg_normalization trig in
      if prefix = "" then trig else
      begin
      (*print_endline ("Normalizing with prefix "^prefix);*)
      let map_names = get_map_names trig in
      (*print_endline ("Map names: "^(String.concat ";" map_names));*)
      let arg_mappings = get_arg_substitutions prefix suffixes map_names in
      let (pm,rel,args,stmts) = trig in
      let substitute c =
         let aux v = if List.mem_assoc v arg_mappings 
                     then List.assoc v arg_mappings else v
         in replace_calc_lf
         (fun inline_agg (n,inv,outv,init) -> MapAccess((n,(List.map aux inv),(List.map aux outv),init), inline_agg ))
         (fun c -> Const(c))
         (fun v -> Var(aux v)) c
      in (pm,rel,args,
          List.map (fun (m,(c,am),sm) -> (m,(substitute c,am),sm)) stmts)
      end
   in List.map normalize triggers


(* M3 preparation.
 * Decorates M3 nodes with the following metadata:
 * 1. Unique identifier, as an integer based on tree traversal. We use a
 * stack-based approach to handle initial value computations, where each
 * initial value expr in a map lookup pushes a new counter onto the stack,
 * and removes when processed. 
 *
 * 2. Extensions:
 * Extensions are deltas to the environment/scope, indicating new variable
 * bindings at various points in the control flow. We summarize these
 * extensions here:
 * -- stmt extensions:
 *   ++ lhs in vars, from accessing an in tier, before evaluating any rhs expr
 *   ++ init slice loop vars, to restrict the portion of the init slice computed
 *      to that of the delta slice
 * -- stmt rhs init and incr extensions: bound LHS out vars, to be added back
 *    to the slice prior to merging/updating the DB
 * -- map lookup init expr: bound out vars, to be added back to the slice
 *    resulting from IVC, just as with the stmt rhs incr extensions
 * -- op slice expr:
 *   ++ lhs operand extensions: propagations from lhs operand to rhs operand
 *   ++ rhs operand extensions: op output schema extensions by the rhs operand 
 * -- op lslice expr: same as above
 * -- op rslice expr: op schema extensions by the rhs operand
 *
 * Except for init value computations at map lookups, extensions are computed
 * for a node by its parent, and computed by prepare_stmt for both entire
 * incr and init RHS expressions. Thus each AST node simply returns the empty
 * list as its extension, leaving this to be filled in by the parent following
 * recursive preparation.
 *
 * See documentation for prepare_operand, prepare_op for more info on
 * extensions+propagation.
 *
 * 3. Singleton, i.e. whether the M3 node is statically known to produce a
 * single value. This applies for subexpressions that use only constants and map
 * lookups that are singletons. A map lookup is a singleton if there are no loop
 * variables in either its in or out variable sets, that is all of its in vars
 * and out vars are bound.
 *
 * 4. Product, i.e. whether we can simplify an operator to take a cross product
 * of its operands, rather than propagating variables. This applies if all
 * operands use disjoint sets of variables.
 *
 * 5. Short-circuiting, i.e. whether we can avoid evaluating the "then" clause
 * in if statements, by testing the conditional first.
 * TODO: to actually implement short-circuiting, we need an operator evaluation
 * method that filters the condition slice to only those k-vs passing the test
 * prior to evaluating the "then" clause. Right now, we evaluate both the
 * test and then clauses.
 *)

let prepare_triggers (triggers : trig_t list)
      : (M3P.trig_t list * pattern_map) =
   let prep_counter = ref [] in
   let add_counter() = prep_counter := 0::(!prep_counter) in
   let remove_counter() =
      if !prep_counter != [] then prep_counter := (List.tl !prep_counter)
   in
   let prepare() =
      if !prep_counter = [] then add_counter();
      let current = List.hd (!prep_counter) in
         prep_counter := (current+1)::(List.tl !prep_counter);
         current
   in
   let save_counter f = add_counter(); let r = f() in remove_counter(); r in
   
   let rec prepare_calc (update_mapn : string)
                        (stmt_lhs_vars: var_t list)
                        (theta_vars : var_t list)
                        (calc : calc_t) : (M3P.calc_t * pattern_map) =
      let recur = prepare_calc update_mapn in
      
      (* Helper function for prepare_op_w_lhs.
       * Prepares an operand by computing its propagations or out schema
       * extension based on whether it is the rightmost operand.
       * Propagations are computed locally, and override any extensions computed
       * recursively (which should be empty).
       * See below for further discussion
       *)
      let prepare_operand stmt_lhs_vars c last_operand in_props
            : (M3P.calc_t * var_t list * pattern_map) =
        let outv = calc_schema c in
        let out_props =
          if last_operand then
            Util.ListAsSet.diff in_props (Util.ListAsSet.union outv theta_vars)
          else (Util.ListAsSet.diff outv in_props) in
        let (pc,pm) = recur stmt_lhs_vars in_props c in
        let new_pc_meta =
          let (w,_,x,y,z) = M3P.get_meta pc in (w,out_props,x,y,z)
        in ((M3P.get_calc pc, new_pc_meta), outv, pm)
      in        

      (* Prepares an operator, computing propagations from operand to operand.
       * Also determines whether the operator is a singleton or suitable for
       * evaluating via a cross-product.
       * 
       * Each operand, except the rightmost child, computes propagations
       * (aka extensions) that can be used by any rightwards sibling.
       * Propagations are loop out vars that have not already been propagated
       * (i.e. bound coming into the operand).
       * The initial bound vars are theta_vars.
       * The rightmost child computes an output schema extension since there are
       * no more propagations to be done. An output schema extension are vars
       * that have been propagated to the rightmost child, that are to appear
       * in the operator's output schema, and are not already part of the
       * rightmost operand's output schemas (thus they must be prepended into
       * the result of the rightmost operand)
       * 
       * We postprocess propagations to make them strict, to avoid additional
       * binding overhead in the interpreter. Strictness eliminates variables
       * from extensions if they are not used in the rhs. However this does
       * not mean the variables are not propagated, indeed they must be
       * propagated since they can still appear in the output schema extension.
       * This works because extensions are only used to add bindings to the
       * environment -- they are not used to create a thinner slice, thus 
       * since we do not actually change the slice, the propagations may be
       * bound in the environment at any rightwards operand since they still
       * exist in the keys of temporary slices.
       *
       * The body is currently implemented for a binary operator, but can easily
       * be extended to an n-way operator as documented below.
       *)
      let prepare_op_w_lhs (c2_stmt_lhs_vars : var_t list)
                           (build_op_f : M3P.calc_t -> M3P.calc_t -> M3P.pcalc_t)
                           (c1: calc_t) (c2: calc_t)
            : (M3P.calc_t * pattern_map)
      =
         (* Generic n-way propgation implementation *)
         let _, operand_eps = List.fold_left
           (fun (acc_propagations, acc_ep) (c, c_lhs_vars, last) ->
             let (e, outv, pats) = 
               prepare_operand c_lhs_vars c last acc_propagations
             in (acc_propagations@(M3P.get_extensions e),
                 acc_ep@[e,outv,pats,last])) 
           (theta_vars, [])
           (* For n-way implementation, add triples for all operands *)
           [c1,stmt_lhs_vars,false; c2,c2_stmt_lhs_vars,true]
         in
         (* This is a minor optimization to reduce the # of vars that get bound
          * for the rhs. It does not eliminate vars being produced as the
          * output schema for the op, by the rhs (that requires preaggregation,
          * i.e. sums explicitly carrying around their out vars and pushing this
          * down through the expression). We assume preaggregation has already
          * been applied, if at all, here. 
          *)
         let _, strict_eps = List.fold_left
           (fun (rhs_operands,acc) (e,ov,p,last) ->
             if last then [], acc@[e,p] else
             let (nc, (id,ext,sing,prod,shortc)) = e in
             let strict_ext = Util.ListAsSet.inter ext
               (List.fold_left Util.ListAsSet.union []
                 (List.map calc_vars rhs_operands)) in 
             let new_e = (nc, (id,strict_ext,sing,prod,shortc))
             in (if rhs_operands = [] then [] else List.tl rhs_operands),
                acc@[new_e,p])
           (* For n-way implementation, add all operands except first below *)
           ([c2],[]) operand_eps
         in
         begin match strict_eps with
         | [e1,p1_pats;e2,p2_pats] ->
           let patterns = merge_pattern_maps p1_pats p2_pats in
           let sing = (M3P.get_singleton e1) && (M3P.get_singleton e2) in
           let prod = (M3P.get_extensions e1) = [] in
           let meta = (prepare(), [], sing, prod, false)
           in ((build_op_f e1 e2, meta), patterns)
         | _ -> failwith "invalid binary operator preparation"  
         end
      in 

      let prepare_op = prepare_op_w_lhs stmt_lhs_vars in

      (* prepare_calc body *)
      begin match (fst calc) with
        | Const _ | Var _ as c->
           let meta = (prepare(), [], true, false, false)
           in ((c, meta), empty_pattern_map())

        | Add  (c1,c2)  -> prepare_op (fun e1 e2 -> Add  (e1, e2)) c1 c2
        | Mult (c1,c2)  -> prepare_op (fun e1 e2 -> Mult (e1, e2)) c1 c2
        | Leq  (c1,c2)  -> prepare_op (fun e1 e2 -> Leq  (e1, e2)) c1 c2
        | Eq   (c1,c2)  -> prepare_op (fun e1 e2 -> Eq   (e1, e2)) c1 c2
        | Lt   (c1,c2)  -> prepare_op (fun e1 e2 -> Lt   (e1, e2)) c1 c2
        
        | IfThenElse0 (c1,c2) ->
           (* if-expressions must explicitly override short-circuiting flag *)
           let c1_inv = Util.ListAsSet.diff (calc_vars c1) (calc_schema c1) in
           let c2_outv = calc_schema c2 in
           
           (* up-and-left propagations are out vars on the rhs m3 expr, and in
            * vars on the lhs m3 expr, except bound vars. This also suffices as
            * a superset of bigsum vars, bigsum vars additionally do not appear
            * as lhs vars of the stmt.
            * Note propagations can also appear as out vars of both the lhs and
            * rhs m3 exprs. In this case we don't need to propagate
            * right-to-left and can short-circuit with standard out var
            * restrictions.
            *)
           let upl_prop = Util.ListAsSet.diff
             (Util.ListAsSet.inter c1_inv c2_outv) theta_vars in

           let short_circuit = (upl_prop = []) in
           let (op_f,l,r) = if short_circuit
             then ((fun e1 e2 -> IfThenElse0(e1,e2)), c1, c2)
             else ((fun e1 e2 -> IfThenElse0(e2,e1)), c2, c1) in
           let ((nc,(id,ext,sing,prod,_)),pat) =
             prepare_op_w_lhs
               (Util.ListAsSet.union stmt_lhs_vars upl_prop)
               op_f l r in
           let new_if_meta = (id,ext,sing,prod,short_circuit)
           in ((nc, new_if_meta), pat)
        
        | MapAccess ((mapn, inv, outv, init_calc),inline_agg) ->
           (* Determine slice or singleton.
            * singleton: fully bound out vars, note in vars must always be fully
            * bound here, since statement evaluation loops over the in tier *)
           let bound_inv = Util.ListAsSet.inter inv theta_vars in
           let bound_outv = Util.ListAsSet.inter outv theta_vars in
           let singleton = 
              (List.length bound_inv) = (List.length inv) &&
              (List.length bound_outv) = (List.length outv) in 
           
           (* For recursive preparation of initial values:
            * 1. Both in and out vars of this map access are added to the
            *    initializers lhs_vars to ensure they are not detected as
            *    bigsum vars in the IVC expression.
            * 2. Only the in vars of this map access are added to the bound
            *    vars of the initializer. Out vars may contain loop out vars,
            *    which come from out vars of base maps in the IVC expression.
            *)
           let init_lhs_vars = 
             List.fold_left Util.ListAsSet.union theta_vars [inv; outv] in
           let init_theta_vars = Util.ListAsSet.union theta_vars inv in
           
           let (init_ecalc, patterns) = save_counter (fun () -> 
             prepare_calc mapn init_lhs_vars init_theta_vars (fst init_calc)) in
           let new_patterns = 
             if singleton then patterns
             else merge_pattern_maps patterns (singleton_pattern_map
                    (mapn, make_out_pattern outv bound_outv)) in

           (* IVC expression extensions are bound out vars, which are used to
            * add the bound out vars back to the slice resulting from IVC *)
           let new_init_meta = let (w,_,x,y,z) =
              M3P.get_meta init_ecalc in (w,bound_outv,x,y,z) in
           let new_init_agg_meta = (update_mapn^"_init_"^mapn, singleton) in
           let new_init_aggecalc =
              (((M3P.get_calc init_ecalc), new_init_meta), new_init_agg_meta)
           in
           let new_incr_meta = (prepare(), [], singleton, false, false) in
           let r = ((MapAccess((mapn, inv, outv, new_init_aggecalc),inline_agg)), new_incr_meta)
           in (r, new_patterns)
      end
   in

   let prepare_stmt theta_vars (stmt : stmt_t) : (M3P.stmt_t * pattern_map) =

      let ((lmapn, linv, loutv, init_calc), (incr_calc,_),_) = stmt in
      let partition v1 v2 =
         (Util.ListAsSet.inter v1 v2, Util.ListAsSet.diff v1 v2) in
      let (bound_inv, loop_inv) = partition linv theta_vars in
      let (bound_outv, loop_outv) = partition loutv theta_vars in
      
      (* For compile_pstmt_loop, extend with loop in vars *)
      let theta_w_loopinv = Util.ListAsSet.union theta_vars loop_inv in
      let theta_w_lhs = Util.ListAsSet.union theta_w_loopinv loop_outv in
      let full_agg = (List.length loop_outv) = 0 in 

      (* Set up top-level extensions for an entire incr/init RHS.
       * Both init and incr expressions have bound LHS out vars as extensions,
       * to ensure these are added back to the incr and init slices prior
       * to updating the DB.
       *)
      let prepare_stmt_ext name_suffix calc calc_ext =
        let calc_theta_vars = Util.ListAsSet.union theta_w_loopinv calc_ext in
        let (c, patterns) =
          prepare_calc lmapn theta_w_lhs calc_theta_vars calc in
        let new_c_aggmeta = (lmapn^name_suffix, full_agg) in
        let new_c_meta =
          let (w,old_ext,x,y,z) = M3P.get_meta c in 
          if not(M3P.get_singleton c || full_agg)
          then (w,bound_outv,x,y,z) else (w,old_ext,x,y,z)
        in
        let new_c = ((M3P.get_calc c, new_c_meta), new_c_aggmeta)
        in (new_c, patterns) 
      in

      (* See below for why statement initializers have LHS loop out vars as
       * extensions. *)
      let (init_ca, init_patterns) =
        prepare_stmt_ext "_init" (fst init_calc) loop_outv in
      let (incr_ca, incr_patterns) = prepare_stmt_ext "_incr" incr_calc []  in

      (* Statement initializer environment is extended by LHS loop out vars, so
       * that any slice initializers are restricted to only produce the part
       * of the slice corresponding to the delta, rather than the entire slice.
       *)
      let pstmtmeta = loop_inv, loop_outv in 
      let pstmt = ((lmapn, linv, loutv, init_ca), incr_ca, pstmtmeta) in

      let stmt_patterns =
        List.fold_left
          (fun acc (pattern_f, bound_part, vl) -> 
            if List.length bound_part = List.length vl then acc
            else add_pattern acc (lmapn, pattern_f vl bound_part))
          (empty_pattern_map())
          [make_in_pattern, bound_inv, linv; make_out_pattern, bound_outv, loutv]
      in
      let patterns = List.fold_left merge_pattern_maps (empty_pattern_map())
        ([init_patterns; incr_patterns]@[stmt_patterns])
      in (pstmt, patterns)
   in

   let prepare_block (trig_args : var_t list) (bl : stmt_t list)
         : (M3P.stmt_t list * pattern_map) =
      let bl_pat_l = List.map (prepare_stmt trig_args) bl in
      let (pbl, patterns_l) = List.split bl_pat_l in
      let patterns = List.fold_left
         merge_pattern_maps (empty_pattern_map()) patterns_l
      in (pbl, patterns)
   in

   let prepare_trig (t : trig_t) : (M3P.trig_t * pattern_map) =
      let (ev,rel,args,block) = t in
      let (pblock, patterns) = prepare_block args block in
         ((ev, rel, args, pblock), patterns)
   in
   let blocks_maps_l =
     (* Disable trigger normalization now that this is done in CalcToM3 *)
     let trigs_to_prep = (*(normalize_triggers triggers)*) triggers
     in List.map prepare_trig trigs_to_prep
   in
   let (pblocks, pms) = List.split blocks_maps_l in
      (pblocks, List.fold_left merge_pattern_maps (empty_pattern_map()) pms)



(* M3 compiler, functorized by a code generator *)
module Make = functor(CG : M3Codegen.CG) ->
struct

open CG

let rec compile_pcalc patterns (incr_ecalc) : code_t = 
   let compile_op ecalc op e1 e2 : code_t =
      let aux ecalc = (calc_schema ecalc, M3P.get_extensions ecalc,
         compile_pcalc patterns ecalc, M3P.get_singleton ecalc) in
      let (outv1, theta_ext, ce1, ce1_sing) = aux e1 in
      let (outv2, schema_ext, ce2, ce2_sing) = aux e2 in
      let schema = calc_schema ecalc in
         begin match (M3P.get_singleton ecalc, ce1_sing, ce2_sing) with
          | (true, false, _) | (true, _, false) | (false, true, true) ->
             failwith "invalid parent singleton"

          | (true, _, _) -> op_singleton_expr op ce1 ce2
          
          | (_, true, false) ->
             op_rslice_expr op outv2 schema schema_ext ce1 ce2
          
          | (_, false, true) ->
             if M3P.get_product ecalc then
                op_lslice_product_expr op outv1 outv2 ce1 ce2
             else op_lslice_expr
                op outv1 outv2 schema theta_ext schema_ext ce1 ce2 

          | (_, false, false) ->
             if M3P.get_product ecalc then
                op_slice_product_expr op outv1 outv2 ce1 ce2
             else op_slice_expr 
                op outv1 outv2 schema theta_ext schema_ext ce1 ce2
         end
   in
   let ccalc : code_t =
      match (M3P.get_calc incr_ecalc) with
        | MapAccess((mapn, inv, outv, init_aggecalc),inline_agg) ->
           if inline_agg then
            failwith "M3 interpreter does not support depth-restricted compiles"
           else
            let cinit = compile_pcalc2
                patterns (M3P.get_agg_meta init_aggecalc) outv
                (M3P.get_ecalc init_aggecalc) in
            (* We build secondary indexes for out tiers as part of initial value
             * computation, and need to pass along patterns for this. *)
            let out_patterns = get_out_patterns patterns mapn in
            let singleton_init_code = 
               (M3P.get_singleton (M3P.get_ecalc init_aggecalc)) ||
               (M3P.get_full_agg (M3P.get_agg_meta init_aggecalc)) in
            let singleton_lookup_code = M3P.get_singleton incr_ecalc in
            let init_val_code =
               if singleton_init_code
               then singleton_init_lookup mapn inv out_patterns outv cinit
               else slice_init_lookup mapn inv out_patterns outv cinit
            in
            (* code that does the final stage of the lookup on the slice *)
            (* Note: we extend theta for this map access' init value comp
             * with bound out vars, thus we can use the extension to find
             * bound vars for the partial key here *)
            let patv = M3P.get_extensions (M3P.get_ecalc init_aggecalc) in
            let pat = List.map (index outv) patv in
            begin match (singleton_init_code, singleton_lookup_code) with
             | (true, true) -> singleton_lookup_and_init mapn inv outv init_val_code
             | (true, false) -> slice_lookup_sing_init mapn inv outv pat patv init_val_code
             | (false, true) -> singleton_lookup mapn inv outv init_val_code
             | (false, false) -> slice_lookup mapn inv outv pat patv init_val_code
            end

      | Add (c1, c2) -> compile_op incr_ecalc CG.add_op  c1 c2
      | Mult(c1, c2) -> compile_op incr_ecalc CG.mult_op c1 c2
      | Lt  (c1, c2) -> compile_op incr_ecalc CG.lt_op   c1 c2
      | Leq (c1, c2) -> compile_op incr_ecalc CG.leq_op  c1 c2
      | Eq  (c1, c2) -> compile_op incr_ecalc CG.eq_op   c1 c2

      | IfThenElse0(c1, c2) ->
         let f = compile_op incr_ecalc in
         if M3P.get_short_circuit incr_ecalc then f CG.ifthenelse0_op c1 c2
         else f CG.ifthenelse0_bigsum_op c2 c1

      | Const(i)   ->
         if M3P.get_singleton incr_ecalc then (const i)
         else failwith "invalid constant singleton"

      | Var(x)     ->
         if M3P.get_singleton incr_ecalc then var x
         else failwith "invalid var singleton"
      
      in
      let cdebug = debug_expr incr_ecalc in
      let cresdebug = debug_expr_result incr_ecalc ccalc
      in debug_sequence cdebug cresdebug ccalc


(* Optimizations:
 * -- aggregates blindy over entire slice for fully bound lhs_outv 
 * -- skips aggregating when rhs_outv = lhs_outv *)
and compile_pcalc2 patterns agg_meta lhs_outv ecalc : code_t =
   let rhs_outv = calc_schema ecalc in
   (* project slice w/ rhs schema to lhs schema, extending by out vars that
    * are bound. *)
   let rhs_ext = M3P.get_extensions ecalc in
   let rhs_projection = Util.ListAsSet.inter rhs_outv lhs_outv in
   let rhs_pattern = List.map (index rhs_outv) rhs_projection in
   let ccalc = compile_pcalc patterns ecalc in
      if M3P.get_singleton ecalc then
         let cdebug = debug_singleton_rhs_expr lhs_outv in
         singleton_expr ccalc cdebug
      else
         let cdebug = debug_slice_rhs_expr rhs_outv in
         if rhs_outv = lhs_outv then direct_slice_expr ccalc cdebug
         else if M3P.get_full_agg agg_meta then full_agg_slice_expr ccalc cdebug
         else slice_expr rhs_pattern rhs_projection lhs_outv rhs_ext ccalc cdebug


(* Optimizations: handles
 * -- current_slice singleton
 * -- delta_slice singleton, only if all map accesses are singletons, i.e.
 *    no bigsum vars, all out vars of map accesses are bound, and no in vars
 *    in any maps (otherwise there may be initial values). *)
let compile_pstmt patterns
   ((mapn, inv, outv, init_aggecalc), incr_aggecalc, _) init_ext : code_t =
   let aux aggecalc : code_t = compile_pcalc2
     patterns (M3P.get_agg_meta aggecalc) outv (M3P.get_ecalc aggecalc) in
   let (cincr, cinit) = (aux incr_aggecalc, aux init_aggecalc) in
   let init_value_code =
      let cinit_debug = debug_rhs_init() in
      if (M3P.get_singleton (M3P.get_ecalc init_aggecalc)) ||
         (M3P.get_full_agg (M3P.get_agg_meta init_aggecalc))
      then singleton_init cinit cinit_debug
      else slice_init inv outv init_ext cinit cinit_debug
   in
   let cdebug = debug_stmt mapn inv outv in
      if (M3P.get_singleton (M3P.get_ecalc incr_aggecalc)) ||
         (M3P.get_full_agg (M3P.get_agg_meta incr_aggecalc))
      then singleton_update outv cincr init_value_code cdebug
      else slice_update mapn inv outv cincr init_value_code cdebug


let compile_pstmt_loop patterns trig_args pstmt : code_t =
   let ((mapn, inv, outv, _), incr_aggecalc, stmt_meta) = pstmt in
   let patv = Util.ListAsSet.inter inv trig_args in
   let pat = List.map (index inv) patv in
   let direct = (List.length patv) = (List.length inv) in 
   let map_out_patterns = get_out_patterns patterns mapn in
   let inv_ext = M3P.get_inv_extensions stmt_meta in
   let init_ext = M3P.get_init_slice_extensions stmt_meta in
   (* Output pattern for partitioning *)
   (*
   let out_patv = Util.ListAsSet.inter lhs_outv trig_args in
   let out_pat = List.map (index lhs_outv) out_patv  in
   *)
   let cstmt = compile_pstmt patterns pstmt init_ext in
   let singleton_update =
      (M3P.get_singleton (M3P.get_ecalc incr_aggecalc)) ||
      (M3P.get_full_agg (M3P.get_agg_meta incr_aggecalc)) in
   if singleton_update then
      singleton_statement mapn inv outv
         map_out_patterns inv_ext patv pat direct cstmt
   else
      statement mapn inv outv inv_ext patv pat direct cstmt

let compile_ptrig (ptrig, patterns) =
   let aux ptrig =
      let (event, rel, trig_args, pblock) = ptrig in
      let aux2 = compile_pstmt_loop patterns trig_args in
      let cblock = List.map aux2 pblock in
      trigger event rel trig_args cblock
   in List.map aux ptrig

let compile_query (dbschema:(string * Calculus.var_t list) list)
                  (((schema,m3prog):M3.prog_t), 
                    (sources:M3.relation_input_t list)) 
                    (toplevel_queries:string list)
                    (out_file_name:Util.GenericIO.out_t) =
   let trig_rels = Util.ListAsSet.no_duplicates
     (List.map (fun (pm,rel,_,_) -> rel) m3prog) in
   let prepared_prog = prepare_triggers m3prog in
   let patterns = snd prepared_prog in
   let ctrigs = compile_ptrig prepared_prog in
   (* Group inputs by source and framing, filtering to only those relations
    * for which we have triggers. This allows sources to provide
    * content for multiple relations, with an adaptor per source x relation.
    * Note: there is no duplicate elimination here, for example if a
    * relation/adaptor pair is specified multiple times per source. It is
    * left to the parser to raise errors on multiply specified relations in
    * query inputs. *)
   let sources_and_adaptors =
      List.fold_left (fun acc (s,f,rel,a) ->
         match (List.mem rel trig_rels, List.mem_assoc (s,f) acc) with
           | (false,_) -> acc
           | (_,false) -> ((s,f),[rel,a])::acc
           | (_, true) ->
           	let existing = List.assoc (s,f) acc
            in ((s,f), ((rel,a)::existing))::(List.remove_assoc (s,f) acc))
     	[] sources in
   let csource =
      List.map (fun ((s,f),ra) -> CG.source s f ra) sources_and_adaptors in
   Util.GenericIO.write out_file_name 
     (fun out_file -> output (main schema patterns csource ctrigs
                              toplevel_queries) out_file; 
                      output_string out_file "\n");
   
end
