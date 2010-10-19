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
         (fun (mapn, _, _, _) -> [mapn]) (fun _ -> []) (fun _ -> []) c)@[mapn]
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
         (fun (n,inv,outv,init) -> MapAccess(n,(List.map aux inv),(List.map aux outv),init))
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
 * -- lhs in tier: lhs in vars, from accessing an in tier, before evaluating
 *                 any rhs expr
 * -- rhs init expr: lhs out vars not defined in the init expr schema
 * -- rhs incr expr: trigger vars, for computing deltas to an out tier
 * -- map lookup init expr: bound out vars, to restrict init vals computed
 * -- op slice expr: propagations from lhs operand to rhs operand, op schema
 *                   extensions by the rhs operand (note these two sets of
 *                   extensions are disjoint, since the rhs schema already
 *                   contains the vars used from the lhs operand, except for
 *                   lhs vars that are only map in vars on the rhs)
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
   
   (* Preparation debugging helpers *)
   (* let debug_bf_equalities lhs_vars c1 c1_bigsums c2 c2_bigsums =
      print_endline ("lhs_vars: "^(M3Common.vars_to_string lhs_vars));
           
      print_endline ("c1_bigsums ("^(M3Common.code_of_calc c1)^"): "^
                     (M3Common.vars_to_string c1_bigsums));
           
      print_endline ("c2_bigsums ("^(M3Common.code_of_calc c2)^"): "^
                     (M3Common.vars_to_string c2_bigsums))           
   in *)

   let rec prepare_calc (update_mapn : string) (lhs_vars: var_t list)
                        (theta_vars : var_t list) (calc : calc_t)
         : (M3P.calc_t * pattern_map) =
      let recur = prepare_calc update_mapn in
      
      (* Prepares either an LHS or RHS operand by computing its extensions.
       * Other metadata for the node (i.e. singleton or cross product traits)
       * is computed recursively. Extensions:
       * -- lhs: schema vars that are not bound from above
       *         (i.e. trigger vars/propagated vars)
       * -- rhs: lhs out vars that are not rhs schema vars, i.e. propagations
       *)
      let prepare_operand lhs_vars c propagated defv theta_ext
            : (M3P.calc_t * var_t list * pattern_map) =
         let outv = calc_schema c in
         let ext = Util.ListAsSet.diff (if propagated then defv else outv)
                      (if propagated then outv else defv) in
         let (pc, pm) = recur lhs_vars (theta_vars@theta_ext) c in 
         (* Override extensions for the operand. *)
         let new_pc_meta = let (w,_,x,y,z) = M3P.get_meta pc in (w,ext,x,y,z)
         in ((M3P.get_calc pc, new_pc_meta), outv, pm)
      in
      (* Prepares a binary operator, computing propagations from lhs to rhs
       * as the extensions for the rhs. Propagations for the rhs are those vars
       * from the lhs schema that also appear in the rhs schema.
       * We now compute propagations strictly, to avoid unused variables, by
       * only propagating those variables that are used in the rhs.
       * Also determines whether the operator is a singleton or suitable for
       * evaluating via a cross-product. 
       * TODO: generalize to k-way operator, only the first operand needs to
       * compare its schema to the globally bound vars.
       * Every other operand simply needs to compare to the union of schemas
       * from prior operands and yield an extension for schema vars not in the
       * union.
       * -- all prior operands allows transitive propagation... 
       * -- why only prior operands' output schemas, and not all globally bound
       *    vars to this point? any vars bound before the first operand is
       *    defined for all operands, and does not need to be considered after
       *    the first operand.
       * -- note to ensure strict propagation, we should only yield extensions
       *    that are used by at least one successor operand.
       *) 
      let prepare_op_w_lhs (c1_lhs_vars : var_t list) (c2_lhs_vars : var_t list)
                           (f : M3P.calc_t -> M3P.calc_t -> M3P.pcalc_t)
                           (c1: calc_t) (c2: calc_t)
            : (M3P.calc_t * pattern_map)
      =
         let (e1, c1_outv, p1_pats) =
            prepare_operand c1_lhs_vars c1 false theta_vars [] in
         let (e2, _, p2_pats) =
            prepare_operand c2_lhs_vars c2 true c1_outv (M3P.get_extensions e1)
         in
         let strict_e1 =
            let (new_c1, (id,ext,sing,prod,cm)) = e1 in
            let strict_ext = Util.ListAsSet.inter ext (calc_vars c2)
            in (new_c1, (id,strict_ext,sing,prod,cm))
         in
         let patterns = merge_pattern_maps p1_pats p2_pats in
         let sing = (M3P.get_singleton strict_e1) && (M3P.get_singleton e2) in
         (* We can use cross products if we don't pass vars sideways... *)
         let prod = (M3P.get_extensions strict_e1) = [] in
         let meta = (prepare(), [], sing, prod, None)
         in 
(*
         let semijoin = (M3P.get_extensions e2) = [] in
         print_endline ((M3Common.code_of_calc c1)^" "^
                        (M3Common.code_of_calc c2)^
                        " semijoin: "^(string_of_bool semijoin)^
                        " prodsemi: "^(string_of_bool (semijoin && prod))^
                        " sing: "^(string_of_bool sing));
*)
         ((f strict_e1 e2, meta), patterns) 
      in
      let prepare_op = prepare_op_w_lhs lhs_vars lhs_vars in
      match (fst calc) with
        | Const(c) ->
           let meta = (prepare(), [], true, false, None)
           in ((Const(c), meta), empty_pattern_map())
        
        | Var(v) ->
           (* Vars are singletons, unless they are bigsum vars. *)
           let singleton = List.mem v lhs_vars in
           let meta = (prepare(), [], singleton, false, None)
           in ((Var(v), meta), empty_pattern_map())

        | Add  (c1,c2)  -> prepare_op (fun e1 e2 -> Add  (e1, e2)) c1 c2
        | Mult (c1,c2)  -> prepare_op (fun e1 e2 -> Mult (e1, e2)) c1 c2
        | Leq  (c1,c2)  -> prepare_op (fun e1 e2 -> Leq  (e1, e2)) c1 c2
        | Eq   (c1,c2)  -> prepare_op (fun e1 e2 -> Eq   (e1, e2)) c1 c2
        | Lt   (c1,c2)  -> prepare_op (fun e1 e2 -> Lt   (e1, e2)) c1 c2
        | IfThenElse0 (c1,c2) ->
           (* if-expressions must explicitly override for short-circuiting *)
           let xor a b = (a || b) && not(a && b) in 
           let get_bigsums l = List.filter (fun v -> not(List.mem v lhs_vars)) l in
           let c1_bigsums = get_bigsums (calc_schema c1) in
           let c2_bigsums = get_bigsums (calc_schema c2) in

           (* debug_cf_bigsums lhs_vars c1 c1_bigsums c2 c2_bigsums; *)

           (* TODO: test bigsum bindings and whether this replaces bigsum
            * slice_var construction with singleton_var.
            * BF-equalities no longer exist in M3, so we need to disable
            * that optimization for testing. *)
           begin match (fst c1) with
            | Eq((Var(x),_), (Var(y),_)) when
               (xor (List.mem x c1_bigsums) (List.mem y c1_bigsums)) ->
               let prebind = [if List.mem x c1_bigsums
                              then (x, y) else (y, x)] in
               let cond_meta = Some(true, prebind, []) in
               let ((nc,(id,ext,sing,prod,_)),pat) =
                  let new_lhs_vars = lhs_vars@[(fst (List.hd prebind))] in
                  prepare_op_w_lhs new_lhs_vars new_lhs_vars
                     (fun e1 e2 -> IfThenElse0 (e1,e2)) c1 c2
               in ((nc, (id,ext,sing,prod,cond_meta)), pat)

            | _ ->
               let index el l =
                  let r = snd( List.fold_left (fun (pos,cur) x ->
                  if cur >= 0 then (pos,cur) else
                  if x = el then (pos,pos) else (pos+1,cur)) (0,-1) l) in
                  if r = -1 then raise Not_found else r 
               in
            
               (* Note: c1_bigsums may be non-empty from nested if-exprs in
                * the then-expr. Thus we only have bigsums locally to this
                * if-expr if c2_bigsums is non-empty *)
               let no_bigsum = c2_bigsums = [] in
               (* Preparation w/ bigsum vars bound *)
               let (lbv, rbv, op_f, l, r) =
                  if no_bigsum then (lhs_vars, lhs_vars,
                     (fun e1 e2 -> IfThenElse0 (e1, e2)), c1, c2)
                  else let new_lhs_vars = lhs_vars@c2_bigsums in
                  (lhs_vars, new_lhs_vars,
                     (fun e1 e2 -> IfThenElse0 (e2, e1)), c2, c1) in
               let cond_meta =
                  let inbind = if no_bigsum then [] else
                     let c2_schema = calc_schema c2 in
                     List.map (fun x -> (x, index x c2_schema)) c2_bigsums
                  in Some(no_bigsum,[],inbind) in
               let ((nc,(id,ext,sing,prod,_)),pat) =
                  (prepare_op_w_lhs lbv rbv op_f l r) in
               
               (* New prepared calc construction w/ bigsum bindings *)
               let new_if_meta = (id,ext,sing,prod,cond_meta)
               in ((nc, new_if_meta), pat)
            end
        
        | MapAccess (mapn, inv, outv, init_calc) ->
           (* Determine slice or singleton.
            * singleton: fully bound out vars, note in vars must always be fully
            * bound here, since statement evaluation loops over the in tier *)
           let bound_inv = Util.ListAsSet.inter inv theta_vars in
           let bound_outv = Util.ListAsSet.inter outv theta_vars in
           let singleton = 
              (List.length bound_inv) = (List.length inv) &&
              (List.length bound_outv) = (List.length outv) in 
           
           (* In vars are bound for recursive preparation for initial
            * value calculus. Out vars are not, these come from out vars of
            * base maps in initial value calculus. *)
           let init_lhs_vars =
              List.fold_left Util.ListAsSet.union theta_vars [inv] in
           
           let (init_ecalc, patterns) =
              save_counter (fun () -> 
                 prepare_calc mapn init_lhs_vars theta_vars (fst init_calc))
           in

           let new_patterns = 
              if singleton then patterns
              else merge_pattern_maps patterns (singleton_pattern_map
                      (mapn, make_out_pattern outv bound_outv)) in

           (* Extend initial val computation for lookups with bound out vars,
            * to restrict initial values computed *)
           let new_init_meta = let (w,_,x,y,z) =
              M3P.get_meta init_ecalc in (w,bound_outv,x,y,z) in
           let new_init_agg_meta = (update_mapn^"_init_"^mapn, singleton) in
           let new_init_aggecalc =
              (((M3P.get_calc init_ecalc), new_init_meta), new_init_agg_meta)
           in
           let new_incr_meta = (prepare(), [], singleton, false, None) in
           let r = (MapAccess(mapn, inv, outv, new_init_aggecalc), new_incr_meta)
           in (r, new_patterns)
   in

   let prepare_stmt theta_vars (stmt : stmt_t) : (M3P.stmt_t * pattern_map) =

      let ((lmapn, linv, loutv, init_calc), (incr_calc,_),_) = stmt in
      let partition v1 v2 =
         (Util.ListAsSet.inter v1 v2, Util.ListAsSet.diff v1 v2) in
      let (bound_inv, loop_inv) = partition linv theta_vars in
      let (bound_outv, loop_outv) = partition loutv theta_vars in
      
      (* For compile_pstmt_loop, extend with loop in vars *)
      let theta_w_loopinv = Util.ListAsSet.union theta_vars loop_inv in
      let theta_w_lhs = Util.ListAsSet.union theta_w_loopinv loutv in
      let full_agg = (List.length loop_outv) = 0 in 

      (* Set up top-level extensions for an entire incr/init RHS.
       * Incr M3 is extended by bound out variables to compute deltas to an
       * out tier.
       * Init M3 is extended by LHS out vars that do not appear in the RHS out
       * vars (i.e. the schema of the init val expr), essentially bound
       * LHS out vars *)
      let init_ext = Util.ListAsSet.diff loutv (calc_schema (fst init_calc)) in
      let prepare_stmt_ext name_suffix calc ext ext_theta =
         let calc_theta_vars = Util.ListAsSet.union theta_w_loopinv
            (if ext_theta then ext else []) in
         let (c, patterns) =
           (* Checking bigsum vars for slices is now done locally by passing
            * down LHS vars through M3 preparation. *)
            prepare_calc lmapn theta_w_lhs calc_theta_vars calc in
         let new_c_aggmeta = (lmapn^name_suffix, full_agg) in
         let new_c_meta = let (w,_,x,y,z) = M3P.get_meta c in (w,ext,x,y,z) in
         let new_c = ((M3P.get_calc c, new_c_meta), new_c_aggmeta) in
            (*print_endline ("singleton="^(string_of_bool (M3P.get_singleton c))^
                           " calc="^(pcalc_to_string (M3P.get_calc c)));*)
            (new_c, patterns) 
      in

      let (init_ca, init_patterns) =
         prepare_stmt_ext "_init" (fst init_calc) init_ext true in
      let (incr_ca, incr_patterns) =
         prepare_stmt_ext "_incr" incr_calc bound_outv false in

      let pstmtmeta = loop_inv in 
      let pstmt = ((lmapn, linv, loutv, init_ca), incr_ca, pstmtmeta) in

      let extra_patterns =
         let extras =
            (if (List.length bound_inv) = (List.length linv) then []
             else [(lmapn, (make_in_pattern linv bound_inv))])@
            (if (List.length bound_outv) = (List.length loutv) then []
             else [(lmapn, (make_out_pattern loutv bound_outv))])
         in List.fold_left add_pattern (empty_pattern_map()) extras
      in

      let patterns = List.fold_left merge_pattern_maps (empty_pattern_map())
        ([ init_patterns; incr_patterns ]@[extra_patterns])
      in
         (pstmt, patterns)
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
   let blocks_maps_l = List.map prepare_trig (normalize_triggers triggers) in
   let (pblocks, pms) = List.split blocks_maps_l in
      (pblocks,
       List.fold_left merge_pattern_maps (empty_pattern_map()) pms)



(* M3 compiler, functorized by a code generator *)
module Make = functor(CG : M3Codegen.CG) ->
struct

open CG

let rec compile_pcalc patterns (incr_ecalc) : code_t = 
   let compile_op ecalc op e1 e2 : code_t =
      let (prebind,inbind) = match M3P.get_cond_meta ecalc with
         | None -> ([],[]) | Some(_,x,y) -> (x,y) in
      let aux ecalc = (calc_schema ecalc, M3P.get_extensions ecalc,
         compile_pcalc patterns ecalc, M3P.get_singleton ecalc) in
      let (outv1, theta_ext, ce1, ce1_sing) = aux e1 in
      let (outv2, schema_ext, ce2, ce2_sing) = aux e2 in
      let schema = calc_schema ecalc in
         begin match (M3P.get_singleton ecalc, ce1_sing, ce2_sing) with
          | (true, false, _) | (true, _, false) | (false, true, true) ->
             failwith "invalid parent singleton"

          | (true, _, _) -> op_singleton_expr prebind op ce1 ce2
          
          | (_, true, false) ->
             op_rslice_expr prebind op outv2 schema schema_ext ce1 ce2
          
          | (_, false, true) ->
             if M3P.get_product ecalc then
                op_lslice_product_expr prebind op outv1 outv2 ce1 ce2
             else op_lslice_expr prebind inbind
                op outv1 outv2 schema theta_ext schema_ext ce1 ce2 

          | (_, false, false) ->
             if M3P.get_product ecalc then
                op_slice_product_expr prebind op outv1 outv2 ce1 ce2
             else op_slice_expr prebind inbind
                op outv1 outv2 schema theta_ext schema_ext ce1 ce2
         end
   in
   let ccalc : code_t =
      match (M3P.get_calc incr_ecalc) with
        MapAccess(mapn, inv, outv, init_aggecalc) ->
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
            (* code that returns the initial value slice *)
            let init_val_code =
               (*print_endline ("generating init lookup for "^mapn^" "^
                                (vars_to_string inv)^" "^
                                (vars_to_string outv)^" "^
                                (string_of_bool (M3P.get_singleton
                                   (get_ecalc init_aggecalc))));*) 
               if singleton_init_code
               then singleton_init_lookup mapn inv out_patterns outv cinit
               else slice_init_lookup mapn inv out_patterns cinit
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
           let short_circuit_if = M3P.get_short_circuit incr_ecalc in
           if short_circuit_if then
              compile_op incr_ecalc CG.ifthenelse0_op c1 c2
           else compile_op incr_ecalc CG.ifthenelse0_bigsum_op c2 c1

      | Const(i)   ->
         if M3P.get_singleton incr_ecalc then (const i)
         else failwith "invalid constant singleton"

      | Var(x)     ->
         if M3P.get_singleton incr_ecalc then (singleton_var x) else slice_var x
      
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
   ((lhs_mapn, lhs_inv, lhs_outv, init_aggecalc), incr_aggecalc, _)
      : code_t
   =
   let aux aggecalc : code_t =
      (*let singleton = M3P.get_singleton (get_ecalc aggecalc) in
      print_endline ((if singleton then "singleton" else "slice")^
                     " calc="^(pcalc_to_string (M3P.get_calc (get_ecalc aggecalc))));*)
      compile_pcalc2 patterns (M3P.get_agg_meta aggecalc) lhs_outv (M3P.get_ecalc aggecalc)
   in
   let (cincr, cinit) = (aux incr_aggecalc, aux init_aggecalc) in
   let init_ext = M3P.get_extensions (M3P.get_ecalc init_aggecalc) in
   let init_value_code =
      let cinit_debug = debug_rhs_init() in
      if (M3P.get_singleton (M3P.get_ecalc init_aggecalc)) ||
         (M3P.get_full_agg (M3P.get_agg_meta init_aggecalc))
      then singleton_init cinit cinit_debug
      else slice_init lhs_inv lhs_outv init_ext cinit cinit_debug
   in
   let cdebug = debug_stmt lhs_mapn lhs_inv lhs_outv in
      if (M3P.get_singleton (M3P.get_ecalc incr_aggecalc)) ||
         (M3P.get_full_agg (M3P.get_agg_meta incr_aggecalc))
      then singleton_update lhs_outv cincr init_value_code cdebug
      else slice_update lhs_mapn lhs_inv lhs_outv cincr init_value_code cdebug


let compile_pstmt_loop patterns trig_args pstmt : code_t =
   let ((lhs_mapn, lhs_inv, lhs_outv, _), incr_aggecalc, stmt_meta) = pstmt in
   let patv = Util.ListAsSet.inter lhs_inv trig_args in
   let pat = List.map (index lhs_inv) patv in
   let direct = (List.length patv) = (List.length lhs_inv) in 
   let map_out_patterns = get_out_patterns patterns lhs_mapn in
   let lhs_ext = M3P.get_inv_extensions stmt_meta in
   (* Output pattern for partitioning *)
   (*
   let out_patv = Util.ListAsSet.inter lhs_outv trig_args in
   let out_pat = List.map (index lhs_outv) out_patv  in
   *)
   let cstmt = compile_pstmt patterns pstmt in
   let singleton_update =
      (M3P.get_singleton (M3P.get_ecalc incr_aggecalc)) ||
      (M3P.get_full_agg (M3P.get_agg_meta incr_aggecalc)) in
   if singleton_update then
      singleton_statement lhs_mapn lhs_inv lhs_outv
         map_out_patterns lhs_ext patv pat direct cstmt
   else
      statement lhs_mapn lhs_inv lhs_outv lhs_ext patv pat direct cstmt

let compile_ptrig (ptrig, patterns) =
   let aux ptrig =
      let (event, rel, trig_args, pblock) = ptrig in
      let aux2 = compile_pstmt_loop patterns trig_args in
      let cblock = List.map aux2 pblock in
      trigger event rel trig_args cblock
   in List.map aux ptrig

let compile_query (((schema,m3prog):M3.prog_t), 
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
