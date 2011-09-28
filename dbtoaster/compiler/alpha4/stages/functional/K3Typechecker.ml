open M3
open K3.SR
open Util

(***************************
 * Typechecking:
 * -- value types are assumed to be floats
 * -- tuple projection is typed based on the arity of indexes requested, that is
 *    single index projections yield field itself, while multiple index
 *    projections yield a sub-tuple
 * -- singletons have no schema since all variables are bound, they are typed
 *    by their values
 * -- collections types are implicitly appended with their value type, that is:
 *      C([x,y]) : C([x,y,Float])
 * -- currying is suffix-based, e.g.
 *       ((fun x y z -> x+y+z) a b) = (fun x -> x+a+b)
 *
 *
 * TODO: make two variants of typechecking, for M3, and less-restricted K3
 *
 *****************************)

(* TODO: Lifting
 * -- we should apply lifting once, before typechecking, rather than lifting for
 *    typechecking as needed.
 *
 * -- when is it safe to lift? collections of functions are generally not
 *    safe for function lifting, because functions define equivalence classes
 *    and it is the class being "indexed"/named/referenced as part of the
 *    collection record (i.e. functions are data).
 * 
 * Map lifting:
 * -- map(f : Fn(b,Fn(c,d)), Lambda(g, h : C([b,c])))
 *      = Lambda(g,map(f,h)) : Fn(g,d)
 *    here Lambda(g,h) can be lifted since maps only apply to collections,
 *    and there is a single function yielding the collection
 *
 * -- map(f : Fn(a,Fn(b,Fn(c,d))), g : C([b,c]))
 *      = Lambda(a,map(f' : Fn(b,Fn(c,d)), g : C([b,c]))) : Fn(a,d)
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *    here f can be partially applied to g, and lifted since f is common to
 *    all collection elements. Note that map uses suffix currying.
 *
 * -- map(f : Fn(a,Fn(b,Fn(c,d))), Lambda(g, h : C([b,c])))
 *      = Lambda(g,Lambda(a,map(f' : Fn(b,Fn(c,d)),h))) : Fn(g,Fn(a,d))
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *    Note lifting occurs right-to-left, that is the collection function is
 *    lifted before any partially evaluated map function. 
 *
 * Aggregation lifting:
 *
 * -- aggregate(f : Fn(b,Fn(c,Fn(d,d))), i:d, Lambda(g, h : C([b,c])))
 *      = Lambda(g,aggregate(f,i,h))
 *
 * -- aggregate(f: Fn(a,Fn(b,Fn(c,Fn(d,d)))), i:d, g : C([b,c]))
 *      = Lambda(a,aggregate(f',i,g))
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *
 * -- aggregate(f: Fn(a,Fn(b,Fn(c,Fn(d,d)))), i:d, Lambda(g, h : C([b,c])))
 *      = Lambda(g,Lambda(a,aggregate(f' : Fn(b,Fn(c,Fn(d,d))), i:d, h : C([b,c]))))
 *      where f  = Lambda(a,Lambda(b,Lambda(c,...)))
 *      and   f' = Lambda(b,Lambda(c,...))
 *
 * Flatten lifting:
 *
 * -- flatten(Lambda(a,C(C(b)))) = Lambda(a,flatten(C(C(b))))
 *
 *
 * Unsafe:
 * -- implicit lifting from collections, e.g.
 *       FFn([w,x,y], Lambda(z,C([a,b]))) : C([w,x,y,Fn(z,C([a,b,Float]))])
 *       = Lambda(z,FFn([w,x,y,C([a,b])])) : Fn(z,C([w,x,y,C([a,b,Float])]))
 *
 * -- lifting left-to-right, recursively, e.g.:
 *      C([Fn(a,b),Fn(c,Fn(d,e)),Float]) = Fn(a, Fn(c, (Fn(d, C([b,e,Float])))))
 *
 *      C([Fn(a,b),Fn(Fn(c,C(d)),Fn(e,f)),Float]) =
 *          Fn(a, Fn(Fn(c,C(d)), Fn(e, C([b,f,Float]))))
 *
 * -- conditionals can be incorrectly lifted w.r.t types, e.g.
 *        Map( f : Fn(a,Fn(b,Fn(c,Fn(d,e))) , C([c,d]) )
 *          : Fn(a, b, Map(f: Fn(c,Fn(d,e)), ...))
 *      where f : Lambda(a, IfThenElse(F,
 *                  Lambda(b,Lambda(c,..)), Lambda(b,Lambda(c,...))))
 *    TODO:
 *    this is because we're typing based on what a lifting simplification would
 *    do, rather than actually applying this simplification, and simply typing
 *    each expression. We should *not* have the typechecker recreate the effect
 *    of optimizations. Conditions should not admit lambda lifting.
 *)

(* Helpers *)
let expr_error (expr:K3.SR.expr_t) (msg:string) = (
   Debug.print "K3-TYPECHECK-DETAIL" (fun () -> msg ^"\n\n"^ (code_of_expr expr));
   failwith msg
)
let type_error (t:K3.SR.type_t) (msg:string) = (
   Debug.print "K3-TYPECHECK-DETAIL" (fun () -> msg ^"\n\n"^ (string_of_type t));
   failwith msg
)
let sch_error (sch) (msg:string) = type_error (TTuple(List.map snd sch)) msg

let find_vars v e =
    let aux _ accll e =
      let acc = List.flatten (List.flatten accll) in
      match e with
      | Var (id,t) -> if v = id then (id,t)::acc else acc
      | OutPC(id,outs,t) -> (List.filter (fun (x,t) -> v=x) outs)@acc
      | PC(id,ins,outs,t)  -> (List.filter (fun (x,t) -> v=x) outs)@acc
      | _ -> acc 
    in fold_expr aux (fun x e -> x) None [] e

(* computes a sub list from start to fin,
 * including element at start, but not fin *)
let sub_list start fin l = snd (List.fold_left (fun (cnt,acc) e ->
    if cnt < start || cnt >= fin
    then (cnt+1,acc) else (cnt+1,acc@[e])) (0,[]) l)
    
let back l = let x = List.rev l in (List.hd x, List.rev (List.tl x))

let type_as_schema t = match t with TTuple(t_l) -> t_l | _ -> [t]

(* Type flattening helpers *)
let is_flat t = match t with | TFloat | TInt -> true | _ -> false
let flat t = if is_flat t then t else type_error t "invalid flat expression"

let promote t1 t2 = match t1,t2 with
    | (TFloat, TInt) | (TInt, TFloat) -> TFloat
    | (TInt, TInt) -> TInt
    | (x,y) -> if x = y then x 
               else type_error (TTuple[x;y]) "could not promote types"

(* returns t2 if it can safely be considered t1 (i.e. t1 < t2 in the type
 * lattice) otherwise fails *)
let valid_promote t1 t2 = if (promote t1 t2) <> t1
    then type_error (TTuple[t1;t2]) "potential loss of precision" else t2

let unify_types l = List.fold_left promote (List.hd l) (List.tl l) 

(* Multivariate function helpers *)

(* flattens the arg types of a multivariate function, i.e.
 *    linearize(Fn([a],Fn([b;c;d],Fn(e,Fn(f,g)))))
 *        = [[a];[b;c;d];[e];[f];[g]]
 *)
let rec linearize_mvf e : type_t list list =
    match e with | Fn(a,b) -> a::(linearize_mvf b) | _ -> [[e]]

(* reconstructs a multivariate function from its linearization *)
let rebuild_mvf rt tll : type_t =
    List.fold_left (fun acc_t tl -> Fn(tl,acc_t)) rt (List.rev tll)


(* Type checking primitives *)

(* checks the type of a schema, to ensure it contains no unit types *)
let tc_schema sch =
    let t_l = List.map snd sch in
    let invalid = List.mem TUnit t_l in
    if invalid then (sch_error sch "invalid schema") else t_l

(* checks if an the types of expression list match a schema *) 
let tc_schema_exprs sch t_l =
    let sch_t = tc_schema sch in
    if List.for_all (fun x->x)
        (List.map2 (fun t (_,t2) -> (promote t t2) = t2) t_l sch)
    then sch_t
    else sch_error sch "invalid expression list schema"

(* checks two operands of a binop *)
let tc_op t1 t2 =
    match t1, t2 with
    | (TUnit,_) | (_,TUnit) -> type_error (TTuple[t1;t2]) "invalid operands"
    | (a,b) -> promote (flat a) (flat b)

(* checks function declarations, ensuring consistent usage of arg in body *)
let tc_fn_arg ae e : type_t list =
    let aux (v,v_t) =
        let vars = find_vars v e in
        if vars = [] then v_t
        else let found_t = unify_types (List.map snd vars) in
        try valid_promote found_t v_t
        with Failure _ ->
            type_error v_t ("arg "^v^"type is not consistent w/ body")
    in begin match ae with
    | AVar(v,v_t) -> [aux (v,v_t)]
    | ATuple(args) -> List.map aux args
    end

(* Map/Aggregate helpers *)

let tc_single_rv tl = match tl with
    | [TUnit] -> type_error (TTuple(tl)) 
                    "invalid collection operation: unit return type found"
    | [x]     -> x
    | []      -> type_error (TTuple(tl)) 
                    "invalid collection operation: no return value type found"
    | _       -> type_error (TTuple(tl)) 
                    "invalid collection operation: multiple rv types found"

let tc_multiple_rv tl = match tl with
    | [TUnit] -> type_error (TTuple(tl)) 
                    "invalid collection operation: unit return type found"
    | [x]     -> x
    | []      -> type_error (TTuple(tl)) 
                    "invalid collection operation: no return value type found"
    | _       -> TTuple(tl) 

let tc_compose_collection expected_rt_opt compose_rt g_rt f_rtl =
    begin match expected_rt_opt with
    | None ->
        begin match g_rt with
        | Collection _ -> compose_rt g_rt f_rtl
        | _ -> type_error g_rt "invalid collection composition"
        end 
    | Some(t) ->
        begin match g_rt, f_rtl with
        | (Collection _, f_t) when f_t = t -> compose_rt g_rt f_rtl
        | (Collection _, f_t) -> failwith
            "apply-each in collection composition yields unexpected type"
        | _ -> failwith "invalid collection composition"
        end 
    end

(* validation for applying a function to elements of a collection,
 * performs suffix validation, and suffix currying. 
 * arguments:
 * -- fn_tll, a linearized nested function type
 * -- sch_t, a collection element type
 * notes:
 * ++ excludes the last elem of fn_tll, which is a fn return type
 * ++ checks sch_t is a suffix that does not cross function nesting boundaries,
 *    which would result in partial application of one of the nested functions
 *    e.g. suppose fn_tl = [[a]; [b;c;d]; [e]; [f] [g]],
 *                 sch_t = [e;f] does not cross a nesting boundary
 *           while sch_t = [c;d;e;f] does
 *             and sch_t = [x;f] is not a valid suffix  
 * ++ returns a fn type, where this fn yields (rt_f (last fn_tll))
 *    and has args of the prefix (fn_tll - sch_t)
 * ++ e.g. fn_tl: [[a]; [b;c;d]; [e]; [f]; [g]; [h]] sch_t: [e;f;g] yields
 *    Fn([a],Fn([b;c;d], rt_f h))
 *)
let tc_fn_app_suffix (rt_f : type_t list -> type_t) fn_tll sch_tl : type_t =
    let rt,rest = back fn_tll in
    let rest_rev = List.rev rest in
    let sch_len = List.length sch_tl in
    let valid_suffix,_,ret_fn_a_t = List.fold_left (fun (v,cnt,acc) tl ->
	        let r = List.length tl in
	        if v then (v,cnt,tl::acc)
	        else if (cnt+r) = sch_len then
                let suffix_tl = List.flatten (tl::acc) in
                (sch_tl=suffix_tl,cnt+r,[]) 
	        else (v, cnt+r, tl::acc))
        (false,0,[]) rest_rev
    in
    if valid_suffix then
        List.fold_left (fun acc_t tl -> Fn(tl,acc_t)) (rt_f rt) ret_fn_a_t
    else type_error (TTuple[TTuple(List.map (fun x -> TTuple(x)) fn_tll); 
                            TTuple(sch_tl)]) 
                    "invalid schema function application"        

(* check apply-to-each, of a fn: fn_tl on a value : t
 * -- supports tuple binding, i.e.
 *   ++ if t : collection(tuple(x)), validates against x
 *   ++ if t : collection(x), validates against x
 * -- yields a function type f : x+ -> coll_fn y, where fn_tl = [x+;y]
 *)
let tc_fn_app_each (rt_f : type_t list -> type_t) fn_tll t : type_t =
    match t with
    | Collection(c_t) ->
        tc_fn_app_suffix rt_f fn_tll (type_as_schema c_t)
    | _ ->
        (*tc_fn_app_suffix rt_f fn_tll [t]*)
        type_error (TTuple[TTuple(List.map (fun x -> TTuple(x)) fn_tll); t]) 
                   "invalid apply-to-each, expected a collection"

(* lifting helper. given a fn, g : t, linearizes g, yields (f ret rest) *)
let lift_fn (lift_f : type_t list -> type_t list list -> 'a) 
            (fn_t : type_t) : 'a =
    let (ret_t, rest_t) = back (linearize_mvf fn_t)
    in lift_f ret_t rest_t

(* validates function composition for fn_app_each of a function f, on another
 * function g, that yields a collection.
 * arguments:
 * -- fn_tll, a linearized nested function f
 * -- fn_c_t, a function g, that yields a collection
 * notes:
 * -- lifts g above f, checking f can be applied to the result of g
 * -- tc_fn_app_suffix strips any commonality in g's ret type and f's suffix
 *   ++ g's ret type is passed to tc_rt_f though
 * -- e.g. fn_tl = [[c];[d]], g = [[a;b];[c]], yields Fn([a;b], (tc_rt_f c) [d])
 *)
let tc_compose_fn (crt_f : type_t -> type_t list -> type_t)
                  (fn_tll : type_t list list) (fn_c_t : type_t) =
    lift_fn (fun ret_tl rest_tll ->
        let c_t = List.hd ret_tl in
        rebuild_mvf (tc_fn_app_each (crt_f c_t) fn_tll c_t) rest_tll) fn_c_t

(* Note: the accumulator arg appears as the last argument, i.e. similar to
 * OCaml's List.fold_right/
 * Lifting: same as map, except for additional checks against initial
 * aggregate
 * -- suffix currying
 * -- implicitly lifts multivariate collection functions and
 *    partial agg function application *)
let tc_agg fn_t i_t c_t (tc_gb_f : type_t -> type_t list -> type_t) =
    let agg_fn_tll = linearize_mvf fn_t in
    if List.length agg_fn_tll < 3 then failwith "invalid aggregate function"
    else
    (* For aggregate functions, we can ignore the accumulator argument,
     * this will always be bound by the initial value within the aggregation.
     * Thus we drop the last type for the function we're lifting (this is
     * the same as dropping the accumulator type, which must be the same
     * as the return type).
     *)
    let agg_ret_tl, acc_tl, fn_tll =
        let x,y = back agg_fn_tll in
        let y_len = (List.length y)-1 in (x, List.nth y y_len, y)
    in
    let valid_agg_fn = List.for_all (fun (x,y) -> x=y)
        [acc_tl,agg_ret_tl; acc_tl,[i_t]; agg_ret_tl,[i_t]]
    in
    if not(valid_agg_fn) then
        failwith "invalid accumulator/agg function return/initial value"
    else
        begin match c_t with
        | TUnit -> failwith "invalid aggregate collection"
        | Fn _ -> tc_compose_fn tc_gb_f fn_tll c_t
        | _ -> tc_gb_f c_t [(tc_fn_app_each tc_single_rv fn_tll c_t)]
        end

(* Persistent collection operation helpers *)

(* given a map collection, m : collection tuple(k@[v]), and key : k_t
 * -- validates k = k_t
 * -- input m may be a fn, m : x+ -> collection tuple(k@[v])
 * -- we lift the fn and validate, yielding: x+ -> (f v (k@[v]))
 *)
let tc_pcop f m_t k_t =
    let m_f ret_tl rest_tll =
        match ret_tl with
        | [Collection(TTuple(kv_tl))] ->
            let (mv_t, t_l) = back kv_tl in
            begin try ignore(List.map2 valid_promote t_l k_t);
                rebuild_mvf (f mv_t kv_tl) rest_tll
                with Failure _ -> failwith "invalid map op elements/key"
            end
        | _ -> failwith "invalid map op"
    in lift_fn m_f m_t


let rec typecheck_expr e : type_t =
   try
    let recur = typecheck_expr in
    match e with
    | Const        c ->
        begin match c with | CFloat _ -> TFloat | CString _ -> TInt end

    | Var          (id,t)     -> t

    (*  e_i : t_i  t_i <> Unit
     * -----------------------------
     *  Tuple([e_i]) : TTuple([t_i])
     *)
    | Tuple         e_l       ->
        let t_l = List.map recur e_l in
        let invalid = List.mem TUnit t_l in
        if invalid then failwith "invalid tuple value" else TTuple(t_l)

    (*  e : TTuple(t)   for all i in idx. i < length(t)
     * -------------------------------------------------
     *  Project(e,idx) :
     *    if length(idx) = 1 then t(idx) else TTuple([t_i | i in idx]) 
     *)
    | Project      (ce, idx)  ->
        let c_t = recur ce in
        begin match c_t with
        | TTuple(t_l) ->
            let len = List.length t_l in
            let valid = List.for_all ((>) len) idx in
            if valid then
                let ret_tl = List.map (List.nth t_l) idx in 
                if (List.length idx) = 1 then List.hd ret_tl else TTuple(ret_tl)
            else failwith "invalid tuple projection index"
        | _ -> failwith "invalid tuple projection"
        end

    (*  e : t    t <> Unit
     * -----------------------------
     *  singleton(e) : collection t
     *)
    | Singleton       (ce)      -> Collection(
        match recur ce with TUnit -> failwith "invalid singleton" | x -> x)

    (*  e1 : collection t1   e2 : collection t2   t1 <> Unit, t1 = t2
     * -----------------------------------------------------------------
     *  combine(e1,e2) : collection t1
     *)
    | Combine         (ce1,ce2) ->
        let ce1_t, ce2_t = (recur ce1, recur ce2) in
        begin match ce1_t,ce2_t with
        | (Collection(c1_t), Collection(c2_t)) ->
            if (c1_t <> TUnit) && (c1_t = c2_t) then ce1_t
            else failwith "invalid collections for combine"
        | _ -> failwith "invalid combine of non-collections" 
        end

    (*  e1 : t1     e2 : t2 
     *  t1 <> Unit, t2 <> Unit, flat(t1), flat(t2)
     * --------------------------------------------
     *  op(e1,e2) : promote(t1,t2)
     *)
    | Add          (ce1,ce2) -> tc_op (recur ce1) (recur ce2)
    | Mult         (ce1,ce2) -> tc_op (recur ce1) (recur ce2)
    | Eq           (ce1,ce2) -> tc_op (recur ce1) (recur ce2)
    | Neq          (ce1,ce2) -> tc_op (recur ce1) (recur ce2)
    | Lt           (ce1,ce2) -> tc_op (recur ce1) (recur ce2)
    | Leq          (ce1,ce2) -> tc_op (recur ce1) (recur ce2)

    (*  e1 : t1     e2 : t2 
     *  t1 <> Unit, t2 <> Unit, flat(t1)
     * --------------------------------------------
     *  op(e1,e2) : t2
     *)
    | IfThenElse0  (ce1,ce2) ->
        (* Check ce1 is a singleton, return 'then' type *)
        ignore(flat (recur ce1)); recur ce2

    | Comment(c, cexpr) ->
        recur cexpr

    (* TODO: it is unsafe to lift functions above a condition,
     * how do we ensure expressions above this condition don't do it?
     * -- I don't think we can... *)
    | IfThenElse   (pe, te, ee) ->
        let (p_t, t_t, e_t) = (recur pe, recur te, recur ee) in
        if is_flat p_t then
            try promote t_t e_t with Failure _ ->
            failwith "mismatch branch types for condition"
        else failwith "invalid condition predicate"

    (* e_i : t_i  len([t_i]) = n, n > 0, for all i < n: t_i = Unit 
     * ------------------------------------------------------------
     * block([t_i]) : t_n 
     *)
    | Block        (e_l) ->
        begin match e_l with
        | [] -> failwith "empty block"
        | _ ->
            let t_l = List.map recur e_l in
            let len = List.length t_l in
            let r = List.nth t_l (len-1) in
            let rest = if len > 1 then sub_list 0 (len-2) t_l else [] in
            if List.for_all (fun t -> t = TUnit) rest then r
            else failwith "invalid block elements"
        end
          

    (* f : x -> unit     c : collection x
     * -------------------------------------------
     * iterate(f,c) : unit
     *) 
    | Iterate      (fn_e, ce) ->
        (* TODO: almost the same as map typechecking... lift *)
        let (fn_t, c_t) = recur fn_e, recur ce in
        let fn_tl = linearize_mvf fn_t in
        let tc_iter_rt = tc_compose_collection (Some([TUnit])) (fun _ _ -> TUnit)
        in begin match c_t with
        | TUnit -> failwith "invalid iterate collection"
        | Fn _ -> tc_compose_fn tc_iter_rt fn_tl c_t
        | _ -> tc_fn_app_each (tc_iter_rt c_t) fn_tl c_t
        end
        

    (* a : x  c : y
     *   forall { z | (b : z) in c, a=b }. unify(x,z) and promote(x->z)
     * ----------------------------------------------------------------------
     * lambda a.c : fn ([x], y) 
     *)
    | Lambda       (ae,ce)    ->
        (* TODO: type inference for args *)
        Fn(tc_fn_arg ae ce, recur ce)
    
    (* a1 : x, a2 : y,  c : z
     *   forall { v | (d : z) in c, a1=b }. unify(x,v) and promote(x->v)
     *   forall { v | (d : z) in c, a2=b }. unify(y,v) and promote(y->v)
     * ----------------------------------------------------------------------
     * lambda a,b.c : fn ([x], fn([y], z)) 
     *)
    | AssocLambda  (arg1_e,arg2_e,be) ->
        (* assumes v1,v2 : Int if v1/v2 is not used in be *)
        (* TODO: type inference for v1, v2 *)
        Fn(tc_fn_arg arg1_e be, Fn(tc_fn_arg arg2_e be, recur be))
    
    (* f : fn ([x],y)  arg : x    f : fn([x;y],z)    arg : tuple(x,y)
     * -----------------------    -----------------------------------
     * apply(f, arg) : y            apply(f, arg) : y
     *)
    | Apply        (fn_e,arg_e) ->
        let arg_t = recur arg_e in
        let fn_t = recur fn_e in
        begin match fn_t with
        | Fn(expected, ret) ->
            let valid = match expected with
                | [x] -> arg_t = x
                | _ -> arg_t = TTuple(expected) 
            in if valid then ret else failwith "invalid function application"
        | _ -> failwith "invalid function application"
        end

    (* f : fn(x,y),  c :           f : fn(x,y),  c : fn() 
     * ----------------------      -----------------------
     *   map(f,c)                    map(f,c)
     *)
    | Map          (fn_e,ce) ->
        let (fn_t, c_t) = (recur fn_e, recur ce) in
        let fn_tll = linearize_mvf fn_t in
        let coll_fn = tc_compose_collection
            None (fun g fl -> Collection(List.hd fl))
        in
        if List.length fn_tll < 2 then failwith "invalid map function"
        else
            begin match c_t with
            | TUnit -> failwith "invalid map collection"
            | Fn _ -> tc_compose_fn coll_fn fn_tll c_t
            | _ -> tc_fn_app_each (coll_fn c_t) fn_tll c_t
            end

    | Aggregate    (fn_e,i_e,ce) ->
        tc_agg (recur fn_e) (recur i_e) (recur ce) (fun c_ret_t agg_tl ->
            match c_ret_t, agg_tl with
            | (Collection _,[agg_t]) -> agg_t
            | (Collection _,_) -> failwith "invalid aggregate type"
            | _ -> failwith "invalid aggregate collection")

    | GroupByAggregate (fn_e, i_e, ge, ce) ->
        let g_t, c_t = (recur ge, recur ce) in
        let g_tll = linearize_mvf g_t in
        let gb_tc_f c_ret_t agg_tl =
            let gb_ret_t = tc_fn_app_each tc_multiple_rv g_tll c_ret_t in
            begin match (c_ret_t, agg_tl, gb_ret_t) with
            | (_,_,TUnit) -> failwith "invalid group by value"
            | (Collection _, [agg_t], TTuple(t_l)) -> Collection(TTuple(t_l@[agg_t]))  
            | (Collection _, [agg_t], x) -> Collection(TTuple([x;agg_t]))
            | (Collection _, _, _) -> failwith "invalid aggregate type"
            | _ -> failwith "invalid gb aggregate collection"  
            end
        in tc_agg (recur fn_e) (recur i_e) c_t gb_tc_f 

    | Flatten      ce ->
        let c_t = recur ce in
        let tc_aux f t = match t with
            | Fn (_,_) -> lift_fn f t | _ -> f [t] []
        in 
        let l1 tl rest_tll = match tl with
            | [Collection(Collection(x))] ->
                rebuild_mvf (Collection(x)) rest_tll
            | _ -> failwith "invalid flatten"
        in tc_aux l1 c_t
    
    (* Persistent collections define records ,i.e. variables and their types,
     * and may contain nested collections, e.g. C(w:int,x:float,y:C(z:float))
     * -- no lifting, in general this is not safe here *)

    | SingletonPC   (id,t) ->
        if t = TUnit then failwith "invalid singleton PC" else t
    
    | OutPC         (id,outs,t) -> Collection(TTuple((tc_schema outs)@[t]))
    | InPC          (id,ins,t) -> Collection(TTuple((tc_schema ins)@[t]))
    | PC            (id,ins,outs,t) ->
        let ins_t = tc_schema ins in
        let outs_t = tc_schema outs
        in Collection(TTuple(ins_t@[Collection(TTuple(outs_t@[t]))]))

    | Member (me, ke) -> tc_pcop (fun _ _ -> TFloat) (recur me) (List.map recur ke) 
    | Lookup (me, ke) -> tc_pcop (fun mv_t _ -> mv_t) (recur me) (List.map recur ke)

    | Slice (me, sch, pat_ve) ->
        let valid_pattern = List.for_all (fun (v,e) ->
            List.mem_assoc v sch &&
            (let t = List.assoc v sch in (promote t (recur e)) = t)) pat_ve in
        if valid_pattern
        then let ke = List.map (fun (v,t) -> Var(v,t)) sch
             in tc_pcop (fun mv_t kv_tl -> Collection(TTuple(kv_tl)))
                    (recur me) (List.map recur ke)
        else failwith "inconsistent schema,pattern for map slice" 

    | PCUpdate     (me,ke,te)           ->
        begin match me,ke with
        | (OutPC _,[]) | (InPC _,[]) ->
            if (recur me) = (recur te) then TUnit
            else failwith "invalid map update tier type"

        | (PC(id,ins,outs,t(*,ie*)),_) ->
            let (m_t, t_t) = (recur me, recur te) in
            let valid_tier = match m_t with
                | Collection(TTuple(kv_tl)) -> (fst (back kv_tl)) = t_t
                | _ -> failwith "internal error, expected collection type for map"
            in
            if valid_tier then
                try ignore(tc_schema_exprs ins (List.map recur ke)); TUnit
                with Failure x -> failwith ("map update key: "^x) 
            else failwith "invalid tier for map update"

        | _ -> failwith "invalid target for map update"
        end

    | PCValueUpdate(me,ine,oute,ve)     ->
        let aux sch v_t ke ve =
            if (promote v_t (recur ve)) = v_t then
                try tc_schema_exprs sch (List.map recur ke)
                with Failure x -> failwith ("map value update: "^x)
            else failwith "map value update: invalid update value expression"
        in 
        begin match me with
        | SingletonPC(id,t) -> TUnit
        | OutPC (id,outs,t) -> ignore(aux outs t oute ve); TUnit 
        | InPC (id,ins,t) -> ignore(aux ins t ine ve); TUnit
        | PC(id,ins,outs,t) ->
            ignore(aux ins t ine ve); ignore(aux outs t oute ve); TUnit
        | _ -> failwith "invalid target for map value update"
        end

    (*| External     efn_id ->
        begin match efn_id with
        | Symbol(id) ->
            begin try let (arg_t, ret_t) = Hashtbl.find ext_fn_symbols id
                      in rebuild_mvf ret_t arg_t
                  with Not_found -> failwith "invalid external function"
            end 
        | _ -> failwith "externals not yet supported"
        end
    *)
   with Failure x -> 
      Debug.print "K3-TYPECHECK-DETAIL" (fun () -> 
         "--------- Expression trace ----------\n"^(code_of_expr e)
      );
      failwith ("K3 Typecheck Error: "^x^"\n(use '-d k3-typecheck-detail' for more information)")