open Type
open Constants
module K = K3

(***************************
* Typechecking:
* -- value types are assumed to be floats
* -- tuple projection is typed based on the arity of indexes requested, that is
* single index projections yield field itself, while multiple index
* projections yield a sub - tuple
* -- singletons have no schema since all variables are bound, they are typed
* by their values
* -- K.Collections types are implicitly appended with their value type, that is
* C([x, y]) : C([x, y, Float])
* -- currying is suffix - based, e.g.
* ((fun x y z -> x + y + z) a b) = (fun x -> x + a + b)
*
*
* TODO: make two variants of typechecking, for M3, and less - restricted K3
*
*****************************)

(* Helpers *)

exception K3TypecheckError of K.expr_t list * string

let string_of_k3_stack (stack:K.expr_t list) = 
   (List.fold_left (fun rest expr ->
      rest^
      "--------- Expression trace ----------\n"^
      (K3.string_of_expr expr)^"\n"
   ) "" stack)

let expr_error ?(old_stack:K.expr_t list=[]) (expr: K.expr_t) (msg: string) = 
   raise (K3TypecheckError((expr::old_stack), msg))

let type_error (t: K.type_t) (msg: string) = 
   raise (K3TypecheckError([],(msg^" ("^(K.string_of_type t)^")")))

let sch_error (sch) (msg: string) = type_error (K.TTuple(List.map snd sch)) msg

let find_vars v e =
   let aux _ accll e =
      let acc = List.flatten (List.flatten accll) in
      match e with
      | K.Var (id, t) -> if v = id then (id, t):: acc else acc
      | K.OutPC(id, outs, t) -> (List.filter (fun (x, t) -> v = x) outs)@acc
      | K.PC(id, ins, outs, t) -> (List.filter (fun (x, t) -> v = x) outs)@acc
      | _ -> acc
   in K.fold_expr aux (fun x e -> x) None [] e

(* computes a sub list from start to fin, * including element at start,    *)
(* but not fin                                                             *)
let sub_list start fin l = snd (
   List.fold_left (fun (cnt, acc) e ->
      if cnt < start || cnt >= fin
      then (cnt +1, acc) 
      else (cnt +1, acc@[e])
   ) (0,[]) l)

let back l = let x = List.rev l in (List.hd x, List.rev (List.tl x))

let type_as_schema t = match t with K.TTuple(t_l) -> t_l | _ -> [t]

(* Type flattening helpers *)
let is_flat t = match t with | K.TBase(_) -> true | _ -> false
let flat t = if is_flat t then t else type_error t "invalid flat expression"

let rec promote t1 t2 =
   match t1, t2 with
   | (K.TBase(TFloat), K.TBase(TInt)) 
   | (K.TBase(TInt), K.TBase(TFloat)) -> K.TBase(TFloat)
   | (K.TBase(TInt), K.TBase(TBool)) 
   | (K.TBase(TBool), K.TBase(TInt)) -> K.TBase(TInt)
   | (K.TBase(TFloat), K.TBase(TBool)) 
   | (K.TBase(TBool), K.TBase(TFloat)) -> K.TBase(TFloat)
   | (K.Collection(x, t1), K.Collection(K.Unknown, t2)) -> 
      K.Collection(x, promote t1 t2)
   | (K.TTuple(fields1), K.TTuple(fields2)) 
      when (List.length fields1) = (List.length fields2) -> 
         K.TTuple(List.map2 promote fields1 fields2)
   | (x, y) -> 
      if x = y then x
      else type_error (K.TTuple[x; y]) "could not promote types"

(* returns t2 if it can safely be considered t1 (i.e. t1 < t2 in the type  *)
(* * lattice) otherwise fails                                              *)
let valid_promote t1 t2 = if (promote t1 t2) <> t1
   then type_error (K.TTuple[t1; t2]) "potential loss of precision" else t2

let unify_types l = List.fold_left promote (List.hd l) (List.tl l)

(* Multivariate function helpers *)

(* flattens the arg types of a multivariate function, i.e. *               *)
(* linearize(Fn([a],Fn([b;c;d],Fn(e,Fn(f,g))))) * =                        *)
(* [[a];[b;c;d];[e];[f];[g]]                                               *)
let rec linearize_mvf e : K.type_t list list =
   match e with | K.Fn(a, b) -> a:: (linearize_mvf b) | _ -> [[e]]

(* reconstructs a multivariate function from its linearization *)
let rebuild_mvf rt tll : K.type_t =
   List.fold_left (fun acc_t tl -> K.Fn(tl, acc_t)) rt (List.rev tll)

(* Type checking primitives *)

(* checks the type of a schema, to ensure it contains no unit types *)
let tc_schema sch =
   let t_l = List.map snd sch in
   let invalid = List.mem K.TUnit t_l in
   if invalid then (sch_error sch "invalid schema") else t_l

(* checks if an the types of expression list match a schema *)
let tc_schema_exprs sch t_l =
   let sch_t = tc_schema sch in
   if List.for_all (fun x -> x)
      (List.map2 (fun t (_, t2) -> (promote t t2) = t2) t_l sch)
   then sch_t
   else sch_error sch "invalid expression list schema"

(* checks two operands of a binop *)
let tc_op t1 t2 =
   match t1, t2 with
   | (K.TUnit, _) | (_, K.TUnit) -> 
      type_error (K.TTuple[t1; t2]) "invalid operands"
   | (a, b) -> promote (flat a) (flat b)

(* checks function declarations, ensuring consistent usage of arg in body *)
let tc_fn_arg ae e : K.type_t list =
   let aux (v, v_t) =
      let vars = find_vars v e in
      if vars = [] then v_t
      else let found_t = unify_types (List.map snd vars) in
              try valid_promote found_t v_t
              with Failure _ ->
                 type_error v_t ("arg "^v^"type is not consistent w/ body")
   in begin match ae with
      | K.AVar(v, v_t) -> [aux (v, v_t)]
      | K.ATuple(args) -> List.map aux args
   end

(* Map/Aggregate helpers *)

let tc_single_rv tl = match tl with
   | [K.TUnit] -> type_error (K.TTuple(tl))
      "invalid K.Collection operation: unit return type found"
   | [x] -> x
   | [] -> type_error (K.TTuple(tl))
      "invalid K.Collection operation: no return value type found"
   | _ -> type_error (K.TTuple(tl))
      "invalid K.Collection operation: multiple rv types found"

let tc_multiple_rv tl = match tl with
   | [K.TUnit] -> type_error (K.TTuple(tl))
      "invalid K.Collection operation: unit return type found"
   | [x] -> x
   | [] -> type_error (K.TTuple(tl))
      "invalid K.Collection operation: no return value type found"
   | _ -> K.TTuple(tl)

let tc_compose_collection expected_rt_opt compose_rt g_rt f_rtl =
   begin match expected_rt_opt with
      | None ->
         begin match g_rt with
            | K.Collection _ -> compose_rt g_rt f_rtl
            | _ -> type_error g_rt "invalid collection composition"
         end
      | Some(t) ->
         begin match g_rt, f_rtl with
            | (K.Collection _, f_t) when f_t = t -> compose_rt g_rt f_rtl
            | (K.Collection _, f_t) -> 
               failwith ("apply-each in collection composition " ^ 
                         "yields unexpected type")
            | _ -> failwith "invalid collection composition"
         end
   end

(* validation for applying a function to elements of a K.Collection, *     *)
(* performs suffix validation, and suffix currying. * arguments: * --      *)
(* fn_tll, a linearized nested function type * -- sch_t, a K.Collection    *)
(* element type * notes: * ++ excludes the last elem of fn_tll, which is a *)
(* fn return type * ++ checks sch_t is a suffix that does not cross        *)
(* function nesting boundaries, * which would result in partial            *)
(* application of one of the nested functions * e.g. suppose fn_tl = [[a]; *)
(* [b;c;d]; [e]; [f] [g]], * sch_t = [e;f] does not cross a nesting        *)
(* boundary * while sch_t = [c;d;e;f] does * and sch_t = [x;f] is not a    *)
(* valid suffix * ++ returns a fn type, where this fn yields (rt_f (last   *)
(* fn_tll)) * and has args of the prefix (fn_tll - sch_t) * ++ e.g. fn_tl: *)
(* [[a]; [b;c;d]; [e]; [f]; [g]; [h]] sch_t: [e;f;g] yields *              *)
(* Fn([a],Fn([b;c;d], rt_f h))                                             *)
let tc_fn_app_suffix (rt_f : K.type_t list -> K.type_t) 
                     fn_tll sch_tl : K.type_t =
   let rt, rest = back fn_tll in
   let rest_rev = List.rev rest in
   let sch_len = List.length sch_tl in
   let valid_suffix, _, ret_fn_a_t = 
      List.fold_left (fun (v, cnt, acc) tl ->
         let r = List.length tl in
         if v then (v, cnt, tl:: acc)
         else if (cnt + r) = sch_len then
                 let suffix_tl = List.flatten (tl:: acc) in
                 let new_v = List.for_all2
                    (fun t1 t2 -> valid_promote t1 t2 = t2) suffix_tl sch_tl
                 in (new_v, cnt + r,[])
              else (v, cnt + r, tl:: acc)
      ) (false,0,[]) rest_rev
   in
   if valid_suffix then
      List.fold_left (fun acc_t tl -> K.Fn(tl, acc_t)) 
                     (rt_f rt) ret_fn_a_t
   else type_error 
           (K.TTuple[ K.TTuple (List.map (fun x -> K.TTuple(x)) fn_tll);
                      K.TTuple(sch_tl) ])
           "invalid schema function application"

(* check apply-to-each, of a fn: fn_tl on a value : t * -- supports tuple  *)
(* binding, i.e. * ++ if t : K.Collection(tuple(x)), validates against x   *)
(* ++ if t : K.Collection(x), validates against x * -- yields a function   *)
(* type f : x+ -> coll_fn y, where fn_tl = [x+;y]                          *)
let tc_fn_app_each (rt_f : K.type_t list -> K.type_t) 
                   fn_tll t : K.type_t =
   match t with
   | K.Collection(_,c_t) ->
      tc_fn_app_suffix rt_f fn_tll (type_as_schema c_t)
   | _ ->
   (* tc_fn_app_suffix rt_f fn_tll [t] *)
      type_error 
         (K.TTuple[ K.TTuple (List.map (fun x -> K.TTuple(x)) fn_tll); 
                    t ])
         "invalid apply-to-each, expected a K.Collection"

(* lifting helper. given a fn, g : t, linearizes g, yields (f ret rest) *)
let lift_fn (lift_f : K.type_t list -> K.type_t list list -> 'a)
            (fn_t : K.type_t) : 'a =
  let (ret_t, rest_t) = back (linearize_mvf fn_t) in 
     lift_f ret_t rest_t

(* validates function composition for fn_app_each of a function f, on      *)
(* another * function g, that yields a K.Collection. * arguments: * --     *)
(* fn_tll, a linearized nested function f * -- fn_c_t, a function g, that  *)
(* yields a K.Collection * notes: * -- lifts g above f, checking f can be  *)
(* applied to the result of g * -- tc_fn_app_suffix strips any commonality *)
(* in g's ret type and f's suffix * ++ g's ret type is passed to tc_rt_f   *)
(* though * -- e.g. fn_tl = [[c];[d]], g = [[a;b];[c]], yields Fn([a;b],   *)
(* (tc_rt_f c) [d])                                                        *)
let tc_compose_fn (crt_f : K.type_t -> K.type_t list -> K.type_t)
                  (fn_tll : K.type_t list list) (fn_c_t : K.type_t) =
   lift_fn (fun ret_tl rest_tll ->
      let c_t = List.hd ret_tl in
         rebuild_mvf (tc_fn_app_each (crt_f c_t) fn_tll c_t) rest_tll
   ) fn_c_t

(* Note: the accumulator arg appears as the last argument, i.e. similar to *)
(* * OCaml's List.fold_right/ * Lifting: same as map, except for           *)
(* additional checks against initial * aggregate * -- suffix currying * -- *)
(* implicitly lifts multivariate K.Collection functions and * partial agg  *)
(* function application                                                    *)
let tc_agg fn_t i_t c_t (tc_gb_f : K.type_t -> K.type_t list -> K.type_t) =
   let agg_fn_tll = linearize_mvf fn_t in
   if List.length agg_fn_tll < 3 then failwith "invalid aggregate function"
   else
      (* For aggregate functions, we can ignore the accumulator argument, *  *)
      (* this will always be bound by the initial value within the           *)
      (* aggregation. * Thus we drop the last type for the function we're    *)
      (* lifting (this is * the same as dropping the accumulator type, which *)
      (* must be the same * as the return type).                             *)
      let agg_ret_tl, acc_tl, fn_tll =
         let x, y = back agg_fn_tll in
         let y_len = (List.length y) -1 in (x, List.nth y y_len, y)
      in
      let valid_agg_fn = List.for_all (fun (x, y) -> x = y)
         [acc_tl, agg_ret_tl; acc_tl,[i_t]; agg_ret_tl,[i_t]]
      in
      if not(valid_agg_fn) then
         failwith "invalid accumulator/agg function return/initial value"
      else
         begin match c_t with
            | K.TUnit -> failwith "invalid aggregate K.Collection"
            | K.Fn _ -> tc_compose_fn tc_gb_f fn_tll c_t
            | _ -> tc_gb_f c_t [(tc_fn_app_each tc_single_rv fn_tll c_t)]
         end

(* Persistent K.Collection operation helpers *)

(* given a map K.Collection, m : K.Collection tuple(k@[v]), and key : k_t  *)
(* validates k = k_t * -- input m may be a fn, m : x+ -> K.Collection      *)
(* tuple(k@[v]) * -- we lift the fn and validate, yielding: x+ -> (f v     *)
(* (k@[v]))                                                                *)
let tc_pcop f m_t k_t =
   let m_f ret_tl rest_tll =
      match ret_tl with
      | [K.Collection(_,K.TTuple(kv_tl))] ->
         let (mv_t, t_l) = back kv_tl in
         begin 
            try 
               ignore(List.map2 valid_promote t_l k_t);
               rebuild_mvf (f mv_t kv_tl) rest_tll
            with Failure _ -> failwith "invalid map op elements/key"
         end
      | _ -> failwith "invalid map op"
   in lift_fn m_f m_t

let rec typecheck_expr e : K.type_t =
   try
      let recur = typecheck_expr in
      match e with
      | K.Const c -> K.TBase(type_of_const c)

      | K.Var (id, t) -> t

      (* e_i : t_i t_i <> Unit * ----------------------------- *             *)
      (* Tuple([e_i]) : K.TTuple([t_i])                                      *)
      | K.Tuple e_l ->
         let t_l = List.map recur e_l in
         let invalid = List.mem K.TUnit t_l in
         if invalid then failwith "invalid tuple value" else K.TTuple(t_l)

      (* e : K.TTuple(t) for all i in idx. i < length(t) *                   *)
      (* ------------------------------------------------- * Project(e,idx)  *)
      (* : * if length(idx) = 1 then t(idx) else K.TTuple([t_i | i in idx])  *)
      | K.Project (ce, idx) ->
         let c_t = recur ce in
         begin match c_t with
            | K.TTuple(t_l) ->
               let len = List.length t_l in
               let valid = List.for_all ((>) len) idx in
               if valid then
                  let ret_tl = List.map (List.nth t_l) idx in
                  if (List.length idx) = 1 then List.hd ret_tl 
                  else K.TTuple(ret_tl)
               else failwith "invalid tuple projection index"
            | _ -> failwith "invalid tuple projection"
         end

      (* e : t t <> Unit * ----------------------------- * singleton(e) :    *)
      (* K.Collection t                                                      *)
      | K.Singleton (ce) -> 
         K.Collection(K.Unknown,
                      match recur ce with 
                      | K.TUnit -> failwith "invalid singleton" 
                      | x -> x)

      (* e1 : K.Collection t1 e2 : K.Collection t2 t1 <> Unit, t1 = t2 *     *)
      (* ----------------------------------------------------------------- * *)
      (* combine(e1,e2) : K.Collection t1                                    *)
      | K.Combine e_l ->
         if e_l = [] then failwith "Invalid (empty) combine operation" 
         else
            let ce_tl = List.map recur e_l in
            begin match List.fold_left K.escalate_type
                                       (List.hd ce_tl) 
                                       (List.tl ce_tl) with
               | K.Collection _ as r -> r
               | _ -> failwith "invalid combine of non-collections"
            end
      (* e1 : t1 e2 : t2 * t1 <> Unit, t2 <> Unit, flat(t1), flat(t2) *      *)
      (* -------------------------------------------- * op(e1,e2) :          *)
      (* promote(t1,t2)                                                      *)
      | K.Add (ce1, ce2) -> tc_op (recur ce1) (recur ce2)
      | K.Mult (ce1, ce2) -> tc_op (recur ce1) (recur ce2)
      | K.Eq (ce1, ce2) -> ignore(tc_op (recur ce1) (recur ce2));
                           K.TBase(Type.TInt)
      | K.Neq (ce1, ce2) -> ignore(tc_op (recur ce1) (recur ce2)); 
                            K.TBase(Type.TInt)
      | K.Lt (ce1, ce2) -> ignore(tc_op (recur ce1) (recur ce2));
                           K.TBase(Type.TInt)
      | K.Leq (ce1, ce2) -> ignore(tc_op (recur ce1) (recur ce2));
                            K.TBase(Type.TInt)

      (* e1 : t1 e2 : t2 * t1 <> Unit, t2 <> Unit, flat(t1) *                *)
      (* -------------------------------------------- * op(e1,e2) : t2       *)
      | K.IfThenElse0 (ce1, ce2) ->
      (* Check ce1 is a singleton, return 'then' type *)
         ignore(flat (recur ce1)); recur ce2

      | K.Comment(c, cexpr) ->
         recur cexpr

      (* TODO: it is unsafe to lift functions above a condition, * how do we *)
      (* ensure expressions above this condition don't do it? * -- I don't   *)
      (* think we can...                                                     *)
      | K.IfThenElse (pe, te, ee) ->
         let (p_t, t_t, e_t) = (recur pe, recur te, recur ee) in
         if is_flat p_t then
            try promote t_t e_t with Failure _ ->
               failwith "mismatch branch types for condition"
         else failwith "invalid condition predicate"

      (* e_i : t_i len([t_i]) = n, n > 0, for all i < n: t_i = Unit *        *)
      (* ------------------------------------------------------------ *      *)
      (* block([t_i]) : t_n                                                  *)
      | K.Block (e_l) ->
         begin match e_l with
            | [] -> failwith "empty block"
            | _ ->
               let t_l = List.map recur e_l in
               let len = List.length t_l in
               let r = List.nth t_l (len -1) in
               let rest = if len > 1 then sub_list 0 (len -2) t_l else [] in
               if List.for_all (fun t -> t = K.TUnit) rest then r
               else failwith "invalid block elements"
         end

      (* f : x -> unit c : K.Collection x *                                  *)
      (* ------------------------------------------- * iterate(f,c) : unit   *)
      | K.Iterate (fn_e, ce) ->
      (* TODO: almost the same as map typechecking... lift *)
         let (fn_t, c_t) = recur fn_e, recur ce in
         let fn_tl = linearize_mvf fn_t in
         let tc_iter_rt = tc_compose_collection (Some([K.TUnit])) 
                                                (fun _ _ -> K.TUnit)
         in begin match c_t with
            | K.TUnit -> failwith "invalid iterate collection"
            | K.Fn _ -> tc_compose_fn tc_iter_rt fn_tl c_t
            | _ -> tc_fn_app_each (tc_iter_rt c_t) fn_tl c_t
         end

      (* a : x c : y * forall { z | (b : z) in c, a=b }. unify(x,z) and      *)
      (* promote(x->z) *                                                     *)
      (* --------------------------------------------------------------------*)
      (* * lambda a.c : fn ([x], y)                                          *)
      | K.Lambda (ae, ce) ->
      (* TODO: type inference for args *)
         K.Fn(tc_fn_arg ae ce, recur ce)

      (* a1 : x, a2 : y, c : z * forall { v | (d : z) in c, a1=b }.          *)
      (* unify(x,v) and promote(x->v) * forall { v | (d : z) in c, a2=b }.   *)
      (* unify(y,v) and promote(y->v) *                                      *)
      (* --------------------------------------------------------------------*)
      (* * lambda a,b.c : fn ([x], fn([y], z))                               *)
      | K.AssocLambda (arg1_e, arg2_e, be) ->
      (* assumes v1,v2 : Int if v1/v2 is not used in be TODO: type inference *)
      (* for v1, v2                                                          *)
         K.Fn(tc_fn_arg arg1_e be, K.Fn(tc_fn_arg arg2_e be, recur be))

      | K.ExternalLambda (fn_id, arg, fn_t) -> K.Fn(K.types_of_arg arg, fn_t)
      
      (* f : fn ([x],y) arg : x f : fn([x;y],z) arg : tuple(x,y) *           *)
      (* ----------------------- ----------------------------------- *       *)
      (* apply(f, arg) : y apply(f, arg) : y                                 *)
      | K.Apply (fn_e, arg_e) ->
         let arg_t = recur arg_e in
         let fn_t = recur fn_e in
         begin match fn_t with
            | K.Fn(expected, ret) ->
               let rec validate_type req_type offered_type = (
                  try 
                     begin match (req_type,offered_type) with 
                        | (K.TBase(rt),K.TBase(ot)) -> 
                           rt = escalate_type rt ot
                        | (K.TTuple(rt),K.TTuple(ot)) ->
                           if List.length rt <> List.length ot then false
                           else List.for_all2 validate_type rt ot
                        | (K.Collection(_, rt),K.Collection(_, ot)) ->
                           validate_type rt ot
                        | (K.Fn(rt_arg,rt_ret),K.Fn(ot_arg,ot_ret))->
                           if List.length rt_arg <> List.length ot_arg
                           then false
                           else (List.for_all2 validate_type rt_arg ot_arg) &&
                                (validate_type rt_ret ot_ret)
                        | (K.TUnit,K.TUnit) -> true
                        | (_,_) -> false
                     end
                  with Failure(_) -> false
               ) in
                  let valid = match expected with
                     | [x] -> validate_type x arg_t
                     | _ -> 
                        begin match arg_t with
                           | K.TTuple(arg_tuple) -> 
                              List.for_all2 validate_type expected arg_tuple
                           | _ -> false
                        end
                  in 
                     if valid then ret 
                     else failwith "invalid function application [1]"
            | _ -> failwith "invalid function application [2]"
         end

      (* f : fn(x,y), c : f : fn(x,y), c : fn() * ----------------------     *)
      (* ----------------------- * map(f,c) map(f,c)                         *)
      | K.Map (fn_e, ce) ->
         let (fn_t, c_t) = (recur fn_e, recur ce) in
         let fn_tll = linearize_mvf fn_t in
         let coll_fn = 
            tc_compose_collection 
               None 
               (fun g fl -> K.Collection(K.Unknown,List.hd fl))
         in
         if List.length fn_tll < 2 then failwith "invalid map function"
         else
            begin match c_t with
               | K.TUnit -> failwith "invalid map collection"
               | K.Fn _ -> tc_compose_fn coll_fn fn_tll c_t               
               | _ -> tc_fn_app_each (coll_fn c_t) fn_tll c_t
            end

      | K.Aggregate (fn_e, i_e, ce) ->
         tc_agg (recur fn_e) (recur i_e) (recur ce) 
                (fun c_ret_t agg_tl ->
                    match c_ret_t, agg_tl with
                       | (K.Collection _,[agg_t]) -> agg_t
                       | (K.Collection _, _) -> 
                          failwith "invalid aggregate type"
                       | _ -> failwith "invalid aggregate K.Collection")

      | K.GroupByAggregate (fn_e, i_e, ge, ce) ->
         let g_t, c_t = (recur ge, recur ce) in
         let g_tll = linearize_mvf g_t in
         let gb_tc_f c_ret_t agg_tl =
            let gb_ret_t = tc_fn_app_each tc_multiple_rv g_tll c_ret_t in
            begin match (c_ret_t, agg_tl, gb_ret_t) with
               | (_, _, K.TUnit) -> failwith "invalid group by value"
               | (K.Collection _, [agg_t], K.TTuple(t_l)) -> 
                  K.Collection(K.Unknown,K.TTuple(t_l@[agg_t]))
               | (K.Collection _, [agg_t], x) -> 
                  K.Collection(K.Unknown,K.TTuple([x; agg_t]))
               | (K.Collection _, _, _) -> failwith "invalid aggregate type"
               | _ -> failwith "invalid gb aggregate K.Collection"
            end
         in tc_agg (recur fn_e) (recur i_e) c_t gb_tc_f

      | K.Flatten ce ->
         let c_t = recur ce in
         let tc_aux f t = match t with
            | K.Fn (_, _) -> lift_fn f t | _ -> f [t] []
         in
         let l1 tl rest_tll = match tl with
            | [K.Collection(_,K.Collection(_,x))] ->
               rebuild_mvf (K.Collection(K.Unknown,x)) rest_tll
            | _ -> failwith "invalid flatten"
         in tc_aux l1 c_t

      (* Persistent K.Collections define records ,i.e. variables and their   *)
      (* types, * and may contain nested K.Collections, e.g.                 *)
      (* C(w:int,x:float,y:C(z:float)) * -- no lifting, in general this is   *)
      (* not safe here                                                       *)

      | K.SingletonPC (id, t) ->
         if t = K.TUnit then failwith "invalid singleton PC" else t

      | K.OutPC (id, outs, t) -> K.Collection(K.Unknown, 
                                              K.TTuple((tc_schema outs) @ [t]))
      | K.InPC (id, ins, t) -> K.Collection(K.Unknown,
                                            K.TTuple((tc_schema ins) @ [t]))
      | K.PC (id, ins, outs, t) ->
         let ins_t = tc_schema ins in
         let outs_t = tc_schema outs in
         K.Collection (K.Unknown,
                       K.TTuple( ins_t @ 
                                 [ K.Collection(K.Unknown, 
                                                K.TTuple(outs_t @ [t])) ]))

      | K.Member (me, ke) -> 
         tc_pcop (fun _ _ -> K.TBase(TFloat)) (recur me) (List.map recur ke)
      | K.Lookup (me, ke) -> 
         tc_pcop (fun mv_t _ -> mv_t) (recur me) (List.map recur ke)

      | K.Slice (me, sch, pat_ve) ->
         let valid_pattern = List.for_all (fun (v, e) ->
            List.mem_assoc v sch &&
            (let t = List.assoc v sch in (promote t (recur e)) = t)
         ) pat_ve 
         in
            if valid_pattern
            then let ke = List.map (fun (v, t) -> K.Var(v, t)) sch in 
               tc_pcop (fun mv_t kv_tl -> 
                  K.Collection(K.Unknown,K.TTuple(kv_tl))
               ) (recur me) (List.map recur ke)
            else failwith "inconsistent schema,pattern for map slice"

      | K.Filter (fn_e, ce) ->
         let (fn_t, c_t) = (recur fn_e, recur ce) in
         begin match fn_t with
            | K.Fn(args, K.TBase(TBool)) when ((List.length args) == 2) -> c_t
            | _ -> failwith "invalid filter function"
         end

      | K.PCUpdate (me, ke, te) ->
         begin match me, ke with
            | (K.OutPC _,[]) | (K.InPC _,[]) ->
               if (recur me) = (recur te) then K.TUnit
               else failwith "invalid map update tier type"

            | (K.PC(id, ins, outs, t(*,ie*)), _) ->
               let (m_t, t_t) = (recur me, recur te) in
               let valid_tier = match m_t with
                  | K.Collection(K.Unknown,K.TTuple(kv_tl)) -> 
                     (fst (back kv_tl)) = t_t
                  | _ -> 
                     failwith ("internal error, expected K.Collection " ^ 
                               "type for map")
               in
                  if valid_tier then
                     try ignore(tc_schema_exprs ins (List.map recur ke));
                         K.TUnit
                     with Failure x -> failwith ("map update key: "^x)
                  else failwith "invalid tier for map update"

            | _ -> failwith "invalid target for map update"
         end

      | K.PCValueUpdate(me, ine, oute, ve) ->
         let aux sch v_t ke ve =
            if (promote v_t (recur ve)) = v_t then
               try tc_schema_exprs sch (List.map recur ke)
               with Failure x | Invalid_argument x
                  -> failwith ("map value update: "^x)
               else 
                  failwith "map value update: invalid update value expression"
         in
         begin match me with
            | K.SingletonPC(id, t) -> ignore(aux [] t [] ve); K.TUnit
            | K.OutPC (id, outs, t) -> ignore(aux outs t oute ve); K.TUnit
            | K.InPC (id, ins, t) -> ignore(aux ins t ine ve); K.TUnit
            | K.PC(id, ins, outs, t) ->
               ignore(aux ins t ine ve); ignore(aux outs t oute ve); 
               K.TUnit
            | _ -> failwith "invalid target for map value update"
         end
      | K.PCElementRemove(me,ine,oute) ->
         let aux sch ke =
            try tc_schema_exprs sch (List.map recur ke)
            with Failure x | Invalid_argument x
               -> failwith ("map element remove: "^x)
         in 
         begin match me with
            | K.SingletonPC(id,t) -> K.TUnit
            | K.OutPC (id,outs,t) -> ignore(aux outs oute); K.TUnit 
            | K.InPC (id,ins,t) -> ignore(aux ins ine); K.TUnit
            | K.PC(id,ins,outs,t) -> ignore(aux ins ine); 
                                     ignore(aux outs oute); K.TUnit
            | _ -> failwith "invalid target for map element remove"
         end
      | K.Unit -> K.TUnit
	  (* TODO: check if defval is the same type as the value of the map *)
      | K.LookupOrElse (me, ke, ve) -> 
         let v_tpe = 
		    tc_pcop (fun mv_t _ -> mv_t) (recur me) (List.map recur ke) 
		 in
		 if v_tpe = (recur ve) then v_tpe else 
		    failwith "type of default value does not match type of collection"
			
   with 
      | Failure(x)                -> expr_error e x
      | K3TypecheckError(stack,x) -> expr_error ~old_stack:stack e x
