open Type
open Arithmetic
open Calculus
open Constants
open Plan
open M3
(* open UnitTest *)

module C = Calculus

type part_table_t = (string, (int list) option) Hashtbl.t

type part_info_t = 
   | Local
   | Distributed of var_t list

type meta_info_t = {
   part_info  : part_info_t option;        (** Partitioning information *)
   ivars      : var_t list;                (** Input variables *)
   ovars      : var_t list;                (** Output variables *)
}
 
type dist_expr_t = 
   | Sum      of meta_info_t * dist_expr_t list
   | Prod     of meta_info_t * dist_expr_t list
   | Neg      of meta_info_t * dist_expr_t
   (** A value expression *)
   | Value    of meta_info_t * value_t
   (** A base relation *)
   | Rel      of meta_info_t * string * var_t list
   (** A delta relation (for batch updates) *)
   | DeltaRel of meta_info_t * string * var_t list
   (** A comparison between two values *)
   | Cmp      of meta_info_t * cmp_t * value_t * value_t
   (** An external (map) *)
   | External of meta_info_t * dist_expr_t external_leaf_t
   (** A sum aggregate, consisting of group-by variables and 
       the nested sub-term being aggregated over *)
   | AggSum   of meta_info_t * var_t list * dist_expr_t
   (** A Lift expression.  The nested sub-term's value is 
       lifted into the indicated variable *)
   | Lift     of meta_info_t * var_t * dist_expr_t
   (** An existence test.  The value of this term is 1 if and 
       only if the nested expression's value is non-zero. *)
   | Exists   of meta_info_t * dist_expr_t
   (** A domain of delta (for batch updates) *)
   | DomainDelta of meta_info_t * dist_expr_t
   (** Gather and sum the partial results of 
       a distributed expression on master. *)
   | Gather      of meta_info_t * dist_expr_t
   (** Repartition expression by given partition keys. *)
   | Repartition of meta_info_t * dist_expr_t * var_t list


(*** Utility ***)
(** A generic exception pertaining to DistCalculus. The first parameter is 
    the DistCalculus expression that triggered the failure *)
exception DistCalculusException of dist_expr_t * string
;;
(**/**)
let bail_out expr msg = 
   raise (DistCalculusException(expr, msg))
;;
(**/**)

let get_meta_info (dexpr: dist_expr_t): meta_info_t =
   match dexpr with
      | Sum(m, _) 
      | Prod(m, _) 
      | Neg(m, _) 
      | Value(m, _) 
      | Rel(m, _, _) 
      | DeltaRel(m, _, _)
      | Cmp(m, _, _, _) 
      | External(m, _) 
      | AggSum(m, _, _)
      | Lift(m, _, _) 
      | Exists(m, _) 
      | DomainDelta(m, _) 
      | Gather(m, _)
      | Repartition(m, _, _) -> m

let get_part_info (dexpr: dist_expr_t): part_info_t option = 
   (get_meta_info dexpr).part_info

let  sum_list e = match e with  Sum(_, l) -> l | _ -> [e]
let prod_list e = match e with Prod(_, l) -> l | _ -> [e]

(* Construction Helpers *)
let mk_gather (dexpr: dist_expr_t): dist_expr_t = 
   let dexpr_meta = get_meta_info dexpr in
   match dexpr_meta.part_info with
      | Some(Local) -> dexpr
      | Some(Distributed(_)) -> 
         let meta_info = { 
            part_info = Some(Local);
            ivars = dexpr_meta.ivars;
            ovars = dexpr_meta.ovars
         } in
            Gather(meta_info, dexpr)
      | None -> dexpr
  
let rec mk_repartition (pkeys: var_t list) (dexpr: dist_expr_t): dist_expr_t =
   let meta_info = get_meta_info dexpr in
   let dexpr_vars = ListAsSet.union meta_info.ivars meta_info.ovars in

   (* Check if partitioning is possible, otherwise replicate *)
   let trunc_pkeys = if ListAsSet.subset pkeys dexpr_vars then pkeys else [] in

   match dexpr with 
      | Repartition(_, subexp, _) -> mk_repartition trunc_pkeys subexp
      | _ ->
         begin match meta_info.part_info with
            (* Check if already partitioned by trunc_pkeys *)
            | Some(Distributed(pkeys2)) when pkeys2 = trunc_pkeys -> dexpr
            (* Repartition if not None *)
            | Some(Distributed(_)) | Some(Local) ->
               let new_meta_info = {
                  part_info = Some(Distributed(trunc_pkeys));
                  ivars = meta_info.ivars;
                  ovars = meta_info.ovars
               } in
               Repartition(new_meta_info, dexpr, trunc_pkeys)
            | None -> dexpr
         end
   
let rec mk_sum (dexpr_list: dist_expr_t list): dist_expr_t =
   match dexpr_list with
      | [] -> failwith "Empty sum list in DistCalculus.Sum"
      | [x] -> x
      | hd1::(hd2::tl) ->
         let meta_hd1 = get_meta_info hd1 in
         let meta_hd2 = get_meta_info hd2 in

         let old_ivars = ListAsSet.union meta_hd1.ivars meta_hd2.ivars in
         let old_ovars = ListAsSet.union meta_hd1.ovars meta_hd2.ovars in
         let new_ivars = ListAsSet.diff old_ovars 
                                        (ListAsSet.inter meta_hd1.ovars 
                                                         meta_hd2.ovars) in
         let hd_ivars = ListAsSet.union old_ivars new_ivars in
         let hd_ovars = ListAsSet.diff  old_ovars new_ivars in
         let common_vars = 
            ListAsSet.inter (ListAsSet.union meta_hd1.ivars meta_hd1.ovars) 
                            (ListAsSet.union meta_hd2.ivars meta_hd2.ovars)
         in
         let merged_hd = 
            match (meta_hd1.part_info, meta_hd2.part_info) with
               | (Some(Distributed(p1)), Some(Distributed(p2))) ->
                  (* If hd1 and hd2 are partitioned identically, 
                     then do nothing *)
                  if p1 = p2 then
                     let meta_info = {
                        part_info = Some(Distributed(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                     Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
                     (* begin match (hd1, hd2) with
                        | (Repartition(m1, e1, p1), Repartition(m2, e2, p2)) ->
                           mk_repartition p1 (
                              Sum(meta_info, (sum_list e1) @ (sum_list e2))
                           ) 
                        | _ -> Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
                     end *)
                  (* If hd1 is replicated, then repartition hd1 *)
                  else if p1 = [] then
                     let repartitioned_hd1 = mk_repartition p2 hd1 in
                     let meta_info = {
                        part_info = Some(Distributed(p2));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Sum(meta_info, (sum_list repartitioned_hd1) @ 
                                       (sum_list hd2))                     
                  (* If hd2 is replicated, then repartition hd2 *)
                  else if p2 = [] then
                     let repartitioned_hd2 = mk_repartition p1 hd2 in
                     let meta_info = {
                        part_info = Some(Distributed(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Sum(meta_info, (sum_list hd1) @
                                       (sum_list repartitioned_hd2))
                  (* If hd1 is partitioned by a subset of common variables,
                     then repartition hd2 *)
                  else if ListAsSet.subset p1 common_vars then
                     let repartitioned_hd2 = mk_repartition p1 hd2 in
                     let meta_info = {
                        part_info = Some(Distributed(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Sum(meta_info, (sum_list hd1) @
                                       (sum_list repartitioned_hd2))
                  (* Default to partitioning hd1 *)
                  else 
                     let repartitioned_hd1 = mk_repartition p2 hd1 in
                     let meta_info = {
                        part_info = Some(Distributed(p2));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Sum(meta_info, (sum_list repartitioned_hd1) @ 
                                       (sum_list hd2))


               | (Some(Distributed(p1)), Some(Local)) -> 
                  let repartitioned_hd2 = mk_repartition p1 hd2 in
                  let meta_info = {
                     part_info = Some(Distributed(p1));
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list hd1) @
                                    (sum_list repartitioned_hd2))

               | (Some(Local), Some(Distributed(p2))) ->
                  let repartitioned_hd1 = mk_repartition p2 hd1 in
                  let meta_info = {
                     part_info = Some(Distributed(p2));
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list repartitioned_hd1) @ 
                                    (sum_list hd2))
                     
               | (Some(Local), Some(Local)) ->
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                  Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
                  (* begin match (hd1, hd2) with
                     | (Gather(m1, e1), Gather(m2, e2)) ->
                        mk_gather (
                           Sum(meta_info, (sum_list e1) @ (sum_list e2))
                        ) 
                     | _ -> Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
                  end *)

               | (None, pinfo) | (pinfo, None)-> 
                  let meta_info = {
                     part_info = pinfo;
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
         in
            mk_sum (merged_hd :: tl)

let rec mk_prod (dexpr_list: dist_expr_t list): dist_expr_t =
   match dexpr_list with
      | [] -> failwith "Empty prod list in DistCalculus.Prod"
      | [x] -> x
      | hd1::(hd2::tl) ->
         let meta_hd1 = get_meta_info hd1 in
         let meta_hd2 = get_meta_info hd2 in

         let hd_ivars = ListAsSet.union meta_hd1.ivars
                                        (ListAsSet.diff meta_hd2.ivars
                                                        meta_hd1.ovars) in
         let hd_ovars = ListAsSet.diff  (ListAsSet.union meta_hd1.ovars
                                                         meta_hd2.ovars)
                                        meta_hd1.ivars in
         let common_vars = 
            ListAsSet.inter (ListAsSet.union meta_hd1.ivars meta_hd1.ovars) 
                            (ListAsSet.union meta_hd2.ivars meta_hd2.ovars)
         in
         let merged_hd = 
            match (meta_hd1.part_info, meta_hd2.part_info) with
               | (Some(Distributed(p1)), Some(Distributed(p2))) ->
                  (* If hd1 and hd2 are partitioned identically or 
                     hd2 is replicated, then do nothing *)
                  if p1 = p2 || p2 = [] then
                     let meta_info = {
                        part_info = Some(Distributed(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list hd1) @ (prod_list hd2))
                  (* If hd1 is replicated, then do nothing *)
                  else if p1 = [] then
                     let meta_info = {
                        part_info = Some(Distributed(p2));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list hd1) @ (prod_list hd2))
                  (* If hd1 is partitioned by a subset of common variables,
                     then repartition hd2 *)
                  else if ListAsSet.subset p1 common_vars then
                     let repartitioned_hd2 = mk_repartition p1 hd2 in
                     let meta_info = {
                        part_info = Some(Distributed(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list hd1) @
                                        (prod_list repartitioned_hd2))
                  (* Default to partitioning hd1 *)
                  else 
                     let repartitioned_hd1 = mk_repartition p2 hd1 in
                     let meta_info = {
                        part_info = Some(Distributed(p2));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list repartitioned_hd1) @ 
                                        (prod_list hd2))

               | (Some(Distributed(p1)), Some(Local)) -> 
                  let repartitioned_hd2 = mk_repartition p1 hd2 in
                  let meta_info = {
                     part_info = Some(Distributed(p1));
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list hd1) @
                                     (prod_list repartitioned_hd2))

               | (Some(Local), Some(Distributed(p2))) ->
                  let repartitioned_hd1 = mk_repartition p2 hd1 in
                  let meta_info = {
                     part_info = Some(Distributed(p2));
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list repartitioned_hd1) @ 
                                     (prod_list hd2))
                     
               | (Some(Local), Some(Local)) ->
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list hd1) @ (prod_list hd2))

               | (None, pinfo) | (pinfo, None) ->
                  let meta_info = {
                     part_info = pinfo;
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list hd1) @ (prod_list hd2))
         in
            mk_prod (merged_hd :: tl)


let rec lift (part_table: part_table_t) (expr: C.expr_t): dist_expr_t = 
   let rcr = lift part_table in
   let (ivars, ovars) = C.schema_of_expr expr in
   match expr with
      | CalcRing.Sum(sum_list) -> mk_sum (List.map rcr sum_list)
         
      | CalcRing.Prod(prod_list) ->  mk_prod (List.map rcr prod_list)

      | CalcRing.Neg(subexp) -> 
         let dsubexp = rcr subexp in
            Neg(get_meta_info dsubexp, dsubexp)

      | CalcRing.Val(Value(v)) -> 
         let meta_info = { 
            part_info = None;
            ivars = ivars;
            ovars = ovars
         } in
            Value(meta_info, v)

      | CalcRing.Val(Cmp(op, v1, v2)) -> 
         let meta_info = { 
            part_info = None;
            ivars = ivars;
            ovars = ovars
         } in 
            Cmp(meta_info, op, v1, v2)

      | CalcRing.Val(Rel(rname, rvars)) ->
         let part_info = 
            try
               begin match (Hashtbl.find part_table rname) with
                  | Some(indexes) ->                     
                     let pkeys = List.map (List.nth rvars) indexes in
                     Some(Distributed(pkeys))
                  | None -> Some(Local)
               end
            with Not_found -> Some(Local)
         in
         let meta_info = { 
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            Rel(meta_info, rname, rvars)

      | CalcRing.Val(DeltaRel(rname, rvars)) ->
         let part_info = 
            try
               begin match (Hashtbl.find part_table ("DELTA_" ^ rname)) with
                  | Some(indexes) ->
                     let pkeys = List.map (List.nth rvars) indexes in
                     Some(Distributed(pkeys))
                  | None -> Some(Local)
               end
            with Not_found -> Some(Local)
         in
         let meta_info = { 
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            DeltaRel(meta_info, rname, rvars)

      | CalcRing.Val(External(ename, eivars, eovars, etype, eivc)) -> 
         (* TODO: Think about the IVC case *)
         let dsubexp = match eivc with
            | Some(subexp) -> Some(rcr subexp)
            | None -> None          
         in            
         let part_info = 
            try
               begin match (Hashtbl.find part_table ename) with
                  | Some(indexes) ->
                     let pkeys = List.map (List.nth eovars) indexes in
                     Some(Distributed(pkeys))
                  | None -> Some(Local)
               end
            with Not_found -> 
               if (Hashtbl.length part_table > 0) 
               then failwith ("Missing partitioning information for " ^ ename)
               else Some(Local)
         in
         let meta_info = { 
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            External(meta_info, (ename, eivars, eovars, etype, dsubexp))

      | CalcRing.Val(AggSum(gb_vars, subexp)) ->
         let dsubexp = rcr subexp in
         let submeta = get_meta_info dsubexp in
         let meta_info = {
            part_info = submeta.part_info;
            ivars = ivars;
            ovars = ovars
         }
         in
         let expr_vars = ListAsSet.union ivars ovars in
         let new_dexpr = AggSum(meta_info, gb_vars, dsubexp)in
         begin match submeta.part_info with
            | Some(Distributed(pkeys)) 
               when not (ListAsSet.subset pkeys expr_vars) -> 
               (* The case when we have partial sums, but not 
                  partitioning information -->  gather singleton 
                  results, otherwise, repartition. *)
               if gb_vars = [] then mk_gather new_dexpr
               else mk_repartition [] new_dexpr
            | _ -> new_dexpr
         end

      | CalcRing.Val(DomainDelta(subexp)) -> 
         let dsubexp = rcr subexp in
            DomainDelta(get_meta_info dsubexp, dsubexp)

      | CalcRing.Val(Lift(v, subexp)) ->
         let dsubexp = rcr subexp in
         let submeta = get_meta_info dsubexp in
         let meta_info = {
            part_info = submeta.part_info;
            ivars = ivars;
            ovars = ovars
         } in
            Lift(meta_info, v, dsubexp)

      | CalcRing.Val(Exists(subexp)) ->
         let dsubexp = rcr subexp in
            Exists(get_meta_info dsubexp, dsubexp)


let string_of_part_info (part_info: part_info_t option): string = 
   if not (Debug.active "PRINT-PARTITION-INFO") then "" else
   match part_info with
      | Some(Local) -> "<Local>"
      | Some(Distributed(pkeys)) -> 
         "<" ^ (ListExtras.string_of_list ~sep:", " string_of_var pkeys) ^ ">"
      | None -> ""

let rec string_of_expr (dexpr: dist_expr_t): string = 
   let (sum_op, prod_op, neg_op) = 
      if Debug.active "PRINT-RINGS" then (" U ", " |><| ", "(<>:-1)*")
                                    else (" + ", " * ", "NEG * ")
   in
   match dexpr with
      | Sum(meta, sl) -> 
         "(" ^ String.concat sum_op (List.map string_of_expr sl) ^")" ^
         (string_of_part_info meta.part_info)

      | Prod(meta, pl) -> 
         "(" ^ String.concat prod_op (List.map string_of_expr pl) ^")" ^
         (string_of_part_info meta.part_info)

      | Neg(meta, el) -> 
         "(" ^ neg_op ^ (string_of_expr el) ^")" ^
         (string_of_part_info meta.part_info)

      | Value(meta, v) -> 
         "{" ^ (Arithmetic.string_of_value v) ^ "}" ^
         (string_of_part_info meta.part_info)

      | Cmp(meta, op, v1, v2) -> 
         "{" ^ (string_of_value v1) ^ 
         " " ^ (string_of_cmp op) ^
         " " ^ (string_of_value v2) ^ "}" ^
         (string_of_part_info meta.part_info)
      | Rel(meta, rname, rvars) ->
         rname ^
         "(" ^ (ListExtras.string_of_list ~sep:", " string_of_var rvars) ^ ")" ^
         (string_of_part_info meta.part_info)

      | DeltaRel(meta, rname, rvars) ->
         "(DELTA " ^ rname ^ ")" ^          
         "(" ^ (ListExtras.string_of_list ~sep:", " string_of_var rvars) ^ ")" ^
         (string_of_part_info meta.part_info)

      | External(meta, (ename, eins, eouts, etype, emeta)) ->
         ename ^
         "(" ^ (string_of_type etype) ^ ")[" ^
         (ListExtras.string_of_list ~sep:", " string_of_var eins) ^ "][" ^
         (ListExtras.string_of_list ~sep:", " string_of_var eouts) ^ "]" ^
         (match emeta with | None -> ""
                           | Some(s) -> ":(" ^ (string_of_expr s) ^ ")") ^
         (string_of_part_info meta.part_info)

      | AggSum(meta, gb_vars, subexp) ->
         "AggSum([" ^ 
         (ListExtras.string_of_list ~sep:", " string_of_var gb_vars) ^"],(" ^ 
         (string_of_expr subexp) ^ "))" ^
         (string_of_part_info meta.part_info)
      
      | DomainDelta(meta, subexp) -> 
         "DOMAIN(" ^ (string_of_expr subexp) ^ ")" ^
         (string_of_part_info meta.part_info)

      | Lift(meta, target, subexp)    ->
         "(" ^ (string_of_var target) ^ " ^= " ^ (string_of_expr subexp) ^ ")" ^
         (string_of_part_info meta.part_info)

(***** BEGIN EXISTS HACK *****)
      | Exists(meta, subexp) ->
         "Exists(" ^ (string_of_expr subexp) ^ ")" ^
         (string_of_part_info meta.part_info)
(***** END EXISTS HACK *****)

      | Gather(meta, subexp) ->          
         if not (Debug.active "PRINT-ANNOTATED-M3") then 
            string_of_expr subexp
         else
            "Gather(" ^ (string_of_expr subexp) ^ ")" ^
            (string_of_part_info meta.part_info)
      | Repartition(meta, subexp, pkeys) -> 
         if not (Debug.active "PRINT-ANNOTATED-M3") then 
            string_of_expr subexp
         else
            "Repartition([" ^ 
            (ListExtras.string_of_list ~sep:", " string_of_var pkeys) ^"], (" ^
            (string_of_expr subexp) ^ "))" ^
            (string_of_part_info meta.part_info)

let rec cost_of_expr (dexpr: dist_expr_t): int =
   match dexpr with
      | Sum(_, sl) -> List.fold_left (+) 0 (List.map cost_of_expr sl)
      | Prod(_, pl) -> List.fold_left (+) 0 (List.map cost_of_expr pl)
      | Neg(_, el) -> cost_of_expr el
      | Value _ | Cmp _ | Rel _ | DeltaRel _ -> 0
      | External(_, (_, _, _, _, eivc)) ->
         begin match eivc with  
            | Some(subexp) -> cost_of_expr subexp 
            | None -> 0
         end
      | AggSum(_, _, subexp) | DomainDelta(_, subexp) 
      | Lift(_, _, subexp)   | Exists(_, subexp) -> cost_of_expr subexp      
      | Gather(_, subexp)
      | Repartition(_, subexp, _) -> cost_of_expr subexp + 1


let rec optimize_expr (dexpr: dist_expr_t): dist_expr_t = 
   let rcr = optimize_expr in   
   begin match dexpr with 
      | Sum(meta, sum_list) -> 
         let rec group sl = match sl with
            | [] -> []
            | [x] -> [x]
            | hd::tl ->                
               let grouped_tl = group tl in
               try
                  let (lhs, rhs) =                   
                     List.partition (fun grp -> match (hd, grp) with
                        | (Repartition(_, _, p1), Repartition(_, _, p2)) 
                           when p1 = p2 -> true
                        | (Gather _, Gather _) -> true
                        | _ -> false
                     ) grouped_tl
                  in
                  if (List.length lhs = 0) then hd::rhs
                  else if (List.length lhs > 1) then
                     failwith "Redundant expressions not being optimized"
                  else match (hd, List.hd lhs) with
                     | (Repartition(_, s1, p1), Repartition(_, s2, p2))
                        when p1 = p2 -> 
                        (mk_repartition p1 (mk_sum [s1; s2]) :: rhs)
                     | (Gather(_, s1), Gather(_, s2)) -> 
                        (mk_gather (mk_sum [s1; s2]) :: rhs)
                     | _ -> failwith "Impossible"

               with Not_found -> (hd :: grouped_tl)
         in
            mk_sum (group (List.map rcr sum_list))

      | Prod(meta, prod_list) -> Prod(meta, List.map rcr prod_list)
      | Neg(meta, subexp) -> Neg(meta, rcr subexp)
      | Value _ | Cmp _ | Rel _ | DeltaRel _ -> dexpr
      | External(meta, (en, ei, eo, et, eivc)) ->
         begin match eivc with  
            | Some(subexp) -> 
               External(meta, (en, ei, eo, et, Some(rcr subexp)))
            | None -> dexpr
         end
      | AggSum(meta, gb_vars, subexp) -> AggSum(meta, gb_vars, rcr subexp)
      | DomainDelta(meta, subexp) -> DomainDelta(meta, rcr subexp)      
      | Lift(meta, v, subexp) -> Lift(meta, v, rcr subexp)   
      | Exists(meta, subexp) -> Exists(meta, rcr subexp)
      | Gather(meta, subexp) -> 
         let new_subexp = rcr subexp in
         begin match new_subexp with
            | Repartition(_, subexp2, _) -> mk_gather subexp2
            | _ -> mk_gather new_subexp
         end
      | Repartition(meta, subexp, pkeys) -> 
         (* Try pushing Repartition down the tree and compare the costs *)
         let new_dexpr = 
            let mk_rep = mk_repartition pkeys in
            (* let mk_rep_opt e = optimize_expr (mk_rep e) in *)
            let new_meta = {
               part_info = Some(Distributed(pkeys));
               ivars = meta.ivars;
               ovars = meta.ovars;
            } in
            begin match subexp with
               | Sum(meta, sum_list) -> 
                  rcr (Sum(new_meta, List.map mk_rep sum_list))
               | Prod(meta, prod_list) -> 
                  rcr (Prod(new_meta, List.map mk_rep prod_list))
               | Neg(meta, subexp) -> 
                  rcr (Neg(new_meta, mk_rep subexp))
               | Value _ | Cmp _ | Rel _ | DeltaRel _ -> mk_rep dexpr
               | External(meta, (en, ei, eo, et, eivc)) -> 
                  begin match eivc with  
                     | Some(subexp) -> 
                        mk_rep (External(new_meta, (en, ei, eo, et, 
                                                   Some(rcr (mk_rep subexp)))))
                     | None -> mk_rep dexpr
                  end
               | AggSum(meta, gb_vars, subexp) -> 
                  AggSum(new_meta, gb_vars, rcr (mk_rep subexp))
               | DomainDelta(meta, subexp) -> 
                  DomainDelta(new_meta, rcr (mk_rep subexp))
               | Lift(meta, v, subexp) -> 
                  Lift(new_meta, v, rcr (mk_rep subexp))
               | Exists(meta, subexp) -> 
                  Exists(new_meta, rcr (mk_rep subexp))
               | Gather(meta, subexp) -> 
                  Gather(new_meta, rcr (mk_rep subexp))
               | Repartition(meta, subexp, pkeys) -> 
                  rcr (mk_rep subexp)
            end
         in
         begin
            let old_cost = cost_of_expr dexpr in
            let new_cost = cost_of_expr new_dexpr in
            if (new_cost < old_cost) then new_dexpr else dexpr
         end
   end

(**
   An annotated M3 program
*)
type dist_stmt_t = dist_expr_t stmt_base_t

type dist_trigger_t = dist_expr_t trigger_base_t

type dist_prog_t = {
   queries   : (string * expr_t) list ref;
   maps      : map_t list ref;
   triggers  : dist_trigger_t list ref;
   db        : Schema.t
}

let lift_statement (part_table: part_table_t) (stmt: stmt_t): dist_stmt_t =
   {  target_map  = lift part_table stmt.target_map;
      update_type = stmt.update_type;
      update_expr = lift part_table stmt.update_expr  }

let lift_trigger (part_table: part_table_t) 
                 (trigger: trigger_t): dist_trigger_t =
   {  event      = trigger.event;
      statements = ref (List.map (lift_statement part_table) 
                                 !(trigger.statements))  }

let lift_prog (part_table: part_table_t) (prog: prog_t): dist_prog_t = 
   {  queries  = ref !(prog.queries);
      maps     = ref !(prog.maps);
      triggers = ref (List.map (lift_trigger part_table) 
                               !(prog.triggers));
      db       = prog.db   }

(** Stringify a statement.  This string conforms to the grammar of 
    Calculusparser *)
let string_of_statement (dstmt: dist_stmt_t): string = 
   let (lhs, rhs) =    
      match ( get_part_info dstmt.target_map,
              get_part_info dstmt.update_expr ) with
         | (Some(Distributed(p1)), Some(Distributed(p2))) when p1 = p2 -> 
            (dstmt.target_map, dstmt.update_expr)

         | (Some(Distributed(p1)), Some(_)) ->
            (dstmt.target_map, mk_repartition p1 dstmt.update_expr)

         | (Some(Local), Some(Distributed(_))) ->
            (dstmt.target_map, mk_gather dstmt.update_expr)

         | (Some(Local), Some(Local))
         | (Some(_), None) ->
            (dstmt.target_map, dstmt.update_expr)

         | (None, _) -> failwith "No partitioning info for LHS target map"
   in
   let optimized_rhs = optimize_expr rhs in
   let expr_string = string_of_expr optimized_rhs in
   (string_of_expr lhs)^
   (if dstmt.update_type = UpdateStmt then " += " else " := ")^
   (if String.contains expr_string '\n' then "\n  " else "")^
   expr_string ^
   (if (Debug.active "PRINT-ANNOTATED-M3-COSTS") then 
       (" @Cost = " ^ string_of_int (cost_of_expr optimized_rhs))
    else "")

(**
   [string_of_trigger trigger]
   
   Generate the Calculusparser compatible declaration for the specified trigger.
   @param trigger  An M3 trigger
   @return         The (Calculusparser-compatible) declaration for [trigger]
*)
let string_of_trigger (dtrigger: dist_trigger_t): string = 
   (Schema.string_of_event dtrigger.event)^" {"^
   (ListExtras.string_of_list ~sep:"" (fun dstmt ->
      "\n   "^(string_of_statement dstmt)^";"
   ) !(dtrigger.statements))^"\n}"

(**
   [string_of_map ~is_query:(...) map]

   Generate the Calculusparser compatible declaration for the specified map.
   @param is_query (optional) True if the map should be declared as a toplevel 
                   query (default: false).
   @param map      A map
   @return         The (Calculusparser-compatible) declaration for [map]
*)
let string_of_map (part_table: part_table_t) 
                  (map:map_t): string = begin match map with
   | DSView(view) -> 
      "DECLARE MAP "^
      (CalculusPrinter.string_of_expr ~show_type:true view.ds_name)^
      " := \n"^
      (CalculusPrinter.string_of_expr view.ds_definition)^
      (  try
            let name = match view.ds_name with
               | CalcRing.Val(External(name, _, _, _, _)) -> name
               | _ -> failwith "LHS map is not of external type." 
            in
            begin match (Hashtbl.find part_table name) with
              | Some(indexes) -> 
                 let ovars = snd (schema_of_expr view.ds_definition) in
                 let pkeys = List.map (List.nth ovars) indexes in
                 (" PARTITIONED BY ["^
                  ListExtras.string_of_list ~sep:", "
                     (string_of_var ~verbose:true) pkeys ^ "]")
              | None -> ""
            end
         with Not_found -> ""
      )^";"
   | DSTable(rel) -> Schema.code_of_rel rel
   end

(**
   [string_of_prog prog]
   
   Generate the Calculusparser compatible string representation of an M3 
   program.
   @param prog  An M3 program
   @return      The (Calculusparser-compatible) string representation of [prog]
*)
let string_of_prog (part_table: part_table_t) (dprog: dist_prog_t): string = 
   "-------------------- SOURCES --------------------\n"^
   (Schema.code_of_schema dprog.db)^"\n\n"^
   "--------------------- MAPS ----------------------\n"^
   (* Skip Table maps -- these are already printed above in the schema *)
   (ListExtras.string_of_list ~sep:"\n\n" (string_of_map part_table) 
      (List.filter (fun x -> match x with 
         DSTable(_) -> false | _ -> true) !(dprog.maps)))^"\n\n"^
   "-------------------- QUERIES --------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (qname,qdefn) ->
      "DECLARE QUERY "^qname^" :=\n"^(CalculusPrinter.string_of_expr qdefn)^";"
   ) !(dprog.queries))^"\n\n"^
   "------------------- TRIGGERS --------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" string_of_trigger !(dprog.triggers))
