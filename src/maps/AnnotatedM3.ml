open Type
open Arithmetic
open Calculus
open Constants
open Plan
open M3
open Partitioner
(* open UnitTest *)

module C = Calculus

type part_info_t = (var_t) Partitioner.gen_part_info_t

type part_table_t = (int) Partitioner.gen_part_table_t

type meta_info_t = {
   part_info  : part_info_t option;       (** Partitioning information *)
   ivars      : var_t list;               (** Input variables *)
   ovars      : var_t list;               (** Output variables *)
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




let string_of_part_info (part_info: part_info_t option) = 
   match part_info with
      | Some(pinfo) -> Partitioner.string_of_part_info pinfo string_of_var
      | None -> ""

let string_of_expr (dexpr: dist_expr_t): string = 
   let (sum_op, prod_op, neg_op) = 
      if Debug.active "PRINT-RINGS" then (" U ", " |><| ", "(<>:-1)*")
                                    else (" + ", " * ", "NEG * ")
   in
   let rec rcr ind dexpr = match dexpr with
      | Sum(meta, sl) -> 
         "(" ^ String.concat sum_op (List.map (rcr (ind + 2)) sl) ^")" ^
         (string_of_part_info meta.part_info) 

      | Prod(meta, pl) -> 
         "(" ^ String.concat prod_op (List.map (rcr ind) pl) ^")" ^
         (string_of_part_info meta.part_info)

      | Neg(meta, el) -> 
         "(" ^ neg_op ^ (rcr ind el) ^")" ^ 
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
                           | Some(s) -> ":(" ^ (rcr ind s) ^ ")") ^
         (string_of_part_info meta.part_info)

      | AggSum(meta, gb_vars, subexp) ->
         "AggSum([" ^          
         (ListExtras.string_of_list ~sep:", " string_of_var gb_vars) ^"],(" ^ 
         (rcr ind subexp) ^ "))" ^ (string_of_part_info meta.part_info)
      
      | DomainDelta(meta, subexp) -> 
         "DOMAIN(" ^ (rcr ind subexp) ^ ")" ^ 
         (string_of_part_info meta.part_info)

      | Lift(meta, target, subexp)    ->
         "(" ^ (string_of_var target) ^ " ^= " ^ (rcr ind subexp) ^ ")" ^
         (string_of_part_info meta.part_info)

(***** BEGIN EXISTS HACK *****)
      | Exists(meta, subexp) ->
         "Exists(" ^ (rcr ind subexp) ^ ")" ^ 
         (string_of_part_info meta.part_info)
(***** END EXISTS HACK *****)

      | Gather(meta, subexp) ->          
         if not (Debug.active "PRINT-ANNOTATED-M3") then rcr (ind + 2) subexp
         else 
            "\n" ^ String.make ind ' ' ^ "Gather(" ^
            (rcr (ind + 2) subexp) ^ ")" ^ (string_of_part_info meta.part_info)

      | Repartition(meta, subexp, pkeys) -> 
         if not (Debug.active "PRINT-ANNOTATED-M3") then rcr (ind + 2) subexp
         else
            "\n" ^ String.make ind ' ' ^ "Repartition([" ^ 
            (ListExtras.string_of_list ~sep:", " string_of_var pkeys) ^"], (" ^
            (rcr (ind + 2) subexp) ^ "))" ^ 
            (string_of_part_info meta.part_info)
   in
      rcr 4 dexpr





(** Construction Helpers **)

let rec mk_repartition (pkeys: var_t list) (dexpr: dist_expr_t): dist_expr_t =
   let meta_info = get_meta_info dexpr in
   let dexpr_vars = ListAsSet.union meta_info.ivars meta_info.ovars in

   (* Check if partitioning is possible, otherwise replicate *)
   let trunc_pkeys = if ListAsSet.subset pkeys dexpr_vars then pkeys else [] in
   let rcr = mk_repartition trunc_pkeys in

   (* Match expression and apply simplifications *)
   match dexpr with 
      | Gather(_, subexp)         -> rcr subexp
      | Repartition(_, subexp, _) -> rcr subexp
      | Lift(_, v, subexp) ->
         let new_subexp = rcr subexp in
         let meta_info = get_meta_info new_subexp in
            Lift(meta_info, v, new_subexp)
      | _ ->
         (* If no input variables, repartition whole expression; else recur *)
         if (meta_info.ivars = []) then 
            begin match meta_info.part_info with
               (* Check if already partitioned by trunc_pkeys *)
               | Some(DistributedByKey(pkeys2)) when pkeys2 = trunc_pkeys -> 
                  dexpr
               | Some(loc) ->
                  begin match dexpr with 
                     | Exists(_, subexp) when (loc == DistributedRandom) ->
                        let new_subexp = rcr subexp in
                        let meta_info = get_meta_info new_subexp in
                          Exists(meta_info, new_subexp)
                     | _ -> 
                        let new_meta_info = {
                           part_info = Some(DistributedByKey(trunc_pkeys));
                           ivars = meta_info.ivars;
                           ovars = meta_info.ovars
                        } 
                        in 
                           Repartition(new_meta_info, dexpr, trunc_pkeys)
                  end
               | None -> dexpr
            end
         else 
            begin match dexpr with 
               | Sum(_, sl)  -> mk_sum (List.map rcr sl)
               | Prod(_, pl) -> mk_prod (List.map rcr pl)
               | Neg(_, subexp) ->
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     Neg(meta_info, new_subexp) 
               | Lift(_, v, subexp) -> 
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     Lift(meta_info, v, new_subexp)                       
               | Exists(_, subexp) -> 
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     Exists(meta_info, new_subexp)
               | DomainDelta(_, subexp) -> 
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     DomainDelta(meta_info, new_subexp)
               | AggSum(_, gb_vars, subexp) ->
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     AggSum(meta_info, gb_vars, new_subexp)
               | Value(m, _) | Cmp(m, _, _, _) -> dexpr
               | Rel _ 
               | DeltaRel _ 
               | External _ 
               | Gather _ 
               | Repartition _ -> failwith "Impossible to happen"
            end

and mk_gather (dexpr: dist_expr_t): dist_expr_t = 
   let meta_info = get_meta_info dexpr in
   let rcr = mk_gather in

   match dexpr with 
      | Gather(_, subexp)         -> rcr subexp
      | Repartition(_, subexp, _) -> rcr subexp
      | Lift(_, v, subexp) ->
         let new_subexp = rcr subexp in
         let meta_info = get_meta_info new_subexp in
            Lift(meta_info, v, new_subexp)
      | _ -> 
         (* If no input variables, gather whole expression; else recur *)      
         if (meta_info.ivars = []) then 
            begin match meta_info.part_info with
               | Some(Local) -> dexpr
               | Some(loc) ->
                  begin match dexpr with 
                     | Exists(_, subexp) when (loc == DistributedRandom) ->
                        let new_subexp = rcr subexp in
                        let meta_info = get_meta_info new_subexp in
                          Exists(meta_info, new_subexp)
                     | _ -> 
                        let new_meta_info = { 
                           part_info = Some(Local);
                           ivars = meta_info.ivars;
                           ovars = meta_info.ovars
                        } in
                           Gather(new_meta_info, dexpr)
                  end
               | None -> dexpr
            end
         else 
            begin match dexpr with 
               | Sum(_, sl)  -> mk_sum (List.map rcr sl)               
               | Prod(_, pl) -> mk_prod (List.map rcr pl)
               | Neg(_, subexp) ->
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     Neg(meta_info, new_subexp) 
               | Lift(_, v, subexp) -> 
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     Lift(meta_info, v, new_subexp)                       
               | Exists(_, subexp) -> 
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     Exists(meta_info, new_subexp)
               | DomainDelta(_, subexp) -> 
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     DomainDelta(meta_info, new_subexp)
               | AggSum(_, gb_vars, subexp) ->
                  let new_subexp = rcr subexp in
                  let meta_info = get_meta_info new_subexp in
                     AggSum(meta_info, gb_vars, new_subexp)
               | Value(m, _) | Cmp(m, _, _, _) -> dexpr
               | Rel _ 
               | DeltaRel _ 
               | External _ 
               | Gather _ 
               | Repartition _ -> failwith "Impossible to happen"            
            end

and mk_sum (dexpr_list: dist_expr_t list): dist_expr_t =
   let rec rcr expr_list = match  expr_list with
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

               | (Some(DistributedByKey(p1)), Some(DistributedByKey(p2))) ->

                  (* If hd1 and hd2 are partitioned equally, then do nothing *)
                  if p1 = p2 then
                     let meta_info = {
                        part_info = Some(DistributedByKey(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                     Sum(meta_info, (sum_list hd1) @ (sum_list hd2))

                  (* If hd1 is replicated, then repartition hd1 *)
                  else if p1 = [] then 
                     let repartitioned_hd1 = mk_repartition p2 hd1 in                     
                     begin match (get_part_info repartitioned_hd1) with 
                        | Some(DistributedByKey(pk)) when p2 = pk -> 
                           let meta_info = {
                              part_info = Some(DistributedByKey(p2));
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list repartitioned_hd1) @ 
                                             (sum_list hd2))          
                        | _ -> 
                           (* failwith "Repartitioned keys do not match" *)
                           
                           (* Default to local operations *)
                           let gathered_hd1 = mk_gather hd1 in
                           let gathered_hd2 = mk_gather hd2 in
                           let meta_info = {
                              part_info = Some(Local);
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list gathered_hd1) @ 
                                             (sum_list gathered_hd2))                        
                     end

                  (* If hd2 is replicated, then repartition hd2 *)
                  else if p2 = [] then
                     let repartitioned_hd2 = mk_repartition p1 hd2 in
                     begin match (get_part_info repartitioned_hd2) with 
                        | Some(DistributedByKey(pk)) when p1 = pk ->
                           let meta_info = {
                              part_info = Some(DistributedByKey(p1));
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list hd1) @
                                             (sum_list repartitioned_hd2))
                        | _ -> 
                           (* failwith "Repartitioned keys do not match" *)

                           (* Default to local operations *)
                           let gathered_hd1 = mk_gather hd1 in
                           let gathered_hd2 = mk_gather hd2 in
                           let meta_info = {
                              part_info = Some(Local);
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list gathered_hd1) @ 
                                             (sum_list gathered_hd2))                           
                     end

                  (* If hd1 is partitioned by a subset of common variables,
                     then repartition hd2 *)
                  else if ListAsSet.subset p1 common_vars then
                     let repartitioned_hd2 = mk_repartition p1 hd2 in
                     begin match (get_part_info repartitioned_hd2) with
                        | Some(DistributedByKey(pk)) when p1 = pk ->
                           let meta_info = {
                              part_info = Some(DistributedByKey(p1));
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list hd1) @
                                             (sum_list repartitioned_hd2))
                        | _ -> 
                           (* failwith "Repartitioned keys do not match" *)

                           (* Default to local operations *)
                           let gathered_hd1 = mk_gather hd1 in
                           let gathered_hd2 = mk_gather hd2 in
                           let meta_info = {
                              part_info = Some(Local);
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list gathered_hd1) @ 
                                             (sum_list gathered_hd2))                           
                     end      

                  (* Default to partitioning hd1 *)
                  else 
                     let repartitioned_hd1 = mk_repartition p2 hd1 in
                     begin match (get_part_info repartitioned_hd1) with
                        | Some(DistributedByKey(pk)) when p2 = pk ->
                           let meta_info = {
                              part_info = Some(DistributedByKey(p2));
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list repartitioned_hd1) @ 
                                             (sum_list hd2))
                        | _ -> 
                           (* failwith "Repartitioned keys do not match"  *)

                           (* Default to local operations *)
                           let gathered_hd1 = mk_gather hd1 in
                           let gathered_hd2 = mk_gather hd2 in
                           let meta_info = {
                              part_info = Some(Local);
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Sum(meta_info, (sum_list gathered_hd1) @ 
                                             (sum_list gathered_hd2))                           
                     end

               | (Some(DistributedByKey(p1)), Some(DistributedRandom)) 
               | (Some(DistributedByKey(p1)), Some(Local)) -> 
                  let repartitioned_hd2 = mk_repartition p1 hd2 in
                  begin match (get_part_info repartitioned_hd2) with
                     | Some(DistributedByKey(pk)) when p1 = pk ->
                        let meta_info = {
                           part_info = Some(DistributedByKey(p1));
                           ivars = hd_ivars;
                           ovars = hd_ovars;
                        } in
                           Sum(meta_info, (sum_list hd1) @
                                          (sum_list repartitioned_hd2))
                     | _ -> 
                        (* failwith "Repartitioned keys do not match" *)

                        (* Default to local operations *)
                        let gathered_hd1 = mk_gather hd1 in
                        let gathered_hd2 = mk_gather hd2 in
                        let meta_info = {
                           part_info = Some(Local);
                           ivars = hd_ivars;
                           ovars = hd_ovars;
                        } in
                           Sum(meta_info, (sum_list gathered_hd1) @ 
                                          (sum_list gathered_hd2))                        
                  end

               | (Some(DistributedRandom), Some(DistributedByKey(p2)))
               | (Some(Local), Some(DistributedByKey(p2))) ->
                  let repartitioned_hd1 = mk_repartition p2 hd1 in
                  begin match (get_part_info repartitioned_hd1) with
                     | Some(DistributedByKey(pk)) when p2 = pk ->
                        let meta_info = {
                           part_info = Some(DistributedByKey(p2));
                           ivars = hd_ivars;
                           ovars = hd_ovars;
                        } in
                           Sum(meta_info, (sum_list repartitioned_hd1) @ 
                                          (sum_list hd2))
                     | _ -> 
                        (* failwith "Repartitioned keys do not match" *)

                        (* Default to local operations *)
                        let gathered_hd1 = mk_gather hd1 in
                        let gathered_hd2 = mk_gather hd2 in
                        let meta_info = {
                           part_info = Some(Local);
                           ivars = hd_ivars;
                           ovars = hd_ovars;
                        } in
                           Sum(meta_info, (sum_list gathered_hd1) @ 
                                          (sum_list gathered_hd2))                        
                  end

               | (Some(DistributedRandom), Some(DistributedRandom)) ->      
                  let meta_info = {
                     part_info = Some(DistributedRandom);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list hd1) @ (sum_list hd2))

               | (Some(Local), Some(Local)) ->
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
               
               | (Some(Local), Some(DistributedRandom)) ->
                  let gathered_hd2 = mk_gather hd2 in
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list hd1) @ 
                                    (sum_list gathered_hd2))               

               | (Some(DistributedRandom), Some(Local)) ->
                  let gathered_hd1 = mk_gather hd1 in
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list gathered_hd1) @ 
                                    (sum_list hd2))                              

               | (None, pinfo) | (pinfo, None)-> 
                  let meta_info = {
                     part_info = pinfo;
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Sum(meta_info, (sum_list hd1) @ (sum_list hd2))
         in
            rcr (merged_hd :: tl)
   in
      rcr (List.flatten (List.map sum_list dexpr_list))

and mk_prod (dexpr_list: dist_expr_t list): dist_expr_t =
   let rec rcr expr_list = match expr_list with
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

               | (Some(DistributedByKey(p1)), Some(DistributedByKey(p2))) ->
                  
                  (* If hd1 and hd2 are partitioned equally or 
                     hd2 is replicated, then do nothing *)
                  if p1 = p2 || p2 = [] then
                     let meta_info = {
                        part_info = Some(DistributedByKey(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list hd1) @ (prod_list hd2))

                  (* If hd1 is replicated, then do nothing *)
                  else if p1 = [] then
                     let meta_info = {
                        part_info = Some(DistributedByKey(p2));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list hd1) @ (prod_list hd2))

                  (* If hd1 is partitioned by a subset of common variables,
                     then repartition hd2 *)
                  else if ListAsSet.subset p1 common_vars then
                     let repartitioned_hd2 = mk_repartition p1 hd2 in
                     let meta_info = {
                        part_info = Some(DistributedByKey(p1));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list hd1) @
                                        (prod_list repartitioned_hd2))

                  (* Default to partitioning hd1 *)
                  else 
                     let repartitioned_hd1 = mk_repartition p2 hd1 in
                     let meta_info = {
                        part_info = Some(DistributedByKey(p2));
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     } in
                        Prod(meta_info, (prod_list repartitioned_hd1) @ 
                                        (prod_list hd2))

               | (Some(DistributedByKey([])), Some(Local)) -> 
                  let gather_hd1 = mk_gather hd1 in
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list gather_hd1) @
                                     (prod_list hd2))

               | (Some(DistributedByKey(p1)), Some(DistributedRandom)) 
               | (Some(DistributedByKey(p1)), Some(Local)) -> 
                  let repartitioned_hd2 = mk_repartition p1 hd2 in
                  let meta_info = {
                     part_info = Some(DistributedByKey(p1));
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list hd1) @
                                     (prod_list repartitioned_hd2))

               | (Some(Local), Some(DistributedByKey([]))) -> 
                  let gather_hd2 = mk_gather hd2 in
                  let meta_info = {
                     part_info = Some(Local);
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list hd1) @
                                     (prod_list gather_hd2))

               | (Some(DistributedRandom), Some(DistributedByKey(p2))) 
               | (Some(Local), Some(DistributedByKey(p2))) ->
                  let repartitioned_hd1 = mk_repartition p2 hd1 in
                  let meta_info = {
                     part_info = Some(DistributedByKey(p2));
                     ivars = hd_ivars;
                     ovars = hd_ovars;
                  } in
                     Prod(meta_info, (prod_list repartitioned_hd1) @ 
                                     (prod_list hd2))

               | (Some(Local), Some(DistributedRandom))
               | (Some(DistributedRandom), Some(Local))
               | (Some(DistributedRandom), Some(DistributedRandom)) ->
                  if (common_vars = []) then
                     let gathered_hd1 = mk_gather hd1 in
                     let gathered_hd2 = mk_gather hd2 in
                     let meta_info = {
                        part_info = Some(Local);
                        ivars = hd_ivars;
                        ovars = hd_ovars;
                     }
                     in
                        Prod(meta_info, (prod_list gathered_hd1) @
                                        (prod_list gathered_hd2))
                  else
                     let repartitioned_hd1 = mk_repartition common_vars hd1 in
                     let repartitioned_hd2 = mk_repartition common_vars hd2 in
                     begin match (get_part_info repartitioned_hd1,
                                  get_part_info repartitioned_hd2) with
                        | (Some(DistributedByKey(pk1)), 
                           Some(DistributedByKey(pk2))) when pk1 = pk2 ->
                           let meta_info = {
                              part_info = Some(DistributedByKey(pk1));
                              ivars = hd_ivars;
                              ovars = hd_ovars;                              
                           }
                           in 
                              Prod(meta_info, (prod_list repartitioned_hd1) @
                                              (prod_list repartitioned_hd2))
                        | _ -> 
                           (* failwith "Repartitioned keys do not match" *)

                           (* Default to local operations *)
                           let gathered_hd1 = mk_gather hd1 in
                           let gathered_hd2 = mk_gather hd2 in
                           let meta_info = {
                              part_info = Some(Local);
                              ivars = hd_ivars;
                              ovars = hd_ovars;
                           } in
                              Prod(meta_info, (prod_list gathered_hd1) @ 
                                              (prod_list gathered_hd2))                           
                     end

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
            rcr (merged_hd :: tl)
   in
     rcr (List.flatten (List.map prod_list dexpr_list))


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
                  | DistributedByKey(indexes) ->
                     let pkeys = List.map (List.nth rvars) indexes in
                     Some(DistributedByKey(pkeys))
                  | DistributedRandom -> Some(DistributedRandom)
                  | Local -> Some(Local)
               end
            with Not_found -> 
               failwith ("Partitioning information not found for " ^ rname)
         in
         let meta_info = { 
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            Rel(meta_info, rname, rvars)

      | CalcRing.Val(DeltaRel(rname, rvars)) ->
         let part_info = 
            let dname = "DELTA_" ^ rname in
            try
               begin match (Hashtbl.find part_table dname) with
                  | DistributedByKey(indexes) ->
                     let pkeys = List.map (List.nth rvars) indexes in
                     Some(DistributedByKey(pkeys))
                  | DistributedRandom -> Some(DistributedRandom)
                  | Local -> Some(Local)
               end
            with Not_found -> 
               failwith ("Missing partitioning information for " ^ dname)
         in
         let meta_info = { 
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            DeltaRel(meta_info, rname, rvars)

      | CalcRing.Val(External(ename, eivars, eovars, etype, eivc)) -> 
         let dsubexp = match eivc with
            | Some(subexp) -> Some(rcr subexp)
            | None -> None          
         in            
         let part_info = 
            try
               begin match (Hashtbl.find part_table ename) with
                  | DistributedByKey(indexes) ->
                     let pkeys = List.map (List.nth eovars) indexes in
                     Some(DistributedByKey(pkeys))
                  | DistributedRandom -> Some(DistributedRandom)
                  | Local -> Some(Local)
               end
            with Not_found -> 
               failwith ("Missing partitioning information for " ^ ename)
         in
         let meta_info = { 
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            External(meta_info, (ename, eivars, eovars, etype, dsubexp))

      | CalcRing.Val(AggSum(gb_vars, subexp)) ->
         let dsubexp = rcr subexp in         
         let part_info = 
            let expr_vars = ListAsSet.union ivars ovars in
            begin match (get_part_info dsubexp) with
               | Some(DistributedByKey(pkeys)) 
                  when not (ListAsSet.subset pkeys expr_vars) -> 
               (* The case when we have partial sums, but not 
                  partitioning info -->  mark as distributed randomly. *)                  
                  Some(DistributedRandom)
               | pinfo -> pinfo
            end
         in
         let meta_info = {
            part_info = part_info;
            ivars = ivars;
            ovars = ovars
         } in
            AggSum(meta_info, gb_vars, dsubexp)

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


(* A list of maps involving delta relations; used in the cost optimizer *)
let delta_maps: string list ref = ref [];;

let is_delta_map (name: string) = 
   List.fold_left (fun found n -> (found || (name = n))) false !delta_maps

let rec expr_has_delta_maps (dexpr: dist_expr_t): bool = 
   match dexpr with
      | Sum(_, sl)  -> List.fold_left (||) false 
                                      (List.map expr_has_delta_maps sl)
      | Prod(_, pl) -> List.fold_left (||) false 
                                      (List.map expr_has_delta_maps pl)
      | Neg(_, e)   -> expr_has_delta_maps e
      | Value _ 
      | Rel _ 
      | DeltaRel _ 
      | Cmp _ -> false
      | AggSum(_, _, subexp)
      | Lift(_, _, subexp) 
      | Exists(_, subexp) 
      | DomainDelta(_, subexp) 
      | Gather(_, subexp)
      | Repartition(_, subexp, _) -> expr_has_delta_maps subexp
      | External(_, (en, _, _, _, _))  -> is_delta_map en         

let cost_of_expr (dexpr: dist_expr_t): (int * int * int) =
   let add (a1,b1,c1) (a2,b2,c2) = (a1 + a2, b1 + b2, c1 + c2) in
   let rec rcr expr =  match expr with
      | Sum(_, sl) -> List.fold_left add (0,0,0) (List.map rcr sl)
      | Prod(_, pl) -> List.fold_left add (0,0,0) (List.map rcr pl)
      | Neg(_, el) -> rcr el
      | Value _ | Cmp _ | Rel _ | DeltaRel _ -> (0,0,0)
      | External(_, (_, _, _, _, eivc)) -> begin match eivc with  
                                             | Some(subexp) -> rcr subexp 
                                             | None -> (0,0,0)
                                           end
      | AggSum(_, _, subexp) | DomainDelta(_, subexp) 
      | Lift(_, _, subexp)   | Exists(_, subexp) -> rcr subexp
      | Gather(m, subexp) ->           
         (* Penalize gathers of non-delta expressions *)
         add ((if (expr_has_delta_maps subexp) then 1 else 10), 
              0, List.length m.ovars) (rcr subexp) 
      | Repartition(m, subexp, _) -> 
         add (0,
              (* Penalize repartitions of non-delta expressions *)
              (if (expr_has_delta_maps subexp) then 1 else 2), 
              List.length m.ovars) (rcr subexp)
   in
      rcr dexpr

let decompose_graph ((schema, expr_list): (var_t list * dist_expr_t list)):
                    var_t list * (var_t list * dist_expr_t) list =
   let get_vars term = 
      let meta = get_meta_info term in
         ListAsSet.union meta.ivars meta.ovars
   in
      (  schema, 
         List.map (fun term_list ->
            let term = mk_prod term_list in
            let term_schema = (get_meta_info term).ovars in
               ( ListAsSet.inter schema term_schema, term )
         ) (HyperGraph.connected_components get_vars expr_list)
      )

let optimize_expr (dexpr: dist_expr_t): dist_expr_t = 
   Debug.print "LOG-ANNOTM3-OPT" (fun () ->
      "Optimizing expr (START): "^(string_of_expr dexpr) 
   ); 
   let rec rcr dexpr = begin match dexpr with 
      | Sum(meta, sum_list) -> 
         let rec group sl = match sl with
            | [] -> []
            | [x] -> [x]
            | hd::tl ->                
               let grouped_tl = group tl in
               let (lhs, rhs) =                   
                  List.partition (fun grp -> match (hd, grp) with
                     | (Repartition(_, _, p1), Repartition(_, _, p2)) 
                        when p1 = p2 -> true
                     | (Prod(_, Repartition(_, _, p1) :: tl), 
                        Repartition(_, _, p2)) 
                        when p1 = p2 &&
                             get_part_info (mk_prod tl) = None -> true
                     | (Repartition(_, _, p1), 
                        Prod(_, Repartition(_, _, p2) :: tl)) 
                        when p1 = p2 &&
                             get_part_info (mk_prod tl) = None -> true
                     | (Prod(_, Repartition(_, _, p1) :: tl1), 
                        Prod(_, Repartition(_, _, p2) :: tl2)) 
                        when p1 = p2 &&
                             get_part_info (mk_prod tl1) = None &&
                             get_part_info (mk_prod tl2) = None -> true
                     | (Gather _, Gather _) -> true
                     | (Prod(_, Gather(_, _) :: tl), Gather _) 
                        when get_part_info (mk_prod tl) = None -> true
                     | (Gather _, Prod(_, Gather(_, _) :: tl))  
                        when get_part_info (mk_prod tl) = None -> true
                     | (Prod(_, Gather(_, _) :: tl1), 
                        Prod(_, Gather(_, _) :: tl2))  
                        when get_part_info (mk_prod tl1) = None && 
                             get_part_info (mk_prod tl2) = None -> true
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
                  | (Prod(_, Repartition(_, s1, p1) :: tl), 
                     Repartition(_, s2, p2))
                     when p1 = p2 && 
                          get_part_info (mk_prod tl) = None ->
                     (mk_repartition p1 
                        (mk_sum [mk_prod (s1 :: tl); s2]) :: rhs)
                  | (Repartition(_, s1, p1), 
                     Prod(_, Repartition(_, s2, p2) :: tl))
                     when p1 = p2 && 
                          get_part_info (mk_prod tl) = None ->
                     (mk_repartition p1 
                        (mk_sum [s1; mk_prod (s2 :: tl)]) :: rhs)
                  | (Prod(_, Repartition(_, s1, p1) :: tl1), 
                     Prod(_, Repartition(_, s2, p2) :: tl2))
                     when p1 = p2 && 
                          get_part_info (mk_prod tl1) = None &&
                          get_part_info (mk_prod tl2) = None ->
                     (mk_repartition p1 
                        (mk_sum [mk_prod (s1 :: tl1); 
                                 mk_prod (s2 :: tl2)]) :: rhs)

                  | (Gather(_, s1), Gather(_, s2)) -> 
                     (mk_gather (mk_sum [s1; s2]) :: rhs)
                  | (Prod(_, Gather(_, s1) :: tl), Gather(_, s2)) 
                     when get_part_info (mk_prod tl) = None ->
                     (mk_gather (mk_sum [mk_prod (s1 :: tl); s2]) :: rhs)
                  | (Gather(_, s1), Prod(_, Gather(_, s2) :: tl)) 
                     when get_part_info (mk_prod tl) = None ->
                     (mk_gather (mk_sum [s1; mk_prod (s2 :: tl)]) :: rhs)
                  | (Prod(_, Gather(_, s1) :: tl1), 
                     Prod(_, Gather(_, s2) :: tl2)) 
                     when get_part_info (mk_prod tl1) = None && 
                          get_part_info (mk_prod tl2) = None ->
                     (mk_gather (mk_sum [mk_prod (s1 :: tl1);
                                         mk_prod (s2 :: tl2)]) :: rhs)

                  | _ -> failwith "Impossible"
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
      | AggSum(meta, gb_vars, subexp) -> 
         begin match rcr subexp with 
            | Repartition(_, new_subexp, pkeys) -> 
               let new_meta = get_meta_info new_subexp in
               mk_repartition pkeys (AggSum(new_meta, gb_vars, new_subexp))
            | Gather(_, new_subexp) -> 
               let new_meta = get_meta_info new_subexp in
               mk_gather (AggSum(new_meta, gb_vars, new_subexp))
            | new_subexp -> AggSum(meta, gb_vars, new_subexp)
         end
      | DomainDelta(meta, subexp) -> DomainDelta(meta, rcr subexp)      
      | Lift(meta, v, subexp) -> Lift(meta, v, rcr subexp)         
      | Exists(meta, subexp) -> 
         begin match rcr subexp with 
            | Repartition(_, new_subexp, pkeys) ->
               let new_meta = get_meta_info new_subexp in
               mk_repartition pkeys (Exists(new_meta, new_subexp))
            | Gather(_, new_subexp) -> 
               let new_meta = get_meta_info new_subexp in
               mk_gather (Exists(new_meta, new_subexp))
            | new_subexp -> Exists(meta, new_subexp)
         end 
      | Gather(meta, subexp) -> 
         let new_dexpr_1 = mk_gather (rcr subexp) in

         (* Try pushing Gather down the tree and compare the costs *)
         let new_dexpr_2 = 
            let new_meta = {
               part_info = Some(Local);
               ivars = meta.ivars;
               ovars = meta.ovars;
            } in
            begin match subexp with
               | Sum(_, sum_list) -> 
                  rcr (Sum(new_meta, List.map mk_gather sum_list))
               | Prod(m, prod_list) -> 
                  let (_, comps) = decompose_graph (m.ovars, prod_list) in
                  if (List.length comps == 1) then
                     rcr (Prod(new_meta, List.map mk_gather prod_list))
                  else 
                     let fn_g = (fun c -> mk_gather (snd c)) in
                     rcr (Prod(new_meta, List.map fn_g comps))
               | Neg(_, subexp) -> 
                  rcr (Neg(new_meta, mk_gather subexp))
               | Value _ | Cmp _ | Rel _ | DeltaRel _ -> mk_gather dexpr
               | External(_, (en, ei, eo, et, eivc)) -> 
                  begin match eivc with  
                     | Some(subexp) -> 
                        mk_gather (External(new_meta, (en, ei, eo, et, 
                                            Some(rcr (mk_gather subexp)))))
                     | None -> mk_gather dexpr
                  end
               | AggSum(_, gb_vars, subexp) -> 
                  begin match rcr (mk_gather subexp) with 
                     | Gather(_, new_subexp) -> 
                        let new_meta = get_meta_info new_subexp in
                        mk_gather (AggSum(new_meta, gb_vars, new_subexp))
                     | new_subexp -> AggSum(new_meta, gb_vars, new_subexp)
                  end
               | DomainDelta(_, subexp) -> 
                  DomainDelta(new_meta, rcr (mk_gather subexp))
               | Lift(_, v, subexp) -> 
                  Lift(new_meta, v, rcr (mk_gather subexp))
               | Exists(_, subexp) -> 
                  begin match rcr (mk_gather subexp) with 
                     | Gather(_, new_subexp) -> 
                        let new_meta = get_meta_info new_subexp in
                        mk_gather (Exists(new_meta, new_subexp))
                     | new_subexp -> Exists(new_meta, new_subexp)
                  end
               | Gather(_, subexp) -> 
                  rcr (mk_gather subexp)
               | Repartition(_, subexp, pkeys) -> 
                  rcr (mk_gather subexp)
            end
         in
         begin
            let (old_g, old_r, old_numvars) = cost_of_expr new_dexpr_1 in
            let (new_g, new_r, new_numvars) = cost_of_expr new_dexpr_2 in
            let old_cost = old_g + old_r in
            let new_cost = new_g + new_r in
            if ((new_cost < old_cost) || 
                (new_cost == old_cost && new_g < old_g) || 
                (new_cost == old_cost && new_g == old_g && 
                 new_numvars < old_numvars))
            then new_dexpr_2 
            else new_dexpr_1
         end
      | Repartition(meta, subexp, pkeys) -> 
         let new_dexpr_1 = mk_repartition pkeys (rcr subexp) in

         (* Try pushing Repartition down the tree and compare the costs *)
         let new_dexpr_2 = 
            let mk_rep = mk_repartition pkeys in
            let new_meta = {
               part_info = Some(DistributedByKey(pkeys));
               ivars = meta.ivars;
               ovars = meta.ovars;
            } in
            begin match subexp with
               | Sum(_, sum_list) -> 
                  rcr (Sum(new_meta, List.map mk_rep sum_list))
               | Prod(m, prod_list) -> 
                  let (_, comps) = decompose_graph (m.ovars, prod_list) in
                  if (List.length comps == 1) then
                     rcr (Prod(new_meta, List.map mk_rep prod_list))
                  else 
                     let fn_rep = (fun c -> mk_rep (snd c)) in
                     rcr (Prod(new_meta, List.map fn_rep comps))               
               | Neg(_, subexp) -> 
                  rcr (Neg(new_meta, mk_rep subexp))
               | Value _ | Cmp _ | Rel _ | DeltaRel _ -> mk_rep dexpr
               | External(_, (en, ei, eo, et, eivc)) -> 
                  begin match eivc with  
                     | Some(subexp) -> 
                        mk_rep (External(new_meta, (en, ei, eo, et, 
                                         Some(rcr (mk_rep subexp)))))
                     | None -> mk_rep dexpr
                  end
               | AggSum(_, gb_vars, subexp) -> 
                  begin match rcr (mk_rep subexp) with 
                     | Repartition(_, new_subexp, pkeys2) 
                        when ListAsSet.seteq pkeys pkeys2 -> 
                        let new_meta = get_meta_info new_subexp in
                        mk_rep (AggSum(new_meta, gb_vars, new_subexp))
                     | new_subexp -> AggSum(new_meta, gb_vars, new_subexp)
                  end
               | DomainDelta(_, subexp) -> 
                  DomainDelta(new_meta, rcr (mk_rep subexp))
               | Lift(_, v, subexp) -> 
                  Lift(new_meta, v, rcr (mk_rep subexp))
               | Exists(_, subexp) -> 
                  begin match rcr (mk_rep subexp) with 
                     | Repartition(_, new_subexp, pkeys2) 
                        when ListAsSet.seteq pkeys pkeys2 -> 
                        let new_meta = get_meta_info new_subexp in
                        mk_rep (Exists(new_meta, new_subexp))
                     | new_subexp -> Exists(new_meta, new_subexp)
                  end
               | Gather(_, subexp) -> 
                  rcr (mk_rep subexp)
               | Repartition(_, subexp, pkeys) -> 
                  rcr (mk_rep subexp)
            end
         in
         begin
            let (old_g, old_r, old_numvars) = cost_of_expr new_dexpr_1 in
            let (new_g, new_r, new_numvars) = cost_of_expr new_dexpr_2 in
            let old_cost = old_g + old_r in
            let new_cost = new_g + new_r in
            if ((new_cost < old_cost) || 
                (new_cost == old_cost && new_g < old_g) || 
                (new_cost == old_cost && new_g == old_g && 
                 new_numvars < old_numvars))
            then new_dexpr_2 
            else new_dexpr_1
         end
      end
   in 
   let rewritten_dexpr = rcr dexpr in
   begin
      Debug.print "LOG-ANNOTM3-OPT" (fun () ->
         "Optimizing expr (END): "^(string_of_expr rewritten_dexpr) 
      );
      rewritten_dexpr
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
   let lhs_raw = lift part_table stmt.target_map in
   let lhs_name = match lhs_raw with 
      | External(_, (ename, _, _, _, _)) -> ename
      | _ -> failwith "LHS expression not External"
   in
   let lhs = match lhs_raw with 
      | External(meta, (en, ei, eo, et, Some(eivc_raw))) ->
         let eivc = 
            begin match ( get_part_info lhs_raw ) with
               | Some(DistributedByKey(p1)) -> mk_repartition p1 eivc_raw
               | Some(Local) -> mk_gather eivc_raw
               | Some(DistributedRandom) when is_delta_map lhs_name -> eivc_raw
               | _ -> failwith "Wrong partitioning info for LHS target map"
            end
         in 
            External(meta, (en, ei, eo, et, Some(optimize_expr eivc)))
      | _ -> lhs_raw
   in
   let rhs_raw = lift part_table stmt.update_expr in
   let rhs = 
      match ( get_part_info lhs_raw ) with
         | Some(DistributedByKey(p1)) -> mk_repartition p1 rhs_raw
         | Some(Local) -> mk_gather rhs_raw
         | Some(DistributedRandom) when is_delta_map lhs_name -> rhs_raw
         | _ -> failwith "Wrong partitioning info for LHS target map"
   in
   {  target_map  = lhs;
      update_type = stmt.update_type;
      update_expr = optimize_expr rhs; }

let lift_trigger (part_table: part_table_t) 
                 (trigger: trigger_t): dist_trigger_t =
   {  event      = trigger.event;
      statements = ref (List.map (lift_statement part_table) 
                                 !(trigger.statements))  }

let lift_prog (part_table: part_table_t) (prog: prog_t): dist_prog_t =    
   (* Extract maps in program that reference only input deltas  *)
   List.iter (fun map -> match map with
      | DSView(view) -> 
         begin match view.ds_name with 
            | CalcRing.Val(External(ename, _, _, _, _)) 
              when (C.deltarels_of_expr view.ds_definition <> [] &&
                    C.rels_of_expr view.ds_definition = []) ->
                 delta_maps := ename :: !delta_maps
            | _ -> ()
         end         
      | _ -> ()
   ) !(prog.maps); 
   {  queries  = ref !(prog.queries);
      maps     = ref !(prog.maps);
      triggers = ref (List.map (lift_trigger part_table) 
                               !(prog.triggers));
      db       = prog.db   }



(** STRINGIFICATION OPERATIONS *)

(** Stringify a statement.  Conforms to the grammar of Calculusparser *)
let string_of_statement (dstmt: dist_stmt_t): string = 
   let string_of_cost (a,b,c) = 
      "(" ^ ListExtras.string_of_list ~sep:", " string_of_int [a; b; c] ^ ")" 
   in
   let rhs_string = string_of_expr dstmt.update_expr in
      (string_of_expr dstmt.target_map)^
      (if dstmt.update_type = UpdateStmt then " += " else " := ")^
      (if String.contains rhs_string '\n' then "\n  " else "")^
      rhs_string ^
      (if not(Debug.active "PRINT-ANNOTATED-M3-COSTS") then ""
       else (" @Cost = " ^ string_of_cost (cost_of_expr dstmt.update_expr)))

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
let string_of_map (part_table: part_table_t) (map:map_t): string = 
   begin match map with
      | DSView(view) -> 
         "DECLARE MAP "^
         (CalculusPrinter.string_of_expr ~show_type:true view.ds_name)^
         " := \n"^
         (CalculusPrinter.string_of_expr view.ds_definition)^
         (  
            (* Stringify partitioning information *)
            let name = match view.ds_name with
               | CalcRing.Val(External(name, _, _, _, _)) -> name
               | _ -> failwith "LHS map is not of external type." 
            in         
            begin match (Hashtbl.find part_table name) with
              | Local -> ""
              | DistributedRandom -> " PARTITIONED RANDOMLY"
              | DistributedByKey(indexes) -> 
                 let ovars = snd (schema_of_expr view.ds_name) in
                 let pkeys = List.map (List.nth ovars) indexes in
                 (" PARTITIONED BY [" ^
                     ListExtras.string_of_list ~sep:", "
                        (string_of_var ~verbose:true) pkeys ^ "]")
            end
         )^";"
      | DSTable(rel) -> Schema.code_of_rel rel
   end

let code_of_schema (part_table: part_table_t) (sch: Schema.t): string =
   ListExtras.string_of_list ~sep:"\n\n" (fun (source, rels) ->
      let source_string = Schema.code_of_source source in
         ListExtras.string_of_list ~sep:"\n\n" (fun (adaptor,rel) ->
            (Schema.code_of_rel rel)^"\n  "^source_string^"\n  "^
            (Schema.code_of_adaptor adaptor)^
            (  
               (* Stringify partitioning information *)               
               let (name, ovars) = match rel with  
                  | (name, ovars, Schema.StreamRel) -> ("DELTA_" ^ name, ovars)
                  | (name, ovars, Schema.TableRel) -> (name, ovars)
               in
               begin match (Hashtbl.find part_table name) with
                 | Local -> ""
                 | DistributedRandom -> " PARTITIONED RANDOMLY"
                 | DistributedByKey(indexes) -> 
                    let pkeys = List.map (List.nth ovars) indexes in
                    (" PARTITIONED BY [" ^
                        ListExtras.string_of_list ~sep:", "
                           (string_of_var ~verbose:true) pkeys ^ "]")
               end
            )^";"
         ) rels
   ) !sch

(**
   [string_of_prog prog]
   
   Generate the Calculusparser compatible string representation of an M3 
   program.
   @param prog  An M3 program
   @return      The (Calculusparser-compatible) string representation of [prog]
*)
let string_of_prog (part_table: part_table_t) (dprog: dist_prog_t): string = 
   "-------------------- SOURCES --------------------\n"^
   (code_of_schema part_table dprog.db)^"\n\n"^
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
