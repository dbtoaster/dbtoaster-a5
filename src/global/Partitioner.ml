open Type
open Arithmetic
open Calculus
open M3

(** Provide hard-coded partitioning information for TPC-H queries *)
type ('key_t) gen_part_info_t =
  | Local                         (* local result *)
  | DistributedRandom             (* randomly partitioned result *)
  | DistributedByKey of 'key_t list  (* key-partitioned result *)

type ('key_t) gen_part_table_t = (string, ('key_t) gen_part_info_t) Hashtbl.t

type part_info_t = (var_t) gen_part_info_t

type card_part_info_t = (var_t * int) gen_part_info_t

type part_table_t = (int) gen_part_table_t

let create_hash_table (pairs: ('k * 'v) list) =
   let hash_table = Hashtbl.create (List.length pairs) in
   List.iter (fun (k, v) -> Hashtbl.add hash_table k v) pairs;
   hash_table

let string_of_part_info (part_info: ('key_t) gen_part_info_t)
                        (string_fn: 'key_t -> string): string =
   if not (Debug.active "PRINT-PARTITION-INFO") then ""
   else match part_info with
      | Local -> "<Local>"
      | DistributedRandom -> "<DistRandom>"
      | DistributedByKey(pkeys) ->
         "<DistByKey(" ^
            (ListExtras.string_of_list ~sep:", " string_fn pkeys) ^ ")>"

(*
   For each relation, keeps *possible* partitioning attributes
   together with their approx. cardinalities

   higher cardinality => higher chances to partition on that attribute

   Example:
      LINEITEM can be key-partitioned on the 0th attribute (orderkey),
      1st attribute (partkey), or 2nd attribute (suppkey), see below.
      The associated cardinalities serve to guide the partitioning strategy
      for other maps built over LINEITEM -- in such cases, the attribute
      with the max estimated cardinality is used as the key for partitioning.

      DELTA_LINEITEM can be either a distributed collection with
      no partitioning key (DistributedRandom) or a local collection (Local).

      All delta relations must be either all distributed or all local.
*)
let relation_part_options: (string * (int * int) gen_part_info_t) list =
   [
      ("LINEITEM", DistributedByKey([(0, 1500000); (1, 200000); (2, 10000)]));
      ("ORDERS", DistributedByKey([(0, 1500000); (1, 150000)]));
      ("CUSTOMER", DistributedByKey([(0, 150000)]));
      ("PART", DistributedByKey([(0, 200000); (3, 1000)]));
      ("SUPPLIER", DistributedByKey([(0, 10000)]));
      ("PARTSUPP", DistributedByKey([(0, 200000); (1, 10000)]));
      ("NATION", DistributedByKey([]));
      ("REGION", DistributedByKey([]));
      ("DELTA_LINEITEM", DistributedRandom);
      ("DELTA_ORDERS", DistributedRandom);
      ("DELTA_CUSTOMER", DistributedRandom);
      ("DELTA_PART", DistributedRandom);
      ("DELTA_SUPPLIER", DistributedRandom);
      ("DELTA_PARTSUPP", DistributedRandom);

      ("R", DistributedByKey([(1, 200)]));
      ("S", DistributedByKey([(1, 2000)]));
      ("T", DistributedByKey([(0, 100)]));
      ("DELTA_R", DistributedRandom);
      ("DELTA_S", DistributedRandom);
      ("DELTA_T", DistributedRandom);

      (* ("DELTA_R", Local); *)
      (* ("DELTA_S", Local); *)
      (* ("DELTA_T", Local); *)

      ("EMPLOYEE", DistributedByKey([(0, 1000)]));
      ("DELTA_EMPLOYEE", DistributedRandom);
   ]

let ht_part_options: (int * int) gen_part_table_t =
   let hash_table = Hashtbl.create 20 in
   List.iter (fun (k, v) -> Hashtbl.add hash_table k v) relation_part_options;
   hash_table

let rec compute_card_part_info (expr: expr_t): card_part_info_t option =
   let sum_fn (p1: card_part_info_t option) (p2: card_part_info_t option): card_part_info_t option =
      begin match (p1, p2) with
         | (None, p) | (p, None) -> p
         | (Some(DistributedByKey([])), Some(DistributedByKey(k2))) -> p2  (* could default to Local *)
         | (Some(DistributedByKey(k1)), Some(DistributedByKey([]))) -> p1  (* could default to Local *)
         | (Some(DistributedByKey(k1)), Some(DistributedByKey(k2))) ->
            let common_options = ListAsSet.inter k1 k2 in
            if (List.length common_options = 0) then Some(DistributedRandom)
            else Some(DistributedByKey(common_options))

         | (Some(DistributedByKey([])), Some(Local))
         | (Some(Local), Some(DistributedByKey([]))) -> Some(Local)
         | (Some(DistributedByKey(k1)), Some(Local)) -> p1          (* could default to Local *)
         | (Some(Local), Some(DistributedByKey(k2))) -> p2          (* could default to Local *)

         | (Some(DistributedByKey([])), Some(DistributedRandom))
         | (Some(DistributedRandom), Some(DistributedByKey([]))) -> Some(DistributedByKey([]))
         | (Some(DistributedByKey(k1)), Some(DistributedRandom)) -> p1     (* could default to Local *)
         | (Some(DistributedRandom), Some(DistributedByKey(k2))) -> p2     (* could default to Local *)

         | (Some(Local), Some(DistributedRandom))
         | (Some(DistributedRandom), Some(Local))
         | (Some(DistributedRandom), Some(DistributedRandom)) -> Some(DistributedRandom)
         | (Some(Local), Some(Local)) -> Some(Local)
      end
   in
   let prod_fn (p1: card_part_info_t option) (p2: card_part_info_t option) : card_part_info_t option =
      begin match (p1, p2) with
         | (None, p) | (p, None) -> p
         | (Some(DistributedByKey([])), Some(DistributedByKey(k2))) -> p2
         | (Some(DistributedByKey(k1)), Some(DistributedByKey([]))) -> p1
         | (Some(DistributedByKey(k1)), Some(DistributedByKey(k2))) ->
            Some(DistributedByKey(ListAsSet.union k1 k2))

         | (Some(DistributedByKey([])), Some(Local))
         | (Some(Local), Some(DistributedByKey([]))) -> Some(Local)
         | (Some(DistributedByKey(k1)), Some(Local)) -> p1
         | (Some(Local), Some(DistributedByKey(k2))) -> p2

         | (Some(DistributedByKey([])), Some(DistributedRandom))
         | (Some(DistributedRandom), Some(DistributedByKey([]))) -> Some(DistributedRandom)
         | (Some(DistributedByKey(k1)), Some(DistributedRandom)) -> p1
         | (Some(DistributedRandom), Some(DistributedByKey(k2))) -> p2

         | (Some(Local), Some(DistributedRandom))
         | (Some(DistributedRandom), Some(Local))
         | (Some(DistributedRandom), Some(DistributedRandom)) (* should never happen *)
         | (Some(Local), Some(Local)) -> Some(Local)
      end
   in
   let rel_fn (name: string) (vars: var_t list): card_part_info_t option =
      try
         begin match Hashtbl.find ht_part_options (String.uppercase_ascii name) with
            | Local -> Some(Local)
            | DistributedRandom -> Some(DistributedRandom)
            | DistributedByKey(ks) ->
               let ks_vars = List.map (fun (idx, c) -> (List.nth vars idx, c)) ks
               in Some(DistributedByKey(ks_vars))
         end
      with Not_found -> failwith ("Partitioning information not found for " ^ name)
   in
   fold ~scope:[]
      (fun _ sl -> List.fold_left sum_fn (List.hd sl) (List.tl sl))
      (fun _ pl -> List.fold_left prod_fn (List.hd pl) (List.tl pl))
      (fun _ x -> x)
      (fun (scope, _) lf -> begin match lf with
         | Value _ -> None
         | External _ -> None
         | AggSum(gb_vars, subexp) ->
            begin match compute_card_part_info subexp with
               | None -> None
               | Some(Local) -> Some(Local)
               | Some(DistributedRandom) -> Some(DistributedRandom)
               | Some(DistributedByKey([])) -> Some(DistributedByKey([]))
               | Some(DistributedByKey(ks)) ->
                  (* Find partitioning options for group-by variables *)
                  let common_options =
                     (* Lift expressions of the form (v1 ^= v2) when v1 is in gb_vars
                        lead to losing partitioning information. The following hack
                        tries to fix that here (although this is not the right place
                        to do so as such lift expressions should have been unified. *)
                     let identities =
                        let lift_mappings =
                           List.flatten (List.map (function
                              | CalcRing.Val(Lift(v1,
                                    CalcRing.Val(Value(ValueRing.Val(AVar(v2))))))
                                 when v1 != v2 -> [(v2, v1)]
                              | _ -> []
                           ) (CalcRing.prod_list subexp))
                        in
                           ListExtras.transitive_closure lift_mappings
                     in
                        List.flatten (List.map (fun (key, card) ->
                           List.flatten (List.map (fun gb_var ->
                              if (key = gb_var) || (List.exists (fun (v1, v2) ->
                                    v1 = key && v2 = gb_var) identities)
                              then [(gb_var, card)]
                              else []
                           ) gb_vars)
                        ) ks)
                  in
                     if (List.length common_options > 0) then
                        Some(DistributedByKey(common_options))
                     else if (List.length gb_vars > 0) then
                        Some(DistributedRandom)
                     else
                        Some(Local)
            end
         | Rel(n, vars) -> rel_fn n vars
         | DeltaRel(n, vars) -> rel_fn ("DELTA_" ^ n) vars
         | DomainDelta(subexp) -> compute_card_part_info subexp
         | Cmp _ -> None
         | CmpOrList _ -> None
         | Lift _ -> None
(***** BEGIN EXISTS HACK *****)
         | Exists(subexp) -> compute_card_part_info subexp
(***** END EXISTS HACK *****)
      end)
      expr

let is_incrementally_maintained (map_name: string) (prog: prog_t): bool =
   List.exists (fun trigger ->
      List.exists (fun (stmt: Plan.stmt_t) ->
         match stmt.target_map with
            | CalcRing.Val(External(n, _, _, _, _)) -> 
                (map_name = n) && (stmt.update_type = UpdateStmt)
            | _ -> false
      ) !(trigger.statements)
   ) !(prog.triggers)

let get_part_table (prog: prog_t) =
   let max_card_variable (p: card_part_info_t): part_info_t = match p with
      | Local -> Local
      | DistributedRandom -> DistributedRandom
      | DistributedByKey([]) -> DistributedByKey([])
      | DistributedByKey(hd :: tl) ->
         let (key, card) =
            List.fold_left (fun (acc_key, acc_card) (x_key, x_card) ->
               if (acc_card < x_card) then (x_key, x_card)
               else (acc_key, acc_card)
            ) hd tl
         in
            DistributedByKey([key])
   in
   let ht_maps_part = Hashtbl.create (List.length !(prog.maps)) in
   begin
      Hashtbl.iter (fun k v ->
         begin match v with
            | Local ->
               Hashtbl.add ht_maps_part k Local
            | DistributedRandom ->
               Hashtbl.add ht_maps_part k DistributedRandom
            | DistributedByKey(pk) ->
               Hashtbl.add ht_maps_part k (DistributedByKey(List.map (fst) pk))
         end
      ) ht_part_options;

      List.iter (function
         | DSView(view) ->
            let (name, ovars) = match view.ds_name with
               | CalcRing.Val(External(name, _, ovars, _, _)) -> (name, ovars)
               | _ -> failwith "LHS map is not of external type."
            in
            let part_info = match compute_card_part_info view.ds_definition with
               | Some(p) ->
                  begin match max_card_variable p with
                     | Local -> Local
                     | DistributedRandom -> 
                        (* When RHS has locality DistRandom and LHS is incrementally 
                           maintained, then LHS must have either Local or DistByKey;
                           otherwise, the += operator cannot be applied. We opt for 
                           Local locality here.

                           Otherwise, the locality can remain DistributedRandom.
                         *)
                        if (is_incrementally_maintained name prog)
                        then Local 
                        else DistributedRandom

                     | DistributedByKey(ks) ->
                        let ks_idx =
                           List.map (fun v -> ListExtras.index_of v ovars) ks
                        in DistributedByKey(ks_idx)
                  end
               | None -> Local
            in
               Hashtbl.add ht_maps_part name part_info
         | DSTable(rel) -> ()
      ) !(prog.maps);

      ht_maps_part
   end