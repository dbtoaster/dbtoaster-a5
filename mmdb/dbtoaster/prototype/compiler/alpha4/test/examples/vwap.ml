open M3;;
open M3Common;;
open M3Common.Patterns;;
open M3OCaml;;

open StandardAdaptors;;
open Util;;
StandardAdaptors.initialize();;
let db = Database.make_empty_db 
   [("INPUT_MAP_BIDS", [], [VT_Float;VT_Int]);("QUERY_1_1", [], []);("QUERY_1_1__3", [], []);("QUERY_1_1__2", [VT_Float], []);("QUERY_1_1__1", [], [VT_Float])]
   [("QUERY_1_1__2", [In([],[])]);("QUERY_1_1__1", [Out([],[])]);("INPUT_MAP_BIDS", [Out([],[])])];;


let on_ins_BIDS = (fun tuple -> 
   let var_QUERY_1_1__1BIDS_B1__PRICE = List.nth tuple 0 in 
   let var_QUERY_1_1__1BIDS_B1__VOLUME = List.nth tuple 1 in 
   (*** Update QUERY_1_1__1 ***)
   let lhs_map = Database.get_map "QUERY_1_1__1" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [var_QUERY_1_1__1BIDS_B1__PRICE] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            match current_singleton with
             | [] -> 
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [var_QUERY_1_1__1BIDS_B1__PRICE] (List.hd [(c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__PRICE]) (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]))])]
             | [current_v] -> [c_sum current_v (List.hd [(c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__PRICE]) (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]))])]
             | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1__1" [[]] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1__2 ***)
   let lhs_map = Database.get_map "QUERY_1_1__2" db in
   let iifilter inv_img = 
      let var_B1__PRICE = List.nth inv_img 0 in 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            match current_singleton with
             | [] -> 
                  let delta_k = [] in
                  let iv = 
                        (fun _ v ->
                        (* 
                         [] v; *)
                        let iv = 
                           let slice0 = 
                              let res1 = 
                                 let res1 = 
                                    let slice =
                                       let m = Database.get_map "INPUT_MAP_BIDS" db in
                                       let inv_img = [] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                    in ValuationMap.strip_indexes slice
                                 in
                                 let f k v1 r =
                                    let r2 = 
                                    ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                    in
                                    let r3 = 
                                       ValuationMap.mapi (fun k2 v2 -> 
                                          ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                       ) r2
                                    in ValuationMap.union r r3
                                 in ValuationMap.fold f (ValuationMap.empty_map()) res1
                              in
                              let f k v1 r =
                                 let r2 = 
                                    let r = 
                                    ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                    in ValuationMap.mapi (fun k2 v2 ->
                                       ([var_B1__PRICE;List.nth k2 0], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) (List.hd [var_B1__PRICE]) v2))) r
                                 in
                                 let r3 = 
                                    ValuationMap.mapi (fun k2 v2 -> 
                                       ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                    ) r2
                                 in ValuationMap.union r r3
                              in ValuationMap.fold f (ValuationMap.empty_map()) res1
                           in
                           (* 
                            slice0; *)
                           [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                        in
                        match iv with
                         | [] -> v
                         | [init_v] -> c_sum v init_v
                         | _ -> failwith "singleton_init: invalid singleton")
                    delta_k (List.hd [(c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) (List.hd [((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) (List.hd [((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) (List.hd [var_B1__PRICE]) (List.hd [var_B1__PRICE]))]))]))])
                  in [iv]
             | [current_v] -> [c_sum current_v (List.hd [(c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) (List.hd [((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) (List.hd [((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) (List.hd [var_B1__PRICE]) (List.hd [var_B1__PRICE]))]))]))])]
             | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1__2" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1__3 ***)
   let lhs_map = Database.get_map "QUERY_1_1__3" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            match current_singleton with
             | [] -> 
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [] (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME])]
             | [current_v] -> [c_sum current_v (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME])]
             | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1__3" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1 ***)
   let lhs_map = Database.get_map "QUERY_1_1" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            let delta_slice = 
               let slice0 = 
                  let res1 = 
                     let res1 = 
                        let res1 = 
                           let res1 = 
                           ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                           in
                           let f k v1 r =
                              let r2 = 
                                 let res1 = 
                                 ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                 in
                                 let f k v1 r =
                                    let r2 = 
                                    ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                    in
                                    let r3 = 
                                       ValuationMap.mapi (fun k2 v2 -> 
                                          ([List.nth k2 0], ((fun a b -> if ((=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                       ) r2
                                    in ValuationMap.union r r3
                                 in ValuationMap.fold f (ValuationMap.empty_map()) res1
                              in
                              let r3 = 
                                 ValuationMap.mapi (fun k2 v2 -> 
                                    ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                 ) r2
                              in ValuationMap.union r r3
                           in ValuationMap.fold f (ValuationMap.empty_map()) res1
                        in
                        let k2 = [var_QUERY_1_1__1BIDS_B1__VOLUME] in
                        ValuationMap.mapi (fun k v -> (k@k2, (c_prod v (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME])))) res1
                     in
                     let f k v r = 
                        let res2 = 
                           let (r1,r2) = ((
                              let r = 
                                 let r = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__3" db in
                                       let inv_img = [] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                    in
                                    let outv_img = [] in
                                       if ValuationMap.mem outv_img slice
                                       then [ValuationMap.find outv_img slice] else []
                                 in match r with
                                  | [] -> []
                                  | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                                  | _ -> failwith "op_singleton_expr: invalid singleton"
                              in match r with
                               | [] -> []
                               | [v] -> [(c_sum v (List.hd [(c_prod (List.hd [CFloat(0.8)]) (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]))]))]
                               | _ -> failwith "op_singleton_expr: invalid singleton"
                           ),(
                              let r = 
                                 let r = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__3" db in
                                       let inv_img = [] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                    in
                                    let outv_img = [] in
                                       if ValuationMap.mem outv_img slice
                                       then [ValuationMap.find outv_img slice] else []
                                 in match r with
                                  | [] -> []
                                  | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                                  | _ -> failwith "op_singleton_expr: invalid singleton"
                              in match r with
                               | [] -> []
                               | [v] -> [(c_sum v (List.hd [(c_prod (List.hd [CFloat(0.8)]) (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]))]))]
                               | _ -> failwith "op_singleton_expr: invalid singleton"
                           )) in
                           match (r1,r2) with
                            | ([], _) | (_,[]) -> []
                            | ([v1], [v2]) -> [((fun a b -> if ((<=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2)]
                            | _ -> failwith "op_singleton_expr: invalid singleton"
                        in begin match res2 with
                         | [] -> r
                         | [v2] ->
                            let nv = ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v v2) in
                            let nk = [var_B1__PRICE;var_QUERY_1_1__1BIDS_B1__VOLUME]
                            in ValuationMap.add nk nv r
                         | _ -> failwith "op_lslice_expr: invalid singleton"
                        end
                     in ValuationMap.fold f (ValuationMap.empty_map()) res1
                  in
                  let f k v1 r =
                     let r2 = 
                        let r = 
                           let r = 
                              let res1 = 
                                 let slice =
                                    let m = Database.get_map "QUERY_1_1__2" db in
                                    let inv_img = [var_B1__PRICE] in
                                       if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                       else 
                                          let init_val = 
                                             let slice0 = 
                                                let res1 = 
                                                   let res1 = 
                                                      let slice =
                                                         let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                         let inv_img = [] in
                                                            if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                            else 
                                                               failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                      in ValuationMap.strip_indexes slice
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                      ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                let f k v1 r =
                                                   let r2 = 
                                                      let res1 = 
                                                      ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                      in
                                                      let res2 = 
                                                      ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                      in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                   in
                                                   let r3 = 
                                                      ValuationMap.mapi (fun k2 v2 -> 
                                                         ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                      ) r2
                                                   in ValuationMap.union r r3
                                                in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                             in
                                             (* 
                                              slice0; *)
                                             [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                          in begin match init_val with
                                           | [] -> ValuationMap.empty_map()
                                           | [v] -> 
                                                (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                   ValuationMap.from_list [([], v)] [])
                                           | _ -> failwith "MapAccess: invalid singleton"
                                          end
                                 in ValuationMap.strip_indexes slice
                              in
                              let f k v1 r =
                                 let r2 = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__2" db in
                                       let inv_img = [var_B1__PRICE] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             let init_val = 
                                                let slice0 = 
                                                   let res1 = 
                                                      let res1 = 
                                                         let slice =
                                                            let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                            let inv_img = [] in
                                                               if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                               else 
                                                                  failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                         in ValuationMap.strip_indexes slice
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                         ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                         let res1 = 
                                                         ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                         in
                                                         let res2 = 
                                                         ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                         in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                (* 
                                                 slice0; *)
                                                [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                             in begin match init_val with
                                              | [] -> ValuationMap.empty_map()
                                              | [v] -> 
                                                   (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                      ValuationMap.from_list [([], v)] [])
                                              | _ -> failwith "MapAccess: invalid singleton"
                                             end
                                    in ValuationMap.strip_indexes slice
                                 in
                                 let r3 = 
                                    ValuationMap.mapi (fun k2 v2 -> 
                                       ([], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                    ) r2
                                 in ValuationMap.union r r3
                              in ValuationMap.fold f (ValuationMap.empty_map()) res1
                           in ValuationMap.mapi (fun k2 v2 ->
                              ([], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                        in ValuationMap.mapi (fun k2 v2 ->
                           ([], (c_prod (List.hd [CFloat(-1.)]) v2))) r
                     in
                     let r3 = 
                        ValuationMap.mapi (fun k2 v2 -> 
                           ([var_B1__PRICE;var_QUERY_1_1__1BIDS_B1__VOLUME], (c_prod v1 v2))
                        ) r2
                     in ValuationMap.union r r3
                  in ValuationMap.fold f (ValuationMap.empty_map()) res1
               in
               (* 
                slice0; *)
               [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
            in match (current_singleton, delta_slice) with
               | (s, []) -> s
               | ([], [delta_v]) ->
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [] delta_v]
               | ([current_v], [delta_v]) -> [c_sum current_v delta_v]
               | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1 ***)
   let lhs_map = Database.get_map "QUERY_1_1" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            let delta_slice = 
               let slice0 = 
                  let res1 = 
                     let res1 = 
                        let res1 = 
                           let res1 = 
                           ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                           in
                           let f k v1 r =
                              let r2 = 
                                 let res1 = 
                                 ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                 in
                                 let f k v1 r =
                                    let r2 = 
                                    ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                    in
                                    let r3 = 
                                       ValuationMap.mapi (fun k2 v2 -> 
                                          ([List.nth k2 0], ((fun a b -> if ((=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                       ) r2
                                    in ValuationMap.union r r3
                                 in ValuationMap.fold f (ValuationMap.empty_map()) res1
                              in
                              let r3 = 
                                 ValuationMap.mapi (fun k2 v2 -> 
                                    ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                 ) r2
                              in ValuationMap.union r r3
                           in ValuationMap.fold f (ValuationMap.empty_map()) res1
                        in
                        let k2 = [var_QUERY_1_1__1BIDS_B1__VOLUME] in
                        ValuationMap.mapi (fun k v -> (k@k2, (c_prod v (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME])))) res1
                     in
                     let f k v1 r =
                        let r2 = 
                           let res1 = 
                              let res1 = 
                                 let slice =
                                    let m = Database.get_map "QUERY_1_1__2" db in
                                    let inv_img = [var_B1__PRICE] in
                                       if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                       else 
                                          let init_val = 
                                             let slice0 = 
                                                let res1 = 
                                                   let res1 = 
                                                      let slice =
                                                         let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                         let inv_img = [] in
                                                            if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                            else 
                                                               failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                      in ValuationMap.strip_indexes slice
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                      ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                let f k v1 r =
                                                   let r2 = 
                                                      let res1 = 
                                                      ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                      in
                                                      let res2 = 
                                                      ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                      in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                   in
                                                   let r3 = 
                                                      ValuationMap.mapi (fun k2 v2 -> 
                                                         ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                      ) r2
                                                   in ValuationMap.union r r3
                                                in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                             in
                                             (* 
                                              slice0; *)
                                             [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                          in begin match init_val with
                                           | [] -> ValuationMap.empty_map()
                                           | [v] -> 
                                                (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                   ValuationMap.from_list [([], v)] [])
                                           | _ -> failwith "MapAccess: invalid singleton"
                                          end
                                 in ValuationMap.strip_indexes slice
                              in
                              let f k v1 r =
                                 let r2 = 
                                    let r = 
                                       let r = 
                                          let res1 = 
                                          ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                          in
                                          let f k v1 r =
                                             let r2 = 
                                             ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                             in
                                             let r3 = 
                                                ValuationMap.mapi (fun k2 v2 -> 
                                                   ([List.nth k2 0], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                                ) r2
                                             in ValuationMap.union r r3
                                          in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                       in ValuationMap.mapi (fun k2 v2 ->
                                          ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                                    in ValuationMap.mapi (fun k2 v2 ->
                                       ([var_QUERY_1_1__1BIDS_B1__VOLUME;List.nth k2 0], (c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) v2))) r
                                 in
                                 let r3 = 
                                    ValuationMap.mapi (fun k2 v2 -> 
                                       ([List.nth k2 0;List.nth k2 1], (c_sum v1 v2))
                                    ) r2
                                 in ValuationMap.union r r3
                              in ValuationMap.fold f (ValuationMap.empty_map()) res1
                           in
                           let f k v1 r =
                              let r2 = 
                                 let res1 = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__2" db in
                                       let inv_img = [var_B1__PRICE] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             let init_val = 
                                                let slice0 = 
                                                   let res1 = 
                                                      let res1 = 
                                                         let slice =
                                                            let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                            let inv_img = [] in
                                                               if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                               else 
                                                                  failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                         in ValuationMap.strip_indexes slice
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                         ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                         let res1 = 
                                                         ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                         in
                                                         let res2 = 
                                                         ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                         in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                (* 
                                                 slice0; *)
                                                [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                             in begin match init_val with
                                              | [] -> ValuationMap.empty_map()
                                              | [v] -> 
                                                   (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                      ValuationMap.from_list [([], v)] [])
                                              | _ -> failwith "MapAccess: invalid singleton"
                                             end
                                    in ValuationMap.strip_indexes slice
                                 in
                                 let f k v1 r =
                                    let r2 = 
                                       let r = 
                                          let r = 
                                             let res1 = 
                                             ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                             in
                                             let f k v1 r =
                                                let r2 = 
                                                ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                in
                                                let r3 = 
                                                   ValuationMap.mapi (fun k2 v2 -> 
                                                      ([List.nth k2 0], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                                   ) r2
                                                in ValuationMap.union r r3
                                             in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                          in ValuationMap.mapi (fun k2 v2 ->
                                             ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                                       in ValuationMap.mapi (fun k2 v2 ->
                                          ([var_QUERY_1_1__1BIDS_B1__VOLUME;List.nth k2 0], (c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) v2))) r
                                    in
                                    let r3 = 
                                       ValuationMap.mapi (fun k2 v2 -> 
                                          ([List.nth k2 0;List.nth k2 1], (c_sum v1 v2))
                                       ) r2
                                    in ValuationMap.union r r3
                                 in ValuationMap.fold f (ValuationMap.empty_map()) res1
                              in
                              let r3 = 
                                 ValuationMap.mapi (fun k2 v2 -> 
                                    ([List.nth k2 0;List.nth k2 1], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                 ) r2
                              in ValuationMap.union r r3
                           in ValuationMap.fold f (ValuationMap.empty_map()) res1
                        in
                        let r3 = 
                           ValuationMap.mapi (fun k2 v2 -> 
                              ([List.nth k2 1;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                           ) r2
                        in ValuationMap.union r r3
                     in ValuationMap.fold f (ValuationMap.empty_map()) res1
                  in
                  let k2 = [] in
                  let res2 = 
                     let r = 
                        let (r1,r2) = ((
                           let r = 
                              let slice =
                                 let m = Database.get_map "QUERY_1_1__3" db in
                                 let inv_img = [] in
                                    if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                    else 
                                       failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                              in
                              let outv_img = [] in
                                 if ValuationMap.mem outv_img slice
                                 then [ValuationMap.find outv_img slice] else []
                           in match r with
                            | [] -> []
                            | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                            | _ -> failwith "op_singleton_expr: invalid singleton"
                        ),(
                           let r = 
                              let slice =
                                 let m = Database.get_map "QUERY_1_1__3" db in
                                 let inv_img = [] in
                                    if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                    else 
                                       failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                              in
                              let outv_img = [] in
                                 if ValuationMap.mem outv_img slice
                                 then [ValuationMap.find outv_img slice] else []
                           in match r with
                            | [] -> []
                            | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                            | _ -> failwith "op_singleton_expr: invalid singleton"
                        )) in
                        match (r1,r2) with
                         | ([], _) | (_,[]) -> []
                         | ([v1], [v2]) -> [((fun a b -> if ((<=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2)]
                         | _ -> failwith "op_singleton_expr: invalid singleton"
                     in match r with
                      | [] -> []
                      | [v] -> [((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v (List.hd [CFloat(1.)]))]
                      | _ -> failwith "op_singleton_expr: invalid singleton"
                  in begin match res2 with
                      | [] ->  ValuationMap.empty_map()
                      | [v2] -> ValuationMap.mapi (fun k v -> (k@k2, (c_prod v v2))) res1
                      | _ -> failwith "op_lslice_product_expr: invalid singleton"
                     end
               in
               (* 
                slice0; *)
               [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
            in match (current_singleton, delta_slice) with
               | (s, []) -> s
               | ([], [delta_v]) ->
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [] delta_v]
               | ([current_v], [delta_v]) -> [c_sum current_v delta_v]
               | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1 ***)
   let lhs_map = Database.get_map "QUERY_1_1" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            let delta_slice = 
               let slice0 = 
                  let res1 = 
                     let slice =
                        let m = Database.get_map "QUERY_1_1__1" db in
                        let inv_img = [] in
                           if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                           else 
                              failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                     in ValuationMap.strip_indexes slice
                  in
                  let f k v1 r =
                     let r2 = 
                        let r = 
                           let res1 = 
                              let r = 
                                 let res1 = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__2" db in
                                       let inv_img = [var_B1__PRICE] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             let init_val = 
                                                let slice0 = 
                                                   let res1 = 
                                                      let res1 = 
                                                         let slice =
                                                            let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                            let inv_img = [] in
                                                               if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                               else 
                                                                  failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                         in ValuationMap.strip_indexes slice
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                         ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                         let res1 = 
                                                         ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                         in
                                                         let res2 = 
                                                         ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                         in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                (* 
                                                 slice0; *)
                                                [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                             in begin match init_val with
                                              | [] -> ValuationMap.empty_map()
                                              | [v] -> 
                                                   (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                      ValuationMap.from_list [([], v)] [])
                                              | _ -> failwith "MapAccess: invalid singleton"
                                             end
                                    in ValuationMap.strip_indexes slice
                                 in
                                 let f k v1 r =
                                    let r2 = 
                                       let slice =
                                          let m = Database.get_map "QUERY_1_1__2" db in
                                          let inv_img = [var_B1__PRICE] in
                                             if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                             else 
                                                let init_val = 
                                                   let slice0 = 
                                                      let res1 = 
                                                         let res1 = 
                                                            let slice =
                                                               let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                               let inv_img = [] in
                                                                  if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                                  else 
                                                                     failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                            in ValuationMap.strip_indexes slice
                                                         in
                                                         let f k v1 r =
                                                            let r2 = 
                                                            ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                            in
                                                            let r3 = 
                                                               ValuationMap.mapi (fun k2 v2 -> 
                                                                  ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                               ) r2
                                                            in ValuationMap.union r r3
                                                         in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                            let res1 = 
                                                            ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                            in
                                                            let res2 = 
                                                            ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                            in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   (* 
                                                    slice0; *)
                                                   [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                                in begin match init_val with
                                                 | [] -> ValuationMap.empty_map()
                                                 | [v] -> 
                                                      (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                         ValuationMap.from_list [([], v)] [])
                                                 | _ -> failwith "MapAccess: invalid singleton"
                                                end
                                       in ValuationMap.strip_indexes slice
                                    in
                                    let r3 = 
                                       ValuationMap.mapi (fun k2 v2 -> 
                                          ([], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                       ) r2
                                    in ValuationMap.union r r3
                                 in ValuationMap.fold f (ValuationMap.empty_map()) res1
                              in ValuationMap.mapi (fun k2 v2 ->
                                 ([], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                           in
                           let k2 = [var_QUERY_1_1__1BIDS_B1__VOLUME] in
                           let res2 = 
                              let r = 
                                 let (r1,r2) = ((
                                    let r = 
                                       let r = 
                                          let slice =
                                             let m = Database.get_map "QUERY_1_1__3" db in
                                             let inv_img = [] in
                                                if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                else 
                                                   failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                          in
                                          let outv_img = [] in
                                             if ValuationMap.mem outv_img slice
                                             then [ValuationMap.find outv_img slice] else []
                                       in match r with
                                        | [] -> []
                                        | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                                        | _ -> failwith "op_singleton_expr: invalid singleton"
                                    in match r with
                                     | [] -> []
                                     | [v] -> [(c_sum v (List.hd [(c_prod (List.hd [CFloat(0.8)]) (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]))]))]
                                     | _ -> failwith "op_singleton_expr: invalid singleton"
                                 ),(
                                    let r = 
                                       let r = 
                                          let slice =
                                             let m = Database.get_map "QUERY_1_1__3" db in
                                             let inv_img = [] in
                                                if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                else 
                                                   failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                          in
                                          let outv_img = [] in
                                             if ValuationMap.mem outv_img slice
                                             then [ValuationMap.find outv_img slice] else []
                                       in match r with
                                        | [] -> []
                                        | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                                        | _ -> failwith "op_singleton_expr: invalid singleton"
                                    in match r with
                                     | [] -> []
                                     | [v] -> [(c_sum v (List.hd [(c_prod (List.hd [CFloat(0.8)]) (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]))]))]
                                     | _ -> failwith "op_singleton_expr: invalid singleton"
                                 )) in
                                 match (r1,r2) with
                                  | ([], _) | (_,[]) -> []
                                  | ([v1], [v2]) -> [((fun a b -> if ((<=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2)]
                                  | _ -> failwith "op_singleton_expr: invalid singleton"
                              in match r with
                               | [] -> []
                               | [v] -> [((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v (List.hd [CFloat(1.)]))]
                               | _ -> failwith "op_singleton_expr: invalid singleton"
                           in begin match res2 with
                               | [] ->  ValuationMap.empty_map()
                               | [v2] -> ValuationMap.mapi (fun k v -> (k@k2, (c_prod v v2))) res1
                               | _ -> failwith "op_lslice_product_expr: invalid singleton"
                              end
                        in ValuationMap.mapi (fun k2 v2 ->
                           ([List.nth k2 0], (c_prod (List.hd [CFloat(-1.)]) v2))) r
                     in
                     let r3 = 
                        ValuationMap.mapi (fun k2 v2 -> 
                           ([var_B1__PRICE;List.nth k2 0], (c_prod v1 v2))
                        ) r2
                     in ValuationMap.union r r3
                  in ValuationMap.fold f (ValuationMap.empty_map()) res1
               in
               (* 
                slice0; *)
               [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
            in match (current_singleton, delta_slice) with
               | (s, []) -> s
               | ([], [delta_v]) ->
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [] delta_v]
               | ([current_v], [delta_v]) -> [c_sum current_v delta_v]
               | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1 ***)
   let lhs_map = Database.get_map "QUERY_1_1" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            let delta_slice = 
               let slice0 = 
                  let res1 = 
                     let slice =
                        let m = Database.get_map "QUERY_1_1__1" db in
                        let inv_img = [] in
                           if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                           else 
                              failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                     in ValuationMap.strip_indexes slice
                  in
                  let f k v1 r =
                     let r2 = 
                        let res1 = 
                           let r = 
                              let res1 = 
                                 let res1 = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__2" db in
                                       let inv_img = [var_B1__PRICE] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             let init_val = 
                                                let slice0 = 
                                                   let res1 = 
                                                      let res1 = 
                                                         let slice =
                                                            let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                            let inv_img = [] in
                                                               if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                               else 
                                                                  failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                         in ValuationMap.strip_indexes slice
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                         ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                         let res1 = 
                                                         ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                         in
                                                         let res2 = 
                                                         ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                         in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                (* 
                                                 slice0; *)
                                                [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                             in begin match init_val with
                                              | [] -> ValuationMap.empty_map()
                                              | [v] -> 
                                                   (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                      ValuationMap.from_list [([], v)] [])
                                              | _ -> failwith "MapAccess: invalid singleton"
                                             end
                                    in ValuationMap.strip_indexes slice
                                 in
                                 let f k v1 r =
                                    let r2 = 
                                       let r = 
                                          let r = 
                                             let res1 = 
                                             ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                             in
                                             let f k v1 r =
                                                let r2 = 
                                                ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                in
                                                let r3 = 
                                                   ValuationMap.mapi (fun k2 v2 -> 
                                                      ([List.nth k2 0], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                                   ) r2
                                                in ValuationMap.union r r3
                                             in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                          in ValuationMap.mapi (fun k2 v2 ->
                                             ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                                       in ValuationMap.mapi (fun k2 v2 ->
                                          ([var_QUERY_1_1__1BIDS_B1__VOLUME;List.nth k2 0], (c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) v2))) r
                                    in
                                    let r3 = 
                                       ValuationMap.mapi (fun k2 v2 -> 
                                          ([List.nth k2 0;List.nth k2 1], (c_sum v1 v2))
                                       ) r2
                                    in ValuationMap.union r r3
                                 in ValuationMap.fold f (ValuationMap.empty_map()) res1
                              in
                              let f k v1 r =
                                 let r2 = 
                                    let res1 = 
                                       let slice =
                                          let m = Database.get_map "QUERY_1_1__2" db in
                                          let inv_img = [var_B1__PRICE] in
                                             if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                             else 
                                                let init_val = 
                                                   let slice0 = 
                                                      let res1 = 
                                                         let res1 = 
                                                            let slice =
                                                               let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                               let inv_img = [] in
                                                                  if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                                  else 
                                                                     failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                            in ValuationMap.strip_indexes slice
                                                         in
                                                         let f k v1 r =
                                                            let r2 = 
                                                            ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                            in
                                                            let r3 = 
                                                               ValuationMap.mapi (fun k2 v2 -> 
                                                                  ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                               ) r2
                                                            in ValuationMap.union r r3
                                                         in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                            let res1 = 
                                                            ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                            in
                                                            let res2 = 
                                                            ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                            in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   (* 
                                                    slice0; *)
                                                   [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                                in begin match init_val with
                                                 | [] -> ValuationMap.empty_map()
                                                 | [v] -> 
                                                      (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                         ValuationMap.from_list [([], v)] [])
                                                 | _ -> failwith "MapAccess: invalid singleton"
                                                end
                                       in ValuationMap.strip_indexes slice
                                    in
                                    let f k v1 r =
                                       let r2 = 
                                          let r = 
                                             let r = 
                                                let res1 = 
                                                ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                in
                                                let f k v1 r =
                                                   let r2 = 
                                                   ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                   in
                                                   let r3 = 
                                                      ValuationMap.mapi (fun k2 v2 -> 
                                                         ([List.nth k2 0], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                                      ) r2
                                                   in ValuationMap.union r r3
                                                in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                             in ValuationMap.mapi (fun k2 v2 ->
                                                ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                                          in ValuationMap.mapi (fun k2 v2 ->
                                             ([var_QUERY_1_1__1BIDS_B1__VOLUME;List.nth k2 0], (c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) v2))) r
                                       in
                                       let r3 = 
                                          ValuationMap.mapi (fun k2 v2 -> 
                                             ([List.nth k2 0;List.nth k2 1], (c_sum v1 v2))
                                          ) r2
                                       in ValuationMap.union r r3
                                    in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                 in
                                 let r3 = 
                                    ValuationMap.mapi (fun k2 v2 -> 
                                       ([List.nth k2 0;List.nth k2 1], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                    ) r2
                                 in ValuationMap.union r r3
                              in ValuationMap.fold f (ValuationMap.empty_map()) res1
                           in ValuationMap.mapi (fun k2 v2 ->
                              ([List.nth k2 0;List.nth k2 1], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                        in
                        let k2 = [] in
                        let res2 = 
                           let r = 
                              let (r1,r2) = ((
                                 let r = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__3" db in
                                       let inv_img = [] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                    in
                                    let outv_img = [] in
                                       if ValuationMap.mem outv_img slice
                                       then [ValuationMap.find outv_img slice] else []
                                 in match r with
                                  | [] -> []
                                  | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                                  | _ -> failwith "op_singleton_expr: invalid singleton"
                              ),(
                                 let r = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__3" db in
                                       let inv_img = [] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                    in
                                    let outv_img = [] in
                                       if ValuationMap.mem outv_img slice
                                       then [ValuationMap.find outv_img slice] else []
                                 in match r with
                                  | [] -> []
                                  | [v] -> [(c_prod v (List.hd [CFloat(0.8)]))]
                                  | _ -> failwith "op_singleton_expr: invalid singleton"
                              )) in
                              match (r1,r2) with
                               | ([], _) | (_,[]) -> []
                               | ([v1], [v2]) -> [((fun a b -> if ((<=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2)]
                               | _ -> failwith "op_singleton_expr: invalid singleton"
                           in match r with
                            | [] -> []
                            | [v] -> [((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v (List.hd [CFloat(1.)]))]
                            | _ -> failwith "op_singleton_expr: invalid singleton"
                        in begin match res2 with
                            | [] ->  ValuationMap.empty_map()
                            | [v2] -> ValuationMap.mapi (fun k v -> (k@k2, (c_prod v v2))) res1
                            | _ -> failwith "op_lslice_product_expr: invalid singleton"
                           end
                     in
                     let r3 = 
                        ValuationMap.mapi (fun k2 v2 -> 
                           ([List.nth k2 1;List.nth k2 0], (c_prod v1 v2))
                        ) r2
                     in ValuationMap.union r r3
                  in ValuationMap.fold f (ValuationMap.empty_map()) res1
               in
               (* 
                slice0; *)
               [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
            in match (current_singleton, delta_slice) with
               | (s, []) -> s
               | ([], [delta_v]) ->
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [] delta_v]
               | ([current_v], [delta_v]) -> [c_sum current_v delta_v]
               | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update QUERY_1_1 ***)
   let lhs_map = Database.get_map "QUERY_1_1" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            let delta_slice = 
               let slice0 = 
                  let res1 = 
                     let res1 = 
                     ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                     in
                     let f k v1 r =
                        let r2 = 
                           let res1 = 
                           ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                           in
                           let f k v1 r =
                              let r2 = 
                              ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                              in
                              let r3 = 
                                 ValuationMap.mapi (fun k2 v2 -> 
                                    ([List.nth k2 0], ((fun a b -> if ((=) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                 ) r2
                              in ValuationMap.union r r3
                           in ValuationMap.fold f (ValuationMap.empty_map()) res1
                        in
                        let r3 = 
                           ValuationMap.mapi (fun k2 v2 -> 
                              ([List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                           ) r2
                        in ValuationMap.union r r3
                     in ValuationMap.fold f (ValuationMap.empty_map()) res1
                  in
                  let f k v1 r =
                     let r2 = 
                        let r = 
                           let r = 
                              let res1 = 
                                 let slice =
                                    let m = Database.get_map "QUERY_1_1__2" db in
                                    let inv_img = [var_B1__PRICE] in
                                       if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                       else 
                                          let init_val = 
                                             let slice0 = 
                                                let res1 = 
                                                   let res1 = 
                                                      let slice =
                                                         let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                         let inv_img = [] in
                                                            if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                            else 
                                                               failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                      in ValuationMap.strip_indexes slice
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                      ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                let f k v1 r =
                                                   let r2 = 
                                                      let res1 = 
                                                      ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                      in
                                                      let res2 = 
                                                      ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                      in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                   in
                                                   let r3 = 
                                                      ValuationMap.mapi (fun k2 v2 -> 
                                                         ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                      ) r2
                                                   in ValuationMap.union r r3
                                                in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                             in
                                             (* 
                                              slice0; *)
                                             [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                          in begin match init_val with
                                           | [] -> ValuationMap.empty_map()
                                           | [v] -> 
                                                (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                   ValuationMap.from_list [([], v)] [])
                                           | _ -> failwith "MapAccess: invalid singleton"
                                          end
                                 in ValuationMap.strip_indexes slice
                              in
                              let f k v1 r =
                                 let r2 = 
                                    let slice =
                                       let m = Database.get_map "QUERY_1_1__2" db in
                                       let inv_img = [var_B1__PRICE] in
                                          if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                          else 
                                             let init_val = 
                                                let slice0 = 
                                                   let res1 = 
                                                      let res1 = 
                                                         let slice =
                                                            let m = Database.get_map "INPUT_MAP_BIDS" db in
                                                            let inv_img = [] in
                                                               if ValuationMap.mem inv_img m then ValuationMap.find inv_img m
                                                               else 
                                                                  failwith "singleton_init_lookup: invalid init value for a lookup with no in vars."
                                                         in ValuationMap.strip_indexes slice
                                                      in
                                                      let f k v1 r =
                                                         let r2 = 
                                                         ValuationMap.from_list [([var_B2__VOLUME], var_B2__VOLUME)] []
                                                         in
                                                         let r3 = 
                                                            ValuationMap.mapi (fun k2 v2 -> 
                                                               ([var_B2__PRICE;List.nth k2 0], (c_prod v1 v2))
                                                            ) r2
                                                         in ValuationMap.union r r3
                                                      in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                   in
                                                   let f k v1 r =
                                                      let r2 = 
                                                         let res1 = 
                                                         ValuationMap.from_list [([var_B1__PRICE], var_B1__PRICE)] []
                                                         in
                                                         let res2 = 
                                                         ValuationMap.from_list [([var_B2__PRICE], var_B2__PRICE)] []
                                                         in ValuationMap.product (fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) res1 res2
                                                      in
                                                      let r3 = 
                                                         ValuationMap.mapi (fun k2 v2 -> 
                                                            ([List.nth k2 1;var_B2__VOLUME;List.nth k2 0], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) v1 v2))
                                                         ) r2
                                                      in ValuationMap.union r r3
                                                   in ValuationMap.fold f (ValuationMap.empty_map()) res1
                                                in
                                                (* 
                                                 slice0; *)
                                                [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
                                             in begin match init_val with
                                              | [] -> ValuationMap.empty_map()
                                              | [v] -> 
                                                   (Database.update_value "QUERY_1_1__2" [] [var_B1__PRICE] [] v db;
                                                      ValuationMap.from_list [([], v)] [])
                                              | _ -> failwith "MapAccess: invalid singleton"
                                             end
                                    in ValuationMap.strip_indexes slice
                                 in
                                 let r3 = 
                                    ValuationMap.mapi (fun k2 v2 -> 
                                       ([], ((fun a b -> if ((<) a b) then CFloat(1.0) else CFloat(0.0)) v1 v2))
                                    ) r2
                                 in ValuationMap.union r r3
                              in ValuationMap.fold f (ValuationMap.empty_map()) res1
                           in ValuationMap.mapi (fun k2 v2 ->
                              ([], ((fun v cond -> match cond with | CFloat(bcond) -> if bcond <> 0.0 then v else CFloat(0.0)) (List.hd [CFloat(1.)]) v2))) r
                        in ValuationMap.mapi (fun k2 v2 ->
                           ([var_QUERY_1_1__1BIDS_B1__VOLUME], (c_prod (List.hd [var_QUERY_1_1__1BIDS_B1__VOLUME]) v2))) r
                     in
                     let r3 = 
                        ValuationMap.mapi (fun k2 v2 -> 
                           ([var_B1__PRICE;List.nth k2 0], (c_prod v1 v2))
                        ) r2
                     in ValuationMap.union r r3
                  in ValuationMap.fold f (ValuationMap.empty_map()) res1
               in
               (* 
                slice0; *)
               [ValuationMap.fold (fun k v acc -> c_sum v acc) (CFloat(0.0)) slice0]
            in match (current_singleton, delta_slice) with
               | (s, []) -> s
               | ([], [delta_v]) ->
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [] delta_v]
               | ([current_v], [delta_v]) -> [c_sum current_v delta_v]
               | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "QUERY_1_1" [] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
   (*** Update INPUT_MAP_BIDS ***)
   let lhs_map = Database.get_map "INPUT_MAP_BIDS" db in
   let iifilter inv_img = 
      let slice = ValuationMap.find inv_img lhs_map in
      let db_f = 
         (fun inv_img in_slice ->
         let outv_img = [var_QUERY_1_1__1BIDS_B1__PRICE;var_QUERY_1_1__1BIDS_B1__VOLUME] in
         let singleton =
            if ValuationMap.mem outv_img in_slice
            then [ValuationMap.find outv_img in_slice] else [] in
         let new_value = 
            (fun current_singleton ->
            (* 
            ; *)
            match current_singleton with
             | [] -> 
                  [(fun _ v -> c_sum v (List.hd [CFloat(0.)])) [var_QUERY_1_1__1BIDS_B1__PRICE;var_QUERY_1_1__1BIDS_B1__VOLUME] (List.hd [CFloat(1.)])]
             | [current_v] -> [c_sum current_v (List.hd [CFloat(1.)])]
             | _ -> failwith "singleton_update: invalid singleton")
            singleton
         in
         match new_value with
          | [] -> ()
          | [v] -> Database.update_value "INPUT_MAP_BIDS" [[]] inv_img outv_img v db
          | _ -> failwith "db_singleton_update: invalid singleton")
      in db_f inv_img slice
   in iifilter [];
);;
let main() = 
let arguments = (ParseArgs.parse (ParseArgs.compile
  [(["-v"],("VEROBSE",ParseArgs.NO_ARG),"","Show all updates")
  ])) in
let log_evt = if ParseArgs.flag_bool arguments "VEROBSE" then
    (fun evt -> match evt with None -> () | Some(pm,rel,t) -> 
      print_endline (M3OCaml.string_of_evt pm rel t))
  else (fun evt -> ()) in
let log_results = if ParseArgs.flag_bool arguments "VEROBSE" then
    (fun () -> 
      print_endline ("QUERY_1_1: "^(
        Database.dbmap_to_string (
          Database.get_map "QUERY_1_1" db
      )))
    )
  else (fun () -> ()) in
   let mux = 
     let mux = FileMultiplexer.create() in
     ref mux
   in
   let start = Unix.gettimeofday() in
   while FileMultiplexer.has_next !mux do
   let (new_mux,evt) = FileMultiplexer.next !mux in
    ( log_evt evt;
      mux := new_mux;
      (match evt with 
      | None -> ()
      | Some(Insert,rel,t) -> (print_string ("Unhandled Insert: "^rel^"\n"))
      | Some(Delete,rel,t) -> (print_string ("Unhandled Delete: "^rel^"\n"))
      );
      log_results ()
    )
   done;
   let finish = Unix.gettimeofday() in
   print_endline ("Tuples: "^(string_of_float (finish -. start)))
in main();;