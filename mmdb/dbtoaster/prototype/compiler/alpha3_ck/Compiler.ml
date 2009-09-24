open MapAlg;;


let rec add_positions (l: 'a list) i =
   if (l=[]) then []
   else ((List.hd l), i) :: (add_positions (List.tl l) (i+1))
;;

(*
add_positions ["a"; "b"; "c"] 1 = [("a", 1); ("b", 2); ("c", 3)];;
*)


(* the schema *)
let all_rels = [("R", ["A"; "B"]); ("S", ["B"; "C"]);
                ("T", ["C"; "D"]); ("U", ["A"; "D"])];;


let string_of_list (sep: string) (l: string list): string =
   if (l = []) then ""
   else List.fold_left (fun x y -> x^sep^y) (List.hd l) (List.tl l);;


let string_of_mapalg (m:mapalg_t)
                     (children: (string * (string list) * mapalg_t) list) =
   let leaf_f (lf: readable_mapalg_lf_t) =
   (match lf with
      RConst(x)    -> string_of_int x
    | RVar(x)      -> x
    | RAggSum(_,_) ->
      let f (mapname, params, mapstructure) =
         if (mapstructure = MapAlg.make(RVal lf)) then
            [mapname^"["^(string_of_list ", " params)^"]"]
         else []  
      in
      (List.hd (List.flatten (List.map f children)))
   )
   in
      (MapAlg.rfold (fun l -> "("^(string_of_list "+" l)^")")
                    (fun l -> "("^(string_of_list "*" l)^")")
                    leaf_f (readable m))
;;


let generate_code (reln, bound_vars, params, old_ma, new_ma, new_ma_aggs) =
   let loop_vars = Util.ListAsSet.diff params bound_vars
   in
      "+"^reln^"("^(string_of_list ", " bound_vars)^ "): "^
        (if (loop_vars=[]) then ""
         else "foreach "^(string_of_list ", " loop_vars)^" do ")
        ^old_ma^"["^(string_of_list ", " params)^"] += "^
        (string_of_mapalg new_ma new_ma_aggs)
;;


let compile_delta_for_rel (reln:   string)
                          (relsch: string list)
                          (mapn:   string)         (* map name *)
                          (params: string list)
                          (mapalg: mapalg_t) =
   let bound_vars = (List.map (fun x -> "x_"^mapn^reln^"_"^x) relsch)
   in
   let (new_params, new_ma) =
      MapAlg.simplify (MapAlg.delta reln bound_vars mapalg)
                      bound_vars params
   in
   let mk_triple (x, i) =
      let t_params = (Util.ListAsSet.inter (collect_vars x)
                            (Util.ListAsSet.union new_params bound_vars))
      in
      (mapn^reln^(string_of_int i), t_params, x)
   in
   let todos =
      (List.map mk_triple (add_positions (extract_aggregates new_ma) 1))
   in
   (bound_vars, new_params, new_ma, todos)
;;


let rec compile ((mapn: string),
                 (params: string list),
                 (mapalg: mapalg_t)): (string list) =
   let f1 (reln, relsch) =
      match (compile_delta_for_rel reln relsch mapn params mapalg) with
         (bound_vars, new_params, new_ma, todos) ->
         (reln, bound_vars, new_params, new_ma, todos)
   in
   let l1 = List.filter (fun (_, _, _, new_ma, _) -> (new_ma <> MapAlg.zero))
                        (List.map f1 all_rels)
   in
   let l = List.map (fun (reln, bound_vars, new_params,       new_ma, todos) ->
                         (reln, bound_vars, new_params, mapn, new_ma, todos))
                    l1
   in
   let ready = List.map generate_code l
   in
   let todo = List.flatten (List.map (fun (x,y,z,v,w) -> w) l1)
   in
   ready @ (List.flatten (List.map compile todo))
;;


