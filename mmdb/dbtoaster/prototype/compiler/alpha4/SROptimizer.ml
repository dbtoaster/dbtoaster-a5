
(* type for structural recursion optimizations *)
type sr_t =
   L of int list
 | SR_Map      of sr_fun_t * sr_t
 | SR_Agg of (int -> int -> int) * int * sr_t
   (* the aggregation function must be associative *)

and sr_fun_t =
   SR_Simple_Fun of (int -> int)
 | SR_Nested_Fun of (int -> int -> int) * sr_t
   (* nested maps will be flattened *)

(* We need our own abstract data type for a structural recursion fragment of
   OCAML. Once this is done, there will be no difference between simple and
   nested functions in map.
*)


(* the eval functions give a semantics. *)
let rec eval_list prog =
   match prog with
      SR_Map (SR_Simple_Fun f, p) -> List.map f (eval_list p)

      (* the nested maps are flattened *)
    | SR_Map (SR_Nested_Fun (f, p2), p1) ->
         List.flatten
            (List.map (fun x -> eval_list (SR_Map (SR_Simple_Fun (f x), p2)))
                      (eval_list p1))
    | L l -> l
    | _ -> failwith "use eval_int"
;;

let eval_int prog =
   match prog with
      SR_Agg (f, i, p) -> List.fold_left f i (eval_list p)
    | _ -> failwith "use eval_list"
;;


let prog1 = SR_Map (SR_Simple_Fun (fun s -> s*2), L [5; 17]);;
let prog2 = SR_Map (SR_Simple_Fun (fun s -> s+1), prog1);;
let prog3 = SR_Agg ((+), 0, prog2);;
let prog4 = SR_Map (SR_Nested_Fun (( * ), L [5; 7]), L [9; 4]);;

eval_list prog2 = [11; 35];;
eval_int  prog3 = 46;;
eval_list prog4 = [45; 63; 20; 28];;


(* flatten folds *)
let rec iron (prog : sr_t) : sr_t =
   match prog with
      SR_Map(SR_Simple_Fun f, (SR_Map (SR_Simple_Fun g, p))) ->
         iron (SR_Map (SR_Simple_Fun (fun x -> f (g x)), p))
      (* flattening maps with nested functions will only work recursively
         if the function arguments can be tuples; right now everything is
         an integer *)

    | SR_Agg(f, i, SR_Map (SR_Simple_Fun g, p)) ->
         iron (SR_Agg((fun j l_el -> f j (g l_el)), i, p))
(*
    (* this does not yet work because we are restricted to Simple_Fun functions
       of type int->int but need to curry here *)
    | SR_Agg(f, i, SR_Map (SR_Nested_Fun (g, p2), p1)) ->
       let h = iron (SR_Agg(f, 0, SR_Map (SR_Simple_Fun g, p2)))
       in
       iron (SR_Agg(f, i, SR_Map (SR_Simple_Fun h, p1)))
*)
    | _ -> prog
;;

(* TODO: to resolve these two problems, implement the functions of sr_t as
   values constructed from our own variant type, rather than as OCAML
   functions.
*)


let i3 = iron prog3;;
eval_int i3 = eval_int prog3;;

iron prog4;;



