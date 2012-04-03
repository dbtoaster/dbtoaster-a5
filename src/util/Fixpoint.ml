type 'x t = 'x -> 'x

let noop ():'x t ref = ref (fun x -> x)
;;
let build (fp_op:'x t ref) (f:'x t):unit = 
   let old_op = !fp_op in
   fp_op := (fun x -> (old_op (f x)))
;;
let build_if (chosen_opts:'a list) (fp_op:'x t ref) (option:'a) (f: 'x t) =
   if List.mem option chosen_opts then build fp_op f
;;
let rec compute f x =
   let new_x = f x in
      if new_x <> x then compute f new_x
      else new_x
;;
let rec compute_with_history ?(history = []) f x =
   if List.mem x history then x
   else compute_with_history ~history:(x::history) f (f x)
   