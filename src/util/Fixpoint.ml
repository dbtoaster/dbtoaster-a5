(**
   Tools for working with iteratively constructed fixpoint operations.  These 
   are most often used in the various optimization stages that we implement.
   
   A typical use case is to start with [Fixpoint.noop ()], and then iteratively
   construct a transform by a combination of [Fixpoint.build] and 
   [Fixpoint.build_if].  The final fixpoint is computed with [Fixpoint.compute]
   or [Fixpoint.compute_with_history].
*)

(** 
   The fundamental construct of Fixpoint: a mapping from a type to itself.
*)
type 'x t = 'x -> 'x

(**
   A reference to the simplest transform, a mapping from an entity to itself.
*)
let noop ():'x t ref = ref (fun x -> x)
;;

(**
   Build up a referenced transform by composing it with a new transform.
   operation.  The final value of [fp_op] will be the transform [fp_op(f(...))].
   @param fp_op The previous (referenced) transform
   @param f     The transform to compose with [fp_op]
*)
let build (fp_op:'x t ref) (f:'x t):unit = 
   let old_op = !fp_op in
   fp_op := (fun x -> (old_op (f x)))
;;

(**
   Build up a referenced transform by composing it with a new transform, if and
   only if a particular option has been selected.
   @param chosen_opts The list of selected options
   @param fp_op       The previous (referenced) transform
   @param option      The option to test for the presence of
   @param f           The transform to compose with [fp_op] if [option] is
                      an element of [chosen_opts]
*)
let build_if (chosen_opts:'a list) (fp_op:'x t ref) (option:'a) (f: 'x t) =
   if List.mem option chosen_opts then build fp_op f
;;

(**
   Compute the fixpoint of a given transform on a given value
   @param f     The transform to apply up to a fixpoint
   @param x     The value to apply [f] to
   @return      The fixpoint of [f] on [x]
*)
let rec compute f x =
   let new_x = f x in
      if new_x <> x then compute f new_x
      else new_x
;;

(**
   Compose two functions
   @param f_list  The list of functions to compose
   @return        an [f x] equivalent to [((List.nth f_list 1) 
                  ((List.nth f_list 2) (... x)))]
*)
let rec compose = (function 
 | [] -> (fun x -> x)
 | [x] -> x
 | hd::rest -> (fun x -> (hd ((compose rest) x)))
)

(**
   Recursively apply a given transform until it produces a duplicate value.  Not
   quite the fixpoint, but it works if the transform produces multiple 
   equivalent representations of the fixpoint value.
   @param f     The transform to apply up to a fixpoint
   @param x     The value to apply [f] to
   @return      The (not quite) fixpoint of [f] on [x]
*)
let rec compute_with_history ?(history = []) f x =
   if List.mem x history then x
   else compute_with_history ~history:(x::history) f (f x)
   