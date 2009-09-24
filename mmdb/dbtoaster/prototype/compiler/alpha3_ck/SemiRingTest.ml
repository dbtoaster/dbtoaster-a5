
(* note that we do not declare this as an instance of module type
   SemiRing.Base here, because otherwise the function delta will
   be invisible to the outside world.
*)
module TST_BASE =
struct
   type t = Const of int | Incr of int
                           (* Incr is a variable that increases by a
                              given value in the delta function *)

   let zero = Const(0)
   let one  = Const(1)

   (* what is below is not part of the SemiRing.Base module type *)


   let delta x = match x with Const c -> zero | Incr c -> (Const c);;
end;;

module TstSemiRing = SemiRing.Make(TST_BASE);;


let t1_delta x = TstSemiRing.mk_val (TST_BASE.delta x);;

let test1 = TstSemiRing.delta t1_delta TstSemiRing.one = TstSemiRing.zero;; 

let test2 =
TstSemiRing.polynomial (TstSemiRing.delta t1_delta
    (TstSemiRing.mk_sum [TstSemiRing.mk_val(TST_BASE.Incr 5);
                         TstSemiRing.mk_val(TST_BASE.Const 3)])) =
TstSemiRing.mk_val (TST_BASE.Const 5);;

open TST_BASE;;
open TstSemiRing;;

let test3 =
polynomial (delta t1_delta (mk_prod [mk_val(Incr 5); mk_val(Const 3)])) =
mk_prod [mk_val (Const 5); mk_val (Const 3)];;


let test4 = TstSemiRing.mk_val(TST_BASE.Const 0) = TstSemiRing.zero;;
let test5 = TstSemiRing.mk_val(TST_BASE.Const 1) = TstSemiRing.one;;

let e1 = mk_sum [(mk_val (Const 1)); (mk_val (Const 2))];;
let e2 = mk_prod [e1; (mk_val (Const 3))];;

let test6 =
substitute e1 (mk_val (Const 4)) e2 =
mk_prod [(mk_val (Const 4)); (mk_val (Const 3))];;

let test7 =
polynomial e2 =
Sum [Val (Const 3); Prod [Val (Const 2); Val (Const 3)]];;


(* outputting by reconstructing the expression using a visible type *)
type 'a expr = Sum2  of 'a expr list
             | Prod2 of 'a expr list
             | Val2  of 'a
;;

let test8 =
fold (fun x -> Sum2 x) (fun x -> Prod2 x) (fun x -> Val2 x) e2;;
Prod2 [Sum2 [Val2 1; Val2 2]; Val2 3];;



(*********************************** A recursive modules example. *)


type my_leaf_t = A2 | B2 | C2;;


(* type t in the base type will be hidden, so we need to add something to
   make it accessible.
*)
module type MY_BASE_T =
sig
   include SemiRing.Base

   val data: t -> my_leaf_t
end;;


module rec REC_BASE : MY_BASE_T =
struct
   type t = A | B | C of RecSR.expr_t

   let  zero = A
   let  one  = B

   let data x = match x with A -> A2 | B -> B2 | C(y) -> C2
end
and RecSR : SemiRing.SemiRing with type leaf_t = REC_BASE.t
          = SemiRing.Make(REC_BASE);;


RecSR.zero = RecSR.mk_prod [RecSR.one; RecSR.zero];;

REC_BASE.data (RecSR.get_val RecSR.one) = B2;;





