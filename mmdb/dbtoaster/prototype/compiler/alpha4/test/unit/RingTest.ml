
(* note that we do not declare this as an instance of module type
   Ring.Base here, because otherwise the function delta will
   be invisible to the outside world.
*)
module TST_BASE =
struct
   type t = Const of int | Incr of int
                           (* Incr is a variable that increases by a
                              given value in the delta function *)

   let zero = Const(0)
   let one  = Const(1)

   (* what is below is not part of the Ring.Base module type *)


   let delta x = match x with Const c -> zero | Incr c -> (Const c);;
end;;

module TstRing = Ring.Make(TST_BASE);;


open TST_BASE;;
open TstRing;;


let test1 =  mk_prod[mk_sum [Val(Const 5); Val(Const 3)];
 mk_sum [Val(Const 4); (mk_neg (Val(Const 7)))]];;

polynomial test1 =
[(1, [Const 5; Const 4]); (1, [Const 3; Const 4]); (-1, [Const 5; Const 7]);
 (-1, [Const 3; Const 7])]
;;

let test1b = polynomial (mk_prod [test1; Neg test1]);;



let t1_delta x = mk_val (TST_BASE.delta x);;

let test1c = delta t1_delta one = zero;; 

let test2 =
polynomial (delta t1_delta (mk_sum [mk_val(Incr 5); mk_val(Const 3)])) =
[(1, [Const 5])];;


let test3 =
polynomial (delta t1_delta (mk_prod [mk_val(Incr 5); mk_val(Const 3)])) =
[(1, [(Const 5); (Const 3)])];;


let test4 = mk_val(Const 0) = zero;;
let test5 = mk_val(Const 1) = one;;

let e1 = mk_sum [(mk_val (Const 1)); (mk_val (Const 2))];;
let e2 = mk_prod [e1; (mk_val (Const 3))];;

let test6 =
substitute e1 (mk_val (Const 4)) e2 =
mk_prod [(mk_val (Const 4)); (mk_val (Const 3))];;

let test7 =
polynomial e2 = [(1, [(Const 1); (Const 3)]); (1, [(Const 2); (Const 3)])];;




(* outputting by reconstructing the expression using a visible type *)
type 'a expr = Sum2  of 'a expr list
             | Prod2 of 'a expr list
             | Val2  of 'a
;;

let test8 =
fold (fun x -> Sum2 x) (fun x -> Prod2 x)
     (fun _ -> failwith "not implemented")
     (fun x -> Val2 x)
     e2;;
Prod2 [Sum2 [Val2 1; Val2 2]; Val2 3];;



(*********************************** A recursive modules example. *)


type my_leaf_t = A2 | B2 | C2;;


(* type t in the base type will be hidden, so we need to add something to
   make it accessible.
*)
module type MY_BASE_T =
sig
   include Ring.Base

   val data: t -> my_leaf_t
end;;


module rec REC_BASE : MY_BASE_T =
struct
   type t = A | B | C of RecSR.expr_t

   let  zero = A
   let  one  = B

   let data x = match x with A -> A2 | B -> B2 | C(y) -> C2
end
and RecSR : Ring.Ring with type leaf_t = REC_BASE.t
          = Ring.Make(REC_BASE);;


RecSR.zero = RecSR.mk_prod [RecSR.one; RecSR.zero];;

REC_BASE.data (RecSR.get_val RecSR.one) = B2;;





