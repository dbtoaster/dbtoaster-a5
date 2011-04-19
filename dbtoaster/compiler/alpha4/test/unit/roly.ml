(* Tracking down the nefarious inability of roly_poly to handle Negations *)

open Util
open Calculus
;;

module rec DUMMY_BASE :
sig
   type t    = string
   val  zero : t
   val  one  : t
end =
struct
   type t    = string
   let  zero = "0"
   let  one  = "1"
end
and DummyRing : Ring.Ring with type leaf_t = DUMMY_BASE.t
    = Ring.Make(DUMMY_BASE)


let nested_prod = 
  DummyRing.Prod [
    DummyRing.Neg ( DummyRing.Prod [ 
      DummyRing.Val("a");
      DummyRing.Val("b"); 
    ] );
    DummyRing.Val("c");
    DummyRing.Val("d")
  ];;

Debug.log_unit_test "Ring.polynomial with negations" 
  (string_of_list0 "\n"
    (fun (c,e_l) -> 
      (string_of_list0 "\n"
        (fun e ->
          (string_of_int c)^" * "^e) e_l)))
  (DummyRing.polynomial nested_prod)
  [(-1,["a";"b";"c";"d"])];;

let rst_true_delta_r = 
  (make_term (
    RVal(AggSum(
      RProd[RVal(Var("a",TInt));RVal(Var("d",TInt))],
      RA_MultiNatJoin[
        RA_Neg(RA_MultiNatJoin[
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("a",TInt)),RVal(Var("Qr_a",TInt))));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("r_b",TInt)),RVal(Var("Qr_r_b",TInt))));
        ]);
        RA_Leaf(AtomicConstraint(Eq,
          RVal(Var("r_b",TInt)),RVal(Var("s_b",TInt))));
        RA_Leaf(Rel("s",["s_b",TInt;"s_c",TInt]));
        RA_Leaf(AtomicConstraint(Eq,
          RVal(Var("s_c",TInt)),RVal(Var("t_c",TInt))));
        RA_Leaf(Rel("t",["t_c",TInt;"d",TInt]))
      ]
    ))
  ));;

let (rst_true_delta_r_term,rst_true_delta_r_calc) = 
  match readable_term rst_true_delta_r with 
    RVal(AggSum(t,c)) -> (make_term t, make_relcalc c)
    | _ -> failwith "BUG IN roly.ml";;
  

Debug.log_unit_test "roly_poly with Negations" term_as_string
  ( (roly_poly rst_true_delta_r))
  (make_term (
    RProd[
      RVal(AggSum(
        RVal(Var("a",TInt)),
        RA_Leaf(AtomicConstraint(Eq,
          RVal(Var("a",TInt)),RVal(Var("Qr_a",TInt))));
      ));
      RVal(AggSum(
        RVal(Var("d",TInt)),
        RA_MultiNatJoin[
          RA_Leaf(Rel("t",["t_c",TInt;"d",TInt]));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("s_c",TInt)),RVal(Var("t_c",TInt))));
          RA_Leaf(Rel("s",["s_b",TInt;"s_c",TInt]));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("r_b",TInt)),RVal(Var("s_b",TInt))));
          RA_Leaf(AtomicConstraint(Eq,
            RVal(Var("r_b",TInt)),RVal(Var("Qr_r_b",TInt))))
        ]
      ));
      RVal(Const(Int(-1)))
    ]
  ));;

Debug.log_unit_test "roly_poly Idempotence" term_as_string
  (roly_poly rst_true_delta_r)
  (roly_poly (roly_poly rst_true_delta_r));;

