open Util;;
open Calculus;;
open Compiler;;

let c num = (RVal(Const(Int(num))));;
let v vn = (vn, TInt);;
let cmp_v op va vb = 
   RA_Leaf(AtomicConstraint(op, (RVal(Var(v va))), (RVal(Var(v vb)))));;
let rel rn rv = RA_Leaf(Rel(rn, List.map v rv));;

let q = make_term (
   RVal(AggSum(
      c 1,
      (RA_MultiNatJoin[
         rel "R" ["RA_A"; "RA_B"];
         rel "R" ["RB_A"; "RB_B"];
         rel "R" ["RC_A"; "RC_B"];
         cmp_v Eq "RA_A" "RB_A";
         cmp_v Eq "RB_A" "RC_A"
      ])
   ))
) in
let (trigs, todos) = compile_delta_for_rel 
      true
      "R"
      [v "REAL_R_A"; v "REAL_R_B"]
      false
      (map_term "QUERY" [])
      []
      []
      []
      q
in
   Debug.log_unit_test_list "Radius-1 Star Deltas"
      (fun (_, _, _, _, term) -> string_of_term term)
      trigs
      []
