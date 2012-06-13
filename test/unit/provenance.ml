open UnitTest
open Provenance
;;

Debug.activate "NO-VISUAL-DIFF";;
Debug.activate "PARSE-CALC-WITH-FLOAT-VARS";;

let test msg expr val_expected var_expected =
   let (var_prov, val_prov) =
      provenance_of_expr (parse_calc expr)
   in (
      log_list_test ("Provenance Value ( "^msg^" )")
         (fun (vn,vp) -> vn^"->"^(string_of_provenance vp))
         (("[value]", val_prov    ) :: (List.map (fun ((vn,_),vp) -> (vn,vp)) 
                                                 var_prov))
         (("[value]", val_expected) :: var_expected)
   )
in
   test "Relation"         "R(A)"
      (rel_source "R")
      ["A", rel_source "R"];
   test "Relation * Const" "R(A)*A"
      (join [rel_source "R"; rel_source "R"])
      ["A", rel_source "R"];
   test "AggSum"           "AggSum([], R(A))"
      (join [rel_source "R"; rel_source "R"])
      [];
   test "r_count_of_one -3"
      "(R(R_A, R_B) * (S_A ^= R_A))"
      (join [rel_source "R"; inline_source])
      [  "R_A", rel_source "R";
         "R_B", rel_source "R";
         "S_A", rel_source "R"];
   test "r_count_of_one -2"
      "AggSum([S_A], (R(R_A, R_B) * (S_A ^= R_A)))"
      (join [rel_source "R"; inline_source; rel_source "R"; rel_source "R"])
      [  "S_A", rel_source "R"];
   test "r_count_of_one -1"
      "((S_C ^= AggSum([S_A], (R(R_A, R_B) * (S_A ^= R_A)))) * (S_A ^= 3))"
      (join [inline_source; inline_source])
      [  "S_C", join [rel_source "R"; inline_source; 
                      rel_source "R"; rel_source "R"];
         "S_A", join [rel_source "R"; inline_source]];
   test "r_count_of_one"
      "AggSum([S_C], 
        ((S_C ^= AggSum([S_A], (R(R_A, R_B) * (S_A ^= R_A)))) * (S_A ^= 3)))"
      (join [inline_source; inline_source; rel_source "R"; inline_source])
      [  "S_C", join [rel_source "R"; inline_source; 
                      rel_source "R"; rel_source "R"]];
      