open Type
open Arithmetic
open Calculus
open UnitTest
;;

Debug.activate "PARSE-CALC-WITH-FLOAT-VARS";
(*Debug.activate "HEURISTICS-PULL-IN-LIFTS";*)
;;

let test_db = mk_db [
   ("R", ["A"; "B"]);
   ("S", ["B"; "C"]);
   ("T", ["D"]);
];;

let test msg input output =
   log_test ("Decomposition ("^msg^")")
      string_of_expr
      (CalcRing.mk_sum (
         snd (List.split (
            CalculusDecomposition.decompose_poly (
               parse_calc input)))))
      (parse_calc output) 
in
   test "Simple Decomposition #1"
      "(R(A,B) + S(B,C)) * T(D)"
      "(R(A,B) * T(D)) + (S(B,C) * T(D))";
   test "Simple Decomposition #2"
      "P(A) * (R(B) + S(C)) * T(D)"
      "(P(A) * R(B) * T(D)) + (P(A) * S(C) * T(D))";
   test "Simple Decomposition #3"
      "P(A) * (R(B) + S(C)) * (T(D) + O(E))"
      ("(P(A) * R(B) * T(D)) + (P(A) * S(C) * T(D)) +"^ 
       "(P(A) * R(B) * O(E)) + (P(A) * S(C) * O(E))");
   test "Aggregates #4"
      "AggSum([A], P(A) * (R(B) + S(C)) * (T(D) + O(E)))"
      ("(P(A) * R(B) * T(D)) + (P(A) * S(C) * T(D)) +"^ 
       "(P(A) * R(B) * O(E)) + (P(A) * S(C) * O(E))");
   test "Aggregates with a lift #4"
      "AggSum([A], P(A) * (R(B) + S(C)) * (ALPHA ^= T(D)))"
      "(P(A) * R(B) * (ALPHA ^= T(D))) + (P(A) * S(C) * (ALPHA ^= T(D)))";
   test "Aggregates with a decomposable lift #5" 
      (* the lift should not be decomposed *)
      "AggSum([A], P(A) * (R(B) + S(C)) * (ALPHA ^= (T(D) + U(E)) * V(F)))"
      ("(P(A) * R(B) * (ALPHA ^= (T(D) + U(E)) * V(F))) +"^ 
       "(P(A) * S(C) * (ALPHA ^= (T(D) + U(E)) * V(F)))");        
  (* TODO: Multiple AggSum have to have disjoint schemas *)                 
;;

let test msg input output scope schema =
   log_test ("Graph Decomposition ("^msg^")")
      (ListExtras.string_of_list string_of_expr)
      (snd (List.split (
         snd (CalculusDecomposition.decompose_graph 
            scope (schema, parse_calc input)))))
      (List.map (parse_calc) output)
in
   test "Simple RS"
        "R(A,B) * S(C)"
        ["R(A,B)";"S(C)"] [] [];
   test "Negation"
        "(-1) * R(A,B) * S(C)"
        [ "-1"; "R(A,B)"; "S(C)"] [] [];
   test "With no scope"
        "(-1) * R(A,B) * S(B,C)"
        [ "-1"; "R(A,B) * S(B,C)"] [] [];
   test "With scope"
        "(-1) * R(A,B) * S(B,C)"
        [ "-1"; "R(A,B)"; "S(B,C)"] [ ("B",TFloat) ] [];                
;;

let test msg 
      ?(event = Some(Schema.InsertEvent(schema_rel "R" ["dA"; "dB"])))
      ?(heuristic_options = [Heuristics.NoIVC; Heuristics.NoInputVariables])
         ?(scope = [])
      input output =
   let history:Heuristics.ds_history_t = ref [] in 
   let expr = parse_calc input in 
      log_test ("Materialization ("^msg^")")
         string_of_expr
         (snd (Heuristics.materialize ~scope:scope heuristic_options
                                      test_db history "M" event expr))
      (parse_calc output) 
in 
   test "Simple"
      "R(A,B)"
      "M1(int)[][A, B]";    
   test "Simple with an aggregation"
      "AggSum([A], R(A,B))"
      "M1(int)[][A]";
   test "Binary sum with an aggregation"
      "AggSum([A], R(A,B) + S(A,B))"
      "(M1(int)[][A] + M2(int)[][A])";
   test "Join with an aggregation"
      "AggSum([A], R(A,B) * S(B,C))"
      "M1(int)[][A]";       
   test "Aggregation with a condition"
      "AggSum([A], R(A,B) * {A < B})"
      "M1(int)[][A]";
   test "Aggregation with an input variable"
       "(C ^= 0) * S(A) * (L ^= (AggSum([A], R(A,B) * {A < C})))"
       "(C ^= 0) * M1(int)[][A] * (L ^= ({A < C} * M1_L2_2(int)[][A]))";
   test "Extending output schema due to an input variable"
      "(C ^= 0) * S(A) * (L ^= (AggSum([A], R(A,B) * {B < C})))"
      "(C ^= 0) * M1(int)[][A] * (L ^= (AggSum([A], M1_L2_1(int)[][A,B] * 
                {B < C})))";
   test "Graph Decomposition"
      "AggSum([A], R(A,B) * S(C,D))"
      "M1(int)[][A] * M2(int)[][]";
   test "Map reuse"
      "AggSum([A], R(A,B) + R(A,B))"
      "(M1(int)[][A] * 2)"; 
   test "Map reuse with renaming"
      "AggSum([A], R(A,B) * (S(C,E) + S(D,E)))"
      "M1(int)[][A] * M2(int)[][] * 2"; 
   test "Map reuse with renaming"
      "(R(A,B) * S(C,D)) + (R(B,C) * S(A,D))"
      "(M1(int)[][A,B] * M2(int)[][C,D]) + (M1(int)[][B,C] * M2(int)[][A,D])";
            
   test "Aggregation with a lift containing no relations"
      "AggSum([A], R(A,B) * (C ^= (A + B)))"
      "M1(int)[][A]";       
   test "Aggregation with a lift containing no relations and a comparison"
      "AggSum([A], R(A,B) * (C ^= (A + B)) * {C > 0})"
      "M1(int)[][A]";       
   test "Aggregation with a lift containing an irrelevant relation"
      "AggSum([A], R(A,B) * (C ^= S(A)) * {C + 1})"
      (if Debug.active "HEURISTICS-PULL-IN-LIFTS" then 
          "M1(float)[][A]"
       else 
          "M1(int)[][A] * AggSum([A], (C ^= M1_L1_1(int)[][A]) * {1 + C})");
   test ("Aggregation with a lift containing an irrelevant relation " ^ 
         "and a comparison")
      "AggSum([A], R(A,B) * (C ^= S(A)) * {C > 0})"
      (if Debug.active "HEURISTICS-PULL-IN-LIFTS" then
          "M1(int)[][A]"  
       else 
          "M1(int)[][A] * AggSum([A], (C ^= M1_L1_1(int)[][A]) * {C > 0})");
   test ("Aggregation with a lift containing an irrelevant relation " ^ 
         "and a common variable")
      "AggSum([A], R(A,B) * (C ^= (S(B) * {B = 0})) * C)"
      "M1(int)[][A]";
   test "Aggregation with a lift containing a relevant relation and a condition"
      "AggSum([A], R(A,B) * S(C,D) * (E ^= R(C,D) * B) * {E > 0})"
      "AggSum([A], (M1_P_1(int)[][A,B] * M1_P_2(int)[][C,D] *  
                   (E ^= M1_P_1(int)[][C,D] * B) * {E > 0}))";  
   test "Aggregation with a lift containing a relevant relation and a variable"
      "AggSum([A], R(A,B) * S(C,D) * (E ^= R(C,D) * B) * E)"
      "AggSum([A], (M1(float)[][A] * M2(int)[][]))";     
         
   test ("Aggregation with a lift containing a relevant relation " ^ 
         "and a common variable")
      "AggSum([A], R(A,B) * S(C,D) * (E ^= ({A = C} * R(C,D))) * {E > 0})"
      "M1_P_1(int)[][A] * AggSum([], M1_P_2(int)[][C,D] * 
             (E ^= ({A = C} * M1_L1_2(int)[][C,D])) * {E > 0})";
    
   test "Extending schema due to a lift"
      "AggSum([A], R(A,B) * S(C,D) * (E ^= R(B,D) * C) * {E > 0})"
      "AggSum([A], M1_P_1(int)[][A,B] * M1_P_2(int)[][C,D] * 
        (E ^= M1_P_1(int)[][B,D] * C) * {E > 0})";
   test "Mapping example"
      "R(A) * S(C) * (E ^= (R(C) * S(A))) * 5"      
      "(M1_P_1(int)[][A] * M1_P_2(int)[][C] * 
          (E ^= (M1_P_1(int)[][C] * M1_P_2(int)[][A])) * 5)";
    
   test "Aggregation with a lift containing an irrelevant relation"
      "AggSum([A,C], R(A,B) * (C ^= S(B)))"
      (if (not (Debug.active("HEURISTICS-PULL-IN-LIFTS")))
       then "AggSum([A,C], M1(int)[][A,B] * (C ^= M1_L1_1(int)[][B]))"
       else "M1(int)[][A,C]");
         
   test ("Aggregation with a lift containing an irrelevant relation " ^
         "and comparison")
      "AggSum([A,C], R(A,B) * (C ^= S(B)) * {C > 0})"
      (if (not (Debug.active("HEURISTICS-PULL-IN-LIFTS")))
       then "AggSum([A,C], M1(int)[][A,B] * (C ^= M1_L1_1(int)[][B]) * {C > 0})"
       else "M1(int)[][A,C]");

   test "Testing IVC optimization - no root relation - OFF case"
       ~heuristic_options:[Heuristics.NoIVC; Heuristics.NoInputVariables]
       ~scope:[("B", TFloat)]
      "AggSum([C], (C ^= R(B)))"
      "AggSum([C], (C ^= M1_L1_1(int)[][B]))";
                                                                               
   test "RExistsNestedAgg Delta Chunk"
      "R(A, B) *
       (foo ^= ( AggSum([], (X ^= R(A,B) + {dA = A} * dB) *
                            {(3 * A) < X})))"
      "M1(int)[][A,B] * 
       (foo ^= ( AggSum([], (X ^= (M1(int)[][A,B] + 
                            {dA = A} * dB)) * 
                            {(3 * A) < X})))"
;;
