open Types
open Arithmetic
open Calculus
open UnitTest
;;

(*Debug.activate "LOG-HEURISTICS-DETAIL";;*) 

let test msg input output =
   log_test ("Decomposition ("^msg^")")
      string_of_expr
      (CalcRing.mk_sum (snd (List.split 
						(CalculusDecomposition.decompose_poly (parse_calc input)))))
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
      ( snd (List.split (snd	(
					CalculusDecomposition.decompose_graph scope (schema, parse_calc input)))) )
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

let test msg input output =
	let history:Heuristics.ds_history_t = ref [] in 
	let event = Some(Schema.InsertEvent(schema_rel "R" ["dA"; "dB"])) in
	let expr = parse_calc input in 
	   log_test ("Materialization ("^msg^")")
     	string_of_expr
     	(snd(Heuristics.materialize history "M" event expr))
      (parse_calc output) 
in 
	test "Simple"
      "R(A,B)"
      "M_t1_g1(int)[][A, B]";	
	test "Simple with an aggregation"
      "AggSum([A], R(A,B))"
      "M_t1_g1(int)[][A]";
	test "Binary sum with an aggregation"
      "AggSum([A], R(A,B) + S(A,C))"
      "(M_t1_g1(int)[][A] + M_t2_g1(int)[][A])";
	test "Join with an aggregation"
      "AggSum([A], R(A,B) * S(B))"
      "M_t1_g1(int)[][A]";		
	test "Aggregation with a condition"
			"AggSum([A], R(A,B) * [A < B])"
      "M_t1_g1(int)[][A]";
	test "Aggregation with an input variable"
			"AggSum([A], R(A,B) * [A < C])"
      "M_t1_g1(int)[][A] * [A < C]";
	test "Extending output schema due to an input variable"
			"AggSum([A], R(A,B) * [B < C])"
      "M_t1_g1(int)[][A,B] * [B < C]";
	test "Graph Decomposition"
			"AggSum([A], R(A,B) * S(C))"
      "M_t1_g1(int)[][A] * M_t1_g2(int)[][]";
	test "Map reuse"
			"AggSum([A], R(A,B) + R(A,B))"
      "(M_t1_g1(int)[][A] + M_t1_g1(int)[][A])";
	test "Map reuse with renaming"
			"AggSum([A], R(A,B) * (S(C) + S(D)))"
      "(M_t1_g1(int)[][A] * M_t1_g2(int)[][]) + (M_t1_g1(int)[][A] * M_t1_g2(int)[][])";
	test "Map reuse with renaming"
			"(R(A,B) * S(C)) + (R(B,C) * S(A))"
      "(M_t1_g1(int)[][A,B] * M_t1_g2(int)[][C]) + (M_t1_g1(int)[][B,C] * M_t1_g2(int)[][A])";
			
(*	
	test "Aggregation with a lift containing no relations"
			"AggSum([A], R(A,B) * (C ^= (A + B)))"
      "M1_1_1(int)[][A]";		
	test "Aggregation with a lift containing no relations"
			"AggSum([A], R(A,B) * (C ^= (A + B)) * [C > 0])"
      "M1_1_1(int)[][A]";		
	test "Aggregation with a lift containing an irrelevant relation"
			"AggSum([A], R(A,B) * (C ^= S(A)))"
      "M1_1_1(int)[][A]";		
	test "Aggregation with a lift containing an irrelevant relation"
			"AggSum([A], R(A,B) * (C ^= S(A)) * [C > 0])"
      "M1_1_1(int)[][A]";
	test "Aggregation with a lift containing a relevant relation"
			"AggSum([A], R(A,B) * (C ^= R(A,B) * B))"
      "(M1_1_1(int)[][A] * (C ^= M1_1_1_1(float)[][A, B]))";
	test "Aggregation with a lift containing a relevant relation and a condition"
			"AggSum([A], R(A,B) * (C ^= R(A,B) * B) * [C > 0])"
      "(M1_1_1(int)[][A] * (C ^= M1_1_1_1(float)[][A, B]) * [C > 0])";	
*)			
(*
		test "Extending schema due to a lift"
			"AggSum([A], R(A) * S(C) * (D ^= R(B) * C))"		
			"(M1_t1_g1(int)[][A, C] * (D ^= M1_1_1_1(float)[][B, C]))";
	test "Mapping example"
			"R(A) * S(C) * (E ^= R(B) * S(D))"		
			"(M1_t1_g1_1(int)[][A, C] * (E ^= M1_1_1_1(int)[][B, D]))";
*)	
(*
	test "Killer - Aggregation with a lift containing an irrelevant relation"
			"AggSum([A], R(A,B) * (C ^= S(D)))"
      "M1_1_1(int)[][A]";		
	test "Killer - Aggregation with a lift containing an irrelevant relation"
			"AggSum([A], R(A,B) * (C ^= S(D)) * [C > 0])"
      "M1_1_1(int)[][A]";
*)
;;