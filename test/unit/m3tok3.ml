module T = Types
module K = K3.SR
module MK = M3ToK3
module U = UnitTest
module Vs = Values
module VK = Vs.K3Value
module Interpreter = K3Compiler.Make(K3Interpreter.K3CG)
;;

let maps = [
   "S", [], [];
   "R", [], [T.TFloat; T.TFloat];
	 "T", [T.TFloat; T.TFloat], [];
	 "W", [T.TFloat; T.TFloat], [T.TFloat; T.TFloat];
	 
	 "sum_tmp_1", [],[T.TFloat];
	 "QS", [], [];
   "QR", [], [T.TFloat; T.TFloat];
	 "QT", [T.TFloat; T.TFloat], [];
	 "QW", [T.TFloat; T.TFloat], [T.TFloat; T.TFloat];
]
;;

let patterns = [
   "R", [Patterns.Out(["X"], [0]); Patterns.Out(["Y"], [1])];
	 "W", [Patterns.Out(["Z"], [0]); Patterns.Out(["ZZ"], [1])];
]
;;

let init_code = 
	let pc_s = K.SingletonPC("S", K.TBase(T.TFloat)) in
	let pc_r = K.OutPC("R", ["X", K.TBase(T.TFloat); "Y", K.TBase(T.TFloat)], K.TBase(T.TFloat)) in
	let pc_t = K.InPC("T", ["X", K.TBase(T.TFloat); "Y", K.TBase(T.TFloat)], K.TBase(T.TFloat)) in
	let pc_w = K.PC("W", ["X", K.TBase(T.TFloat); "Y", K.TBase(T.TFloat)], 
											 ["Z", K.TBase(T.TFloat); "ZZ", K.TBase(T.TFloat)], K.TBase(T.TFloat)) in
	
	let pc_qs = K.SingletonPC("QS", K.TBase(T.TFloat)) in
	let pc_qr = K.OutPC("QR", ["X", K.TBase(T.TFloat); "Y", K.TBase(T.TFloat)], K.TBase(T.TFloat)) in
	let pc_qt = K.InPC("QT", ["X", K.TBase(T.TFloat); "Y", K.TBase(T.TFloat)], K.TBase(T.TFloat)) in
	let pc_qw = K.PC("QW", ["X", K.TBase(T.TFloat); "Y", K.TBase(T.TFloat)], 
											 ["Z", K.TBase(T.TFloat); "ZZ", K.TBase(T.TFloat)], K.TBase(T.TFloat)) in
											
	let to_kconst cst = K.Const(T.CFloat(cst)) in
	let to_kconsts csts = List.map to_kconst csts in
	Interpreter.compile_k3_expr (K.Block([
		K.PCValueUpdate(pc_s, [], [], to_kconst 5.);
		
	  K.PCValueUpdate(pc_r, [], to_kconsts [1.; 1.;], to_kconst 1.);
 		K.PCValueUpdate(pc_r, [], to_kconsts [1.; 2.;], to_kconst 2.);
 		K.PCValueUpdate(pc_r, [], to_kconsts [2.; 1.;], to_kconst 2.);
 		K.PCValueUpdate(pc_r, [], to_kconsts [2.; 2.;], to_kconst 4.);
 		K.PCValueUpdate(pc_r, [], to_kconsts [1.; 3.;], to_kconst 3.);
 		
		K.PCValueUpdate(pc_t, to_kconsts [1.; 1.;], [], to_kconst 1.);
 		K.PCValueUpdate(pc_t, to_kconsts [1.; 2.;], [], to_kconst 2.);
 		K.PCValueUpdate(pc_t, to_kconsts [2.; 1.;], [], to_kconst 2.);
 		K.PCValueUpdate(pc_t, to_kconsts [2.; 2.;], [], to_kconst 4.);
 		K.PCValueUpdate(pc_t, to_kconsts [1.; 3.;], [], to_kconst 3.);
 		
	  K.PCValueUpdate(pc_w, to_kconsts [1.; 1.;], to_kconsts [1.; 1.;], to_kconst 1.);
 		K.PCValueUpdate(pc_w, to_kconsts [1.; 1.;], to_kconsts [1.; 2.;], to_kconst 2.);
 		K.PCValueUpdate(pc_w, to_kconsts [2.; 2.;], to_kconsts [2.; 1.;], to_kconst 2.);
 		K.PCValueUpdate(pc_w, to_kconsts [2.; 2.;], to_kconsts [2.; 2.;], to_kconst 4.);
 		K.PCValueUpdate(pc_w, to_kconsts [3.; 3.;], to_kconsts [1.; 3.;], to_kconst 3.);
 		
		K.PCValueUpdate(pc_qs, [], [], to_kconst 5.);
		
	  K.PCValueUpdate(pc_qr, [], to_kconsts [1.; 1.;], to_kconst 1.);
 		K.PCValueUpdate(pc_qr, [], to_kconsts [1.; 2.;], to_kconst 2.);
 		K.PCValueUpdate(pc_qr, [], to_kconsts [2.; 1.;], to_kconst 2.);
 		K.PCValueUpdate(pc_qr, [], to_kconsts [2.; 2.;], to_kconst 4.);
 		K.PCValueUpdate(pc_qr, [], to_kconsts [1.; 3.;], to_kconst 3.);
 		
	  K.PCValueUpdate(pc_qt, to_kconsts [1.; 1.;], [], to_kconst 1.);
 		K.PCValueUpdate(pc_qt, to_kconsts [1.; 2.;], [], to_kconst 2.);
 		K.PCValueUpdate(pc_qt, to_kconsts [2.; 1.;], [], to_kconst 2.);
 		K.PCValueUpdate(pc_qt, to_kconsts [2.; 2.;], [], to_kconst 4.);
 		K.PCValueUpdate(pc_qt, to_kconsts [1.; 3.;], [], to_kconst 3.);
		
		K.PCValueUpdate(pc_qw, to_kconsts [1.; 1.;], to_kconsts [1.; 1.;], to_kconst 1.);
 		K.PCValueUpdate(pc_qw, to_kconsts [1.; 1.;], to_kconsts [1.; 2.;], to_kconst 2.);
 		K.PCValueUpdate(pc_qw, to_kconsts [2.; 2.;], to_kconsts [2.; 1.;], to_kconst 2.);
 		K.PCValueUpdate(pc_qw, to_kconsts [2.; 2.;], to_kconsts [2.; 2.;], to_kconst 4.);
 		K.PCValueUpdate(pc_qw, to_kconsts [2.; 2.;], to_kconsts [1.; 1.;], to_kconst 1.);
 		K.PCValueUpdate(pc_qw, to_kconsts [2.; 2.;], to_kconsts [1.; 2.;], to_kconst 2.);
 		K.PCValueUpdate(pc_qw, to_kconsts [3.; 3.;], to_kconsts [1.; 3.;], to_kconst 3.);
		K.PCValueUpdate(pc_qw, to_kconsts [3.; 3.;], to_kconsts [1.; 1.;], to_kconst 1.);
 		K.PCValueUpdate(pc_qw, to_kconsts [3.; 3.;], to_kconsts [1.; 2.;], to_kconst 2.);
]))
in

let calc_string_to_code env calc_s =
	 let calc = U.parse_calc calc_s in
	 let env_vars = List.map (fun (vn,vl) -> (vn,T.type_of_const vl)) env in
	 let env_el = MK.varIdType_to_k3_expr env_vars in
	 let (_,_,code),_ = MK.calc_to_k3_expr MK.empty_meta env_el calc in
	 (*
	 print_endline "\n--------------\n--------------";
	 print_endline ("Calculus: "^(Calculus.string_of_expr calc));
	 print_endline "\n--------------";
	 print_endline ("K3Expr: "^(K.code_of_expr code));
	 *)
	 Interpreter.compile_k3_expr code
in
let stmt_string_to_code env stmt_s =
	 let stmt = U.parse_stmt stmt_s in
	 let env_vars = List.map (fun (vn,vl) -> (vn,T.type_of_const vl)) env in
	 let (target_coll,code),_ = MK.collection_stmt MK.empty_meta env_vars stmt in
	 (*
	 print_endline "\n--------------\n--------------";
	 print_endline ("Calculus: "^(Calculus.string_of_expr calc));
	 print_endline "\n--------------";
	 print_endline ("K3Expr: "^(K.code_of_expr code));
	 *)
	 Interpreter.compile_k3_expr (K.Block([code;target_coll]))
in

let test_code env msg code rval =
	 let (vars, vals) = List.split env in
   let db = Database.NamedK3Database.make_empty_db maps patterns in
	 let _ = (K3Interpreter.K3CG.eval init_code [] [] db) in
	 U.log_test ("M3ToK3("^msg^")")
      Vs.K3Value.string_of_value
      (K3Interpreter.K3CG.eval code vars vals db)
      rval
in
let test_code_coll env msg code rval =
	 let (vars, vals) = List.split env in
   let db = Database.NamedK3Database.make_empty_db maps patterns in
	 let _ = (K3Interpreter.K3CG.eval init_code [] [] db) in
	 U.log_collection_test ("M3ToK3("^msg^")")
      (K3Interpreter.K3CG.eval code vars vals db)
      rval
in

let test_expr ?(env = []) msg calc_s rval =
	 let code = calc_string_to_code env calc_s in	
   test_code env msg code rval
in
let test_expr_coll ?(env = []) msg calc_s rval =
   let code = calc_string_to_code env calc_s in	
   test_code_coll env msg code rval
in

let test_stmt ?(env = []) msg stmt_s rval =
	 let code = stmt_string_to_code env stmt_s in	
   test_code env msg code rval
in
let test_stmt_coll ?(env = []) msg stmt_s rval =
   let code = stmt_string_to_code env stmt_s in	
   test_code_coll env msg code rval
in


		test_expr " AConst " 
							"[3]" (VK.BaseValue(T.CInt(3)))		;
		test_expr " AVar " 
							~env:["A", (T.CFloat(3.))]
							"[A]" (VK.BaseValue(T.CFloat(3.)))		;
		
		test_expr " Singleton Negation " 
							~env:["A", (T.CFloat(3.))]
							"[-A]" (VK.BaseValue(T.CFloat(-3.)))		;
		test_expr " Singleton Sum " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A+B+A+B]" (VK.BaseValue(T.CFloat(12.)))		;
		test_expr " Singleton Prod " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A*B*A*B]" (VK.BaseValue(T.CFloat(81.)))		;
		
		test_expr " Cmp Eq " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A=B]" (VK.BaseValue(T.CInt(1)))		;
		test_expr " Cmp Lt " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A<B]" (VK.BaseValue(T.CInt(0)))		;
		test_expr " Cmp Lte " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A<=B]" (VK.BaseValue(T.CInt(1)))		;
		test_expr " Cmp Gt " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A>B - 1]" (VK.BaseValue(T.CInt(1)))		;
		test_expr " Cmp Gte " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A>=B+1]" (VK.BaseValue(T.CInt(0)))		;
		test_expr " Cmp Neq " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A!=B]" (VK.BaseValue(T.CInt(0)))		;
		
		
		test_expr " External - SingletonPC - Singleton " 
							"S[][]" (VK.BaseValue(T.CFloat(5.)));
		test_expr " External - OutPC - Singleton " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(2.))]
							"R[][A,B]" (VK.BaseValue(T.CFloat(4.)));
		test_expr " External - InPC - Singleton " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(2.))]
							"T[A,B][]" (VK.BaseValue(T.CFloat(4.)));
		test_expr " External - PC - Singleton " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.));
										"C", (T.CFloat(1.));"D", (T.CFloat(3.))]
							"W[A,B][C,D]" (VK.BaseValue(T.CFloat(3.)));
		
		test_expr_coll " External - OutPC - Collection " 
							"R[][A,B]" 
							[  [1.; 3.], 3.;
				         [1.; 2.], 2.;
				         [2.; 2.], 4.;
				         [1.; 1.], 1.;
				         [2.; 1.], 2.;
				      ];
		test_expr_coll " External - PC - Collection " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(2.))]
							"W[A,B][C,D]" 
							[  [2.; 2.], 4.;
				         [2.; 1.], 2.;
				      ];
		
		test_expr_coll " External - OutPC - Collection Slice " 
							~env:["B", (T.CFloat(2.))]
							"R[][A,B]" 
							[  [2.], 4.;
				         [1.], 2.;
				      ];
		test_expr_coll " External - PC - Collection Slice " 
							~env:["A", (T.CFloat(1.));"B", (T.CFloat(1.));"C", (T.CFloat(1.))]
							"W[A,B][C,D]" 
							[  [2.], 2.;
				         [1.], 1.;
				      ];
		
		
		Debug.activate "M3ToK3-GENERATE-INIT";
		test_expr " External - OutPC - Singleton Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.))]
							"R[][A,B]" (VK.BaseValue(T.CFloat(0.)));
		test_expr " External - InPC - Singleton Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.));"D", (T.CFloat(1.))]
							"T[A,B][]( R[][A,D] * [3] )" (VK.BaseValue(T.CFloat(6.)));
		test_expr " External - PC - Singleton Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.));"C", (T.CFloat(1.));"D", (T.CFloat(3.))]
							"W[A,B][C,D]( R[][C,D] * [3] )" (VK.BaseValue(T.CFloat(9.)));
		test_expr_coll " External - PC - Collection Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.))]
							"W[A,B][C,D]( R[][C,D] * [3] )" 
							[  [1.; 3.], 9.;
				         [1.; 2.], 6.;
				         [2.; 2.], 12.;
				         [1.; 1.], 3.;
				         [2.; 1.], 6.;
				      ];
		test_expr_coll " External - PC - Slice Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.));"C", (T.CFloat(2.))]
							"W[A,B][C,D]( R[][C,D] * [3] )" 
							[  [2.], 12.;
				         [1.], 6.;
				      ];
		Debug.deactivate "M3ToK3-GENERATE-INIT";
		
		
		
		test_expr " AggSum Singleton "
							"AggSum([], R[][A,B] * [A = B])" (VK.BaseValue(T.CFloat(5.)));
		test_expr_coll " AggSum Collection "
							"AggSum([B], R[][A,B])" 
							[  [2.], 6.;
				         [1.], 3.;
								 [3.], 3.;
				      ];
		
		test_expr_coll " Lift Singleton "
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(2.));]
							"( L ^= ( R[][A,B] ) )" [ [4.], 1.;];
		test_expr_coll " Lift Collection "
							~env:["A", (T.CFloat(2.));]
							"( L ^= ( R[][A,B] ) )" 
							[  [2.; 4.], 1.;
				         [1.; 2.], 1.;
				      ];
		
		
		test_expr " Sum Singleton "
							~env:["A", (T.CFloat(1.));"B", (T.CFloat(3.));
										"C", (T.CFloat(2.));"D", (T.CFloat(2.));
										"E", (T.CFloat(2.));"F", (T.CFloat(2.));]
							" R[][A,B] + W[C,D][E,F] " (VK.BaseValue(T.CFloat(7.)));
		test_expr_coll " Sum Collection "
							~env:["A", (T.CFloat(1.));
										"C", (T.CFloat(3.));"D", (T.CFloat(2.));
										"E", (T.CFloat(2.));]
							" R[][A,B] + W[C,C][A,B] - W[D,D][E,B]" 
							[  [2.], -2.;
				         [1.], -1.;
				         [3.], 6.;
				      ];
		
		test_expr " Prod Singleton "
							~env:["A", (T.CFloat(1.));"B", (T.CFloat(3.));
										"C", (T.CFloat(2.));"D", (T.CFloat(2.));
										"E", (T.CFloat(2.));"F", (T.CFloat(2.));]
							" R[][A,B] * W[C,D][E,F] " (VK.BaseValue(T.CFloat(12.)));
		test_expr_coll " Prod Collection "
							~env:["A", (T.CFloat(1.));
										"C", (T.CFloat(3.));"D", (T.CFloat(2.));
										"E", (T.CFloat(2.));]
							" R[][A,Y] * W[C,C][A,ZZ] * W[C,C][A,ZZ] * W[D,D][E,ZZZ] " 
							[  [2.; 3.; 1.], 36.;
				         [2.; 3.; 2.], 72.;
				         [1.; 3.; 1.], 18.;
				         [1.; 3.; 2.], 36.;
				         [3.; 3.; 1.], 54.;
								 [3.; 3.; 2.], 108.;
				      ];
		
		test_stmt " Replace - SingletonPC - Singleton"
							" QS[][] := [12.] " 
							(VK.BaseValue(T.CFloat(12.)));
		test_stmt_coll " Replace - OutPC - Singleton"
							~env:["A", (T.CFloat(1.));
										"B", (T.CFloat(1.));"C", (T.CFloat(2.));]
							" QR[][B,C] := W[A,A][B,C] * [3.] " 
							[  [1.; 1.], 1.;
				         [1.; 2.], 6.;
				         [1.; 3.], 3.;
				         [2.; 1.], 2.;
				         [2.; 2.], 4.;
				      ];
		test_stmt_coll " Replace - InPC - Singleton"
							" QT[X,Y][] := [X  + Y] " 
							[  [1.; 1.], 2.;
				         [1.; 2.], 3.;
				         [1.; 3.], 4.;
				         [2.; 1.], 3.;
				         [2.; 2.], 4.;
				      ];
		test_stmt_coll " Replace - PC - Singleton"
							~env:["A", (T.CFloat(1.));
										"B", (T.CFloat(1.));"C", (T.CFloat(2.));]
							" QW[X,Y][B,C] := W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 1.; [1.; 1.; 1.; 2.], 6.;
				         [2.; 2.; 2.; 1.], 2.; [2.; 2.; 2.; 2.], 4.; [2.; 2.; 1.; 1.], 1.; [2.; 2.; 1.; 2.], 6.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 1.; [3.; 3.; 1.; 2.], 6.;  
				      ];		
		test_stmt_coll " Replace - OutPC - Collection"
							~env:["A", (T.CFloat(2.));]
							" QR[][B,C] := W[A,A][B,C] * [3.] " 
							[  [1.; 1.], 1.;
				         [1.; 2.], 2.;
				         [1.; 3.], 3.;
				         [2.; 1.], 6.;
				         [2.; 2.], 12.;
				      ];
		test_stmt_coll " Replace - PC - Collection"
							~env:["A", (T.CFloat(1.));]
							" QW[X,Y][B,C] := W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 3.; [1.; 1.; 1.; 2.], 6.;
				         [2.; 2.; 2.; 1.], 2.; [2.; 2.; 2.; 2.], 4.; [2.; 2.; 1.; 1.], 3.; [2.; 2.; 1.; 2.], 6.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 3.; [3.; 3.; 1.; 2.], 6.;  
				      ];		
		
		test_stmt " Update - SingletonPC - Singleton"
							" QS[][] += [12.] " 
							(VK.BaseValue(T.CFloat(17.)));
		test_stmt_coll " Update - OutPC - Singleton"
							~env:["A", (T.CFloat(1.));
										"B", (T.CFloat(1.));"C", (T.CFloat(2.));]
							" QR[][B,C] += W[A,A][B,C] * [3.] " 
							[  [1.; 1.], 1.;
				         [1.; 2.], 8.;
				         [1.; 3.], 3.;
				         [2.; 1.], 2.;
				         [2.; 2.], 4.;
				      ];
		test_stmt_coll " Update - InPC - Singleton"
							" QT[X,Y][] += [X  + Y] " 
							[  [1.; 1.], 3.;
				         [1.; 2.], 5.;
				         [1.; 3.], 7.;
				         [2.; 1.], 5.;
				         [2.; 2.], 8.;
				      ];
		test_stmt_coll " Update - PC - Singleton"
							~env:["A", (T.CFloat(1.));
										"B", (T.CFloat(1.));"C", (T.CFloat(2.));]
							" QW[X,Y][B,C] += W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 1.; [1.; 1.; 1.; 2.], 8.;
				         [2.; 2.; 2.; 1.], 2.; [2.; 2.; 2.; 2.], 4.; [2.; 2.; 1.; 1.], 1.; [2.; 2.; 1.; 2.], 8.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 1.; [3.; 3.; 1.; 2.], 8.;
				      ];		
		test_stmt_coll " Update - OutPC - Collection"
							~env:["A", (T.CFloat(2.));]
							" QR[][B,C] += W[A,A][B,C] * [3.] " 
							[  [1.; 1.], 1.;
				         [1.; 2.], 2.;
				         [1.; 3.], 3.;
				         [2.; 1.], 8.;
				         [2.; 2.], 16.;
				      ];
		test_stmt_coll " Update - PC - Collection"
							~env:["A", (T.CFloat(1.));]
							" QW[X,Y][B,C] += W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 4.; [1.; 1.; 1.; 2.], 8.;
				         [2.; 2.; 2.; 1.], 2.; [2.; 2.; 2.; 2.], 4.; [2.; 2.; 1.; 1.], 4.; [2.; 2.; 1.; 2.], 8.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 4.; [3.; 3.; 1.; 2.], 8.;  
				      ];		
		test_stmt_coll " Update - OutPC - Slice"
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(2.));]
							" QR[][B,C] += W[A,A][B,C] * [3.] " 
							[  [1.; 1.], 1.;
				         [1.; 2.], 2.;
				         [1.; 3.], 3.;
				         [2.; 1.], 8.;
				         [2.; 2.], 16.;
				      ];
		test_stmt_coll " Update - PC - Slice"
							~env:["A", (T.CFloat(1.));"B", (T.CFloat(1.));]
							" QW[X,Y][B,C] += W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 4.; [1.; 1.; 1.; 2.], 8.;
				         [2.; 2.; 2.; 1.], 2.; [2.; 2.; 2.; 2.], 4.; [2.; 2.; 1.; 1.], 4.; [2.; 2.; 1.; 2.], 8.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 4.; [3.; 3.; 1.; 2.], 8.;  
				      ];
		
		Debug.activate "M3ToK3-GENERATE-INIT";
		test_stmt_coll " Update - PC - Singleton Init "
							~env:["A", (T.CFloat(2.));
										"B", (T.CFloat(2.));"C", (T.CFloat(1.));]
							" QW[X,Y][B,C] ( R[][B,C] * [3] ) += W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 1.; [1.; 1.; 1.; 2.], 2.; [1.; 1.; 2.; 1.], 12.;
				         [2.; 2.; 2.; 1.], 8.; [2.; 2.; 2.; 2.], 4.; [2.; 2.; 1.; 1.], 1.; [2.; 2.; 1.; 2.], 2.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 1.; [3.; 3.; 1.; 2.], 2.; [3.; 3.; 2.; 1.], 12.;
				      ];		
		test_stmt_coll " Update - PC - Collection Init "
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(2.));]
							" QW[X,Y][B,C] ( R[][B,C] * [3] ) += W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 1.; [1.; 1.; 1.; 2.], 2.; [1.; 1.; 2.; 1.], 12.; [1.; 1.; 2.; 2.], 24.;
				         [2.; 2.; 2.; 1.], 8.; [2.; 2.; 2.; 2.], 16.; [2.; 2.; 1.; 1.], 1.; [2.; 2.; 1.; 2.], 2.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 1.; [3.; 3.; 1.; 2.], 2.; [3.; 3.; 2.; 1.], 12.; [3.; 3.; 2.; 2.], 24.;
				      ];		
		test_stmt_coll " Update - PC - Slice Init "
							~env:["A", (T.CFloat(2.));]
							" QW[X,Y][B,C] ( R[][B,C] * [3] ) += W[A,A][B,C] * [3.] " 
							[  [1.; 1.; 1.; 1.], 1.; [1.; 1.; 1.; 2.], 2.; [1.; 1.; 2.; 1.], 12.; [1.; 1.; 2.; 2.], 24.;
				         [2.; 2.; 2.; 1.], 8.; [2.; 2.; 2.; 2.], 16.; [2.; 2.; 1.; 1.], 1.; [2.; 2.; 1.; 2.], 2.;
								 [3.; 3.; 1.; 3.], 3.; [3.; 3.; 1.; 1.], 1.; [3.; 3.; 1.; 2.], 2.; [3.; 3.; 2.; 1.], 12.; [3.; 3.; 2.; 2.], 24.;
				      ];		
		Debug.deactivate "M3ToK3-GENERATE-INIT";
		()