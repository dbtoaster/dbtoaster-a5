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
]
;;
let pc_s = K.SingletonPC("S", K.TBase(T.TFloat))
let pc_r = K.OutPC("R", ["A", K.TBase(T.TFloat); "B", K.TBase(T.TFloat)], K.TBase(T.TFloat))
let pc_t = K.InPC("T", ["A", K.TBase(T.TFloat); "B", K.TBase(T.TFloat)], K.TBase(T.TFloat))
let pc_w = K.PC("W", ["A", K.TBase(T.TFloat); "B", K.TBase(T.TFloat)], 
										 ["C", K.TBase(T.TFloat); "D", K.TBase(T.TFloat)], K.TBase(T.TFloat))
;;

let patterns = [
   "R", [Patterns.Out(["A"], [0])];
	 "W", [Patterns.Out(["C"], [0])];
]
;;

let db = Database.NamedK3Database.make_empty_db maps patterns
;;

let init_code = [
		K.PCValueUpdate(pc_s, [], [], (K.Const(T.CFloat(5.))));
		
	  K.PCValueUpdate(pc_r, [], [K.Const(T.CFloat(1.)); K.Const(T.CFloat(1.))], (K.Const(T.CFloat(1.))));
 		K.PCValueUpdate(pc_r, [], [K.Const(T.CFloat(1.)); K.Const(T.CFloat(2.))], (K.Const(T.CFloat(2.))));
 		K.PCValueUpdate(pc_r, [], [K.Const(T.CFloat(2.)); K.Const(T.CFloat(1.))], (K.Const(T.CFloat(2.))));
 		K.PCValueUpdate(pc_r, [], [K.Const(T.CFloat(2.)); K.Const(T.CFloat(2.))], (K.Const(T.CFloat(4.))));
 		K.PCValueUpdate(pc_r, [], [K.Const(T.CFloat(1.)); K.Const(T.CFloat(3.))], (K.Const(T.CFloat(3.))));

	  K.PCValueUpdate(pc_t, [K.Const(T.CFloat(1.)); K.Const(T.CFloat(1.))], [], (K.Const(T.CFloat(1.))));
 		K.PCValueUpdate(pc_t, [K.Const(T.CFloat(1.)); K.Const(T.CFloat(2.))], [], (K.Const(T.CFloat(2.))));
 		K.PCValueUpdate(pc_t, [K.Const(T.CFloat(2.)); K.Const(T.CFloat(1.))], [], (K.Const(T.CFloat(2.))));
 		K.PCValueUpdate(pc_t, [K.Const(T.CFloat(2.)); K.Const(T.CFloat(2.))], [], (K.Const(T.CFloat(4.))));
 		K.PCValueUpdate(pc_t, [K.Const(T.CFloat(1.)); K.Const(T.CFloat(3.))], [], (K.Const(T.CFloat(3.))));
		
		K.PCValueUpdate(pc_w, [K.Const(T.CFloat(1.)); K.Const(T.CFloat(1.))], 
													[K.Const(T.CFloat(1.)); K.Const(T.CFloat(1.))], (K.Const(T.CFloat(1.))));
 		K.PCValueUpdate(pc_w, [K.Const(T.CFloat(1.)); K.Const(T.CFloat(1.))], 
													[K.Const(T.CFloat(1.)); K.Const(T.CFloat(2.))], (K.Const(T.CFloat(2.))));
 		K.PCValueUpdate(pc_w, [K.Const(T.CFloat(2.)); K.Const(T.CFloat(2.))], 
													[K.Const(T.CFloat(2.)); K.Const(T.CFloat(1.))], (K.Const(T.CFloat(2.))));
 		K.PCValueUpdate(pc_w, [K.Const(T.CFloat(2.)); K.Const(T.CFloat(2.))], 
													[K.Const(T.CFloat(2.)); K.Const(T.CFloat(2.))], (K.Const(T.CFloat(4.))));
 		K.PCValueUpdate(pc_w, [K.Const(T.CFloat(3.)); K.Const(T.CFloat(3.))], 
													[K.Const(T.CFloat(1.)); K.Const(T.CFloat(3.))], (K.Const(T.CFloat(3.))));
]
in

let calc_string_to_k3_code ?(env = []) calc_s =
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
	 code
in

let test_expr ?(env = []) msg calc_s rval =
	 let code = calc_string_to_k3_code ~env:env calc_s in	
   let compiled = Interpreter.compile_k3_expr (K.Block(init_code@[code])) in
   let (vars, vals) = List.split env in
   U.log_test ("M3ToK3("^msg^")")
      Vs.K3Value.string_of_value
      (  K3Interpreter.K3CG.eval 
            compiled 
            vars vals
            db)
      rval
in

let test_expr_coll ?(env = []) msg calc_s rval =
   let code = calc_string_to_k3_code ~env:env calc_s in	
   let compiled = Interpreter.compile_k3_expr code in
   let (vars, vals) = List.split env in
      U.log_collection_test ("M3ToK3("^msg^")")
         (  K3Interpreter.K3CG.eval 
               compiled 
               vars vals
               db)
         rval
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
		
		Debug.activate "M3ToK3-GENERATE-INIT";
		test_expr " External - OutPC - Singleton Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.))]
							"R[][A,B]" (VK.BaseValue(T.CFloat(0.)));
		test_expr " External - InPC - Singleton Init " 
							~env:["A", (T.CFloat(2.));"B", (T.CFloat(3.));"D", (T.CFloat(1.))]
							"T[A,B][]( R[][A,D] * [3] )" (VK.BaseValue(T.CFloat(6.)));
		Debug.deactivate "M3ToK3-GENERATE-INIT";
		
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
							~env:["A", (T.CFloat(2.))]
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
		()