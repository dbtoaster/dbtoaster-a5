module T = Types
module MK = M3ToK3
module U = UnitTest
module Vs = Values
module VK = Vs.K3Value
module Interpreter = K3Compiler.Make(K3Interpreter.K3CG)
;;

let db = Database.NamedK3Database.make_empty_db [] []
;;

let test_expr ?(env = []) msg calc_s rval =
	 let calc = U.parse_calc calc_s in
	 let env_vars = List.map (fun (vn,vl) -> (vn,T.type_of_const vl)) env in
	 let env_el = MK.varIdType_to_k3_expr env_vars in
	 let (_,_,code),_ = MK.calc_to_k3_expr MK.empty_meta env_el calc in
	
   let compiled = Interpreter.compile_k3_expr code in
   let (vars, vals) = List.split env in
   U.log_test ("M3ToK3("^msg^")")
      Vs.K3Value.string_of_value
      (  K3Interpreter.K3CG.eval 
            compiled 
            vars vals
            db)
      rval
in
		test_expr " AConst " 
							"[3]" (VK.BaseValue(T.CInt(3)))
		;
		test_expr " AVar " 
							~env:["A", (T.CFloat(3.))]
							"[A]" (VK.BaseValue(T.CFloat(3.)))
		;
		test_expr " Singleton Negation " 
							~env:["A", (T.CFloat(3.))]
							"[-A]" (VK.BaseValue(T.CFloat(-3.)))
		;
		test_expr " Singleton Sum " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A+B+A+B]" (VK.BaseValue(T.CFloat(12.)))
		;
		test_expr " Singleton Prod " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A*B*A*B]" (VK.BaseValue(T.CFloat(81.)))
		;
		test_expr " Cmp Eq " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A=B]" (VK.BaseValue(T.CInt(1)))
		;
		test_expr " Cmp Lt " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A<B]" (VK.BaseValue(T.CInt(0)))
		;
		test_expr " Cmp Lte " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A<=B]" (VK.BaseValue(T.CInt(1)))
		;
		test_expr " Cmp Gt " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A > (B - 1.)]" (VK.BaseValue(T.CInt(1)))
		;
		test_expr " Cmp Gte " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A>=B+1]" (VK.BaseValue(T.CInt(0)))
		;
		test_expr " Cmp Neq " 
							~env:["A", (T.CFloat(3.));"B", (T.CFloat(3.))]
							"[A!=B]" (VK.BaseValue(T.CInt(0)))
		;
		()