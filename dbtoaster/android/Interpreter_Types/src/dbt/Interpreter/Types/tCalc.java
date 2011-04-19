package dbt.Interpreter.Types;

import java.util.ArrayList;

public interface tCalc {
	
	public class Add implements tCalc {
		public tCalc s1 = null;
		public tCalc s2 = null;
		
		public Add() {
			
		}
		
		public Add(tCalc _s1, tCalc _s2) {
			s1 = _s1;
			s2 = _s2;
		}
	}
	
	public class Mult implements tCalc {
		public tCalc s1 = null;
		public tCalc s2 = null;
		
		public Mult() {
			
		}
		
		public Mult(tCalc _s1, tCalc _s2) {
			s1 = _s1;
			s2 = _s2;
		}
	}
	
	public class Const implements tCalc {
		// Does this here needs multi-typed as suggested in the comments of 
		// M3.ml, as CString of string, CInt of int, or CFloat of float?
		public int c = 0;
		
		public Const() {
			
		}
		
		public Const(int _c) {
			c = _c;
		}
	}
	
	public class Var implements tCalc {
		public String name = null;
		
		public Var() {
			
		}
		
		public Var(String _name) {
			name = _name;
		}
	}
	
	public class IfThenElse0 implements tCalc {
		public tCalc cond = null;
		public tCalc s = null;
		
		public IfThenElse0() {
			
		}
		
		public IfThenElse0(tCalc _cond, tCalc _s) {
			cond = _cond;
			s = _s;
		}
	}
	
	public class MapAccess implements tCalc {
		public tMapAcc mapAcc = null;
		
		public MapAccess() {
			
		}
		
		public MapAccess(tMapAcc _mapAcc) {
			mapAcc = _mapAcc;
		}
	}
	
	// Do we still need this?
	public class Null implements tCalc {
		public ArrayList<String> lst = null;
		
		public Null() {
			
		}
		
		public Null(ArrayList<String> _lst) {
			lst = _lst;
		}
	}
	
	public class LEQ implements tCalc {
		public tCalc s1 = null;
		public tCalc s2 = null;
		
		public LEQ() {
			
		}
		
		public LEQ(tCalc _s1, tCalc _s2) {
			s1 = _s1;
			s2 = _s2;
		}
	}
	
	public class EQ implements tCalc {
		public tCalc s1 = null;
		public tCalc s2 = null;
		
		public EQ() {
			
		}
		
		public EQ(tCalc _s1, tCalc _s2) {
			s1 = _s1;
			s2 = _s2;
		}
	}
	
	public class LT implements tCalc {
		public tCalc s1 = null;
		public tCalc s2 = null;
		
		public LT() {
			
		}
		
		public LT(tCalc _s1, tCalc _s2) {
			s1 = _s1;
			s2 = _s2;
		}
	}	
}
