package dbt.Interpreter.Types;

import java.util.ArrayList;

public class tMapAcc {
	public String id = null;
	public ArrayList<String> in_var = null;
	public ArrayList<String> out_var = null;
	public tCalc calc = null;
	
	public tMapAcc() {
		
	}
	
	public tMapAcc(String _id, ArrayList<String> _in_var, 
			ArrayList<String> _out_var, tCalc _calc) {
		id = _id;
		in_var = _in_var;
		out_var = _out_var;
		calc = _calc;
	}
}
