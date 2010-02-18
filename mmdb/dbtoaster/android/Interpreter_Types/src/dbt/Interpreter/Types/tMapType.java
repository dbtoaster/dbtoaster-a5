package dbt.Interpreter.Types;

import java.util.ArrayList;

public class tMapType {
	public String id = null;	// Map type could also be int?
	
	// Not sure if we should use Object.
	public ArrayList<Object> in_var = null;
	public ArrayList<Object> out_var = null;
	
	public tMapType() {
		
	}
	
	public tMapType(String _id, ArrayList<Object> _in_var, 
			ArrayList<Object> _out_var) {
		id = _id;
		in_var = _in_var;
		out_var = _out_var;
	}
}
