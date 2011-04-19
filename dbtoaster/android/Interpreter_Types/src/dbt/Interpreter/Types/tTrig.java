package dbt.Interpreter.Types;

import java.util.ArrayList;

public class tTrig {
	public enum tPM {Insert, Delete};
	
	public tPM trigerType = null;
	public String id = null; 
	
	public ArrayList<String> arguments = null;
	public ArrayList<tStatement> statements = null;
	
	public tTrig() {
		
	}
	
	public tTrig(tPM _trigerType, String _id, ArrayList<String> _arguments,
			ArrayList<tStatement> _statements) {
		trigerType = _trigerType;
		id = _id;
		arguments = _arguments;
		statements = _statements;
	}
}
