package dbt.Interpreter.Types;

import java.util.ArrayList;

public class tProgram {
	public ArrayList<tMapType> maps = null;
	public ArrayList<tTrig> trigers = null;
	
	public tProgram() { 
		
	}
	
	public tProgram(ArrayList<tMapType> _maps, ArrayList<tTrig> _trigers) {
		maps = _maps;
		trigers = _trigers;
	}
}