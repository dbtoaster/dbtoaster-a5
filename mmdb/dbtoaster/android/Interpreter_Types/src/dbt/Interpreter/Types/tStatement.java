package dbt.Interpreter.Types;

public class tStatement {
	public tMapAcc mapAcc = null;
	public tCalc   calc = null;
	
	public tStatement() {
		
	}
	
	public tStatement(tMapAcc _mapAcc, tCalc _calc) {
		mapAcc = _mapAcc;
		calc = _calc;
	}
}
