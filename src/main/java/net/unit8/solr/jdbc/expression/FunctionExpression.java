package net.unit8.solr.jdbc.expression;

import net.sf.jsqlparser.expression.Function;



public class FunctionExpression extends Expression {
	private final String functionName;

	public FunctionExpression(Function function) {
		columnName = function.toString();
		functionName = function.getName();
	}

	public String getFunctionName() {
		return functionName;
	}

	@Override
	public long getPrecision() {
		return 0;
	}

	@Override
	public int getScale() {
		return 0;
	}


}
