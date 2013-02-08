package net.unit8.solr.jdbc.parser;

import java.sql.Date;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.FunctionExpression;
import net.unit8.solr.jdbc.expression.ValueExpression;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.SolrValue;
import net.unit8.solr.jdbc.value.ValueDate;
import net.unit8.solr.jdbc.value.ValueDouble;
import net.unit8.solr.jdbc.value.ValueLong;
import net.unit8.solr.jdbc.value.ValueNull;
import net.unit8.solr.jdbc.value.ValueString;

public class ExpressionParser implements ExpressionVisitor {

	private Expression expression;
	private boolean isParameter;
	private DatabaseMetaDataImpl metaData;
	private String tableName;

	public ExpressionParser() {
	}

	public ExpressionParser(String talbeName, DatabaseMetaDataImpl metaData) {
		this.tableName = talbeName;
		this.metaData = metaData;
	}

	public Expression getExpression() {
		return this.expression;
	}

	public boolean isParameter() {
		return isParameter;
	}

	@Override
	public void visit(NullValue val) {
		expression = ValueExpression.get(ValueNull.INSTANCE);
	}

	@Override
	public void visit(Function func) {
		expression = new FunctionExpression(func);
	}

	@Override
	public void visit(InverseExpression val) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(JdbcParameter param) {
		isParameter = true;
	}

	@Override
	public void visit(DoubleValue val) {
		SolrValue value = ValueDouble.get(val.getValue());
		expression = ValueExpression.get(value);
	}

	@Override
	public void visit(LongValue val) {
		SolrValue value = ValueLong.get(val.getValue());
		expression = ValueExpression.get(value);
	}

	@Override
	public void visit(DateValue val) {
		SolrValue value = ValueDate.get(val.getValue());
		expression = ValueExpression.get(value);
	}

	@Override
	public void visit(TimeValue val) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "Time Literal:"+val);
	}

	@Override
	public void visit(TimestampValue val) {
		SolrValue value = ValueDate.get(new Date(val.getValue().getTime()));
		expression = ValueExpression.get(value);
	}

	@Override
	public void visit(Parenthesis arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(StringValue val) {
		SolrValue value = ValueString.get(val.getValue());
		expression = ValueExpression.get(value);
	}

	@Override
	public void visit(Addition arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "+");
	}

	@Override
	public void visit(Division arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "/");
	}

	@Override
	public void visit(Multiplication arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "*");
	}

	@Override
	public void visit(Subtraction arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "-");
	}

	@Override
	public void visit(AndExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(OrExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(Between arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(EqualsTo arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(GreaterThan arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(InExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(LikeExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(MinorThan arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(Column column) {
		expression = metaData.getSolrColumn(tableName, column.getColumnName());
		if(expression == null)
			throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, column.getColumnName());
	}

	@Override
	public void visit(SubSelect arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(CaseExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(WhenClause arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(Concat arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "concat");
	}

	@Override
	public void visit(Matches arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "matches");
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "&");
	}

	@Override
	public void visit(BitwiseOr arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "|");
	}

	@Override
	public void visit(BitwiseXor arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "^");
	}
}
