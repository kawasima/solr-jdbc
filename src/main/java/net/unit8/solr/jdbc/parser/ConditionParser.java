package net.unit8.solr.jdbc.parser;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import org.apache.commons.lang.StringUtils;

import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.Item;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.ValueDate;
import net.unit8.solr.jdbc.value.ValueDouble;
import net.unit8.solr.jdbc.value.ValueLong;

public class ConditionParser implements ExpressionVisitor {

	private final StringBuilder query;
	private List<Parameter> parameters;
	private String tableName;
	private final DatabaseMetaDataImpl metaData;
	private String likeEscapeChar;
	private ParseContext context = ParseContext.NONE;
	private Expression currentColumn = null;
	private static final Pattern pattern = Pattern.compile("\\?(\\d+)");

	public ConditionParser(DatabaseMetaDataImpl metaData) {
		this.metaData = metaData;
		query = new StringBuilder();
		parameters = new ArrayList<Parameter>();
	}

	public ConditionParser(DatabaseMetaDataImpl metaData, List<Parameter> parameters) {
		this(metaData);
		this.parameters.addAll(parameters);
	}

	public void setTableName(String tableName) {
		this.tableName = metaData.getOriginalTableName(tableName);
	}

	public List<Parameter> getParameters() {
		return parameters;
	}

	public String getQuery(List<Parameter> params) {
		String queryString;
		if(query.length() == 0) {
			queryString = "id:@"+tableName+".*";
		} else {
			queryString = query.toString();
		}

		Matcher matcher = pattern.matcher(queryString);
		StringBuffer sb = new StringBuffer();
		while(matcher.find()) {
			String index = matcher.group(1);
			int paramIndex = Integer.parseInt(index);
			String paramStr = params.get(paramIndex).getQueryString();
			matcher.appendReplacement(sb, paramStr);
		}
		matcher.appendTail(sb);

		return sb.toString();
	}

	@Override
	public void visit(SubSelect arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "subquery");
	}

	@Override
	public void visit(NullValue arg0) {
		query.append("\"\"");
	}

	@Override
	public void visit(Function func) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, func.getName());
	}

	@Override
	public void visit(InverseExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "-");
	}

	@Override
	public void visit(JdbcParameter ph) {
		Parameter p = new Parameter(parameters.size());
		if(context == ParseContext.LIKE) {
			p.setNeedsLikeEscape();
			if(StringUtils.isNotBlank(likeEscapeChar)) {
				p.setLikeEscapeChar(likeEscapeChar);
			}
		}
		p.setColumn(currentColumn);
		parameters.add(p);
 		query.append("?").append(p.getIndex());
	}

	@Override
	public void visit(DoubleValue value) {
		query.append(ValueDouble.get(value.getValue()).getQueryString());
	}

	@Override
	public void visit(LongValue value) {
		query.append(ValueLong.get(value.getValue()).getQueryString());
	}

	@Override
	public void visit(DateValue value) {
		ValueDate d = ValueDate.get(value.getValue());
		query.append(d.getQueryString());
	}

	@Override
	public void visit(TimeValue value) {
		query.append(value.getValue().toString());
	}

	@Override
	public void visit(TimestampValue value) {
		ValueDate d = ValueDate.get(new Date(value.getValue().getTime()));
		query.append(d.getQueryString());
	}

	@Override
	public void visit(Parenthesis expr) {
		query.append("(");
		expr.getExpression().accept(this);
		query.append(")");
	}

	@Override
	public void visit(StringValue value) {
		query.append(value.getValue());
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
	public void visit(AndExpression expr) {
		expr.getLeftExpression().accept(this);
		query.append(" AND ");
		expr.getRightExpression().accept(this);
	}

	@Override
	public void visit(OrExpression expr) {
		expr.getLeftExpression().accept(this);
		query.append(" OR ");
		expr.getRightExpression().accept(this);
	}

	@Override
	public void visit(Between expr) {
		context = ParseContext.BETWEEN;
		expr.getLeftExpression().accept(this);
		query.append(":[");
		expr.getBetweenExpressionStart().accept(this);
		query.append(" TO ");
		expr.getBetweenExpressionEnd().accept(this);
		query.append("] ");
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(EqualsTo expr) {
		context = ParseContext.EQUAL;
		expr.getLeftExpression().accept(this);
		query.append(":");
		expr.getRightExpression().accept(this);
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(GreaterThan expr) {
		context = ParseContext.GREATER_THAN;
		expr.getLeftExpression().accept(this);
		query.append(":{");
		expr.getRightExpression().accept(this);
		query.append(" TO *} ");
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(GreaterThanEquals expr) {
		context = ParseContext.GREATER_THAN_EQUAL;
		expr.getLeftExpression().accept(this);
		query.append(":[");
		expr.getRightExpression().accept(this);
		query.append(" TO *] ");
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(InExpression expr) {
		context = ParseContext.IN;
		expr.getLeftExpression().accept(this);
		query.append(":(");
		ItemListParser itemListParser = new ItemListParser(parameters);
		expr.getItemsList().accept(itemListParser);
		parameters = itemListParser.getParameters();

		for(Item item : itemListParser.getItemList()) {
			if(item instanceof Parameter) {
				query.append("?").append(((Parameter)item).getIndex());
			} else {
				query.append(item.getValue().getQueryString()).append(" ");
			}
		}
		query.append(") ");
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "is null");
	}

	@Override
	public void visit(LikeExpression expr) {
		context = ParseContext.LIKE;
		expr.getLeftExpression().accept(this);
		query.append(":");
		likeEscapeChar = expr.getEscape();
		expr.getRightExpression().accept(this);
		likeEscapeChar = null;
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(MinorThan expr) {
		context = ParseContext.MINOR_THAN;
		expr.getLeftExpression().accept(this);
		query.append(":{* TO ");
		expr.getRightExpression().accept(this);
		query.append("}");
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(MinorThanEquals expr) {
		context = ParseContext.MINOR_THAN_EQUAL;
		expr.getLeftExpression().accept(this);
		query.append(":[* TO ");
		expr.getRightExpression().accept(this);
		query.append("]");
		context = ParseContext.NONE;
		currentColumn = null;
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "!=");
	}

	@Override
	public void visit(Column column) {
		Expression solrColumn = metaData.getSolrColumn(tableName, column.getColumnName());
		if (solrColumn == null) {
			throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, column.getColumnName());
		}
		query.append(solrColumn.getSolrColumnName());
		currentColumn = solrColumn;

	}

	@Override
	public void visit(CaseExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "case");
	}

	@Override
	public void visit(WhenClause arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "when");
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "exists");
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "all");
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "any");
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
