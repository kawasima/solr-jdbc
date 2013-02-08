package net.unit8.solr.jdbc.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;

import net.unit8.solr.jdbc.expression.ColumnExpression;
import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;


public class SelectParser implements SelectVisitor, FromItemVisitor, ItemsListVisitor{

	private ConditionParser conditionParser;

	private String tableName;
	private final Map<String, String> solrOptions;
	private final DatabaseMetaDataImpl metaData;
	private final List<String> selectColumns;
	private List<Expression> expressions;
	private boolean hasGroupBy = false;
	private SolrQuery solrQuery;

	public SelectParser(DatabaseMetaDataImpl metaData) {
		this.selectColumns = new ArrayList<String>();
		this.solrQuery = new SolrQuery();
		this.solrOptions = new HashMap<String, String>();
		this.metaData = metaData;
	}

	public String getTableName() {
		return tableName;
	}

	public Map<String,String> getSolrOptions() {
		return solrOptions;
	}
	public List<String> getSelectColumns() {
		return selectColumns;
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	public SolrQuery getQuery(List<Parameter> params) {
		String queryString;
		if(conditionParser == null) {
			// select all records if there is no WHERE clause.
			queryString = "id:@"+tableName+".*";
		} else {
			queryString = conditionParser.getQuery(params);
		}
		solrQuery.setQuery(queryString);

		return solrQuery;
	}

	public List<Parameter> getParameters() {
		if(conditionParser == null) {
			return new ArrayList<Parameter>();
		}
		return conditionParser.getParameters();
	}
	@SuppressWarnings("unchecked")
	@Override
	public void visit(PlainSelect plainSelect) {
		plainSelect.getFromItem().accept(this);


		// 取得するカラムの解析
		SelectItemFinder selectItemFinder = new SelectItemFinder(tableName, metaData);
		for(Object obj : plainSelect.getSelectItems()) {
			((SelectItem)obj).accept(selectItemFinder);
		}
		expressions = selectItemFinder.getExpressions();
		for(Expression expression : expressions) {
			if(expression instanceof ColumnExpression) {
				solrQuery.addField(expression.getSolrColumnName());
			}
		}

		// Where句の解析
		if (plainSelect.getWhere()!=null) {
			conditionParser = new ConditionParser(metaData);
			conditionParser.setTableName(tableName);
			plainSelect.getWhere().accept(conditionParser);
		}

		// ORDER BYの解析
		if (plainSelect.getOrderByElements() != null) {
			parseOrderBy(plainSelect.getOrderByElements());
		}

		// GROUP BYの解析
		List<Column> groupByColumns = plainSelect.getGroupByColumnReferences();
		if (groupByColumns != null) {
			parseGroupBy(groupByColumns);
			hasGroupBy = true;
		}

		// Limitの解析
		Limit limit = plainSelect.getLimit();
		if(limit != null) {
			solrOptions.put("start", Long.toString(limit.getOffset()));
			solrOptions.put("rows", Long.toString(limit.getRowCount()));
		} else {
			solrOptions.put("start", "0");
			solrOptions.put("rows", "10000");
		}
	}

	private void parseGroupBy(List<Column> groupByColumns) {
		for(Column column : groupByColumns) {
			solrOptions.put("facet", "true");
			Expression solrColumn = metaData.getSolrColumn(tableName, column.getColumnName());
			solrOptions.put("facet.field", solrColumn.getSolrColumnName());
		}
	}

	public void parseOrderBy(List<OrderByElement> orderByElements) {
		final List<String> sortColumns = new ArrayList<String>();
		for(Object elm : orderByElements) {
			final OrderByElement orderByElement = (OrderByElement)elm;
			orderByElement.getExpression().accept(new ExpressionVisitor() {

				@Override
				public void visit(BitwiseXor arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(BitwiseOr arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(BitwiseAnd arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Matches arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Concat arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(AnyComparisonExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(AllComparisonExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(ExistsExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(WhenClause arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(CaseExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(SubSelect arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Column col) {
					Expression solrColumn = metaData.getSolrColumn(tableName, col.getColumnName());
					String order = (orderByElement.isAsc()) ? "asc" : "desc";
					sortColumns.add(solrColumn.getSolrColumnName() + " " + order);
				}

				@Override
				public void visit(NotEqualsTo arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(MinorThanEquals arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(MinorThan arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(LikeExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(IsNullExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(InExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(GreaterThanEquals arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(GreaterThan arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(EqualsTo arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Between arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(OrExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(AndExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Subtraction arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Multiplication arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Division arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Addition arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(StringValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Parenthesis arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(TimestampValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(TimeValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(DateValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(LongValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(DoubleValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(JdbcParameter arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(InverseExpression arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(Function arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}

				@Override
				public void visit(NullValue arg0) {
					// TODO 自動生成されたメソッド・スタブ

				}
			});
		}
		solrOptions.put("sort", StringUtils.join(sortColumns.iterator(), ","));
	}
	@Override
	public void visit(Union union) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "union");
	}

	@Override
	public void visit(Table table){
		if(!metaData.hasTable(table.getName()))
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, table.getName());

		tableName = metaData.getOriginalTableName(table.getName());
	}

	@Override
	public void visit(SubSelect arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "subquery");
	}

	@Override
	public void visit(SubJoin arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "subjoin");
	}


	@Override
	public void visit(ExpressionList arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "expressionList");
	}

	public boolean hasGroupBy() {
		return hasGroupBy ;
	}

}
