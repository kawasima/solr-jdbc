package net.unit8.solr.jdbc.parser;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.unit8.solr.jdbc.expression.ColumnExpression;
import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SelectParser implements SelectVisitor, FromItemVisitor, ItemsListVisitor{

	private ConditionParser conditionParser;

	private String tableName;
	private final Map<String, String> solrOptions;
	private final DatabaseMetaDataImpl metaData;
	private final List<String> selectColumns;
    /** Expressions of SELECT items */
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

		// Parse WHERE clause
		if (plainSelect.getWhere()!=null) {
			conditionParser = new ConditionParser(metaData);
			conditionParser.setTableName(tableName);
			plainSelect.getWhere().accept(conditionParser);
		}

		// Parse ORDER BY clause
		if (plainSelect.getOrderByElements() != null) {
			parseOrderBy(plainSelect.getOrderByElements());
		}

		// Parse GROUP BY clause
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
				public void visit(BitwiseXor arg) {
					throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(BitwiseOr arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(BitwiseAnd arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Matches arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Concat arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(AnyComparisonExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(AllComparisonExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(ExistsExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(WhenClause arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(CaseExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(SubSelect arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Column col) {
					Expression solrColumn = metaData.getSolrColumn(tableName, col.getColumnName());
                    if (solrColumn == null) {
                        // Try to find the column aliases.
                        for (Expression selectItemExpression : expressions) {
                            String alias = selectItemExpression.getAlias();
                            if (StringUtils.equals(col.getColumnName(), alias)) {
                                solrColumn = selectItemExpression;
                                break;
                            }
                        }
                    }
                    if (solrColumn == null) {
                        throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, col.getColumnName());
                    }
					String order = (orderByElement.isAsc()) ? "asc" : "desc";
					sortColumns.add(solrColumn.getSolrColumnName() + " " + order);
				}

				@Override
				public void visit(NotEqualsTo arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(MinorThanEquals arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(MinorThan arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(LikeExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(IsNullExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(InExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(GreaterThanEquals arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(GreaterThan arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(EqualsTo arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Between arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(OrExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(AndExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Subtraction arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Multiplication arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Division arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Addition arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(StringValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Parenthesis arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(TimestampValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(TimeValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(DateValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(LongValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(DoubleValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(JdbcParameter arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(InverseExpression arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(Function arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
				}

				@Override
				public void visit(NullValue arg) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR, arg.toString());
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
