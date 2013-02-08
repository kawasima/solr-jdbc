package net.unit8.solr.jdbc.parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;


public class SelectItemFinder implements SelectItemVisitor {
	private final String tableName;
	private final DatabaseMetaDataImpl dbMetaData;
	private final List<Expression> expressions;

	public SelectItemFinder(String tableName, DatabaseMetaDataImpl metaData) {
		this.tableName  = tableName;
		this.dbMetaData = metaData;
		this.expressions = new ArrayList<Expression>();
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public void visit(AllColumns cols) {
		List<Expression> columnExpressions = dbMetaData.getSolrColumns(tableName);
		for(Expression columnExpression : columnExpressions) {
			expressions.add(columnExpression);
		}
	}

	@Override
	public void visit(AllTableColumns cols) {
	}

	@Override
	public void visit(SelectExpressionItem selectItem) {
		ExpressionParser expressionParser = new ExpressionParser(tableName, dbMetaData);
		selectItem.getExpression().accept(expressionParser);
		Expression expression = expressionParser.getExpression();
		expression.setAlias(selectItem.getAlias());
		expressions.add(expression);
	}

}
