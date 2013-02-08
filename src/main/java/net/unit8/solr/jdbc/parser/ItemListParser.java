package net.unit8.solr.jdbc.parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import net.unit8.solr.jdbc.expression.Item;
import net.unit8.solr.jdbc.expression.Literal;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.expression.ValueExpression;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;

public class ItemListParser implements ItemsListVisitor{
	private List<Parameter> parameters;
	private List<Item> itemList;

	public ItemListParser() {
		this.itemList = new ArrayList<Item>();
		this.parameters = new ArrayList<Parameter>();
	}
	
	public ItemListParser(List<Parameter> parameters) {
		this();
		this.parameters.addAll(parameters);
	}
	
	@Override
	public void visit(SubSelect subselect) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "SubQuery");
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void visit(ExpressionList expressionList) {
		ExpressionParser expressionParser = new ExpressionParser();
		for(Expression expression : (List<Expression>)expressionList.getExpressions()) {
			expression.accept(expressionParser);
			if (expressionParser.isParameter()) {
				Parameter p = new Parameter(parameters.size());
				parameters.add(p);
				itemList.add(p);
			} else {
				if (expressionParser.getExpression() instanceof ValueExpression) {
					itemList.add(new Literal(((ValueExpression)expressionParser.getExpression()).getValue()));
				} else {
					DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "itemList must be literal.");
				}
			}
		}
		
	}

	public List<Parameter> getParameters() {
		return parameters;
	}

	public List<Item> getItemList() {
		return itemList;
	}
}
