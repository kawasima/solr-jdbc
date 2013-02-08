package net.unit8.solr.jdbc.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import net.unit8.solr.jdbc.expression.ColumnExpression;
import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.SolrType;

public class CollectionResultSet extends AbstractResultSet {
	private List<String> columns;

	public CollectionResultSet() {
		this.docList = new SolrDocumentList();
		this.columns = new ArrayList<String>();
	}

	protected void add(List<Object> record) {
		SolrDocument doc = new SolrDocument();
		for (int i=0; i<columns.size(); i++) {
			doc.setField(columns.get(i), record.get(i));
		}
		docList.add(doc);
	}

	protected void setColumns(List<String> columnNames) {
		this.columns = columnNames;
		List<Expression> expressions = new ArrayList<Expression>();
		for(String columnName : columns) {
			ColumnExpression expression = new ColumnExpression(null, columnName, SolrType.UNKNOWN);
			expressions.add(expression);
		}
		this.metaData = new ResultSetMetaDataImpl(this, expressions, null);
	}
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		int i=0;
		for(; i<columns.size(); i++) {
			if (StringUtils.equals(columnLabel, columns.get(i))) {
				break;
			}
		}
		if(i==columns.size()) {
			throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, columnLabel);
		}
		return i+1;
	}

}
