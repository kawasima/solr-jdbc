package net.unit8.solr.jdbc.impl;

import java.util.List;

import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.ColumnExpression;


/**
 * ResultSet for facet query
 *
 * @author kawasima
 *
 */
public class FacetResultSetImpl extends AbstractResultSet {
	public FacetResultSetImpl(QueryResponse response, List<Expression> expressions) {
		this.metaData = new ResultSetMetaDataImpl(this, expressions, null);
		docList = new SolrDocumentList();
		for (FacetField field : response.getFacetFields()) {
			try {
				int columnIndex = metaData.findColumn(new ColumnExpression(field.getName()).getColumnName());
				String columnName = metaData.getSolrColumnName(columnIndex);
				if (field.getValueCount() == 0)
					continue;
				for (Count count : field.getValues()) {
					SolrDocument doc = new SolrDocument();
					doc.setField(columnName, count.getName());
					for(Expression countColumn : metaData.getCountColumnList()) {
						doc.setField(countColumn.getResultName(), count.getCount());
					}
					docList.add(doc);
				}
			} catch(Exception ignore) {}
		}
	}

}
