package net.unit8.solr.jdbc.command;

import net.sf.jsqlparser.statement.select.Select;
import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.FunctionExpression;
import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.impl.DatabaseMetaDataImpl;
import net.unit8.solr.jdbc.impl.DefaultResultSetImpl;
import net.unit8.solr.jdbc.impl.FacetResultSetImpl;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.parser.SelectParser;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.util.ArrayList;
import java.util.Map;


public class SelectCommand extends Command {
	private transient final Select select;
	private SelectParser selectParser;

	public SelectCommand(Select statement) {
		this.select = statement;
		this.parameters = new ArrayList<Parameter>();
	}

	@Override
	public boolean isQuery() {
		return true;
	}

	@Override
	public void parse() {
		DatabaseMetaDataImpl metaData= this.conn.getMetaDataImpl();
		selectParser = new SelectParser(metaData);
		select.getSelectBody().accept(selectParser);

		Map<String, String> options = selectParser.getSolrOptions();
		if(!selectParser.hasGroupBy()) {
			for(Expression expression : selectParser.getExpressions()) {
				if(expression instanceof FunctionExpression &&
					StringUtils.equalsIgnoreCase(((FunctionExpression)expression).getFunctionName(), "count")) {
					options.put("rows", "0");
				}
			}
		}

		parameters = selectParser.getParameters();
	}

	@Override
	public AbstractResultSet executeQuery() {
		SolrQuery query = selectParser.getQuery(parameters);
		Map<String,String> options = selectParser.getSolrOptions();
		for(Map.Entry<String, String> entry:options.entrySet()) {
			query.set(entry.getKey(), entry.getValue());
		}
		AbstractResultSet rs = null;
		QueryResponse response;
		try {
			response = conn.getSolrServer().query(query);
			if(selectParser.hasGroupBy()) {
				rs = new FacetResultSetImpl(response, selectParser.getExpressions());
			} else {
				rs = new DefaultResultSetImpl(response.getResults(), selectParser.getExpressions());
			}
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e,
					"Solr Server Error:" + e.getMessage());
		}

		return rs;
	}

	@Override
	public int executeUpdate() {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
	}

}
