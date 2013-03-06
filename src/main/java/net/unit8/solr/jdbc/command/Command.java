package net.unit8.solr.jdbc.command;

import net.unit8.solr.jdbc.expression.Parameter;
import net.unit8.solr.jdbc.impl.AbstractResultSet;
import net.unit8.solr.jdbc.impl.SolrConnection;

import java.io.Serializable;
import java.util.List;


public abstract class Command implements Serializable {
	protected transient SolrConnection conn;
	protected List<Parameter> parameters;

	public abstract AbstractResultSet executeQuery();
	public abstract int executeUpdate();

	public void setConnection(SolrConnection conn) {
		this.conn = conn;
	}
	public List<Parameter> getParameters() {
		return parameters;
	}

	public abstract void parse();
	public abstract boolean isQuery();
	
}
