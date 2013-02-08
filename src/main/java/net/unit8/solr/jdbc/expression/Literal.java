package net.unit8.solr.jdbc.expression;

import net.unit8.solr.jdbc.value.SolrValue;

public class Literal implements Item {

	private SolrValue value;
	
	public Literal(SolrValue value) {
		this.value = value;
	}
	@Override
	public SolrValue getValue() {
		return value;
	}

}
