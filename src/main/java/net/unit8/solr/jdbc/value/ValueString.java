package net.unit8.solr.jdbc.value;


import org.apache.solr.client.solrj.util.ClientUtils;

public class ValueString extends SolrValue{
	private static final ValueString EMPTY = new ValueString("");
	
	protected final String value;
	
	protected ValueString(String value) {
		this.value = value;
	}
	
	public static ValueString get(String s) {
		if(s.length() == 0) {
			return EMPTY;
		}
		return new ValueString(s);
	}

	@Override
	public String getQueryString() {
		return "\"" + ClientUtils.escapeQueryChars(value) + "\"";
	}
	
	@Override
	public SolrType getType() {
		return SolrType.STRING;
	}

	@Override
	public String getString() {
		return value;
	}

	@Override
	public Object getObject() {
		return value;
	}
	
}
