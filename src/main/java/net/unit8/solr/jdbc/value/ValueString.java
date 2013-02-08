package net.unit8.solr.jdbc.value;


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
		ValueString obj = new ValueString(s);
		return obj;
	}

	/**
	 * TODO エスケープ
	 */
	@Override
	public String getQueryString() {
		return "\""+value+"\"";
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
