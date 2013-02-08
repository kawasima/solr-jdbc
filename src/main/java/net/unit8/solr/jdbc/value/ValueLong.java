package net.unit8.solr.jdbc.value;

public class ValueLong extends SolrValue {

	private final long value;
	
	private ValueLong(long value) {
		this.value = value;
	}
	
	@Override
	public int getSignum() {
		return Long.signum(value);
	}
	
	@Override
	public SolrType getType() {
		return SolrType.LONG;
	}

	@Override
	public Object getObject() {
		return value;
	}

	@Override
	public String getQueryString() {
		return getString();
	}

	@Override
	public String getString() {
		return String.valueOf(value);
	}
	
	public static ValueLong get(long i) {
		return new ValueLong(i);
	}

}
