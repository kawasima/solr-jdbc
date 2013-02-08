package net.unit8.solr.jdbc.value;

public class ValueInt extends SolrValue {

	private final int value;

	private ValueInt(int value) {
		this.value = value;
	}

	@Override
	public SolrType getType() {
		return SolrType.INT;
	}

	@Override
	public int getSignum() {
		return Integer.signum(value);
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

	@Override
	public int getInt() {
		return value;
	}

	public static ValueInt get(int i) {
		return new ValueInt(i);
	}
}
