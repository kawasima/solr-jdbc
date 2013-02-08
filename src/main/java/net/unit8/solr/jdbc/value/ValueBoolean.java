package net.unit8.solr.jdbc.value;

public class ValueBoolean extends SolrValue {
	
	private static final Object TRUE  = new ValueBoolean(true);
	private static final Object FALSE = new ValueBoolean(false);
	
	private final Boolean value;
	private ValueBoolean(boolean b) {
		this.value = b;
	}
	
	public static ValueBoolean get(boolean b) {
		return (ValueBoolean) (b ? TRUE : FALSE);
	}
	
	@Override
	public SolrType getType() {
		return SolrType.BOOLEAN;
	}
	@Override
	public String getQueryString() {
		return value.toString();
	}

	@Override
	public String getString() {
		return value.toString();
	}

	@Override
	public Object getObject() {
		return value;
	}

}
