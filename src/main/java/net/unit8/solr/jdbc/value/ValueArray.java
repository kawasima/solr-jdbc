package net.unit8.solr.jdbc.value;

import org.apache.commons.lang.StringUtils;

public class ValueArray extends SolrValue {
	private final SolrValue[] values;

	private ValueArray(SolrValue[] list) {
		this.values = list;
	}
	
	/**
	 * Get or create a array value for the given value array.
	 * Do not clone the data.
	 * 
	 * @param list the value array
	 * @return the value
	 */
	public static ValueArray get(SolrValue[] list) {
		return new ValueArray(list);
	}
	
	public SolrValue[] getList() {
		return values;
	}
	
	@Override
	public SolrType getType() {
		return SolrType.ARRAY;
	}
	
	@Override
	public String getQueryString() {
		return values[0].getQueryString();
	}

	@Override
	public String getString() {
		String[] strValues = new String[values.length];
		for(int i=0; i<values.length; i++) {
			strValues[i] = values[i].getString();
		}
		return "[" + StringUtils.join(strValues, ',') + "]";
	}

	@Override
	public Object getObject() {
		Object[] list = new Object[values.length];
		for (int i=0; i < values.length; i++) {
			list[i] = values[i].getObject();
		}
		return list;
	}

}
