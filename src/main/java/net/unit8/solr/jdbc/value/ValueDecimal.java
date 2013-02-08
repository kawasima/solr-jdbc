package net.unit8.solr.jdbc.value;

import java.math.BigDecimal;


public class ValueDecimal extends SolrValue{
	public static final Object ZERO = new ValueDecimal(BigDecimal.ZERO);
	public static final Object ONE = new ValueDecimal(BigDecimal.ONE);
	
	private final BigDecimal value;
	private String valueString;
	
	private ValueDecimal(BigDecimal value) {
		if(value == null) {
			throw new IllegalArgumentException();
		}
		this.value = value;
	}
	@Override
	public String getQueryString() {
		return getString();
	}
	
	@Override
	public int getSignum() {
		return value.signum();
	}
	@Override
	public SolrType getType() {
		return SolrType.DECIMAL;
	}

	@Override
	public String getString() {
		if (valueString == null) {
			valueString = value.toString();
		}
		return valueString;
	}

	public static ValueDecimal get(BigDecimal dec) {
		if(BigDecimal.ZERO.equals(dec)) {
			return (ValueDecimal)ZERO;
		} else if (BigDecimal.ONE.equals(dec)) {
			return (ValueDecimal)ONE;
		}
		return new ValueDecimal(dec);
	}
	@Override
	public Object getObject() {
		return value;
	}
}
