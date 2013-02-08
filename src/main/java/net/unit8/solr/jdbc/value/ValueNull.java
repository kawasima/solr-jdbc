package net.unit8.solr.jdbc.value;

import java.math.BigDecimal;
import java.sql.Date;

public class ValueNull extends SolrValue {
	public static final ValueNull INSTANCE = new ValueNull();

	public static final ValueNull DELETED = new ValueNull();

	private ValueNull() {

	}

	@Override
	public BigDecimal getBigDecimal() {
		return null;
	}
	
	@Override
	public int getInt() {
		return 0;
	}
	
	@Override
	public SolrType getType() {
		return SolrType.NULL;
	}
	@Override
	public String getQueryString() {
		return null;
	}

	@Override
	public String getString() {
		return null;
	}

	@Override
	public Object getObject() {
		return null;
	}

	@Override
	public Date getDate() {
		return null;
	}
}
