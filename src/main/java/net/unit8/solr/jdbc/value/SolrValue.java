package net.unit8.solr.jdbc.value;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;


public abstract class SolrValue {

	public SolrType getType() {
		return null;
	}

	public abstract Object getObject();
	public abstract String getString();
	public abstract String getQueryString();

	public Boolean getBoolean() {
		return ((ValueBoolean)convertTo(SolrType.BOOLEAN)).getBoolean();
	}

	public Date getDate() {
		return ((ValueDate)convertTo(SolrType.DATE)).getDate();
	}

	public Date getDateNoCopy() {
		return ((ValueDate)convertTo(SolrType.DATE)).getDateNoCopy();
	}

	public Timestamp getTimestamp() {
		Date d = getDate();
		return new Timestamp(d.getTime());
	}
	
	public BigDecimal getBigDecimal() {
		return ((ValueDecimal)convertTo(SolrType.DECIMAL)).getBigDecimal();
	}

	public int getInt() {
		return ((ValueInt) convertTo(SolrType.INT)).getInt();
	}

	public double getDouble() {
		return ((ValueDouble) convertTo(SolrType.DOUBLE)).getDouble();
	}
	
	public SolrValue convertTo(SolrType targetType) {
		if(getType() == targetType) {
			return this;
		}
		String s = getString();
		switch(targetType) {
		case NULL: {
			return ValueNull.INSTANCE;
		}
		case BOOLEAN: {
			return ValueBoolean.get(getSignum() != 0);
		}
		case INT: {
			return ValueInt.get(Integer.parseInt(s.trim()));
		}
		case LONG: {
			return ValueLong.get(Long.parseLong(s.trim()));
		}
		case DECIMAL: {
			return ValueDecimal.get(new BigDecimal(s.trim()));
		}
		case DATE: {
			return ValueDate.get(ValueDate.parseDate(s.trim()));
		}
		case STRING: {
			return ValueString.get(s);
		}
		case ARRAY: {
			return ValueArray.get(new SolrValue[]{ValueString.get(s)});
		}
		default:
			// TODO DbExceptionを使う
			throw new RuntimeException("データ変換エラー");
		}
	}

	public int getSignum() {
		throw new UnsupportedOperationException();
	}
}
