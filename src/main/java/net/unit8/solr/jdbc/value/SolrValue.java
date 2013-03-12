package net.unit8.solr.jdbc.value;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;

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
		return convertTo(SolrType.BOOLEAN).getBoolean();
	}

	public Date getDate() {
		return convertTo(SolrType.DATE).getDate();
	}

	public Date getDateNoCopy() {
		return convertTo(SolrType.DATE).getDateNoCopy();
	}

	public Timestamp getTimestamp() {
		Date d = getDate();
		return new Timestamp(d.getTime());
	}
	
	public BigDecimal getBigDecimal() {
		return convertTo(SolrType.DECIMAL).getBigDecimal();
	}

	public int getInt() {
		return convertTo(SolrType.INT).getInt();
	}

	public double getDouble() {
		return convertTo(SolrType.DOUBLE).getDouble();
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
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
		}
	}

	public int getSignum() {
		throw new UnsupportedOperationException();
	}
}
