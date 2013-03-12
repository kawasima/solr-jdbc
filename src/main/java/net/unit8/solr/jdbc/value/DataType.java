package net.unit8.solr.jdbc.value;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Mapping solr types to sql types, vice-versa
 *
 * @author kawasima
 *
 */
public class DataType {
	private static final HashMap<String, DataType> TYPES_BY_NAME = new HashMap<String, DataType>();
	private static final ArrayList<DataType> TYPES_BY_VALUE_TYPE = new ArrayList<DataType>();
	public SolrType type;
	public int sqlType;
	public String jdbc;
	public String name;

	static {
		for (int i = 0; i < SolrType.values().length; i++) {
			TYPES_BY_VALUE_TYPE.add(null);
		}
 		add(SolrType.NULL, Types.NULL, "Null", new String[]{"NULL"});
 		add(SolrType.BOOLEAN, Types.BOOLEAN, "Boolean", new String[]{"BOOLEAN", "BIT", "BOOL", "TINYINT"});
 		add(SolrType.INT, Types.INTEGER, "Int", new String[]{"INT", "INTEGER"});
 		add(SolrType.LONG, Types.BIGINT, "Long", new String[]{"LONG", "BIGINT"});
 		add(SolrType.DECIMAL, Types.NUMERIC, "BigDecimal", new String[]{"NUMERIC","NUMBER"});
 		add(SolrType.DOUBLE, Types.DOUBLE, "Double", new String[]{"DOUBLE"});
 		add(SolrType.FLOAT, Types.FLOAT, "Float", new String[]{"FLOAT"});
 		add(SolrType.STRING, Types.VARCHAR, "String", new String[]{"VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2", "VARCHAR_CASESENSITIVE", "CHARACTER VARYING", "TID"});
 		add(SolrType.TEXT, Types.CLOB, "Clob", new String[]{"CLOB", "TEXT"});
 		add(SolrType.DATE, Types.DATE, "Date", new String[]{"DATE"});
 		add(SolrType.ARRAY, Types.ARRAY, "Array", new String[]{"ARRAY", "VARCHAR_ARRAY", "DATE_ARRAY", "INT_ARRAY", "BIGINT_ARRAY"});
	}

	private static void add(SolrType type, int sqlType, String jdbc, String[] names) {
		for(int i=0; i<names.length; i++) {
			DataType dt = new DataType();
			dt.type = type;
			dt.sqlType = sqlType;
			dt.jdbc = jdbc;
			dt.name = names[i];
			TYPES_BY_NAME.put(dt.name, dt);
			if(TYPES_BY_VALUE_TYPE.get(type.ordinal()) == null) {
				TYPES_BY_VALUE_TYPE.set(type.ordinal(), dt);
			}
		}

	}

	public static DataType getDataType(SolrType type) {
		DataType dt = TYPES_BY_VALUE_TYPE.get(type.ordinal());
		if (dt == null) {
			dt = TYPES_BY_VALUE_TYPE.get(SolrType.NULL.ordinal());
		}
		return dt;
	}

	public static int covertSolrTypeToSQLType(SolrType type) {
		DataType dt = TYPES_BY_VALUE_TYPE.get(type.ordinal());
		return dt.sqlType;
	}

	public static SolrType convertSQLTypeToSolrType(int sqlType) {
		switch(sqlType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return SolrType.STRING;
		case Types.BIGINT:
			return SolrType.LONG;
		case Types.INTEGER:
			return SolrType.INT;
		case Types.NUMERIC:
		case Types.DECIMAL:
			return SolrType.DECIMAL;
		case Types.CLOB:
			return SolrType.TEXT;
		case Types.BOOLEAN:
		case Types.BIT:
			return SolrType.BOOLEAN;
		case Types.DATE:
			return SolrType.DATE;
		case Types.ARRAY:
			return SolrType.ARRAY;
		default:
			throw new RuntimeException("UNKNOWN sql types:" + sqlType);
		}
	}

	public static SolrValue convertToValue(Object x) {
		if (x == null) {
			return ValueNull.INSTANCE;
		}
		if (x instanceof Long) {
			return ValueLong.get((Long)x);
		} else if (x instanceof Integer) {
			return ValueInt.get((Integer)x);
		} else if (x instanceof BigDecimal) {
			return ValueDecimal.get((BigDecimal)x);
		} else if (x instanceof Date) {
			return ValueDate.get((Date)x);
		} else if (x instanceof Object[]) {
			Object[] o = (Object[]) x;
			int len = o.length;
			SolrValue[] v = new SolrValue[len];
			for(int i=0; i<len; i++) {
				v[i] = convertToValue(o[i]);
			}
			return ValueArray.get(v);
		} else {
			return ValueString.get(x.toString());
		}
	}

	public static DataType getTypeByName(String name) {
		return TYPES_BY_NAME.get(name.toUpperCase());
	}

    public static String getTypeClassName(SolrType type) {
        switch(type) {
        case BOOLEAN:
            // "java.lang.Boolean";
            return Boolean.class.getName();
        case BYTE:
            // "java.lang.Byte";
            return Byte.class.getName();
        case SHORT:
            // "java.lang.Short";
            return Short.class.getName();
        case INT:
            // "java.lang.Integer";
            return Integer.class.getName();
        case LONG:
            // "java.lang.Long";
            return Long.class.getName();
        case DECIMAL:
            // "java.math.BigDecimal";
            return BigDecimal.class.getName();
        case DATE:
            // "java.sql.Date";
            return Date.class.getName();
        case STRING:
            // "java.lang.String";
            return String.class.getName();
        case TEXT:
            return String.class.getName();
        case DOUBLE:
            // "java.lang.Double";
            return Double.class.getName();
        case FLOAT:
            // "java.lang.Float";
            return Float.class.getName();
        case NULL:
            return null;
        case UNKNOWN:
            // anything
            return Object.class.getName();
        case ARRAY:
            return Array.class.getName();
        default:
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE, type.name());
        }
    }

}
