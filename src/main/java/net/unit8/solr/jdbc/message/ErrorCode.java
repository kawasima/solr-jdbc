package net.unit8.solr.jdbc.message;

public class ErrorCode {
	public static final int NO_DATA_AVAILABLE = 2000;
	public static final int COLUMN_COUNT_DOES_NOT_MATCH = 21002;
	public static final int NUMERIC_VALUE_OUT_OF_RANGE = 22003;
	public static final int DUPLICATE_KEY = 23001;
	public static final int SYNTAX_ERROR = 42000;
	public static final int TABLE_OR_VIEW_ALREADY_EXISTS = 42101;
	public static final int TABLE_OR_VIEW_NOT_FOUND = 42102;
	public static final int COLUMN_NOT_FOUND = 42122;
	public static final int GENERAL_ERROR = 50000;

	public static final int UNKNOWN_DATA_TYPE = 50004;
	public static final int FEATURE_NOT_SUPPORTED = 50100;
	public static final int METHOD_NOT_ALLOWED_FOR_QUERY = 90001;
	public static final int METHOD_ONLY_ALLOWED_FOR_QUERY = 90002;
	public static final int NULL_NOT_ALLOWED = 90006;
	public static final int OBJECT_CLOSED = 90007;
	public static final int INVALID_VALUE = 90008;
	public static final int IO_EXCEPTION = 90028;
	public static final int EXCEPTION_IN_FUNCTION = 90105;
	public static final int OUT_OF_MEMORY = 90108;
	public static final int METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT = 90130;

	public static String getState(int errorCode) {
		switch(errorCode) {
		case NO_DATA_AVAILABLE: return "02000";
		case COLUMN_COUNT_DOES_NOT_MATCH: return "21S02";
		case SYNTAX_ERROR: return "42000";
		case TABLE_OR_VIEW_ALREADY_EXISTS: return "42S01";
		case TABLE_OR_VIEW_NOT_FOUND: return "42S02";
		case COLUMN_NOT_FOUND: return "42S22";
		case FEATURE_NOT_SUPPORTED: return "HYC00";
		case GENERAL_ERROR: return "HY000";
		default:
			return ""+errorCode;
		}
	}
}
