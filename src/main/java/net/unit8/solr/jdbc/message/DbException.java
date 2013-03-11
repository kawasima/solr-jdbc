package net.unit8.solr.jdbc.message;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ResourceBundle;

public class DbException extends RuntimeException {
	private static final long serialVersionUID = -2273043887990931388L;
	private static final ResourceBundle MESSAGES;

	static {
		MESSAGES = ResourceBundle.getBundle("net.unit8.solr.jdbc.message.messages");
	}

	private DbException(SQLException e) {
		super(e.getMessage(), e);
	}

	public static DbException get(int errorCode) {
		return get(errorCode, (String)null);
	}

	public static DbException get(int errorCode, Throwable cause, String...params) {
		return new DbException(getSQLException(errorCode, cause, params));
	}

    public static DbException get(int errorCode, String... params) {
        return new DbException(getSQLException(errorCode, null, params));
    }

	private static String translate(String key, String ... params) {
		String message = null;
		if(MESSAGES != null) {
			message = MESSAGES.getString(key);
		}
		if (message == null) {
			message = "(Message " + key + " not found)";
		}
		if (params != null) {
			message = MessageFormat.format(message, (Object[])params);
		}
		return message;
	}

	public SQLException getSQLException() {
		return (SQLException) getCause();
	}
	private static SQLException getSQLException(int errorCode, Throwable cause, String ... params) {
		String sqlstate = ErrorCode.getState(errorCode);
		String message = translate(sqlstate, params);
		SQLException sqlException = new SQLException(message, sqlstate, errorCode);
        if (cause != null)
            sqlException.initCause(cause);
        return sqlException;
	}

    public static SQLException toSQLException(Exception e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        return convert(e).getSQLException();
    }

    public static DbException convert(Throwable e) {
        if (e instanceof DbException) {
            return (DbException) e;
        } else if (e instanceof SQLException) {
            return new DbException((SQLException) e);
        } else if (e instanceof InvocationTargetException) {
            return convertInvocation((InvocationTargetException) e, null);
        } else if (e instanceof IOException) {
            return get(ErrorCode.IO_EXCEPTION, e, e.toString());
        } else if (e instanceof OutOfMemoryError) {
            return get(ErrorCode.OUT_OF_MEMORY, e);
        } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
            return get(ErrorCode.GENERAL_ERROR, e, e.toString());
        } else if (e instanceof Error) {
            throw (Error) e;
        }
        return get(ErrorCode.GENERAL_ERROR, e, e.toString());
    }

    public static DbException convertInvocation(InvocationTargetException te, String message) {
        Throwable t = te.getTargetException();
        if (t instanceof SQLException || t instanceof DbException) {
            return convert(t);
        }
        message = message == null ? t.getMessage() : message + ": " + t.getMessage();
        return get(ErrorCode.EXCEPTION_IN_FUNCTION, t, message);
    }

	public static DbException getInvalidValueException(String value, String param) {
		return get(ErrorCode.INVALID_VALUE, value, param);
	}

}
