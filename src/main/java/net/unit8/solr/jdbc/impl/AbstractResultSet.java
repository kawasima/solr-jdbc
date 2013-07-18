package net.unit8.solr.jdbc.impl;

import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.DataType;
import net.unit8.solr.jdbc.value.SolrType;
import net.unit8.solr.jdbc.value.SolrValue;
import net.unit8.solr.jdbc.value.ValueNull;
import org.apache.solr.common.SolrDocumentList;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;


public abstract class AbstractResultSet implements ResultSet {
	protected SolrDocumentList docList;
	protected int docIndex = -1;
	protected ResultSetMetaDataImpl metaData;
	protected boolean isClosed = false;
	protected StatementImpl statement;
	private boolean wasNull;

	@Override
	public boolean absolute(int i) throws SQLException {
		checkClosed();
		if (0 <= i && i < docList.size()) {
			return false;
		}
		docIndex = i;
		return true;
	}

	@Override
	public void afterLast() throws SQLException {
		checkClosed();
		docIndex = docList.size();
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkClosed();
		docIndex = -1;
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();
	}

	@Override
	public void close() throws SQLException {
		this.docList = null;
		isClosed = true;
	}

	@Override
	public void deleteRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "deleteRow");
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		checkClosed();
		return metaData.findColumn(columnLabel);
	}

	@Override
	public boolean first() throws SQLException {
		checkClosed();
		docIndex = 0;
		return true;
	}

	@Override
	public boolean last() throws SQLException {
		// TODO TYPE_FORWARD_ONLYのときはSQLExceptionをだす
		docIndex = docList.size() - 1;
		return true;
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();

		docIndex +=1;
		if (docIndex >= docList.size()) {
			docIndex = docList.size();
			return false;
		}

		return true;
	}

	@Override
	public boolean previous() throws SQLException {
		// TODO スクロールが不可能な場合はSQLException
		checkClosed();

		docIndex -= 1;
		if (docIndex < 0) {
			docIndex = 0;
			return false;
		}
		return true;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return (docIndex >= docList.size());
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return (docIndex < 0);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return docIndex == 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		return docIndex == docList.size() - 1;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metaData;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		SolrValue v = get(columnIndex);
		return v == ValueNull.INSTANCE ? null : new ArrayImpl(v);
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		SolrValue v = get(columnLabel);

		return v == ValueNull.INSTANCE ? null : new ArrayImpl(v);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		checkClosed();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getAsciiStream");
	}

	@Override
	public InputStream getAsciiStream(String s) throws SQLException {
		checkClosed();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getAsciiStream");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getBigDecimal();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getBigDecimal();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int j) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBigDecimal");
	}

	@Override
	public BigDecimal getBigDecimal(String s, int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBigDecimal");
	}

	@Override
	public InputStream getBinaryStream(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBinaryStream");
	}

	@Override
	public InputStream getBinaryStream(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBinaryStream");
	}

	@Override
	public Blob getBlob(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBlob");
	}

	@Override
	public Blob getBlob(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBlob");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getBoolean();
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getBoolean();
	}

	@Override
	public byte getByte(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getByte");
	}

	@Override
	public byte getByte(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getByte");
	}

	@Override
	public byte[] getBytes(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBytes");
	}

	@Override
	public byte[] getBytes(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBytes");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
        String value = get(columnIndex).getString();
        return (value == null) ? null : new StringReader(value);
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
        String value = get(columnLabel).getString();
        return (value == null) ? null : new StringReader(value);
	}

	@Override
	public Clob getClob(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getClob");
	}

	@Override
	public Clob getClob(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getClob");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getDate();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getDate();
	}

	@Override
	public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getDouble();
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getDouble();
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return (float)get(columnIndex).getDouble();
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return (float)get(columnLabel).getDouble();
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getInt();
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getInt();
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return (long)get(columnIndex).getInt();
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return (long)get(columnLabel).getInt();
	}

	@Override
	public Reader getNCharacterStream(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Reader getNCharacterStream(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public NClob getNClob(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public NClob getNClob(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getString();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getString();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getObject();
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getObject();
	}

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Ref getRef(int i) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Ref getRef(String s) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public int getRow() throws SQLException {
		if (docIndex < 0 || docIndex >= docList.size()) {
			return 0;
		}
		return docIndex+1;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return (short)get(columnIndex).getInt();
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return (short)get(columnLabel).getInt();
	}

	@Override
	public Statement getStatement() throws SQLException {
		return statement;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getString();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getString();
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnIndex).getTimestamp();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		return get(columnLabel).getTimestamp();
	}

	@Override
	public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getTimestamp");
	}

	@Override
	public Timestamp getTimestamp(String s, Calendar calendar)
			throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getTimestamp");
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getURL");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getURL");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getUnicodeStream");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getUnicodeStream");
	}

	@Override
	public int getType() throws SQLException {
		checkClosed();
		return (statement==null) ? ResultSet.TYPE_FORWARD_ONLY : statement.resultSetType;
	}


	@Override
	public int getConcurrency() throws SQLException {
		checkClosed();
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		checkClosed();
		checkAvailable();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCursorName");
	}

	@Override
	public void setFetchDirection(int fetchDirection) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setFetchDirection");

	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkClosed();
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int fetchSize) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setFetchSize");
	}

	@Override
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public int getHoldability() throws SQLException {
		checkClosed();
		if(statement == null || statement.getConnection() == null) {
			return ResultSet.HOLD_CURSORS_OVER_COMMIT;
		}
		return statement.getConnection().getHoldability();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getWarnings");
	}

	@Override
	public void insertRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "insertRow");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "moveToCurrentRow");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "moveToInsertRow");
	}

	@Override
	public void refreshRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "refreshRow");

	}

	@Override
	public boolean relative(int rowCount) throws SQLException {
		checkClosed();
		int row = docIndex + rowCount + 1;
		if (row < 0) {
			row = 0;
		} else if (row > docList.size()) {
			row = docList.size() + 1;
		}
		return absolute(row);
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return false;
	}

	@Override
	public void updateArray(int i, Array array) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateArray(String s, Array array) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream, int j)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream, int i)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBigDecimal(int i, BigDecimal bigdecimal)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBigDecimal(String s, BigDecimal bigdecimal)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream, int j)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream, int i)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int i, Blob blob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String s, Blob blob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int i, InputStream inputstream) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String s, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int i, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String s, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBoolean(int i, boolean flag) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBoolean(String s, boolean flag) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateByte(int i, byte byte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateByte(String s, byte byte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBytes(int i, byte[] abyte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBytes(String s, byte[] abyte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int i, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String s, Reader reader)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int i, Reader reader, int j)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String s, Reader reader, int i)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int i, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int i, Clob clob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String s, Clob clob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int i, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String s, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int i, Reader reader, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String s, Reader reader, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDate(int i, Date date) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDate(String s, Date date) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDouble(int i, double d) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDouble(String s, double d) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateFloat(int i, float f) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateFloat(String s, float f) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateInt(int i, int j) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateInt(String s, int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateLong(int i, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateLong(String s, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(int i, Reader reader)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(String s, Reader reader)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(int i, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int i, NClob nclob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String s, NClob nclob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int i, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String s, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int i, Reader reader, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String s, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNString(int i, String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNString(String s, String s1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNull(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNull(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(int i, Object obj) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(String s, Object obj) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(int i, Object obj, int j) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(String s, Object obj, int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRef(int i, Ref ref) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRef(String s, Ref ref) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRowId(int i, RowId rowid) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRowId(String s, RowId rowid) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateSQLXML");
	}

	@Override
	public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateSQLXML");
	}

	@Override
	public void updateShort(int i, short word0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateShort");
	}

	@Override
	public void updateShort(String s, short word0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateShort");
	}

	@Override
	public void updateString(int i, String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateString");
	}

	@Override
	public void updateString(String s, String s1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateString");
	}

	@Override
	public void updateTime(int i, Time time) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateTime");
	}

	@Override
	public void updateTime(String s, Time time) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateTime");
	}

	@Override
	public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateTimestamp");
	}

	@Override
	public void updateTimestamp(String s, Timestamp timestamp)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "updateTimestamp");
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkClosed();
		return wasNull;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "isWrapperFor");
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap");
	}

	private SolrValue get(int columnIndex) throws SQLException{
		checkClosed();
		Expression column = metaData.getColumn(columnIndex);

		String columnName = (column.getType() == SolrType.UNKNOWN) ? column.getResultName():column.getSolrColumnName();

		Object x;
		Collection<Object> objs = docList.get(docIndex).getFieldValues(columnName);
		if (objs == null) {
			x = null;
		} else if (objs.size() == 1) {
			x = objs.toArray()[0];
		} else {
			x = objs.toArray();
		}

		SolrValue value = DataType.convertToValue(x);
		wasNull = (value == ValueNull.INSTANCE);
		return value;
	}

	private SolrValue get(String columnLabel) throws SQLException {
		int columnIndex = findColumn(columnLabel);
		return get(columnIndex);
	}

	protected void checkClosed() throws SQLException {
		if (isClosed)
			throw DbException.get(ErrorCode.OBJECT_CLOSED);
	}

	protected void checkAvailable() throws SQLException {
		if (docIndex < 0 ||  docIndex >= docList.size()) {
			throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
		}
	}
}
