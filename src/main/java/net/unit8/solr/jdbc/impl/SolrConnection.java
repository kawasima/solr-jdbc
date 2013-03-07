package net.unit8.solr.jdbc.impl;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

/**
 * implements Connection
 *
 * @author kawasima
 */
public abstract class SolrConnection implements Connection {
	private SolrServer solrServer;
	private DatabaseMetaDataImpl metaData;
	private final Boolean isClosed = false;

	private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
	private boolean autoCommit = false;
	private String catalog;
	protected Statement executingStatement;

	private boolean updatedInTx = false;
    private boolean softCommit = true;

	protected SolrConnection(String serverUrl) {

	}

	protected void setSolrServer(SolrServer solrServer) {
		this.solrServer = solrServer;
	}

	public SolrServer getSolrServer() {
		return solrServer;
	}

	public void refreshMetaData() {
		metaData = null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();
	}

	@Override
	public void commit() throws SQLException {
		try {
			if(updatedInTx) {
				UpdateResponse response = solrServer.commit(true, true, softCommit);
                if (response.getStatus() != 0)
                    throw DbException.get(ErrorCode.GENERAL_ERROR, "");
            }
		} catch (SolrServerException e) {
			throw new SQLException(e);
        } catch (IOException e) {
            throw DbException.get(ErrorCode.IO_EXCEPTION, e);
		} finally {
			updatedInTx = false;
            softCommit = true;
		}
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createArrayOf");
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createBlob");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createClob");
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createNClob");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createSQLXML");
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createStruct");
	}

	@Override
	public Statement createStatement() throws SQLException {
		checkClosed();
		return new StatementImpl(this, ResultSet.FETCH_FORWARD,
				ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		checkClosed();
		return new StatementImpl(this, resultSetType, resultSetConcurrency);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkClosed();
		return new StatementImpl(this, resultSetType, resultSetConcurrency);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		checkClosed();
		return autoCommit;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkClosed();
		this.autoCommit = autoCommit;
	}

	@Override
	public String getCatalog() throws SQLException {
		checkClosed();
		return catalog;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		checkClosed();
		// ignore
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLClientInfoException();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public int getHoldability() throws SQLException {
		checkClosed();
		return holdability;
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		checkClosed();
		this.holdability = holdability;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		if (metaData == null) {
			metaData = new DatabaseMetaDataImpl(this);
		}
		return metaData;
	}

	public DatabaseMetaDataImpl getMetaDataImpl() {
		if (metaData == null) {
			metaData = new DatabaseMetaDataImpl(this);
		}
		return metaData;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_READ_COMMITTED;
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		throw DbException
				.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setTransaction")
				.getSQLException();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		checkClosed();
		return false;
	}

	/**
	 * According to the JDBC specs, this setting is only a hint to the database
	 * to enable optimizations - it does not cause writes to be prohibited.
	 *
	 * @throws SQLException
	 *             if the connection is closed
	 */
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		checkClosed();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setSavepoint")
				.getSQLException();
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setSavepoint")
				.getSQLException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// do nothing
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return isClosed();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return sql;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "prepareCall")
				.getSQLException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "prepareCall")
				.getSQLException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "prepareCall")
				.getSQLException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkClosed();
		PreparedStatementImpl stmt;
		try {
			stmt = new PreparedStatementImpl(this, sql,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} catch (DbException e) {
			throw e.getSQLException();
		}
		return stmt;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKey)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		checkClosed();
		PreparedStatementImpl stmt;
		try {
			stmt = new PreparedStatementImpl(this, sql, resultSetType,
					resultSetConcurrency);
		} catch (DbException e) {
			throw e.getSQLException();
		}
		return stmt;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkClosed();
		PreparedStatementImpl stmt;
		try {
			stmt = new PreparedStatementImpl(this, sql, resultSetType,
					resultSetConcurrency);
		} catch (DbException e) {
			throw e.getSQLException();
		}
		return stmt;
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
				"releaseSavepoint").getSQLException();
	}

	@Override
	public void rollback() throws SQLException {
		try {
			if(updatedInTx)
				solrServer.rollback();
		} catch (Exception e) {
			throw new SQLException(e);
		} finally {
			updatedInTx = false;
		}
	}

	@Override
	public void rollback(Savepoint savePoint) throws SQLException {
		this.rollback();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "isWrapperFor");
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap")
				.getSQLException();
	}

    public boolean isUpdatedInTx() {
        return updatedInTx;
    }

	public void setUpdatedInTx(boolean updateInTx) {
		this.updatedInTx = updateInTx;
	}

    public void setSoftCommit(boolean softCommit) {
        this.softCommit = softCommit;
    }

	public abstract void setQueryTimeout(int second);

	public abstract int getQueryTimeout();

	protected void checkClosed() throws SQLException {
		if (isClosed) {
			throw DbException.get(ErrorCode.OBJECT_CLOSED, "Connection");
		}
	}

	protected void setExecutingStatement(Statement statement) {
		this.executingStatement = statement;
	}

}
