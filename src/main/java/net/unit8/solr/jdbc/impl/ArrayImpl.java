package net.unit8.solr.jdbc.impl;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.SolrType;
import net.unit8.solr.jdbc.value.SolrValue;


public class ArrayImpl implements Array {
	private SolrValue value;

	ArrayImpl(SolrValue value) {
		this.value = value;
	}

	@Override
	public Object getArray() throws SQLException {
		return get();
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		checkMap(map);
		return get();
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		return get(index, count);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map)
			throws SQLException {
		checkMap(map);
		return get(index, count);
	}

	@Override
	public int getBaseType() throws SQLException {
		return Types.NULL;
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return "NULL";
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getResultSet")
			.getSQLException();
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getResultSet")
			.getSQLException();
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getResultSet")
			.getSQLException();
	}

	@Override
	public ResultSet getResultSet(long index, int count,
			Map<String, Class<?>> map) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getResultSet")
			.getSQLException();
	}
	
	@Override
	public void free() throws SQLException {
		value = null;
	}
	
	private Object[] get() {
		return (Object[]) value.convertTo(SolrType.ARRAY).getObject();
	}

	private Object[] get(long index, int count) {
		Object[] array = get();
		if (count < 0 || count > array.length) {
			throw new IllegalArgumentException(String.valueOf(count));
		}
		Object[] subset = new Object[count];
		System.arraycopy(array, (int) (index - 1), subset, 0, count);
		return subset;
	}
	
	private void checkMap(Map<String, Class<?>> map) {
		if (map != null && map.size() > 0) {
			throw new UnsupportedOperationException("map.size > 0");
		}
	}
}
