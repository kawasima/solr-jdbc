package net.unit8.solr.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import net.unit8.solr.jdbc.expression.Expression;
import net.unit8.solr.jdbc.expression.FunctionExpression;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import net.unit8.solr.jdbc.value.DataType;
import net.unit8.solr.jdbc.value.SolrType;


public class ResultSetMetaDataImpl implements ResultSetMetaData {
	private final String catalog;
	private final AbstractResultSet resultSet;
	private final List<Expression> expressions;
//	private final List<ColumnExpression> solrColumns = new ArrayList<ColumnExpression>();

	public ResultSetMetaDataImpl(AbstractResultSet resultSet, List<Expression> expressions, String catalog) {
		this.catalog = catalog;
		this.expressions = expressions;
		this.resultSet = resultSet;
	}

	/**
	 * カラムの名前から何番目のカラムかを検索する
	 *
	 * @param columnLabel カラムのラベル(ASで別名を割り当てている場合はそちらが優先される)
	 * @return index of column
	 * @throws SQLException
	 */
	public int findColumn(String columnLabel) throws SQLException{
		for(int i=0; i<expressions.size(); i++) {
			if (StringUtils.equalsIgnoreCase(expressions.get(i).getAlias(), columnLabel)
				|| StringUtils.equalsIgnoreCase(expressions.get(i).getColumnName(), columnLabel)) {
				return i+1; // parameterIndexは1始まりなので+1する
			}
		}
		throw new SQLException("column not found: "+columnLabel);
	}
	@Override
	public String getCatalogName(int column) throws SQLException {
		checkClosed();
		return catalog;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		column--;
		SolrType type = expressions.get(column).getType();
		return DataType.getTypeClassName(type);
	}

	@Override
	public int getColumnCount() throws SQLException {
		return expressions.size();
	}

	@Override
	public int getColumnDisplaySize(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return 0;
	}

	@Override
	public String getColumnLabel(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression expression = expressions.get(columnIndex - 1);
		String columnName = expression.getAlias();
		if(columnName != null)
			return columnName;
		return expression.getColumnName();
	}

	@Override
	public String getColumnName(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression solrColumn = expressions.get(columnIndex - 1);
		return solrColumn.getColumnName();
	}

	public String getSolrColumnName(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression solrColumn = expressions.get(columnIndex - 1);
		return solrColumn.getSolrColumnName();
	}

	public Expression getColumn(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return expressions.get(columnIndex - 1);
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		Expression solrColumn = expressions.get(column - 1);
		return DataType.getDataType(solrColumn.getType()).sqlType;
	}

	@Override
	public String getColumnTypeName(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression solrColumn = expressions.get(columnIndex - 1);
		return DataType.getDataType(solrColumn.getType()).jdbc;
	}

	@Override
	public int getPrecision(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression expression = expressions.get(columnIndex -1);
		return (int)expression.getPrecision();
	}

	@Override
	public int getScale(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression expression = expressions.get(columnIndex - 1);
		return expression.getScale();
	}

	@Override
	public String getSchemaName(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression expression = expressions.get(columnIndex - 1);
		return expression.getSchemaName();
	}

	@Override
	public String getTableName(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return expressions.get(columnIndex - 1).getTableName();
	}

	@Override
	public boolean isAutoIncrement(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return false;
	}

	@Override
	public boolean isCaseSensitive(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return true;
	}

	/**
	 * Checks if this is a currency column
	 * It always return false.
	 *
	 * @param columnIndex the column index (1,2,...)
	 * @return true
	 */
	@Override
	public boolean isCurrency(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return false;
	}

	/**
	 * Checks whether a write on this column will definitely succeed.
	 * It always returns false
	 *
	 * @return false
	 */
	@Override
	public boolean isDefinitelyWritable(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return false;
	}

	/**
	 * Checks if this is nullable column.
	 */
	@Override
	public int isNullable(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		Expression expression = expressions.get(columnIndex - 1);
		return expression.getNullable();
	}

	/**
	 * Checks if this is read only.
	 * It always returns true.
	 *
	 * @return true
	 */
	@Override
	public boolean isReadOnly(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return true;
	}

	/**
	 * Checks if this column is searchable.
	 * It always returns true.
	 *
	 * @return true
	 */
	@Override
	public boolean isSearchable(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return true;
	}

	/**
	 * Checks if this column is signed.
	 * It always returns true
	 *
	 * @return true
	 */
	@Override
	public boolean isSigned(int columnIndex) throws SQLException {
		checkClosed();
		checkColumnIndex(columnIndex);
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "isWrapperFor")
			.getSQLException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap")
			.getSQLException();
	}

	public List<Expression> getCountColumnList() {
		List<Expression> countColumns = new ArrayList<Expression>();
		for(Expression solrColumn : expressions) {
			if(solrColumn instanceof FunctionExpression &&
				StringUtils.equalsIgnoreCase(((FunctionExpression)solrColumn).getFunctionName(), "count")) {
				countColumns.add(solrColumn);
			}
		}
		return countColumns;
	}

	private void checkClosed() throws SQLException {
		if (resultSet != null) {
			resultSet.checkClosed();
		}
	}

	private void checkColumnIndex(int columnIndex) throws SQLException {
		if (columnIndex < 1 || columnIndex > getColumnCount()) {
			throw DbException.get(ErrorCode.INVALID_VALUE, "columnIndex:" + columnIndex);
		}
	}
}
