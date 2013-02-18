package net.unit8.solr.jdbc.expression;

import net.unit8.solr.jdbc.value.DataType;
import net.unit8.solr.jdbc.value.SolrType;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSetMetaData;

public abstract class Expression {
	protected SolrType type;
	protected String columnName;
	protected String tableName;
	protected String alias;
	protected SolrType originalType;

	public SolrType getType() {
		if(type == null)
			return SolrType.UNKNOWN;
		return type;
	}

	/**
	 * JDBCとしてのカラム名を返します
	 *
	 * @return JDBCカラム名
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * テーブル名を返します
	 *
	 * @return テーブル名
	 */
	public String getTableName() {
		return tableName;
	}


	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * Solr内部のフィールド名を返します
	 *
	 * @return Solrの内部フィールド名
	 */
	public String getSolrColumnName() {
		String typeName = (originalType == null) ? type.name() : "M_"+originalType.name();
		return tableName + "." + columnName + "." + typeName;
	}

	public String getResultName() {
		if (StringUtils.isNotEmpty(alias)) {
			return alias;
		}
		return columnName;
	}

	public abstract long getPrecision();

	public abstract int getScale();

	public int getNullable() {
		return ResultSetMetaData.columnNullableUnknown;
	}

	public String getSchemaName() {
		return null;
	}

    public String getTypeName() {

        return (originalType == null) ? type.name() : DataType.getDataType(originalType).name + "_ARRAY";
    }

	@Override
	public String toString() {
		return getColumnName();
	}
}
