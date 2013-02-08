package net.unit8.solr.jdbc.util;

import net.unit8.solr.jdbc.value.DataType;

public class ColumnUtil {
	public static DataType getColumnType(String columnName) {
		if (columnName == null) {
			return null;
		}
		int idx = columnName.lastIndexOf('.');
		String typeName = columnName.substring(idx);
		return DataType.getTypeByName(typeName.toUpperCase());
	}
}
