package net.unit8.solr.jdbc;

import java.util.List;

public class ColumnSpec {
	private Boolean nullable = true;
	private Boolean isArray = false;
	private Boolean primaryKey = false;
	
	public ColumnSpec(List<String> columnSpecList) {
		if (columnSpecList == null)
			return;
		
		for (String specElement : columnSpecList) {
			if (specElement.equals("ARRAY")) {
				isArray = true;
			}

		}
	}
	
	public Boolean nullable() {
		return nullable;
	}
	
	public Boolean isArray() {
		return isArray;
	}
	
	public Boolean isPrimaryKey() {
		return primaryKey;
	}
}
