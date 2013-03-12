package net.unit8.solr.jdbc;

import java.util.List;

public class ColumnSpec {
	private Boolean isArray = false;

	public ColumnSpec(List<String> columnSpecList) {
		if (columnSpecList == null)
			return;
		
		for (String specElement : columnSpecList) {
			if (specElement.equals("ARRAY")) {
				isArray = true;
			}

		}
	}
	
	public Boolean isArray() {
		return isArray;
	}
}
