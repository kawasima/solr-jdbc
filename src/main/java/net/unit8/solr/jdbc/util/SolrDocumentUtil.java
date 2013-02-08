package net.unit8.solr.jdbc.util;

import org.apache.solr.common.SolrInputDocument;

import net.unit8.solr.jdbc.value.SolrValue;
import net.unit8.solr.jdbc.value.ValueArray;


public class SolrDocumentUtil {
	public static void setValue(SolrInputDocument document, String columnName, SolrValue value) {
		if (value instanceof ValueArray) {
			SolrValue[] values = ((ValueArray)value).getList();
			for(SolrValue v : values) {
				document.addField(columnName, v.getObject());
			}
		}
		else {
			document.setField(columnName, value.getString());
		}

	}
}
