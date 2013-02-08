package net.unit8.solr.jdbc.value;

import java.sql.Date;
import java.text.ParseException;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;


public class ValueDate extends SolrValue{
	
	private final Date value;
	private static final String[] parsePatterns = new String[]{
		"yyyy/MM/dd",
		"yyyy/MM/dd HH:mm:ss",
		"yyyy-MM-dd",
		"yyyy-MM-dd'T'HH:mm:ssZZ"
	};
	
	private ValueDate(Date value) {
		this.value = value;
	}

	public static Date parseDate(String s) {
		java.util.Date date; 
		try {
			date = DateUtils.parseDate(s, parsePatterns);
		} catch (ParseException e) {
			return null;
		}
		return (Date)date;
	}
	
	@Override
	public Date getDate() {
		return (Date)value.clone();
	}
	
	@Override
	public Date getDateNoCopy() {
		return value;
	}
	
	public static ValueDate get(Date date){
		return new ValueDate(date);
	}
	
	@Override
	public String getQueryString() {
		return DateFormatUtils.formatUTC(value,"yyyy-MM-dd'T'HH:mm:ss'Z'");
	}
	
	@Override
	public SolrType getType() {
		return SolrType.DATE;
	}

	@Override
	public String getString() {
		return DateFormatUtils.formatUTC(value,"yyyy-MM-dd'T'HH:mm:ss'Z'");
	}

	@Override
	public Object getObject() {
		return getDate();
	}
}
