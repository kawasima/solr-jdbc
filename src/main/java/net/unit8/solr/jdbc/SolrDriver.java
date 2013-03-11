package net.unit8.solr.jdbc;

import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.Properties;


/**
 * JDBC Driver for Apache Solr
 * 
 * @author kawasima
 */
public class SolrDriver implements Driver {
	private String serverUrl;
    private static ConnectionTypeDetector connectionTypeDetector = ConnectionTypeDetector.getInstance();
	
	static {
		Driver driver = new SolrDriver();
		try {
			DriverManager.registerDriver(driver);
		} catch (SQLException ignore) {}
	}
	
	private boolean parseUrl(String url) {
		String[] elm = url.split(":", 3);
		if(!StringUtils.equals(elm[1], "solr")) {
			return false;
		}
		serverUrl = elm[2];
		return true;
	}
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return parseUrl(url);
	}

	@Override
	public Connection connect(String url, Properties properties)
			throws SQLException {
		if(!parseUrl(url)) {
			throw DbException.get(ErrorCode.URL_FORMAT_ERROR_2, url).getSQLException();
		}
        try {
            return  connectionTypeDetector.find(serverUrl);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.URL_FORMAT_ERROR_2, e, "jdbc:solr:[url]", serverUrl).getSQLException();
        }
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 1;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties)
			throws SQLException {
		return new DriverPropertyInfo[0];
	}

	/**
	 * JDBC Compliantのテストにパスしていないのでfalseを返します
	 */
	@Override
	public boolean jdbcCompliant() {
		return false;
	}

}
