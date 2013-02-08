package net.unit8.solr.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import net.unit8.solr.jdbc.impl.CommonsHttpConnectionImpl;
import net.unit8.solr.jdbc.impl.EmbeddedConnectionImpl;
import net.unit8.solr.jdbc.impl.SolrConnection;


/**
 * JDBC Driver for Apache Solr
 * 
 * @author kawasima
 */
public class SolrDriver implements Driver {
	private String serverUrl;
	
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
			throw new SQLException("Driver URLが解釈できません");
		}
		SolrConnection conn;
		try {
			if (serverUrl.startsWith("http://") || serverUrl.startsWith("https://")) {
				conn = new CommonsHttpConnectionImpl(serverUrl);
			} else {
				conn = new EmbeddedConnectionImpl(serverUrl);
			}

		} catch(Exception e) {
			throw new SQLException("URLが解釈できません", e);
		}
		return conn;
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
