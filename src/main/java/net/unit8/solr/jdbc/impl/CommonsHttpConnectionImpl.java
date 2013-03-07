package net.unit8.solr.jdbc.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import java.net.MalformedURLException;



public class CommonsHttpConnectionImpl extends SolrConnection {
	private int timeout = 0;
	
	public CommonsHttpConnectionImpl(String serverUrl) throws MalformedURLException {
		super(serverUrl);
		HttpClient httpClient = new DefaultHttpClient();
		SolrServer solrServer = new HttpSolrServer(serverUrl, httpClient);
		setSolrServer(solrServer);
	}

    public static boolean accept(String url) {
        return StringUtils.startsWith(url, "http://") || StringUtils.startsWith(url, "http://");
    }

    @Override
	public void close() {
		
	}

	@Override
	public int getQueryTimeout() {
		return timeout;
	}
	
	@Override
	public void setQueryTimeout(int timeout) {
		this.timeout = timeout;
		((HttpSolrServer)getSolrServer()).setConnectionTimeout(timeout*1000);
		((HttpSolrServer)getSolrServer()).setSoTimeout(timeout*1000);
	}
}
