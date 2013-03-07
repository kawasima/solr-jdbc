package net.unit8.solr.jdbc.impl;

import org.apache.commons.lang.ArrayUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Properties;



public class EmbeddedConnectionImpl extends SolrConnection {
	private final CoreContainer coreContainer;
	private int timeout = 0;

	private static final Properties defaults;
	static {
		defaults = new Properties();
		defaults.put("DATA_DIR", "target/solrdata");
		defaults.put("SOLR_HOME", "target/test-classes");
	}

    public EmbeddedConnectionImpl(String serverUrl)
			throws ParserConfigurationException, IOException, SAXException {
		super(serverUrl);
		String[] tokens = serverUrl.split(";");
        Properties options = new Properties(defaults);
        if(tokens != null && tokens.length > 1) {
			tokens = (String[])ArrayUtils.remove(tokens, 0);
			for (String token : tokens) {
				String[] params = token.split("=", 2);
				options.put(params[0], params[1]);
			}
		}
        if (options.containsKey("SOLR_HOME")) {
            System.setProperty("solr.solr.home", (String) options.get("SOLR_HOME"));
        }
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        coreContainer = initializer.initialize();

		EmbeddedSolrServer solrServer = new EmbeddedSolrServer(coreContainer, coreContainer.getDefaultCoreName());
		setSolrServer(solrServer);
	}

    public static boolean accept(String url) {
        return true;
    }

	@Override
	public void close() {
		coreContainer.shutdown();
	}

	@Override
	public int getQueryTimeout() {
		return timeout;
	}

	@Override
	public void setQueryTimeout(int timeout) {
		this.timeout = timeout;
	}
}
