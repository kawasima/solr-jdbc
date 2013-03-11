package net.unit8.solr.jdbc.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.Properties;



public class EmbeddedConnectionImpl extends SolrConnection {
	private final CoreContainer coreContainer;
	private int timeout = 0;

	private static final Properties defaults = new Properties();
	static {
		defaults.setProperty("DATA_DIR", "target/solr/data");
		defaults.setProperty("SOLR_HOME", "target/solr");
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
        System.setProperty("solr.solr.home", options.getProperty("SOLR_HOME"));
        File solrHomeDirectory = new File(options.getProperty("SOLR_HOME"));
        File dataDirectory = new File(options.getProperty("DATA_DIR"));
        if (!solrHomeDirectory.exists())
            createSolrHome(solrHomeDirectory, dataDirectory);
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

    protected void createSolrHome(File solrHomeDirectory, File dataDirectory) throws IOException {
        FileUtils.forceMkdir(solrHomeDirectory);
        FileUtils.writeStringToFile(new File(solrHomeDirectory, "solr.xml"),
                "<?xml version='1.0' encoding='UTF-8'?>\n" +
                        "<solr persistent=\"true\" sharedLib=\"lib\">\n" +
                        "    <cores adminPath=\"/admin/cores\" defaultCoreName=\"core0\">\n" +
                        "        <core name=\"core0\" instanceDir=\".\" dataDir=\"" +
                        dataDirectory.getAbsolutePath() +
                        "\"/>\n" +
                        "    </cores>\n" +
                        "</solr>\n");
        copyResource(solrHomeDirectory, "solrconfig.xml");
        copyResource(solrHomeDirectory, "schema.xml");
        copyResource(solrHomeDirectory, "elevate.xml");
    }

    protected void copyResource(File solrHomeDirectory, String resourceName) throws IOException {
        File confDirectory = new File(solrHomeDirectory, "conf");
        if (!confDirectory.exists())
            FileUtils.forceMkdir(confDirectory);
        ClassLoader classLoader = EmbeddedConnectionImpl.class.getClassLoader();
        InputStream in = classLoader.getResourceAsStream("net/unit8/solr/jdbc/config/" + resourceName);
        OutputStream out = new FileOutputStream(new File(confDirectory, resourceName));
        try {
            IOUtils.copy(in, out);
        } finally {
            IOUtils.closeQuietly(out);
            IOUtils.closeQuietly(in);
        }
    }
}
