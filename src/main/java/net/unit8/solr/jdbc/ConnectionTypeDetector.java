package net.unit8.solr.jdbc;

import net.unit8.solr.jdbc.impl.CommonsHttpConnectionImpl;
import net.unit8.solr.jdbc.impl.EmbeddedConnectionImpl;
import net.unit8.solr.jdbc.impl.SolrConnection;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Create coonnection by url.
 *
 * @author kawasima
 */
public class ConnectionTypeDetector {
    private final Logger logger = Logger.getLogger(ConnectionTypeDetector.class.getName());
    private static ConnectionTypeDetector connectionTypeDetector;
    private List<Class<? extends SolrConnection>> connectionClasses = new ArrayList<Class<? extends SolrConnection>>();
    private Map<Class<? extends SolrConnection>, Constructor<? extends SolrConnection>> connectionConstructors
            = new HashMap<Class<? extends SolrConnection>, Constructor<? extends SolrConnection>>();
    private static Constructor<? extends SolrConnection> defaultConnectionConstructor;

    static {
        try {
            defaultConnectionConstructor = EmbeddedConnectionImpl.class.getConstructor(String.class);
        } catch(NoSuchMethodException e) {

        }
    }

    public static ConnectionTypeDetector getInstance() {
        if (connectionTypeDetector == null) {
            connectionTypeDetector = new ConnectionTypeDetector();
            connectionTypeDetector.add(CommonsHttpConnectionImpl.class);
        }
        return connectionTypeDetector;
    }

    public SolrConnection find(String url) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        for (Class<? extends SolrConnection> connectionClass : connectionClasses) {
            try {
                Method m = connectionClass.getMethod("accept", new Class[]{String.class});
                Object ret = m.invoke(null, url);
                if (ret instanceof Boolean || (Boolean) ret) {
                    return connectionConstructors.get(connectionClass).newInstance(url);
                }
            } catch (Exception ignore) {
                logger.log(Level.WARNING, "Connection class " + connectionClass + " is invalid.");
            }
        }
        return defaultConnectionConstructor.newInstance(url);
    }

    public void add(Class<? extends SolrConnection> connectionClass) {
        connectionClasses.add(connectionClass);
    }
}
