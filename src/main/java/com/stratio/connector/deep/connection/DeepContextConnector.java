package com.stratio.connector.deep.connection;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.engine.DeepQueryEngine;
import com.stratio.connector.deep.engine.DeepStorageEngine;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

/**
 * Created by dgomez on 16/09/14.
 */
public class DeepContextConnector implements IConnector {

    /**
     * The connectionHandler.
     */
    private DeepConnectionHandler connectionHandler;

    DeepSparkContext deepContext = ConnectionConfiguration.getDeepContext();

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) {

        DeepContextConnector conn = new DeepContextConnector();

    }

    @Override
    public String getConnectorName() {
        return "DeepConnector";
    }

    @Override
    public String[] getDatastoreName() {
        return new String[] { "DeepConnector" };
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {

        connectionHandler = new DeepConnectionHandler(configuration);

    }

    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        try {

            connectionHandler.createConnection(credentials, config);

        } catch (HandlerConnectionException e) {
            String msg = "fail creating the Connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);
        }

    }

    @Override
    public void close(ClusterName name) throws ConnectionException {

        connectionHandler.closeConnection(name.getName());
    }

    @Override
    public void shutdown() throws ExecutionException {

        Iterator it = connectionHandler.getConnections().values().iterator();
        while (it.hasNext()) {
            DeepConnection conn = (DeepConnection) it.next();
            while (conn.isWorkInProgress()) {
            }
        }
        deepContext.stop();
    }

    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {

        // TODO throw new UnsupportedException("");
        return null;

    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {

        return new DeepQueryEngine(deepContext,connectionHandler);

    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {

        // TODO throw new UnsupportedException("");
        return null;
    }
}
