package com.stratio.connector.deep;

import com.stratio.connector.deep.connection.DeepConnector;
import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;

public class ConnectionsHandler {

    protected final DeepConnector deepConnector;

    public ConnectionsHandler() throws InitializationException {

        this.deepConnector = new DeepConnector();
        this.deepConnector.init(null);
    }

    public void connect(ConnectorClusterConfig configuration) throws ConnectionException {

        this.deepConnector.connect(null, configuration);
    }

    public DeepConnector getDeepContextConnector() {

        return this.deepConnector;
    }

    public DeepQueryEngine getQueryEngine() throws UnsupportedException {

        return (DeepQueryEngine) this.deepConnector.getQueryEngine();
    }

    public void close(ClusterName clusterName) throws ConnectionException {
        this.deepConnector.close(clusterName);
    }
}
