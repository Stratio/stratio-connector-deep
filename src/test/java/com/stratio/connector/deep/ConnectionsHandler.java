package com.stratio.connector.deep;

import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

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

    public void shutdown() throws ExecutionException {
        this.deepConnector.shutdown();
    }
}
