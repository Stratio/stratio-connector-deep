package com.stratio.connector.deep.connection;


import com.stratio.connector.ConnectorApp;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;

import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.connector.deep.engine.DeepStorageEngine;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


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
        return new String[] {"DeepConnector"};
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {

        connectionHandler = new DeepConnectionHandler(configuration);

    }

    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        try{

            connectionHandler.createConnection(credentials, config);

        } catch (HandlerConnectionException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close(ClusterName name) throws ConnectionException {

        connectionHandler.closeConnection(name.getName());
    }

    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {

        //TODO throw new UnsupportedException("");
        return null;

    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {

        return new DeepStorageEngine(connectionHandler);

    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {

        //TODO throw new UnsupportedException("");
        return null;
    }
}
