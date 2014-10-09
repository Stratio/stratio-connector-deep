package com.stratio.connection;



import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;

import com.stratio.connector.deep.configuration.ExtractorConnectConstants;

import com.stratio.connector.deep.connection.DeepContextConnector;
import com.stratio.connector.deep.engine.DeepMetadataEngine;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;

import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;

import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;


public class DeepConnectorTest {

    /**
     * Class logger.
     */
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DeepConnectorTest.class);

    private static final String CASSANDRA_CELL_CLASS = "com.stratio.deep.cassandra.extractor.CassandraCellExtractor";
    private static final String CATALOG_CONSTANT = "test";
    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("clusterName");
    private static final String KEYSPACENAME = CATALOG_CONSTANT;
    private static final String CQLPORT = "9042";
    private static final String RPCPORT = "9160";
    private static final String HOST = "127.0.0.1";
    private static final String CLUSTER_NAME = "clusterName";



    DeepContextConnector deepContextConnector = new DeepContextConnector();


    @Before
    public void setUp()
            throws InitializationException, ConnectionException, UnsupportedException, HandlerConnectionException {

        deepContextConnector.init(getConfiguration());

    }

    @Test
    public void isConnectedTest() throws HandlerConnectionException, InitializationException,
            ConnectionException, UnsupportedException {

        deepContextConnector.connect(getICredentials(), prepareConnectionConfig());
        assertEquals(true, deepContextConnector.isConnected(CLUSTERNAME_CONSTANT));
    }

    @Test
    public void closeConnectionTest() throws HandlerConnectionException, InitializationException,
            ConnectionException, UnsupportedException {

        deepContextConnector.connect(getICredentials(), prepareConnectionConfig());
        deepContextConnector.close(CLUSTERNAME_CONSTANT);
        assertEquals(false, deepContextConnector.isConnected(CLUSTERNAME_CONSTANT));

    }

    @Test
    public void getQueryEngineTest() throws HandlerConnectionException, InitializationException,
            ConnectionException, UnsupportedException {

        deepContextConnector.connect(getICredentials(), prepareConnectionConfig());
        deepContextConnector.getQueryEngine();

    }

    @Test(expected = UnsupportedException.class )
    public void getMetadataEngineTest() throws HandlerConnectionException, InitializationException,
            ConnectionException, UnsupportedException {

        deepContextConnector.connect(getICredentials(), prepareConnectionConfig());
        DeepMetadataEngine deepMetadataEngine = (DeepMetadataEngine) deepContextConnector.getMetadataEngine();

    }

    private ConnectorClusterConfig prepareConnectionConfig() throws HandlerConnectionException {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConnectConstants.HOST, HOST);

        options.put(ExtractorConnectConstants.PORT, CQLPORT);
        options.put(ExtractorConnectConstants.KEYSPACE, KEYSPACENAME);

        options.put(ExtractorConnectConstants.PORT, CQLPORT);
        options.put(ExtractorConnectConstants.RPCPORT, RPCPORT);
        options.put(ExtractorConnectConstants.INNERCLASS, CASSANDRA_CELL_CLASS);

        ConnectorClusterConfig config = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT,options);

        return config;
    }

    public IConfiguration getConfiguration() {
        return null;
    }

    public ICredentials getICredentials() {
        return null;
    }
}
