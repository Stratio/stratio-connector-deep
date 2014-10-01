package com.stratio.connector.deep.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ContextProperties;
import com.stratio.connector.deep.engine.DeepStorageEngine;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by dgomez on 15/09/14.
 */
public class DeepContextConnectorTest {

    private static final Logger logger = Logger.getLogger(DeepContextConnectorTest.class);

    /**
     * Default Deep HOST using 127.0.0.1.
     */
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final String CLUSTER_NAME = "CLUSTER_NAME";

    /**
     *
     */
    protected static DeepContextConnector deepConnector;

    @Before
    public void setUp() {
        // TODO Start All the DeepSparkContext
        // TODO loadTestData
        // Creating the Deep Context
        String job = "java:deepSparkContext";
        String[] args = null;

        deepConnector = new DeepContextConnector();

    }

    @Test
    public void testInit() throws Exception {

        IConfiguration iconfiguration = mock(IConfiguration.class);

        deepConnector.init(iconfiguration);

        DeepConnectionHandler connectionHandler = (DeepConnectionHandler) Whitebox.getInternalState(deepConnector,
                "connectionHandler");

        Object recoveredConfiguration = Whitebox.getInternalState(connectionHandler, "configuration");

        assertNotNull("The configuration is not null", recoveredConfiguration);
        assertEquals("The configuration is correct", iconfiguration, recoveredConfiguration);
        assertNotNull("The connection handle is not null", connectionHandler);
    }

    /**
     * Establish the connection with DeepSparkContext in order to be able to retrieve metadata from the system columns
     * with the connection config.
     * 
     * @param
     * @return Whether the connection has been established or not.
     */

    @Test
    public void testConnect() throws Exception, HandlerConnectionException {

        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, options);
        DeepConnectionHandler connectionHandler = mock(DeepConnectionHandler.class);
        Whitebox.setInternalState(deepConnector, "connectionHandler", connectionHandler);

        deepConnector.connect(iCredentials, config);

        verify(connectionHandler, times(1)).createConnection(iCredentials, config);
    }

    @Test
    // (expected = HandlerConnectionException.class)
    public void testClose() throws ConnectionException, HandlerConnectionException {

        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, options);
        DeepConnectionHandler connectionHandler = mock(DeepConnectionHandler.class);
        Whitebox.setInternalState(deepConnector, "connectionHandler", connectionHandler);

        deepConnector.connect(iCredentials, config);

        connectionHandler.closeConnection(clusterName.getName());

        DeepConnection conn = (DeepConnection) connectionHandler.getConnection(clusterName.getName());
        assertNull(conn);

    }

}
