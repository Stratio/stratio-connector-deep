package com.stratio.connector.deep.connection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.security.ICredentials;

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
    private static final String DATASTORE_NAME = "mongo";

    /**
     *
     */
    protected static DeepConnector deepConnector;

    @Before
    public void setUp() throws InitializationException {
        // TODO Start All the DeepSparkContext
        // TODO loadTestData
        // Creating the Deep Context
        String job = "java:deepSparkContext";
        String[] args = null;

        deepConnector = new DeepConnector();

    }

    @Test
    public void testInit() throws Exception {

        IConfiguration iconfiguration = mock(IConfiguration.class);

        deepConnector.init(iconfiguration);

        DeepConnectionHandler connectionHandler = (DeepConnectionHandler) Whitebox.getInternalState(deepConnector,
                        "connectionHandler");

        Object recoveredConfiguration = Whitebox.getInternalState(connectionHandler, "configuration");

        assertNull("The configuration is not null", recoveredConfiguration);
        assertNotNull("The connection handle is not null", connectionHandler);
        // assertEquals ("The configuration is correct" , iconfiguration, recoveredConfiguration);
        deepConnector.shutdown();
    }

    /**
     * Establish the connection with DeepSparkContext in order to be able to retrieve metadata from the system columns
     * with the connection config.
     * 
     * @param
     * @return Whether the connection has been established or not.
     */

    @Test
    public void testConnect() throws Exception {

        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, options, options);
        config.setDataStoreName(new DataStoreName(DATASTORE_NAME));

        DeepConnectionHandler connectionHandler = mock(DeepConnectionHandler.class);
        Whitebox.setInternalState(deepConnector, "connectionHandler", connectionHandler);

        deepConnector.connect(iCredentials, config);

        verify(connectionHandler, times(1)).createConnection(iCredentials, config);
    }

    @Test
    public void testClose() throws ConnectionException, ExecutionException {

        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, options, options);
        config.setDataStoreName(new DataStoreName(DATASTORE_NAME));
        DeepConnectionHandler connectionHandler = mock(DeepConnectionHandler.class);
        Whitebox.setInternalState(deepConnector, "connectionHandler", connectionHandler);

        deepConnector.connect(iCredentials, config);

        connectionHandler.closeConnection(clusterName.getName());

        DeepConnection conn = (DeepConnection) connectionHandler.getConnection(clusterName.getName());
        assertNull(conn);

    }

}
