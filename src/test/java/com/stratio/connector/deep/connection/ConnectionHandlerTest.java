package com.stratio.connector.deep.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * Created by dgomez on 18/09/14.
 */

@RunWith(PowerMockRunner.class)
public class ConnectionHandlerTest {

    private static final String CLUSTER_NAME = "cluster_name";
    private DeepConnectionHandler connectionHandler = null;
    @Mock
    private IConfiguration iConfiguration;
    DeepSparkContext deepSparkContext;

    @Before
    public void before() throws Exception {

        iConfiguration = mock(IConfiguration.class);
        connectionHandler = new DeepConnectionHandler(iConfiguration);
        deepSparkContext = ConnectionConfiguration.getDeepContext();
    }

    /**
     * Method: createConnection(String clusterName, Connection connection)
     */
    @Test
    public void testCreateConnection() throws Exception, HandlerConnectionException {

        ICredentials credentials = mock(ICredentials.class);
        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConstants.HOST, "127.0.0.1");
        options.put(ExtractorConstants.HOSTS, "127.0.0.1 , 127.0.0.2");
        options.put(ExtractorConstants.PORT, "PORT");

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), options,options );
        config.setDataStoreName(new DataStoreName("dataStoreName"));

        DeepConnection connection = mock(DeepConnection.class);
        whenNew(DeepConnection.class).withArguments(credentials, config).thenReturn(connection);

        connectionHandler.createConnection(credentials, config);

        Map<String, DeepConnection> mapConnection = (Map<String, DeepConnection>) Whitebox.getInternalState(
                connectionHandler, "connections");

        DeepConnection recoveredConnection = mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null", recoveredConnection);
        // assertEquals("The recoveredConnection is correct", connection, recoveredConnection);
    }

    @Test
    public void testCloseConnection() throws Exception {

        Map<String, DeepConnection> mapConnection = (Map<String, DeepConnection>) Whitebox.getInternalState(
                connectionHandler, "connections");
        DeepConnection connection = mock(DeepConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        connectionHandler.closeConnection(CLUSTER_NAME);

        assertFalse(mapConnection.containsKey(CLUSTER_NAME));
        verify(connection, times(1)).close();
    }

    @Test
    public void testGetConnection() throws HandlerConnectionException {

    }

}
