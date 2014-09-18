package com.stratio.connector.deep.connection;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by dgomez on 18/09/14.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {DeepConnectionHandler.class})
public class ConnectionHandlerTest {


    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private DeepConnectionHandler connectionHandle = null;
    @Mock
    private IConfiguration iConfiguration;


    @Before
    public void before() throws Exception {
        connectionHandle = new DeepConnectionHandler(iConfiguration);

    }


    @After
    public void after() throws Exception {
    }

    /**
     * Method: createConnection(String clusterName, Connection connection)
     */
    @Test
    public void testCreateConnection() throws Exception, HandlerConnectionException {

        ICredentials credentials = mock(ICredentials.class);
        Map<String, String> options = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), options);

        DeepConnection connection = mock(DeepConnection.class);
        whenNew(DeepConnection.class).withArguments(credentials, config).thenReturn(connection);

        connectionHandle.createConnection(credentials, config);

        Map<String, DeepConnection> mapConnection = (Map<String, DeepConnection>) Whitebox.getInternalState(connectionHandle, "connections");

        DeepConnection recoveredConnection = mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null", recoveredConnection);
        assertEquals("The recoveredConnection is correct", connection, recoveredConnection);
    }

    @Test
    public void testCloseConnection() throws Exception {

        Map<String, DeepConnection> mapConnection = (Map<String, DeepConnection>) Whitebox.getInternalState(connectionHandle, "connections");
        DeepConnection connection = mock(DeepConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        connectionHandle.closeConnection(CLUSTER_NAME);


        assertFalse(mapConnection.containsKey(CLUSTER_NAME));
        verify(connection, times(1)).close();
    }
    @Test
    public void testGetConnection() throws HandlerConnectionException{

    }

}
