/**
 * 
 */
package com.stratio.connector.deep.connection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Unit tests related to the {@link DeepConnection} class.
 */
@RunWith(PowerMockRunner.class)
public class DeepConnectionTest {

    private final static String MOCK_CONSTANT = "MOCK";

    private final static String CASSANDRA_CONSTANT = "cassandra";

    private final static String UNKNOWN_CONSTANT = "unknown";

    @Mock
    private ConnectorClusterConfig config;

    @Mock
    private ICredentials credentials;

    @Mock
    private Map<String, String> clusterOptions;

    @Mock
    private ConnectionHandler connectionHandler;

    @Test
    public void createNewDeepConnectionWithDataSourceNameTest()
            throws ConnectionException, HandlerConnectionException, InitializationException {

        // Set up
        DataStoreName dataStoreName = new DataStoreName(CASSANDRA_CONSTANT);

        // Stubbing
        when(config.getClusterOptions()).thenReturn(clusterOptions);
        when(clusterOptions.get(any(String.class))).thenReturn(MOCK_CONSTANT);
        when(clusterOptions.get(DeepConnectorConstants.EXTRACTOR_IMPL_CLASS)).thenReturn("com.stratio.deep.cassandra.extractor.CassandraCellExtractor");
        when(config.getDataStoreName()).thenReturn(dataStoreName);


        // Execution
        DeepConnection connection = new DeepConnection(credentials, config);

        // Assertions
        assertEquals("Data store name not set properly", "com.stratio.deep.cassandra.extractor.CassandraCellExtractor",
                connection.getExtractorConfig().getExtractorImplClassName());
    }

    @Test(expected = ConnectionException.class)
    public void createNewDeepConnectionWithUnknownDataSourceNameTest() throws ConnectionException {

        // Set up
        DataStoreName dataStoreName = new DataStoreName(UNKNOWN_CONSTANT);

        // Stubbing
        when(config.getClusterOptions()).thenReturn(clusterOptions);
        when(clusterOptions.get(any(String.class))).thenReturn(MOCK_CONSTANT);
        when(clusterOptions.get(DeepConnectorConstants.EXTRACTOR_IMPL_CLASS)).thenReturn(null);

        when(config.getDataStoreName()).thenReturn(dataStoreName);

        // Execution
        new DeepConnection(credentials, config);
    }
}
