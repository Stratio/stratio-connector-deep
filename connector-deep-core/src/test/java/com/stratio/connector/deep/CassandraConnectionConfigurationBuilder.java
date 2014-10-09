package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta2.common.data.ClusterName;

public class CassandraConnectionConfigurationBuilder {

    private static final String CASSANDRA_CELL_CLASS = "com.stratio.deep.cassandra.extractor.CassandraCellExtractor";

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("cassandra");

    private static final String CQLPORT = "9042";

    private static final String RPCPORT = "9160";

    private static final String HOST = "127.0.0.1";

    /**
     * Create the configuration object to config the connector cluster information
     * 
     * @return Cluster configuration object
     */
    public static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConnectConstants.HOST, HOST);
        options.put(ExtractorConnectConstants.PORT, CQLPORT);
        options.put(ExtractorConnectConstants.PORT, CQLPORT);
        options.put(ExtractorConnectConstants.RPCPORT, RPCPORT);
        options.put(ExtractorConnectConstants.INNERCLASS, CASSANDRA_CELL_CLASS);
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options);

        return configuration;
    }
}
