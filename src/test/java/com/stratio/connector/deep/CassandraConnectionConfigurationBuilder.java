package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

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
        options.put(ExtractorConstants.HOST, HOST);
        options.put(ExtractorConstants.PORT, CQLPORT);
        options.put(ExtractorConstants.RPCPORT, RPCPORT);
        options.put(ExtractorConstants.INNERCLASS, CASSANDRA_CELL_CLASS);
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options,options);
        configuration.setDataStoreName(new DataStoreName("cassandra"));
        return configuration;
    }
}
