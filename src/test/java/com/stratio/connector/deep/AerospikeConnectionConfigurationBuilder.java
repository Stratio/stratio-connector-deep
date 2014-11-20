package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;

public class AerospikeConnectionConfigurationBuilder {

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("aerospike");

    private static final String AEROSPIKE_CELL_CLASS = "com.stratio.deep.aerospike.extractor.AerospikeCellExtractor";

    private static final String HOST = "10.200.0.58";

    private static final String PORT = "3000";

    /**
     * Create the configuration object to config the connector cluster information
     * 
     * @return Cluster configuration object
     */
    public static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConnectConstants.HOST, HOST);
        options.put(ExtractorConnectConstants.PORT, PORT);
        options.put(ExtractorConnectConstants.INNERCLASS, AEROSPIKE_CELL_CLASS);
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options);

        return configuration;
    }

}
