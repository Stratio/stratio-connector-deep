package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

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
        options.put(ExtractorConstants.HOST, HOST);
        options.put(ExtractorConstants.PORT, PORT);
        options.put(ExtractorConstants.INNERCLASS, AEROSPIKE_CELL_CLASS);
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options,options);
        configuration.setDataStoreName(new DataStoreName("aerospike"));
        return configuration;
    }

}
