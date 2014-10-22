package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;

public class MongoConnectionConfigurationBuilder {

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("mongodb");

    private static final String MONGO_CELL_CLASS = "com.stratio.deep.mongodb.extractor.MongoCellExtractor";

    private static final String HOST = "localhost";

    private static final String PORT = "27017";

    /**
     * Create the configuration object to config the connector cluster information
     * 
     * @return Cluster configuration object
     */
    public static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConnectConstants.HOST, HOST);
        options.put(ExtractorConnectConstants.PORT, PORT);
        options.put(ExtractorConnectConstants.INNERCLASS, MONGO_CELL_CLASS);
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options);

        return configuration;
    }

}
