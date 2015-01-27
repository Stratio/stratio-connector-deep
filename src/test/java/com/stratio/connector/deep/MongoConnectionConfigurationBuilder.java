package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

public class MongoConnectionConfigurationBuilder {

    public static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("mongodb");

    private static final String MONGO_CELL_CLASS = "com.stratio.deep.mongodb.extractor.MongoCellExtractor";

    public static final String HOST = "10.200.0.58";

    public static final String PORT = "27100";

    /**
     * Create the configuration object to config the connector cluster information
     * 
     * @return Cluster configuration object
     */
    public static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConstants.HOST, HOST);
        options.put(ExtractorConstants.PORT, PORT);
        options.put(ExtractorConstants.INNERCLASS, MONGO_CELL_CLASS);
        Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put(DeepConnectorConstants.PROPERTY_DEFAULT_LIMIT,
                        String.valueOf(DeepConnectorConstants.DEFAULT_RESULT_SIZE));
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, connectorOptions,
                        options);
        configuration.setDataStoreName(new DataStoreName("mongo"));
        return configuration;
    }

}
