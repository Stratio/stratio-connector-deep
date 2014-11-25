package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

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
        options.put(ExtractorConstants.HOST, HOST);
        options.put(ExtractorConstants.PORT, PORT);
        options.put(ExtractorConstants.INNERCLASS, MONGO_CELL_CLASS);
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options,options);
        configuration.setDataStoreName(new DataStoreName("mongo"));
        return configuration;
    }

}
