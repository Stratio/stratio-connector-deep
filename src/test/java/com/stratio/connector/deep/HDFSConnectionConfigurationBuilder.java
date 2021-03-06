package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

public class HDFSConnectionConfigurationBuilder {

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("hdfs");

    public static final String HOST = "10.200.0.60";

    public static final String PORT = "9000";

    private static final String HDFS = "hdfs";

    /**
     * Create the configuration object to config the connector cluster information
     *
     * @return Cluster configuration object
     */
    public static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConstants.HOST, HOST);
        options.put(ExtractorConstants.HOSTS, HOST);
        options.put(ExtractorConstants.PORTS, PORT);
        options.put(ExtractorConstants.INNERCLASS, HDFS);

        options.put(ExtractorConstants.FS_FILE_SEPARATOR, ",");
        options.put(ExtractorConstants.FS_FILE_PATH, "/");

        // options.put(ExtractorConstants.FS_SCHEMA,"[id:java.lang.String,author:java.lang.String," +
        // "title:java.lang.String,year:java.lang.Integer,length:java.lang.Integer,single:java.lang.String]");
        options.put(ExtractorConstants.TYPE, ExtractorConstants.HDFS_TYPE);
        options.put(ExtractorConstants.TABLE, "songs");
        options.put(ExtractorConstants.CATALOG, "test");
        options.put(ExtractorConstants.HDFS_FILE_EXTENSION, ".csv");
        Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put(DeepConnectorConstants.PROPERTY_DEFAULT_LIMIT,
                        String.valueOf(DeepConnectorConstants.DEFAULT_RESULT_SIZE));
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, connectorOptions,
                        options);
        configuration.setDataStoreName(new DataStoreName("hdfs"));
        return configuration;
    }
}
