package com.stratio.connector.deep;

import java.util.HashMap;
import java.util.Map;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

public class HDFSConnectionConfigurationBuilder {

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("hdfs");

    private static final String HOST = "127.0.0.1";

    private static final String PORT = "9000";

    private static final String HDFS ="hdfs";

    /**
     * Create the configuration object to config the connector cluster information
     *
     * @return Cluster configuration object
     */
    public static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConstants.HOST, HOST);
        options.put(ExtractorConstants.PORT, PORT);
        options.put(ExtractorConstants.INNERCLASS, HDFS);

        options.put(ExtractorConstants.HDFS_FILE_SEPARATOR, ",");
        options.put(ExtractorConstants.HDFS_FILE_PATH, "user/hadoop/test/songs.csv");

        options.put(ExtractorConstants.HDFS_SCHEMA,"[id:java.lang.String,author:java.lang.String," +
                "title:java.lang.String,year:java.lang.Integer,length:java.lang.Integer,single:java.lang.String]");
        options.put(ExtractorConstants.TYPE,ExtractorConstants.HDFS_TYPE);
        options.put(ExtractorConstants.TABLE,"songs");
        options.put(ExtractorConstants.CATALOG,"test");



        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options);

        return configuration;
    }
}
