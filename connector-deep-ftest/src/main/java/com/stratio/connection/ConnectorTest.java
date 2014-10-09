package com.stratio.connection;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;


import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.connector.deep.connection.DeepContextConnector;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;

public class ConnectorTest {

    private static final String CASSANDRA_CELL_CLASS =  "com.stratio.deep.cassandra.extractor.CassandraCellExtractor";

    private static final String CATALOG_CONSTANT = "test";

    private static final TableName TABLE1_CONSTANT = new TableName(CATALOG_CONSTANT, "mytable1");

    private static final TableName TABLE2_CONSTANT = new TableName(CATALOG_CONSTANT, "mytable");

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("clusterName");

    private static final String DATA_CONSTANT = "user1";

    private String KEYSPACENAME = CATALOG_CONSTANT;
    private String TABLENAME_1 = TABLE1_CONSTANT.getName();
    private String TABLENAME_2 = TABLE2_CONSTANT.getName();
    private String CQLPORT = "9042";
    private String RPCPORT = "9160";
    private String HOST = "127.0.0.1";


    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private DeepConnectionHandler connectionHandler = null;

    private IConfiguration iConfiguration;
    DeepSparkContext deepSparkContext;



    public DeepContextConnector getConnection() throws HandlerConnectionException {

        prepareConnectionConfig();
        connectionHandler.getConnection(CLUSTERNAME_CONSTANT.getName());
        return new DeepContextConnector();
    }

    private void prepareConnectionConfig() throws HandlerConnectionException {

        iConfiguration = mock(IConfiguration.class);
        connectionHandler = new DeepConnectionHandler(iConfiguration);
        deepSparkContext = ConnectionConfiguration.getDeepContext();

        ICredentials credentials = mock(ICredentials.class);
        Map<String, String> options = new HashMap<>();
        options.put(ExtractorConnectConstants.HOST, HOST);

        options.put(ExtractorConnectConstants.PORT, CQLPORT);
        options.put(ExtractorConnectConstants.KEYSPACE, KEYSPACENAME);
        options.put(ExtractorConnectConstants.TABLE, TABLENAME_1);
        options.put(ExtractorConnectConstants.PORT, CQLPORT);
        options.put(ExtractorConnectConstants.RPCPORT, RPCPORT);
        options.put(ExtractorConnectConstants.INNERCLASS, CASSANDRA_CELL_CLASS);
        ConnectorClusterConfig config = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT,options);
        connectionHandler.createConnection(credentials, config);
    }




}
