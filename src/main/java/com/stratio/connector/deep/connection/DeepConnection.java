package com.stratio.connector.deep.connection;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ClusterProperties;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;

public class DeepConnection extends Connection {

    private Properties configProperties ;

    private final DeepSparkContext deepSparkContext;

    private boolean isConnect = false;

    private final ExtractorConfig extractorConfig;

    /**
     * Constructor.
     * 
     * @param credentials
     *            the credentials.
     * @param config
     *            The cluster configuration.
     */
    public DeepConnection(ICredentials credentials, ConnectorClusterConfig config) {

        Map<String, String> clusterOptions = config.getOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig<>(Cells.class);

        Map<String, Serializable> values = new HashMap<>();

        if (clusterOptions.get(ExtractorConnectConstants.HOSTS) != null) {
            values.put(ExtractorConnectConstants.HOSTS, clusterOptions.get(ExtractorConnectConstants.HOSTS));
            String[] hosts = clusterOptions.get(ExtractorConnectConstants.HOSTS)
                    .substring(1, clusterOptions.get(ExtractorConnectConstants.HOSTS).length() - 1).split(",");

            values.put(ExtractorConnectConstants.HOST, hosts[0]);
        } else {
            values.put(ExtractorConnectConstants.HOST, clusterOptions.get(ExtractorConnectConstants.HOST));
        }

        if (clusterOptions.get(ExtractorConnectConstants.PORTS) != null) {
            values.put(ExtractorConnectConstants.PORTS, clusterOptions.get(ExtractorConnectConstants.PORTS));
            String[] ports = clusterOptions.get(ExtractorConnectConstants.PORTS)
                    .substring(1, clusterOptions.get(ExtractorConnectConstants.PORTS).length() - 1).split(",");

            values.put(ExtractorConnectConstants.PORT, ports[0]);
        } else {
            values.put(ExtractorConnectConstants.PORT, clusterOptions.get(ExtractorConnectConstants.PORT));
        }

        extractorconfig.setValues(values);

        ClusterProperties clusterProperties = new ClusterProperties();

        //TODO Find new field add by meta to recognise the database to associate the CellExtractor config correct
        String dataBase = checkDatabaseFromClusterName(config);

        extractorconfig.setExtractorImplClassName(clusterProperties.getValue("cluster." + dataBase + "."
                + ExtractorConnectConstants.INNERCLASS));

        extractorConfig = extractorconfig;

        configProperties = ConnectionConfiguration.getConfigProperties();

        deepSparkContext = ConnectionConfiguration.getDeepContext();

        isConnect = true;
    }



    @Override
    public void close() {
        if (deepSparkContext != null) {

            isConnect = false;
        }

    }

    @Override
    public boolean isConnect() {

        return isConnect;
    }

    @Override
    public DeepSparkContext getNativeConnection() {
        return deepSparkContext;
    }

    public ExtractorConfig getExtractorConfig() {
        return extractorConfig;
    }

    public void forceShutDown() throws HandlerConnectionException {
        deepSparkContext.stop();
    }


    private String checkDatabaseFromClusterName(ConnectorClusterConfig config) {

        String db ="";
        if(config.getName().getName().contains(ExtractorConnectConstants.db_cassandra)){
            db = ExtractorConnectConstants.db_cassandra;
        }else  if(config.getName().getName().contains(ExtractorConnectConstants.db_mongo)){
            db = ExtractorConnectConstants.db_mongo;
        }else  if(config.getName().getName().contains(ExtractorConnectConstants.db_elasticsearch)){
            db = ExtractorConnectConstants.db_elasticsearch;
        }

        return db;
    }

    public Properties getConfigProperties() {
        return configProperties;
    }
}
