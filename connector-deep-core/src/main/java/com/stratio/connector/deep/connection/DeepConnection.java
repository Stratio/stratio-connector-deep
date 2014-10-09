package com.stratio.connector.deep.connection;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;


public class DeepConnection extends Connection {

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
        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig<>(Cells.class);

        Map<String, Serializable> values = new HashMap<>();

        if(clusterOptions.get(ExtractorConnectConstants.HOSTS)!=null){
            values.put(ExtractorConnectConstants.HOSTS, clusterOptions.get(ExtractorConnectConstants.HOSTS));
            String[] hosts = clusterOptions.get(ExtractorConnectConstants.HOSTS)
                    .substring(1, clusterOptions.get(ExtractorConnectConstants.HOSTS).length() - 1).split(",");

            values.put(ExtractorConnectConstants.HOST, hosts[0]);
        }else{
            values.put(ExtractorConnectConstants.HOST, clusterOptions.get(ExtractorConnectConstants.HOST));
        }

        if(clusterOptions.get(ExtractorConnectConstants.PORTS)!=null){
            values.put(ExtractorConnectConstants.PORTS, clusterOptions.get(ExtractorConnectConstants.PORTS));
            String[] ports = clusterOptions.get(ExtractorConnectConstants.PORTS)
                    .substring(1, clusterOptions.get(ExtractorConnectConstants.PORTS).length() - 1).split(",");

            values.put(ExtractorConnectConstants.PORT, ports[0]);
        }else{
            values.put(ExtractorConnectConstants.PORT, clusterOptions.get(ExtractorConnectConstants.PORT));
        }
        values.put(ExtractorConnectConstants.CQLPORT,clusterOptions.get(ExtractorConnectConstants.CQLPORT) );
        values.put(ExtractorConnectConstants.RCPPORT,clusterOptions.get(ExtractorConnectConstants.RCPPORT) );
        values.put(ExtractorConnectConstants.INNERCLASS,clusterOptions.get(ExtractorConnectConstants.INNERCLASS) );

        extractorconfig.setValues(values);

        extractorConfig = extractorconfig;

        deepSparkContext = ConnectionConfiguration.getDeepContext();
    }

    @Override
    public void close() {
        if (deepSparkContext != null) {
            // deepSparkContext.stop();
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

}
