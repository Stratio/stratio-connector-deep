package com.stratio.connector.deep.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dgomez on 18/09/14.
 */
public class DeepConnectionHandler extends ConnectionHandler {

    private Map<String, ExtractorConfig> extractorClusters;

    DeepSparkContext deepContext = ConnectionConfiguration.getDeepContext();


    public DeepConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }


    @Override
    protected Connection createNativeConnection(ICredentials iCredentials, ConnectorClusterConfig connectorClusterConfig) throws CreateNativeConnectionException {

        Connection connection;
        ClusterName clusterName = connectorClusterConfig.getName();
        Map<String, String> clusterOptions = connectorClusterConfig.getOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig();


        Map<String, String> values = new HashMap<String, String>();

        values.put(ExtractorConnectConstants.PORT,  clusterOptions.get("Port"));
        String[] hosts =   clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1).split(",");

        values.put(ExtractorConnectConstants.HOST, hosts[0] );
        values.put(ExtractorConnectConstants.HOSTS, clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1) );

        extractorconfig.setValues(values);

        deepContext = ConnectionConfiguration.getDeepContext();

        extractorClusters.put(clusterName.getName(), extractorconfig);

        return null;
    }
}
