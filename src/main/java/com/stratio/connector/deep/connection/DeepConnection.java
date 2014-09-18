package com.stratio.connector.deep.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dgomez on 18/09/14.
 */
public class DeepConnection implements Connection<DeepSparkContext> {


    private DeepSparkContext deepSparkContext;

    private boolean isConnect = false;

    private Map<String,ExtractorConfig> extractorConfigMap ;


    /**
     * Constructor.
     *
     * @param credentials the credentials.
     * @param config      The cluster configuration.
     */
    public DeepConnection(ICredentials credentials, ConnectorClusterConfig config) {
        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig();


        Map<String, String> values = new HashMap<String, String>();

        values.put(ExtractorConnectConstants.PORT,  clusterOptions.get("Port"));
        String[] hosts =   clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1).split(",");

        values.put(ExtractorConnectConstants.HOST, hosts[0] );
        values.put(ExtractorConnectConstants.HOSTS, clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1) );

        extractorconfig.setValues(values);

        extractorConfigMap.put(clusterName.getName(), extractorconfig);

        deepSparkContext = ConnectionConfiguration.getDeepContext();
    }


    @Override
    public void close() {
        if (deepSparkContext != null) {
            deepSparkContext.stop();
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


    public Map<String, ExtractorConfig> getExtractorConfigMap() {
        return extractorConfigMap;
    }
}
