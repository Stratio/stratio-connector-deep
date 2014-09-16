package com.stratio.connector.deep.connector;

import com.stratio.connector.deep.configuration.ConfigurationConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;

import java.util.Map;

/**
 * Created by dgomez on 16/09/14.
 */
public class DeepContextConnector implements IConnector {


    @Override
    public String getConnectorName() {
        return null;
    }

    @Override
    public String[] getDatastoreName() {
        return new String[0];
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {

    }

    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getOptions();


        DeepSparkContext deepContext = new DeepSparkContext(clusterOptions.get(ConfigurationConstants.HOST),clusterOptions.get(ConfigurationConstants.JOB),clusterOptions.get(ConfigurationConstants.SPARK_HOME),clusterOptions.get(ConfigurationConstants.JARS));

    }

    @Override
    public void close(ClusterName name) throws ConnectionException {

    }

    @Override
    public boolean isConnected(ClusterName name) {
        return false;
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {
        return null;
    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return null;
    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return null;
    }
}
