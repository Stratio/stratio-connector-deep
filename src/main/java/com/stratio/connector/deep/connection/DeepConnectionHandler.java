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




    public DeepConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    @Override
    public boolean isConnected(String clusterName) {
        return super.isConnected(clusterName);
    }


    @Override
    protected Connection createNativeConnection(ICredentials iCredentials, ConnectorClusterConfig connectorClusterConfig) throws CreateNativeConnectionException {

        Connection connection;

        connection = new DeepConnection(iCredentials,connectorClusterConfig) ;

        return connection;
    }


}
