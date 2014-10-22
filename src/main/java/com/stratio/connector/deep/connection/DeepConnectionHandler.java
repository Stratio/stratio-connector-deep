package com.stratio.connector.deep.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Created by dgomez on 18/09/14.
 */
public class DeepConnectionHandler extends ConnectionHandler {

    public DeepConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }


    @Override
    protected Connection createNativeConnection(ICredentials iCredentials, ConnectorClusterConfig connectorClusterConfig)
            throws CreateNativeConnectionException {

        Connection connection;

        connection = new DeepConnection(iCredentials, connectorClusterConfig);

        return connection;
    }

}
