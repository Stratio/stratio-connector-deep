/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.deep.connection;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.connectors.ConnectorApp;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * Class implements Crossdata Interface to connect. {@link com.stratio.crossdata.common.connector.IConnector}.
 *
 */
public class DeepConnector implements IConnector {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connectionHandler.
     */
    private DeepConnectionHandler connectionHandler;

    /**
     * The deepContext.
     */
    private DeepSparkContext deepContext;


    /**
     * Main uses to asociate the connector to crossdata.
     *
     * */
    public static void main(String[] args) {

        DeepConnector deepConnector = new DeepConnector();

        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(deepConnector);
    }

    @Override
    public String getConnectorName() {
        return "DeepConnector";
    }

    @Override
    public String[] getDatastoreName() {
        return new String[] { "DeepConnector" };
    }

    /**
    * Init Connection.
    * @param  configuration @see{com.stratio.connector.deep.configuration.ConnectionConfiguration}.
    */
    @Override
    public void init(IConfiguration configuration) throws InitializationException {

        this.connectionHandler = new DeepConnectionHandler(new ConnectionConfiguration());
        this.deepContext = ConnectionConfiguration.getDeepContext();

    }
    /**
       * Connect with the config expecified associate to a clusterName {ConnectionHandler}.
       * {@link com.stratio.connector.deep.connection.DeepConnectionHandler.createNativeConnection}.
       *
       * @param  credentials
       * @param  config {@link com.stratio.crossdata.common.connector.ConnectorClusterConfig}
       */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        try {

            connectionHandler.createConnection(credentials, config);

        } catch (HandlerConnectionException e) {
            String msg = "fail creating the Connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);
        }

    }

    /**
       * Close connection associate to the clusterName.
       * @see{com.stratio.connector.commons.connection.ConnectionHandler.close}.
       *
       * @param name {@link com.stratio.crossdata.common.data.ClusterName}
       */
    @Override
    public void close(ClusterName name) throws ConnectionException {

        connectionHandler.closeConnection(name.getName());
    }

    /**
      * Shutdown when all the connections associate to the clusterNames end all the works
      * stop the context.
      */
    @Override
    public void shutdown() throws ExecutionException {

        Iterator it = connectionHandler.getConnections().values().iterator();
        while (it.hasNext()) {
            DeepConnection conn = (DeepConnection) it.next();
            while (conn.isWorkInProgress()) {
                shutdown();
            }
        }
        deepContext.stop();
    }

    /**
     * Check if the  connection associate to the clusterName is connected.
     * {@link com.stratio.connector.commons.connection.ConnectionHandler.isConnected}.
     *
     * @param name {@link com.stratio.crossdata.common.data.ClusterName}
     * @return boolean
     *
     */
    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());
    }

    /**
      * Unsupported method.
      * @return IStorageEngine
      *
      */

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {

        throw new UnsupportedException("Not yet supported");

    }

    /*
     * Return  the interface to invoke queries from crossdata.
     * @return DeepQueryEngine.
     *
     */
    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {

        return new DeepQueryEngine(deepContext, connectionHandler);

    }

    /*
   * Unsupported method.
   * @return IMetadataEngine.
   *
   */
    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {

        throw new UnsupportedException("Not yet supported");

    }
}
