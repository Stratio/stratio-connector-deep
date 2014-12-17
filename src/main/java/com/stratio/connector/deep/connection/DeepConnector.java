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

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.DeepConnectorConstants;
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
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

/**
 * Class implements Crossdata Interface to connect. {@link com.stratio.crossdata.common.connector.IConnector}.
 * 
 */
public class DeepConnector implements IConnector {

    private static final Logger logger = LoggerFactory.getLogger(DeepConnector.class);

    private static final String CONFIGURATION_FILE_CONSTANT = "connector-application.conf";

    /**
     * The connectionHandler.
     */
    private DeepConnectionHandler connectionHandler;

    /**
     * The deepContext.
     */
    private DeepSparkContext deepContext;

    /**
     * Connector configuration from the properties file
     */
    private Config connectorConfig;

    /**
     * Main uses to asociate the connector to crossdata.
     * 
     * */
    public static void main(String[] args) {

        DeepConnector deepConnector = new DeepConnector();

        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(deepConnector);
    }

    public DeepConnector() {

        // Retrieving configuration
        InputStream input = DeepConnector.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILE_CONSTANT);

        if (input == null) {
            logger.error("Sorry, unable to find [" + CONFIGURATION_FILE_CONSTANT + "]");
            return;
        }

        try {
            connectorConfig = ConfigFactory.load(CONFIGURATION_FILE_CONSTANT);
        } catch (Exception e) {
            logger.error("Error loading configuration from: [" + CONFIGURATION_FILE_CONSTANT + "]");
            return;
        }
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
     * 
     * @param configuration
     * @see{com.stratio.connector.deep.configuration.ConnectionConfiguration.
     */
    @Override
    public void init(IConfiguration configuration) throws InitializationException {

        this.connectionHandler = new DeepConnectionHandler(null);

        logger.info("-------------StartUp the SparkContext------------ ");

        logger.info("spark.serializer: " + System.getProperty("spark.serializer"));
        logger.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        String sparkMaster = connectorConfig.getString(DeepConnectorConstants.SPARK_MASTER);
        String sparkHome   = connectorConfig.getString(DeepConnectorConstants.SPARK_HOME);
        List<String> sparkJars   = null;
        String[] jarsArray = new String[0];
        try{
            sparkJars = connectorConfig.getConfig(DeepConnectorConstants.SPARK).getStringList(DeepConnectorConstants
                    .SPARK_JARS);

        }catch (ConfigException e){
            logger.info("--No spark Jars added--");
        }
        if(sparkJars!=null) {
            jarsArray = new String[sparkJars.size()];
            sparkJars.toArray(jarsArray);
        }

        logger.info("---SPARK-Master---->"+sparkMaster);
        logger.info("---SPARK-Home---->"+sparkHome);

        this.deepContext = new DeepSparkContext(sparkMaster, DeepConnectorConstants.DEEP_CONNECTOR_JOB_CONSTANT,
                sparkHome, jarsArray);
        
        logger.info("-------------End StartUp the SparkContext------------ ");
    }

    /**
     * Connect with the config expecified associate to a clusterName {ConnectionHandler}
     * {@link com.stratio.connector.deep.connection.DeepConnectionHandler.createNativeConnection}.
     * 
     * @param credentials
     * @param config
     *            {@link com.stratio.crossdata.common.connector.ConnectorClusterConfig}.
     */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        try {
            // Setting the extractor class
            String dataSourceName = config.getDataStoreName().getName();

            String extractorImplClassName = connectorConfig.getConfig(DeepConnectorConstants.CLUSTER_PREFIX_CONSTANT)
                    .getString(dataSourceName + DeepConnectorConstants.IMPL_CLASS_SUFIX_CONSTANT);

            config.getClusterOptions().put(DeepConnectorConstants.EXTRACTOR_IMPL_CLASS, extractorImplClassName);

            if (extractorImplClassName!=null && extractorImplClassName.equals(ExtractorConstants.HDFS)) {
                config.getClusterOptions().put(ExtractorConstants.HDFS_FILE_PATH,
                        connectorConfig.getConfig(ExtractorConstants.HDFS).getString(ExtractorConstants.HDFS_FILE_PATH));
            }


            connectionHandler.createConnection(credentials, config);

        } catch (HandlerConnectionException e) {
            String msg = "fail creating the Connection. " + e.getMessage();
            logger.error(msg);
            throw new ConnectionException(msg, e);
        }

    }

    /**
     * Close connection associate to the clusterName.
     * 
     * @see{com.stratio.connector.commons.connection.ConnectionHandler.close
     * 
     * @param name
     *            {@link com.stratio.crossdata.common.data.ClusterName}.
     */
    @Override
    public void close(ClusterName name) throws ConnectionException {

        connectionHandler.closeConnection(name.getName());
    }

    /**
     * Shutdown when all the connections associate to the clusterNames end all the works stop the context.
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
     * Check if the connection associate to the clusterName is connected
     * {@link com.stratio.connector.commons.connection.ConnectionHandler.isConnected}.
     * 
     * @param name
     *            {@link com.stratio.crossdata.common.data.ClusterName}.
     * @return boolean
     * 
     */
    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandler.isConnected(name.getName());
    }

    /**
     * Unsupported method.
     * 
     * @return IStorageEngine
     * 
     */
    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {

        throw new UnsupportedException("Not yet supported");

    }

    /*
     * Return the interface to invoke queries from crossdata.
     * 
     * @return DeepQueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {

        return new DeepQueryEngine(deepContext, connectionHandler);

    }

    /*
     * Unsupported method.
     * 
     * @return IMetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {

        throw new UnsupportedException("Not yet supported");

    }
}
