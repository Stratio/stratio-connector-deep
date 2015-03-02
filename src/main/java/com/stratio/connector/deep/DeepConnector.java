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

package com.stratio.connector.deep;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.ISqlEngine;
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

/**
 * Class that implements Crossdata Interface to connect. {@link com.stratio.crossdata.common.connector.IConnector}.
 * 
 */
public class DeepConnector implements IConnector {

	private static transient final Logger LOGGER = LoggerFactory.getLogger(DeepConnector.class);


	/**
	 * The connectionHandler.
	 */
	private DeepConnectionHandler connectionHandler;

	/**
	 * The deepContext.
	 */
	private DeepSparkContext deepContext;

    /**
     * The DeepConfigurationBuilder.
     */
    private DeepConfigurationBuilder deepConfigurationBuilder;

	/**
	 * Main uses to associate the connector to Crossdata.
	 * 
	 * @param args
	 * 				Args
	 * @throws InitializationException
	 */
	public static void main(String[] args)  throws InitializationException{

		DeepConnector deepConnector = new DeepConnector();

		ConnectorApp connectorApp = new ConnectorApp();
		connectorApp.startup(deepConnector);
		deepConnector.attachShutDownHook();
	}

	/**
	 * Run the shutdown.
	 */
	public void attachShutDownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					shutdown();
				} catch (ExecutionException e) {
					LOGGER.error("Fail ShutDown", e);
				}
			}
		});
	}

	/**
	 * Basic constructor.
	 * 
	 * @throws InitializationException
	 */
	public DeepConnector() throws InitializationException {

        deepConfigurationBuilder = new DeepConfigurationBuilder();

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
     *
	 * Init Connection.
	 * 
	 * @param configuration
	 * @see{com.stratio.connector.deep.configuration.ConnectionConfiguration.
	 */
	@Override
	public void init(IConfiguration configuration) throws InitializationException {

		this.connectionHandler = new DeepConnectionHandler(null);

		LOGGER.info("-------------StartUp the SparkContext------------ ");


        Long time = System.currentTimeMillis();

        this.deepContext = new DeepSparkContext(deepConfigurationBuilder.createDeepSparkConf());

		LOGGER.info("-------------End StartUp the SparkContext in ["+(System.currentTimeMillis()-time)+" ms] "
                + "------------ ");
	}





    /**
	 * Connect with the config expecified associate to a clusterName {ConnectionHandler}
	 * {@link com.stratio.connector.deep.connection.DeepConnectionHandler.createNativeConnection}.
	 * 
	 * @param credentials
	 * @param config
	 *            {@link com.stratio.crossdata.common.connector.ConnectorClusterConfig}
	 */
	@Override
	public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

		String extractorImplClassName = deepConfigurationBuilder.getImplementationClass(config.getDataStoreName().getName());

		config.getClusterOptions().put(DeepConnectorConstants.EXTRACTOR_IMPL_CLASS, extractorImplClassName);

        if (extractorImplClassName != null && extractorImplClassName.equals(ExtractorConstants.HDFS)) {
			config.getClusterOptions().put(
					ExtractorConstants.FS_FILE_PATH,
                    deepConfigurationBuilder.getHDFSFilePath());
		}

		connectionHandler.createConnection(credentials, config);

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

		connectionHandler.closeAllConnections();
		deepContext.stop();
	}

	/**
	 * Check if the connection associate to the clusterName is connected
	 * {@link com.stratio.connector.commons.connection.ConnectionHandler.isConnected}.
	 * 
	 * @param name
	 *            {@link com.stratio.crossdata.common.data.ClusterName}
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

		throw new UnsupportedException("getStorageEngine not yet supported");

	}

	/**
	 * Return the interface to invoke queries from crossdata.
	 * 
	 * @return DeepQueryEngine
	 */
	@Override
	public IQueryEngine getQueryEngine() throws UnsupportedException {

		return new DeepQueryEngine(deepContext, connectionHandler);

	}

	/**
	 * Unsupported method.
	 * 
	 * @return IMetadataEngine
	 */
	@Override
	public IMetadataEngine getMetadataEngine() throws UnsupportedException {

		throw new UnsupportedException("getMetadataEngine not yet supported");

	}
    /**
     * Unsupported method.
     *
     * @return IMetadataEngine
     */
    @Override public ISqlEngine getSqlEngine() throws UnsupportedException {
        throw new UnsupportedException("getSqlEngine not yet supported");
    }

}
