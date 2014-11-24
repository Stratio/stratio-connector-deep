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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.connector.deep.configuration.ClusterProperties;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;
import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * .Connection object exist in the ConnectionHandler and contains all the connection info & config.
 *  {@link com.stratio.connector.commons.connection.Connection}
 *
 */
public class DeepConnection extends Connection {

    private final Properties configProperties;

    private final DeepSparkContext deepSparkContext;

    private boolean isConnect = false;

    private final ExtractorConfig extractorConfig;

    /**
     * Constructor using credentials and cluster config.
     * 
     * @param credentials
     *            the credentials.
     * @param config
     *            The cluster configuration.
     */
    public DeepConnection(ICredentials credentials, ConnectorClusterConfig config) {

        if (credentials != null) {
            // TODO check the credentials
        }

        Map<String, String> clusterOptions = config.getOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig<>(Cells.class);

        Map<String, Serializable> values = new HashMap<>();

        if (clusterOptions.get(ExtractorConstants.HOSTS) != null) {
            values.put(ExtractorConstants.HOSTS, clusterOptions.get(ExtractorConstants.HOSTS));
            String[] hosts = ConnectorParser.hosts(clusterOptions.get(ExtractorConstants.HOSTS));

            values.put(ExtractorConstants.HOST, hosts[0]);
        } else {
            values.put(ExtractorConstants.HOST, clusterOptions.get(ExtractorConstants.HOST));
        }

        if (clusterOptions.get(ExtractorConstants.PORTS) != null) {
            values.put(ExtractorConstants.PORTS, clusterOptions.get(ExtractorConstants.PORTS));
            String[] ports = ConnectorParser.ports(clusterOptions.get(ExtractorConstants.PORTS));

            values.put(ExtractorConstants.PORT, ports[0]);
        } else {
            values.put(ExtractorConstants.PORT, clusterOptions.get(ExtractorConstants.PORT));
        }

        if (clusterOptions.get(ExtractorConstants.HDFS_SCHEMA) != null) {
            values.put(ExtractorConstants.HDFS_SCHEMA,clusterOptions.get(ExtractorConstants.HDFS_SCHEMA));
        }

        if (clusterOptions.get(ExtractorConstants.HDFS_FILE_SEPARATOR) != null) {
            values.put(ExtractorConstants.HDFS_FILE_SEPARATOR,clusterOptions.get(ExtractorConstants.HDFS_FILE_SEPARATOR));
        }

        if (clusterOptions.get(ExtractorConstants.HDFS_FILE_PATH) != null) {
            values.put(ExtractorConstants.HDFS_FILE_PATH,clusterOptions.get(ExtractorConstants.HDFS_FILE_PATH));
        }
        extractorconfig.setValues(values);

        ClusterProperties clusterProperties = new ClusterProperties();

        // TODO Find new field add by meta to recognise the database to associate the CellExtractor config correct
        String dataBase = checkDatabaseFromClusterName(config);

        extractorconfig.setExtractorImplClassName(clusterProperties.getValue("cluster." + dataBase + "."
                + ExtractorConstants.INNERCLASS));

        extractorConfig = extractorconfig;

        configProperties = ConnectionConfiguration.getConfigProperties();

        deepSparkContext = ConnectionConfiguration.getDeepContext();

        isConnect = true;
    }

    /**
    * Change the connection status.
    *
    */
    @Override
    public void close() {
        if (deepSparkContext != null) {

            isConnect = false;
        }

    }

    /**
    * return the connection status.
     *
     * @return Boolean
    */
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

    /**
     * return the connection status.
     * @param config {@link ConnectorClusterConfig}
     * @return String
     */
    private String checkDatabaseFromClusterName(ConnectorClusterConfig config) {

        String db = "";
        if (config.getName().getName().contains(DeepConnectorConstants.DB_CASSANDRA)) {
            db = DeepConnectorConstants.DB_CASSANDRA;
        } else if (config.getName().getName().contains(DeepConnectorConstants.DB_MONGO)) {
            db = DeepConnectorConstants.DB_MONGO;
        } else if (config.getName().getName().contains(DeepConnectorConstants.DB_ELASTICSEARCH)) {
            db = DeepConnectorConstants.DB_ELASTICSEARCH;
        }else if (config.getName().getName().contains(DeepConnectorConstants.HDFS)) {
            db = DeepConnectorConstants.HDFS;
        }

        return db;
    }

    public Properties getConfigProperties() {
        return configProperties;
    }
}
