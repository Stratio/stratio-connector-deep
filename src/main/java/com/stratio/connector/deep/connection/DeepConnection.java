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
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * .Connection object exist in the ConnectionHandler and contains all the connection info & config.
 * {@link com.stratio.connector.commons.connection.Connection}
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
    public DeepConnection(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        if (credentials != null) {
            // TODO check the credentials
        }

        Map<String, String> clusterOptions = config.getClusterOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig<>(Cells.class);

        Map<String, Serializable> values = new HashMap<>();

        if (clusterOptions.get(ExtractorConnectConstants.HOSTS) != null) {
            values.put(ExtractorConnectConstants.HOSTS, clusterOptions.get(ExtractorConnectConstants.HOSTS));
            String[] hosts = ConnectorParser.hosts(clusterOptions.get(ExtractorConnectConstants.HOSTS));

            values.put(ExtractorConnectConstants.HOST, hosts[0]);
        } else {
            values.put(ExtractorConnectConstants.HOST, clusterOptions.get(ExtractorConnectConstants.HOST));
        }

        if (clusterOptions.get(ExtractorConnectConstants.PORTS) != null) {
            values.put(ExtractorConnectConstants.PORTS, clusterOptions.get(ExtractorConnectConstants.PORTS));
            String[] ports = ConnectorParser.ports(clusterOptions.get(ExtractorConnectConstants.PORTS));

            values.put(ExtractorConnectConstants.PORT, ports[0]);
        } else {
            values.put(ExtractorConnectConstants.PORT, clusterOptions.get(ExtractorConnectConstants.PORT));
        }

        extractorconfig.setValues(values);

        ClusterProperties clusterProperties = new ClusterProperties();

        String dataBaseName = config.getDataStoreName().getName();
        String extractorImplClassName = clusterProperties.getValue("cluster." + dataBaseName + "."
                + ExtractorConnectConstants.INNERCLASS);

        if (extractorImplClassName == null) {
            throw new ConnectionException("Unknown data source, please add it to the configuration.");
        }

        extractorconfig.setExtractorImplClassName(extractorImplClassName);

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

    public Properties getConfigProperties() {
        return configProperties;
    }
}
