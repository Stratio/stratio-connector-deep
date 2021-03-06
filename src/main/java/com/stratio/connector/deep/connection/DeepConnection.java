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
import java.util.Map.Entry;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

/**
 * .Connection object exist in the ConnectionHandler and contains all the connection info & config.
 * {@link com.stratio.connector.commons.connection.Connection}
 *
 */
public class DeepConnection extends Connection<Object> {

	private boolean isConnect = false;

	private final ExtractorConfig<Cells> extractorConfig;

	/**
	 * Constructor using credentials and cluster config.
	 *
	 * @param credentials
	 *            the credentials.
	 * @param config
	 *            The cluster configuration.
	 */
	public DeepConnection(ICredentials credentials, ConnectorClusterConfig config)
			throws ConnectionException {

		if (credentials != null) {
			// TODO check the credentials
		}

		Map<String, String> clusterOptions = config.getClusterOptions();
		Map<String, String> connectorOptions = config.getConnectorOptions();
		// Creating a configuration for the Extractor and initialize it
		ExtractorConfig<Cells> extractorconfig = new ExtractorConfig<>(Cells.class);
		String extractorImplClassName = clusterOptions.get(DeepConnectorConstants.EXTRACTOR_IMPL_CLASS);

		if (extractorImplClassName == null) {
			throw new ConnectionException("Unknown data source, please add it to the configuration.");
		}
		Map<String,Serializable> values = returnConfig(clusterOptions);
		values.put(DeepConnectorConstants.PROPERTY_DEFAULT_LIMIT,connectorOptions.get(DeepConnectorConstants
				.PROPERTY_DEFAULT_LIMIT));
		extractorconfig.setValues(values);

		extractorconfig.setExtractorImplClassName(extractorImplClassName);

		this.extractorConfig = extractorconfig;

		this.isConnect = true;
	}

	private Map<String,Serializable> returnConfig(Map<String, String> clusterOptions) {

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


		for (Entry<String, String> entry : clusterOptions.entrySet()){
			Serializable val = entry.getValue();
			if (entry.getKey().equals(ExtractorConstants.HOSTS) ||entry.getKey().equals(ExtractorConstants.HOST) || entry.getKey().equals
					(ExtractorConstants.PORTS) || entry.getKey().equals(ExtractorConstants.ES_REST_PORTS)){
				String formatArray = entry.getValue().replaceAll("\\s+", "").replaceAll("\\[",
				                                                                            "").replaceAll("]", "");
				values.put(entry.getKey(),formatArray);
			}else{
				values.put(entry.getKey(),val);
			}

		}

		return values;

	}

	/**
	 * Change the connection status.
	 *
	 */
	@Override
	public void close() {
		isConnect = false;
	}

	/**
	 * return the connection status.
	 *
	 * @return Boolean
	 */
	@Override
	public boolean isConnected() {

		return isConnect;
	}

	@Override
	public Object getNativeConnection() {
		return null;
	}

	public ExtractorConfig<Cells> getExtractorConfig() {
		return extractorConfig;
	}
}
