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

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Class implements native deep connection.
 * 
 */
public class DeepConnectionHandler extends ConnectionHandler {

	/**
	 * Basic constructor.
	 * 
	 * @param configuration
	 * 						The configuration
	 */
	public DeepConnectionHandler(IConfiguration configuration) {
		super(configuration);
	}

	/**
	 * Use config & Credentials to create Deep native connection.
	 * 
	 * @param iCredentials
	 *            			The credentials
	 * @param connectorClusterConfig
	 *            					The connector cluster configuration
	 * 
	 * @return DeepConnection
	 **/
	@Override
	protected Connection createNativeConnection(ICredentials iCredentials, ConnectorClusterConfig connectorClusterConfig)
			throws ConnectionException {

		Connection connection;

		connection = new DeepConnection(iCredentials, connectorClusterConfig);


		return connection;
	}

}
