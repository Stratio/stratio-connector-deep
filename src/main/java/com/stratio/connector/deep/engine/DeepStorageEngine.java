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

package com.stratio.connector.deep.engine;

import java.util.Collection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Class use for insert Data table, not implemented for Deep connector
 * {@link com.stratio.connector.commons.engine.CommonsMetadataEngine}.
 * 
 * @throw UnsupportedException
 */
public class DeepStorageEngine extends CommonsStorageEngine {

    public DeepStorageEngine(DeepConnectionHandler connectionHandler) {

        super(connectionHandler);

    }

    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow, Connection connection)
            throws UnsupportedException {

        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }


    @Override
    public void truncate(ClusterName targetCluster, TableName tableName) throws UnsupportedException {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.connector.commons.engine.CommonsStorageEngine#truncate(com.stratio.crossdata.common.data.TableName,
     * com.stratio.connector.commons.connection.Connection)
     */
    @Override
    protected void truncate(TableName tableName, Connection connection) throws UnsupportedException, ExecutionException {

        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.connector.commons.engine.CommonsStorageEngine#delete(com.stratio.crossdata.common.data.TableName,
     * java.util.Collection, com.stratio.connector.commons.connection.Connection)
     */

    protected void delete(TableName tableName, Collection whereClauses, Connection connection)
            throws UnsupportedException, ExecutionException {

        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.connector.commons.engine.CommonsStorageEngine#update(com.stratio.crossdata.common.data.TableName,
     * java.util.Collection, java.util.Collection, com.stratio.connector.commons.connection.Connection)
     */
    @Override
    protected void update(TableName tableName, Collection assignments, Collection whereClauses, Connection connection)
            throws UnsupportedException, ExecutionException {

        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }


    public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row, boolean isNotExists)
            throws ConnectorException {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

    public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows,
            boolean isNotExists) throws ConnectorException {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void insert(TableMetadata tableMetadata, Row row, Connection connection) throws UnsupportedException
    {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void insert(TableMetadata tableMetadata, Collection collection, Connection connection)
            throws UnsupportedException {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);
    }

}
