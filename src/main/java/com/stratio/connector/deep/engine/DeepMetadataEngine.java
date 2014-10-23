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

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 *
 *  Class use for admin table meta Data, not implemented for Deep connector.
 *  {@link com.stratio.connector.commons.engine.CommonsMetadataEngine}
 *
 *  @throw UnsupportedException
 */
public class DeepMetadataEngine extends CommonsMetadataEngine {

    protected DeepMetadataEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    protected void createCatalog(CatalogMetadata catalogMetadata, Connection connection) throws UnsupportedException
    {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void createTable(TableMetadata tableMetadata, Connection connection) throws UnsupportedException
    {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void dropCatalog(CatalogName catalogName, Connection connection) throws UnsupportedException
    {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void dropTable(TableName tableName, Connection connection)
            throws UnsupportedException {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void createIndex(IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }
}
