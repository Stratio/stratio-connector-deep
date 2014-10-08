package com.stratio.connector.deep.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * Created by dgomez on 30/09/14.
 */
public class DeepMetadataEngine extends CommonsMetadataEngine {

    protected DeepMetadataEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    protected void createCatalog(CatalogMetadata catalogMetadata, Connection connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected void createTable(TableMetadata tableMetadata, Connection connection) throws UnsupportedException,
            ExecutionException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected void dropCatalog(CatalogName catalogName, Connection connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected void dropTable(TableName tableName, Connection connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected void createIndex(IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not supported");
    }
}
