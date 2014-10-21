package com.stratio.connector.deep.engine;

import java.util.Collection;

import com.stratio.connector.commons.connection.Connection;

import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.TableMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dgomez on 18/09/14.
 */
public class DeepStorageEngine extends CommonsStorageEngine {

    /**
     * The log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public DeepStorageEngine(DeepConnectionHandler connectionHandler) {

        super(connectionHandler);

    }

    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow, Connection connection)
            throws UnsupportedException {

        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void insert(TableMetadata tableMetadata, Row row, Connection connection) throws UnsupportedException
             {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }

    @Override
    protected void insert(TableMetadata tableMetadata, Collection collection, Connection connection)
            throws UnsupportedException {
        throw new UnsupportedException(ExtractorConnectConstants.METHOD_NOT_SUPPORTED);
    }
}
