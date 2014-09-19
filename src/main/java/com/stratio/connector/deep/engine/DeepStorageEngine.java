package com.stratio.connector.deep.engine;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dgomez on 18/09/14.
 */
public class DeepStorageEngine implements IQueryEngine{

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());


    /**
     * The connection handler.
     */
    private DeepConnectionHandler connectionHandler;


    public DeepStorageEngine(DeepConnectionHandler connectionHandler) {

        this.connectionHandler = connectionHandler;

    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {

        try {
            DeepConnection connection = (DeepConnection) connectionHandler.getConnection(targetCluster.getName());

            //Return the Extractor Config for this ClusterName
            connection.getExtractorConfig();

            //Return the DeeSparkpContext
            connection.getNativeConnection();

        } catch (HandlerConnectionException e) {
            e.printStackTrace();
        }


        return null;
    }
}
