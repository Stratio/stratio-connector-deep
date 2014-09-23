package com.stratio.connector.deep.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.engine.CommonsQueryEngine;
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
public class DeepStorageEngine extends CommonsQueryEngine {

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());





    public DeepStorageEngine(DeepConnectionHandler connectionHandler) {

       super(connectionHandler);

    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow, Connection connection) throws UnsupportedException, ExecutionException {


        DeepConnection deepConnection = (DeepConnection) connection;

        //Return the Extractor Config for this ClusterName
        deepConnection.getExtractorConfig();

        //Return the DeeSparkpContext
        deepConnection.getNativeConnection();

        try {
            wait(10000000000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
