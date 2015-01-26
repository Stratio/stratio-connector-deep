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

package com.stratio.connector.deep.engine.query;

import com.stratio.connector.commons.engine.CommonsQueryEngine;
import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * 
 * Subclass that extends CommonsQueryEngine.
 *
 */
public class DeepQueryEngine extends CommonsQueryEngine {

    private final DeepSparkContext deepContext;

    private final DeepConnectionHandler deepConnectionHandler;

    /**
     * Basic constructor.
     * 
     * @param deepContext
     * 						The Deep Context
     * @param deepConnectionHandler
     * 								The connection handler
     */
    public DeepQueryEngine(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
        super(deepConnectionHandler);
        this.deepContext = deepContext;
        this.deepConnectionHandler = deepConnectionHandler;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.crossdata.common.connector.IQueryEngine#execute(com.stratio.crossdata.common.logicalplan.LogicalWorkflow
     * )
     */
    @Override
    public QueryResult executeWorkFlow(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {
        QueryExecutor executor = new QueryExecutor(deepContext, deepConnectionHandler);
        return executor.executeWorkFlow(workflow);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.crossdata.common.connector.IQueryEngine#asyncExecute(java.lang.String,
     * com.stratio.crossdata.common.logicalplan.LogicalWorkflow, com.stratio.crossdata.common.connector.IResultHandler)
     */
    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.crossdata.common.connector.IQueryEngine#stop(java.lang.String)
     */
    @Override
    public void stop(String queryId) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException(DeepConnectorConstants.METHOD_NOT_SUPPORTED);

    }
}
