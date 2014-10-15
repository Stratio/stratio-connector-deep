/*
 * Licensed to STRATIO (C) under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership. The STRATIO
 * (C) licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.stratio.connector.deep.engine.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.connector.commons.engine.CommonsQueryEngine;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;

public class DeepQueryEngine extends CommonsQueryEngine {

    private final DeepSparkContext deepContext;

    private final DeepConnectionHandler deepConnectionHandler;

    private final Map<String, JavaRDD<Cells>> partialResultsMap = new HashMap<>();

    public DeepQueryEngine(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
        super(deepConnectionHandler);
        this.deepContext = deepContext;
        this.deepConnectionHandler = deepConnectionHandler;
    }

    @Override
    @Deprecated
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws UnsupportedException,
            ExecutionException {

        return execute(workflow);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#execute(com.stratio.meta.common.logicalplan.LogicalWorkflow)
     */
    @Override
    public QueryResult executeWorkFlow(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {

        QueryExecutor executor = new QueryExecutor(deepContext, deepConnectionHandler);
        return executor.executeWorkFlow(workflow);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#asyncExecute(java.lang.String,
     * com.stratio.meta.common.logicalplan.LogicalWorkflow, com.stratio.meta.common.connector.IResultHandler)
     */
    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws UnsupportedException, ExecutionException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#stop(java.lang.String)
     */
    @Override
    public void stop(String queryId) throws UnsupportedException, ExecutionException {
        // TODO Auto-generated method stub

    }
}
