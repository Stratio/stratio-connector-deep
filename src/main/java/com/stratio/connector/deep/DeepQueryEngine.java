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
package com.stratio.connector.deep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.connector.deep.data.LogicalQuery;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.UnionStep;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;


public class DeepQueryEngine implements IQueryEngine {

  private final static int MAX_THREADS_ALLOWED_CONSTANT = 16;

  private ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS_ALLOWED_CONSTANT);

  private DeepSparkContext deepContext;

  private DeepConnectionHandler deepConnectionHandler;

  private Map<String, LogicalQuery> partialResultsMap = new HashMap<>();

  public DeepQueryEngine(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
    this.deepContext = deepContext;
    this.deepConnectionHandler = deepConnectionHandler;
  }

  @Override
  public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow)
      throws UnsupportedException, ExecutionException {

    Iterator<LogicalStep> logicalSteps = workflow.getInitialSteps().iterator();
    while (logicalSteps.hasNext()) {
      executeStep(targetCluster.getName(), logicalSteps.next());
    }

    // Future<LinkedList<HasNextElement<T>>> future = executor.submit(new Callable() {
    // public Object call() throws Exception {
    // channel.writeAndFlush(new HasNextAction<>());
    // return ((HasNextResponse) answer.take()).getHasNextElementsPage();
    // }
    // });

    // ss.addTablenameToIds();
    //
    // // LEAF
    // String[] columnsSet = {};
    // if (ss.getSelectionClause().getType() == SelectionClause.TYPE_SELECTION) {
    // columnsSet = DeepUtils.retrieveSelectorFields(ss);
    // }
    // ICassandraDeepJobConfig<Cells> config =
    // CassandraConfigFactory.create().session(session).host(engineConfig.getRandomCassandraHost())
    // .rpcPort(engineConfig.getCassandraPort()).keyspace(ss.getEffectiveKeyspace())
    // .table(ss.getTableName());
    //
    // config =
    // (columnsSet.length == 0) ? config.initialize() : config.inputColumns(columnsSet)
    // .initialize();

    // JavaRDD<Cells> rdd = deepContext.createJavaRDD(config);

    return null;
  }

  private void executeStep(String clusterName, LogicalStep logicalStep) throws ExecutionException {

    Connection<Cells> deepConnection = null;
    try {
      deepConnection = deepConnectionHandler.getConnection(clusterName);
    } catch (HandlerConnectionException ex) {
      throw new ExecutionException(ex);
    }
    LogicalQuery logicalQuery = mapLogicalStepToLogicalQuery(logicalStep);

    // TODO Retrieve the extractor configuration from a global map or whatever
    ExtractorConfig<Cells> config = deepConnection.getNativeConnection();

    final DeepJobConfig<Cells> jobConfig = new DeepJobConfig<>(config);
    jobConfig.inputColumns((String[]) logicalQuery.getProjection().getColumnList().toArray());
    jobConfig.tableName(logicalQuery.getProjection().getTableName().getName());
    jobConfig.catalogName(logicalQuery.getProjection().getCatalogName());

    Future<RDD<Cells>> future = executor.submit(new Callable<RDD<Cells>>() {
      public RDD<Cells> call() throws Exception {
        JavaRDD<Cells> rdd = deepContext.createJavaRDD(jobConfig);
        return null;
      }
    });

    logicalQuery.setRdd(future);
  }

  private LogicalQuery mapLogicalStepToLogicalQuery(LogicalStep logicalStep)
      throws ExecutionException {

    Project projection = (Project) logicalStep;

    // Retrieving filters
    List<Filter> filters = new ArrayList<>();
    LogicalStep currentStep = logicalStep.getNextStep();
    while ((currentStep != null) && (currentStep instanceof Filter)) {
      filters.add((Filter) currentStep);

      currentStep = currentStep.getNextStep();
    }

    // Checking we have not found an unexpected logical step. Expected steps are Filter and
    // UnionStep
    if ((currentStep != null) && !(currentStep instanceof UnionStep)) {
      throw new ExecutionException("Unexpected logical step [" + logicalStep.getOperation() + "]");
    }

    return new LogicalQuery(projection, filters);
  }
}
