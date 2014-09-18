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


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import com.stratio.connector.deep.data.LogicalQuery;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;


public class DeepQueryEngine implements IQueryEngine {

  private DeepSparkContext deepContext;
  
  private Map<Project, Future<RDD<Cells>>> partialRddsMap = new HashMap<>();
  
  public DeepQueryEngine(DeepSparkContext deepContext) {
    this.deepContext = deepContext;
  }
  
  @Override
  public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow)
      throws UnsupportedException, ExecutionException {

    //TODO Retrieve the extractor configuration from a global map or whatever
    ExtractorConfig<Cells> config = new ExtractorConfig<>();
    
    Iterator<LogicalStep> logicalSteps = workflow.getInitialSteps().iterator();
    while (logicalSteps.hasNext()) {
      executeStep(logicalSteps.next());
    }
    
//    Future<LinkedList<HasNextElement<T>>> future = executor.submit(new Callable() {
//      public Object call() throws Exception {
//        channel.writeAndFlush(new HasNextAction<>());
//        return ((HasNextResponse) answer.take()).getHasNextElementsPage();
//      }
//    });
    
//    ss.addTablenameToIds();
//
//    // LEAF
//    String[] columnsSet = {};
//    if (ss.getSelectionClause().getType() == SelectionClause.TYPE_SELECTION) {
//      columnsSet = DeepUtils.retrieveSelectorFields(ss);
//    }
//    ICassandraDeepJobConfig<Cells> config =
//            CassandraConfigFactory.create().session(session).host(engineConfig.getRandomCassandraHost())
//            .rpcPort(engineConfig.getCassandraPort()).keyspace(ss.getEffectiveKeyspace())
//            .table(ss.getTableName());
//
//    config =
//        (columnsSet.length == 0) ? config.initialize() : config.inputColumns(columnsSet)
//            .initialize();

    JavaRDD<Cells> rdd = deepContext.createJavaRDD(config);
    
    return null;
  }

  private void executeStep(LogicalStep logicalStep) {
    
    
  }
  
  private List<LogicalQuery> mapLogicalStepsToLogicalQueries(List<LogicalStep> logicalSteps) {
    
    for (LogicalStep logicalStep : logicalSteps) {
      if (logicalStep isInstanceOf)
    }
  }
}
