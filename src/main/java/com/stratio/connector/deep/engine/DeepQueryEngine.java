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
package com.stratio.connector.deep.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.logicalplan.UnionStep;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;

public class DeepQueryEngine implements IQueryEngine {

    private final DeepSparkContext deepContext;

    private final DeepConnectionHandler deepConnectionHandler;

    private final Map<String, JavaRDD<Cells>> partialResultsMap = new HashMap<>();

    public DeepQueryEngine(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
        this.deepContext = deepContext;
        this.deepConnectionHandler = deepConnectionHandler;
    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws UnsupportedException,
            ExecutionException {

        List<LogicalStep> initialSteps = workflow.getInitialSteps();
        JavaRDD<Cells> partialResultRdd = null;
        for (LogicalStep initialStep : initialSteps) {
            Project project = (Project) initialStep;
            ExtractorConfig<Cells> extractorConfig = retrieveConfiguration(project.getClusterName());
            JavaRDD<Cells> initialRdd = createRDD(project, extractorConfig);
            partialResultRdd = executeInitialStep(initialStep.getNextStep(), initialRdd, project.getTableName()
                    .toString());
        }

        return buildQueryResult(partialResultRdd);
    }

    /**
     * @param clusterName
     * @return
     * @throws ExecutionException
     */
    private ExtractorConfig<Cells> retrieveConfiguration(ClusterName clusterName) throws ExecutionException {

        DeepConnection deepConnection = null;
        try {
            deepConnection = (DeepConnection) deepConnectionHandler.getConnection(clusterName.getName());
        } catch (HandlerConnectionException ex) {
            throw new ExecutionException("Error retrieving the cluster connection information ["
                    + clusterName.toString() + "]", ex);
        }

        ExtractorConfig<Cells> extractorConfig = null;
        if (deepConnection != null) {
            extractorConfig = deepConnection.getExtractorConfig();
        } else {
            throw new ExecutionException("Unknown cluster [" + clusterName.toString() + "]");
        }

        return extractorConfig;
    }

    /**
     * @param partialResultRdd
     * @return
     */
    private QueryResult buildQueryResult(JavaRDD<Cells> partialResultRdd) {

        QueryResult queryResult = QueryResult.createSuccessQueryResult();
        return queryResult;
    }

    /**
     * @param projection
     * @param extractorConfig
     * @return
     */
    private JavaRDD<Cells> createRDD(Project projection, ExtractorConfig<Cells> extractorConfig) {

        DeepJobConfig<Cells> jobConfig = new DeepJobConfig<>(extractorConfig);

        // Retrieving project information
        List<String> columnsList = new ArrayList<>();
        for (ColumnName columnName : projection.getColumnList()) {
            columnsList.add(columnName.getName());
        }
        // TODO Review data in deepJobConfig / extractorConfig; not sure it's reachable
        jobConfig.inputColumns(columnsList.toArray(new String[columnsList.size()]));
        jobConfig.tableName(projection.getTableName().getName());
        jobConfig.catalogName(projection.getCatalogName());

        JavaRDD<Cells> rdd = deepContext.createJavaRDD(jobConfig);

        return rdd;
    }

    /**
     * @param logicalStep
     * @param rdd
     * @return
     * @throws ExecutionException
     */
    private JavaRDD<Cells> executeInitialStep(LogicalStep logicalStep, JavaRDD<Cells> rdd, String tableName)
            throws ExecutionException {

        LogicalStep currentStep = logicalStep;
        while (currentStep != null) {
            if (currentStep instanceof Filter) {
                executeFilter((Filter) currentStep, rdd);
            } else if (currentStep instanceof Select) {
                prepareResult((Select) currentStep, rdd);
            } else if (currentStep instanceof UnionStep) {
                if (!executeUnion((UnionStep) currentStep, rdd)) {
                    break;
                }
            } else {
                throw new ExecutionException("Unexpected step found [" + currentStep.getOperation().toString() + "]");
            }

            currentStep = currentStep.getNextStep();
        }

        partialResultsMap.put(tableName, rdd);

        return rdd;
    }

    /**
     * @param unionStep
     * @param rdd
     * @throws ExecutionException
     */
    private boolean executeUnion(UnionStep unionStep, JavaRDD<Cells> rdd) throws ExecutionException {

        boolean unionSucceed = false;
        if (unionStep instanceof Join) {
            Join joinStep = (Join) unionStep;
            String joinLeftTableName = joinStep.getSourceIdentifiers().get(0);
            JavaRDD<Cells> leftRdd = partialResultsMap.get(joinLeftTableName);
            if (leftRdd != null) {
                executeJoin(leftRdd, rdd, joinStep.getJoinRelations());
                partialResultsMap.remove(joinLeftTableName);
                partialResultsMap.put(joinStep.getId(), rdd);
                unionSucceed = true;
            }
        } else {
            throw new ExecutionException("Unknown union step found [" + unionStep.getOperation().toString() + "]");
        }

        return unionSucceed;
    }

    /**
     * @param leftRdd
     * @param rdd
     * @param joinRelations
     */
    private void executeJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rdd, List<Relation> joinRelations) {
        // TODO Auto-generated method stub
        // TODO Call to DeepUtils to apply rdd.join(leftRdd)
    }

    /**
     * @param selectStep
     * @param rdd
     */
    private void prepareResult(Select selectStep, JavaRDD<Cells> rdd) {
        // TODO Auto-generated method stub
        // TODO Call to DeepUtils to keep the requested columns and remove the rest of them
    }

    /**
     * @param filterStep
     * @param rdd
     */
    private void executeFilter(Filter filterStep, JavaRDD<Cells> rdd) {
        // TODO Auto-generated method stub
        // TODO Call to DeepUtils to apply a filter from an operator (filterStep.relation.operator)
    }
}
