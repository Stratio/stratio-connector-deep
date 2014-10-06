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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.logicalplan.UnionStep;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;

public class DeepQueryEngine implements IQueryEngine {

    private final DeepSparkContext deepContext;

    private final DeepConnectionHandler deepConnectionHandler;

    private final Map<String, JavaRDD<Cells>> partialResultsMap = new HashMap<>();

    public DeepQueryEngine(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
        this.deepContext = deepContext;
        this.deepConnectionHandler = deepConnectionHandler;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#execute(com.stratio.meta.common.logicalplan.LogicalWorkflow)
     */
    @Override
    public QueryResult execute(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {

        List<LogicalStep> initialSteps = workflow.getInitialSteps();
        JavaRDD<Cells> partialResultRdd = null;
        for (LogicalStep initialStep : initialSteps) {
            Project project = (Project) initialStep;
            ExtractorConfig<Cells> extractorConfig = retrieveConfiguration(project.getClusterName());
            JavaRDD<Cells> initialRdd = createRDD(project, extractorConfig);
            partialResultRdd = executeInitialStep(initialStep.getNextStep(), initialRdd, project.getTableName()
                    .toString());
        }

        return buildQueryResult(partialResultRdd, (Select) workflow.getLastStep());
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
    private QueryResult buildQueryResult(JavaRDD<Cells> resultRdd, Select selectStep) {

        // TODO Build the ResultSet
        List<Cells> resultCells = resultRdd.collect();

        Map<ColumnName, String> columnMap = selectStep.getColumnMap();
        Map<String, ColumnType> columnType = selectStep.getTypeMap();

        // Adding column metadata information
        List<ColumnMetadata> resultMetadata = new LinkedList<>();
        for (Entry<ColumnName, String> columnItem : columnMap.entrySet()) {

            ColumnName columnName = columnItem.getKey();
            String columnAlias = columnItem.getValue();

            ColumnMetadata columnMetadata = new ColumnMetadata(columnName.getTableName().getQualifiedName(),
                    columnName.getName());
            columnMetadata.setColumnAlias(columnAlias);
            columnMetadata.setType(columnType.get(columnAlias));
        }

        List<Row> resultRows = new LinkedList<>();
        for (Cells cells : resultCells) {
            resultRows.add(buildRowFromCells(cells, columnMap));
        }

        ResultSet resultSet = new ResultSet();
        resultSet.setRows(resultRows);
        QueryResult queryResult = QueryResult.createQueryResult(resultSet);

        return queryResult;
    }

    /**
     * @param cells
     * @return
     */
    private Row buildRowFromCells(Cells cells, Map<ColumnName, String> columnMap) {

        Row row = new Row();
        for (Entry<ColumnName, String> columnItem : columnMap.entrySet()) {
            ColumnName columnName = columnItem.getKey();

            // Retrieving the cell to create a new meta cell with its value
            com.stratio.deep.commons.entity.Cell cellsCell = cells.getCellByName(columnName.getTableName()
                    .getQualifiedName(),
                    columnName.getName());
            Cell rowCell = new Cell(cellsCell.getCellValue());

            // Adding the cell by column alias
            row.addCell(columnItem.getValue(), rowCell);
        }

        return row;
    }

    /**
     * @param projection
     * @param extractorConfig
     * @return
     */
    private JavaRDD<Cells> createRDD(Project projection, ExtractorConfig<Cells> extractorConfig) {

        // Retrieving project information
        List<String> columnsList = new ArrayList<>();
        for (ColumnName columnName : projection.getColumnList()) {
            columnsList.add(columnName.getName());
        }

        extractorConfig.putValue(ExtractorConstants.INPUT_COLUMNS, columnsList.toArray(new String[columnsList.size()]));
        extractorConfig.putValue(ExtractorConstants.TABLE, projection.getTableName().getName());
        extractorConfig.putValue(ExtractorConstants.CATALOG, projection.getCatalogName());

        JavaRDD<Cells> rdd = deepContext.createJavaRDD(extractorConfig);

        return rdd;
    }

    /**
     * @param logicalStep
     * @param rdd
     * @return
     * @throws ExecutionException
     * @throws UnsupportedException
     */
    private JavaRDD<Cells> executeInitialStep(LogicalStep logicalStep, JavaRDD<Cells> rdd, String tableName)
            throws ExecutionException, UnsupportedException {

        String stepId = tableName;
        LogicalStep currentStep = logicalStep;
        while (currentStep != null) {
            if (currentStep instanceof Filter) {
                executeFilter((Filter) currentStep, rdd);
            } else if (currentStep instanceof Select) {
                prepareResult((Select) currentStep, rdd);
            } else if (currentStep instanceof UnionStep) {
                UnionStep unionStep = (UnionStep) currentStep;
                if (!executeUnion(unionStep, rdd)) {
                    break;
                } else {
                    if (unionStep instanceof Join) {
                        stepId = ((Join) unionStep).getId();
                    } else {
                        throw new ExecutionException("Unknown union step found ["
                                + unionStep.getOperation().toString() + "]");
                    }
                }
            } else {
                throw new ExecutionException("Unexpected step found [" + currentStep.getOperation().toString() + "]");
            }

            currentStep = currentStep.getNextStep();
        }

        partialResultsMap.put(stepId, rdd);

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

        rdd = QueryFilterUtils.doJoin(leftRdd,rdd,joinRelations);
    }

    /**
     * @param selectStep
     * @param rdd
     */
    private void prepareResult(Select selectStep, JavaRDD<Cells> rdd) throws ExecutionException {

        rdd = QueryFilterUtils.filterSelectedColumns(rdd, selectStep.getColumnMap().keySet());
    }

    /**
     * @param filterStep
     * @param rdd
     * @throws UnsupportedException
     */
    private void executeFilter(Filter filterStep, JavaRDD<Cells> rdd) throws ExecutionException, UnsupportedException {
        Relation relation = filterStep.getRelation();
        if (relation.getOperator().isInGroup(Operator.Group.COMPARATOR)) {

            QueryFilterUtils.doWhere(rdd, relation);

        } else {

            throw new ExecutionException("Unknown Filter found [" + filterStep.getRelation().getOperator().toString()
                    + "]");

        }

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
