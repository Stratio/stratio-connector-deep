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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.engine.CommonsQueryEngine;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.connector.deep.engine.query.utils.PartialResultsUtils;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.PartialResults;
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
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;

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
            extractorConfig = deepConnection.getExtractorConfig().clone();
        } else {
            throw new ExecutionException("Unknown cluster [" + clusterName.toString() + "]");
        }

        return extractorConfig;
    }

    /**
     * @param resultRdd
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
            // TODO Check if we have to get the alias or the column qualified name
            // columnMetadata.setType(columnType.get(columnAlias));
            columnMetadata.setType(columnType.get(columnName.getQualifiedName()));

            resultMetadata.add(columnMetadata);
        }

        List<Row> resultRows = new LinkedList<>();
        for (Cells cells : resultCells) {
            resultRows.add(buildRowFromCells(cells, columnMap));
        }

        ResultSet resultSet = new ResultSet();
        resultSet.setRows(resultRows);
        resultSet.setColumnMetadata(resultMetadata);
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
                            .getQualifiedName(), columnName.getName());
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
                rdd = executeFilter((Filter) currentStep, rdd);

            } else if (currentStep instanceof Select) {
                rdd = prepareResult((Select) currentStep, rdd);
            } else if (currentStep instanceof UnionStep) {
                UnionStep unionStep = (UnionStep) currentStep;
                JavaRDD<Cells> joinedRdd = executeUnion(unionStep, rdd);
                if (joinedRdd == null) {
                    break;
                } else {
                    rdd = joinedRdd;
                    if (unionStep instanceof Join) {
                        stepId = ((Join) unionStep).getId();
                    } else {
                        throw new ExecutionException("Unknown union step found [" + unionStep.getOperation().toString()
                                        + "]");
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
     * @throws UnsupportedException
     */
    private JavaRDD<Cells> executeUnion(UnionStep unionStep, JavaRDD<Cells> rdd) throws ExecutionException,
                    UnsupportedException {

        JavaRDD<Cells> joinedRdd = null;
        if (unionStep instanceof Join) {
            Join joinStep = (Join) unionStep;

            if (joinStep.getOperation().equals(Operations.SELECT_INNER_JOIN_PARTIALS_RESULTS)) {
                Iterator<LogicalStep> iterator = joinStep.getPreviousSteps().iterator();
                PartialResults partialResults = null;
                while (iterator.hasNext() && partialResults == null) {
                    LogicalStep lStep = iterator.next();
                    if (lStep instanceof PartialResults) {
                        partialResults = (PartialResults) lStep;
                    }
                }
                if (partialResults == null)
                    throw new UnsupportedException(
                                    "Missing logical step \"partialResults\" in a join with partial results");

                JavaRDD<Cells> leftRdd = PartialResultsUtils.createRDDFromResultSet(deepContext,
                                partialResults.getResults());

                // TODO do an executeJoin where left and right selectar order is checked
                joinedRdd = executeUnorderedJoin(leftRdd, rdd, joinStep.getJoinRelations(), partialResults.getResults()
                                .getColumnMetadata().get(0).getTableName());
            } else {
                String joinLeftTableName = joinStep.getSourceIdentifiers().get(0);
                JavaRDD<Cells> leftRdd = partialResultsMap.get(joinLeftTableName);
                if (leftRdd != null) {
                    joinedRdd = executeJoin(leftRdd, rdd, joinStep.getJoinRelations());
                    partialResultsMap.remove(joinLeftTableName);
                    partialResultsMap.put(joinStep.getId(), rdd);
                }
            }
        } else {
            throw new ExecutionException("Unknown union step found [" + unionStep.getOperation().toString() + "]");
        }

        return joinedRdd;
    }

    /**
     * @param leftRdd
     * @param rdd
     * @param joinRelations
     */
    private JavaRDD<Cells> executeJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rdd, List<Relation> joinRelations) {

        return QueryFilterUtils.doJoin(leftRdd, rdd, joinRelations);
    }

    /**
     * @param leftRdd
     * @param rdd
     * @param joinRelations
     */
    private JavaRDD<Cells> executeUnorderedJoin(JavaRDD<Cells> partialResultRDD, JavaRDD<Cells> rdd,
                    List<Relation> joinRelations, String partialResultsQuilifiedName) {

        List<Relation> orderedRelations = new ArrayList<Relation>();
        for (Relation relation : joinRelations) {
            ColumnSelector colSelector = (ColumnSelector) relation.getLeftTerm();
            if (colSelector.getName().getTableName().getQualifiedName().equals(partialResultsQuilifiedName)) {
                orderedRelations.add(relation);
            } else {
                orderedRelations.add(new Relation(relation.getRightTerm(), relation.getOperator(), relation
                                .getLeftTerm()));
            }
        }

        return executeJoin(partialResultRDD, rdd, orderedRelations);

    }

    /**
     * @param selectStep
     * @param rdd
     */
    private JavaRDD<Cells> prepareResult(Select selectStep, JavaRDD<Cells> rdd) throws ExecutionException {

        return QueryFilterUtils.filterSelectedColumns(rdd, selectStep.getColumnMap().keySet());

    }

    /**
     * @param filterStep
     * @param rdd
     * @throws UnsupportedException
     */

    private JavaRDD<Cells> executeFilter(Filter filterStep, JavaRDD<Cells> rdd) throws ExecutionException,
                    UnsupportedException {

        Relation relation = filterStep.getRelation();
        if (relation.getOperator().isInGroup(Operator.Group.COMPARATOR)) {

            rdd = QueryFilterUtils.doWhere(rdd, relation);

        } else {

            throw new ExecutionException("Unknown Filter found [" + filterStep.getRelation().getOperator().toString()
                            + "]");

        }

        return rdd;

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
