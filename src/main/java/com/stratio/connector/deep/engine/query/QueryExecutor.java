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

public class QueryExecutor {

    private final DeepSparkContext deepContext;

    private final DeepConnectionHandler deepConnectionHandler;

    private final Map<String, JavaRDD<Cells>> partialResultsMap = new HashMap<>();

    public QueryExecutor(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
        this.deepContext = deepContext;
        this.deepConnectionHandler = deepConnectionHandler;
    }

    public QueryResult executeWorkFlow(LogicalWorkflow workflow) throws UnsupportedException, ExecutionException {

        List<LogicalStep> initialSteps = workflow.getInitialSteps();
        JavaRDD<Cells> partialResultRdd = null;
        for (LogicalStep initialStep : initialSteps) {
            Project project = (Project) initialStep;
            ExtractorConfig<Cells> extractorConfig = retrieveConfiguration(project.getClusterName());


            partialResultRdd = executeInitialStep((Project) initialStep, extractorConfig);
        }

        return buildQueryResult(partialResultRdd, (Select) workflow.getLastStep());
    }

    /**
     * @param project
     * @param extractorConfig
     * @return
     * @throws ExecutionException
     * @throws UnsupportedException
     */
    private JavaRDD<Cells> executeInitialStep(Project project, ExtractorConfig<Cells> extractorConfig)
                    throws ExecutionException, UnsupportedException {
        // Retrieving project information
        List<String> columnsList = new ArrayList<>();
        for (ColumnName columnName : project.getColumnList()) {
            columnsList.add(columnName.getName());
        }

        List<Filter> indexFilters = new ArrayList<Filter>();
        List<Filter> nonIndexFilters = new ArrayList<Filter>();
        LogicalStep nextStep = project.getNextStep();
        while (nextStep instanceof Filter) {
            switch (nextStep.getOperation()) {
            case FILTER_INDEXED_EQ:
            case FILTER_INDEXED_DISTINCT:
            case FILTER_INDEXED_GET:
            case FILTER_INDEXED_GT:
            case FILTER_INDEXED_LET:
            case FILTER_INDEXED_LT:
                indexFilters.add((Filter) nextStep);
                break;
            case FILTER_PK_EQ:
            case FILTER_PK_DISTINCT:
            case FILTER_PK_GET:
            case FILTER_PK_GT:
            case FILTER_PK_LET:
            case FILTER_PK_LT:
            case FILTER_NON_INDEXED_EQ:
            case FILTER_NON_INDEXED_DISTINCT:
            case FILTER_NON_INDEXED_GET:
            case FILTER_NON_INDEXED_GT:
            case FILTER_NON_INDEXED_LET:
            case FILTER_NON_INDEXED_LT:
                nonIndexFilters.add((Filter) nextStep);
                break;
            default:
                throw new ExecutionException("Unexpected filter type [" + nextStep.getOperation().toString() + "]");
            }

            nextStep = nextStep.getNextStep();
        }

        JavaRDD<Cells> initialRdd = createRdd(project, extractorConfig, indexFilters);

        JavaRDD<Cells> filteredRdd = initialRdd;
        filteredRdd.collect();
        for (Filter filter : nonIndexFilters) {
            filteredRdd = executeFilter(filter, filteredRdd);
        }

        return executeNextStep(nextStep, filteredRdd, project.getTableName().getQualifiedName());
    }

    private JavaRDD<Cells> createRdd(Project project, ExtractorConfig<Cells> extractorConfig, List<Filter> filtersList)
                    throws ExecutionException {


        // Retrieving project information
        List<String> columnsList = new ArrayList<>();
        for (ColumnName columnName : project.getColumnList()) {
            columnsList.add(columnName.getName());
        }

        extractorConfig.putValue(ExtractorConstants.INPUT_COLUMNS, columnsList.toArray(new String[columnsList.size()]));
        extractorConfig.putValue(ExtractorConstants.TABLE, project.getTableName().getName());
        extractorConfig.putValue(ExtractorConstants.CATALOG, project.getCatalogName());


        extractorConfig.putValue(ExtractorConstants.FILTER_QUERY, generateFilters(filtersList));



        JavaRDD<Cells> rdd = deepContext.createJavaRDD(extractorConfig);

        rdd.collect();
        return rdd;
    }

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
     * @param filtersList
     * @return
     * @throws ExecutionException
     */
    private com.stratio.deep.commons.filter.Filter[] generateFilters(List<Filter> filtersList)
                    throws ExecutionException {

        List<com.stratio.deep.commons.filter.Filter> resultList = new ArrayList<>();
        for (Filter filter : filtersList) {
            resultList.add(transformFilter(filter));
        }

        com.stratio.deep.commons.filter.Filter[] resultArray = new com.stratio.deep.commons.filter.Filter[resultList
                        .size()];
        return resultList.toArray(resultArray);
    }

    /**
     * @param filter
     * @return
     * @throws ExecutionException
     */
    private com.stratio.deep.commons.filter.Filter transformFilter(Filter filter) throws ExecutionException {

        ColumnSelector column = (ColumnSelector) filter.getRelation().getLeftTerm();

        return new com.stratio.deep.commons.filter.Filter(column.getName().getName(),
                        QueryFilterUtils.retrieveFilterOperator(filter.getRelation().getOperator()),
                        QueryFilterUtils.filterFromRightTermWhereRelation(filter.getRelation()));
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
                            columnName.getQualifiedName());
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
     * @param logicalStep
     * @param rdd
     * @return
     * @throws ExecutionException
     * @throws UnsupportedException
     */
    private JavaRDD<Cells> executeNextStep(LogicalStep logicalStep, JavaRDD<Cells> rdd, String tableName)
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

                PartialResults partialResults = QueryPartialResultsUtils.getPartialResult(joinStep);
                JavaRDD<Cells> partialResultsRdd = QueryPartialResultsUtils.createRDDFromResultSet(deepContext,
                                partialResults.getResults());
                List<Relation> relations = QueryPartialResultsUtils.getOrderedRelations(partialResults,
                                joinStep.getJoinRelations());
                List<Cells> collect = partialResultsRdd.collect();
                List<Cells> collect1 = rdd.collect();
                joinedRdd = executeJoin(partialResultsRdd, rdd, relations);

            } else {

                String joinLeftTableName = joinStep.getSourceIdentifiers().get(0);
                JavaRDD<Cells> leftRdd = partialResultsMap.get(joinLeftTableName);
                if (leftRdd != null) {
                    joinedRdd = executeJoin(leftRdd, rdd, joinStep.getJoinRelations());
                    partialResultsMap.remove(joinLeftTableName);
                    // partialResultsMap.put(joinStep.getId(), rdd);
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
}
