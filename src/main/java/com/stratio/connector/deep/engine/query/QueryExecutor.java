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
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.GroupBy;
import com.stratio.crossdata.common.logicalplan.Join;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.PartialResults;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.logicalplan.UnionStep;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

public class QueryExecutor {

    private final DeepSparkContext deepContext;

    private final DeepConnectionHandler deepConnectionHandler;

    private final Map<String, JavaRDD<Cells>> partialResultsMap = new HashMap<>();

    private List<Filter> indexFilters;

    private List<Filter> nonIndexFilters;

    public QueryExecutor(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
        this.deepContext = deepContext;
        this.deepConnectionHandler = deepConnectionHandler;
    }

    /**
     * Execute a workflow to retrieve a subset of data.
     * 
     * @param workflow
     *            The {@link com.stratio.crossdata.common.logicalplan.LogicalWorkflow} that contains the
     *            {@link com.stratio.crossdata.common.logicalplan.LogicalStep} to be executed.
     * @return A {@link com.stratio.crossdata.common.result.QueryResult}.
     * @throws UnsupportedException
     *             If the required set of operations are not supported by the connector.
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     */
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
     * Executes an initial step returning a partial query result if there are more steps waiting to be executed.
     * Otherwise, it returns the final result.
     * 
     * @param project
     *            Initial step
     * @param extractorConfig
     *            Query job configuration
     * 
     * @return A {@link JavaRDD}. It can be a partial result if more steps are waiting to be executed, otherwise, a
     *         final one.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     * @throws UnsupportedException
     *             If the required set of operations are not supported by the connector.
     */
    private JavaRDD<Cells> executeInitialStep(Project project, ExtractorConfig<Cells> extractorConfig)
            throws ExecutionException, UnsupportedException {

        LogicalStep nextStep = project.getNextStep();

        // Filters arrangement determining whether they are executed by the data store or by deep
        nextStep = arrangeQueryFilters(nextStep);

        JavaRDD<Cells> initialRdd = createRdd(project, extractorConfig, indexFilters);

        JavaRDD<Cells> filteredRdd = initialRdd;

        for (Filter filter : nonIndexFilters) {
            filteredRdd = executeFilter(filter, filteredRdd);
        }

        return executeNextStep(nextStep, filteredRdd, project.getTableName().getQualifiedName());
    }

    /**
     * Sets the filters fields depending on whether they are executed by the data source or by deep.
     * 
     * @param nextStep
     *            Next {@link LogicalStep} to the project.
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     */
    private LogicalStep arrangeQueryFilters(LogicalStep step) throws ExecutionException {

        this.indexFilters = new ArrayList<Filter>();
        this.nonIndexFilters = new ArrayList<Filter>();

        LogicalStep nextStep = step;

        while (nextStep instanceof Filter) {
            switch (nextStep.getOperation()) {
            case FILTER_INDEXED_EQ:
            case FILTER_INDEXED_DISTINCT:
            case FILTER_INDEXED_GET:
            case FILTER_INDEXED_GT:
            case FILTER_INDEXED_LET:
            case FILTER_INDEXED_LT:
            case FILTER_INDEXED_MATCH:
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

        return nextStep;
    }

    /**
     * Creates a new {@link JavaRDD} based on the project and the query job configurations. It the filters list is not
     * empty, the rdd will contain the data filtered by them.
     * 
     * @param project
     *            Query columns needed to be retrieved.
     * @param extractorConfig
     *            Query job configuration.
     * @param filtersList
     *            List of filters to be applied while retrieving the data.
     * 
     * @return A {@link JavaRDD} contained the requested information.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     */
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

        return rdd;
    }

    /**
     * Generates the deep filters list from the crossdata ones.
     * 
     * @param filtersList
     *            List of crossdata filters.
     * 
     * @return List of deep filters.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
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
     * Returns a deep filter from a crossdata one.
     * 
     * @param filter
     *            Crossdata filter.
     * 
     * @return Deep filter.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     */
    private com.stratio.deep.commons.filter.Filter transformFilter(Filter filter) throws ExecutionException {

        ColumnSelector column = (ColumnSelector) filter.getRelation().getLeftTerm();

        return new com.stratio.deep.commons.filter.Filter(column.getName().getName(),
                QueryFilterUtils.retrieveFilterOperator(filter.getRelation().getOperator()),
                QueryFilterUtils.filterFromRightWhereRelation(filter.getRelation()));
    }

    /**
     * Creates a new query job configuration object from the generic one related to the cluster name.
     * 
     * @param clusterName
     *            Cluster name.
     * 
     * @return A new query job object containing the cluster configuration.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
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
     * Creates a {@link QueryResult} from the given {@link JavaRDD} based on the select information.
     * 
     * @param resultRdd
     *            Result {@link JavaRDD}.
     * @param selectStep
     *            {@link LogicalStep} containing the select information such as choosen columns and aliases.
     * 
     * @return {@link QueryResult} containing the result.
     */
    private QueryResult buildQueryResult(JavaRDD<Cells> resultRdd, Select selectStep) {

        List<Cells> resultCells = resultRdd.collect();

        Map<ColumnName, String> columnMap = selectStep.getColumnMap();
        Map<ColumnName, ColumnType> columnType = selectStep.getTypeMapFromColumnName();

        // Adding column metadata information
        List<ColumnMetadata> resultMetadata = new LinkedList<>();
        Object[] parameters = {};
        for (Entry<ColumnName, String> columnItem : columnMap.entrySet()) {

            ColumnName columnName = columnItem.getKey();
            String columnAlias = columnItem.getValue();
            columnName.setAlias(columnAlias);

            ColumnMetadata columnMetadata = new ColumnMetadata(columnName, parameters, columnType.get(columnName));

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
     * Transforms a {@link Cells} object to a {@link Row} one.
     * 
     * @param cells
     *            Input data to be transformed.
     * @param columnMap
     *            Aliases information.
     * 
     * @return A {@link Row} containing the information in the related database row.
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
     * Transforms the given {@link JavaRDD} using the operations in the {@link LogicalStep}.
     * 
     * @param logicalStep
     *            The {@link LogicalStep} including the requested transformations.
     * @param rdd
     *            The initial, but filtered if needed, {@link JavaRDD} retrieved from the data source.
     * 
     * @return The {@link JavaRDD} after applying the requested transformations.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     * @throws UnsupportedException
     *             If the required set of operations are not supported by the connector.
     */
    private JavaRDD<Cells> executeNextStep(LogicalStep logicalStep, JavaRDD<Cells> rdd, String tableName)
            throws ExecutionException, UnsupportedException {

        JavaRDD<Cells> resultRdd = rdd;
        String stepId = tableName;
        LogicalStep currentStep = logicalStep;
        while (currentStep != null) {
            if (currentStep instanceof Filter) {
                resultRdd = executeFilter((Filter) currentStep, resultRdd);
            } else if (currentStep instanceof UnionStep) {
                UnionStep unionStep = (UnionStep) currentStep;
                JavaRDD<Cells> joinedRdd = executeUnion(unionStep, resultRdd);
                if (joinedRdd == null) {
                    break;
                } else {
                    resultRdd = joinedRdd;
                    if (unionStep instanceof Join) {
                        stepId = ((Join) unionStep).getId();
                    } else {
                        throw new ExecutionException("Unknown union step found [" + unionStep.getOperation().toString()
                                + "]");
                    }
                }
            } else if (currentStep instanceof GroupBy) {
                resultRdd = executeGroupBy((GroupBy) currentStep, resultRdd);
            } else if (currentStep instanceof Select) {
                resultRdd = prepareResult((Select) currentStep, resultRdd);
            } else {
                throw new ExecutionException("Unexpected step found [" + currentStep.getOperation().toString() + "]");
            }

            currentStep = currentStep.getNextStep();
        }

        partialResultsMap.put(stepId, resultRdd);

        return resultRdd;
    }

    /**
     * Joins the given {@link JavaRDD} to the one specified in the {@link UnionStep} if it's ready; otherwise, the
     * {@link JavaRDD} is stored to wait for the other {@link JavaRDD} to be ready.
     * 
     * @param unionStep
     *            Union information.
     * @param rdd
     *            Original {@link JavaRDD} to be joined.
     * 
     * @return The resultant {@link JavaRDD} after executing the join method. It might be the original one if the other
     *         {@link JavaRDD} is not ready yet.
     * 
     * @throws ExecutionException
     *             If the execution of the required steps fails.
     * @throws UnsupportedException
     *             If the required set of operations are not supported by the connector.
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

                joinedRdd = executeJoin(partialResultsRdd, rdd, relations);

            } else {
                String joinLeftTableName = joinStep.getSourceIdentifiers().get(0);
                JavaRDD<Cells> partialRdd = partialResultsMap.get(joinLeftTableName);

                if (partialRdd != null) {
                    joinedRdd = executeJoin(partialRdd, rdd, joinStep.getJoinRelations());
                } else {
                    String joinRightTableName = joinStep.getSourceIdentifiers().get(1);
                    partialRdd = partialResultsMap.get(joinRightTableName);
                    partialResultsMap.remove(joinRightTableName);
                    if (partialRdd != null) {
                        joinedRdd = executeJoin(rdd, partialRdd, joinStep.getJoinRelations());
                    }
                }
            }
        } else {
            throw new ExecutionException("Unknown union step found [" + unionStep.getOperation().toString() + "]");
        }

        return joinedRdd;
    }

    /**
     * Joins the left {@link JavaRDD} to the right one based on the given list of relations.
     * 
     * @param leftRdd
     *            Left {@link JavaRDD}.
     * @param rdd
     *            right {@link JavaRDD}.
     * @param joinRelations
     *            List of relations to take into account when joining.
     * 
     * @return Joined {@link JavaRDD}.
     */
    private JavaRDD<Cells> executeJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rdd, List<Relation> joinRelations) {

        return QueryFilterUtils.doJoin(leftRdd, rdd, joinRelations);
    }

    /**
     * Groups the result by the given fields.
     * 
     * @param groupByStep
     *            GroupBy step.
     * @param rdd
     *            Initial {@link JavaRDD}.
     * 
     * @return Grouped {@link JavaRDD}.
     */
    private JavaRDD<Cells> executeGroupBy(GroupBy groupByStep, JavaRDD<Cells> rdd) {

        return QueryFilterUtils.groupByFields(rdd, groupByStep.getIds());
    }

    /**
     * Returns a {@link JavaRDD} just containing the columns specified in the {@link Select}.
     * 
     * @param selectStep
     *            Selection columns information.
     * @param rdd
     *            Original {@link JavaRDD}.
     * 
     * @return The {@link JavaRDD} with the desired columns.
     */
    private JavaRDD<Cells> prepareResult(Select selectStep, JavaRDD<Cells> rdd) throws ExecutionException {

        return QueryFilterUtils.filterSelectedColumns(rdd, selectStep.getColumnMap().keySet());

    }

    /**
     * Returns a {@link JavaRDD} filtered with the requested {@link Filter}.
     * 
     * @param filterStep
     *            Filtering information.
     * @param rdd
     *            Original {@link JavaRDD}.
     * 
     * @return The {@link JavaRDD} filtered by the given criteria.
     * 
     * @throws UnsupportedException
     *             If the required set of operations are not supported by the connector.
     */
    private JavaRDD<Cells> executeFilter(Filter filterStep, JavaRDD<Cells> rdd) throws ExecutionException,
            UnsupportedException {

        JavaRDD<Cells> resultRdd;

        Relation relation = filterStep.getRelation();
        if (relation.getOperator().isInGroup(Operator.Group.COMPARATOR)) {

            resultRdd = QueryFilterUtils.doWhere(rdd, relation);

        } else {

            throw new ExecutionException("Unknown Filter found [" + filterStep.getRelation().getOperator().toString()
                    + "]");

        }

        return resultRdd;

    }
}
