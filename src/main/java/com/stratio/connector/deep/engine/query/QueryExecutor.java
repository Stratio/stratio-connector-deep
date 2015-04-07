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

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.deep.configuration.DeepConnectorConstants;
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
import com.stratio.crossdata.common.logicalplan.OrderBy;
import com.stratio.crossdata.common.logicalplan.PartialResults;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.logicalplan.UnionStep;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.fs.utils.SchemaMap;
import com.stratio.deep.core.fs.utils.TableName;
import com.stratio.deep.core.fs.utils.TextFileDataTable;

/**
 * 
 *  Subclass responsible of the queries execution.
 *
 */
public class QueryExecutor {

    private static transient final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);



    private final DeepSparkContext deepContext;

	private final DeepConnectionHandler deepConnectionHandler;

	private final Map<String, JavaRDD<Cells>> partialResultsMap = new HashMap<>();

	private List<Filter> indexFilters;

	private List<Filter> nonIndexFilters;

	private OrderBy orderBy;

	private static int limit = DeepConnectorConstants.DEFAULT_RESULT_SIZE;

	/**
	 * Basic constructor.
	 * 
	 * @param deepContext
	 * 					The Deep Context
	 * @param deepConnectionHandler
	 * 								The Connection Handler
	 */
	public QueryExecutor(DeepSparkContext deepContext, DeepConnectionHandler deepConnectionHandler) {
		this.deepContext = deepContext; 
		this.deepConnectionHandler = deepConnectionHandler;
	}

	/**
	 * Execute a workflow to retrieve a subset of data.
	 * 
	 * @param workflow
	 *            The {@link com.stratio.crossdata.common.logicalplan.LogicalWorkflow} that contains the
	 *            {@link com.stratio.crossdata.
	 *            ommon.logicalplan.LogicalStep} to be executed.
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
		nextStep = arrangeOrderBy(nextStep);

		JavaRDD<Cells> filteredRdd = createRdd(project, extractorConfig, indexFilters);

		for (Filter filter : nonIndexFilters) {
			filteredRdd = executeFilter(filter, filteredRdd);
		}

		return executeNextStep(nextStep, filteredRdd, project.getTableName().getQualifiedName());
	}

	/**
	 * Sets the filters fields depending on whether they are executed by the data source or by deep.
	 * 
	 * @param step
	 *            Next {@link LogicalStep} to the project.
	 * @throws ExecutionException
	 *             If the execution of the required steps fails.
	 */
	private LogicalStep arrangeQueryFilters(LogicalStep step) throws ExecutionException {

		this.indexFilters = new ArrayList<Filter>();
		this.nonIndexFilters = new ArrayList<Filter>();

		LogicalStep nextStep = step;


		while (nextStep instanceof Filter) {
            Operations operation = recoveredOperation((Filter)nextStep);
			switch (operation) {
			case FILTER_INDEXED_EQ:
			case FILTER_INDEXED_DISTINCT:
			case FILTER_INDEXED_GET:
			case FILTER_INDEXED_GT:
			case FILTER_INDEXED_LET:
			case FILTER_INDEXED_LT:
			case FILTER_INDEXED_MATCH:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Add ["+operation.name()+"] ["+((Filter)nextStep).toString()+"] to indexed filters");

                }
				indexFilters.add((Filter) nextStep);
				break;
            case FILTER_PK_EQ:
			case FILTER_PK_DISTINCT:
			case FILTER_PK_GET:
			case FILTER_PK_GT:
			case FILTER_PK_LET:
			case FILTER_PK_LT:
            case FILTER_PK_MATCH:
			case FILTER_NON_INDEXED_EQ:
			case FILTER_NON_INDEXED_DISTINCT:
			case FILTER_NON_INDEXED_GET:
			case FILTER_NON_INDEXED_GT:
			case FILTER_NON_INDEXED_LET:
			case FILTER_NON_INDEXED_LT:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("add ["+operation.name()+"] ["+((Filter)nextStep).toString()+"] to NON  indexed filters");
                }
				nonIndexFilters.add((Filter) nextStep);
				break;
			default:
				throw new ExecutionException("Unexpected filter type [" + operation.toString() + "]");
			}

			nextStep = nextStep.getNextStep();
		}

		return nextStep;
	}

    /**
     * This method must recovered the operation from a filter
     * @param filter the filter.
     * @return the operation.
     * @throws ExecutionException if any operation is not supported.
     */
    private Operations recoveredOperation(Filter filter) throws ExecutionException {
        Set<Operations> operations = filter.getOperations();
        if (operations.size()!=1) {
            String message = "Deep connector only support one operation in a filter.";
            LOGGER.error(message);
            throw new ExecutionException(message);
        }
        return operations.toArray(new Operations[0])[0];
    }

    /**
	 *
	 *
	 * @param step
	 *            Next {@link LogicalStep} to the project.
	 * @throws ExecutionException
	 *             If the execution of the required steps fails.
	 */
	private LogicalStep arrangeOrderBy(LogicalStep step) throws ExecutionException {
		LogicalStep nextStep = step;

		while (nextStep instanceof OrderBy) {

			orderBy = (OrderBy) nextStep;

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
		JavaRDD<Cells> rdd;
		// Retrieving project information
		List<String> columnsList = new ArrayList<>();
		for (ColumnName columnName : project.getColumnList()) {
			columnsList.add(columnName.getName());
		}
        configureExtractorConfig(project, extractorConfig, filtersList, columnsList);

        LOGGER.info("Creating RDD");
		if(extractorConfig.getExtractorImplClassName()!=null && extractorConfig.getExtractorImplClassName().equals
				(DeepConnectorConstants.HDFS)){

            rdd = createRDDToHDFS(project, extractorConfig);
		}else{
			rdd = deepContext.createJavaRDD(extractorConfig);
		}
        LOGGER.info("RDD["+rdd.id()+"] has been created successfully" );

		return rdd;
	}

    private void configureExtractorConfig(Project project, ExtractorConfig<Cells> extractorConfig,
            List<Filter> filtersList, List<String> columnsList) throws ExecutionException {
        Serializable auxLimit = extractorConfig.getValues().get(DeepConnectorConstants
                .PROPERTY_DEFAULT_LIMIT);

        limit = (auxLimit != null) ? Integer.valueOf((String) auxLimit): DeepConnectorConstants.DEFAULT_RESULT_SIZE ;

        String[] inputColumns = columnsList.toArray(new String[columnsList.size()]);
        extractorConfig.putValue(ExtractorConstants.INPUT_COLUMNS, inputColumns);
        extractorConfig.putValue(ExtractorConstants.TABLE, project.getTableName().getName());
        extractorConfig.putValue(ExtractorConstants.CATALOG, project.getCatalogName());

        com.stratio.deep.commons.filter.Filter[] filters = generateFilters(filtersList);
        extractorConfig.putValue(ExtractorConstants.FILTER_QUERY, filters.length>0? filters :null);
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("ExtractorConfig "+ Arrays.toString(extractorConfig.getValues().entrySet().toArray())
            );

            if (filters!=null) {
            	LOGGER.debug("  Filters: " + Arrays.toString(filters));
            }
            if (inputColumns!=null){
            	LOGGER.debug("  inputColumns: " + Arrays.toString(inputColumns));
            }
        }
    }

    private JavaRDD<Cells> createRDDToHDFS(Project project, ExtractorConfig<Cells> extractorConfig)
            throws ExecutionException {
        JavaRDD<Cells> rdd;
        TextFileDataTable textFileDataTable = formatterFromSchema(extractorConfig,project);

        extractorConfig.putValue(ExtractorConstants.FS_FILEDATATABLE, textFileDataTable);
        extractorConfig.putValue(ExtractorConstants.TYPE_CONSTANT,ExtractorConstants.HDFS_TYPE);
        String path = (String)extractorConfig.getValues().get(ExtractorConstants.FS_FILE_PATH);
        extractorConfig.putValue(ExtractorConstants.FS_FILE_PATH,path+project.getCatalogName()+"/"+project
                .getTableName().getName()+extractorConfig.getValues().get(ExtractorConstants.HDFS_FILE_EXTENSION));

        rdd = deepContext.createHDFSRDD(extractorConfig).toJavaRDD();
        return rdd;
    }

    private TextFileDataTable formatterFromSchema(ExtractorConfig extractorConfig, Project project)
			throws ExecutionException {

		String schemaStr = (String) extractorConfig.getValues().get(ExtractorConstants.FS_SCHEMA);
		TextFileDataTable textFileDataTable = null;
		ArrayList<SchemaMap> columnMap = new ArrayList<>();
		if(schemaStr!=null) {
			try {
				String[] splits = schemaStr.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("]", "").split(",");
				for (String column : splits) {
					String[] columnData = column.split(":");
					Class<?> cls = Class.forName(columnData[1]);

					columnMap.add(new SchemaMap(columnData[0], cls));
				}
				textFileDataTable = new TextFileDataTable(new TableName(project.getCatalogName(),
						project.getTableName().getName()), columnMap);

				textFileDataTable.setLineSeparator((String) extractorConfig.getValues().get(ExtractorConstants
						.FS_FILE_SEPARATOR));

			} catch (ClassNotFoundException e) {
				throw new ExecutionException("" + e);
			}
		}
		return textFileDataTable ;
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

		deepConnection = (DeepConnection) deepConnectionHandler.getConnection(clusterName.getName());


		ExtractorConfig<Cells> extractorConfig;
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

		List<Cells> resultCells;

		if(orderBy!=null){
			resultCells = executeOrderBy(orderBy,resultRdd);
		}else{
            Long timeTake = System.currentTimeMillis();

			resultCells = resultRdd.take(limit);
            LOGGER.info("TIME - execute take("+limit+") in ["+(System.currentTimeMillis()-timeTake)+" ms]");
            if (LOGGER.isDebugEnabled()){
                LOGGER.debug("List<Cells> = RDD["+resultRdd.id()+"].take("+limit+")");
                LOGGER.debug("RDD["+resultRdd.id()+"].toDebugString()"+resultRdd.toDebugString());
            }
		}

		Map<Selector, String> columnMap = selectStep.getColumnMap();
		Map<Selector, ColumnType> columnType = selectStep.getTypeMapFromColumnName();


		// Adding column metadata information

        List<ColumnMetadata> resultMetadata = generateMetadata(columnMap, columnType);


        Long timeRows = System.currentTimeMillis();
        List<Row> resultRows = generateRowsResult(resultCells, columnMap);
        LOGGER.info("TIME - Generate rows result in ["+(System.currentTimeMillis()-timeRows)+" ms]");
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("The resultRows has been created with ["+resultRows.size()+"] rows");
        }



        ResultSet resultSet = new ResultSet();
        resultSet.setRows(resultRows);
        resultSet.setColumnMetadata(resultMetadata);
        QueryResult queryResult = QueryResult.createQueryResult(resultSet,0,true);
		return queryResult;
	}

    private List<Row> generateRowsResult(List<Cells> resultCells, Map<Selector, String> columnMap) {
        List<Row> resultRows = new LinkedList<>();
        long count = 1;
        for (Cells cells : resultCells) {

            resultRows.add(buildRowFromCells(cells, columnMap));
            if (LOGGER.isDebugEnabled() && count==1000){
                LOGGER.debug("The connector has been recovered ["+resultRows.size()+"] rows from Deep");
                count =   1;
            }
            count++;
        }
        return resultRows;
    }

    private List<ColumnMetadata> generateMetadata(Map<Selector, String> columnMap,
            Map<Selector, ColumnType> columnType) {
        Long timeMetadata = System.currentTimeMillis();
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("Metadata generation starting");
        }
        List<ColumnMetadata> resultMetadata = new LinkedList<>();
        Object[] parameters = {};
        for (Entry<Selector, String> columnItem : columnMap.entrySet()) {
            if (LOGGER.isDebugEnabled()){
                LOGGER.debug("Generate metadata from ["+columnItem.getKey()+"]");
            }
            ColumnName columnName = columnItem.getKey().getColumnName();
            String columnAlias = columnItem.getValue();
            columnName.setAlias(columnAlias);

            ColumnMetadata columnMetadata = new ColumnMetadata(columnName, parameters, columnType.get(columnName));

            resultMetadata.add(columnMetadata);
            if (LOGGER.isDebugEnabled()){
                LOGGER.debug("columnName ["+columnName+"] : columnAlias ["+columnAlias+"] : columnType ["+columnType
                        .get(columnName)+"]");
            }
        }

        LOGGER.info("TIME - Generate Metadata) in ["+(System.currentTimeMillis()-timeMetadata)+" ms]");
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("The metadata has been created");
        }
        return resultMetadata;
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
	private Row buildRowFromCells(Cells cells, Map<Selector, String> columnMap) {

		Row row = new Row();
		for (Entry<Selector, String> columnItem : columnMap.entrySet()) {
			ColumnName columnName = columnItem.getKey().getColumnName();

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
						throw new ExecutionException("Unknown union step found [" + unionStep + "]");
					}
				}
			} else if (currentStep instanceof GroupBy) {
				resultRdd = executeGroupBy((GroupBy) currentStep, resultRdd);
			} else if (currentStep instanceof OrderBy) {
				orderBy = ((OrderBy) currentStep);
			} else if (currentStep instanceof Select) {
				resultRdd = prepareResult((Select) currentStep, resultRdd);
			} else {
				throw new ExecutionException("Unexpected step found [" + currentStep + "]");
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
	private JavaRDD<Cells> executeUnion(UnionStep unionStep, JavaRDD<Cells> rdd) throws
            ExecutionException,
	UnsupportedException {


		if (!(unionStep instanceof Join)) {
            throw new ExecutionException("Unknown union step found [" + unionStep+ "]");
        }


        JavaRDD<Cells> joinedRdd = null;
        Join joinStep = (Join) unionStep;
        JavaRDD<Cells> leftPartialRdd;
        JavaRDD<Cells> rightPartialRdd = rdd;
        List<Relation> relations;


        if (joinStep.getOperations().contains((Operations.SELECT_INNER_JOIN_PARTIALS_RESULTS))){

            PartialResults partialResults = QueryPartialResultsUtils.getPartialResult(joinStep);
             leftPartialRdd = QueryPartialResultsUtils.createRDDFromResultSet(deepContext,
                    partialResults.getResults());
            relations = QueryPartialResultsUtils.getOrderedRelations(partialResults,
                   joinStep.getJoinRelations());

        } else {
            String joinLeftTableName = joinStep.getSourceIdentifiers().get(0);
            leftPartialRdd = partialResultsMap.get(joinLeftTableName);
            relations = joinStep.getJoinRelations();
            if (leftPartialRdd == null) {
                String joinRightTableName = joinStep.getSourceIdentifiers().get(1);
                rightPartialRdd = partialResultsMap.get(joinRightTableName);
                leftPartialRdd = rdd;
                partialResultsMap.remove(joinRightTableName);
            }
        }

        if (rightPartialRdd != null) {
            joinedRdd = executeJoin(leftPartialRdd,rightPartialRdd, relations);
        }

		return joinedRdd;
	}

	/**
	 * Joins the left {@link JavaRDD} to the right one based on the given list of relations.
	 * 
	 * @param leftRdd
	 *            Left {@link org.apache.spark.api.java.JavaRDD}.
	 * @param rdd
	 *            right {@link org.apache.spark.api.java.JavaRDD}.
	 * @param joinRelations
	 *            List of relations to take into account when joining.

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
	 * Order the result by the given fields.
	 *
	 * @param orderByStep
	 *            orderBy step.
	 * @param rdd
	 *            Initial {@link JavaRDD}.
	 *
	 * @return Grouped {@link JavaRDD}.
	 */
	private List<Cells> executeOrderBy(OrderBy orderByStep, JavaRDD<Cells> rdd) {

		return QueryFilterUtils.orderByFields(rdd, orderByStep.getIds(), limit);
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

	return  QueryFilterUtils.doWhere(rdd, filterStep.getRelation());

	}
}
