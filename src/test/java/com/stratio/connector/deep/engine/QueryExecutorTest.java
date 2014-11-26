/**
 * 
 */
package com.stratio.connector.deep.engine;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.connector.deep.engine.query.QueryExecutor;
import com.stratio.connector.deep.engine.query.functions.DeepEquals;
import com.stratio.connector.deep.engine.query.transformation.FilterColumns;
import com.stratio.connector.deep.engine.query.transformation.JoinCells;
import com.stratio.connector.deep.engine.query.transformation.MapKeyForJoin;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Join;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

import scala.Tuple2;

/**
 * DeepQueryEngine testing class
 */
@RunWith(PowerMockRunner.class)
public class QueryExecutorTest {

    private static final String CATALOG_CONSTANT = "catalogname";

    private static final TableName TABLE1_CONSTANT = new TableName(CATALOG_CONSTANT, "tablename1");

    private static final TableName TABLE2_CONSTANT = new TableName(CATALOG_CONSTANT, "tablename2");

    private static final TableName TABLE3_CONSTANT = new TableName(CATALOG_CONSTANT, "tablename3");

    private static final String COLUMN1_CONSTANT = "column1name";

    private static final String COLUMN2_CONSTANT = "column2name";

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("clustername");

    private static final String DATA_CONSTANT = "DATA";

    @Mock
    private DeepSparkContext deepContext;

    @Mock
    private DeepConnectionHandler deepConnectionHandler;

    @Mock
    private DeepConnection deepConnection;

    @Mock
    private ExtractorConfig<Cells> extractorConfig;

    @Mock
    private JavaRDD<Cells> singleRdd;

    @Mock
    private JavaPairRDD<List<Object>, Cells> pairRdd;

    @Mock
    private JavaPairRDD<List<Object>, Tuple2<Cells, Cells>> joinedRdd;

    @Mock
    private Tuple2<List<Object>, Cells> tuple;

    private QueryExecutor queryExecutor;

    @Before
    public void before() throws Exception, HandlerConnectionException {

        queryExecutor = new QueryExecutor(deepContext, deepConnectionHandler);

        // Stubs
        when(deepConnectionHandler.getConnection(CLUSTERNAME_CONSTANT.getName())).thenReturn(deepConnection);
        when(deepConnection.getExtractorConfig()).thenReturn(extractorConfig);
        when(extractorConfig.clone()).thenReturn(extractorConfig);
        when(deepContext.createJavaRDD(any(ExtractorConfig.class))).thenReturn(singleRdd);
        when(deepContext.createHDFSRDD(any(ExtractorConfig.class))).thenReturn(singleRdd);
        when(singleRdd.collect()).thenReturn(generateListOfCells(3));
        when(singleRdd.filter(any(Function.class))).thenReturn(singleRdd);
        when(singleRdd.map(any(FilterColumns.class))).thenReturn(singleRdd);
        when(singleRdd.mapToPair(any(PairFunction.class))).thenReturn(pairRdd);
        when(pairRdd.join(pairRdd)).thenReturn(joinedRdd);
        when(joinedRdd.map(any(JoinCells.class))).thenReturn(singleRdd);
    }

    @Test
    public void simpleProjectAndSelectQueryTest() throws UnsupportedException, ExecutionException,
            HandlerConnectionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        project.setNextStep(createSelect());

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        queryExecutor.executeWorkFlow(logicalWorkflow);

        // Assertions
        verify(deepContext, times(1)).createJavaRDD(any(ExtractorConfig.class));
        verify(singleRdd, times(0)).filter(any(Function.class));
        verify(singleRdd, times(0)).mapToPair(any(MapKeyForJoin.class));
        verify(singleRdd, times(0)).mapToPair(any(MapKeyForJoin.class));
        verify(pairRdd, times(0)).join(pairRdd);
        verify(joinedRdd, times(0)).map(any(JoinCells.class));
        verify(singleRdd, times(1)).map(any(Function.class));
        verify(joinedRdd, times(0)).map(any(Function.class));

    }


    @Test
    public void simpleProjectAndSelectQueryHDFSTest() throws UnsupportedException, ExecutionException,
            HandlerConnectionException {

        ExtractorConfig<Cells> config = createExtractorForHDFS();

        when(deepConnection.getExtractorConfig()).thenReturn(config);

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        project.setNextStep(createSelect());

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        queryExecutor.executeWorkFlow(logicalWorkflow);

        // Assertions
        verify(deepContext, times(0)).createJavaRDD(any(ExtractorConfig.class));
        verify(deepContext, times(1)).createHDFSRDD(any(ExtractorConfig.class));
        verify(singleRdd, times(0)).filter(any(Function.class));
        verify(singleRdd, times(0)).mapToPair(any(MapKeyForJoin.class));
        verify(singleRdd, times(0)).mapToPair(any(MapKeyForJoin.class));
        verify(pairRdd, times(0)).join(pairRdd);
        verify(joinedRdd, times(0)).map(any(JoinCells.class));
        verify(singleRdd, times(1)).map(any(Function.class));
        verify(joinedRdd, times(0)).map(any(Function.class));

    }

    private ExtractorConfig createExtractorForHDFS() {

        ExtractorConfig<Cells> extractorConfigHDFS = new ExtractorConfig<>();
        extractorConfigHDFS.setExtractorImplClassName("hdfs");
        extractorConfigHDFS.setValues(extractorConfig.getValues());
        extractorConfigHDFS.putValue(ExtractorConstants.HDFS_SCHEMA,"[id:java.lang.String,author:java.lang.String," +
                "title:java.lang.String,year:java.lang.Integer,length:java.lang.Integer,single:java.lang.String]");

        return extractorConfigHDFS;

    }

    @Test
    public void simpleProjectAndSelectWithOneFilterQueryTest() throws UnsupportedException, ExecutionException,
            HandlerConnectionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        Filter filter = createFilter();
        filter.setNextStep(createSelect());
        project.setNextStep(filter);

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        queryExecutor.executeWorkFlow(logicalWorkflow);

        // Assertions
        verify(deepContext, times(1)).createJavaRDD(any(ExtractorConfig.class));
        verify(singleRdd, times(1)).filter(any(DeepEquals.class));
        verify(singleRdd, times(0)).mapToPair(any(MapKeyForJoin.class));
        verify(singleRdd, times(1)).map(any(FilterColumns.class));
        verify(pairRdd, times(0)).join(pairRdd);
        verify(joinedRdd, times(0)).map(any(JoinCells.class));
        verify(singleRdd, times(1)).map(any(Function.class));
        verify(joinedRdd, times(0)).map(any(Function.class));

        // TODO Add deep utils calls verifications
    }

    @Test
    public void simpleProjectAndSelectWithThreeFiltersQueryTest() throws UnsupportedException, ExecutionException,
            HandlerConnectionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        Filter filter1 = createFilter();
        Filter filter2 = createFilter();
        Filter filter3 = createFilter();
        filter3.setNextStep(createSelect());
        filter2.setNextStep(filter3);
        filter1.setNextStep(filter2);
        project.setNextStep(filter1);

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        queryExecutor.executeWorkFlow(logicalWorkflow);

        // Assertions
        verify(deepContext, times(1)).createJavaRDD(any(ExtractorConfig.class));
        verify(singleRdd, times(3)).filter(any(DeepEquals.class));
        verify(singleRdd, times(0)).mapToPair(any(MapKeyForJoin.class));
        verify(singleRdd, times(1)).map(any(FilterColumns.class));
        verify(pairRdd, times(0)).join(pairRdd);
        verify(joinedRdd, times(0)).map(any(JoinCells.class));
        verify(singleRdd, times(1)).map(any(Function.class));
        verify(joinedRdd, times(0)).map(any(Function.class));

        // TODO Add deep utils calls verifications
    }

    @Test
    public void twoProjectsJoinedAndSelectQueryTest() throws UnsupportedException, ExecutionException,
            HandlerConnectionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project projectLeft = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        Project projectRight = createProject(CLUSTERNAME_CONSTANT, TABLE2_CONSTANT);

        Join join = createJoin("joinId", TABLE1_CONSTANT, TABLE2_CONSTANT);

        join.setNextStep(createSelect());
        projectLeft.setNextStep(join);
        projectRight.setNextStep(join);

        // Two initial steps
        stepList.add(projectLeft);
        stepList.add(projectRight);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        queryExecutor.executeWorkFlow(logicalWorkflow);

        // Assertions
        verify(deepContext, times(2)).createJavaRDD(any(ExtractorConfig.class));
        verify(singleRdd, times(0)).filter(any(DeepEquals.class));
        verify(singleRdd, times(2)).mapToPair(any(MapKeyForJoin.class));
        verify(pairRdd, times(1)).join(pairRdd);
        verify(joinedRdd, times(1)).map(any(JoinCells.class));
        verify(singleRdd, times(1)).map(any(Function.class));

        // TODO Add deep utils calls verifications
    }

    @Test
    public void threeProjectsJoinedAndSelectQueryTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project1 = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        Project project2 = createProject(CLUSTERNAME_CONSTANT, TABLE2_CONSTANT);
        Project project3 = createProject(CLUSTERNAME_CONSTANT, TABLE3_CONSTANT);

        Join join1 = createJoin("joinId1", TABLE1_CONSTANT, TABLE2_CONSTANT);
        Join join2 = createJoin("joinId2", "joinId1", TABLE3_CONSTANT);

        join2.setNextStep(createSelect());
        join1.setNextStep(join2);
        project1.setNextStep(join1);
        project2.setNextStep(join1);
        project3.setNextStep(join2);

        // Three initial steps
        stepList.add(project1);
        stepList.add(project2);
        stepList.add(project3);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        queryExecutor.executeWorkFlow(logicalWorkflow);
    }

    private Project createProject(ClusterName clusterName, TableName tableName) {

        List<ColumnName> columns = new ArrayList<>();
        columns.add(new ColumnName(CATALOG_CONSTANT, tableName.getName(), COLUMN1_CONSTANT));
        columns.add(new ColumnName(CATALOG_CONSTANT, tableName.getName(), COLUMN2_CONSTANT));

        Project project = new Project(Operations.PROJECT, tableName, clusterName, columns);

        return project;
    }

    private Filter createFilter() {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT));
        StringSelector rightSelector = new StringSelector(DATA_CONSTANT);

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Filter filter = new Filter(Operations.FILTER_NON_INDEXED_EQ, relation);

        return filter;
    }

    private Join createJoin(String joinId, String leftSourceId, TableName rightSourceId) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(null, leftSourceId,
                COLUMN1_CONSTANT));
        ColumnSelector rightSelector = new ColumnSelector(new ColumnName(rightSourceId.getCatalogName().getName(),
                rightSourceId.getName(),
                COLUMN1_CONSTANT));

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Join join = new Join(Operations.SELECT_INNER_JOIN, joinId);
        join.addJoinRelation(relation);
        join.addSourceIdentifier(leftSourceId);
        join.addSourceIdentifier(rightSourceId.getName());

        return join;
    }

    private Join createJoin(String joinId, TableName leftSourceId, TableName rightSourceId) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(leftSourceId.getCatalogName().getName(),
                leftSourceId.getName(), COLUMN1_CONSTANT));
        ColumnSelector rightSelector = new ColumnSelector(new ColumnName(rightSourceId.getCatalogName().getName(),
                rightSourceId.getName(), COLUMN1_CONSTANT));

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Join join = new Join(Operations.SELECT_INNER_JOIN, joinId);
        join.addJoinRelation(relation);
        join.addSourceIdentifier(leftSourceId.getQualifiedName());
        join.addSourceIdentifier(rightSourceId.getQualifiedName());

        return join;
    }

    private Select createSelect() {

        ColumnName columnName = new ColumnName("catalogname", "tablename1", "column1Name");

        Map<ColumnName, String> columnsAliases = new HashMap<>();
        columnsAliases.put(columnName, "nameAlias");

        Map<String, ColumnType> columnsTypes = new HashMap<>();
        columnsTypes.put("catalogname.tablename1.column1Name", ColumnType.BIGINT);

        Map<ColumnName, ColumnType> typeMapFromColumnName = new LinkedHashMap<>();
        typeMapFromColumnName.put(columnName, ColumnType.BIGINT);

        Select select = new Select(Operations.PROJECT, columnsAliases, columnsTypes, typeMapFromColumnName);

        return select;
    }

    private List<Cells> generateListOfCells(int numElements) {

        List<Cells> cellsList = new ArrayList<>();
        for (int i = 0; i < numElements; i++) {
            cellsList.add(generateCells(null));
        }

        return cellsList;
    }

    private Cells generateCells(Cell cellValue) {

        Cells cells = new Cells();

        if (cellValue != null) {
            cells.add(TABLE1_CONSTANT.getQualifiedName(), cellValue);
        }

        cells.add(TABLE1_CONSTANT.getQualifiedName(), Cell.create(COLUMN1_CONSTANT, DATA_CONSTANT));
        cells.add(TABLE1_CONSTANT.getQualifiedName(), Cell.create(COLUMN2_CONSTANT, DATA_CONSTANT));

        return cells;

    }
}
