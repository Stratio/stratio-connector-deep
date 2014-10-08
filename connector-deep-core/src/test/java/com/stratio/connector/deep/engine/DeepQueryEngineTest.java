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
import java.util.List;
import java.util.Map;

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
import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.connector.deep.engine.query.functions.DeepEquals;
import com.stratio.connector.deep.engine.query.transformation.MapKeyForJoin;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * DeepQueryEngine testing class
 */
@RunWith(PowerMockRunner.class)
public class DeepQueryEngineTest {

    private static final String CATALOG_CONSTANT = "catalogName";

    private static final TableName TABLE1_CONSTANT = new TableName(CATALOG_CONSTANT, "tableName1");

    private static final TableName TABLE2_CONSTANT = new TableName(CATALOG_CONSTANT, "tableName2");

    private static final String COLUMN1_CONSTANT = "column1Name";

    private static final String COLUMN2_CONSTANT = "column2Name";

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("clusterName");

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
    private JavaRDD<Cells> leftRdd;

    @Mock
    private JavaRDD<Cells> rightRdd;

    private DeepQueryEngine deepQueryEngine;

    @Before
    public void before() throws Exception, HandlerConnectionException {

        deepQueryEngine = new DeepQueryEngine(deepContext, deepConnectionHandler);

        // Stubs
        when(deepConnectionHandler.getConnection(CLUSTERNAME_CONSTANT.getName())).thenReturn(deepConnection);
        when(deepConnection.getExtractorConfig()).thenReturn(extractorConfig);
        when(deepContext.createJavaRDD(any(ExtractorConfig.class))).thenReturn(leftRdd, rightRdd);
        when(leftRdd.collect()).thenReturn(generateListOfCells(3));
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
        deepQueryEngine.execute(logicalWorkflow);

        // Assertions
        verify(deepContext, times(1)).createJavaRDD(any(ExtractorConfig.class));
        verify(leftRdd, times(0)).filter(any(Function.class));
        verify(leftRdd, times(0)).mapToPair(any(PairFunction.class));
        verify(leftRdd, times(1)).map(any(Function.class));
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
        deepQueryEngine.execute(logicalWorkflow);

        // Assertions
        verify(deepContext, times(1)).createJavaRDD(any(ExtractorConfig.class));
        verify(leftRdd, times(1)).filter(any(DeepEquals.class));
        verify(leftRdd, times(0)).mapToPair(any(PairFunction.class));
        verify(leftRdd, times(1)).map(any(Function.class));

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
        deepQueryEngine.execute(logicalWorkflow);

        // Assertions
        verify(deepContext, times(1)).createJavaRDD(any(ExtractorConfig.class));
        verify(leftRdd, times(3)).filter(any(DeepEquals.class));
        verify(leftRdd, times(0)).mapToPair(any(PairFunction.class));
        verify(leftRdd, times(1)).map(any(Function.class));

        // TODO Add deep utils calls verifications
    }

    @Test
    public void TwoProjectsJoinedAndSelectQueryTest() throws UnsupportedException, ExecutionException,
            HandlerConnectionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project projectLeft = createProject(CLUSTERNAME_CONSTANT, TABLE1_CONSTANT);
        Project projectRight = createProject(CLUSTERNAME_CONSTANT, TABLE2_CONSTANT);

        Join join = createJoin("joinId", TABLE1_CONSTANT.toString(), TABLE2_CONSTANT.toString());

        join.setNextStep(createSelect());
        projectLeft.setNextStep(join);
        projectRight.setNextStep(join);

        // One single initial step
        stepList.add(projectLeft);
        stepList.add(projectRight);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        deepQueryEngine.execute(logicalWorkflow);

        // Assertions
        verify(deepContext, times(2)).createJavaRDD(any(ExtractorConfig.class));
        verify(leftRdd, times(0)).filter(any(DeepEquals.class));
        verify(leftRdd, times(1)).mapToPair(new MapKeyForJoin(any(List.class)));

        verify(rightRdd, times(1)).mapToPair(new MapKeyForJoin(any(List.class)));


        // TODO Add deep utils calls verifications
    }

    private Project createProject(ClusterName clusterName, TableName tableName) {

        List<ColumnName> columns = new ArrayList<>();
        columns.add(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(), COLUMN1_CONSTANT));
        columns.add(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(), COLUMN2_CONSTANT));

        Project project = new Project(Operations.PROJECT, tableName, clusterName, columns);

        return project;
    }

    private Filter createFilter() {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT));
        StringSelector rightSelector = new StringSelector(DATA_CONSTANT);

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Filter filter = new Filter(Operations.FILTER_INDEXED_EQ, relation);

        return filter;
    }

    private Join createJoin(String joinId, String leftSourceId, String rightSourceId) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, leftSourceId,
                COLUMN1_CONSTANT));
        ColumnSelector rightSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, rightSourceId,
                COLUMN1_CONSTANT));

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Join join = new Join(Operations.SELECT_INNER_JOIN, joinId);
        join.addJoinRelation(relation);
        join.addSourceIdentifier(leftSourceId);
        join.addSourceIdentifier(rightSourceId);

        return join;
    }

    private Select createSelect() {

        ColumnName columnName = new ColumnName("catalogname", "tablename1", "column1Name");

        Map<ColumnName, String> columnsAliases = new HashMap<>();
        columnsAliases.put(columnName, "nameAlias");

        Map<String, ColumnType> columnsTypes = new HashMap<>();
        columnsTypes.put("catalogname.tablename1.column1Name", ColumnType.BIGINT);

        Select select = new Select(Operations.PROJECT, columnsAliases, columnsTypes);

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
