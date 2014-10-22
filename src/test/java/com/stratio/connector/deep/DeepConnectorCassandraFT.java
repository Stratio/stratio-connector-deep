/**
 *
 */
package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createFilter;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createJoin;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static com.stratio.connector.deep.PrepareFunctionalTest.prepareDataForCassandra;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Join;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.relationships.Operator;

/**
 * Functional tests using Cassandra DB
 */
public class DeepConnectorCassandraFT {
    private static final String KEYSPACE = "functionaltest";
    private static final String MYTABLE1_CONSTANT = "songs";
    private static final String MYTABLE2_CONSTANT = "artists";

    private static final String ID_CONSTANT = "id";

    private static final String ARTIST_CONSTANT = "artist";
    private static final String AGE_CONSTANT = "age";
    private static final String RATE_CONSTANT = "rate";
    private static final String ACTIVE_CONSTANT = "active";

    private static final String ARTIST_WILDCARD = "*enne*";

    private static final String ARTIST_ALIAS_CONSTANT = "artistAlias";
    private static final String ARTIST_ALIAS2_CONSTANT = "artistAlias2";
    private static final String DESCRIPTION_ALIAS_CONSTANT = "descriptionAlias";
    private static final String AGE_ALIAS_CONSTANT = "ageAlias";
    private static final String DESCRIPTION_CONSTANT = "description";
    private static final String TITLE_CONSTANT = "title";

    private static final String TITLE_EX = "Hey Jude";

    private static final Boolean ACTIVE_EX = true;
    private static final Long YEAR_EX = 2004L;
    private static final Long AGE_EX = 36L;
    private static final Integer ID_EX = 10;
    private static final Float RATE_EX = 8.3F;

    private static final String RATE_ST_EX = "8.3";
    private static final Long RATE_LNG_EX = 5L;
    private static final Integer RATE_INT_EX = 5;
    private static final Float RATE_FLO_EX = 8.3F;
    private static final Double RATE_DOU_EX = 8.3D;

    private static final String YEAR_CONSTANT = "year";
    private static final String CASSANDRA_CLUSTERNAME_CONSTANT = "cassandra";
    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(CassandraConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
        prepareDataForCassandra();
    }

    @Test
    public void singleProjectAndSelectTest() throws UnsupportedException, ExecutionException {
        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, DESCRIPTION_CONSTANT, ID_CONSTANT));
        project.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT)), Arrays.asList(ARTIST_ALIAS_CONSTANT)));
        // One single initial step
        stepList.add(project);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);
        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
        assertEquals("Wrong number of rows", 210, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE1_CONSTANT + "." + ARTIST_CONSTANT,
                columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());
        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting artist column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
        }
    }

    @Test
    public void singleProjectWithOneNonIndexStringFilterAndSelectTest() throws UnsupportedException,
            ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));

        project.setNextStep(createFilter(KEYSPACE, MYTABLE1_CONSTANT, TITLE_CONSTANT, Operator.EQ, TITLE_EX, false));

        LogicalStep filter = project.getNextStep();
        filter.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT)), Arrays.asList(ARTIST_ALIAS_CONSTANT)));
        // One single initial step
        stepList.add(project);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);
        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
        assertEquals("Wrong number of rows", 1, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE1_CONSTANT + "." + ARTIST_CONSTANT,
                columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());
        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
        }
    }

    @Test
    public void singleProjectWithOneNonIndexIntegerFilterAndSelectTest() throws UnsupportedException,
            ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ID_CONSTANT, ARTIST_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));

        project.setNextStep(createFilter(KEYSPACE, MYTABLE1_CONSTANT, ID_CONSTANT, Operator.LET, ID_EX, false));

        LogicalStep filter = project.getNextStep();
        filter.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT)), Arrays.asList(ARTIST_ALIAS_CONSTANT)));

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);

        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();

        // Checking results number
        assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
        assertEquals("Wrong number of rows", 10, rowsList.size());

        // Checking metadata
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE1_CONSTANT + "." + ARTIST_CONSTANT,
                columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());

    }

    @Test
    public void twoProjectsJoinedAndSelectTest() throws UnsupportedException, ExecutionException {
        // Input data
        List<LogicalStep> stepList = new LinkedList<>();
        Project projectLeft = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));
        Project projectRight = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, AGE_CONSTANT));
        Join join = createJoin("joinId", createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                ARTIST_CONSTANT));
        join.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                ARTIST_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                AGE_CONSTANT), createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                DESCRIPTION_CONSTANT)),
                Arrays.asList(ARTIST_ALIAS_CONSTANT, ARTIST_ALIAS2_CONSTANT, DESCRIPTION_ALIAS_CONSTANT,
                        AGE_ALIAS_CONSTANT)));
        projectLeft.setNextStep(join);
        projectRight.setNextStep(join);
        // Two initial steps
        stepList.add(projectLeft);
        stepList.add(projectRight);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
        // Execution
        QueryResult result = deepQueryEngine.execute(logicalWorkflow);
        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 4, columnsMetadata.size());
        assertEquals("Wrong number of rows", 72, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", ARTIST_ALIAS_CONSTANT, columnsMetadata.get(0).getColumnAlias());
        assertEquals("Author expected", ARTIST_ALIAS2_CONSTANT, columnsMetadata.get(1).getColumnAlias());
        assertEquals("Author expected", DESCRIPTION_ALIAS_CONSTANT, columnsMetadata.get(2).getColumnAlias());
        assertEquals("Author expected", AGE_ALIAS_CONSTANT, columnsMetadata.get(3).getColumnAlias());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());
        assertEquals("mytable2 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(1)
                .getTableName());
        assertEquals("mytable2 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(2)
                .getTableName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(3)
                .getTableName());
        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 4, row.size());
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS2_CONSTANT));
            assertNotNull("Expecting author column in row", row.getCell(DESCRIPTION_ALIAS_CONSTANT));
            assertNotNull("Expecting author column in row", row.getCell(AGE_ALIAS_CONSTANT));
        }
    }

    @Test
    public void twoProjectsNonIndexFilteredAndJoinedAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new LinkedList<>();
        Project projectLeft = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));
        Project projectRight = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, AGE_CONSTANT));

        Filter filterLeft = createFilter(KEYSPACE, MYTABLE1_CONSTANT, TITLE_CONSTANT, Operator.EQ, TITLE_EX, false);

        projectLeft.setNextStep(filterLeft);
        Join join = createJoin("joinId", createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                ARTIST_CONSTANT));
        join.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ARTIST_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                ARTIST_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                AGE_CONSTANT), createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                DESCRIPTION_CONSTANT)),
                Arrays.asList(ARTIST_ALIAS_CONSTANT, ARTIST_ALIAS2_CONSTANT, AGE_ALIAS_CONSTANT,
                        DESCRIPTION_ALIAS_CONSTANT)));
        filterLeft.setNextStep(join);
        projectRight.setNextStep(join);
        // Two initial steps
        stepList.add(projectLeft);
        stepList.add(projectRight);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
        // Execution
        QueryResult result = deepQueryEngine.execute(logicalWorkflow);
        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 4, columnsMetadata.size());
        assertEquals("Wrong number of rows", 1, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", ARTIST_ALIAS_CONSTANT, columnsMetadata.get(0).getColumnAlias());
        assertEquals("Author expected", ARTIST_ALIAS2_CONSTANT, columnsMetadata.get(1).getColumnAlias());
        assertEquals("Author expected", AGE_ALIAS_CONSTANT, columnsMetadata.get(2).getColumnAlias());
        assertEquals("Author expected", DESCRIPTION_ALIAS_CONSTANT, columnsMetadata.get(3).getColumnAlias());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());
        assertEquals("mytable2 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(1)
                .getTableName());
        assertEquals("mytable2 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(2)
                .getTableName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(3)
                .getTableName());
        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 4, row.size());
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS2_CONSTANT));
            assertNotNull("Expecting author column in row", row.getCell(DESCRIPTION_ALIAS_CONSTANT));
            assertNotNull("Expecting author column in row", row.getCell(AGE_ALIAS_CONSTANT));
        }
    }

    @Test
    public void singleProjectWithOneFilterAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, AGE_CONSTANT, RATE_CONSTANT, ACTIVE_CONSTANT));
        project.setNextStep(createFilter(KEYSPACE, MYTABLE2_CONSTANT, RATE_CONSTANT, Operator.LET, RATE_EX,
                false));
        LogicalStep filter = project.getNextStep();
        filter.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                ARTIST_CONSTANT)), Arrays.asList(ARTIST_ALIAS_CONSTANT)));
        // One single initial step
        stepList.add(project);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);
        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
        assertEquals("Wrong number of rows", 40, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE2_CONSTANT + "." + ARTIST_CONSTANT,
                columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
        }
    }

    @Test
    public void testSingleProjectWithAllComparatorFiltersAndSelectTest() throws UnsupportedException,
            ExecutionException {
        // Input data
        List<Serializable> rateValues = new ArrayList<>();
        // rateValues.add(RATE_ST_EX);
        rateValues.add(RATE_LNG_EX);
        rateValues.add(RATE_INT_EX);
        rateValues.add(RATE_FLO_EX);
        rateValues.add(RATE_DOU_EX);

        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, AGE_CONSTANT, RATE_CONSTANT, ACTIVE_CONSTANT));

        for (Operator op : Operator.values()) {

            if (op.isInGroup(Operator.Group.COMPARATOR) && !op.equals(Operator.IN) && !op.equals(Operator.BETWEEN)
                    && !op.equals(Operator.LIKE) && !op.equals(Operator.MATCH)) {

                for (Serializable value : rateValues) {

                    if (case1(op, value) || case2(op, value)) {

                        project.setNextStep(createFilter(KEYSPACE, MYTABLE2_CONSTANT, RATE_CONSTANT, op, value,
                                false));
                        LogicalStep filter = project.getNextStep();
                        filter.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                                ARTIST_CONSTANT)), Arrays.asList(ARTIST_ALIAS_CONSTANT)));
                        // One single initial step
                        stepList.add(project);
                        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
                        // Execution
                        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);
                        // Assertions
                        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
                        List<Row> rowsList = result.getResultSet().getRows();
                        // Checking results number
                        assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
                        assertEquals(
                                "Wrong number of rows in Operation " + op.name() + " with value " + value + " type ->"
                                        + value
                                                .getClass(),
                                getResultExpectedFomOp(op, value), rowsList.size());
                        System.out.println("number of rows in Operation " + op.name() + " with value " + value + " " +
                                "type ->" + value.getClass() + "  " + getResultExpectedFomOp(op, value));
                        // Checking metadata
                        assertEquals("Author expected", KEYSPACE + "." + MYTABLE2_CONSTANT + "." + ARTIST_CONSTANT,
                                columnsMetadata.get(0).getColumnName());
                        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(0)
                                .getTableName());

                        // Checking rows
                        for (Row row : rowsList) {
                            assertEquals("Wrong number of columns in the row", 1, row.size());
                            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
                        }
                    }
                }
            }
        }
    }

    private boolean case1(Operator op, Serializable value) {

        return (op.equals(Operator.LT) || op.equals(Operator.LET) || op.equals(Operator.GT) || op.equals
                (Operator.GET)) && (value.getClass().equals(Float.class) || value.getClass().equals
                (Double.class));
    }

    private boolean case2(Operator op, Serializable value) {

        return (op.equals(Operator.EQ) || op.equals(Operator.DISTINCT));
    }

    private int getResultExpectedFomOp(Operator op, Serializable data) {

        int result = 0;

        switch (op) {

        case EQ:
            if (data instanceof String) {
                result = 1;
            } else if (data instanceof Integer) {
                result = 39;
            } else if (data instanceof Long) {
                result = 39;
            } else if (data instanceof Float) {
                result = 1;
            } else if (data instanceof Double) {
                result = 1;
            }
            break;

        case LT:
            if (data instanceof String) {
                result = 0;
            } else if (data instanceof Integer) {
                result = 0;
            } else if (data instanceof Long) {
                result = 0;
            } else if (data instanceof Float) {
                result = 40;
            } else if (data instanceof Double) {
                result = 39;
            }
            break;
        case GT:
            if (data instanceof String) {
                result = 0;
            } else if (data instanceof Integer) {
                result = 0;
            } else if (data instanceof Long) {
                result = 0;
            } else if (data instanceof Float) {
                result = 0;
            } else if (data instanceof Double) {
                result = 0;
            }
            break;
        case LET:
            if (data instanceof String) {
                result = 2;
            } else if (data instanceof Integer) {
                result = 2;
            } else if (data instanceof Long) {
                result = 2;
            } else if (data instanceof Float) {
                result = 40;
            } else if (data instanceof Double) {
                result = 40;
            }
            break;
        case GET:
            if (data instanceof String) {
                result = 0;
            } else if (data instanceof Integer) {
                result = 0;
            } else if (data instanceof Long) {
                result = 0;
            } else if (data instanceof Float) {
                result = 0;
            } else if (data instanceof Double) {
                result = 1;
            }
            break;
        case DISTINCT:
            if (data instanceof String) {
                result = 2;
            } else if (data instanceof Integer) {
                result = 1;
            } else if (data instanceof Long) {
                result = 1;
            } else if (data instanceof Float) {
                result = 39;
            } else if (data instanceof Double) {
                result = 39;
            }
            break;
        default:

            break;
        }
        return result;
    }

    @Test
    public void luceneIndexFilterTest() throws UnsupportedException, ExecutionException {

        // Wait 10 secons...for the Cassandra index
        try {

            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, AGE_CONSTANT, RATE_CONSTANT, ACTIVE_CONSTANT));

        project.setNextStep(createFilter(KEYSPACE, MYTABLE2_CONSTANT, ARTIST_CONSTANT, Operator.MATCH,
                ARTIST_WILDCARD, true));

        LogicalStep filter = project.getNextStep();
        filter.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                ARTIST_CONSTANT)), Arrays.asList(ARTIST_ALIAS_CONSTANT)));

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);

        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
        assertEquals("Wrong number of rows", 1, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE2_CONSTANT + "." + ARTIST_CONSTANT,
                columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE2_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(ARTIST_ALIAS_CONSTANT));
        }

    }

    @AfterClass
    public static void setDown() {
        // clearData();

    }
}
