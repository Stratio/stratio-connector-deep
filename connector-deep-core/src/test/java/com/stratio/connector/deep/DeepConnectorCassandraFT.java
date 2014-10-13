/**
 * 
 */
package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createFilter;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static com.stratio.connector.deep.PrepareFunctionalTest.clearData;
import static com.stratio.connector.deep.PrepareFunctionalTest.prepareDataForTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Operator;

/**
 * Functional tests using Cassandra DB
 */
public class DeepConnectorCassandraFT {

    private static final String KEYSPACE = "functionaltest";

    private static final String MYTABLE1_CONSTANT = "songs";

    private static final String MYTABLE2_CONSTANT = "artists";

    private static final String AUTHOR_CONSTANT = "artist";

    private static final String AUTHOR_ALIAS_CONSTANT = "artistAlias";

    private static final String DESCRIPTION_CONSTANT = "description";

    private static final String TITLE_CONSTANT = "title";

    private static final String TITLE_EX = "Hey Jude";

    private static final String YEAR_EX = "2004";

    private static final String YEAR_CONSTANT = "year";

    private static final String CASSANDRA_CLUSTERNAME_CONSTANT = "cassandra";

    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(CassandraConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
        prepareDataForTest();
    }

    @Test
    public void testSingleProjectAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(AUTHOR_CONSTANT, DESCRIPTION_CONSTANT));
        project.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                AUTHOR_CONSTANT)), Arrays.asList(AUTHOR_ALIAS_CONSTANT)));

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
        assertEquals("Author expected", AUTHOR_CONSTANT, columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(AUTHOR_CONSTANT));
        }
    }

    @Test
    public void testSingleProjectWithOneFilterAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(AUTHOR_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));

        //for (Operator op : Operator.values()){

            project.setNextStep(createFilter(KEYSPACE, MYTABLE1_CONSTANT, YEAR_CONSTANT , Operator.GET, YEAR_EX ));

        //}

        LogicalStep filter =  project.getNextStep();

        filter.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                AUTHOR_CONSTANT)), Arrays.asList(AUTHOR_ALIAS_CONSTANT)));

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
        assertEquals("Author expected", AUTHOR_CONSTANT, columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(AUTHOR_CONSTANT));
        }
    }

    @AfterClass
    public static void setDown() {
        clearData();
    }

}
