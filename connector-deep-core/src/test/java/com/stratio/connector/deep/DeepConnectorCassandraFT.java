/**
 * 
 */
package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createFilter;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
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

    private static final String CATALOG_CONSTANT = "twitter";

    private static final String LEFTTABLE_CONSTANT = "mytable1";

    private static final String RIGHTTABLE_CONSTANT = "mytable2";

    private static final String LEFTTABLE_MAIN_COLUMN_CONSTANT = "author";

    private static final String LEFTTABLE_MAIN_COLUMN_ALIAS_CONSTANT = "authorAlias";

    private static final String LEFTTABLE_SECONDARY_COLUMN_CONSTANT = "description";

    private static final String RIGHTTABLE_MAIN_COLUMN_CONSTANT = "author";

    private static final String RIGHTTABLE_MAIN_COLUMN_ALIAS_CONSTANT = "authorAlias";

    private static final String RIGHTTABLE_SECONDARY_COLUMN_CONSTANT = "author_name";

    private static final String CASSANDRA_CLUSTERNAME_CONSTANT = "cassandra";

    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(CassandraConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
    }

    @Test
    public void testSingleProjectAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, CATALOG_CONSTANT, LEFTTABLE_CONSTANT,
                Arrays.asList(LEFTTABLE_MAIN_COLUMN_CONSTANT, LEFTTABLE_SECONDARY_COLUMN_CONSTANT));
        project.setNextStep(createSelect(Arrays.asList(createColumn(CATALOG_CONSTANT, LEFTTABLE_CONSTANT,
                LEFTTABLE_MAIN_COLUMN_CONSTANT)), Arrays.asList(LEFTTABLE_MAIN_COLUMN_ALIAS_CONSTANT)));

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
        assertEquals("Wrong number of rows", 7, rowsList.size());

        // Checking metadata
        assertEquals("Author expected", LEFTTABLE_MAIN_COLUMN_CONSTANT, columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", CATALOG_CONSTANT + "." + LEFTTABLE_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(LEFTTABLE_MAIN_COLUMN_ALIAS_CONSTANT));
        }
    }

    @Test
    public void testSingleProjectWithOneFilterAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, CATALOG_CONSTANT, LEFTTABLE_CONSTANT,
                Arrays.asList(LEFTTABLE_MAIN_COLUMN_CONSTANT, LEFTTABLE_SECONDARY_COLUMN_CONSTANT));
        Filter filter = createFilter(CATALOG_CONSTANT, LEFTTABLE_CONSTANT,
                LEFTTABLE_MAIN_COLUMN_CONSTANT, Operator.EQ, "id457");
        project.setNextStep(filter);
        filter.setNextStep(createSelect(Arrays.asList(createColumn(CATALOG_CONSTANT, LEFTTABLE_CONSTANT,
                LEFTTABLE_MAIN_COLUMN_CONSTANT)), Arrays.asList(LEFTTABLE_MAIN_COLUMN_ALIAS_CONSTANT)));

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
        assertEquals("Wrong number of rows", 7, rowsList.size());

        // Checking metadata
        assertEquals("Author expected", LEFTTABLE_MAIN_COLUMN_CONSTANT, columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", CATALOG_CONSTANT + "." + LEFTTABLE_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 1, row.size());
            assertNotNull("Expecting author column in row", row.getCell(LEFTTABLE_MAIN_COLUMN_ALIAS_CONSTANT));
        }
    }

}
