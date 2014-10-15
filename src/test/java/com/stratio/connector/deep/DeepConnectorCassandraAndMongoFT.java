/**
 * 
 */
package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createJoin;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.LinkedList;
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
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;

/**
 * Functional tests using Cassandra DB
 */
public class DeepConnectorCassandraAndMongoFT {

    private static final String KEYSPACE = "functionaltest";

    private static final String MYTABLE1_CONSTANT = "songs";

    private static final String MYTABLE2_CONSTANT = "artists";

    private static final String ARTIST_CONSTANT = "artist";

    private static final String AGE_CONSTANT = "age";

    private static final String ARTIST_ALIAS_CONSTANT = "artistAlias";

    private static final String ARTIST_ALIAS2_CONSTANT = "artistAlias2";

    private static final String DESCRIPTION_ALIAS_CONSTANT = "descriptionAlias";

    private static final String AGE_ALIAS_CONSTANT = "ageAlias";

    private static final String DESCRIPTION_CONSTANT = "description";

    private static final String TITLE_CONSTANT = "title";

    private static final String TITLE_EX = "Hey Jude";

    private static final String YEAR_EX = "2004";

    private static final String YEAR_CONSTANT = "year";

    private static final String CASSANDRA_CLUSTERNAME_CONSTANT = "cassandra";

    private static final String MONGODB_CLUSTERNAME_CONSTANT = "mongodb";

    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(CassandraConnectionConfigurationBuilder.prepareConfiguration());
        connectionBuilder.connect(MongoConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
        // prepareDataForTest();

    }

    @Test
    public void testTwoProjectsJoinedAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new LinkedList<>();
        Project projectLeft = createProject(CASSANDRA_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ARTIST_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));
        Project projectRight = createProject(MONGODB_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
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
        assertEquals("Wrong number of rows", 74, rowsList.size());

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

    @AfterClass
    public static void setDown() {
        // clearData();
    }

}