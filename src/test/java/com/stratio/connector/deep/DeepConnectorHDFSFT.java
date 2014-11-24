package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stratio.connector.deep.engine.DeepMetadataEngine;
import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Functional tests using HDFS
 */

public class DeepConnectorHDFSFT {

    private static final String KEYSPACE = "test";
    private static final String MYTABLE1_CONSTANT = "songs";
    private static final String HDFS_CLUSTERNAME_CONSTANT = "hdfs";

    private static final String ID_CONSTANT     = "id";
    private static final String AUTHOR_CONSTANT = "author";
    private static final String TITLE_CONSTANT  = "Title";
    private static final String YEAR_CONSTANT   = "Year";
    private static final String LENGHT_CONSTANT = "Length";
    private static final String SINGLE_CONSTANT = "Single";

    private static DeepMetadataEngine deepMetadataEngine;

    private static DeepQueryEngine deepQueryEngine;
    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(HDFSConnectionConfigurationBuilder.prepareConfiguration());
        //deepMetadataEngine = connectionBuilder.getMetadataEngine();
        deepQueryEngine    = connectionBuilder.getQueryEngine();
    }

    @Test
    public void singleProjectAndSelectTest() throws UnsupportedException, ExecutionException {
        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(HDFS_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(ID_CONSTANT,AUTHOR_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT, LENGHT_CONSTANT,SINGLE_CONSTANT));
        project.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                ID_CONSTANT),createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                AUTHOR_CONSTANT),createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                TITLE_CONSTANT),createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                YEAR_CONSTANT),createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                LENGHT_CONSTANT),createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                SINGLE_CONSTANT)), Arrays.asList(ID_CONSTANT,AUTHOR_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT, LENGHT_CONSTANT,SINGLE_CONSTANT)));

        // One single initial step
        stepList.add(project);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);
        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);
        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
        // Checking results number
        assertEquals("Wrong number of rows metadata", 6, columnsMetadata.size());
        assertEquals("Wrong number of rows", 210, rowsList.size());
        // Checking metadata
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE1_CONSTANT + "." + ID_CONSTANT,
                columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());
        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 6, row.size());
            assertNotNull("Expecting artist column in row", row.getCell(AUTHOR_CONSTANT));
        }
    }

}
