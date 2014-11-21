package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * Functional tests using MongoDB
 */
public class DeepConnectorAerospikeFT {

    private static final Logger logger = Logger.getLogger(DeepConnectorAerospikeFT.class);

    private static final String KEYSPACE = "book";

    private static final String MYTABLE1_CONSTANT = "input";

    private static final String AEROSPIKE_CLUSTERNAME_CONSTANT = "aerospike";

    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(AerospikeConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
    }

    @Test
    public void testSingleProjectAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(AEROSPIKE_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList("cantos", "id", "metadata"));
        project.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                "cantos")), Arrays.asList("cantos")));

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);

        // Assertions
        List<ColumnMetadata> columnsMetadata = result.getResultSet().getColumnMetadata();
        List<Row> rowsList = result.getResultSet().getRows();
    }
}
