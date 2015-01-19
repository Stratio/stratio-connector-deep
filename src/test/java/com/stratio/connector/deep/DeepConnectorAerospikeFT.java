package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.jgroups.util.Util.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
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

    private static final String KEYSPACE = "test";

    private static final String MYTABLE1_CONSTANT = "input";

    private static final String AEROSPIKE_CLUSTERNAME_CONSTANT = "aerospike";

    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(AerospikeConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
        Host[] hosts = {   new Host(AerospikeConnectionConfigurationBuilder.HOST,Integer.valueOf
                (AerospikeConnectionConfigurationBuilder
                .PORT))};
        AerospikeClient client =  new AerospikeClient(null, hosts);
        client.put(null,new Key(KEYSPACE,MYTABLE1_CONSTANT,1),new Bin("id",1),new Bin("cantos","cantos1"),new Bin
                ("metadata","metadata1"));
        client.put(null,new Key(KEYSPACE,MYTABLE1_CONSTANT,2),new Bin("id",2),new Bin("cantos","cantos2"),new Bin
                ("metadata","metadata3"));
        client.put(null,new Key(KEYSPACE,MYTABLE1_CONSTANT,3),new Bin("id",3),new Bin("cantos","cantos3"),new Bin
                ("metadata","metadata3"));
        client.close();
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


        List<Row> rowsList = result.getResultSet().getRows();
        assertEquals("We revive the correct row number",3,rowsList.size());
    }


}
