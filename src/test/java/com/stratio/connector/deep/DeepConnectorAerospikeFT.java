package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.jgroups.util.Util.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.stratio.connector.deep.engine.query.DeepQueryEngine;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
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

    private static AerospikeClient client;
    private static ConnectionsHandler connectionBuilder;
    static {
        Host[] hosts = { new Host(AerospikeConnectionConfigurationBuilder.HOST,
                        Integer.valueOf(AerospikeConnectionConfigurationBuilder.PORT)) };
        client = new AerospikeClient(null, hosts);
    }

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(AerospikeConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();

        deleteSet(KEYSPACE, MYTABLE1_CONSTANT);

        Key key1 = new Key(KEYSPACE, MYTABLE1_CONSTANT, 1);
        Key key2 = new Key(KEYSPACE, MYTABLE1_CONSTANT, 2);
        Key key3 = new Key(KEYSPACE, MYTABLE1_CONSTANT, 3);
        client.put(null, key1, new Bin("id", 1), new Bin("cantos", "cantos1"), new Bin("metadata", "metadata1"));
        client.put(null, key2, new Bin("id", 2), new Bin("cantos", "cantos2"), new Bin("metadata", "metadata3"));
        client.put(null, key3, new Bin("id", 3), new Bin("cantos", "cantos3"), new Bin("metadata", "metadata3"));
    }

    private static void deleteSet(String keyspace2, String mytable1Constant) {
        /*
         * // Delete existing keys client.delete(null, key1); client.delete(null, key2); client.delete(null, key3);
         */
        ScanCallback deleteAll = new ScanCallback() {

            @Override
            public void scanCallback(Key key, Record record) throws AerospikeException {
                client.delete(null, key);

            }
        };
        client.scanAll(new ScanPolicy(), KEYSPACE, MYTABLE1_CONSTANT, deleteAll, new String[] {});

    }

    @Test
    public void testSingleProjectAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(AEROSPIKE_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                        Arrays.asList("cantos", "id", "metadata"));
        project.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT, "cantos")),
                        Arrays.asList("cantos")));

        // One single initial step
        stepList.add(project);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(stepList);

        // Execution
        QueryResult result = deepQueryEngine.executeWorkFlow(logicalWorkflow);

        // Assertions

        List<Row> rowsList = result.getResultSet().getRows();
        assertEquals("We revive the correct row number", 3, rowsList.size());
    }

    @AfterClass
    public static void tearDown() throws ExecutionException {

        deleteSet(KEYSPACE, MYTABLE1_CONSTANT);
        client.close();
        connectionBuilder.shutdown();
    }
}
