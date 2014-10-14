package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createFilter;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createJoin;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
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
import com.stratio.meta.common.statements.structures.relationships.Operator;

/**
 * Functional tests using MongoDB
 */

public class DeepConnectorMongoFT {


    private static final Logger logger = Logger.getLogger(DeepConnectorMongoFT.class);

    private static final String KEYSPACE = "functionaltest";

    private static final String MYTABLE1_CONSTANT = "songs";

    private static final String MYTABLE2_CONSTANT = "artists";

    private static final String AUTHOR_CONSTANT = "artist";

    private static final String AUTHOR_ALIAS_CONSTANT = "artistAlias";

    private static final String AUTHOR_ALIAS2_CONSTANT = "artistAlias";

    private static final String TITLE_CONSTANT = "title";

    private static final String YEAR_CONSTANT = "year";

    private static final String AGE_CONSTANT  = "age";

    private static final String DESCRIPTION_CONSTANT = "description";

    private static final String TITLE_EX = "Hey Jude";

    private static final String YEAR_EX = "2004";

    private static final String DESCRIPTION_ALIAS_CONSTANT = "descriptionAlias";

    private static final String AGE_ALIAS_CONSTANT = "ageAlias";

    private static final String MONGO_CLUSTERNAME_CONSTANT = "mongodb";

    private static DeepQueryEngine deepQueryEngine;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException {
        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(MongoConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();
        //prepareDataForTest();
    }


    @Test
    public void testSingleProjectAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(MONGO_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
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
            assertNotNull("Expecting author column in row", row.getCell(AUTHOR_ALIAS_CONSTANT));
        }
    }

    @Test
    public void testSingleProjectWithOneFilterAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(MONGO_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(AUTHOR_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));

        for (Operator op : Operator.values()){


            if(op.isInGroup(Operator.Group.COMPARATOR) && !op.equals(Operator.IN) && !op.equals(Operator.BETWEEN)
                    && !op.equals(Operator.LIKE) && !op.equals(Operator.MATCH)){

                logger.debug("--------------FILTER TEST FOR OPERATOR "+op+" ------------------------------------");
                project.setNextStep(createFilter(KEYSPACE, MYTABLE1_CONSTANT, YEAR_CONSTANT , op , YEAR_EX ));


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
                int resultExpectedFomOp = getResultExpectedFomOp(op);
                assertEquals("Wrong number of rows metadata", 1, columnsMetadata.size());
                logger.debug("------- RESULT FILTER TEST FOR OPERATOR " + op + " " +
                        "RESULT ---> " + rowsList.size() + "--EXPECTED ---> " + resultExpectedFomOp);
                assertEquals("Wrong number of rows", resultExpectedFomOp, rowsList.size());

                // Checking metadata
                assertEquals("Author expected", AUTHOR_CONSTANT, columnsMetadata.get(0).getColumnName());
                assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                        .getTableName());

                // Checking rows
                for (Row row : rowsList) {
                    assertEquals("Wrong number of columns in the row", 1, row.size());
                    assertNotNull("Expecting author column in row", row.getCell(AUTHOR_ALIAS_CONSTANT));
                }
            }

        }


    }




    @Test
    public void testTwoProjectsJoinedAndSelectTest() throws UnsupportedException, ExecutionException {

        // Input data
        List<LogicalStep> stepList = new LinkedList<>();
        Project projectLeft = createProject(MONGO_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT,
                Arrays.asList(AUTHOR_CONSTANT, DESCRIPTION_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT));
        Project projectRight = createProject(MONGO_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE2_CONSTANT,
                Arrays.asList(AUTHOR_CONSTANT, AGE_CONSTANT));

        Join join = createJoin("joinId", createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                AUTHOR_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                AUTHOR_CONSTANT));

        join.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                        AUTHOR_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                        AUTHOR_CONSTANT), createColumn(KEYSPACE, MYTABLE2_CONSTANT,
                        AGE_CONSTANT), createColumn(KEYSPACE, MYTABLE1_CONSTANT,
                        DESCRIPTION_CONSTANT)),
                Arrays.asList(AUTHOR_ALIAS_CONSTANT, AUTHOR_ALIAS2_CONSTANT, DESCRIPTION_ALIAS_CONSTANT,
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
        assertEquals("Wrong number of rows", 76, rowsList.size());

        // Checking metadata
        assertEquals("Author expected", AUTHOR_CONSTANT, columnsMetadata.get(0).getColumnName());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0)
                .getTableName());

        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 3, row.size());
            assertNotNull("Expecting author column in row", row.getCell(AUTHOR_ALIAS_CONSTANT));
        }
    }
    //Results expected for Comparator Operators by pattersn 2004
    private int getResultExpectedFomOp(Operator op) {

        int result =0;

        switch (op){

            case EQ:
                result=2;
                break;
            case LT:
                result= 183;
                break;
            case GT:
                result= 25;
                break;
            case LET:
                result= 185;
                break;
            case GET:
                result= 27;
                break;
            case DISTINCT:
                result= 208;
                break;
            default:
                result=210;
                break;
        }


        return result;
    }

}
