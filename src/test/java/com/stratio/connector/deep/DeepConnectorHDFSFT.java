package com.stratio.connector.deep;

import static com.stratio.connector.deep.LogicalWorkflowBuilder.createColumn;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createProject;
import static com.stratio.connector.deep.LogicalWorkflowBuilder.createSelect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
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
 * Functional tests using HDFS ¡¡¡WARNING!!! Create fileSystem under the hdfs path (connector-application.conf)
 * {hdfs.path}/KEYSPACE with metaFile.csv songs.csv files in test resources
 */

public class DeepConnectorHDFSFT {

    private static final Logger logger = Logger.getLogger(DeepConnectorHDFSFT.class);

    private static final String KEYSPACE = "test";
    private static final String MYTABLE1_CONSTANT = "songs";
    private static final String HDFS_CLUSTERNAME_CONSTANT = "hdfs";

    private static final String ID_CONSTANT = "id";
    private static final String AUTHOR_CONSTANT = "author";
    private static final String TITLE_CONSTANT = "Title";
    private static final String YEAR_CONSTANT = "Year";
    private static final String LENGHT_CONSTANT = "Length";
    private static final String SINGLE_CONSTANT = "Single";

    private static DeepQueryEngine deepQueryEngine;

    private static final String DESTINO = "/" + KEYSPACE + "/";

    private static final String NAMENODE_ADDRESS = "conectores3:9000";
    private static Configuration config = new Configuration();
    private static ConnectionsHandler connectionBuilder;

    @BeforeClass
    public static void setUp() throws InitializationException, ConnectionException, UnsupportedException, IOException,
                    ExecutionException {
        connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(HDFSConnectionConfigurationBuilder.prepareConfiguration());
        deepQueryEngine = connectionBuilder.getQueryEngine();

        prepareData();

    }

    private static void prepareData() throws IOException, ExecutionException {

        config.set("dfs.replication", "2");
        config.set("fs.defaultFS", "hdfs://" + NAMENODE_ADDRESS);

        addFile(DeepConnectorHDFSFT.class.getClassLoader().getResource(".").getPath() + "metaFile.csv", DESTINO);
        addFile(DeepConnectorHDFSFT.class.getClassLoader().getResource(".").getPath() + "songs.csv", DESTINO);
        addFile(DeepConnectorHDFSFT.class.getClassLoader().getResource(".").getPath() + "artists.csv", DESTINO);

    }

    private static void addFile(String source, String dest) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            // Get the filename out of the file path
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

            // Create the destination path including the filename.
            if (dest.charAt(dest.length() - 1) != '/') {
                dest = dest + "/" + filename;
            } else {
                dest = dest + filename;
            }

            // Check if the file already exists
            Path path = new Path(dest);
            if (fileSystem.exists(path)) {
                logger.info("File " + dest + " already exists");
                return;
            }

            // Create a new file and write data to it.
            FSDataOutputStream out = fileSystem.create(path);

            InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

            IOUtils.copyBytes(in, out, config);

            // Close all the file descripters
            in.close();
            out.close();
            fileSystem.close();

        } catch (IOException e) {
            throw new ExecutionException("Exception " + e);
        }

    }

    @Test
    public void singleProjectAndSelectTest() throws UnsupportedException, ExecutionException {
        // Input data
        List<LogicalStep> stepList = new ArrayList<>();
        Project project = createProject(HDFS_CLUSTERNAME_CONSTANT, KEYSPACE, MYTABLE1_CONSTANT, Arrays.asList(
                        ID_CONSTANT, AUTHOR_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT, LENGHT_CONSTANT, SINGLE_CONSTANT));
        project.setNextStep(createSelect(Arrays.asList(createColumn(KEYSPACE, MYTABLE1_CONSTANT, ID_CONSTANT),
                        createColumn(KEYSPACE, MYTABLE1_CONSTANT, AUTHOR_CONSTANT),
                        createColumn(KEYSPACE, MYTABLE1_CONSTANT, TITLE_CONSTANT),
                        createColumn(KEYSPACE, MYTABLE1_CONSTANT, YEAR_CONSTANT),
                        createColumn(KEYSPACE, MYTABLE1_CONSTANT, LENGHT_CONSTANT),
                        createColumn(KEYSPACE, MYTABLE1_CONSTANT, SINGLE_CONSTANT)), Arrays.asList(ID_CONSTANT,
                        AUTHOR_CONSTANT, TITLE_CONSTANT, YEAR_CONSTANT, LENGHT_CONSTANT, SINGLE_CONSTANT)));

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
        assertEquals("Author expected", KEYSPACE + "." + MYTABLE1_CONSTANT + "." + ID_CONSTANT, columnsMetadata.get(0)
                        .getName().toString());
        assertEquals("mytable1 expected", KEYSPACE + "." + MYTABLE1_CONSTANT, columnsMetadata.get(0).getName()
                        .getTableName().getQualifiedName());
        // Checking rows
        for (Row row : rowsList) {
            assertEquals("Wrong number of columns in the row", 6, row.size());
            assertNotNull("Expecting artist column in row", row.getCell(AUTHOR_CONSTANT));
        }
    }

    @AfterClass
    public static void tearDown() throws InitializationException, ConnectionException, UnsupportedException,
                    IOException, ExecutionException {
        deleteFile("/" + KEYSPACE + "/metaFile.csv");
        deleteFile("/" + KEYSPACE + "/artists.csv");
        deleteFile("/" + KEYSPACE + "/songs.csv");
        connectionBuilder.shutdown();

    }

    private static void deleteFile(String filename) throws ExecutionException, IOException {
        FileSystem fileSystem = null;
        try {

            fileSystem = FileSystem.get(config);
            Path path = new Path(filename);

            if (!fileSystem.exists(path)) {
                logger.info("File " + filename + " does not exists");
                return;
            }

            fileSystem.delete(path, false);

        } catch (IOException e) {
            throw new ExecutionException(" " + e);
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }
}
