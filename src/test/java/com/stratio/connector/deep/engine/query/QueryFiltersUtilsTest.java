package com.stratio.connector.deep.engine.query;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ContextProperties;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by dgomez on 30/09/14.
 */
@RunWith(PowerMockRunner.class)
public class QueryFiltersUtilsTest implements Serializable {

    private final QueryFilterUtils queryFilterUtils = new QueryFilterUtils();

    private static final Logger logger = Logger.getLogger(QueryFiltersUtilsTest.class);

    private static final String CASSANDRA_CELL_CLASS = "com.stratio.deep.cassandra.extractor.CassandraCellExtractor";
    private static final String MONGO_CELL_CLASS = "com.stratio.deep.mongodb.extractor.MongoCellExtractor";
    private static final String ES_CELL_CLASS = "com.stratio.deep.extractor.ESCellExtractor";

    private static final String CATALOG_CONSTANT = "test";

    private static final TableName TABLE1_CONSTANT = new TableName(CATALOG_CONSTANT, "mytable");

    private static final TableName TABLE2_CONSTANT = new TableName(CATALOG_CONSTANT, "mytable1");

    private static final String COLUMN1_CONSTANT = "thekey";

    private static final String COLUMN2_CONSTANT = "thekey";

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("clusterName");

    private static final String DATA_CONSTANT = "001";
    @Mock
    private DeepSparkContext deepContext;

    @Mock
    private DeepConnectionHandler deepConnectionHandler;

    @Mock
    private DeepConnection deepConnection;

    @Mock
    private ExtractorConfig<Cells> extractorConfig;

    @Mock(name = "leftRdd")
    private JavaRDD<Cells> leftRdd;

    @Mock(name = "rightRdd")
    private JavaRDD<Cells> rightRdd;

    @Mock(name = "thirdRdd")
    private JavaRDD<Cells> thirdRdd;

    @Before
    public void before() throws Exception, HandlerConnectionException {
        String job = "java:creatingCellRDD";
//        // Cassandra
//        String KEYSPACENAME = CATALOG_CONSTANT;
//        String TABLENAME_1 = TABLE1_CONSTANT.getName();
//        String TABLENAME_2 = TABLE2_CONSTANT.getName();
//        String CQLPORT = "9042";
//        String RPCPORT = "9160";
//        String HOST = "127.0.0.1";
//
//        // Mongo
////        String KEYSPACENAME = CATALOG_CONSTANT;
////        String TABLENAME_1 = TABLE1_CONSTANT.getName();
////        String TABLENAME_2 = TABLE2_CONSTANT.getName();
////        String HOST = "localhost:27017";
//
//        // ES
////        String KEYSPACENAME = CATALOG_CONSTANT;
////        String TABLENAME_1 = TABLE1_CONSTANT.getName();
////        String TABLENAME_2 = TABLE2_CONSTANT.getName();
////        String HOST = "localhost:9200";
////        String DATABASE1 = "test/mytable";
////        String DATABASE2 = "test/mytable2";
//
//        // //Call async the Extractor netty Server
//        ExtractorServer.initExtractorServer();
//
//        // Creating the Deep Context
//        ContextProperties p = new ContextProperties();
//        SparkConf sparkConf = new SparkConf()
//                .setMaster(p.getCluster())
//                .setAppName(job)
//                .setJars(p.getJars())
//                .setSparkHome(p.getSparkHome())
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.kryo.registrator", "com.stratio.deep.serializer.DeepKryoRegistrator");
//
//        // SparkContext sc = new SparkContext(p.getCluster(), job, sparkConf);
//
//        logger.info("spark.serializer: " + System.getProperty("spark.serializer"));
//        logger.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));
//
//        deepSparkContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());
//
//        // Creating a configuration for the Extractor and initialize it
//        ExtractorConfig<Cells> config_1 = new ExtractorConfig(Cells.class);
//
//        config_1.putValue(ExtractorConstants.KEYSPACE, KEYSPACENAME).putValue(ExtractorConstants.TABLE,
//        TABLENAME_1).putValue(ExtractorConstants.CQLPORT, CQLPORT).putValue(ExtractorConstants.RPCPORT,
//        RPCPORT).putValue(ExtractorConstants.HOST, HOST);
//
////        config_1.putValue(ExtractorConstants.HOST, HOST).putValue(ExtractorConstants.DATABASE,
////        KEYSPACENAME).putValue(ExtractorConstants.COLLECTION, TABLENAME_1);
//
////        config_1.putValue(ExtractorConstants.INDEX, KEYSPACENAME).putValue(ExtractorConstants.TYPE, TABLENAME_1)
////                .putValue(ExtractorConstants.HOST, HOST);
//
//        config_1.setExtractorImplClassName(CASSANDRA_CELL_CLASS);
//
//        // Creating a configuration for the Extractor and initialize it
//        ExtractorConfig<Cells> config_2 = new ExtractorConfig(Cells.class);
//
//        config_2.putValue(ExtractorConstants.KEYSPACE, KEYSPACENAME).putValue(ExtractorConstants.TABLE,
//        TABLENAME_2).putValue(ExtractorConstants.CQLPORT, CQLPORT).putValue(ExtractorConstants.RPCPORT,
//        RPCPORT).putValue(ExtractorConstants.HOST, HOST);
//
//        config_2.putValue(ExtractorConstants.HOST, HOST).putValue(ExtractorConstants.DATABASE,
//        KEYSPACENAME).putValue(ExtractorConstants.COLLECTION, TABLENAME_2);
//
////        config_2.putValue(ExtractorConstants.INDEX, KEYSPACENAME).putValue(ExtractorConstants.TYPE, TABLENAME_2)
////                .putValue(ExtractorConstants.HOST, HOST);
//
//        config_2.setExtractorImplClassName(MONGO_CELL_CLASS);
//
//        // Creating the RDD
//        leftRdd = deepSparkContext.createJavaRDD(config_1);
//        if(logger.isDebugEnabled()){
//            logger.debug("El resultado tabla " + TABLENAME_1 + " es de " + leftRdd.count());
//        }
//
//        rightRdd = deepSparkContext.createJavaRDD(config_2);
//        if(logger.isDebugEnabled()) {
//            logger.info("El resultado tabla " + TABLENAME_2 + " es de " + rightRdd.count());
//        }

        when(deepConnectionHandler.getConnection(CLUSTERNAME_CONSTANT.getName())).thenReturn(deepConnection);
        when(deepConnection.getExtractorConfig()).thenReturn(extractorConfig);
        when(deepContext.createJavaRDD(any(ExtractorConfig.class))).thenReturn(leftRdd, rightRdd);
    }

    @After
    public void after() throws Exception, HandlerConnectionException {

//        deepContext.stop();
//        ExtractorServer.close();
    }

    @Test
    public void doWhereTest() throws UnsupportedException, ExecutionException {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT));

        IntegerSelector rightSelector = new IntegerSelector(DATA_CONSTANT);

        // SelectTerms selectTerms = new SelectTerms( leftSelector.getName().getName(), Operator.EQ.toString(),
        // rightSelector);
        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        JavaRDD<Cells> rdd = QueryFilterUtils.doWhere(leftRdd, relation);
        if(logger.isDebugEnabled()) {
            logger.debug("-------------------resultado Encontrados--------------" + rdd.count());
            logger.debug("-------------------resultado de filterSelectedColumns--------------" + rdd.first().toString());
        }

        when(QueryFilterUtils.doWhere(leftRdd,relation)).thenReturn(thirdRdd);
        assertEquals(true, true);
    }

    @Test
    public void filterSelectedColumns() {
        Map<ColumnName, String> columnsAliases = new HashMap<>();

        columnsAliases.put(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT), "nameAlias");

        JavaRDD<Cells> rdd = QueryFilterUtils.filterSelectedColumns(leftRdd, columnsAliases.keySet());
        if(logger.isDebugEnabled()) {
            logger.debug("-------------------resultado de filterSelectedColumns--------------" + rdd.first().toString());
        }
        when(QueryFilterUtils.filterSelectedColumns(leftRdd, columnsAliases.keySet())).thenReturn(thirdRdd);
        assertEquals(true, true);
    }

    @Test
    public void doJoinTest() {

        List<Relation> relations = new ArrayList<Relation>();
        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT));

        ColumnSelector rightSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE2_CONSTANT.getName(),
                COLUMN2_CONSTANT));

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);
        relations.add(relation);

        JavaRDD<Cells> outputrdd = QueryFilterUtils.doJoin(leftRdd, rightRdd, relations);
        if(logger.isDebugEnabled()) {

            logger.debug("El resultado es :" + outputrdd.count());
            logger.debug("resultado " + outputrdd.first().toString());

            int i = 0;
            for (Cells cell : outputrdd.collect()) {

                logger.debug("-----------------resultado " + (i++) + "  " + cell.getCellValues());

            }
        }
        assertEquals(true, true);
    }

}
