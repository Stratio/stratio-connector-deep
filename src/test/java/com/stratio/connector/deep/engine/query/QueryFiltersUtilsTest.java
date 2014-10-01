package com.stratio.connector.deep.engine.query;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.configuration.ContextProperties;
import com.stratio.deep.commons.config.DeepJobConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.server.ExtractorServer;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * Created by dgomez on 30/09/14.
 */
@RunWith(PowerMockRunner.class)
public class QueryFiltersUtilsTest {


    private static final Logger logger = Logger.getLogger(QueryFiltersUtilsTest.class);

    @Before
    public void before() throws Exception, HandlerConnectionException {
        String job = "java:creatingCellRDD";

        String KEYSPACENAME = "test";
        String TABLENAME = "tweets";
        String CQLPORT = "9042";
        String RPCPORT = "9160";
        String HOST = "127.0.0.1";

        // //Call async the Extractor netty Server
        ExtractorServer.initExtractorServer();

        // Creating the Deep Context
        ContextProperties p = new ContextProperties();
        SparkConf sparkConf = new SparkConf()
                .setMaster(p.getCluster())
                .setAppName(job)
                .setJars(p.getJars())
                .setSparkHome(p.getSparkHome())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "com.stratio.deep.serializer.DeepKryoRegistrator");

        SparkContext sc = new SparkContext(p.getCluster(), job, sparkConf);

        logger.info("spark.serializer: " + System.getProperty("spark.serializer"));
        logger.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        DeepSparkContext deepContext = new DeepSparkContext(sc);

        // Creating a configuration for the Extractor and initialize it
        DeepJobConfig<Cells> config = new DeepJobConfig<Cells>(new ExtractorConfig(Cells.class));



        Map<String, Serializable> values = new HashMap<String, Serializable>();
        values.put(ExtractorConstants.KEYSPACE, KEYSPACENAME);
        values.put(ExtractorConstants.TABLE, TABLENAME);
        values.put(ExtractorConstants.CQLPORT, CQLPORT);
        values.put(ExtractorConstants.RPCPORT, RPCPORT);
        values.put(ExtractorConstants.HOST, HOST);

        config.setValues(values);

        // Creating the RDD
        JavaRDD<Cells> rdd = deepContext.createJavaRDD(config);

        rdd.count();
    }

    @Test
    public void doWhereTest(){
        assertEquals(true,true);
    }

    @Test
    public void doJoinTest(){
        assertEquals(true,true);
    }

}
