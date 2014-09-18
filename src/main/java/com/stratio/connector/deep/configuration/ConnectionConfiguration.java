package com.stratio.connector.deep.configuration;

import com.stratio.connector.deep.util.ContextProperties;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by dgomez on 17/09/14.
 */
public class ConnectionConfiguration implements IConfiguration{

    private static final Logger logger = Logger.getLogger(ConnectionConfiguration.class);

    private static DeepSparkContext deepContext;

    private static SparkContext sparkContext;


    static {

        logger.info("-------------StartUp the SparkContext------------ ");
        logger.info("-------------StartUp the SparkContext------------ ");
        logger.info("-------------StartUp the SparkContext------------ ");

        String [] args = new String [0];
        String job = "java:testJob_1";

        ContextProperties p = new ContextProperties(args);

        SparkConf sparkConf = new SparkConf()
                .setMaster(p.getCluster())
                .setAppName(job)
                .setJars(p.getJars())
                .setSparkHome(p.getSparkHome())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "com.stratio.deep.serializer.DeepKryoRegistrator");

        SparkContext sc = new SparkContext(p.getCluster(), job, sparkConf);

        logger.info("spark.serializer: "       + System.getProperty("spark.serializer"));
        logger.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        logger.info("-------------End StartUp the SparkContext------------ ");
        logger.info("-------------End StartUp the SparkContext------------ ");
        logger.info("-------------End StartUp the SparkContext------------ ");
    }

    public static DeepSparkContext getDeepContext() {
        return deepContext;
    }

    public static SparkContext getSparkContext() {
        return sparkContext;
    }

}
