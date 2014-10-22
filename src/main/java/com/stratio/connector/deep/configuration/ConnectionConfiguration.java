package com.stratio.connector.deep.configuration;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.crossdata.common.connector.IConfiguration;

/**
 * Created by dgomez on 17/09/14.
 */
public class ConnectionConfiguration implements IConfiguration {

    private static final Logger logger = Logger.getLogger(ConnectionConfiguration.class);

    private static DeepSparkContext deepContext;

    private static Properties configProperties;

    static {

        logger.info("-------------StartUp the SparkContext------------ ");

        String job = "java:deepJob";

        ContextProperties p = new ContextProperties();


        logger.info("spark.serializer: " + System.getProperty("spark.serializer"));
        logger.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        configProperties = p.getProp();
        logger.info("-------------End StartUp the SparkContext------------ ");
    }

    public static DeepSparkContext getDeepContext() {
        return deepContext;
    }

    public static Properties getConfigProperties() {
        return configProperties;
    }
}
