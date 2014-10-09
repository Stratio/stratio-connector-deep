package com.stratio.connector.deep.configuration;

import org.apache.log4j.Logger;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.IConfiguration;

/**
 * Created by dgomez on 17/09/14.
 */
public class ConnectionConfiguration implements IConfiguration {

    private static final Logger logger = Logger.getLogger(ConnectionConfiguration.class);

    private static DeepSparkContext deepContext;

    static {

        logger.info("-------------StartUp the SparkContext------------ ");

        String job = "java:deepJob";

        ContextProperties p = new ContextProperties(new String[0]);

        logger.info("spark.serializer: " + System.getProperty("spark.serializer"));
        logger.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        logger.info("-------------End StartUp the SparkContext------------ ");
    }

    public static DeepSparkContext getDeepContext() {
        return deepContext;
    }
}
