package com.stratio.connector.deep.configuration;

import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

/**
 * Created by dgomez on 17/09/14.
 */
public class ExtractorConnectConstants extends ExtractorConstants {

    public final static String HOSTS      = "Hosts";
    public final static String PORTS      = "Port";
    public final static String PORT       = "Port";
    public final static String INNERCLASS = "implClass";

    public static final String DB_CASSANDRA     = "cassandra";
    public static final String DB_ELASTICSEARCH = "elasticsearch";
    public static final String DB_MONGO         = "mongo";

    public static final String DEFAULT_HOST   = "localhost";
    public static final int DEFAULT_RPC_PORT  = 9160;
    public static final int DEFAULT_PORT      = 9042;

    public final static String CQLPORT = "cqlPort";
    public final static String RCPPORT = "rcpPort";

    public static final String DEFAULT_MONGO_HOST = "localhost:27017";

    public static final int DEFAULT_BATCH_SIZE    = 100;
    public static final int DEFAULT_PAGE_SIZE     = 1000;
    public static final int DEFAULT_MAX_PAGE_SIZE = 10000;

    public static final int DEFAULT_BISECT_FACTOR = 1;

    //CONSTANTS IN THE context.properties
    public static final String SPARK_SERIALIZER   = "spark.serializer";
    public static final String SPARK_KRYO_REGISTRATOR = "spark.kryo.registrator";
    public static final String SPARK_MASTER       = "spark.master";
    public static final String SPARK_HOME         = "spark.home";
    public static final String SPARK_HOST         = "host";
    public static final String SPARK_PORT         = "port";
    public static final String SPARK_THRIFTPORT   = "thriftPort";
    public static final String SPARK_JARS         = "jars";
    public static final String SPARK_PARTITION_ID = "spark.partition.id";
    public static final String SPARK_RDD_ID       = "spark.rdd.id";

    public static final String METHOD_NOT_SUPPORTED = "Not supported";
}
