package com.stratio.connector.deep.configuration;

import com.stratio.deep.commons.extractor.utils.ExtractorConstants;

/**
 * Created by dgomez on 17/09/14.
 */
public class ExtractorConnectConstants extends ExtractorConstants {

    public final static String HOSTS      = "hosts";
    public final static String PORTS      = "ports";
    public final static String INNERCLASS = "implClass";

    public static final String db_cassandra     = "cassandra";
    public static final String db_elasticsearch = "elasticsearch";
    public static final String db_mongo         = "mongo";

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

    public static final String SPARK_PARTITION_ID = "spark.partition.id";

    public static final String SPARK_RDD_ID       = "spark.rdd.id";

}
