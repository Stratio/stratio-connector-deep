package com.stratio.connector.deep.configuration;

/**
 * Created by dgomez on 30/09/14.
 */
public final class Constants {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_RPC_PORT = 9160;
    public static final int DEFAULT_PORT = 9042;

    public static final String DEFAULT_MONGO_HOST = "localhost:27017";

    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final int DEFAULT_PAGE_SIZE = 1000;
    public static final int DEFAULT_MAX_PAGE_SIZE = 10000;

    public static final int DEFAULT_BISECT_FACTOR = 1;

    public static final String SPARK_PARTITION_ID = "spark.partition.id";

    public static final String SPARK_RDD_ID = "spark.rdd.id";
}
