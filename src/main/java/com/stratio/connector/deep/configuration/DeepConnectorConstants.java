/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.deep.configuration;

/**
 * Config constants.
 */

public class DeepConnectorConstants {

    public static final String HOSTS = "Hosts";
    public static final String PORTS = "Port";

    public static final String HDFS = "hdfs";

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_RPC_PORT = 9160;
    public static final int DEFAULT_PORT = 9042;

    public static final String CQLPORT = "cqlPort";
    public static final String RCPPORT = "rcpPort";

    public static final String DEFAULT_MONGO_HOST = "localhost:27017";

    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_PAGE_SIZE = 1000;
    public static final int DEFAULT_MAX_PAGE_SIZE = 10000;

    public static final int DEFAULT_BISECT_FACTOR = 1;

    // CONSTANTS IN THE context.properties
    public static final String SPARK_SERIALIZER = "spark.serializer";
    public static final String SPARK_KRYO_REGISTRATOR = "spark.kryo.registrator";
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_HOME = "spark.home";
    public static final String SPARK_HOST = "host";
    public static final String SPARK_PORT = "port";
    public static final String SPARK_THRIFTPORT = "thriftPort";
    public static final String SPARK_JARS = "jars";
    public static final String SPARK_PARTITION_ID = "spark.partition.id";
    public static final String SPARK_RDD_ID = "spark.rdd.id";

    public static final String METHOD_NOT_SUPPORTED = "Not supported";
    public static final String TYPE = "type";

    public static final String CLUSTER_PREFIX_CONSTANT = "cluster.";
    public static final String IMPL_CLASS_SUFIX_CONSTANT = ".implClass";

    public static final String EXTRACTOR_IMPL_CLASS = "extractorImplClass";

    public static final String DEEP_CONNECTOR_JOB_CONSTANT = "DeepConnectorJob";


    public static final String ES_PORTS   ="Restful Ports";
}
