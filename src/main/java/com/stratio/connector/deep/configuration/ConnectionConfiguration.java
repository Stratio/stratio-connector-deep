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

import java.util.Properties;

import org.apache.log4j.Logger;

import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * Created by dgomez on 17/09/14.
 */
public class ConnectionConfiguration implements IConfiguration {

    private static final Logger LOG = Logger.getLogger(ConnectionConfiguration.class);

    private static DeepSparkContext deepContext;

    private static Properties configProperties;

    static {

        LOG.info("-------------StartUp the SparkContext------------ ");

        String job = "java:deepJob";

        ContextProperties p = new ContextProperties();

        LOG.info("spark.serializer: " + System.getProperty("spark.serializer"));
        LOG.info("spark.kryo.registrator: " + System.getProperty("spark.kryo.registrator"));

        deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

        configProperties = p.getProp();
        LOG.info("-------------End StartUp the SparkContext------------ ");
    }

    private ConnectionConfiguration() {
    }

    public static DeepSparkContext getDeepContext() {
        return deepContext;
    }

    public static Properties getConfigProperties() {
        return configProperties;
    }
}
