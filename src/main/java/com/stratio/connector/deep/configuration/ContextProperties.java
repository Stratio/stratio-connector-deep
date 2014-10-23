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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Charge the properties to up the Deep spark context.
 *
 *
 */

public class ContextProperties {

    static final String OPT_INT = "int";

    private static final Logger LOG = Logger.getLogger(ContextProperties.class);

    /*
    * spark config propertes.
    *
    */
    private Properties prop;

    /**
     * spark cluster endpoint.
     */
    private String cluster;

    /**
     * spark home.
     */
    private String sparkHome;

    /**
     * The jar to be added to the spark context.
     */
    private String[] jar;

    /**
     * Endpoint of the Extractor cluster against which the examples will be run. Defaults to 'localhost'.
     */
    private String host;

    /**
     * Extractor's port. Defaults Extractor Cassandra to 9042.
     */
    private int port;

    /**
     * Cassandra's cql port. Defaults to 9160.
     */
    private int thriftPort;

    /**
     * Public constructor. Load the context.properties.
     */
    public ContextProperties() {

        prop = new Properties();
        InputStream input = null;

        try {

            String filename = "context.properties";
            input = getClass().getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                LOG.error("Sorry, unable to find " + filename);
                return;
            }

            prop.load(input);

            jar = prop.get("jars") != null ? ((String) prop.get("jars")).split(",") : new String[] {};

            cluster = prop.get("spark.master") != null ? (String)prop.get("spark.master") : "local";
            sparkHome =  prop.get("spark.home") != null ? (String)prop.get("spark.home") : "";
            host = !prop.get("host").equals("") ? (String) prop.get("host") : ExtractorConnectConstants.DEFAULT_HOST;
            port = !prop.get("port").equals("") ? Integer.valueOf((String) prop.get("port"))
                    : ExtractorConnectConstants.DEFAULT_PORT;
            thriftPort = !prop.get("thriftPort").equals("") ? Integer.valueOf((String) prop.get("thriftPort"))
                    : ExtractorConnectConstants.DEFAULT_RPC_PORT;

        } catch (IOException e) {

            LOG.error("Unexpected exception: ", e);
        }
    }

    public String getCluster() {
        return cluster;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String[] getJars() {
        return jar;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public Properties getProp() {
        return prop;
    }

}
