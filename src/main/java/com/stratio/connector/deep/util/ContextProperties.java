/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.connector.deep.util;

import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.utils.Constants;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.commons.lang.StringUtils.defaultIfEmpty;

/**
 * Created by dgomez on 15/09/14.
 */

public class ContextProperties {

    static final String OPT_INT = "int";

    private static final Logger LOG = Logger.getLogger(ContextProperties.class);
    /**
     * spark cluster endpoint.
     */
    private String cluster;

    /**
     * spark home
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
     * Public constructor.
     */
    public ContextProperties() {

        Properties prop = new Properties();
        InputStream input = null;

        Options options = new Options();

        options.addOption("m", "master", true, "The spark's master endpoint");
        options.addOption("h", "sparkHome", true, "/opt/stratio/deep");
        options.addOption("H", "host", true, "endpoint");

        options.addOption(OptionBuilder.hasArg().withType(Integer.class).withLongOpt("cassandraCqlPort")
                .withArgName("cassandra_cql_port").withDescription("cassandra's cql port, defaults to 9042").create());
        options.addOption(OptionBuilder.hasArg().withType(Integer.class).withLongOpt("cassandraThriftPort")
                .withArgName("cassandra_thrift_port").withDescription("cassandra's thrift port, " + "defaults to 9160")
                .create());
        options.addOption(OptionBuilder.hasArg().withValueSeparator(',').withLongOpt("jars").withArgName("jars_to_add")
                .withDescription("comma separated list of jars to add").create());
        Option help = new Option("help", "print this message");
        // options.addOption("j","jars", true, "comma separated list of jars to add");
        options.addOption(help);
        CommandLineParser parser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();

        try {

            String filename = "context.properties";
            input = getClass().getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                System.out.println("Sorry, unable to find " + filename);
                return;
            }

            prop.load(input);

            jar = prop.get("jars") != null ? ((String) prop.get("jars")).split(",") : new String[] {};
            cluster = defaultIfEmpty((String) prop.get("master"), "local");
            sparkHome = defaultIfEmpty((String) prop.get("spark.home"), "");
            host = !prop.get("host").equals("") ? (String) prop.get("host") : Constants.DEFAULT_CASSANDRA_HOST;
            port = !prop.get("port").equals("") ? Integer.valueOf((String) prop.get("port"))
                    : Constants.DEFAULT_CASSANDRA_CQL_PORT;
            thriftPort = !prop.get("thriftPort").equals("") ? Integer.valueOf((String) prop.get("thriftPort"))
                    : Constants.DEFAULT_CASSANDRA_RPC_PORT;

        } catch (IOException e) {
            formatter.printHelp("", options);
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
}
