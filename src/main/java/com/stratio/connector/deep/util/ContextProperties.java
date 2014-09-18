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

import static org.apache.commons.lang.StringUtils.defaultIfEmpty;

/**
 * Created by dgomez on 15/09/14.
 */

public class ContextProperties {


    static final String OPT_INT           = "int";

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
    public ContextProperties(String[] args) {
        Options options = new Options();

        options.addOption("m","master", true, "The spark's master endpoint");
        options.addOption("h","sparkHome", true, "The spark's home, eg. /opt/spark");
        options.addOption("H","host", true, "endpoint");

        options.addOption(OptionBuilder.hasArg().withType(Integer.class).withLongOpt("cassandraCqlPort").withArgName("cassandra_cql_port").withDescription("cassandra's cql port, defaults to 9042").create());
        options.addOption(OptionBuilder.hasArg().withType(Integer.class).withLongOpt("cassandraThriftPort")
                .withArgName("cassandra_thrift_port").withDescription("cassandra's thrift port, " +
                        "defaults to 9160").create());
        options.addOption(OptionBuilder.hasArg().withValueSeparator(',').withLongOpt("jars")
                .withArgName("jars_to_add").withDescription("comma separated list of jars to add").create());
        Option help = new Option( "help", "print this message" );
        //options.addOption("j","jars", true, "comma separated list of jars to add");
        options.addOption(help);
        CommandLineParser parser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine line = parser.parse( options, args );

            if (line.hasOption("help")){
                formatter.printHelp( "", options );

                throw new DeepGenericException("Help command issued");
            }

            jar = (line.hasOption("jars") ? line.getOptionValues("jars") : new String[]{});
            cluster = line.getOptionValue ("master", defaultIfEmpty(System.getProperty("spark.master"), "local"));
            sparkHome = line.getOptionValue ("sparkHome", defaultIfEmpty(System.getProperty("spark.home"), ""));
            host = line.getOptionValue ("host", Constants.DEFAULT_CASSANDRA_HOST);
            port = line.hasOption ("port") ? Integer.parseInt(line.getOptionValue("port")): Constants.DEFAULT_CASSANDRA_CQL_PORT;
            thriftPort = line.hasOption ("thriftPort") ? Integer.parseInt(line.getOptionValue("cassandraThriftPort")): Constants.DEFAULT_CASSANDRA_RPC_PORT;

        } catch (ParseException e) {
            formatter.printHelp( "", options );
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
