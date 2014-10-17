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

package com.stratio.connector.deep.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Created by dgomez on 15/09/14.
 */

public class ClusterProperties {

    static final String OPT_INT = "int";

    private static final Logger LOG = Logger.getLogger(ClusterProperties.class);

    private final Properties clusterProperties = new Properties();

    /**
     * Public constructor.
     */
    public ClusterProperties() {

        InputStream input = null;

        try {

            String filename = "context.properties";
            input = getClass().getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                System.out.println("Sorry, unable to find " + filename);
                return;
            }

            clusterProperties.load(input);

        } catch (IOException e) {

            LOG.error("Unexpected exception: ", e);
        }
    }

    public String getValue(String key) {

        return (String) clusterProperties.get(key);
    }
}
