package com.stratio.connector.deep.connection;


import com.stratio.connector.deep.util.ContextProperties;
import com.stratio.deep.core.context.DeepSparkContext;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;

/**
 * Created by dgomez on 15/09/14.
 */
public class DeepContextConnectorTest {

    private static final Logger logger = Logger.getLogger(DeepContextConnectorTest.class);

    /**
     * Default Deep HOST using 127.0.0.1.
     */
    private static final String DEFAULT_HOST = "127.0.0.1";


    /**
     *
     */
    protected static DeepSparkContext context = null;


    @BeforeClass
    public static void setUp(){
        //TODO  Start All the DeepSparkContext
        // Creating the Deep Context
        String job = "java:deepSparkContext";
        String [] args = null;
        ContextProperties p = new ContextProperties(args);
        context = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());


        //TODO loadTestData
    }


    /**
     * Establish the connection with DeepSparkContext in order to be able to retrieve metadata from the
     * system columns with the connection config.
     *
     * @param host The target host.
     * @return Whether the connection has been established or not.
     */

    protected static boolean connect(String host) {
        boolean result = false;

        return result;
    }

}
