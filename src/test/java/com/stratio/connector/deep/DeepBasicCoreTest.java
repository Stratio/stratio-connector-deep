package com.stratio.connector.deep;

import com.datastax.driver.core.Session;
import com.stratio.deep.core.context.DeepSparkContext;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;

/**
 * Created by dgomez on 15/09/14.
 */
public class DeepBasicCoreTest {

    private static final Logger logger = Logger.getLogger(DeepBasicCoreTest.class);

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
        //TODO loadTestData
    }

}
