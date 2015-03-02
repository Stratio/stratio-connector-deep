package com.stratio.connector.deep;

import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * This class must build the deep configuration.
 * Created by jmgomez on 25/02/15.
 */
public class DeepConfigurationBuilder {


    /**
     * Constructor.
     * @throws InitializationException
     */
    public DeepConfigurationBuilder() throws InitializationException {
        // Retrieving configuration
        InputStream input = DeepConnector.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILE_CONSTANT);

        if (input == null) {
            String message = "Sorry, unable to find [" + CONFIGURATION_FILE_CONSTANT + "]";
            LOGGER.error(message);
            throw new InitializationException(message);
        }
    }


    /**
     * Return the HDFS path.
     * @return the HDFS path.
     */
    public String getHDFSFilePath(){
       return connectorConfig.getConfig(ExtractorConstants.HDFS).getString(
                ExtractorConstants.FS_FILE_PATH);
    }
    /**
     * The log.
     */
    private static transient final Logger LOGGER = LoggerFactory.getLogger(DeepConnector.class);

    /**
     * The configuration file name.
     */
    private static final String CONFIGURATION_FILE_CONSTANT = "connector-application.conf";


    /**
     * Connector configuration from the properties file.
     */
    private Config connectorConfig = ConfigFactory.load(CONFIGURATION_FILE_CONSTANT);


    /**
     * This method return the implementation class.
     * @param dataSourceName the datasource.
     * @return
     */
    public String getImplementationClass(String dataSourceName){
       return connectorConfig.getConfig(DeepConnectorConstants.CLUSTER_PREFIX_CONSTANT)
                .getString(dataSourceName + DeepConnectorConstants.IMPL_CLASS_SUFIX_CONSTANT);
    }

    /**
     * This method must built the DeepSparkConf.
     * @return the DeepSparkConf.
     */
    public DeepSparkConfig createDeepSparkConf() {
        String sparkMaster = connectorConfig.getString(DeepConnectorConstants.SPARK_MASTER);

        String sparkHome = connectorConfig.getString(DeepConnectorConstants.SPARK_HOME);
        String sparkDriverMemory = connectorConfig.getString(DeepConnectorConstants.SPARK_DRIVER_MEMORY);
        String sparkExecutorMemory = connectorConfig.getString(DeepConnectorConstants.SPARK_EXECUTOR_MEMORY);
        String sparkTaskCpus = connectorConfig.getString(DeepConnectorConstants.SPARK_TASK_CPUS);
        String spatkDefaultParalelism = connectorConfig.getString(DeepConnectorConstants.SPARK_DEFAULT_PARALELISM);
        String spatkCoresMax = connectorConfig.getString(DeepConnectorConstants.SPARK_CORES_MAX);
        String spatkResultSize = connectorConfig.getString(DeepConnectorConstants.SPARK_DRIVER_RESULTSIZE);
        String spatkTaskCpu = connectorConfig.getString(DeepConnectorConstants.SPARK_TASK_CPU);


        String[] jarsArray = setJarPath();

        LOGGER.info("Creating DeepSparkContest");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("spark.serializer: [" + System.getProperty("spark.serializer")+"]");
            LOGGER.debug("spark.kryo.registrator: [" + System.getProperty("spark.kryo.registrator")+"]");
            LOGGER.debug("spark.executor.memory [" +sparkExecutorMemory+"]");
            LOGGER.debug("spark.driver.memory [" +sparkDriverMemory+"]");
            LOGGER.debug(DeepConnectorConstants.SPARK_DEFAULT_PARALELISM +" [" +spatkDefaultParalelism+"]");
            LOGGER.debug(DeepConnectorConstants.SPARK_CORES_MAX +" [" +spatkCoresMax+"]");
            LOGGER.debug(DeepConnectorConstants.SPARK_DRIVER_RESULTSIZE +" [" +spatkResultSize+"]");
            LOGGER.debug(DeepConnectorConstants.SPARK_TASK_CPU +" [" +spatkTaskCpu+"]");
            LOGGER.debug("spark.task.cpus [" +sparkTaskCpus+"]");
            LOGGER.debug("SPARK-Master [" + sparkMaster + "]");
            LOGGER.debug("SPARK-Home   [" + sparkHome + "]");
            LOGGER.debug("Jars "+ Arrays.toString(jarsArray));

        }


        DeepSparkConfig sparkConf = new DeepSparkConfig();
        sparkConf.setSparkHome(sparkHome);
        sparkConf.setMaster(sparkMaster);
        sparkConf.setAppName(DeepConnectorConstants.DEEP_CONNECTOR_JOB_CONSTANT);
        sparkConf.setJars(jarsArray);
        sparkConf.set(DeepConnectorConstants.SPARK_EXECUTOR_MEMORY, sparkExecutorMemory);
        sparkConf.set(DeepConnectorConstants.SPARK_DRIVER_MEMORY, sparkDriverMemory);
        sparkConf.set(DeepConnectorConstants.SPARK_TASK_CPUS, sparkTaskCpus);
        sparkConf.set(DeepConnectorConstants.SPARK_DEFAULT_PARALELISM, spatkDefaultParalelism);
        sparkConf.set(DeepConnectorConstants.SPARK_CORES_MAX, spatkCoresMax);
        sparkConf.set(DeepConnectorConstants.SPARK_DRIVER_RESULTSIZE, spatkResultSize);
        sparkConf.set(DeepConnectorConstants.SPARK_TASK_CPU, spatkTaskCpu);
        return sparkConf;
    }



    private String[] setJarPath() {

        String[] jarsArray = new String[0];

        try {


            List<String> sparkJars = connectorConfig.getStringList(DeepConnectorConstants.SPARK_JARS);
            jarsArray = new String[sparkJars.size()];
            sparkJars.toArray(jarsArray);

        } catch (ConfigException e) {
            LOGGER.info("--No spark Jars added--", e);
        }
        return jarsArray;
    }
}
