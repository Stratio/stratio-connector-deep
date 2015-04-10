package com.stratio.connector.deep;

import com.stratio.connector.deep.configuration.DeepConnectorConstants;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkConfig;
import com.typesafe.config.Config;
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

        DeepSparkConfig deepSparkConf = createDeepSpakConfConfigured(new Configuration(connectorConfig));

        return deepSparkConf;
    }

    /**
     * Create the deepSparConf·
     * @param configuration the configuration.
     * @return a DeepSparkConfig configured.
     */
    private DeepSparkConfig createDeepSpakConfConfigured(Configuration configuration) {
        DeepSparkConfig deepSparkConf = new DeepSparkConfig();
        String sparkHome = configuration.sparkHome;
        if (sparkHome !=null && !sparkHome.isEmpty()) {
            deepSparkConf.setSparkHome(sparkHome);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Property sparkHome has been added to DeepSparkContext with value %s", sparkHome));
            }

        }else{
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Property sparkHome is void. It doesn't  add to DeepSparkContext");
            }

        }
        String sparkMaster = configuration.sparkMaster;
        if (sparkMaster !=null && !sparkMaster.isEmpty()) {
            deepSparkConf.setMaster(sparkMaster);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Property sparkMaster has been added to DeepSparkContext with value %s", sparkMaster));
            }
        }else{
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Property sparkMaster is void. It doesn't  add to DeepSparkContext");
            }

        }
        deepSparkConf.setAppName(DeepConnectorConstants.DEEP_CONNECTOR_JOB_CONSTANT);

        String[] jarsArray = configuration.jarsArray;
        if (jarsArray !=null && jarsArray.length!=0) {
            deepSparkConf.setJars(jarsArray);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Property jarsArray has been added to DeepSparkContext with value %s", Arrays.deepToString(jarsArray)));
            }
        }else{
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Property jarsArray is void. It doesn't  add to DeepSparkContext");
            }
        }
        addPropertyToDeepSparConfig(deepSparkConf,DeepConnectorConstants.SPARK_EXECUTOR_MEMORY, configuration.sparkExecutorMemory);
        addPropertyToDeepSparConfig(deepSparkConf, DeepConnectorConstants.SPARK_DRIVER_MEMORY, configuration.sparkDriverMemory);
        addPropertyToDeepSparConfig(deepSparkConf, DeepConnectorConstants.SPARK_TASK_CPUS, configuration.sparkTaskCpus);
        addPropertyToDeepSparConfig(deepSparkConf, DeepConnectorConstants.SPARK_DEFAULT_PARALELISM, configuration.spatkDefaultParalelism);
        addPropertyToDeepSparConfig(deepSparkConf, DeepConnectorConstants.SPARK_CORES_MAX, configuration.spatkCoresMax);
        addPropertyToDeepSparConfig(deepSparkConf,DeepConnectorConstants.SPARK_DRIVER_RESULTSIZE, configuration.spatkResultSize);
        return deepSparkConf;
    }


    /**
     * Add the property to DeepSparkContext if the property is not null.
     * @param deepSparkConf DeepSparkContext
     * @param value the proper tyValue.
     * @param property the property.
     */
    private void addPropertyToDeepSparConfig(DeepSparkConfig deepSparkConf, String property, String value){
        if (value!=null && !value.isEmpty()) {
            deepSparkConf.set(property, value);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Property %s has been added to DeepSparkContext with value %s", property,value));
            }
        }else{
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Property %s is void. It doesn't  add to DeepSparkContext", property));
            }
        }
    }

}

    class Configuration{

        /**
         * The log.
         */
        private static transient final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

          String sparkMaster;
          String sparkHome;
          String sparkDriverMemory;
          String sparkExecutorMemory;
          String sparkTaskCpus;
          String spatkDefaultParalelism;
          String spatkCoresMax;
          String spatkResultSize;
          String[] jarsArray;
         Config connectorConfig;
        public Configuration(Config connectorConfig){
            this.connectorConfig = connectorConfig;

             sparkMaster = getProperty(DeepConnectorConstants.SPARK_MASTER);
             sparkHome = getProperty(DeepConnectorConstants.SPARK_HOME);
             sparkDriverMemory = getProperty(DeepConnectorConstants.SPARK_DRIVER_MEMORY);
             sparkExecutorMemory = getProperty(DeepConnectorConstants.SPARK_EXECUTOR_MEMORY);
             sparkTaskCpus = getProperty(DeepConnectorConstants.SPARK_TASK_CPUS);
             spatkDefaultParalelism = getProperty(DeepConnectorConstants.SPARK_DEFAULT_PARALELISM);
             spatkCoresMax = getProperty(DeepConnectorConstants.SPARK_CORES_MAX);
             spatkResultSize = getProperty(DeepConnectorConstants.SPARK_DRIVER_RESULTSIZE);
             jarsArray = setJarPath();
        }


        /**
         * Return a property.
         *
         * @param  property the property
         * @return a property value or a empty string if no property exists.
         */
        private String getProperty(String property){
            String value ="";
            if (connectorConfig.hasPath(property)){
                value = connectorConfig.getString(property);
                LOGGER.info(String.format("property [%s] has value [%s]",property,value));
            }else{
                LOGGER.info(String.format("No property [%s] added",property,value));
            }

            return value;
        }

        /**
         * recovered the jarPath.
         * @return the jarPath.
         */
        private String[] setJarPath() {

            String[] jarsArray = new String[0];

            if (connectorConfig.hasPath(DeepConnectorConstants.SPARK_JARS)) {
                List<String> sparkJars = connectorConfig.getStringList(DeepConnectorConstants.SPARK_JARS);
                jarsArray = new String[sparkJars.size()];
                sparkJars.toArray(jarsArray);
                LOGGER.info(String.format("property jars has value [%s]",Arrays.deepToString(jarsArray)));

            }else {
                LOGGER.info("--No spark Jars added--");
            }


            return jarsArray;

        }
}
