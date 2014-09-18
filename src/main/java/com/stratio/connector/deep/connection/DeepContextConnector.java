package com.stratio.connector.deep.connection;


import com.stratio.connector.ConnectorApp;
import com.stratio.connector.deep.configuration.ConnectionConfiguration;

import com.stratio.connector.deep.configuration.ExtractorConnectConstants;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;

import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by dgomez on 16/09/14.
 */
public class DeepContextConnector implements IConnector {


    private Map<String, ExtractorConfig> extractorClusters;

    /**
     * The connectionHandler.
     */
    private DeepConnectionHandler connectionHandler = null;


    DeepSparkContext deepContext = ConnectionConfiguration.getDeepContext();


    final Logger logger = LoggerFactory.getLogger(this.getClass());


    public static void main(String[] args) {

        ConnectorApp connectorApp = new ConnectorApp();

        DeepContextConnector conn = new DeepContextConnector();
        connectorApp.startup(conn);


    }


    @Override
    public String getConnectorName() {
        return "DeepConnector";
    }

    @Override
    public String[] getDatastoreName() {
        return new String[] {"DeepConnector"};
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {



    }

    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {

        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getOptions();

        // Creating a configuration for the Extractor and initialize it
        ExtractorConfig<Cells> extractorconfig = new ExtractorConfig();


        Map<String, String> values = new HashMap<String, String>();

        values.put(ExtractorConnectConstants.PORT,  clusterOptions.get("Port"));
        String[] hosts =   clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1).split(",");

        values.put(ExtractorConnectConstants.HOST, hosts[0] );
        values.put(ExtractorConnectConstants.HOSTS, clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1) );

        extractorconfig.setValues(values);

        extractorClusters.put(clusterName.getName(), extractorconfig);


    }

    @Override
    public void close(ClusterName name) throws ConnectionException {

        if(extractorClusters.containsValue(name.getName())){

            extractorClusters.remove(name.getName());

        }
    }

    @Override
    public boolean isConnected(ClusterName name) {

        boolean connected = false;

        if(extractorClusters.containsValue(name.getName())) {

            RDD rdd = deepContext.createRDD(extractorClusters.get(name.getName()));
            rdd.count();
            connected=true;

        }else{
            connected = false;
        }
        return connected;
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {

        //TODO throw new UnsupportedException("");
        return null;

    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return null;
    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {

        //TODO throw new UnsupportedException("");
        return null;
    }
}
