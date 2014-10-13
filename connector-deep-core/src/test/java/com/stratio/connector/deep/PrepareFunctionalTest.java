package com.stratio.connector.deep;

import static com.stratio.deep.commons.utils.Utils.quote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.io.Resources;

public class PrepareFunctionalTest implements CommonsPrepareTestData {

    private static final Logger logger = Logger.getLogger(PrepareFunctionalTest.class);

    public static final String HOST = "127.0.0.1";
    public static final String KEYSPACE = "functionaltest";
    public static final String TABLE_1 = "songs";
    public static final String TABLE_2 = "artists";
    public static Cluster cluster1 = Cluster.builder().addContactPoints(HOST).build();

    public static Session session = cluster1.connect();

    public static void prepareDataForTest(){
    //public static void main(String args[]){


        session.execute(String.format(DROP_KEYSPACE,KEYSPACE));

        session.execute(String.format(CREATE_KEYSPACE,KEYSPACE));

        session = cluster1.connect(KEYSPACE);

        session.execute(
                "CREATE TABLE "+KEYSPACE+"."+TABLE_1+" (" +
                        "id int PRIMARY KEY," +
                        "artist text," +
                        "title text," +
                        "year  text," +
                        "length  text," +
                        "description text" +
                        ");");

        session.execute(
                "CREATE TABLE "+KEYSPACE+"."+TABLE_2+" (" +
                        "id int PRIMARY KEY," +
                        "artist text," +
                        "age text" +
                        ");");


        buildTestDataInsertBatch(session,TABLE_1,TABLE_2);


    }

    public static void clearData(){

        session.execute(String.format(DROP_KEYSPACE,KEYSPACE));

        session.close();
    }

    protected static Boolean buildTestDataInsertBatch(Session session, String ... csvOrigin) {

        for(String origin : csvOrigin) {

            URL testData = Resources.getResource(origin+".csv");

            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
                    new File(testData.toURI()))))) {
                String line;

                while ((line = br.readLine()) != null) {
                    String[] fields = (quote(origin) + "," + line).split(",");
                    String insert = origin.equals(TABLE_1)? String.format(rawSongsInsert,
                            (Object[]) fields):String.format(rawArtistsInsert, (Object[]) fields);
                    logger.debug("INSERT---->" + insert);
                    session.execute(insert);
                }
            } catch (Exception e) {
                logger.error("Error", e);
            }
        }

//        try {
//            //Wait 6 secons...change to 60
//           // Thread.sleep(6000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        return true;
    }


}
