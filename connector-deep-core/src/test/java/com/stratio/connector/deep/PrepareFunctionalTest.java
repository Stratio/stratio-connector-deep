package com.stratio.connector.deep;

import static com.stratio.deep.commons.utils.Utils.quote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.io.Resources;

public class PrepareFunctionalTest implements CommonsPrepareTestQuerys{

    private static final Logger logger = Logger.getLogger(PrepareFunctionalTest.class);

    public static final String HOST = "127.0.0.1";
    public static final String KEYSPACE = "functionaltest";
    public static final String TABLE = "songs";


    public static void main(String[] args){

        Cluster cluster1 = Cluster.builder().addContactPoints(HOST).build();

        Session session = cluster1.connect();

        session.execute(String.format(DROP_KEYSPACE,KEYSPACE));

        session.execute(String.format(CREATE_KEYSPACE,KEYSPACE));

        session = cluster1.connect(KEYSPACE);

        session.execute(
                "CREATE TABLE "+KEYSPACE+"."+TABLE+" (" +
                        "id int PRIMARY KEY," +
                        "artist text," +
                        "title text," +
                        "year  text," +
                        "length  text," +
                        "description text" +
                        ");");

        String uuid= UUID.randomUUID().toString();

        buildTestDataInsertBatch(session);

        session.execute(String.format(DROP_KEYSPACE,KEYSPACE));

        session.close();
    }


    protected static String buildTestDataInsertBatch(Session session) {

        URL testData = Resources.getResource("songs.csv");

        String batch = "BEGIN BATCH \n";
        java.util.List<String> inserts = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
                new File(testData.toURI()))))) {
            String line;


            while ((line = br.readLine()) != null) {
                String[] fields = (quote("songs") + ","+line).split(",");
                String insert = String.format(rawInsert, (Object[]) fields);
                logger.debug("INSERT---->"+insert);
                session.execute(insert);
            }
        } catch (Exception e) {
            logger.error("Error", e);
        }

        if (inserts.size() > 0) {
            for (String insert : inserts) {
                batch += insert;
            }
        }
        batch += " APPLY BATCH; ";

        return batch;
    }


}
