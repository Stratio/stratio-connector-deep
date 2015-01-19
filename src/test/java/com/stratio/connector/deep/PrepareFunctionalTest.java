package com.stratio.connector.deep;

import static com.stratio.deep.commons.utils.Utils.quote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.io.Resources;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class PrepareFunctionalTest implements CommonsPrepareTestData {

    private static final Logger logger = Logger.getLogger(PrepareFunctionalTest.class);

    public static final Integer ES_NATIVE_PORT = 9300;
    public static final String KEYSPACE = "functionaltest";
    public static final String TABLE_1 = "songs";
    public static final String TABLE_2 = "artists";
    public static Cluster cluster1 = Cluster.builder().addContactPoints(CassandraConnectionConfigurationBuilder.HOST)
                    .build();

    public static Session session;
    /**
     * The Mongo client.
     */
    public static MongoClient mongoClient;
    /**
     * The Elasticsearch client.
     */
    public static Client elasticClient;
    /**
     * The elasticsearch node connection.
     */
    public static Node node;

    public static void prepareDataForES() {

        elasticClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "statioESCluster")
                        .build()).addTransportAddress(new InetSocketTransportAddress("10.200.0.58", ES_NATIVE_PORT))
                        .addTransportAddress(new InetSocketTransportAddress("10.200.0.59", ES_NATIVE_PORT))
                        .addTransportAddress(new InetSocketTransportAddress("10.200.0.60", ES_NATIVE_PORT))
                        .addTransportAddress(new InetSocketTransportAddress("10.200.0.61", ES_NATIVE_PORT))
                        .addTransportAddress(new InetSocketTransportAddress("10.200.0.62", ES_NATIVE_PORT));
        // ClusterStatsResponse actionGet = elasticClient.admin().cluster().prepareClusterStats().execute().actionGet();
        // actionGet.

        buildTestESDataInsertBatch(TABLE_1, TABLE_2);

        elasticClient.close();
    }

    public static void prepareDataForMongo() {

        // To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
        // if it's a member of a replica set:
        try {

            mongoClient = new MongoClient(MongoConnectionConfigurationBuilder.HOST,
                            Integer.parseInt(MongoConnectionConfigurationBuilder.PORT));

            clearDataFromMongo();

            DB db = mongoClient.getDB(KEYSPACE);

            buildTestMongoDataInsertBatch(db, TABLE_1, TABLE_2);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    public static void clearDataFromES() {
        // TODO not implemented

    }

    public static void clearDataFromMongo() {

        mongoClient.dropDatabase(KEYSPACE);
    }

    public static void clearDataFromCassandra() {

        session = cluster1.connect();

        session.execute(String.format(DROP_KEYSPACE, KEYSPACE));

        session.close();
    }

    public static void prepareDataForCassandra() {

        session = cluster1.connect();

        session.execute(String.format(DROP_KEYSPACE, KEYSPACE));

        session.execute(String.format(CREATE_KEYSPACE, KEYSPACE));

        session = cluster1.connect(KEYSPACE);

        session.execute("CREATE TABLE " + KEYSPACE + "." + TABLE_1 + " (" + "id int PRIMARY KEY," + "artist text,"
                        + "title text," + "year  int," + "length  text," + "description text," + "lucene1 text);");

        session.execute("CREATE TABLE " + KEYSPACE + "." + TABLE_2 + " (" + "id int PRIMARY KEY," + "artist text,"
                        + "age int," + "rate float," + "active boolean," + "lucene2 text);");

        session.execute("CREATE CUSTOM INDEX lucene1 ON "
                        + KEYSPACE
                        + "."
                        + TABLE_1
                        + " (lucene1) USING 'org.apache.cassandra.db.index.stratio.RowIndex' "
                        + "WITH OPTIONS = { 'refresh_seconds':'10', 'filter_cache_size':'10', "
                        + "'write_buffer_size':'100', 'stored_rows':'false', "
                        + "'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\", fields:{ "
                        + "artist:{type:\"string\"}, " + "title:{type:\"string\"}, " + "year:{type:\"integer\"}, "
                        + "length:{type:\"string\"}, " + "description:{type:\"string\"}}}'};");

        session.execute("CREATE CUSTOM INDEX lucene2 ON "
                        + KEYSPACE
                        + "."
                        + TABLE_2
                        + " (lucene2) USING 'org.apache.cassandra.db.index.stratio.RowIndex' "
                        + "WITH OPTIONS = { 'refresh_seconds':'10', 'filter_cache_size':'10', "
                        + "'write_buffer_size':'100', 'stored_rows':'false', "
                        + "'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\", fields:{ "
                        + "artist:{type:\"string\"}, " + "age:{type:\"integer\"}, " + "rate:{type:\"float\"}, "
                        + "active:{type:\"boolean\"}}}'};");

        buildTestDataInsertBatch(session, TABLE_1, TABLE_2);
        session.close();
    }

    protected static Boolean buildTestMongoDataInsertBatch(DB db, String... csvOrigin) {

        for (String origin : csvOrigin) {

            URL testData = Resources.getResource(origin + ".csv");

            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
                            testData.toURI()))))) {
                String line;

                DBCollection collection = db.getCollection(origin);

                while ((line = br.readLine()) != null) {
                    String[] fields = (line).split(",");
                    BasicDBObject doc = origin.equals(TABLE_1) ? new BasicDBObject("artist", fields[1])
                                    .append("title", fields[2]).append("year", fields[3]).append("length", fields[4])
                                    .append("description", fields[5])
                                    .append("description2", new BasicDBObject("foo", "bar")) : new BasicDBObject(
                                    "artist", fields[1]).append("age", fields[2]).append("rate", fields[3])
                                    .append("active", fields[4]);

                    collection.insert(doc);
                }
            } catch (Exception e) {
                logger.error("Error", e);
            }
        }

        return true;
    }

    protected static Boolean buildTestESDataInsertBatch(String... csvOrigin) {

        for (String origin : csvOrigin) {

            URL testData = Resources.getResource(origin + ".csv");

            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
                            testData.toURI()))))) {
                String line;

                while ((line = br.readLine()) != null) {
                    String[] fields = (line).split(",");
                    BasicDBObject doc = origin.equals(TABLE_1) ? new BasicDBObject("artist", fields[1])
                                    .append("title", fields[2]).append("year", fields[3]).append("length", fields[4])
                                    .append("description", fields[5]) : new BasicDBObject("artist", fields[1])
                                    .append("age", fields[2]).append("rate", fields[3]).append("active", fields[4]);

                    IndexResponse response = elasticClient
                                    .prepareIndex(KEYSPACE, origin.equals(TABLE_1) ? TABLE_1 : TABLE_2)
                                    .setSource(doc.toString()).execute().actionGet();
                    response.getHeaders();
                }
            } catch (Exception e) {
                logger.error("Error", e);
            }
        }

        return true;

    }

    protected static Boolean buildTestDataInsertBatch(Session session, String... csvOrigin) {

        for (String origin : csvOrigin) {

            URL testData = Resources.getResource(origin + ".csv");

            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
                            testData.toURI()))))) {
                String line;

                while ((line = br.readLine()) != null) {
                    String[] fields = (quote(origin) + "," + line).split(",");
                    String insert = origin.equals(TABLE_1) ? String.format(rawSongsInsert, (Object[]) fields) : String
                                    .format(rawArtistsInsert, (Object[]) fields);
                    logger.debug("INSERT---->" + insert);
                    session.execute(insert);
                }
            } catch (Exception e) {
                logger.error("Error", e);
            }
        }

        return true;
    }

}
