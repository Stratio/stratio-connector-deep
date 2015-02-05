package com.stratio.connector.deep;

import static com.stratio.connector.deep.ESConnectionConfigurationBuilder.ES_NATIVE_PORT;
import static com.stratio.deep.commons.utils.Utils.quote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexMissingException;
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

    public static Cluster cluster;
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

        elasticClient = new TransportClient(ImmutableSettings.settingsBuilder()
                        .put("cluster.name", ESConnectionConfigurationBuilder.ES_CLUSTERNAME).build())
                        .addTransportAddress(new InetSocketTransportAddress(ESConnectionConfigurationBuilder.HOST,
                                        ES_NATIVE_PORT.intValue()));

        deleteESIndex();

        buildTestESDataInsertBatch(MYTABLE1_CONSTANT, MYTABLE2_CONSTANT);

    }

    private static void deleteESIndex() {
        try {
            elasticClient.admin().indices().delete(new DeleteIndexRequest(KEYSPACE)).actionGet();
        } catch (IndexMissingException indexEception) {
            logger.info("Trying to delete a non-existing index " + KEYSPACE);
        }

    }

    public static void prepareDataForMongo() {

        // To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
        // if it's a member of a replica set:
        try {

            mongoClient = new MongoClient(MongoConnectionConfigurationBuilder.HOST,
                            Integer.parseInt(MongoConnectionConfigurationBuilder.PORT));

            clearDataFromMongo();

            DB db = mongoClient.getDB(KEYSPACE);

            buildTestMongoDataInsertBatch(db, MYTABLE1_CONSTANT, MYTABLE2_CONSTANT);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    public static void clearDataFromES() {
        deleteESIndex();
        elasticClient.close();

    }

    public static void clearDataFromMongo() {

        mongoClient.dropDatabase(KEYSPACE);
    }

    public static void clearDataFromCassandra() {

        connectCassandra();
        session.execute(String.format(DROP_KEYSPACE, KEYSPACE));
        session.close();
        cluster.close();

    }

    public static void reconnectCassandra() {

        session.close();
        cluster.close();
        connectCassandra();
    }

    private static void connectCassandra() {

        cluster = Cluster.builder().addContactPoints(CassandraConnectionConfigurationBuilder.HOST).build();
        session = cluster.connect();
    }

    public static void prepareDataForCassandra() {

        clearDataFromCassandra();

        connectCassandra();

        session.execute(String.format(CREATE_KEYSPACE, KEYSPACE));

        reconnectCassandra();

        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + MYTABLE1_CONSTANT + " ("
                        + "id int PRIMARY KEY," + "artist text," + "title text," + "year  int," + "length  text,"
                        + "description text," + "lucene1 text);");

        reconnectCassandra();

        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + MYTABLE2_CONSTANT + " ("
                        + "id int PRIMARY KEY," + "artist text," + "age int," + "rate float," + "active boolean,"
                        + "lucene2 text);");

        reconnectCassandra();

        session.execute("CREATE CUSTOM INDEX IF NOT EXISTS lucene1 ON "
                        + KEYSPACE
                        + "."
                        + MYTABLE1_CONSTANT
                        + " (lucene1) USING 'org.apache.cassandra.db.index.stratio.RowIndex' "
                        + "WITH OPTIONS = { 'refresh_seconds':'10', 'filter_cache_size':'10', "
                        + "'write_buffer_size':'100', 'stored_rows':'false', "
                        + "'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\", fields:{ "
                        + "artist:{type:\"string\"}, " + "title:{type:\"string\"}, " + "year:{type:\"integer\"}, "
                        + "length:{type:\"string\"}, " + "description:{type:\"string\"}}}'};");

        reconnectCassandra();

        session.execute("CREATE CUSTOM INDEX IF NOT EXISTS lucene2 ON "
                        + KEYSPACE
                        + "."
                        + MYTABLE2_CONSTANT
                        + " (lucene2) USING 'org.apache.cassandra.db.index.stratio.RowIndex' "
                        + "WITH OPTIONS = { 'refresh_seconds':'10', 'filter_cache_size':'10', "
                        + "'write_buffer_size':'100', 'stored_rows':'false', "
                        + "'schema':'{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\", fields:{ "
                        + "artist:{type:\"string\"}, " + "age:{type:\"integer\"}, " + "rate:{type:\"float\"}, "
                        + "active:{type:\"boolean\"}}}'};");

        reconnectCassandra();

        buildTestDataInsertBatch(session, MYTABLE1_CONSTANT, MYTABLE2_CONSTANT);
        session.close();
        cluster.close();

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
                    BasicDBObject doc = origin.equals(MYTABLE1_CONSTANT) ? new BasicDBObject("artist", fields[1])
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
                    BasicDBObject doc = origin.equals(MYTABLE1_CONSTANT) ? new BasicDBObject("artist", fields[1])
                                    .append("title", fields[2]).append("year", fields[3]).append("length", fields[4])
                                    .append("description", fields[5]) : new BasicDBObject("artist", fields[1])
                                    .append("age", fields[2]).append("rate", fields[3]).append("active", fields[4]);

                    IndexResponse response = elasticClient
                                    .prepareIndex(KEYSPACE,
                                                    origin.equals(MYTABLE1_CONSTANT) ? MYTABLE1_CONSTANT
                                                                    : MYTABLE2_CONSTANT).setSource(doc.toString())
                                    .execute().actionGet();
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
                    String insert = origin.equals(MYTABLE1_CONSTANT) ? String.format(rawSongsInsert, (Object[]) fields)
                                    : String.format(rawArtistsInsert, (Object[]) fields);
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
