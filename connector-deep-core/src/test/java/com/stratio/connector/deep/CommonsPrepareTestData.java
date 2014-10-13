package com.stratio.connector.deep;

public interface CommonsPrepareTestData {

    public static final String DROP_KEYSPACE =" DROP KEYSPACE IF EXISTS %s  ";

    public static final String CREATE_KEYSPACE =" CREATE  KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : " +
            "'SimpleStrategy', 'replication_factor' : 1 } ";

    public static final String rawSongsInsert = "INSERT INTO %s (" + "\"id\", \"artist\", \"title\", "
            + "\"year\", \"length\", " + "\"description\") "
            + "values (%s, \'%s\',\'%s\', \'%s\', \'%s\', \'%s\');";

    public static final String rawArtistsInsert = "INSERT INTO %s (" + "\"id\", \"artist\", \"age\") "
            + "values (%s, \'%s\',\'%s\');";
    public static final String CREATE_TABLE =" DROP KEYSPACE ";


}
