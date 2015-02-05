package com.stratio.connector.deep;

public interface CommonsPrepareTestData {

    public static final String KEYSPACE = "functionaltests";

    public static final String MYTABLE1_CONSTANT = "songs";

    public static final String MYTABLE2_CONSTANT = "artists";

    public static final String AUTHOR_CONSTANT = "artist";

    public static final String AUTHOR_ALIAS_CONSTANT = "artist";

    public static final String DESCRIPTION_CONSTANT = "description";

    public static final String TITLE_CONSTANT = "title";

    public static final String TITLE_EX = "Hey Jude";

    public static final String YEAR_EX = "2004";

    public static final String YEAR_CONSTANT = "year";

    public static final String CASSANDRA_CLUSTERNAME_CONSTANT = "cassandra";

    public static final String DROP_KEYSPACE = " DROP KEYSPACE IF EXISTS %s  ";

    public static final String CREATE_KEYSPACE = " CREATE  KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : "
                    + "'SimpleStrategy', 'replication_factor' : 1 } ";

    public static final String rawSongsInsert = "INSERT INTO " + KEYSPACE + ".%s (" + "\"id\", \"artist\", \"title\", "
                    + "\"year\", \"length\", " + "\"description\") " + "values (%s, \'%s\',\'%s\',%s, \'%s\', \'%s\');";

    public static final String rawArtistsInsert = "INSERT INTO " + KEYSPACE + ".%s ("
                    + "\"id\", \"artist\", \"age\", \"rate\", " + "\"active\") " + "values (%s, \'%s\', %s, %s,  %s);";

}
