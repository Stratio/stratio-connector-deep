# Using Crossdata & Stratio-Connectors. Step By Step Example #



1. First of all [Stratio Crossdata 0.1.1] https://github.com/Stratio/crossdata.git is needed and must be installed. The
   server and the Shell must be running.

   Start [crossdata-server and then crossdata-shell].
   ...
    mvn exec:java -pl crossdata-server -Dexec.mainClass="com.stratio.crossdata.server.CrossdataApplication"
   ...
    mvn exec:java -pl crossdata-shell -Dexec.mainClass="com.stratio.crossdata.sh.Shell"
   ...

2. Add the Manifest info to add the dataStores to crossdata, use the manifest from stratio-native connectors

[Stratio Connector deep](https://github.com/Stratio/stratio-connector-deep) must be installed and started.

[Stratio Connector Mongo](https://github.com/Stratio/stratio-connector-mongodb) must be installed and started.

[Stratio Connector Cassandra](https://github.com/Stratio/stratio-connector-cassandra) must be installed and started.

    ...
    xdsh:user> add datastore "/../CassandraDataStore.xml"; [From Stratio Connector Cassandra ]
    xdsh:user> add datastore "/../MongoDataStore.xml";     [From Stratio Connector Mongo ]


    xdsh:user> add connector "/../CassandraConnector.xml"; [From Stratio Connector Cassandra ]
    xdsh:user> add connector "/../MongoConnector.xml";     [From Stratio Connector Mongo ]

    xdsh:user> add connector "/../DeepConnector.xml";      [From Stratio Connector deep]
    ...

    The output must be foreach add:

    ```
       [INFO|Shell] Response time: 0 seconds
       [INFO|Shell] OK
    ```


3. Attach the clusters to crossdata ( Cassandra & Mongo must be be started )

ATTACH CLUSTER cassandra_cluster_name ON DATASTORE Cassandra WITH OPTIONS {'Hosts': '[127.0.0.1]', 'Port': 9042  };
ATTACH CLUSTER mongo_cluster_name     ON DATASTORE Mongo     WITH OPTIONS {'Hosts': '[127.0.0.1]', 'Port': 27017 };

4.Start All the connectors

    ```
       > mvn exec:java -Dexec.mainClass="com.stratio.connector.deep.connection.DeepConnector"
       > mvn exec:java -Dexec.mainClass="com.stratio.connector.mongodb.core.MongoConnector"
       > mvn exec:java -Dexec.mainClass="com.stratio.connector.cassandra.CassandraConnector"
    ```

5. Attach the connectors to Crossdata

 ...
   xdsh:user> ATTACH CONNECTOR DeepConnector TO cassandra_cluster_name WITH OPTIONS {};
   xdsh:user> ATTACH CONNECTOR DeepConnector TO mongo_cluster_name  WITH OPTIONS {};

   xdsh:user> ATTACH CONNECTOR CassandraConnector TO cassandra_cluster_name WITH OPTIONS {};
   xdsh:user> ATTACH CONNECTOR MongoConnector TO mongo_cluster_name  WITH OPTIONS {};
 ...

6. Ready to Play!!

With Cassandra

    ...
       xdsh:user> CREATE CATALOG catalogTest;

       xdsh:user> CREATE TABLE catalogTest.tableTest ON CLUSTER cassandra_prod (id int PRIMARY KEY, name text);
       xdsh:user> INSERT INTO catalogTest.tableTest(id, name) VALUES (1, 'one_stratio');
       xdsh:user> INSERT INTO catalogTest.tableTest(id, name) VALUES (2, 'two_stratio');
       xdsh:user> SELECT * FROM catalogTest.tableTest;

       //Join Cassandra
       xdsh:user> SELECT tableTest.id, tableTest.name, tableTest2.description FROM catalogTest.tableTest
                  INNER JOIN catalogTest.tableTest2  ON tableTest.id = tableTest2.id;
    ...

With Mongo

    ...
      xdsh:user> CREATE CATALOG catalogm  ;
      xdsh:user> CREATE TABLE catalogm.tabletest1 ON CLUSTER mongo_prod (id int PRIMARY KEY, name text);
      xdsh:user> CREATE TABLE catalogm.tabletest2 ON CLUSTER mongo_prod (id int PRIMARY KEY, description text);

      xdsh:user> INSERT INTO catalogm.tabletest2(id, description) VALUES (1, 'mongo descr stratio1');
                SELECT * FROM catalogm.tabletest2;
    ...

Try Join Both

     ...
        xdsh:user> SELECT catalogTest.tableTest.id, catalogTest.tableTest.name, catalogm.tabletest2.description FROM catalogTest.tableTest
                  INNER JOIN catalogm.tableTest2  ON catalogTest.tableTest.id = catalogm.tableTest2.id;
     ...
