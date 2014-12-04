Stratio-Connector-Deep
======================

Deep connector for multiple data sources

## Requirements ##

[Stratio Connector deep](https://github.com/Stratio/stratio-connector-deep) must be installed and started.
[Crossdata] (https://github.com/Stratio/crossdata) is needed to interact with this connector.

## Compiling Stratio Connector Deep ##

To automatically build execute the following command:

```
   > mvn clean compile install
```

## Running the Stratio Connector Deep ##

```
   > mvn exec:java -Dexec.mainClass="com.stratio.connector.deep.connection.DeepConnector"
```


## How to use Deep Connector ##

 1. Start [crossdata-server and then crossdata-shell](https://github.com/Stratio/crossdata).
 2. https://github.com/Stratio/crossdata
 3. Start Deep Connector as it is explained before
 4. In crossdata-shell:

    Add a datastore with this command:

      ```
         xdsh:user>  ADD DATASTORE <Absolute path to Datastore manifest>;
      ```

    Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest.

      ```
         xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<ipHost_1,
         ipHost_2,...ipHost_n>]', 'Port': <hdfs_port> };
      ```

    Add the connector manifest.

       ```
         xdsh:user>  ADD CONNECTOR <Path to HDFS Connector Manifest>
       ```

    Attach the connector to the previously defined cluster. The connector name must match the one defined in the
    Connector Manifest.

        ```
            xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit': '1000'};
        ```

    At this point, we can start to send queries.

        ...
            xdsh:user> CREATE CATALOG catalogTest;

            xdsh:user> USE catalogTest;

            xdsh:user> CREATE TABLE tableTest ON CLUSTER datastore_prod (id int PRIMARY KEY, name text);

        ...

# Using Crossdata & Stratio-Connectors to connect to Cassandra & Mongo. Step By Step Example #

1. Start [crossdata-server and then crossdata-shell]
   ...
    mvn exec:java -pl crossdata-server -Dexec.mainClass="com.stratio.crossdata.server.CrossdataApplication"
   ...
    mvn exec:java -pl crossdata-shell -Dexec.mainClass="com.stratio.crossdata.sh.Shell"
   ...

2.Add the Manifest info to add the dataStores to crossdata, use the manifest from stratio-native connectors

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

3.Attach the clusters to crossdata ( Cassandra & Mongo must be be started )

ATTACH CLUSTER cassandra_cluster_name ON DATASTORE Cassandra WITH OPTIONS {'Hosts': '[127.0.0.1]', 'Port': 9042  };
ATTACH CLUSTER mongo_cluster_name     ON DATASTORE Mongo     WITH OPTIONS {'Hosts': '[127.0.0.1]', 'Port': 27017 };

4.Start All the connectors

    ```
       > mvn exec:java -Dexec.mainClass="com.stratio.connector.deep.connection.DeepConnector"
       > mvn exec:java -Dexec.mainClass="com.stratio.connector.mongodb.core.MongoConnector"
       > mvn exec:java -Dexec.mainClass="com.stratio.connector.cassandra.CassandraConnector"
    ```

5.Attach the connectors to Crossdata

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

# License #

Stratio Crossdata is licensed as [Apache2](http://www.apache.org/licenses/LICENSE-2.0.txt)

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

