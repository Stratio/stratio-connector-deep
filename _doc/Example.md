# Using Crossdata & Stratio-Connectors. #
# Step By Step Example #



1. First of all [Stratio Crossdata 0.1.1] https://github.com/Stratio/crossdata.git is needed and must be installed. The server and the Shell must be running.

   Start [crossdata-server and then crossdata-shell].

```
   > mvn exec:java -pl crossdata-server -Dexec.mainClass="com.stratio.crossdata.server.CrossdataApplication"

   > mvn exec:java -pl crossdata-shell -Dexec.mainClass="com.stratio.crossdata.sh.Shell"
```

2. Add the Manifest info to add the dataStores to crossdata, use the manifest from stratio-native connectors

   [Stratio Connector deep](https://github.com/Stratio/stratio-connector-deep) must be installed and started.

   [Stratio Connector Mongo](https://github.com/Stratio/stratio-connector-mongodb) must be installed and started.

   [Stratio Connector Cassandra](https://github.com/Stratio/stratio-connector-cassandra) must be installed and started.

```
    xdsh:user> add datastore "/<path_to_manifest_folder>/CassandraDataStore.xml"; [From Stratio Connector Cassandra ]
    xdsh:user> add datastore "/<path_to_manifest_folder>/MongoDataStore.xml";     [From Stratio Connector Mongo ]


    xdsh:user> add connector "/<path_to_manifest_folder>/CassandraConnector.xml"; [From Stratio Connector Cassandra ]
    xdsh:user> add connector "/<path_to_manifest_folder>/MongoConnector.xml";     [From Stratio Connector Mongo ]

    xdsh:user> add connector "/<path_to_manifest_folder>/DeepConnector.xml";      [From Stratio Connector deep]
```

For each row the output must be:

    ```
       [INFO|Shell] Response time: 0 seconds
       [INFO|Shell] OK
    ```


3. Attach the clusters to crossdata ( Cassandra & Mongo must be be started )

```
    xdsh:user> ATTACH CLUSTER cassandraClusterName ON DATASTORE Cassandra WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]','Port': '[Port1,Port2,...,Portn]'  };
    xdsh:user> ATTACH CLUSTER mongoClusterName     ON DATASTORE Mongo     WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]','Port': '[Port1,Port2,...,Portn]' };
```

4. Start All the connectors

```
    > mvn exec:java -Dexec.mainClass="com.stratio.connector.deep.connection.DeepConnector"
    > mvn exec:java -Dexec.mainClass="com.stratio.connector.mongodb.core.MongoConnector"
    > mvn exec:java -Dexec.mainClass="com.stratio.connector.cassandra.CassandraConnector"
```

5. Attach the connectors to Crossdata

```
   xdsh:user> ATTACH CONNECTOR DeepConnector TO cassandraClusterName WITH OPTIONS {};
   xdsh:user> ATTACH CONNECTOR DeepConnector TO mongoClusterName  WITH OPTIONS {};
   xdsh:user> ATTACH CONNECTOR CassandraConnector TO cassandraClusterName WITH OPTIONS {};
   xdsh:user> ATTACH CONNECTOR MongoConnector TO mongoClusterName  WITH OPTIONS {};
```
For each row the output must be:
```
CONNECTOR attached successfully
```   

6. Ready to Play!!

With Cassandra

## Create catalog ##
```
   xdsh:user> CREATE CATALOG catalogTest;
```
The output must be:
```
CATALOG created successfully
```   
## Create Table ##
```
   xdsh:user> CREATE TABLE catalogTest.tableTest ON CLUSTER cassandraClusterName (id int PRIMARY KEY, name text);
   xdsh:user> CREATE TABLE catalogTest.tableTest2 ON CLUSTER cassandraClusterName (id int PRIMARY KEY, description text);
```
The output must be:
```
TABLE created successfully
```   
## Insert ##

At first we must insert some rows in the table created before.
```
   xdsh:user> INSERT INTO catalogTest.tableTest(id, name) VALUES (1, 'one_stratio');
   xdsh:user> INSERT INTO catalogTest.tableTest(id, name) VALUES (2, 'two_stratio');
   
   xdsh:user> INSERT INTO catalogTest.tableTest2(id, description) VALUES (1, 'desc_stratio1);
   xdsh:user> INSERT INTO catalogTest.tableTest2(id, description) VALUES (2, 'desc_stratio2');
```
For each row the output must be:

```
STORED successfully
```
## Select ###
Now we execute a set of queries and we will show the expected results.

### Select All ###
```
   xdsh:user> SELECT * FROM catalogTest.tableTest;
  
the output must be:

  Partial result: true
  ---------------------
  | id  | name        | 
  ---------------------
  | 1   | one_stratio | 
  | 2   | two_stratio | 
  ---------------------
```
### Select All ###
```
   xdsh:user> SELECT * FROM catalogTest.tableTest;

the output must be:

  Partial result: true
  -----------------------
  | id  | description   | 
  -----------------------
  | 1   | desc_stratio1 | 
  | 2   | desc_stratio2 | 
  -----------------------  
```  
## Join ###

```
   xdsh:user> SELECT tableTest.id, tableTest.name, tableTest2.description FROM catalogTest.tableTest
            INNER JOIN catalogTest.tableTest2  ON tableTest.id = tableTest2.id;

the output must be:

   Partial result: true
  --------------------------------------
  | id  | name        |  description   | 
  --------------------------------------
  | 1   | one_stratio |  desc_stratio1 | 
  | 2   | two_stratio |  desc_stratio2 | 
  --------------------------------------
```
With Mongo

```
   xdsh:user> CREATE CATALOG catalogm  ;
   xdsh:user> CREATE TABLE catalogm.tabletest1 ON CLUSTER mongoClusterName (id int PRIMARY KEY, name text);
   xdsh:user> CREATE TABLE catalogm.tabletest2 ON CLUSTER mongoClusterName (id int PRIMARY KEY, description text);
   xdsh:user> INSERT INTO catalogm.tabletest2(id, name) VALUES (1, 'mongoStratio1');
   xdsh:user> INSERT INTO catalogm.tabletest2(id, name) VALUES (1, 'mongoStratio2');
   
   xdsh:user> INSERT INTO catalogm.tabletest2(id, description) VALUES (1, 'mongo descr stratio1');
   xdsh:user> INSERT INTO catalogm.tabletest2(id, description) VALUES (1, 'mongo descr stratio2');
   xdsh:user> SELECT * FROM catalogm.tabletest2;
```

Try Join Both

```
  xdsh:user> SELECT catalogTest.tableTest.id, catalogTest.tableTest.name, catalogm.tabletest2.description            FROM catalogTest.tableTest  INNER JOIN catalogm.tableTest2  ON catalogTest.tableTest.id =    catalogm.tableTest2.id;
```