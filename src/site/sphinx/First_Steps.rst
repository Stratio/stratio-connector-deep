First Steps
===========

Deep connector allows the integration between Crossdata and
Deep-Connector. Crossdata provides a easy and common language as well as
the integration with several other databases. More information about
Crossdata can be found at
`Crossdata <https://github.com/Stratio/crossdata>`__ For the different
examples you need the native connectors to store the data for the
diferents Examples

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Creating the database and
   collection <#creating-the-database-and-collection>`__

   -  `Step 1: Create the database <#step-1-create-the-database>`__
   -  `Step 2: Create the collection <#step-2-create-the-collection>`__

-  `Inserting Data <#inserting-data>`__

   -  `Step 4: Insert into collection
      students <#step-4-insert-into-collection-students>`__

-  `Querying Data <#querying-data>`__

   -  `Step 5: Select From <#step-5-select-from>`__

      -  `Select all <#select-all>`__
      -  `Select with primary key <#select-with-primary-key>`__
      -  `Select with alias <#select-with-alias>`__
      -  `Select with limit <#select-with-limit>`__
      -  `Select with several where
         clauses <#select-with-several-where-clauses>`__
      -  `Select with groupby <#select-with-groupby>`__
      -  `Select with orderby <#select-with-orderby>`__
      -  `Select Inner Join <#select-inner-join>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------

-  Basic knowledge of SQL like language.
-  First of all `Stratio Crossdata
   0.1.1 <https://github.com/Stratio/crossdata>`__ is needed and must be
   installed. The server and the shell must be running.
-  An installation of
   `MongoDB <http://docs.mongodb.org/manual/installation/>`__.
-  An installation of
   `Cassandra <http://wiki.apache.org/cassandra/GettingStarted>`__.
-  Build an MongoConnector executable and run it following this
   `guide <https://github.com/Stratio/stratio-connector-mongodb#build-an-executable-connector-mongo>`__.
-  Build an CassandraConnector executable and run it following this
   `guide <https://github.com/Stratio/stratio-connector-cassandra/blob/master/README.md>`__.

Configuration
-------------

In the Crossdata Shell we need to add the Datastore Manifest.

::

       >  add datastore "/<path_to_manifest_folder>/CassandraDataStore.xml";
       >  add datastore "/<path_to_manifest_folder>/MongoDataStore.xml";

The output in both cases must be:

::

       [INFO|Shell] Response time: 0 seconds    
       [INFO|Shell] OK

Now we need to add the ConnectorManifest.

::

       > add connector "<path_to_manifest_folder>/MongoConnector.xml";
       > add connector "<path_to_manifest_folder>/CassandraConnector.xml";

       > add connector "<path_to_manifest_folder>/DeepConnector.xml";      [From Stratio Connector deep]

The output in both cases must be:

::

       [INFO|Shell] Response time: 0 seconds    
       [INFO|Shell] OK

At this point we have reported to Crossdata the connector options and
operations. Now we configure the datastore cluster.

::

    > ATTACH CLUSTER mongoCluster ON DATASTORE Mongo WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
    'Port': '[Port1,Port2,...,Portn]'};
    > ATTACH CLUSTER cassandraCluster ON DATASTORE Cassandra WITH OPTIONS {'Hosts': '[Ip1,..,Ipn]','Port':
    '[Port1,...,Portn]'  };

The output must be similar to:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

It is possible to add options like the read preference, write concern,
etc... All options available are described in the MongoDataStore.xml
(e.g. 'mongo.readPreference' : 'secondaryPreferred')

Now we must run the connector.

The last step is to attach the connector to the cluster created before.

::

      >  ATTACH CONNECTOR MongoConnector TO mongoCluster  WITH OPTIONS {};
      >  ATTACH CONNECTOR CassandraConnector TO cassandraCluster WITH OPTIONS {};

      > ATTACH CONNECTOR DeepConnector TO cassandraCluster WITH OPTIONS {};
      > ATTACH CONNECTOR DeepConnector TO mongoCluster  WITH OPTIONS {};

The output must be:

::

    CONNECTOR attached successfully

To ensure that the connector is online we can execute the Crossdata
Shell command:

::

      > describe connectors;

And the output must show a message similar to:

::

    Connector: connector.mongoconnector ONLINE  []  [datastore.mongo]   akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/
    Connector: connector.cassandraconnector ONLINE  []  [datastore.mongo]   akka.tcp://CrossdataServerCluster@127.0.0
    .1:46646/user/ConnectorActor/
    Connector: connector.Deepconnector  ONLINE  []  [datastore.mongo]   akka.tcp://CrossdataServerCluster@127.0.0
    .1:46646/user/ConnectorActor/

Creating the database and collection
====================================

Step 1: Create the database
---------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG highschool;

The output must be:

::

    CATALOG created successfully;

Step 2: Create the collection
-----------------------------

We switch to the database we have just created.

::

      > USE highschool;

To create the table we must execute the next command.

::

      > CREATE TABLE students ON CLUSTER mongoCluster (id int PRIMARY KEY, name text, age int, 
    enrolled boolean);

      > CREATE TABLE students2 ON CLUSTER cassandraCluster (id int PRIMARY KEY, name text, age int,
    enrolled boolean);

And the output must show:

::

    TABLE created successfully

Inserting Data
==============

Step 4: Insert into collection students
---------------------------------------

At first we must insert some rows in the table created before.

::

      >  INSERT INTO students(id, name,age,enrolled) VALUES (1, 'Jhon', 16,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (2, 'Eva',20,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (3, 'Lucie',18,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (4, 'Cole',16,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (5, 'Finn',17,false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (6, 'Violet',21,false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (7, 'Beatrice',18,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (8, 'Henry',16,false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (9, 'Tom',17,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (10, 'Betty',19,true);

For each row the output must be:

::

    STORED successfully

Querying Data
=============

Step 6: Select From
-------------------

Now we execute a set of queries and we will show the expected results.

Select all
~~~~~~~~~~

::

     > SELECT * FROM students;
     
      Partial result: true
      ----------------------------------
      | age | name     | id | enrolled | 
      ----------------------------------
      | 16  | Jhon     | 1  | true     | 
      | 20  | Eva      | 2  | true     | 
      | 18  | Lucie    | 3  | true     | 
      | 16  | Cole     | 4  | true     | 
      | 17  | Finn     | 5  | false    | 
      | 21  | Violet   | 6  | false    | 
      | 18  | Beatrice | 7  | true     | 
      | 16  | Henry    | 8  | false    | 
      | 17  | Tommy    | 9  | true     | 
      | 20  | Betty    | 10 | true     | 
      ----------------------------------

Select with primary key
~~~~~~~~~~~~~~~~~~~~~~~

::

      > SELECT name, enrolled FROM students where id = 1;
      
      Partial result: true
      -------------------
      | name | enrolled | 
      -------------------
      | Jhon | true     | 
      -------------------

Select with alias
~~~~~~~~~~~~~~~~~

::

       >  SELECT name as the_name, enrolled  as is_enrolled FROM students;
       
      Partial result: true
      --------------------------
      | the_name | is_enrolled | 
      --------------------------
      | Jhon     | true        | 
      | Eva      | true        | 
      | Lucie    | true        | 
      | Cole     | true        | 
      | Finn     | false       | 
      | Violet   | false       | 
      | Beatrice | true        | 
      | Henry    | false       | 
      | Tommy    | true        | 
      | Betty    | true        | 
    --------------------------

Select with limit
~~~~~~~~~~~~~~~~~

::

      Partial result: true
      -------------------------------
      | age | name  | id | enrolled | 
      -------------------------------
      | 16  | Jhon  | 1  | true     | 
      | 20  | Eva   | 2  | true     | 
      | 18  | Lucie | 3  | true     | 
      -------------------------------

Select with several where clauses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

      >  SELECT * FROM students WHERE age > 19 AND enrolled = true;
      
      Partial result: true
      -------------------------------
      | age | name  | id | enrolled | 
      -------------------------------
      | 20  | Eva   | 2  | true     |
      | 20  | Betty | 10 | true     | 
      -------------------------------

Select with groupby
~~~~~~~~~~~~~~~~~~~

::

      >  SELECT age FROM students GROUP BY age;

      Partial result: true
      -------
      | age | 
      -------
      | 21  | 
      | 17  | 
      | 18  | 
      | 20  | 
      | 16  | 
      -------
      

Select with orderby
~~~~~~~~~~~~~~~~~~~

::

      >  SELECT age FROM students GROUP BY age ORDER BY age ASC;

      Partial result: true
        -------
        | age |
        -------
        | 16  |
        | 17  |
        | 18  |
        | 20  |
        | 21  |
        -------

Select Inner JOIN
~~~~~~~~~~~~~~~~~

...

::

    > SELECT students.id, students.age, students2.name FROM catalogTest.students
            INNER JOIN catalogTest.students2  ON students.id = students2.id;

the output must be:

::

       Partial result: true      
      -----------------------
      | id | age | name     |
      +----------------------
      | 1  |     | Jhon     |
      | 2  |     | Eva      | 
      | 3  |     | Lucie    |  
      | 4  |     | Cole     |
      | 5  |     | Finn     |  
      | 6  |     | Violet   | 
      | 7  |     | Beatrice |
      | 8  |     | Henry    |
      -----------------------

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the
`Crossdata
Reference <https://github.com/Stratio/crossdata/blob/0.1.1/_doc/meta-reference.md>`__.

