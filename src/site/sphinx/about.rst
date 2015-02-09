Stratio-Connector-Deep
**********************

Deep connector for multiple data sources

Requirements
------------

`Stratio Connector
deep <https://github.com/Stratio/stratio-connector-deep>`__ must be
installed and started. [Crossdata]
(https://github.com/Stratio/crossdata) is needed to interact with this
connector.

Compiling Stratio Connector Deep
--------------------------------

To automatically build execute the following command:

::

       > mvn clean compile install

Running the Stratio Connector Deep
----------------------------------

::

       > mvn exec:java -Dexec.mainClass="com.stratio.connector.deep.connection.DeepConnector"

How to use Deep Connector
-------------------------

1. Start `crossdata-server and then
   crossdata-shell <https://github.com/Stratio/crossdata>`__.
2. https://github.com/Stratio/crossdata
3. Start Deep Connector as it is explained before
4. In crossdata-shell:

   Add a datastore with this command:

   ::


     xdsh:user>  ADD DATASTORE <Absolute path to Datastore manifest>;

   Attach cluster on that datastore. The datastore name must be the same
   as the defined in the Datastore manifest.

   ::

     xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<ipHost_1,      ipHost_2,...ipHost_n>]', 'Port': <hdfs_port> };

   Add the connector manifest.

   ::
  
     xdsh:user>  ADD CONNECTOR <Path to HDFS Connector Manifest>

   Attach the connector to the previously defined cluster. The connector
   name must match the one defined in the Connector Manifest.

   ::

      xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit': '1000'};


   At this point, we can start to send queries.

   ::

       ...
           xdsh:user> CREATE CATALOG catalogTest;

           xdsh:user> USE catalogTest;

           xdsh:user> CREATE TABLE tableTest ON CLUSTER datastore_prod (id int PRIMARY KEY, name text);

       ...

License
=======

Stratio Crossdata is licensed as
`Apache2 <http://www.apache.org/licenses/LICENSE-2.0.txt>`__

Licensed to STRATIO (C) under one or more contributor license
agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The STRATIO (C)
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

