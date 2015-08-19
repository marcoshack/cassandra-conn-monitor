# Cassandra Connection Monitor

A simple client to monitor Cassandra cluster connectivity and experiment configuration combinations outside a real application. It was used during a troubleshooting in a client application where cluster connections were being closed for no apparent reason.

    gradle shadowJar
    java -jar build/libs/cassandra-conn-monitor-all.jar

The available parameters are shown in the STDOUT and can be changed using Java system params:

    java -jar build/libs/cassandra-conn-monitor-all.jar \
      -Dhosts=10.0.0.1,10.0.0.2,10.0.0.3 \
      -DqueryInterval=5 \
      -Dusername=foo \
      -Dpassword=bar \
      -DcoreConn=1 \
      -DmaxConn=1 \
      -DheartbeatInterval=30

Non obvious parameters:

* `queryInterval`: interval in seconds to run the test query (_select now() from system.local_)

* `coreConn`: initial number of connections to each host

* `maxConn`: maximun number of connections to each host

* `heartbeatInterval`: interval in seconds to heartbeat the connections
