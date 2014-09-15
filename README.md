kite-spring-hbase-example
=========================

An example KiteSDK web application that uses Spring MVC and HBase.
This application is a web caching app that can be used to fetch web pages and
store their content in a local HBase cluster. The cached web page can be viewed,
and metadata about that page, like size, time to fetch, outlinks can be queried.
This application supports deployment to Redhat's OpenShift for development
deploys.

Building
=========================

There are three build profiles in the application: dev, openshift, and prod.

The default build profile is dev, and in that mode, it will be built so that an
in-process HBase cluster is launched and configured on startup. That cluster
will re-use the same data directory across restarts, so data remains persistent.
This enables us to quickly build a web application on this framework without
having to install a Hadoop and HBase for dev purposes.

The openshift build profile is used by the OpenShift environment automatically.
This is a development mode that also launches an in-process HBase cluster, but
configures it in a way that makes it compatible with OpenShift's environment.

The prod build profile will construct a WAR that won't launch an in-process
HBase cluster on startup. One can configure the properties file
src/main/resources/hbase-prod.properties with the appropriate HBase configs.

Running
==========================

To run locally in dev mode, simply run the following maven command, which
launches an in process Tomcat to run the app in (we have pretty high memory
settings since not only is this running Tomcat, but it's also launching an
HDFS and HBase cluster in the app):

env MAVEN_OPTS="-Xmx2048m -XX:MaxPermSize=256m" mvn clean install tomcat7:run

To run on openshift, create a JBoss Tomcat7 application, giving this github
as the source application. See openshift docs for further directions on
deploying an application.

Today, there's no UI, so to test the deployment, you can use the REST api to
take and fetch snapshots. To take a snapshot, run:

curl --data "url=http://www.google.com/" http://hostname/takeSnapshot

Then go go to the following URL in your browser to see the result:

http://hostname/mostRecentMeta?url=http://www.google.com/
