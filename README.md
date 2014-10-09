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

Once launched, you can view the web application in your browser by going to
the appropriate URL. For example:

http://localhost:8080/home

Once there, you can take snapshots, and view older snapshots of web pages.

Running in RedHat OpenShift
===========================

1. rhc app create snapshot https://raw.githubusercontent.com/kite-sdk/kite-minicluster-openshift-cartridge/master/metadata/manifest.yml jbossews-2.0 -g large
2. rhc cartridge storage jbossews-2.0 --app snapshot --set 4
3. cd snapshot
4. git remote add upstream -m master git://github.com/kite-sdk/kite-spring-hbase-example.git
5. git pull -s recursive -X theirs upstream master
6. git push origin master
