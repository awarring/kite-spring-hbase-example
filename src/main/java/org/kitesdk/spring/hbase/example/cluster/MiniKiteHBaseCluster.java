/**
 * Copyright 2014 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.spring.hbase.example.cluster;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.DNS;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in process mini HBase cluster that can be configured to clean or keep data
 * around across restarts. It also configures the appropriate HBase system
 * tables required by the Kite HBase module.
 * 
 * This cluster is able to be run within the OpenShift environment, which has
 * special restrictions on which IP addresses daemons are able to be bound to.
 */
public class MiniKiteHBaseCluster {

  private static final Logger LOG = LoggerFactory
      .getLogger(MiniKiteHBaseCluster.class);

  private static final String HBASE_META_TABLE = "hbase:meta";
  private static final String MANAGED_SCHEMAS_TABLE = "managed_schemas";
  private static final String CLASSPATH_PREFIX = "classpath:";
  private static final String OPENSHIFT_BIND_KEY = "OPENSHIFT_JBOSSEWS_IP";

  private final String localFsLocation;
  private final int zkPort;
  private final boolean clean;

  private Configuration config;
  private MiniDFSCluster dfsCluster;
  private MiniKiteZooKeeperCluster zkCluster;
  private MiniHBaseCluster hbaseCluster;

  private HBaseDatasetRepository repo;

  /**
   * Construct the MiniKiteHBaseCluster
   * 
   * @param localFsLocation
   *          The location on the local filesystem where the HDFS filesystem
   *          blocks and metadata are stored.
   * @param zkPort
   *          The zookeeper port
   * @param clean
   *          True if we want any old filesystem to be removed, and we want to
   *          start fresh. Otherwise, false.
   */
  public MiniKiteHBaseCluster(String localFsLocation, int zkPort, boolean clean) {
    this.localFsLocation = localFsLocation;
    this.zkPort = zkPort;
    this.clean = clean;
  }

  /**
   * Startup the mini cluster
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void startup() throws Exception {
    config = new Configuration();

    // If clean, then remove the localFsLocation so we can start fresh.
    if (clean) {
      LOG.info("Cleaning cluster data at: " + localFsLocation
          + " and starting fresh.");
      File file = new File(localFsLocation);
      file.delete();
    }

    // Configure and start the HDFS cluster
    boolean format = shouldFormatDFSCluster(localFsLocation, clean);
    config = configureDFSCluster(config, localFsLocation);
    dfsCluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
        .format(format).checkDataNodeAddrConfig(true)
        .checkDataNodeHostConfig(true).build();

    // Configure and start a 1 node Zookeeper cluster
    String zkLocation = localFsLocation + "/zk";
    zkCluster = new MiniKiteZooKeeperCluster(config);
    zkCluster.setDefaultClientPort(zkPort);
    String bindAddress = "0.0.0.0";
    if (System.getenv(OPENSHIFT_BIND_KEY) != null) {
      bindAddress = System.getenv(OPENSHIFT_BIND_KEY);
    }
    int clientPort = zkCluster.startup(new File(zkLocation), 1, bindAddress,
        format);
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));

    // Configure and start an HBase cluster, and initialize for Kite HBase.
    hbaseCluster = new MiniHBaseCluster(config, 0, 0, null, null);
    config = configureHBaseCluster(hbaseCluster.getConf(),
        dfsCluster.getFileSystem());
    hbaseCluster.startMaster();
    hbaseCluster.startRegionServer();
    waitForHBaseToComeOnline(hbaseCluster);
    createManagedSchemasTable(config);

    repo = new HBaseDatasetRepository.Builder().configuration(config).build();
  }

  /**
   * Shutdown the mini cluster, and set member variables to null
   * 
   * @throws IOException
   */
  public void shutdown() throws IOException {
    // unset the configuration for MIN and MAX RS to start
    config.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    config.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1);
    if (hbaseCluster != null) {
      hbaseCluster.shutdown();
      // Wait till hbase is down before going on to shutdown zk.
      this.hbaseCluster.waitUntilShutDown();
      this.hbaseCluster = null;
    }

    zkCluster.shutdown();
    zkCluster = null;

    dfsCluster.shutdown();
    dfsCluster = null;
    repo = null;
  }

  /**
   * Get the location on the local FS where we store the HDFS data.
   * 
   * @param baseFsLocation
   *          The base location on the local filesystem we have write access to
   *          create dirs.
   * @return The location for HDFS data.
   */
  private static String getDFSLocation(String baseFsLocation) {
    return baseFsLocation + Path.SEPARATOR + "dfs";
  }

  /**
   * Returns true if we should format the DFS Clsuter. We'll format if clean is
   * true, or if the baseFsLocation does not exist.
   * 
   * @param baseFsLocation
   *          The base location for cluster data
   * @param clean
   *          Specifies if we want to start a clean cluster
   * @return Returns true if we should format a DFSCluster, otherwise false
   */
  private static boolean shouldFormatDFSCluster(String baseFsLocation,
      boolean clean) {
    boolean format = true;
    String dfsLocation = getDFSLocation(baseFsLocation);
    File f = new File(dfsLocation);
    if (f.exists() && f.isDirectory()) {
      format = false;
    }
    return format;
  }

  /**
   * Configure the DFS Cluster before launching it.
   * 
   * @param config
   *          The already created Hadoop configuration we'll further configure
   *          for HDFS
   * @param baseFsLocation
   *          The location on the local filesystem where cluster data is stored
   * @return The updated Configuration object.
   */
  private static Configuration configureDFSCluster(Configuration config,
      String baseFsLocation) {
    String dfsLocation = getDFSLocation(baseFsLocation);

    // If running in Openshift, we only have permission to bind to the private
    // IP address, accessible through an environment variable. Set bind
    // addresses for servers if we are on OpenShift .
    if (System.getenv(OPENSHIFT_BIND_KEY) != null) {
      String bindAddress = System.getenv(OPENSHIFT_BIND_KEY);
      config = new OpenshiftCompatibleConfiguration(config, bindAddress);
      config.set("dfs.datanode.address", bindAddress + ":50010");
      config.set("dfs.datanode.http.address", bindAddress + ":50075");
      config.set("dfs.datanode.ipc.address", bindAddress + ":50020");
      // When a datanode registers with the namenode, the Namenode do a hostname
      // check of the datanode which will fail on OpenShift due to reverse DNS
      // issues with the internal IP addresses. This config disables that check,
      // and will allow a datanode to connect regardless.
      config.setBoolean("dfs.namenode.datanode.registration.ip-hostname-check",
          false);
    } else {
      config = new Configuration();
    }
    config.set("hdfs.minidfs.basedir", dfsLocation);
    return config;
  }

  /**
   * Configure the HBase cluster before launching it
   * 
   * @param config
   *          already created Hadoop configuration we'll further configure for
   *          HDFS
   * @param hdfsFs
   *          The HDFS FileSystem this HBase cluster will run on top of
   * @return The updated Configuration object.
   * @throws IOException
   */
  private static Configuration configureHBaseCluster(Configuration config,
      FileSystem hdfsFs) throws IOException {
    // Initialize HDFS path configurations required by HBase
    Path hbaseDir = new Path(hdfsFs.makeQualified(hdfsFs.getHomeDirectory()),
        "hbase");
    FSUtils.setRootDir(config, hbaseDir);
    hdfsFs.mkdirs(hbaseDir);
    config.set("fs.defaultFS", hdfsFs.getUri().toString());
    config.set("fs.default.name", hdfsFs.getUri().toString());
    FSUtils.setVersion(hdfsFs, hbaseDir);

    // Configure the bind addresses and ports. If running in Openshift, we only
    // have permission to bind to the private IP address, accessible through an
    // environment variable.
    if (System.getenv(OPENSHIFT_BIND_KEY) != null) {
      String bindAddress = System.getenv(OPENSHIFT_BIND_KEY);
      config.set("hbase.master.ipc.address", bindAddress);
      config.set("hbase.regionserver.ipc.address", bindAddress);
      config.set(HConstants.ZOOKEEPER_QUORUM, bindAddress);

      // By default, the HBase master and regionservers will report to zookeeper
      // that it's hostname is what it determines by reverse DNS lookup, and not
      // what we use as the bind address. This means when we set the bind
      // address, daemons won't actually be able to connect to eachother if they
      // are different. Here, we do something that's illegal in 48 states - use
      // reflection to override a private static final field in the DNS class
      // that is a cachedHostname. This way, we are forcing the hostname that
      // reverse dns finds. This may not be compatible with newer versions of
      // Hadoop.
      try {
        Field cachedHostname = DNS.class.getDeclaredField("cachedHostname");
        cachedHostname.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(cachedHostname, cachedHostname.getModifiers()
            & ~Modifier.FINAL);
        cachedHostname.set(null, bindAddress);
      } catch (Exception e) {
        // Reflection can throw so many checked exceptions. Let's wrap in an
        // IOException.
        throw new IOException(e);
      }
    }
    // By setting the info ports to -1 for, we won't launch the master or
    // regionserver info web interfaces
    config.set(HConstants.MASTER_INFO_PORT, "-1");
    config.set(HConstants.REGIONSERVER_INFO_PORT, "-1");
    return config;
  }

  /**
   * Wait for the hbase cluster to start up and come online, and then return.
   * 
   * @param hbaseCluster
   *          The hbase cluster to wait for.
   * @throws IOException
   */
  private static void waitForHBaseToComeOnline(MiniHBaseCluster hbaseCluster)
      throws IOException {
    // wait for regionserver to come online, and then break out of loop.
    while (true) {
      if (hbaseCluster.getRegionServer(0).isOnline()) {
        break;
      }
    }
    // Don't leave here till we've done a successful scan of the hbase:meta
    HTable t = new HTable(hbaseCluster.getConf(), HBASE_META_TABLE);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();
  }

  /**
   * Create the HBase datasets in the map of dataset names to schema files
   * 
   * @param datasetNameSchemaMap
   *          A map of dataset names to the Avro schema files that we want to
   *          create. The schema files are a location, which can be a location
   *          on the classpath, represented with a "classpath:/" prefix.
   * @return THe list of created datasets.
   * @throws URISyntaxException
   * @throws IOException
   */
  public List<RandomAccessDataset<?>> createOrUpdateDatasets(
      Map<String, String> datasetNameSchemaMap) throws URISyntaxException,
      IOException {
    List<RandomAccessDataset<?>> datasets = new ArrayList<RandomAccessDataset<?>>();
    for (Entry<String, String> entry : datasetNameSchemaMap.entrySet()) {
      String datasetName = entry.getKey();
      String schemaLocation = entry.getValue();
      File schemaFile;
      if (schemaLocation.startsWith(CLASSPATH_PREFIX)) {
        schemaLocation = schemaLocation.substring(CLASSPATH_PREFIX.length());
        schemaFile = new File(MiniKiteHBaseCluster.class.getClassLoader()
            .getResource(schemaLocation).toURI());
      } else {
        schemaFile = new File(schemaLocation);
      }
      DatasetDescriptor desc = new DatasetDescriptor.Builder().schema(
          schemaFile).build();

      if (!repo.exists(datasetName)) {
        datasets.add(repo.create(datasetName, desc));
      } else {
        datasets.add(repo.update(datasetName, desc));
      }
    }
    return datasets;
  }

  /**
   * Create the required HBase tables for the Kite HBase module. If those are
   * already initialized, this method will do nothing.
   * 
   * @param config
   *          The HBase configuration
   */
  private static void createManagedSchemasTable(Configuration config)
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(config);
    try {
      if (!admin.tableExists(MANAGED_SCHEMAS_TABLE)) {
        @SuppressWarnings("deprecation")
        HTableDescriptor desc = new HTableDescriptor(MANAGED_SCHEMAS_TABLE);
        desc.addFamily(new HColumnDescriptor("meta"));
        desc.addFamily(new HColumnDescriptor("schema"));
        desc.addFamily(new HColumnDescriptor("_s"));
        admin.createTable(desc);
      }
    } finally {
      admin.close();
    }
  }

  /**
   * A Hadoop Configuration class that won't override the Namenode RPC and
   * Namenode HTTP bind addresses. The mini DFS cluster sets this bind address
   * to 127.0.0.1, and this can't be overridden. In the OpenShift environment,
   * you can't bind to 127.0.0.1. You can only bind to the private ip address.
   */
  public static class OpenshiftCompatibleConfiguration extends Configuration {

    public OpenshiftCompatibleConfiguration(Configuration config,
        String bindAddress) {
      super(config);
      super.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, bindAddress
          + ":8020");
      super.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, bindAddress
          + ":50070");
    }

    @Override
    public void set(String key, String value) {
      if (!key.equals(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY)
          && !key.equals(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)) {
        super.set(key, value);
      }
    }
  }
}
