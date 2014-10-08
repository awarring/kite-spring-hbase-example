package org.kitesdk.spring.hbase.example.helper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.RandomAccessDataset;

/**
 * A Helper class for running this example in dev mode, which automatically
 * creates and updates the schemas.
 */
public class DevHelper {

  private static final String CLASSPATH_PREFIX = "classpath:";
  private static final String MANAGED_SCHEMAS_TABLE = "managed_schemas";

  /**
   * Create the HBase datasets in the map of dataset names to schema files
   * 
   * @param zkHost
   *          HBase zookeeper client hostname
   * @param zkPort
   *          HBase zookeeper client port
   * @param datasetNameSchemaMap
   *          A map of dataset names to the Avro schema files that we want to
   *          create. The schema files are a location, which can be a location
   *          on the classpath, represented with a "classpath:/" prefix.
   * @return THe list of created datasets.
   * @throws URISyntaxException
   * @throws IOException
   */
  public static List<RandomAccessDataset<?>> createOrUpdateDatasets(
      String zkHost, String zkPort, Map<String, String> datasetNameSchemaMap)
      throws URISyntaxException, IOException {

    createManagedSchemasTable(zkHost, zkPort);

    List<RandomAccessDataset<?>> datasets = new ArrayList<RandomAccessDataset<?>>();
    for (Entry<String, String> entry : datasetNameSchemaMap.entrySet()) {
      String datasetName = entry.getKey();
      String schemaLocation = entry.getValue();
      File schemaFile;
      if (schemaLocation.startsWith(CLASSPATH_PREFIX)) {
        schemaLocation = schemaLocation.substring(CLASSPATH_PREFIX.length());
        schemaFile = new File(DevHelper.class.getClassLoader()
            .getResource(schemaLocation).toURI());
      } else {
        schemaFile = new File(schemaLocation);
      }
      DatasetDescriptor desc = new DatasetDescriptor.Builder().schema(
          schemaFile).build();

      String datasetURI = "dataset:hbase:" + zkHost + ":" + zkPort + "/"
          + datasetName;
      if (!Datasets.exists(datasetURI)) {
        datasets
            .add((RandomAccessDataset<?>) Datasets.create(datasetURI, desc));
      } else {
        datasets
            .add((RandomAccessDataset<?>) Datasets.update(datasetURI, desc));
      }
    }
    return datasets;
  }

  /**
   * Create the required HBase tables for the Kite HBase module. If those are
   * already initialized, this method will do nothing.
   * 
   * @param zkHost
   *          HBase zookeeper client hostname
   * @param zkPort
   *          HBase zookeeper client port
   */
  public static void createManagedSchemasTable(String zkHost, String zkPort)
      throws IOException {
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", zkHost);
    config.set("hbase.zookeeper.property.clientPort", zkPort);
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
}
