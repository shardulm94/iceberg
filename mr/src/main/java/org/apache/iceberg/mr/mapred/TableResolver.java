/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Tables;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mr.InputFormatConfig;

final class TableResolver {

  private TableResolver() {
  }

  static Table resolveTableFromJob(JobConf conf) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(InputFormatConfig.CATALOG_NAME, extractProperty(conf, InputFormatConfig.CATALOG_NAME));
    if (conf.get(InputFormatConfig.CATALOG_NAME).equals(InputFormatConfig.HADOOP_CATALOG)) {
      properties.setProperty(InputFormatConfig.SNAPSHOT_TABLE, conf.get(InputFormatConfig.SNAPSHOT_TABLE, "true"));
    }
    properties.setProperty(InputFormatConfig.TABLE_LOCATION, extractProperty(conf, InputFormatConfig.TABLE_LOCATION));
    properties.setProperty(InputFormatConfig.TABLE_NAME, extractProperty(conf, InputFormatConfig.TABLE_NAME));
    return resolveTableFromConfiguration(conf, properties);
  }

  static Table resolveTableFromConfiguration(Configuration conf, Properties properties) throws IOException {
    String catalogName = properties.getProperty(InputFormatConfig.CATALOG_NAME);
    URI tableLocation = pathAsURI(properties.getProperty(InputFormatConfig.TABLE_LOCATION));
    if (catalogName == null) {
      throw new IllegalArgumentException("Catalog property: 'iceberg.catalog' not set in JobConf");
    }
    switch (catalogName) {
      case InputFormatConfig.HADOOP_TABLES:
        HadoopTables tables = new HadoopTables(conf);
        return tables.load(tableLocation.getPath());
      case InputFormatConfig.HADOOP_CATALOG:
        String tableName = properties.getProperty(InputFormatConfig.TABLE_NAME);
        TableIdentifier id = TableIdentifier.parse(tableName);
        if (tableName.endsWith(InputFormatConfig.SNAPSHOT_TABLE_SUFFIX)) {
          if (!Boolean.parseBoolean(properties.getProperty(InputFormatConfig.SNAPSHOT_TABLE,
                  Boolean.TRUE.toString()))) {
            String tablePath = id.toString().replaceAll("\\.", "/");
            URI warehouseLocation = pathAsURI(tableLocation.getPath().replaceAll(tablePath, ""));
            HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
            return catalog.loadTable(id);
          } else {
            return resolveMetadataTable(conf, tableLocation.getPath(), tableName);
          }
        } else {
          URI warehouseLocation = pathAsURI(extractWarehousePath(tableLocation.getPath(), tableName));
          HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
          return catalog.loadTable(id);
        }
      case InputFormatConfig.HIVE_CATALOG:
        return null;
      case "direct":
        return new BaseTable(new TableOperations() {
          private volatile boolean shouldRefresh = true;
          private volatile TableMetadata currentMetadata = null;
          private HadoopFileIO defaultFileIo = null;
          @Override
          public TableMetadata current() {
            if (shouldRefresh) {
              return refresh();
            }
            return currentMetadata;
          }

          @Override
          public TableMetadata refresh() {
            this.shouldRefresh = false;
            this.currentMetadata = TableMetadataParser.read(io(), tableLocation.toString());
            return currentMetadata;
          }

          @Override
          public void commit(TableMetadata base, TableMetadata metadata) {

          }

          @Override
          public FileIO io() {
            if (defaultFileIo == null) {
              defaultFileIo = new HadoopFileIO(conf);
            }
            return defaultFileIo;
          }

          @Override
          public String metadataFileLocation(String fileName) {
            return null;
          }

          @Override
          public LocationProvider locationProvider() {
            return null;
          }
        }, tableLocation.toString());
    }
    return null;
  }

  static Table resolveMetadataTable(Configuration conf, String location, String tableName) throws IOException {
    URI warehouseLocation = pathAsURI(extractWarehousePath(location, tableName));
    HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
    String baseTableName = StringUtils.removeEnd(tableName, InputFormatConfig.SNAPSHOT_TABLE_SUFFIX);

    TableIdentifier snapshotsId = TableIdentifier.parse(baseTableName +
            InputFormatConfig.ICEBERG_SNAPSHOTS_TABLE_SUFFIX);
    return catalog.loadTable(snapshotsId);
  }

  static URI pathAsURI(String path) throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("Path is null.");
    }
    try {
      return new URI(path);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + path + "'", e);
    }
  }

  protected static String extractProperty(JobConf conf, String key) {
    String value = conf.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Property not set in JobConf: " + key);
    }
    return value;
  }

  protected static String extractWarehousePath(String location, String tableName) {
    String tablePath = tableName.replaceAll("\\.", "/").replaceAll(
            InputFormatConfig.SNAPSHOT_TABLE_SUFFIX, "");
    return location.replaceAll(tablePath, "");
  }

}
