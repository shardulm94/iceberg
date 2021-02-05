/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.*;
import static org.apache.spark.sql.functions.*;


public class TestIcebergSourceWrite {

  protected static SparkSession spark = null;
  private static final int NUM_ROWS = 50000;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startMetastoreAndSpark() {
    TestIcebergSourceWrite.spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .getOrCreate();
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    spark.stop();
    TestIcebergSourceWrite.spark = null;
  }

  @Test
  public void writeIcebergParquet() throws IOException {
    Table table = benchmarkTable();
    String tableLocation = table.location();
    benchmarkData().write().format("iceberg").mode(SaveMode.Append).save(tableLocation);
  }

  @Test
  public void writeIcebergORC() throws IOException {
    Table table = benchmarkTable();
    String tableLocation = table.location();
    benchmarkData().write().format("iceberg").option("write-format", "orc").mode(SaveMode.Append).save(tableLocation);
  }

  private Dataset<Row> benchmarkData() {
    return spark.range(NUM_ROWS)
        .withColumn(
            "list",
            expr("array_repeat(CAST(id AS string), 1000)")
        )
        .coalesce(1);
  }

  protected Table benchmarkTable() throws IOException {
    Schema schema = new Schema(
        required(0, "id", Types.LongType.get()),
        optional(1, "list", Types.ListType.ofOptional(2, Types.StringType.get()))
    );
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return tables.create(schema, partitionSpec, properties, temp.newFolder().toString());
  }
}
