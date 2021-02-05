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

package org.apache.iceberg.spark.source.orc;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceNestedDataBenchmark;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import static org.apache.iceberg.TableProperties.*;
import static org.apache.iceberg.types.Types.NestedField.*;
import static org.apache.spark.sql.functions.*;


/**
 * A benchmark that evaluates the performance of writing nested ORC data using Iceberg
 * and the built-in file source in Spark.
 *
 * To run this benchmark:
 * <code>
 *   ./gradlew :iceberg-spark2:jmh
 *       -PjmhIncludeRegex=IcebergSourceNestedORCDataWriteBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-nested-orc-data-write-benchmark-result.txt
 * </code>
 */
public class IcebergSourceNestedORCDataWriteBenchmark extends IcebergSourceNestedDataBenchmark {

  private static final int NUM_ROWS = 500;

  @Setup
  public void setupBenchmark() {
    setupSpark();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void writeIceberg2000() {
    String tableLocation = table().location();
    benchmarkData(2000).write().format("iceberg").option("write-format", "orc").mode(SaveMode.Append).save(tableLocation);
  }

  @Benchmark
  @Threads(1)
  public void writeIceberg20000() {
    String tableLocation = table().location();
    benchmarkData(20000).write().format("iceberg").option("write-format", "orc").mode(SaveMode.Append).save(tableLocation);
  }

  @Benchmark
  @Threads(1)
  public void writeIceberg20000DictionaryOff() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put("orc.dictionary.key.threshold", "0");
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      benchmarkData(20000).write()
          .format("iceberg")
          .option("write-format", "orc")
          .mode(SaveMode.Append)
          .save(tableLocation);
    });
  }

  private Dataset<Row> benchmarkData(int rows) {
    return spark().range(rows)
        .withColumn(
            "outerlist",
            array_repeat(
                struct(
                    expr("array_repeat(CAST(id AS string), 1000) AS innerlist")
                ),
                10
            )
        )
        .coalesce(1);
  }

  @Override
  protected final Table initTable() {
    Schema schema = new Schema(
        required(0, "id", Types.LongType.get()),
        optional(5, "outerlist", Types.ListType.ofOptional(4,
            Types.StructType.of(
                required(1, "innerlist", Types.ListType.ofRequired(2, Types.StringType.get()))
            )
        ))
    );
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }
}
