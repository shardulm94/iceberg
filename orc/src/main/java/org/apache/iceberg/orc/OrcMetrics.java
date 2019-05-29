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

package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Conversions.toByteBuffer;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

public class OrcMetrics {

  private OrcMetrics() {}

  public static Metrics fromInputFile(InputFile file) {
    final Configuration config = (file instanceof HadoopInputFile)
        ? ((HadoopInputFile)file).getConf()
        : new Configuration();
    return fromInputFile(file, config);
  }

  public static Metrics fromInputFile(InputFile file, Configuration config) {
    try {
      final Reader orcReader = OrcFile.createReader(new Path(file.location()),
          OrcFile.readerOptions(config));
      final TypeDescription orcSchema = orcReader.getSchema();
      final Schema schema = TypeConversion.fromOrc(orcSchema);

      ColumnStatistics[] colStats = orcReader.getStatistics();
      List<OrcProto.ColumnStatistics> colStatsProto = orcReader.getOrcProtoFileStatistics();
      Map<Integer, Long> columSizes = Maps.newHashMapWithExpectedSize(colStats.length);
      Map<Integer, Long> valueCounts = Maps.newHashMapWithExpectedSize(colStats.length);
      Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
      Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

      for (int i = 0; i < colStats.length; i++) {
        columSizes.put(i, colStats[i].getBytesOnDisk());
        valueCounts.put(i, colStats[i].getNumberOfValues());

        final OrcProto.ColumnStatistics protoStats = colStatsProto.get(i);
        final Types.NestedField col = schema.findField(i);
        if (col != null) {
          Optional<ByteBuffer> orcMin = fromOrcMin(col, protoStats);
          if (orcMin.isPresent()) {
            lowerBounds.put(i, orcMin.get());
          }
          Optional<ByteBuffer> orcMax = fromOrcMax(col, protoStats);
          if (orcMax.isPresent()) {
            upperBounds.put(i, orcMax.get());
          }
        }
      }

      return new Metrics(orcReader.getNumberOfRows(),
          columSizes,
          valueCounts,
          Collections.emptyMap(),
          lowerBounds,
          upperBounds);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to read footer of file: %s", file);
    }
  }

  private static Optional<ByteBuffer> fromOrcMin(Types.NestedField column,
                                                    OrcProto.ColumnStatistics columnStats) {
    ByteBuffer min = null;
    if (columnStats.hasIntStatistics()) {
      if (column.type().typeId() == Type.TypeID.INTEGER) {
        min = toByteBuffer(column.type(), (int) columnStats.getIntStatistics().getMinimum());
      } else {
        min = toByteBuffer(column.type(), columnStats.getIntStatistics().getMinimum());
      }
    } else if (columnStats.hasDoubleStatistics()) {
      min = toByteBuffer(column.type(), columnStats.getDoubleStatistics().getMinimum());
    } else if (columnStats.hasStringStatistics()) {
      min = toByteBuffer(column.type(), columnStats.getStringStatistics().getMinimum());
    } else if (columnStats.hasDecimalStatistics()) {
      min = toByteBuffer(column.type(), columnStats.getDecimalStatistics().getMinimum());
    } else if (columnStats.hasDateStatistics()) {
      min = toByteBuffer(column.type(), columnStats.getDateStatistics().getMinimum());
    } else if (columnStats.hasTimestampStatistics()) {
      min = toByteBuffer(column.type(), columnStats.getTimestampStatistics().getMinimum());
    }
    return Optional.ofNullable(min);
  }

  private static Optional<ByteBuffer> fromOrcMax(Types.NestedField column,
                                                    OrcProto.ColumnStatistics columnStats) {
    ByteBuffer max = null;
    if (columnStats.hasIntStatistics()) {
      if (column.type().typeId() == Type.TypeID.INTEGER) {
        max = toByteBuffer(column.type(), (int) columnStats.getIntStatistics().getMaximum());
      } else {
        max = toByteBuffer(column.type(), columnStats.getIntStatistics().getMaximum());
      }
    } else if (columnStats.hasDoubleStatistics()) {
      max = toByteBuffer(column.type(), columnStats.getDoubleStatistics().getMaximum());
    } else if (columnStats.hasStringStatistics()) {
      max = toByteBuffer(column.type(), columnStats.getStringStatistics().getMaximum());
    } else if (columnStats.hasDecimalStatistics()) {
      max = toByteBuffer(column.type(), columnStats.getDecimalStatistics().getMaximum());
    } else if (columnStats.hasDateStatistics()) {
      max = toByteBuffer(column.type(), columnStats.getDateStatistics().getMaximum());
    } else if (columnStats.hasTimestampStatistics()) {
      max = toByteBuffer(column.type(), columnStats.getTimestampStatistics().getMaximum());
    }
    return Optional.ofNullable(max);
  }

  static Map<Integer, ?> fromBufferMap(Schema schema, Map<Integer, ByteBuffer> map) {
    Map<Integer, ?> values = Maps.newHashMap();
    for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
      values.put(entry.getKey(),
          Conversions.fromByteBuffer(schema.findType(entry.getKey()), entry.getValue()));
    }
    return values;
  }
}
