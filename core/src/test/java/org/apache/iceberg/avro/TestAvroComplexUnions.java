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

package org.apache.iceberg.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.*;


public class TestAvroComplexUnions {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateComplexUnions() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .and()
        .stringType()
        .endUnion()
        .nullDefault()
        .endRecord();

    Record unionRecord1 = new Record(avroSchema);
    unionRecord1.put("unionCol", 1);
    Record unionRecord2 = new Record(avroSchema);
    unionRecord2.put("unionCol", "test");
    List<Record> expected = ImmutableList.of(unionRecord1, unionRecord2);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
    }

//    Schema schema = new Schema(AvroSchemaUtil.convert(avroSchema).asStructType().fields());
    Schema schema = new Schema(
        optional(1, "unionCol", Types.StructType.of(
            optional(2, "member0", Types.IntegerType.get()),
            optional(3, "member1", Types.StringType.get())
        ))
    );
    List<Record> rows;
    try (AvroIterable<Record> reader = Avro.read(Files.localInput(testFile)).project(schema).build()) {
      rows = Lists.newArrayList(reader);
    }

    // Iceberg will return enums as strings, so compare String value of enum field instead of comparing Record objects
    for (int i = 0; i < expected.size(); i += 1) {
      String expectedEnumString =
          expected.get(i).get("unionCol") == null ? null : expected.get(i).get("unionCol").toString();
      Assert.assertEquals(expectedEnumString, rows.get(i).get("unionCol"));
    }
  }

  @Test
  public void writeAndValidateComplexUnions2() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .and()
        .stringType()
        .and()
        .record("nested")
        .fields()
        .name("col1")
        .type()
        .nullable().intType()
        .noDefault()
        .name("col2")
        .type().longType().longDefault(1234)
        .endRecord()
        .endUnion()
        .nullDefault()
        .endRecord();

    Record unionRecord1 = new Record(avroSchema);
    unionRecord1.put("unionCol", 1);
    Record unionRecord2 = new Record(avroSchema);
    unionRecord2.put("unionCol", "test");
    Record nested = new Record(avroSchema.getField("unionCol").schema().getTypes().get(3));
    nested.put("col1", 1);
    nested.put("col2", 2L);
    Record unionRecord3 = new Record(avroSchema);
    unionRecord3.put("unionCol", nested);
    List<Record> expected = ImmutableList.of(unionRecord1, unionRecord2, unionRecord3);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
      writer.append(unionRecord3);
    }

    org.apache.avro.Schema avroReadSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .and()
        .stringType()
        .and()
        .record("nested")
        .fields()
        .name("col1")
        .type()
        .nullable().intType()
        .noDefault()
        .name("col2")
        .type().longType().longDefault(1234)
        .endRecord()
        .and()
        .booleanType()
        .endUnion()
        .nullDefault()
        .endRecord();

    Schema schema = new Schema(AvroSchemaUtil.convert(avroReadSchema).asStructType().fields());
    List<Record> rows;
    try (AvroIterable<Record> reader = Avro.read(Files.localInput(testFile)).project(schema).build()) {
      rows = Lists.newArrayList(reader);
    }

    // Iceberg will return enums as strings, so compare String value of enum field instead of comparing Record objects
    for (int i = 0; i < expected.size(); i += 1) {
      String expectedEnumString =
          expected.get(i).get("unionCol") == null ? null : expected.get(i).get("unionCol").toString();
      Assert.assertEquals(expectedEnumString, rows.get(i).get("unionCol"));
    }
  }
}
