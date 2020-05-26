package org.apache.iceberg.orc;

import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.Test;


public class TestOrcSchemaEvolution {

  @Test
  public void testUnionSchemaEvolution() {
    TypeDescription fileSchema = TypeDescription.fromString(
        "struct<unionCol:uniontype<int,string>>"
    );

    TypeDescription readSchema = TypeDescription.fromString(
        "struct<unionCol:uniontype<int,string>>"
    );

    SchemaEvolution evolution = new SchemaEvolution(fileSchema, readSchema, new Reader.Options());
    evolution.hasConversion();
  }

  private SchemaEvolution create(String fileSchema, String readSchema) {
    TypeDescription fileSchemaTD = TypeDescription.fromString(fileSchema);

    TypeDescription readSchemaTD = TypeDescription.fromString(readSchema);

    return new SchemaEvolution(fileSchemaTD, readSchemaTD, new Reader.Options());
  }
}
