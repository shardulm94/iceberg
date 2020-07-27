package org.apache.iceberg;

import org.apache.iceberg.spark.IcebergExtensions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.runtime.BoxedUnit;


public class TestSparkExtensions {

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestSparkExtensions.spark = SparkSession.builder()
        .master("local[2]")
        .withExtensions(new scala.runtime.AbstractFunction1<SparkSessionExtensions, BoxedUnit>() {
          @Override
          public BoxedUnit apply(SparkSessionExtensions extensions) {
            new IcebergExtensions().accept(extensions);
            return BoxedUnit.UNIT;
          }
        })
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkExtensions.spark;
    TestSparkExtensions.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testParser() {
    spark.sql("SELECT * FROM db.table");
  }
}
