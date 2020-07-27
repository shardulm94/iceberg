package org.apache.iceberg.spark;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;


public class IcebergExtensions implements Consumer<SparkSessionExtensions> {

  @Override
  public void accept(SparkSessionExtensions sparkSessionExtensions) {
    sparkSessionExtensions.injectParser(new scala.runtime.AbstractFunction2<SparkSession, ParserInterface, ParserInterface>() {

      @Override
      public ParserInterface apply(SparkSession session, ParserInterface parser) {
        return new ParserInterface() {
          @Override
          public LogicalPlan parsePlan(String sqlText) throws ParseException {
            return parser.parsePlan(sqlText);
          }

          @Override
          public Expression parseExpression(String sqlText) throws ParseException {
            return parser.parseExpression(sqlText);
          }

          @Override
          public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
            return parser.parseTableIdentifier(sqlText);
          }

          @Override
          public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
            return parser.parseFunctionIdentifier(sqlText);
          }

          @Override
          public StructType parseTableSchema(String sqlText) throws ParseException {
            return parser.parseTableSchema(sqlText);
          }

          @Override
          public DataType parseDataType(String sqlText) throws ParseException {
            return parser.parseDataType(sqlText);
          }
        };
      }
    });
  }
}
