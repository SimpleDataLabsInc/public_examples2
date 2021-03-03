package graph

import io.delta.tables.DeltaTable
import io.prophecy.libs._
import io.prophecy.libs.UDFUtils._
import io.prophecy.libs.Component._
import io.prophecy.libs.DataHelpers._
import io.prophecy.libs.SparkFunctions._
import io.prophecy.libs.FixedFileFormatImplicits._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import graph._

@Visual(id = "OverwriteDelta", label = "OverwriteDelta", x = 690, y = 140, phase = 1)
object OverwriteDelta {

  @UsesDataset(id = "873", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("first_name",      StringType,  true),
            StructField("last_name",       StringType,  true),
            StructField("middle_initial",  StringType,  true),
            StructField("address",         StringType,  true),
            StructField("city",            StringType,  true),
            StructField("state",           StringType,  true),
            StructField("zip_code",        StringType,  true),
            StructField("customer_number", IntegerType, true),
            StructField("is_current",      BooleanType, true),
            StructField("is_first",        BooleanType, true),
            StructField("eff_start_date",  DateType,    true),
            StructField("eff_end_date",    DateType,    true)
          )
        )
        in.write
          .format("delta")
          .option("overwriteSchema", true)
          .mode("overwrite")
          .save("dbfs:/Prophecy/raj@prophecy.io/delta-scd2-customer")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
