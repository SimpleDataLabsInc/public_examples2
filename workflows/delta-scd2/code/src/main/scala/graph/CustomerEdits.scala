package graph

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

@Visual(id = "CustomerEdits", label = "CustomerEdits", x = 89, y = 363, phase = 2)
object CustomerEdits {

  @UsesDataset(id = "872", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_number", IntegerType, true),
            StructField("first_name",      StringType,  true),
            StructField("last_name",       StringType,  true),
            StructField("middle_initial",  StringType,  true),
            StructField("address",         StringType,  true),
            StructField("city",            StringType,  true),
            StructField("state",           StringType,  true),
            StructField("zip_code",        StringType,  true)
          )
        )
        val data = Seq(
          Row(31568,  "William", "Chase",  "X", "57895 Sharp Way",    "Oldtown",      "CA", "98554-1285"),
          Row(289374, "John",    "Smith",  "G", "456 Derry Court",    "Springville",  "VT", "01234-5678"),
          Row(862447, "Susan",   "Harris", "L", "987 Central Avenue", "Central City", "MO", "49257-2657"),
          Row(932574, "Lisa",    "Cohen",  "S", "68846 Mason Road",   "Atlanta",      "GA", "26584-3591")
        )
        spark.createDataFrame(spark.sparkContext.parallelize(data), schemaArg)
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
