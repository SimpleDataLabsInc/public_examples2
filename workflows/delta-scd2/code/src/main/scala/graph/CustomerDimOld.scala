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

@Visual(id = "CustomerDimOld", label = "CustomerDimOld", x = 91, y = 141, phase = 1)
object CustomerDimOld {

  @UsesDataset(id = "876", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_dim_key", StringType,  true),
            StructField("first_name",       StringType,  true),
            StructField("last_name",        StringType,  true),
            StructField("middle_initial",   StringType,  true),
            StructField("address",          StringType,  true),
            StructField("city",             StringType,  true),
            StructField("state",            StringType,  true),
            StructField("zip_code",         StringType,  true),
            StructField("customer_number",  IntegerType, true),
            StructField("eff_start_date",   StringType,  true),
            StructField("eff_end_date",     StringType,  true),
            StructField("is_current",       StringType,  true),
            StructField("is_first",         StringType,  true)
          )
        )
        val data = Seq(
          Row("1",
              "John",
              "Smith",
              "G",
              "123 MainStreet",
              "Springville",
              "VT",
              "01234-5678",
              289374,
              "1/1/14",
              "12/31/99",
              "1",
              "1"
          ),
          Row("2",
              "Susan",
              "Jones",
              "L",
              "987 Central Avenue",
              "Central City",
              "MO",
              "49257-2657",
              862447,
              "3/23/15",
              "11/17/18",
              "0",
              "1"
          ),
          Row("3",
              "Susan",
              "Harris",
              "L",
              "987 Central Avenue",
              "Central City",
              "MO",
              "49257-2657",
              862447,
              "11/18/18",
              "12/31/99",
              "1",
              "0"
          ),
          Row("4",
              "William",
              "Chase",
              "X",
              "57895 Sharp Way",
              "Oldtown",
              "CA",
              "98554-1285",
              31568,
              "12/7/18",
              "12/31/99",
              "1",
              "1"
          )
        )
        spark.createDataFrame(spark.sparkContext.parallelize(data), schemaArg)
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
