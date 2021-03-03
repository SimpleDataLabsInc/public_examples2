package graph

import org.apache.spark.sql.types._
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
import config.ConfigStore._
import udfs.UDFs._
import graph._

@Visual(id = "Target0", label = "Target0", x = 1346, y = 136, phase = 0)
object Target0 {

  @UsesDataset(id = "879", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(StructField("name",        StringType,  false),
                StructField("amount",      LongType,    false),
                StructField("customer_id", IntegerType, false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("dbfs:/Prophecy/raj@prophecy.io/CustomerTotals")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
