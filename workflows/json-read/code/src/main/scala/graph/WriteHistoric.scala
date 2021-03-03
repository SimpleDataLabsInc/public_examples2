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

@Visual(id = "WriteHistoric", label = "WriteHistoric", x = 584, y = 123, phase = 0)
object WriteHistoric {

  @UsesDataset(id = "885", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("description", StringType, false),
            StructField("date",        StringType, false),
            StructField("category1",   StringType, false),
            StructField("category2",   StringType, false)
          )
        )
        in.write
          .format("delta")
          .option("fileFormat", "parquet")
          .mode("overwrite")
          .saveAsTable("default.prophecy_historic_event")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
