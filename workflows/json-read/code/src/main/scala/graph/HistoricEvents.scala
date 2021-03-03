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

@Visual(id = "HistoricEvents", label = "HistoricEvents", x = 70, y = 122, phase = 0)
object HistoricEvents {

  @UsesDataset(id = "884", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField(
              "result",
              StructType(
                Array(
                  StructField("count", StringType, true),
                  StructField(
                    "events",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("category1",   StringType, true),
                          StructField("category2",   StringType, true),
                          StructField("date",        StringType, true),
                          StructField("description", StringType, true),
                          StructField("granularity", StringType, true),
                          StructField("lang",        StringType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            )
          )
        )
        spark.read
          .format("json")
          .option("multiLine", true)
          .schema(schemaArg)
          .load("dbfs:/FileStore/tables/old_events.json")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
