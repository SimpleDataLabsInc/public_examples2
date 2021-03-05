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

@Visual(id = "ReadHistoricEvents", label = "ReadHistoricEvents", x = 80, y = 108, phase = 0)
object ReadHistoricEvents {

  @UsesDataset(id = "992", version = 0)
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
          .load("dbfs:/FileStore/tables/historic/*.json")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
