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

@Visual(id = "ReadGoT", label = "ReadGoT", x = 82, y = 134, phase = 0)
object ReadGoT {

  @UsesDataset(id = "881", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField(
              "allseasons",
              ArrayType(
                StructType(
                  Array(
                    StructField("episodes",
                                ArrayType(StructType(
                                            Array(StructField("episode", StringType,                  true),
                                                  StructField("quotes",  ArrayType(StringType, true), true)
                                            )
                                          ),
                                          true
                                ),
                                true
                    ),
                    StructField("season_id", LongType, true)
                  )
                ),
                true
              ),
              true
            )
          )
        )
        spark.read
          .format("json")
          .option("multiLine", true)
          .schema(schemaArg)
          .load("dbfs:/FileStore/tables/allseasons_raj.json")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
