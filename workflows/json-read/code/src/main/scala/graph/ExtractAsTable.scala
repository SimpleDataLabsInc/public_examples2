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

@Visual(id = "ExtractAsTable", label = "ExtractAsTable", x = 305, y = 124, phase = 0)
object ExtractAsTable {

  def apply(spark: SparkSession, in: DataFrame): FlattenSchema = {
    import spark.implicits._

    val shortenNames = true
    val delimiter    = "-"

    val flattened = in.withColumn("result-events", explode_outer(col("result.events")))
    val out = flattened.select(
      col("result-events.description").as("description"),
      col("result-events.date").as("date"),
      col("result-events.category1").as("category1"),
      col("result-events.category2").as("category2")
    )

    out

  }

}
