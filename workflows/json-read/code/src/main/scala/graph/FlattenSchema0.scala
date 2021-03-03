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

@Visual(id = "FlattenSchema0", label = "FlattenSchema0", x = 320, y = 135, phase = 0)
object FlattenSchema0 {

  def apply(spark: SparkSession, in: DataFrame): FlattenSchema = {
    import spark.implicits._

    val shortenNames = true
    val delimiter    = "-"

    val flattened = in
      .withColumn("allseasons",          explode_outer(col("allseasons")))
      .withColumn("allseasons-episodes", explode_outer(col("allseasons.episodes")))
    val out = flattened.select(col("allseasons.season_id").as("season_id"), col("allseasons.episodes").as("episodes"))

    out

  }

}
