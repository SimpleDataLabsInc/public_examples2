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

@Visual(id = "RowDistributor0", label = "RowDistributor0", x = 517, y = 281, phase = 0)
object RowDistributor0 {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor, RowDistributor) = {
    import spark.implicits._

    val out0 = in.filter(col("category1") === lit("by topic"))
    val out1 = in.filter(col("category1") === lit("by place"))
    val rest = in.filter((col("category1") =!= lit("by topic")).and(col("category1") =!= lit("by place")))

    (out0, out1, rest)

  }

}
