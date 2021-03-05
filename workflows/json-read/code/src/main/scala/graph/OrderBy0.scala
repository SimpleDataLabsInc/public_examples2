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

@Visual(id = "OrderBy0", label = "OrderBy0", x = 832, y = 114, phase = 0)
object OrderBy0 {

  def apply(spark: SparkSession, in: DataFrame): OrderBy = {
    import spark.implicits._

    val out = in.orderBy(col("count").desc)

    out

  }

}
