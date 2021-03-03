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

@Visual(id = "FixDates", label = "FixDates", x = 347, y = 143, phase = 1)
object FixDates {

  def apply(spark: SparkSession, in: DataFrame): SchemaTransformer = {
    import spark.implicits._

    val out = in
      .withColumn("eff_start_date", col("eff_start_date").cast(DateType))
      .withColumn("eff_end_date",   col("eff_end_date").cast(DateType))
      .drop("customer_dim_key")

    out

  }

}
