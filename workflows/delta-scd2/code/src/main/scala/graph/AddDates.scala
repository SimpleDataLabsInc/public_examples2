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

@Visual(id = "AddDates", label = "AddDates", x = 343, y = 362, phase = 2)
object AddDates {

  def apply(spark: SparkSession, in: DataFrame): SchemaTransformer = {
    import spark.implicits._

    val out = in
      .withColumn("eff_start_date", current_date())
      .withColumn("eff_end_date",   lit("9999-12-31").cast(DateType))
      .withColumn("is_current",     lit(1))
      .withColumn("is_first",       lit(1))

    out

  }

}
