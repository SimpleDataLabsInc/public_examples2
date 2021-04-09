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

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_Customer:  Source   = Customer(spark)
    val df_Orders:    Source   = Orders(spark)
    val df_Join0:     Join     = Join0(spark,     df_Customer, df_Orders)
    val df_Cleanup:   Reformat = Cleanup(spark,   df_Join0)
    val df_Reformat0: Reformat = Reformat0(spark, df_Cleanup)
    val df_Reformat1: Reformat = Reformat1(spark, df_Reformat0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("dbdemo")
      .config("spark.default.parallelism", 4)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
