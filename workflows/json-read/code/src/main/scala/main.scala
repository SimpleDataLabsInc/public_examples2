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

    val df_ReadHistoricEvents: Source        = ReadHistoricEvents(spark)
    val df_MakeFlat:           FlattenSchema = MakeFlat(spark,  df_ReadHistoricEvents)
    val df_Reformat4:          Reformat      = Reformat4(spark, df_MakeFlat)
    val (df_RowDistributor0_0, df_RowDistributor0_1, df_RowDistributor0_2): (
      RowDistributor,
      RowDistributor,
      RowDistributor
    ) = RowDistributor0(spark, df_Reformat4)
    val df_Reformat2:  Reformat  = Reformat2(spark,  df_RowDistributor0_1)
    val df_Reformat3:  Reformat  = Reformat3(spark,  df_RowDistributor0_2)
    val df_Aggregate0: Aggregate = Aggregate0(spark, df_MakeFlat)
    val df_OrderBy0:   OrderBy   = OrderBy0(spark,   df_Aggregate0)
    val df_Reformat0:  Reformat  = Reformat0(spark,  df_OrderBy0)
    val df_Reformat1:  Reformat  = Reformat1(spark,  df_RowDistributor0_0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("jsonread")
      .config("spark.default.parallelism", 4)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
