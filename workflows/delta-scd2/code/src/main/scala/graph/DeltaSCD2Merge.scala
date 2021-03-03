package graph

import io.delta.tables.DeltaTable
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

@Visual(id = "DeltaSCD2Merge", label = "DeltaSCD2Merge", x = 675, y = 367, phase = 2)
object DeltaSCD2Merge {

  @UsesDataset(id = "874", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_number", IntegerType, true),
            StructField("first_name",      StringType,  true),
            StructField("last_name",       StringType,  true),
            StructField("middle_initial",  StringType,  true),
            StructField("address",         StringType,  true),
            StructField("city",            StringType,  true),
            StructField("state",           StringType,  true),
            StructField("zip_code",        StringType,  true),
            StructField("eff_start_date",  DateType,    true),
            StructField("eff_end_date",    DateType,    true),
            StructField("is_current",      BooleanType, true)
          )
        )
        val keyColumns = List("customer_number")
        val scdHistoricColumns =
          List("zip_code", "first_name", "last_name", "middle_initial", "address", "city", "state")
        val fromTimeColumn       = "eff_start_date"
        val toTimeColumn         = "eff_end_date"
        val minFlagColumn        = "is_first"
        val maxFlagColumn        = "is_current"
        val exitingTableLocation = "dbfs:/Prophecy/raj@prophecy.io/delta-scd2-customer"
        val flagY                = 1
        val flagN                = 0

        val updateColumns: Array[String] = in.columns
        val updatesDF = in

        val existingTable: DeltaTable = DeltaTable.forPath("dbfs:/Prophecy/raj@prophecy.io/delta-scd2-customer")
        val existingDF:    DataFrame  = existingTable.toDF
        val rowsToUpdate = updatesDF
          .join(existingDF, keyColumns)
          .where(
            existingDF.col(s"$maxFlagColumn") === lit(flagY) && (
              scdHistoricColumns
                .map { scdCol =>
                  existingDF.col(scdCol) =!= updatesDF.col(scdCol)
                }
                .reduce((c1, c2) => c1 || c2)
              )
          )
          .select(updateColumns.map(updatesDF.col): _*)
          .withColumn(s"$minFlagColumn", lit(flagN))
        val stagedUpdatesDF = rowsToUpdate
          .withColumn("mergeKey",                 lit(null))
          .union(updatesDF.withColumn("mergeKey", concat(keyColumns.map(col): _*)))
        existingTable
          .as("existingTable")
          .merge(
            stagedUpdatesDF.as("staged_updates"),
            concat(keyColumns.map(existingDF.col): _*) === stagedUpdatesDF("mergeKey")
          )
          .whenMatched(
            existingDF.col(s"$maxFlagColumn") === lit(flagY) && (
              scdHistoricColumns
                .map { scdCol =>
                  existingDF.col(scdCol) =!= stagedUpdatesDF.col(scdCol)
                }
                .reduce((c1, c2) => c1 || c2)
              )
          )
          .updateExpr(
            Map(
              s"$maxFlagColumn" -> s"$flagN",
              s"$toTimeColumn" -> s"staged_updates.$fromTimeColumn"
            )
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
