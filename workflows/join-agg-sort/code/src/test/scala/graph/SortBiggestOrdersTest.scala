
package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, when}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.junit.Assert
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

@RunWith(classOf[JUnitRunner])
class SortBiggestOrdersTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Test_2QEalYfdLp for out columns: amount") {
    val dfIn = inDf(Seq("amount"), Seq(
      Seq("7281"),
      Seq("4649"),
      Seq("2766"),
      Seq("3797"),
      Seq("4136"),
      Seq("2115"),
      Seq("7266"),
      Seq("6053"),
      Seq("6938"),
      Seq("2482"),
      Seq("2988"),
      Seq("2257"),
      Seq("3526"),
      Seq("6439"),
      Seq("4726"),
      Seq("8336"),
      Seq("4354"),
      Seq("3828"),
      Seq("5403"),
      Seq("5409"),
      Seq("4057"),
      Seq("3239"),
      Seq("7510"),
      Seq("7001"),
      Seq("5587"),
      Seq("5741"),
      Seq("6814"),
      Seq("2422"),
      Seq("2668"),
      Seq("6020"),
      Seq("5053"),
      Seq("3225"),
      Seq("10938"),
      Seq("3516"),
      Seq("4965"),
      Seq("3202"),
      Seq("5952"),
      Seq("7673"),
      Seq("4864"),
      Seq("4753")
    ))
    val dfOut = outDf(Seq("amount"), Seq(
      Seq("10938"),
      Seq("8336"),
      Seq("7673"),
      Seq("7510"),
      Seq("7281"),
      Seq("7266"),
      Seq("7001"),
      Seq("6938"),
      Seq("6814"),
      Seq("6439"),
      Seq("6053"),
      Seq("6020"),
      Seq("5952"),
      Seq("5741"),
      Seq("5587"),
      Seq("5409"),
      Seq("5403"),
      Seq("5053"),
      Seq("4965"),
      Seq("4864"),
      Seq("4753"),
      Seq("4726"),
      Seq("4649"),
      Seq("4354"),
      Seq("4136"),
      Seq("4057"),
      Seq("3828"),
      Seq("3797"),
      Seq("3526"),
      Seq("3516"),
      Seq("3239"),
      Seq("3225"),
      Seq("3202"),
      Seq("2988"),
      Seq("2766"),
      Seq("2668"),
      Seq("2482"),
      Seq("2422"),
      Seq("2257"),
      Seq("2115")
    ))

    val dfOutComputed = graph.SortBiggestOrders(spark, dfIn)
    assertDFEquals(dfOut.select("amount"), dfOutComputed.select("amount"))
  }

  def inDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val inSchema = StructType(List(
      StructField("amount", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
      "amount" -> ""
    )

    val loweredColumns = columns.map(_.toLowerCase)
    val missingColumns = (defaults.keySet -- columns.toSet).toList.filter(x => !loweredColumns.contains(x))
    val allColumns     = columns ++ missingColumns
    val allValues      = moddedValues.map { row => row ++ missingColumns.map(column => defaults(column)) }

    val rdd    = spark.sparkContext.parallelize(allValues.map(values => Row.fromSeq(values)))
    val schema = StructType(allColumns.map(column => StructField(column, StringType)))
    val df     = spark.createDataFrame(rdd, schema)

    nullify(df)
  }

  def outDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val outSchema = StructType(List(
      StructField("amount", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
      "amount" -> ""
    )

    val loweredColumns = columns.map(_.toLowerCase)
    val missingColumns = (defaults.keySet -- columns.toSet).toList.filter(x => !loweredColumns.contains(x))
    val allColumns     = columns ++ missingColumns
    val allValues      = moddedValues.map { row => row ++ missingColumns.map(column => defaults(column)) }

    val rdd    = spark.sparkContext.parallelize(allValues.map(values => Row.fromSeq(values)))
    val schema = StructType(allColumns.map(column => StructField(column, StringType)))
    val df     = spark.createDataFrame(rdd, schema)

    df
  }

  
  def preProcess(s: Seq[String]) = {
    val isoDateTimePat = ("^(\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:" +
      "[0-5]\\d\\.\\d+([+-][0-2]\\d:[0-5]\\d|Z))|" +
      "(\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:" +
      "[0-5]\\d([+-][0-2]\\d:[0-5]\\d|Z))|" +
      "(\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d([+-][0-2]\\d:[0-5]\\d|Z))$").r
    val isoDatePat = "^\\d{4}-[01]\\d-[0-3]\\d$".r

    s.map(x =>
      if (isoDateTimePat.findFirstIn(x).isDefined)
        Timestamp.valueOf(LocalDateTime.parse(x.substring(0, 23))).toString
      else if (isoDatePat.findFirstIn(x).isDefined)
        Date.valueOf(x).toString
      else
        x
    )
  }
  
  private def nullify(df: DataFrame): DataFrame = {
    val exprs = df.schema.map { f =>
      f.dataType match {
        case StringType =>
          when(
            col(f.name) === "null",
            lit(null: String).cast(StringType)).otherwise(col(f.name)
          ).as(f.name)
        case _ => col(f.name)
      }
    }

    df.select(exprs: _*)
  }

  
  def postProcess(origDf: DataFrame) : DataFrame = {
    val df = origDf.select(origDf.columns.map(c => col(c).cast(StringType)) : _*).na.fill("null")
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = true))))
  }


  /**
   * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
   * When comparing inexact fields uses tol.
   *
   * @param tol max acceptable tolerance, should be less than 1.
   */
  def assertDFEquals(resultUnsorted: DataFrame, expectedUnsorted: DataFrame, tol: Double = 0.0) {
    def _sort(df: DataFrame) = df.orderBy(df.columns.map(col):_*)
    
    val expected = _sort(postProcess(expectedUnsorted))
    val result = _sort(postProcess(resultUnsorted))
    try {
      expected.rdd.cache
      result.rdd.cache
      
      // assert row count
      Assert.assertEquals(
        s"Length not Equal." +
          s"\nExpected: ${expected.take(maxUnequalRowsToShow).mkString(",")}" +
          s"\nActual: ${result.take(maxUnequalRowsToShow).mkString(",")}",
        expected.rdd.count,
        result.rdd.count
      )
      
      val expectedIndexValue = expected.rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
      val resultIndexValue   = result.rdd.zipWithIndex().map { case (row, idx) => (idx, row) }

      val unequalRDD = expectedIndexValue
        .join(resultIndexValue)
        .filter { case (idx, (r1, r2)) => !SparkTestingUtils.rowEquals(r1, r2, tol) }
      
      // assert all rows literal equality
      Assert.assertEquals(
        s"Expected != Actual\nMismatch: ${unequalRDD.take(maxUnequalRowsToShow).mkString("    +    ")}",
        unequalRDD.take(maxUnequalRowsToShow).length,
        0
      )
      
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }
}

