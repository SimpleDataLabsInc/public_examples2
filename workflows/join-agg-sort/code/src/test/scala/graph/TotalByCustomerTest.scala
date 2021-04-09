
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
class TotalByCustomerTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Test_KaKVeScLbV for out columns: first_name, last_name, amount") {
    val dfIn = inDf(Seq("first_name", "last_name", "amount", "customer_id"), Seq(
      Seq("Waite","Petschelt","586.08","26"),
      Seq("Waite","Petschelt","9.85","26"),
      Seq("Constance","Sleith","287.5","63"),
      Seq("Viva","Schulke","514.19","35"),
      Seq("Barrett","Amies","154.41","31"),
      Seq("Kit","Skamell","924.89","51"),
      Seq("Viva","Schulke","198.05","35"),
      Seq("Gillan","Heritege","121.03","100"),
      Seq("Ogdan","Bussetti","551.87","94"),
      Seq("Homer","Lindstedt","343.59","62"),
      Seq("Marjy","Della","60.72","3"),
      Seq("Gillan","Heritege","449.74","100"),
      Seq("Ogdan","Bussetti","915.25","94"),
      Seq("Balduin","Birdsall","726.97","84"),
      Seq("Barrett","Amies","607.46","31"),
      Seq("Jonis","Bigly","174.29","10"),
      Seq("Barrett","Amies","747.61","31"),
      Seq("Dallas","Davoren","383.14","14"),
      Seq("Twyla","Coppledike","283.1","98"),
      Seq("Casey","McFater","825.13","29"),
      Seq("Leigha","Yurikov","242.69","45"),
      Seq("Dallas","Davoren","218.55","14"),
      Seq("Heywood","Stork","563.05","42"),
      Seq("Evelyn","Goulbourne","556.41","65"),
      Seq("Sibley","Reasun","269.46","8"),
      Seq("Griz","Roubeix","250.99","1"),
      Seq("Baryram","Ades","745.73","61"),
      Seq("Lewes","Kennan","586.83","92"),
      Seq("Uriel","Iacivelli","259.29","71"),
      Seq("Earl","Colenutt","778.37","93"),
      Seq("Baryram","Ades","237.5","61"),
      Seq("Leigh","Mackett","598.37","81"),
      Seq("Viva","Schulke","372.04","35"),
      Seq("Tish","Mankor","809.84","83"),
      Seq("Violet","Alston","877.45","13"),
      Seq("Deeanne","Kaye","776.76","77"),
      Seq("Deeanne","Kaye","843.36","77"),
      Seq("Aguistin","Lipman","71.47","64"),
      Seq("Alyssa","Mance","305.39","25"),
      Seq("Queenie","Basire","696.56","48")
    ))
    val dfOut = outDf(Seq("first_name", "last_name", "amount"), Seq(
      Seq("Barrett","Amies","1509.48"),
      Seq("Evelyn","Goulbourne","556.41"),
      Seq("Leigh","Mackett","598.37"),
      Seq("Waite","Petschelt","595.9300000000001"),
      Seq("Earl","Colenutt","778.37"),
      Seq("Griz","Roubeix","250.99"),
      Seq("Violet","Alston","877.45"),
      Seq("Marjy","Della","60.72"),
      Seq("Ogdan","Bussetti","1467.12"),
      Seq("Queenie","Basire","696.56"),
      Seq("Lewes","Kennan","586.83"),
      Seq("Aguistin","Lipman","71.47"),
      Seq("Baryram","Ades","983.23"),
      Seq("Viva","Schulke","1084.28"),
      Seq("Sibley","Reasun","269.46"),
      Seq("Gillan","Heritege","570.77"),
      Seq("Balduin","Birdsall","726.97"),
      Seq("Kit","Skamell","924.89"),
      Seq("Constance","Sleith","287.5"),
      Seq("Jonis","Bigly","174.29"),
      Seq("Deeanne","Kaye","1620.12"),
      Seq("Leigha","Yurikov","242.69"),
      Seq("Alyssa","Mance","305.39"),
      Seq("Homer","Lindstedt","343.59"),
      Seq("Casey","McFater","825.13"),
      Seq("Twyla","Coppledike","283.1"),
      Seq("Tish","Mankor","809.84"),
      Seq("Uriel","Iacivelli","259.29"),
      Seq("Dallas","Davoren","601.69"),
      Seq("Heywood","Stork","563.05")
    ))

    val dfOutComputed = graph.TotalByCustomer(spark, dfIn)
    assertDFEquals(dfOut.select("first_name", "last_name", "amount"), dfOutComputed.select("first_name", "last_name", "amount"))
  }

  def inDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val inSchema = StructType(List(
      StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("amount", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
      "amount" -> "",
      "customer_id" -> ""
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
      StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("amount", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
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

