
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
class PerCustomerTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Test_RSLVZoOHUN for out columns: first_name, last_name, amount, customer_id") {
    val dfLeft = leftDf(Seq("customer_id", "first_name", "last_name"), Seq(
      Seq("1","Griz","Roubeix"),
      Seq("2","Carleen","Eles"),
      Seq("11","Adaline","Loudwell"),
      Seq("12","Jeromy","Spaice"),
      Seq("13","Violet","Alston"),
      Seq("14","Dallas","Davoren"),
      Seq("15","Goldina","Cliff"),
      Seq("16","Brit","Winnett"),
      Seq("17","Christa","Bunclark"),
      Seq("18","Barnett","Rolinson"),
      Seq("19","Joellyn","Startin"),
      Seq("20","Gray","Dominka"),
      Seq("3","Marjy","Della"),
      Seq("21","Sonya","Petrelli"),
      Seq("22","Valery","Clubb"),
      Seq("23","Imojean","Schwerin"),
      Seq("24","Elsbeth","Vanne"),
      Seq("25","Alyssa","Mance"),
      Seq("26","Waite","Petschelt"),
      Seq("27","Mitchell","Sumshon"),
      Seq("28","Meyer","Marshland"),
      Seq("29","Casey","McFater"),
      Seq("30","Eldridge","Manifold"),
      Seq("4","Osborne","MacAdam"),
      Seq("31","Barrett","Amies"),
      Seq("32","Freida","Riccardini"),
      Seq("33","Ginevra","Tocknell"),
      Seq("34","Mauricio","Panyer"),
      Seq("35","Viva","Schulke"),
      Seq("36","Tony","Bickmore"),
      Seq("37","Mariann","Tomsa"),
      Seq("38","Temp","Mash"),
      Seq("39","Cazzie","Le Fevre"),
      Seq("40","Lavena","Fiddler"),
      Seq("5","Roselia","Trethewey"),
      Seq("6","Cassy","Gainsford"),
      Seq("7","Robers","Scala"),
      Seq("8","Sibley","Reasun"),
      Seq("9","Natka","Birdsey"),
      Seq("10","Jonis","Bigly")
    ))
    val dfRight = rightDf(Seq("customer_id", "amount"), Seq(
      Seq("26","586.08"),
      Seq("26","9.85"),
      Seq("63","287.5"),
      Seq("35","514.19"),
      Seq("31","154.41"),
      Seq("51","924.89"),
      Seq("35","198.05"),
      Seq("100","121.03"),
      Seq("94","551.87"),
      Seq("62","343.59"),
      Seq("3","60.72"),
      Seq("100","449.74"),
      Seq("94","915.25"),
      Seq("84","726.97"),
      Seq("31","607.46"),
      Seq("10","174.29"),
      Seq("31","747.61"),
      Seq("14","383.14"),
      Seq("98","283.1"),
      Seq("29","825.13"),
      Seq("45","242.69"),
      Seq("14","218.55"),
      Seq("42","563.05"),
      Seq("65","556.41"),
      Seq("8","269.46"),
      Seq("1","250.99"),
      Seq("61","745.73"),
      Seq("92","586.83"),
      Seq("71","259.29"),
      Seq("93","778.37"),
      Seq("61","237.5"),
      Seq("81","598.37"),
      Seq("35","372.04"),
      Seq("83","809.84"),
      Seq("13","877.45"),
      Seq("77","776.76"),
      Seq("77","843.36"),
      Seq("64","71.47"),
      Seq("25","305.39"),
      Seq("48","696.56")
    ))
    val dfOut = outDf(Seq("first_name", "last_name", "amount", "customer_id"), Seq(
      Seq("Barrett","Amies","154.41","31"),
      Seq("Barrett","Amies","607.46","31"),
      Seq("Barrett","Amies","747.61","31"),
      Seq("Waite","Petschelt","586.08","26"),
      Seq("Waite","Petschelt","9.85","26"),
      Seq("Griz","Roubeix","250.99","1"),
      Seq("Violet","Alston","877.45","13"),
      Seq("Marjy","Della","60.72","3"),
      Seq("Viva","Schulke","514.19","35"),
      Seq("Viva","Schulke","198.05","35"),
      Seq("Viva","Schulke","372.04","35"),
      Seq("Sibley","Reasun","269.46","8"),
      Seq("Jonis","Bigly","174.29","10"),
      Seq("Alyssa","Mance","305.39","25"),
      Seq("Casey","McFater","825.13","29"),
      Seq("Dallas","Davoren","383.14","14"),
      Seq("Dallas","Davoren","218.55","14")
    ))

    val dfOutComputed = graph.PerCustomer(spark, dfLeft, dfRight)
    assertDFEquals(dfOut.select("first_name", "last_name", "amount", "customer_id"), dfOutComputed.select("first_name", "last_name", "amount", "customer_id"))
  }

  def leftDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val leftSchema = StructType(List(
      StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true)
    ))

      val rightSchema = StructType(List(
      StructField("amount", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
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


  def rightDf(columns: Seq[String], values: Seq[Seq[String]]): DataFrame = {
    val leftSchema = StructType(List(
      StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true)
    ))

      val rightSchema = StructType(List(
      StructField("amount", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
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

