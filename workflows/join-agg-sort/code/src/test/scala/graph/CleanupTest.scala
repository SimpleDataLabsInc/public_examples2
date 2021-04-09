
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
class CleanupTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Test_fTjTbTqHx4 for out columns: name, amount, customer_id") {
    val dfIn = inDf(Seq("customer_id", "first_name", "last_name", "amount"), Seq(
      Seq("31","Barrett","Amies","7280.12"),
      Seq("85","Tiffani","Mossman","4648.290000000001"),
      Seq("44","Allyn","Loade","2765.0800000000004"),
      Seq("12","Jeromy","Spaice","3796.2100000000005"),
      Seq("91","Artemus","Fatkin","4135.27"),
      Seq("22","Valery","Clubb","2114.52"),
      Seq("93","Earl","Colenutt","7265.6900000000005"),
      Seq("47","Papageno","La Wille","6052.400000000001"),
      Seq("1","Griz","Roubeix","6937.530000000001"),
      Seq("52","Trever","Swetman","2481.4"),
      Seq("13","Violet","Alston","2987.5800000000004"),
      Seq("6","Cassy","Gainsford","2256.26"),
      Seq("65","Evelyn","Goulbourne","3525.55"),
      Seq("3","Marjy","Della","6438.87"),
      Seq("20","Gray","Dominka","4725.56"),
      Seq("94","Ogdan","Bussetti","8335.87"),
      Seq("57","Mela","Venditti","4353.379999999999"),
      Seq("54","Vassili","Vick","3827.62"),
      Seq("96","Dane","Twigge","5402.28"),
      Seq("48","Queenie","Basire","5408.99"),
      Seq("5","Roselia","Trethewey","4056.0600000000004"),
      Seq("19","Joellyn","Startin","3238.85"),
      Seq("92","Lewes","Kennan","7509.770000000001"),
      Seq("53","Cathe","Cridlon","7000.629999999998"),
      Seq("64","Aguistin","Lipman","5586.46"),
      Seq("41","Marci","Klaes","5740.41"),
      Seq("15","Goldina","Cliff","6813.64"),
      Seq("37","Mariann","Tomsa","2421.5699999999997"),
      Seq("61","Baryram","Ades","2667.86"),
      Seq("88","Abbott","Duncombe","6019.62"),
      Seq("9","Natka","Birdsey","5052.31"),
      Seq("72","Sergio","Dudman","3224.8199999999997"),
      Seq("35","Viva","Schulke","10937.2"),
      Seq("4","Osborne","MacAdam","3515.9599999999996"),
      Seq("78","Tabor","Calbrathe","4964.92"),
      Seq("34","Mauricio","Panyer","3201.7200000000003"),
      Seq("81","Leigh","Mackett","5951.23"),
      Seq("28","Meyer","Marshland","7672.25"),
      Seq("76","Alfred","Garshore","4863.82"),
      Seq("26","Waite","Petschelt","4752.12")
    ))
    val dfOut = outDf(Seq("name", "amount", "customer_id"), Seq(
      Seq("Barrett Amies","7281","31"),
      Seq("Tiffani Mossman","4649","85"),
      Seq("Allyn Loade","2766","44"),
      Seq("Jeromy Spaice","3797","12"),
      Seq("Artemus Fatkin","4136","91"),
      Seq("Valery Clubb","2115","22"),
      Seq("Earl Colenutt","7266","93"),
      Seq("Papageno La Wille","6053","47"),
      Seq("Griz Roubeix","6938","1"),
      Seq("Trever Swetman","2482","52"),
      Seq("Violet Alston","2988","13"),
      Seq("Cassy Gainsford","2257","6"),
      Seq("Evelyn Goulbourne","3526","65"),
      Seq("Marjy Della","6439","3"),
      Seq("Gray Dominka","4726","20"),
      Seq("Ogdan Bussetti","8336","94"),
      Seq("Mela Venditti","4354","57"),
      Seq("Vassili Vick","3828","54"),
      Seq("Dane Twigge","5403","96"),
      Seq("Queenie Basire","5409","48"),
      Seq("Roselia Trethewey","4057","5"),
      Seq("Joellyn Startin","3239","19"),
      Seq("Lewes Kennan","7510","92"),
      Seq("Cathe Cridlon","7001","53"),
      Seq("Aguistin Lipman","5587","64"),
      Seq("Marci Klaes","5741","41"),
      Seq("Goldina Cliff","6814","15"),
      Seq("Mariann Tomsa","2422","37"),
      Seq("Baryram Ades","2668","61"),
      Seq("Abbott Duncombe","6020","88"),
      Seq("Natka Birdsey","5053","9"),
      Seq("Sergio Dudman","3225","72"),
      Seq("Viva Schulke","10938","35"),
      Seq("Osborne MacAdam","3516","4"),
      Seq("Tabor Calbrathe","4965","78"),
      Seq("Mauricio Panyer","3202","34"),
      Seq("Leigh Mackett","5952","81"),
      Seq("Meyer Marshland","7673","28"),
      Seq("Alfred Garshore","4864","76"),
      Seq("Waite Petschelt","4753","26")
    ))

    val dfOutComputed = graph.Cleanup(spark, dfIn)
    assertDFEquals(dfOut.select("name", "amount", "customer_id"), dfOutComputed.select("name", "amount", "customer_id"))
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
      StructField("name", StringType, nullable = true),
        StructField("amount", StringType, nullable = true),
        StructField("customer_id", StringType, nullable = true)
    ))


    val moddedValues = values.map(preProcess(_))
    val defaults = Map[String, Any](
      "name" -> "",
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

