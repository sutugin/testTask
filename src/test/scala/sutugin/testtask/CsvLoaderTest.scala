package sutugin.testtask

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite

class CsvLoaderTest
    extends FunSuite
    with SharedSparkContext
    with DataFrameSuiteBase {

  override lazy val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark session")
    .getOrCreate()
  override implicit def reuseContextIfPossible: Boolean = true

  test(
    "filter dataframe where all cells is not empty and not whitespace result 2") {
    val rdd = spark.sparkContext.parallelize(
      Seq(Row("John", "26"), Row("Lisa", "xyz"))
    )
    val schema = new StructType()
      .add(StructField("col0", StringType, true))
      .add(StructField("col1", StringType, true))

    val df = spark.createDataFrame(rdd, schema)
    val result = CsvLoaderApp.filterEmptyOrWhiteSpaceCells(df)
    assert(result.count(), 2)
  }

  test(
    "filter dataframe where all cells is empty or whitespace 2 columns result 0") {
    val rdd = spark.sparkContext.parallelize(
      Seq(Row("", ""), Row("  ", "  "), Row("  ", ""), Row("", "  "))
    )
    val schema = new StructType()
      .add(StructField("col0", StringType, true))
      .add(StructField("col1", StringType, true))

    val df = spark.createDataFrame(rdd, schema)
    val result = CsvLoaderApp.filterEmptyOrWhiteSpaceCells(df)
    assert(result.count(), 0)
  }

  test(
    "filter dataframe where 3 row is null or not empty, 1 row with empty, 1 row with whitespace, 1 row empty and whitespace result 3") {
    val rdd = spark.sparkContext.parallelize(
      Seq(Row("a", "b", "c"),
          Row(null, "z", null),
          Row("123", "321", null),
          Row("", "", ""),
          Row(" ", "  ", ""))
    )
    val schema = new StructType()
      .add(StructField("col0", StringType, true))
      .add(StructField("col1", StringType, true))
      .add(StructField("col2", StringType, true))

    val df = spark.createDataFrame(rdd, schema)
    val result = CsvLoaderApp.filterEmptyOrWhiteSpaceCells(df)
    assert(result.count(), 3)
  }

  test("cast columns") {
    val rdd = spark.sparkContext.parallelize(
      Seq(Row("John", "26"), Row("Lisa", "xyz"))
    )
    val castMap = Seq[Map[String, String]](
      Map("existing_col_name" -> "col0",
          "new_col_name" -> "Name",
          "new_data_type" -> "string"),
      Map("existing_col_name" -> "col1",
          "new_col_name" -> "total_years",
          "new_data_type" -> "integer")
    )
    val schema = new StructType()
      .add(StructField("col0", StringType, true))
      .add(StructField("col1", StringType, true))

    val schemaForValidate = new StructType()
      .add(StructField("Name", StringType, true))
      .add(StructField("total_years", IntegerType, true))

    val df = spark.createDataFrame(rdd, schema)
    val result = CsvLoaderApp.castQuery(castMap, df)
    assert(result.schema, schemaForValidate)

  }

  test("all together") {
    val schema = new StructType()
      .add(StructField("name", StringType, true))
      .add(StructField("age", StringType, true))
      .add(StructField("birthday", StringType, true))
      .add(StructField("gender", StringType, true))

    val schemaStep3 = new StructType()
      .add(StructField("first_name", StringType, true))
      .add(StructField("total_years", IntegerType, true))
      .add(StructField("d_o_b", DateType, true))

    val rowsRdd: RDD[Row] = spark.sparkContext.parallelize(
      Seq(
        Row("John", "26", "26-01-1995", "male"),
        Row("Lisa", "xyz", "26-01-1996", "female"),
        Row(null, "26", "26-01-1995", "male"),
        Row("John", "", "26-01-1995", "male"),
        Row("John", "26", "26-01-1995", "male"),
        Row("Pete", "36", null, "   "),
        Row("   ", "26", "26-01-1995", "male")
      )
    )
    val resultColumns = Array("Column", "Unique_values", "Values")

    val step1 = spark.createDataFrame(rowsRdd, schema)
    val step2 = CsvLoaderApp.filterEmptyOrWhiteSpaceCells(step1)
    val filteredCount = step2.count()
    assert(filteredCount, 4)

    val step3 = CsvLoaderApp.castQuery(
      CsvLoaderApp.jsonStringToSeqOfMap(Config.convertArgsStr),
      step2)
    assert(step3.schema, schemaStep3)

    val step4 = CsvLoaderApp.finalReport(step3)
    assert(step4.columns, resultColumns)

  }

}
