package sutugin.testtask

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import sutugin.testtask.Config._

object CsvLoaderApp {

  /** Convert json string like {"a":"1", "b":2} to Map
    * @param input json string
    * @return Map[String, String]
    */
  def jsonStringToMap(input: String): Map[String, String] = {
    val mapper = new ObjectMapper

    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(input, classOf[Map[String, String]])
  }


  /** Convert json string like [{"a":"1"}, {"b":2}] to Array[Map]
    *
    * @param input json string
    * @return Seq[Map[String, String]
    */
  def jsonStringToSeqOfMap(input: String): Seq[Map[String, String]] = {
    val mapper = new ObjectMapper

    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(input, classOf[Seq[Map[String, String]]])
  }


  /**
    * Create spark session from settings
    * @param sparkConfig Map[String, String]
    * @param appName spark application name
    * @param master
    * @param logLevel
    * @return
    */
  def initSparkSession(sparkConfig: Map[String, String],
                       appName: String,
                       master: String = "local",
                       logLevel: String = "WARN"): SparkSession = {


    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    sparkConfig.foreach({ case (key, value) => conf.setIfMissing(key, value) })
    val spark: SparkSession = SparkSession.builder().appName(appName).config(conf = conf).getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  /** Load csv file whit predefine options
    *
    * @param spark SparkSession
    * @param options Map[String, String] with csv reader options
    * @param path String path to csv file
    * @return DataFrame
    */
  def loadCsv(spark: SparkSession, options: Map[String, String], path: String): DataFrame = {
    val reader: DataFrameReader = spark.read
    options.foreach({ case (key, value) => reader.option(key, value) })
    reader.csv(path)
  }

  /** Filter rows where cell contain empty string or whitespace    *
    * @param input DataFrame with String cells
    * @return filtered DataFrame
    */
  def filterEmptyOrWhiteSpaceCells(input: DataFrame): DataFrame = {
    val spark = input.sparkSession
    input.createOrReplaceTempView(Config.tempTableName)
    val emptyStringFilter =
      s"""select * from ${Config.tempTableName} where ${input.columns.map(x => s"($x is null or trim($x) !='')").mkString(" and ")}"""
    spark.sql(emptyStringFilter)
  }

  /** Convert data-type and names of the columns as per user’s choice
    * @param params Seq of Map[String, String] with user predefine cast rules
    * @param input DataFrame for cast
    * @return DataFrame with casted cols
    */
  def castQuery(params: Seq[Map[String, String]],
                input: DataFrame): DataFrame = {

    val spark = input.sparkSession

    input.createOrReplaceTempView(Config.tempTableName)

    val builder = StringBuilder.newBuilder
    builder.append("select")
    params.foreach(input => {
      val existing_col_name = input.getOrElse("existing_col_name", "")
      val new_col_name = input.getOrElse("new_col_name", "")
      val new_data_type = input.getOrElse("new_data_type", "")
      val date_expression = input.getOrElse("date_expression", "")
      if (existing_col_name.isEmpty || new_col_name.isEmpty || new_data_type.isEmpty) {
        throw new IllegalArgumentException("not all columns are specified")
      }

      val castedItem = castSubquery(existing_col_name, new_col_name, new_data_type, date_expression)
      builder.append(castedItem)
    })
    builder.delete(builder.length - 1, builder.length)
    builder.append(s" from ${Config.tempTableName}")

    spark.sql(builder.toString())
  }
  /**
    * Make cast query for chosen column
    * for extend of new types add new case
    * @param existColName String exist column name
    * @param newColName String new column bame
    * @param dataType String with spark.sql.types
    * @param dataTypeExpression rule for cast and format
    * @return string with sql cast query
    */
  def castSubquery(existColName: String,
                   newColName: String,
                   dataType: String,
                   dataTypeExpression: String): String = {
    dataType match {
      case "date" =>
        s" to_date($existColName,'$dataTypeExpression') as $newColName,"
      case _ => s" cast($existColName as $dataType) as $newColName,"
    }

  }


  /** Return a count of the total number of unique values and count of each unique value
    *
    * @param input DataFrame
    * @return DataFrame
    */
  def finalReport(input: DataFrame): DataFrame = {
    val spark = input.sparkSession

    input.createOrReplaceTempView(Config.tempTableName)
    spark.sql(s"cache table ${Config.tempTableName}")
    var resultDf =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Config.reportSchema)
    input.columns.foreach(x => {
      val query =
        s"select '$x' as Column, approx_count_distinct($x) as Unique_values ,concat_ws(':',$x, count(*)) as Values from ${Config.tempTableName} where $x is not null group by $x"
      val tmp = spark.sql(query)
      resultDf = resultDf.union(tmp)
    })
    spark.sql(s"uncache table ${Config.tempTableName}")
    resultDf.createOrReplaceTempView(Config.tempTableName)
    val query = s"select Column, sum(Unique_values) as Unique_values, collect_list(Values) as Values from ${Config.tempTableName} group by Column"
    spark.sql(query)
  }

  def main(args: Array[String]): Unit = {

    val params = CommandLineArgsParser.initConfig(args)

    val spark = initSparkSession(sparkConfig = Config.sparkSessionArgs, appName = params.appName)

    // step 1 - load csv from local file
    val step1 = loadCsv(spark, jsonStringToMap(params.csvOptions), params.inputCsv)

    // step 2 filter input empty or whitespace cells
    val step2 = filterEmptyOrWhiteSpaceCells(step1)

    // step 3 cast columns
    val step3 = castQuery(jsonStringToSeqOfMap(params.convertArgs), step2)

    // step 4
    val step4 = finalReport(step3)

    step4.coalesce(1).write.mode("overwrite").json(params.resultOutput)

  }

}
