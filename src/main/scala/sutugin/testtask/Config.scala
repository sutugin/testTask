package sutugin.testtask

import org.apache.spark.sql.types.{LongType, StringType, StructType}

object Config {
  val appName = "testTask"

  val sparkSessionArgs: Map[String, String] = Map[String, String](
    "spark.executor.instances" -> "2",
    "spark.executor.memory" -> "4g",
    "spark.driver.memory" -> "4g",
    "spark.executor.cores" -> "4",
    "spark.yarn.executor.memoryOverhead" -> "2g",
    "spark.yarn.driver.memoryOverhead" -> "2g",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.driver.maxResultSize" -> "20g"
  )

  val inputCsv = "file:///home/"

  val tempTableName = "tempTable"
  val csvOptionsStr =
    """{"delimiter": ",", "charset": "UTF8", "infernSchema":"true", "header": "true"}"""

  val convertArgsStr =
    """[
      |{"existing_col_name":"name", "new_col_name":"first_name","new_data_type":"string"},
      |{"existing_col_name":"age", "new_col_name":"total_years","new_data_type":"integer"},
      |{"existing_col_name":"birthday", "new_col_name":"d_o_b", "new_data_type":"date","date_expression":"dd-MM-yyyy"}
      |]""".stripMargin

  val reportSchema = new StructType()
    .add("Column", StringType, false)
    .add("Unique_values", LongType, false)
    .add("Values", StringType, false)

  val resultOutput = "file:///home/"

}
