package com.cxi.cdp.data_processing
package support

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.{JObject, JsonAST}

import java.io.File
import java.nio.file.Paths

object SparkTestSupport extends BaseSparkBatchJobTest {
  //  implicit val formats: DefaultFormats.type = DefaultFormats
  dbutils.widgets.text("isTesting", "true")
  dbutils.widgets.text("dataSourcesSettings", "{}")
  dbutils.widgets.text("executionSettings", "{}")
  dbutils.widgets.text("configurationSettings", "{}")
  dbutils.widgets.text("loggerSettings",
    """{
      | "loggerName": "CuratedLogger",
		| "logSystem": "App",
		| "logLevel": "INFO",
		| "logAppender": "CuratedFile",
		| "isRootLogEnabled": "False"
	}""".stripMargin)
  val baseTestDataPath = "/work_area/hubpro-unit-tests/"
  val baseTestDataBase = "hubpro_test"

  def test(testName: String)(testFun: => Any /* Assertion */) {
    testFun
  }

  def jsonToDF(jsonObjects: JObject*): DataFrame = {
    import spark.implicits._
    val jsonStrings: Seq[String] = jsonObjects.map(j => compact(render(j)))
    spark.read.json(jsonStrings.toDS)
  }

  def jsonToDFWithSchema(schemaStr: String, jsonObjects: JObject*): DataFrame = {
    import spark.implicits._
    val jsonStrings = jsonObjects.map(j => compact(render(j)))
    spark.read.schema(schemaStr).json(jsonStrings.toDS)
  }

  def compareDataFrameWithList(actual: DataFrame, expected: List[Seq[Any]], fieldName: Seq[String]): Unit = {
    val actualQueriedData = actual.select(fieldName.map(r => col(r)): _*).collect()
    assert(actualQueriedData.length == expected.length, actualQueriedData.mkString("\n", "\n", "\n"))
    withClue("\nActual:\n" +
      actualQueriedData.sortWith((r1, r2) => r1.toString() < r2.toString()).mkString("\n", "\n", "\n") +
      "\nExpected:\n" +
      expected.sortWith((r1, r2) => r1.toString() < r2.toString()).mkString("\n", "\n", "\n")) {
      actualQueriedData.map(r =>
        fieldName.map(n => r.getAs[Any](n))
      ) should contain theSameElementsAs expected
    }
  }

  def compareDataFrameWithJson(actual: DataFrame, expected: Seq[JsonAST.JObject], fieldName: Seq[String]): Unit = {
    val actualQueriedData: Array[Row] = actual.select(fieldName.map(r => col(r)): _*).collect()
    val expectedExtractedData: Seq[Seq[Any]] = expected.map(s => fieldName.map(f => (s \ f).extract[Any]))
    assert(actualQueriedData.length == expected.length, actualQueriedData.mkString("\n", "\n", "\n"))
    withClue("\nActual:\n" +
      actualQueriedData.sortWith((r1, r2) => r1.toString() < r2.toString()).mkString("\n", "\n", "\n") +
      "\nExpected:\n" +
      expectedExtractedData.sortWith((r1, r2) => r1.toString() < r2.toString()).mkString("\n", "\n", "\n")) {
      actualQueriedData.map(r =>
        fieldName.map(n => r.getAs[Any](n))
      ) should contain theSameElementsAs expectedExtractedData
    }
  }

  private def fixHadoopOnWindows(): Unit = {
    if (System.getProperty("os.name").contains("Windows")) {
      val hadoopHomePath = Paths.get(s"src/test/resources/windows_os/hadoop-2.8.3")
      System.setProperty("hadoop.home.dir", hadoopHomePath.toAbsolutePath.toString)
      val tmpDir = hadoopHomePath.toAbsolutePath + "\\tmp"
      new File(tmpDir).mkdirs()
      Runtime.getRuntime.exec(s"$hadoopHomePath/bin/winutils.exe chmod -R 733 $tmpDir")
      System.setProperty("hive.exec.scratchdir", tmpDir + "\\hive")
    }
  }

}
