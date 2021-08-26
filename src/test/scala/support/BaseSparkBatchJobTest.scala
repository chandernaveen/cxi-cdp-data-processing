package com.cxi.cdp.data_processing
package support

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.Paths


class BaseSparkBatchJobTest extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {
  override implicit def reuseContextIfPossible: Boolean = true

  implicit val formats: DefaultFormats.type = DefaultFormats
  override lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override protected implicit def enableHiveSupport: Boolean = true

  override def beforeAll(): Unit = {
    fixHadoopOnWindows()
    super.beforeAll()
  }

  /**
   * Info on the issue:
   *
   * 1. http://letstalkspark.blogspot.com/2016/02/getting-started-with-spark-on-window-64.html
   * 2. https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
   * 3. https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#cannot-find-winutilsexe-on-windows
   *
   * Use https://github.com/steveloughran/winutils to download winutils for specific Hadoop version.
   *
   * Additionally make sure that path to the project dir does not contain whitespace characters in folder names,
   * as they are not correctly resolved by winutils.
   */
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

  override def conf: SparkConf = {
    val c = spark.conf
    val conf = new SparkConf()
    conf.setAll(c.getAll)
    conf.set("spark.sql.orc.impl", "hive")
    conf.set("spark.sql.dialect", "hiveql")
    conf.set("hive.default.fileformat", "Orc")
    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.debug.maxToStringFields", "1000")
    conf.set("spark.sql.session.timeZone", "UTC")
    conf
  }
}
