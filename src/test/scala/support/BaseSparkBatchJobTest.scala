package com.cxi.cdp.data_processing
package support

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SparkSessionProvider}
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.Paths
import java.util.UUID

class BaseSparkBatchJobTest extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {
    override implicit def reuseContextIfPossible: Boolean = true

    override protected implicit def enableHiveSupport: Boolean = true

    override def beforeAll(): Unit = {
        fixHadoopOnWindows()
        super.beforeAll()
        spark.sparkContext.addJar("target/scala-2.12/cxi_cdp_data_processing_assembly_test_2_12.jar")
    }

    /** Info on the issue:
      *
      * 1. http://letstalkspark.blogspot.com/2016/02/getting-started-with-spark-on-window-64.html
      * 2. https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
      * 3. https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#cannot-find-winutilsexe-on-windows
      *
      * Use https://github.com/cdarlint/winutils to download winutils for specific Hadoop version.
      *
      * Additionally make sure that path to the project dir does not contain whitespace characters in folder names,
      * as they are not correctly resolved by winutils.
      */
    private def fixHadoopOnWindows(): Unit = {
        if (System.getProperty("os.name").contains("Windows")) {
            val hadoopHomePath = Paths.get(s"src/test/resources/windows_os/hadoop-3.2.0")
            System.setProperty("hadoop.home.dir", hadoopHomePath.toAbsolutePath.toString)
            val tmpDir = hadoopHomePath.toAbsolutePath + "\\tmp"
            new File(tmpDir).mkdirs()
            Runtime.getRuntime.exec(s"$hadoopHomePath/bin/winutils.exe chmod -R 733 $tmpDir")
            System.setProperty("hive.exec.scratchdir", tmpDir + "\\hive")
        }
    }

    override def conf: SparkConf = {
        val conf = super.conf
        conf.set("spark.databricks.service.server.enabled", "false")
        conf.set("spark.sql.dialect", "hiveql")
        conf.set("hive.default.fileformat", "delta")
        conf.set("spark.sql.shuffle.partitions", "1")
        conf.set("spark.debug.maxToStringFields", "1000")
        conf.set("spark.sql.session.timeZone", "UTC")
        conf.set("spark.databricks.service.client.checkDeps", "false")
        conf.set("spark.databricks.service.client.autoAddDeps", "false")
        conf
    }

    /** Generates a unique table name with the provided prefix.
      * Useful in tests that create tables in Databricks to allow parallel test runs.
      */
    def generateUniqueTableName(tablePrefix: String): String = {
        val randomSuffix = UUID.randomUUID().toString.substring(0, 8)
        tablePrefix + "_" + randomSuffix
    }

}
