package com.cxi.cdp.data_processing
package support.template

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession


object VSCodeTest {
  //The following object is meant to test a VSCode IDE using Databricks Connect plugin
  //The code should run using Metals and local sbt
    def main(args: Array[String]): Unit = {
      val spark = SparkSession
            .builder()
            .appName("Test")
            .config("spark.master", "local")
            .getOrCreate();
      //val spark = SparkSession.builder().getOrCreate()
      println("Hello World!")
      //val test = DBUtils.fs.help("cp")

      //DBUtils.fs.ls("/mnt/raw_zone/")
      val dfRaw = spark.read.format("delta").load("/mnt/raw_zone/cxi/template/test/test_products")

      dfRaw.show()
    }
}