package com.cxi.cdp.data_processing

import com.cxi.cdp.data_processing.support.SparkTestSupport.{compareDataFrameWithList, jsonToDF}

import org.json4s._
import org.json4s.JsonDSL._
import org.apache.spark.sql.functions._

class SomeAggregateTest extends BaseSparkBatchJobTest{

  before {
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
  }

  test("Calculate Some Metric and verify it") {
    val actual = someTransform(spark.emptyDataFrame)

    compareDataFrameWithList(actual, List(Seq()), Seq.empty[String])
  }
  
}
