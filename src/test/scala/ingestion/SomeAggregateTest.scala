package com.cxi.cdp.data_processing
package ingestion

import support.BaseSparkBatchJobTest
import support.SparkTestSupport.compareDataFrameWithList

import org.apache.spark.sql.DataFrame

class SomeAggregateTest extends BaseSparkBatchJobTest {

  before {
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  def someTransform(df: DataFrame) = {
    df
  }

  test("Calculate Some Metric and verify it") {
    val actual = someTransform(spark.emptyDataFrame)

    compareDataFrameWithList(actual, List(Seq()), Seq.empty[String])
  }
}
