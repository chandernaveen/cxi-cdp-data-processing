package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import support.BaseSparkBatchJobTest

import org.mockito.Mockito.when
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

class WriteOptionsFunctionsTest extends BaseSparkBatchJobTest {

    test("test replaceWhereForFeedDate") {
        // given
        val feedDate = "2022-02-24"

        // when
        val writeOptions = WriteOptionsFunctions.replaceWhereForFeedDate(feedDate)(spark.emptyDataFrame, Map())

        // then
        writeOptions shouldBe Map("replaceWhere" -> s"feed_date = '$feedDate'")
    }

    test("test replaceWhereForSingleColumn") {
        // given
        val df = spark
            .createDataFrame(
                Seq(
                    ("2022-02-24", 1, true),
                    ("2022-02-25", 2, false),
                    ("2022-02-24", 3, true)
                )
            )
            .toDF("col1", "col2", "col3")

        // when
        val writeOptions = WriteOptionsFunctions.replaceWhereForSingleColumn()(df, Map("controlCol" -> "col1"))

        // then
        writeOptions shouldBe Map("replaceWhere" -> s"col1 in ('2022-02-24','2022-02-25')")
    }

    test("test getWriteOptions - no write option function") {
        // given
        val config = mock[FileIngestionFrameworkConfig]
        val writeOptions = Map("foo" -> "bar")
        when(config.writeOptions).thenReturn(writeOptions)
        when(config.writeOptionsFunctionName).thenReturn(None)

        // when
        val allWriteOptions = WriteOptionsFunctions.getWriteOptions(spark.emptyDataFrame, "2022-02-24", config)

        // then
        allWriteOptions shouldBe writeOptions
    }

    test("test getWriteOptions - with write option function") {
        // given
        val feedDate = "2022-02-24"
        val config = mock[FileIngestionFrameworkConfig]
        val writeOptions = Map("foo" -> "bar")
        when(config.writeOptions).thenReturn(writeOptions)
        when(config.writeOptionsFunctionName).thenReturn(Some("replaceWhereForFeedDate"))

        // when
        val allWriteOptions = WriteOptionsFunctions.getWriteOptions(spark.emptyDataFrame, feedDate, config)

        // then
        allWriteOptions shouldBe (writeOptions ++ Map("replaceWhere" -> s"feed_date = '$feedDate'"))
    }

}
