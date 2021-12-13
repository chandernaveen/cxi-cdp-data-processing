package com.cxi.cdp.data_processing
package support.change_data_feed

import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest
import org.mockito.Mockito._
import org.scalatest.Matchers

/* Uses Mockito spies to test specific Databricks commands (DESCRIBE HISTORY, SHOW TBLPROPERTIES). */
class ChangeDataFeedServiceTest extends BaseSparkBatchJobTest with Matchers {

    import ChangeDataFeedServiceTest._
    import spark.implicits._

    val cdfTable = "cross_cutting.cdf_tracker"
    val cdfService = new ChangeDataFeedService(cdfTable)
    val testTable = "refined_db.some_table"

    test("isCdfEnabled if delta.enableChangeDataFeed property is not set") {
        val sparkSpy = spy(spark)
        doReturn(Seq.empty[ShowTablePropertiesRow].toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")

        cdfService.isCdfEnabled(testTable)(sparkSpy) shouldBe false
    }

    test("isCdfEnabled if delta.enableChangeDataFeed is set to false") {
        val sparkSpy = spy(spark)
        val properties = Seq(
            ShowTablePropertiesRow("some_key", "some_value"),
            ShowTablePropertiesRow("delta.enableChangeDataFeed", "false")
        )
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")

        cdfService.isCdfEnabled(testTable)(sparkSpy) shouldBe false
    }

    test("isCdfEnabled if delta.enableChangeDataFeed is set to true") {
        val sparkSpy = spy(spark)
        val properties = Seq(
            ShowTablePropertiesRow("some_key", "some_value"),
            ShowTablePropertiesRow("delta.enableChangeDataFeed", "true")
        )
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")

        cdfService.isCdfEnabled(testTable)(sparkSpy) shouldBe true
    }

    test("getCdfEnabledVersion when CDF was enabled at the table creation time") {
        val sparkSpy = spy(spark)
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            DescibeHistoryRow(0L, "CREATE TABLE", Map("isManaged" -> "true", "properties" -> """{"delta.enableChangeDataFeed":"true"}""")),
            DescibeHistoryRow(1L, "UPDATE", Map.empty)
        )
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        cdfService.getCdfEnabledVersion(testTable)(sparkSpy) shouldBe 0L
    }

    test("getCdfEnabledVersion when CDF was enabled after the table creation time") {
        val sparkSpy = spy(spark)
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            DescibeHistoryRow(0L, "CREATE TABLE", Map("isManaged" -> "true")),
            DescibeHistoryRow(1L, "UPDATE", Map.empty),
            DescibeHistoryRow(2L, "CREATE TABLE", Map("properties" -> """{"delta.enableChangeDataFeed":"true"}""")),
            DescibeHistoryRow(3L, "CREATE TABLE", Map("properties" -> """{"delta.enableChangeDataFeed":"false"}""")),
            DescibeHistoryRow(4L, "CREATE TABLE", Map("properties" -> """{"delta.enableChangeDataFeed":"true"}"""))
        )
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        cdfService.getCdfEnabledVersion(testTable)(sparkSpy) shouldBe 4L
    }

}

object ChangeDataFeedServiceTest {

    case class ShowTablePropertiesRow(key: String, value: String)

    // This is only a subset of DESCRIBE HISTORY columns
    case class DescibeHistoryRow(version: Long, operation: String, operationParameters: Map[String, String])

}
