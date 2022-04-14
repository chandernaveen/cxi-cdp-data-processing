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
    val testConsumerId = "consumer-1"
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

    test("getEarliestCdfEnabledVersion when CDF was enabled at the table creation time") {
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

        cdfService.getEarliestCdfEnabledVersion(testTable)(sparkSpy) shouldBe Some(0L)
    }

    test("getEarliestCdfEnabledVersion when CDF was enabled but that particular version is no longer available") {
        val sparkSpy = spy(spark)
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            // CDF-enabled version 0 was vacuumed
            DescibeHistoryRow(1L, "UPDATE", Map.empty)
        )
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        cdfService.getEarliestCdfEnabledVersion(testTable)(sparkSpy) shouldBe None
    }

    test("getEarliestCdfEnabledVersion when CDF was enabled after the table creation time") {
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

        cdfService.getEarliestCdfEnabledVersion(testTable)(sparkSpy) shouldBe Some(4L)
    }

    test("getStartVersion fails if some unprocessed versions are missing and there was no previous processing") {
        val sparkSpy = spy(spark)
        val cdfTrackerRows = Seq.empty[CdfTrackerRow]
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            // versions 0 - 2 were vacuumed
            DescibeHistoryRow(3L, "CREATE TABLE", Map("isManaged" -> "true")),
            DescibeHistoryRow(4L, "UPDATE", Map.empty)
        )
        doReturn(cdfTrackerRows.toDF, Seq.empty: _*)
            .when(sparkSpy).table(cdfTable)
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        assertThrows[Exception] {
            cdfService.getStartVersion(testConsumerId, testTable)(sparkSpy)
        }
    }

    test("getStartVersion works if all unprocessed versions are present and there was no previous processing") {
        val sparkSpy = spy(spark)
        val cdfTrackerRows = Seq.empty[CdfTrackerRow]
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            DescibeHistoryRow(0L, "CREATE TABLE", Map("isManaged" -> "true")),
            DescibeHistoryRow(1L, "UPDATE", Map.empty),
            DescibeHistoryRow(2L, "CREATE TABLE", Map("properties" -> """{"delta.enableChangeDataFeed":"true"}""")),
            DescibeHistoryRow(3L, "UPDATE", Map.empty)
        )
        doReturn(cdfTrackerRows.toDF, Seq.empty: _*)
            .when(sparkSpy).table(cdfTable)
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        cdfService.getStartVersion(testConsumerId, testTable)(sparkSpy) shouldBe 2L
    }

    test("getStartVersion fails if some unprocessed versions are missing and there was previous processing") {
        val sparkSpy = spy(spark)
        val cdfTrackerRows = Seq(CdfTrackerRow(testConsumerId, testTable, 1L))
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            // versions 0 - 2 were vacuumed
            DescibeHistoryRow(3L, "CREATE TABLE", Map("isManaged" -> "true")),
            DescibeHistoryRow(4L, "UPDATE", Map.empty)
        )
        doReturn(cdfTrackerRows.toDF, Seq.empty: _*)
            .when(sparkSpy).table(cdfTable)
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        assertThrows[Exception] {
            cdfService.getStartVersion(testConsumerId, testTable)(sparkSpy)
        }
    }

    test("getStartVersion works if all unprocessed versions are present and there was previous processing") {
        val sparkSpy = spy(spark)
        val cdfTrackerRows = Seq(CdfTrackerRow(testConsumerId, testTable, 1L))
        val properties = Seq(ShowTablePropertiesRow("delta.enableChangeDataFeed", "true"))
        val historyRows = Seq(
            // version 0 was vacuumed but version 2 is present
            DescibeHistoryRow(1L, "CREATE TABLE", Map("isManaged" -> "true")),
            DescibeHistoryRow(2L, "UPDATE", Map.empty),
            DescibeHistoryRow(3L, "UPDATE", Map.empty)
        )
        doReturn(cdfTrackerRows.toDF, Seq.empty: _*)
            .when(sparkSpy).table(cdfTable)
        doReturn(properties.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"SHOW TBLPROPERTIES $testTable")
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        cdfService.getStartVersion(testConsumerId, testTable)(sparkSpy) shouldBe 2L
    }

    test("getEarliest/LatestAvailableVersion") {
        val sparkSpy = spy(spark)
        val historyRows = Seq(
            // version 0 was vacuumed but version 2 is present
            DescibeHistoryRow(1L, "CREATE TABLE", Map("isManaged" -> "true")),
            DescibeHistoryRow(2L, "UPDATE", Map.empty),
            DescibeHistoryRow(3L, "UPDATE", Map.empty)
        )
        doReturn(historyRows.toDF, Seq.empty: _*)
            .when(sparkSpy).sql(s"DESCRIBE HISTORY $testTable")

        cdfService.getEarliestAvailableVersion(testTable)(sparkSpy) shouldBe 1L
        cdfService.getLatestAvailableVersion(testTable)(sparkSpy) shouldBe 3L
    }

}

object ChangeDataFeedServiceTest {

    case class ShowTablePropertiesRow(key: String, value: String)

    // This is only a subset of DESCRIBE HISTORY columns
    case class DescibeHistoryRow(version: Long, operation: String, operationParameters: Map[String, String])

}
