package com.cxi.cdp.data_processing
package support.change_data_feed

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.{BeforeAndAfterEach, Matchers}

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class ChangeDataFeedServiceIntegrationTest extends BaseSparkBatchJobTest with Matchers with BeforeAndAfterEach {

    val destDb = "default"
    val cdfTable = generateUniqueTableName("integration_test_cdf_tracker")

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createCdfTrackerTable(s"$destDb.$cdfTable")
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropCdfTrackerTable(s"$destDb.$cdfTable")
    }

    test("setLatestProcessedVersion") {
        val cdfService = new ChangeDataFeedService(cdfTable)

        val firstConsumer = "first_consumer"
        val secondConsumer = "second_consumer"

        val firstTable = "first_table"
        val secondTable = "second_table"

        cdfService.setLatestProcessedVersion(
            firstConsumer,
            CdfTrackerTableVersion(firstTable, 11L),
            CdfTrackerTableVersion(secondTable, 15L)
        )(spark)
        cdfService.getLatestProcessedVersion(firstConsumer, firstTable)(spark) shouldBe Some(11L)
        cdfService.getLatestProcessedVersion(firstConsumer, secondTable)(spark) shouldBe Some(15L)
        cdfService.getLatestProcessedVersion(secondConsumer, firstTable)(spark) shouldBe None

        cdfService.setLatestProcessedVersion(firstConsumer, CdfTrackerTableVersion(firstTable, 17L))(spark)
        cdfService.setLatestProcessedVersion(secondConsumer, CdfTrackerTableVersion(firstTable, 4L))(spark)
        cdfService.getLatestProcessedVersion(firstConsumer, firstTable)(spark) shouldBe Some(17L)
        cdfService.getLatestProcessedVersion(firstConsumer, secondTable)(spark) shouldBe Some(15L)
        cdfService.getLatestProcessedVersion(secondConsumer, firstTable)(spark) shouldBe Some(4L)
    }

    private def createCdfTrackerTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE $tableName
               |(
               |  consumer_id STRING,
               |  table_name STRING,
               |  latest_processed_version BIGINT
               |) USING delta
               |  PARTITIONED BY (consumer_id);
               |""".stripMargin)
    }

    private def dropCdfTrackerTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
