package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.model.OrderTenderType
import refined_zone.pos_parbrink.processor.OrderTendersProcessorTest.OrderTenderRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class OrderTendersProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_order_tenders_processor")

    test("write Parbrink order tenders") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val orderTender_A = OrderTenderRefined(
            tender_id = 1,
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            tender_nm = "Cash",
            tender_type = OrderTenderType.Cash.value
        )
        val orderTenders_1 = Seq(orderTender_A).toDF()

        // when
        // write orderTenders_1 first time
        OrderTendersProcessor.writeOrderTenders(orderTenders_1, cxiPartnerId, destTable)

        // then
        withClue("Saved order tenders do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", orderTenders_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(orderTenders_1, actual)
        }

        // when
        // write orderTenders_1 one more time
        OrderTendersProcessor.writeOrderTenders(orderTenders_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved order tenders do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", orderTenders_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(orderTenders_1, actual)
        }

        // given
        val orderTender_A_modified = orderTender_A.copy(tender_nm = "Cash modified")
        val orderTender_B = OrderTenderRefined(
            tender_id = 2,
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            tender_nm = "$1",
            tender_type = OrderTenderType.GiftCard.value
        )
        val orderTenders_2 = Seq(orderTender_A_modified, orderTender_B).toDF()

        // when
        // write modified orderTender_A and new orderTender_B
        OrderTendersProcessor.writeOrderTenders(orderTenders_2, cxiPartnerId, destTable)

        // then
        // orderTender_A updated, orderTender_B added
        withClue("Saved order tenders do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", orderTenders_2.columns.size, actual.columns.size)
            assertDataFrameDataEquals(orderTenders_2, actual)
        }

    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(destTable)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(destTable)
    }

    def createTempTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE $tableName
               |(
               |    `tender_id`      STRING NOT NULL,
               |    `cxi_partner_id` STRING NOT NULL,
               |    `location_id`    STRING,
               |    `tender_nm`      STRING,
               |    `tender_type`    INT    NOT NULL
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
