package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.identity.model.IdentityId
import refined_zone.hub.identity.model.IdentityType.{CardHolderNameTypeNumber, Email, Phone}
import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import refined_zone.pos_parbrink.processor.OrderSummaryProcessorTest.OrderRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.utils.DateTimeTestUtils.{sqlDate, sqlTimestamp}
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class OrderSummaryProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_order_summary_processor")

    test("write Parbrink orders") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val order_A = OrderRefined(
            ord_id = "3132415954391041",
            ord_desc = "DT 1",
            ord_total = 8.68,
            ord_date = sqlDate("2022-08-03"),
            ord_timestamp = sqlTimestamp("2022-08-03T10:05:53.960489-07:00"),
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            ord_state_id = OrderStateType.Completed.code,
            ord_type = null,
            emp_id = "240005",
            total_taxes_amount = 0.7,
            total_tip_amount = 0.5,
            tender_ids = Seq("9"),
            item_quantity = null,
            item_total = 3.99,
            discount_id = null,
            discount_amount = None,
            dsp_qty = None,
            dsp_ttl = None,
            guest_check_line_item_id = "1",
            line_id = null,
            item_price_id = null,
            reason_code_id = null,
            service_charge_id = null,
            service_charge_amount = None,
            ord_sub_total = 7.98,
            ord_pay_total = 8.68,
            item_id = "211481",
            taxes_id = "[1]",
            taxes_amount = 0.35,
            ord_originate_channel_id = OrderChannelType.PhysicalLane.code,
            ord_target_channel_id = OrderChannelType.PhysicalPickup.code,
            cxi_identity_ids = Seq(
                IdentityId(Email.code, "hashed_email_1"),
                IdentityId(Phone.code, "hashed_phone_1"),
                IdentityId(Phone.code, "hashed_phone_2"),
                IdentityId(CardHolderNameTypeNumber.code, "hashed_payment_details")
            ),
            feed_date = sqlDate("2022-02-24")
        )

        val orders_1 = Seq(order_A).toDF()

        // when
        // write orders_1 first time
        OrderSummaryProcessor.writeOrders(orders_1, cxiPartnerId, destTable)

        // then
        withClue("Saved orders do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", orders_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(orders_1, actual)
        }

        // when
        // write orders_1 one more time
        OrderSummaryProcessor.writeOrders(orders_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved orders do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", orders_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(orders_1, actual)
        }

        // given
        val order_A_edited = order_A.copy(ord_desc = "modified") // order_A should be updated
        val order_B = order_A.copy(guest_check_line_item_id = "2") // should be added
        val orders_2 = Seq(order_A_edited, order_B).toDF()

        // when
        OrderSummaryProcessor.writeOrders(orders_2, cxiPartnerId, destTable)

        // then
        withClue("Saved orders do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", orders_2.columns.size, actual.columns.size)
            assertDataFrameDataEquals(orders_2, actual)
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
                     |`ord_id`                   STRING,
                     |    `ord_desc`                 STRING,
                     |    `ord_total`                DECIMAL(9, 2),
                     |    `ord_date`                 DATE,
                     |    `ord_timestamp`            TIMESTAMP,
                     |    `cxi_partner_id`           STRING,
                     |    `location_id`              STRING,
                     |    `ord_state_id`             INT,
                     |    `ord_type`                 STRING,
                     |    `ord_originate_channel_id` INT,
                     |    `ord_target_channel_id`    INT,
                     |    `item_quantity`            INT,
                     |    `item_total`               DECIMAL(9, 2),
                     |    `emp_id`                   STRING,
                     |    `discount_id`              STRING,
                     |    `discount_amount`          DECIMAL(9, 2),
                     |    `dsp_qty`                  INT,
                     |    `dsp_ttl`                  DECIMAL(9, 2),
                     |    `guest_check_line_item_id` STRING,
                     |    `line_id`                  STRING,
                     |    `taxes_id`                 STRING,
                     |    `taxes_amount`             DECIMAL(9, 2),
                     |    `item_id`                  STRING,
                     |    `item_price_id`            STRING,
                     |    `reason_code_id`           STRING,
                     |    `service_charge_id`        STRING,
                     |    `service_charge_amount`    DECIMAL(9, 2),
                     |    `total_taxes_amount`       DECIMAL(9, 2),
                     |    `total_tip_amount`         DECIMAL(9, 2),
                     |    `tender_ids`               ARRAY<STRING>,
                     |    `cxi_identity_ids`         ARRAY<STRUCT<`identity_type`: STRING, `cxi_identity_id`: STRING>>,
                     |    `ord_sub_total`            DECIMAL(9, 2),
                     |    `ord_pay_total`            DECIMAL(9, 2),
                     |    `feed_date`                DATE
                     |) USING delta
                     |PARTITIONED BY (cxi_partner_id, ord_date);
                     |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
